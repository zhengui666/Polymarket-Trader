pub mod execution;
pub mod portfolio;
pub mod risk;
pub mod settlement;
pub mod sim;

pub use execution::{
    run_execution_engine, ExecutionVenue, VenueHeartbeat, VenueOrderState, VenueSubmitAck,
};
pub use polymarket_config::SimConfig;
pub use polymarket_core::{SimDriftReport, SimMode, SimRunReport};
pub use portfolio::{
    BucketUsage, PortfolioAuditTrail, PortfolioCandidateDecision, PortfolioConfig, PortfolioEngine,
    PortfolioEngineService, PortfolioError, PortfolioOpportunity, PortfolioResult, StrategyBucket,
};
pub use risk::{
    ApprovedTradeIntentBatch, RiskCode, RiskConfig, RiskContext, RiskEngine, RiskEngineService,
    RiskError, RiskEvaluation, RiskFinding, RiskMarketState, RiskRejection, RiskRuntimeMode,
    RuntimeDecision, StrategyRiskState,
};
pub use settlement::{
    run_settlement_engine, SettlementConfig, SettlementEngineService, TOPIC_SETTLEMENT_CASH,
    TOPIC_SETTLEMENT_REPORT, TOPIC_SETTLEMENT_TASK,
};
pub use sim::{
    run_sim_engine, FeeModel, FillModel, LatencyModel, MaintenanceCalendar, QueueModel,
    SimEngineService, SimExchangeAdapter, SimulatedOrderOutcome, SimulatedOrderRequest,
};

use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use polymarket_audit::{AuditSink, StorageAuditSink};
use polymarket_common::{init_tracing, shutdown_signal};
use polymarket_config::{
    DomainConfig, MdGatewayConfig, MdGatewayMode, NodeConfig, OpportunityEngineConfig,
    RiskEngineConfig, RulesEngineConfig,
};
use polymarket_core::{
    now, AccountDomain, ConstraintGraphSnapshot, DurableEvent, EventFamilySnapshot, GraphScope,
    MarketBook, MarketDataChannel, MarketQuoteLevel, MarketSnapshot, MarketTrade,
    MdGatewayCursor, MdGatewayRuntime, NewDurableEvent, NewStateSnapshot, PortfolioSnapshot,
    RawMarketDocument, ReplayRequest, RiskBudgetSnapshot, RulesInvalidation,
    RuntimeHealth as CoreRuntimeHealth, RuntimeMode, ServiceKind, StartupManifest,
    StateSnapshot, StrategyKind, TradeIntentBatch, TradeSide,
};
use polymarket_msgbus::MessageBus;
use polymarket_opportunity::{
    OpportunityEngine, OpportunityEngineService, OrderbookSnapshotSet,
    RuntimeHealth as OpportunityRuntimeHealth, ScanContext, ScanSettings,
};
use polymarket_rules::{RulesEngine, RulesEngineService};
use polymarket_storage::Store;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::task::JoinSet;
use tokio::time::{interval, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[derive(Clone)]
pub struct ServiceContext {
    pub domain: AccountDomain,
    pub domain_config: DomainConfig,
    pub store: Store,
    pub bus: MessageBus,
    pub audit: Arc<dyn AuditSink>,
    pub heartbeat_interval: Duration,
}

pub struct NodeRuntime {
    pub config: NodeConfig,
    pub manifest: StartupManifest,
}

pub fn run_service(kind: ServiceKind) -> Result<()> {
    let domain = env::args()
        .nth(1)
        .or_else(|| env::var("POLYMARKET_NODE_DOMAIN").ok())
        .unwrap_or_else(|| "SIM".to_owned())
        .parse::<AccountDomain>()?;
    let config = NodeConfig::from_env(domain)?;
    let mut runtime = NodeRuntime::new(config);
    runtime
        .manifest
        .services
        .retain(|service| service.kind == kind);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build tokio runtime")?;
    rt.block_on(async move {
        let cancellation = CancellationToken::new();
        let shutdown = cancellation.clone();
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown.cancel();
        });
        runtime.run_until_cancelled(cancellation).await
    })
}

impl NodeRuntime {
    pub fn new(config: NodeConfig) -> Self {
        Self {
            config,
            manifest: StartupManifest::default(),
        }
    }

    pub fn bootstrap(&self) -> Result<ServiceContext> {
        init_tracing(&self.config.shared.log_filter);

        let selected = &self.config.selected_domain_config;
        log_domain_summary(selected);

        if selected.runtime_mode == RuntimeMode::Execute && !selected.execution_approved {
            return Err(anyhow!(
                "domain `{}` cannot enter EXECUTE without explicit execution approval",
                selected.domain()
            ));
        }

        let store = Store::new(
            selected.database_path.clone(),
            selected.domain(),
            selected.namespace().to_owned(),
            selected.audit_prefix.clone(),
        );
        store.init().context("failed to initialize storage")?;
        store
            .set_runtime_mode(
                selected.domain(),
                selected.runtime_mode,
                format!(
                    "bootstrap namespace={} execution_approved={} l2={} signer={}",
                    selected.namespace(),
                    selected.execution_approved,
                    selected.l2_credentials.kind(),
                    selected.signer.kind()
                ),
            )
            .context("failed to persist startup runtime mode")?;

        let audit: Arc<dyn AuditSink> = Arc::new(StorageAuditSink::new(store.clone()));
        audit.record(
            Some(selected.domain()),
            "node_runtime",
            "bootstrap",
            &format!(
                "namespace={} runtime_mode={} execution_approved={} l2={} signer={}",
                selected.namespace(),
                selected.runtime_mode,
                selected.execution_approved,
                selected.l2_credentials.kind(),
                selected.signer.kind()
            ),
        )?;

        Ok(ServiceContext {
            domain: selected.domain(),
            domain_config: selected.clone(),
            store,
            bus: MessageBus::new(512),
            audit,
            heartbeat_interval: self.config.shared.heartbeat_interval,
        })
    }

    pub async fn run_until_cancelled(self, cancellation: CancellationToken) -> Result<()> {
        let context = self.bootstrap()?;
        let mut tasks = JoinSet::new();

        for service in self.manifest.services {
            if !service.enabled {
                continue;
            }

            let task_context = context.clone();
            let task_cancellation = cancellation.clone();
            tasks.spawn(async move {
                if let Err(error) =
                    run_service_loop(task_context, service.kind, task_cancellation).await
                {
                    error!(service = %service.kind, error = %error, "service loop failed");
                    Err(error)
                } else {
                    Ok(())
                }
            });
        }

        let mut failure = None;
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure = Some(error);
                    break;
                }
                Err(error) => {
                    failure = Some(anyhow!("service task join error: {error}"));
                    break;
                }
            }
        }

        cancellation.cancel();
        while tasks.join_next().await.is_some() {}

        if let Some(error) = failure {
            return Err(error);
        }
        Ok(())
    }
}

async fn run_service_loop(
    context: ServiceContext,
    kind: ServiceKind,
    cancellation: CancellationToken,
) -> Result<()> {
    context.audit.record(
        Some(context.domain),
        kind.as_str(),
        "service_started",
        &format!("runtime_mode={}", context.domain_config.runtime_mode),
    )?;

    match kind {
        ServiceKind::MdGateway => run_md_gateway(context, cancellation).await,
        ServiceKind::RulesEngine => run_rules_engine(context, cancellation).await,
        ServiceKind::OpportunityEngine => run_opportunity_engine(context, cancellation).await,
        ServiceKind::PortfolioEngine => run_portfolio_engine(context, cancellation).await,
        ServiceKind::RiskEngine => run_risk_engine(context, cancellation).await,
        ServiceKind::SimEngine => run_sim_engine(context, cancellation).await,
        ServiceKind::ExecutionEngine => run_execution_engine(context, cancellation).await,
        ServiceKind::SettlementEngine => run_settlement_engine(context, cancellation).await,
        _ => run_heartbeat_only_service(context, kind, cancellation).await,
    }
}

async fn run_heartbeat_only_service(
    context: ServiceContext,
    kind: ServiceKind,
    cancellation: CancellationToken,
) -> Result<()> {
    let mut ticker = interval(context.heartbeat_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                context.audit.record(
                    Some(context.domain),
                    kind.as_str(),
                    "service_stopped",
                    "cancellation requested",
                )?;
                return Ok(());
            }
            _ = ticker.tick() => {
                publish_heartbeat(&context, kind, "idle");
            }
        }
    }
}

async fn run_md_gateway(context: ServiceContext, cancellation: CancellationToken) -> Result<()> {
    let config = MdGatewayConfig::from_env(&context.domain_config)?;
    context.audit.record(
        Some(context.domain),
        ServiceKind::MdGateway.as_str(),
        "md_gateway_configured",
        &format!(
            "mode={} market_ws={} user_ws={} replay_input={}",
            config.mode.as_str(),
            config.market_ws_url,
            config.user_ws_url,
            config.replay_input_path.display(),
        ),
    )?;

    match config.mode {
        MdGatewayMode::ReplayFile => {
            let mut processor = ReplayMdGatewayProcessor::restore(context.clone(), config)?;
            processor.ensure_ready()?;
            let mut heartbeat = interval(context.heartbeat_interval);
            heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
            let mut poller = interval(processor.config.replay_poll_interval);
            poller.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        processor.context.audit.record(
                            Some(processor.context.domain),
                            ServiceKind::MdGateway.as_str(),
                            "service_stopped",
                            "cancellation requested",
                        )?;
                        return Ok(());
                    }
                    _ = heartbeat.tick() => {
                        let detail = format!(
                            "mode=replay_file source={} line={} offset={} processed={} errors={} stale={}",
                            processor.config.replay_input_path.display(),
                            processor.cursor.line_number,
                            processor.offset,
                            processor.stats.processed,
                            processor.stats.errors,
                            processor.stats.stale
                        );
                        publish_heartbeat(&processor.context, ServiceKind::MdGateway, &detail);
                    }
                    _ = poller.tick() => {
                        if let Err(error) = processor.poll_once() {
                            processor.stats.errors += 1;
                            warn!(error = %error, "md-gateway replay polling failed");
                            processor.context.audit.record(
                                Some(processor.context.domain),
                                ServiceKind::MdGateway.as_str(),
                                "poll_failed",
                                &error.to_string(),
                            )?;
                        }
                    }
                }
            }
        }
        MdGatewayMode::Realtime => RealtimeMdGatewayProcessor::new(context, config)
            .run(cancellation)
            .await,
    }
}

fn publish_heartbeat(context: &ServiceContext, kind: ServiceKind, detail: &str) {
    if let Err(error) = context
        .store
        .heartbeat_service(kind, context.domain, true, detail)
    {
        error!(service = %kind, error = %error, "failed to persist service heartbeat");
    }
    context
        .bus
        .publish(kind, context.domain, "service.heartbeat", detail.to_owned());
    info!(service = %kind, domain = %context.domain, detail = detail, "service heartbeat");
}

#[derive(Debug, Default)]
struct MdGatewayStats {
    processed: u64,
    errors: u64,
    stale: u64,
    market_messages: u64,
    user_messages: u64,
}

#[derive(Debug, Clone, Default)]
struct MdGatewayRuntimeState {
    market_stream_connected: bool,
    user_stream_connected: bool,
    last_market_event_at: Option<polymarket_core::Timestamp>,
    last_user_event_at: Option<polymarket_core::Timestamp>,
}

#[derive(Debug, Clone)]
struct MarketMetadata {
    market_id: String,
    event_id: String,
    condition_id: String,
    token_ids: Vec<String>,
    title: String,
    category: String,
    outcomes: Vec<String>,
    resolution_source: Option<String>,
    neg_risk: bool,
    neg_risk_augmented: bool,
    tick_size: f64,
    raw_rules_text: String,
    market_status: String,
}

#[derive(Debug, Clone)]
struct MdGatewayL2Credentials {
    key: String,
    secret: String,
    passphrase: Option<String>,
}

struct ReplayMdGatewayProcessor {
    context: ServiceContext,
    config: MdGatewayConfig,
    offset: u64,
    cursor: MdGatewayCursor,
    stats: MdGatewayStats,
    runtime: MdGatewayRuntimeState,
}

impl ReplayMdGatewayProcessor {
    fn restore(context: ServiceContext, config: MdGatewayConfig) -> Result<Self> {
        let snapshot = context
            .store
            .latest_snapshot(context.domain, "md_gateway_cursor", "market-data")
            .context("failed to load md-gateway cursor snapshot")?;
        let cursor = match snapshot {
            Some(snapshot) => serde_json::from_value::<MdGatewayCursor>(snapshot.payload)
                .context("failed to decode md-gateway cursor snapshot")?,
            None => MdGatewayCursor {
                source_path: config.replay_input_path.display().to_string(),
                line_number: 0,
                checksum: None,
                updated_at: now(),
            },
        };

        Ok(Self {
            context,
            config,
            offset: 0,
            cursor,
            stats: MdGatewayStats::default(),
            runtime: MdGatewayRuntimeState::default(),
        })
    }

    fn ensure_ready(&self) -> Result<()> {
        if !self.config.replay_input_path.exists() {
            return Err(anyhow!(
                "replay input file `{}` does not exist",
                self.config.replay_input_path.display()
            ));
        }
        Ok(())
    }

    fn poll_once(&mut self) -> Result<()> {
        let mut file = File::open(&self.config.replay_input_path).with_context(|| {
            format!(
                "failed to open {}",
                self.config.replay_input_path.display()
            )
        })?;
        let current_len = file.metadata()?.len();
        if self.offset > current_len {
            self.offset = 0;
            self.cursor.line_number = 0;
            self.context.audit.record(
                Some(self.context.domain),
                ServiceKind::MdGateway.as_str(),
                "cursor_reset",
                "input file truncated; restarting from beginning",
            )?;
        }

        file.seek(SeekFrom::Start(self.offset))?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();

        loop {
            line.clear();
            let bytes = reader.read_line(&mut line)?;
            if bytes == 0 {
                self.offset = reader.stream_position()?;
                break;
            }

            self.offset = reader.stream_position()?;
            self.cursor.line_number += 1;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            match self.process_line(trimmed) {
                Ok(()) => {
                    self.stats.processed += 1;
                    self.stats.market_messages += 1;
                    self.runtime.market_stream_connected = true;
                    self.runtime.last_market_event_at = Some(now());
                    self.persist_cursor()?;
                }
                Err(error) => {
                    self.stats.errors += 1;
                    self.context.audit.record(
                        Some(self.context.domain),
                        ServiceKind::MdGateway.as_str(),
                        "normalize_failed",
                        &format!("line={} error={error}", self.cursor.line_number),
                    )?;
                }
            }
        }

        persist_md_gateway_runtime(
            &self.context,
            self.config.mode,
            &self.runtime,
            &self.stats,
        )?;
        Ok(())
    }

    fn process_line(&mut self, line: &str) -> Result<()> {
        let raw = serde_json::from_str::<RawMarketEvent>(line)
            .with_context(|| format!("invalid json on line {}", self.cursor.line_number))?;
        let normalized = normalize_market_event(&raw, self.cursor.line_number)?;
        let payload = merged_market_payload(&raw, &normalized)?;
        publish_market_snapshot(
            &self.context,
            &normalized,
            payload,
            Some(json!({
                "source_path": self.config.replay_input_path.display().to_string(),
                "line_number": self.cursor.line_number,
            })),
        )?;

        if normalized.book.is_none() && normalized.status.is_none() {
            self.stats.stale += 1;
        }

        Ok(())
    }

    fn persist_cursor(&mut self) -> Result<()> {
        self.cursor.source_path = self.config.replay_input_path.display().to_string();
        self.cursor.updated_at = now();
        self.cursor.checksum = Some(self.offset);

        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: "md_gateway_cursor".to_owned(),
            aggregate_id: "market-data".to_owned(),
            version: self.cursor.line_number as i64,
            payload: serde_json::to_value(&self.cursor)?,
            derived_from_sequence: None,
            created_at: self.cursor.updated_at,
        })?;
        Ok(())
    }
}

struct RealtimeMdGatewayProcessor {
    context: ServiceContext,
    config: MdGatewayConfig,
    stats: MdGatewayStats,
    runtime: MdGatewayRuntimeState,
    metadata_by_market: HashMap<String, MarketMetadata>,
    metadata_by_asset: HashMap<String, MarketMetadata>,
}

impl RealtimeMdGatewayProcessor {
    fn new(context: ServiceContext, config: MdGatewayConfig) -> Self {
        Self {
            context,
            config,
            stats: MdGatewayStats::default(),
            runtime: MdGatewayRuntimeState::default(),
            metadata_by_market: HashMap::new(),
            metadata_by_asset: HashMap::new(),
        }
    }

    async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        self.refresh_metadata().await?;
        let mut heartbeat = interval(self.context.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut metadata_refresh = interval(self.config.metadata_refresh_interval);
        metadata_refresh.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            let mut market_stream = self.connect_market_stream().await?;
            let mut user_stream = self.connect_user_stream().await?;
            self.runtime.market_stream_connected = true;
            self.runtime.user_stream_connected = user_stream.is_some();
            persist_md_gateway_runtime(
                &self.context,
                self.config.mode,
                &self.runtime,
                &self.stats,
            )?;

            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        self.context.audit.record(
                            Some(self.context.domain),
                            ServiceKind::MdGateway.as_str(),
                            "service_stopped",
                            "cancellation requested",
                        )?;
                        return Ok(());
                    }
                    _ = heartbeat.tick() => {
                        publish_heartbeat(
                            &self.context,
                            ServiceKind::MdGateway,
                            &format!(
                                "mode=realtime markets={} market_connected={} user_connected={} market_messages={} user_messages={}",
                                self.metadata_by_market.len(),
                                self.runtime.market_stream_connected,
                                self.runtime.user_stream_connected,
                                self.stats.market_messages,
                                self.stats.user_messages,
                            ),
                        );
                        let _ = market_stream.send(Message::Text("PING".to_owned())).await;
                        if let Some(user_stream) = user_stream.as_mut() {
                            let _ = user_stream.send(Message::Text("PING".to_owned())).await;
                        }
                        persist_md_gateway_runtime(
                            &self.context,
                            self.config.mode,
                            &self.runtime,
                            &self.stats,
                        )?;
                    }
                    _ = metadata_refresh.tick() => {
                        self.refresh_metadata().await?;
                    }
                    maybe_message = market_stream.next() => {
                        match maybe_message {
                            Some(Ok(message)) => self.handle_market_message(message)?,
                            Some(Err(error)) => {
                                self.runtime.market_stream_connected = false;
                                persist_md_gateway_runtime(
                                    &self.context,
                                    self.config.mode,
                                    &self.runtime,
                                    &self.stats,
                                )?;
                                warn!(error = %error, "market websocket receive failed");
                                break;
                            }
                            None => {
                                self.runtime.market_stream_connected = false;
                                persist_md_gateway_runtime(
                                    &self.context,
                                    self.config.mode,
                                    &self.runtime,
                                    &self.stats,
                                )?;
                                break;
                            }
                        }
                    }
                    maybe_message = async {
                        match user_stream.as_mut() {
                            Some(stream) => stream.next().await,
                            None => None,
                        }
                    } => {
                        if let Some(result) = maybe_message {
                            match result {
                                Ok(message) => self.handle_user_message(message)?,
                                Err(error) => {
                                    self.runtime.user_stream_connected = false;
                                    persist_md_gateway_runtime(
                                        &self.context,
                                        self.config.mode,
                                        &self.runtime,
                                        &self.stats,
                                    )?;
                                    warn!(error = %error, "user websocket receive failed");
                                    user_stream = None;
                                }
                            }
                        }
                    }
                }
            }

            tokio::time::sleep(self.config.reconnect_backoff).await;
        }
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        let url = format!(
            "{}/markets?active=true&closed=false&limit=1000",
            self.config.metadata_api_base_url.trim_end_matches('/'),
        );
        let response = reqwest::get(url).await?.error_for_status()?;
        let payload = response.json::<Value>().await?;
        let items = payload
            .as_array()
            .cloned()
            .or_else(|| payload.get("data").and_then(Value::as_array).cloned())
            .unwrap_or_default();

        let mut metadata_by_market = HashMap::new();
        let mut metadata_by_asset = HashMap::new();
        for item in items {
            if let Some(metadata) = parse_market_metadata(&item) {
                for asset_id in &metadata.token_ids {
                    metadata_by_asset.insert(asset_id.clone(), metadata.clone());
                }
                metadata_by_market.insert(metadata.market_id.clone(), metadata);
            }
        }
        self.metadata_by_market = metadata_by_market;
        self.metadata_by_asset = metadata_by_asset;
        Ok(())
    }

    async fn connect_market_stream(
        &self,
    ) -> Result<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    > {
        let (mut stream, _) = connect_async(self.config.market_ws_url.as_str()).await?;
        let asset_ids = self
            .metadata_by_asset
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for chunk in asset_ids.chunks(self.config.market_subscription_chunk_size) {
            stream
                .send(Message::Text(
                    json!({
                        "assets_ids": chunk,
                        "type": "market",
                        "custom_feature_enabled": true,
                    })
                    .to_string(),
                ))
                .await?;
        }
        Ok(stream)
    }

    async fn connect_user_stream(
        &self,
    ) -> Result<
        Option<
            tokio_tungstenite::WebSocketStream<
                tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
            >,
        >,
    > {
        if self.context.domain == AccountDomain::Sim {
            return Ok(None);
        }
        let raw_credentials = self
            .context
            .domain_config
            .l2_credentials
            .resolve_string()?
            .ok_or_else(|| anyhow!("l2 credentials are required for realtime user channel"))?;
        let credentials = parse_l2_credentials(&raw_credentials, self.context.domain_config.wallet_id.clone())?;
        let (mut stream, _) = connect_async(self.config.user_ws_url.as_str()).await?;
        let markets = self
            .metadata_by_market
            .values()
            .map(|item| item.condition_id.clone())
            .collect::<Vec<_>>();
        stream
            .send(Message::Text(
                json!({
                    "type": "user",
                    "auth": {
                        "apiKey": credentials.key,
                        "secret": credentials.secret,
                        "passphrase": credentials.passphrase,
                    },
                    "markets": markets,
                })
                .to_string(),
            ))
            .await?;
        Ok(Some(stream))
    }

    fn handle_market_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Text(text) => {
                self.stats.market_messages += 1;
                self.runtime.market_stream_connected = true;
                self.runtime.last_market_event_at = Some(now());
                for raw in decode_market_events(&text, &self.metadata_by_asset, &self.metadata_by_market) {
                    let normalized = normalize_market_event(&raw, self.stats.market_messages)?;
                    let payload = merged_market_payload(&raw, &normalized)?;
                    publish_market_snapshot(&self.context, &normalized, payload, None)?;
                    self.stats.processed += 1;
                    if normalized.book.is_none() && normalized.status.is_none() {
                        self.stats.stale += 1;
                    }
                }
            }
            Message::Ping(payload) => {
                self.runtime.market_stream_connected = true;
                self.runtime.last_market_event_at = Some(now());
                self.stats.market_messages += 1;
                self.context.bus.publish(
                    ServiceKind::MdGateway,
                    self.context.domain,
                    "md_gateway.market.ping",
                    String::from_utf8_lossy(&payload).to_string(),
                );
            }
            Message::Pong(_) => {
                self.runtime.market_stream_connected = true;
                self.runtime.last_market_event_at = Some(now());
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_user_message(&mut self, message: Message) -> Result<()> {
        match message {
            Message::Text(text) => {
                self.runtime.user_stream_connected = true;
                self.runtime.last_user_event_at = Some(now());
                self.stats.user_messages += 1;
                self.context.bus.publish(
                    ServiceKind::MdGateway,
                    self.context.domain,
                    "md_gateway.user.event",
                    text,
                );
            }
            Message::Ping(_) | Message::Pong(_) => {
                self.runtime.user_stream_connected = true;
                self.runtime.last_user_event_at = Some(now());
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawMarketEvent {
    market_id: String,
    #[serde(default)]
    event_id: Option<String>,
    channel: MarketDataChannel,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    best_bid_price: Option<f64>,
    #[serde(default)]
    best_bid_size: Option<f64>,
    #[serde(default)]
    best_ask_price: Option<f64>,
    #[serde(default)]
    best_ask_size: Option<f64>,
    #[serde(default)]
    trade_price: Option<f64>,
    #[serde(default)]
    trade_size: Option<f64>,
    #[serde(default)]
    trade_side: Option<String>,
    #[serde(default)]
    trade_id: Option<String>,
    #[serde(default)]
    observed_at: Option<polymarket_core::Timestamp>,
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    outcomes: Option<Vec<String>>,
    #[serde(default)]
    token_ids: Option<Vec<String>>,
    #[serde(default)]
    resolution_source: Option<String>,
    #[serde(default)]
    neg_risk: Option<bool>,
    #[serde(default)]
    neg_risk_augmented: Option<bool>,
    #[serde(default)]
    tick_size: Option<f64>,
    #[serde(default)]
    rules_text: Option<String>,
}

fn normalize_market_event(raw: &RawMarketEvent, sequence: u64) -> Result<MarketSnapshot> {
    let received_at = now();
    let observed_at = raw.observed_at.unwrap_or(received_at);
    let best_bid = quote_level(raw.best_bid_price, raw.best_bid_size)?;
    let best_ask = quote_level(raw.best_ask_price, raw.best_ask_size)?;

        let last_trade = match (raw.trade_price, raw.trade_size) {
        (Some(price), Some(size)) => Some(MarketTrade {
            price,
            size,
            side: raw.trade_side.clone(),
            trade_id: raw.trade_id.clone(),
            occurred_at: observed_at,
        }),
        (None, None) => None,
        _ => {
            return Err(anyhow!(
                "trade_price and trade_size must be provided together"
            ))
        }
    };

    let mid_price = match (&best_bid, &best_ask) {
        (Some(bid), Some(ask)) if bid.price <= ask.price => Some((bid.price + ask.price) / 2.0),
        (Some(bid), Some(ask)) => {
            return Err(anyhow!(
                "crossed book detected: bid {} ask {}",
                bid.price,
                ask.price
            ));
        }
        _ => None,
    };
    let spread_bps = match (&best_bid, &best_ask, mid_price) {
        (Some(bid), Some(ask), Some(mid)) if mid > 0.0 => {
            Some(((ask.price - bid.price) / mid) * 10_000.0)
        }
        _ => None,
    };

    let book = if best_bid.is_some() || best_ask.is_some() || last_trade.is_some() {
        Some(MarketBook {
            best_bid,
            best_ask,
            last_trade,
            mid_price,
            spread_bps,
            observed_at,
        })
    } else {
        None
    };

    Ok(MarketSnapshot {
        market_id: raw.market_id.clone(),
        channel: raw.channel,
        status: raw.status.clone(),
        book,
        sequence: sequence as i64,
        source_event_id: raw.event_id.clone(),
        observed_at,
        received_at,
    })
}

fn merged_market_payload(raw: &RawMarketEvent, normalized: &MarketSnapshot) -> Result<Value> {
    let mut payload = serde_json::to_value(normalized)?;
    let Value::Object(ref mut object) = payload else {
        return Ok(payload);
    };
    insert_optional_string(object, "event_id", raw.event_id.clone());
    insert_optional_string(object, "condition_id", raw.condition_id.clone());
    insert_optional_string(object, "title", raw.title.clone());
    insert_optional_string(object, "category", raw.category.clone());
    insert_optional_string(object, "resolution_source", raw.resolution_source.clone());
    insert_optional_string(object, "rules_text", raw.rules_text.clone());
    if let Some(outcomes) = raw.outcomes.clone() {
        object.insert("outcomes".to_owned(), json!(outcomes));
    }
    if let Some(token_ids) = raw.token_ids.clone() {
        object.insert("token_ids".to_owned(), json!(token_ids));
    }
    if let Some(neg_risk) = raw.neg_risk {
        object.insert("neg_risk".to_owned(), json!(neg_risk));
    }
    if let Some(neg_risk_augmented) = raw.neg_risk_augmented {
        object.insert("neg_risk_augmented".to_owned(), json!(neg_risk_augmented));
    }
    if let Some(tick_size) = raw.tick_size {
        object.insert("tick_size".to_owned(), json!(tick_size));
    }
    Ok(payload)
}

fn insert_optional_string(
    object: &mut serde_json::Map<String, Value>,
    key: &str,
    value: Option<String>,
) {
    if let Some(value) = value {
        object.insert(key.to_owned(), Value::String(value));
    }
}

fn publish_market_snapshot(
    context: &ServiceContext,
    snapshot: &MarketSnapshot,
    payload: Value,
    metadata: Option<Value>,
) -> Result<()> {
    let aggregate_id = snapshot.market_id.clone();
    let event_type = format!("md_gateway.{}", snapshot.channel.as_str());
    let event = context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("market:{}", aggregate_id),
        aggregate_type: "market_snapshot".to_owned(),
        aggregate_id: aggregate_id.clone(),
        event_type,
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: metadata.unwrap_or_else(|| json!({})),
        created_at: snapshot.received_at,
    })?;

    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "market_snapshot".to_owned(),
        aggregate_id: aggregate_id.clone(),
        version: event.sequence,
        payload,
        derived_from_sequence: Some(event.sequence),
        created_at: snapshot.received_at,
    })?;

    context.bus.publish(
        ServiceKind::MdGateway,
        context.domain,
        format!("market_data.{}", snapshot.channel.as_str()),
        serde_json::to_string(snapshot)?,
    );
    Ok(())
}

fn persist_md_gateway_runtime(
    context: &ServiceContext,
    mode: MdGatewayMode,
    state: &MdGatewayRuntimeState,
    stats: &MdGatewayStats,
) -> Result<()> {
    let runtime = MdGatewayRuntime {
        domain: context.domain,
        mode: mode.as_str().to_owned(),
        market_stream_connected: state.market_stream_connected,
        user_stream_connected: state.user_stream_connected,
        last_market_event_at: state.last_market_event_at,
        last_user_event_at: state.last_user_event_at,
        market_messages_total: stats.market_messages,
        user_messages_total: stats.user_messages,
        observed_at: now(),
    };
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "md_gateway_runtime".to_owned(),
        aggregate_id: "latest".to_owned(),
        version: runtime.observed_at.timestamp_millis(),
        payload: serde_json::to_value(runtime)?,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    Ok(())
}

fn parse_l2_credentials(
    raw: &str,
    wallet_id: Option<String>,
) -> Result<MdGatewayL2Credentials> {
    if let Ok(parsed) = serde_json::from_str::<Value>(raw.trim()) {
        let key = parsed
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let secret = parsed
            .get("secret")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if !key.is_empty() && !secret.is_empty() {
            return Ok(MdGatewayL2Credentials {
                key,
                secret,
                passphrase: parsed
                    .get("passphrase")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            });
        }
    }

    let _ = wallet_id;
    let parts: Vec<_> = raw.trim().split(':').collect();
    if parts.len() >= 2 {
        return Ok(MdGatewayL2Credentials {
            key: parts[0].to_owned(),
            secret: parts[1].to_owned(),
            passphrase: parts.get(2).map(|value| (*value).to_owned()),
        });
    }

    Err(anyhow!("unsupported credential format"))
}

fn parse_market_metadata(value: &Value) -> Option<MarketMetadata> {
    let condition_id = value
        .get("conditionId")
        .or_else(|| value.get("condition_id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let fallback_market_id = value
        .get("market")
        .or_else(|| value.get("market_id"))
        .or_else(|| value.get("id"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let market_id = if !condition_id.is_empty() {
        condition_id.clone()
    } else {
        fallback_market_id
    };
    if market_id.is_empty() {
        return None;
    }

    let token_ids = parse_string_list(
        value.get("clobTokenIds")
            .or_else(|| value.get("token_ids"))
            .or_else(|| value.get("tokenIds")),
    );
    if token_ids.is_empty() {
        return None;
    }

    Some(MarketMetadata {
        market_id,
        event_id: value
            .get("eventId")
            .or_else(|| value.get("event_id"))
            .or_else(|| value.get("questionID"))
            .and_then(Value::as_str)
            .unwrap_or("unknown-event")
            .to_owned(),
        condition_id,
        token_ids,
        title: value
            .get("question")
            .or_else(|| value.get("title"))
            .and_then(Value::as_str)
            .unwrap_or("untitled")
            .to_owned(),
        category: value
            .get("category")
            .and_then(Value::as_str)
            .unwrap_or("uncategorized")
            .to_owned(),
        outcomes: parse_string_list(value.get("outcomes")),
        resolution_source: value
            .get("resolutionSource")
            .or_else(|| value.get("resolution_source"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        neg_risk: value
            .get("negRisk")
            .or_else(|| value.get("neg_risk"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        neg_risk_augmented: value
            .get("negRiskAugmented")
            .or_else(|| value.get("neg_risk_augmented"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        tick_size: value
            .get("minimum_tick_size")
            .or_else(|| value.get("tickSize"))
            .or_else(|| value.get("tick_size"))
            .and_then(as_f64_value)
            .unwrap_or(0.01),
        raw_rules_text: value
            .get("rules")
            .or_else(|| value.get("description"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        market_status: value
            .get("status")
            .or_else(|| value.get("marketStatus"))
            .and_then(Value::as_str)
            .unwrap_or("open")
            .to_owned(),
    })
}

fn parse_string_list(value: Option<&Value>) -> Vec<String> {
    match value {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|item| item.as_str().map(ToOwned::to_owned))
            .collect(),
        Some(Value::String(raw)) => serde_json::from_str::<Vec<String>>(raw)
            .unwrap_or_else(|_| {
                raw.split(',')
                    .map(str::trim)
                    .filter(|item| !item.is_empty())
                    .map(ToOwned::to_owned)
                    .collect()
            }),
        _ => Vec::new(),
    }
}

fn as_f64_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|raw| raw.parse::<f64>().ok()))
}

fn decode_market_events(
    text: &str,
    metadata_by_asset: &HashMap<String, MarketMetadata>,
    metadata_by_market: &HashMap<String, MarketMetadata>,
) -> Vec<RawMarketEvent> {
    let Ok(payload) = serde_json::from_str::<Value>(text) else {
        return Vec::new();
    };
    let mut values = Vec::new();
    flatten_values(&payload, &mut values);
    values
        .into_iter()
        .filter_map(|value| decode_market_event_value(value, metadata_by_asset, metadata_by_market))
        .collect()
}

fn flatten_values<'a>(value: &'a Value, output: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                flatten_values(item, output);
            }
        }
        Value::Object(_) => output.push(value),
        _ => {}
    }
}

fn decode_market_event_value(
    value: &Value,
    metadata_by_asset: &HashMap<String, MarketMetadata>,
    metadata_by_market: &HashMap<String, MarketMetadata>,
) -> Option<RawMarketEvent> {
    let asset_id = value
        .get("asset_id")
        .or_else(|| value.get("assetId"))
        .or_else(|| value.get("token_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let market_key = value
        .get("market")
        .or_else(|| value.get("market_id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let metadata = asset_id
        .as_ref()
        .and_then(|asset| metadata_by_asset.get(asset))
        .or_else(|| market_key.as_ref().and_then(|market| metadata_by_market.get(market)))
        .cloned()?;
    let best_bid_price = value
        .get("best_bid")
        .or_else(|| value.get("bestBid"))
        .and_then(as_f64_value)
        .or_else(|| first_book_level(value.get("bids"), 0));
    let best_bid_size = value
        .get("best_bid_size")
        .or_else(|| value.get("bestBidSize"))
        .and_then(as_f64_value)
        .or_else(|| first_book_level(value.get("bids"), 1));
    let best_ask_price = value
        .get("best_ask")
        .or_else(|| value.get("bestAsk"))
        .and_then(as_f64_value)
        .or_else(|| first_book_level(value.get("asks"), 0));
    let best_ask_size = value
        .get("best_ask_size")
        .or_else(|| value.get("bestAskSize"))
        .and_then(as_f64_value)
        .or_else(|| first_book_level(value.get("asks"), 1));

    Some(RawMarketEvent {
        market_id: metadata.market_id.clone(),
        event_id: Some(metadata.event_id.clone()),
        channel: MarketDataChannel::Market,
        status: value
            .get("status")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .or_else(|| Some(metadata.market_status.clone())),
        best_bid_price,
        best_bid_size,
        best_ask_price,
        best_ask_size,
        trade_price: value
            .get("price")
            .or_else(|| value.get("trade_price"))
            .and_then(as_f64_value),
        trade_size: value
            .get("size")
            .or_else(|| value.get("trade_size"))
            .and_then(as_f64_value),
        trade_side: value
            .get("side")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        trade_id: value
            .get("id")
            .or_else(|| value.get("trade_id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
        observed_at: None,
        condition_id: Some(metadata.condition_id.clone()),
        title: Some(metadata.title.clone()),
        category: Some(metadata.category.clone()),
        outcomes: Some(metadata.outcomes.clone()),
        token_ids: Some(metadata.token_ids.clone()),
        resolution_source: metadata.resolution_source.clone(),
        neg_risk: Some(metadata.neg_risk),
        neg_risk_augmented: Some(metadata.neg_risk_augmented),
        tick_size: Some(metadata.tick_size),
        rules_text: Some(metadata.raw_rules_text.clone()),
    })
}

fn first_book_level(value: Option<&Value>, index: usize) -> Option<f64> {
    let first = value?.as_array()?.first()?;
    match first {
        Value::Array(items) => items.get(index).and_then(as_f64_value),
        Value::Object(map) => {
            if index == 0 {
                map.get("price").and_then(as_f64_value)
            } else {
                map.get("size").and_then(as_f64_value)
            }
        }
        _ => None,
    }
}

#[derive(Debug, Clone)]
struct RulesCursor {
    after_sequence: Option<i64>,
}

#[derive(Debug, Clone)]
struct OpportunityCursor {
    after_sequence: Option<i64>,
}

#[derive(Debug, Clone)]
struct PortfolioCursor {
    source_sequence: Option<i64>,
}

async fn run_rules_engine(context: ServiceContext, cancellation: CancellationToken) -> Result<()> {
    let config = RulesEngineConfig::from_env()?;
    let engine = RulesEngineService::new(context.store.clone(), config.max_clarification_history);
    let mut heartbeat = interval(context.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut poller = interval(config.batch_window);
    poller.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut cursor = restore_rules_cursor(&context.store, context.domain)?;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                context.audit.record(Some(context.domain), ServiceKind::RulesEngine.as_str(), "service_stopped", "cancellation received")?;
                return Ok(());
            }
            _ = heartbeat.tick() => {
                publish_heartbeat(&context, ServiceKind::RulesEngine, "rules_engine_running");
            }
            _ = poller.tick() => {
                process_rules_batch(&context, &engine, &config, &mut cursor).await?;
            }
        }
    }
}

async fn run_opportunity_engine(
    context: ServiceContext,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = OpportunityEngineConfig::from_env()?;
    let engine = OpportunityEngineService;
    let mut heartbeat = interval(context.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut poller = interval(config.batch_window);
    poller.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut cursor = restore_opportunity_cursor(&context.store, context.domain)?;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                context.audit.record(
                    Some(context.domain),
                    ServiceKind::OpportunityEngine.as_str(),
                    "service_stopped",
                    "cancellation received",
                )?;
                return Ok(());
            }
            _ = heartbeat.tick() => {
                publish_heartbeat(&context, ServiceKind::OpportunityEngine, "opportunity_engine_running");
            }
            _ = poller.tick() => {
                process_opportunity_batch(&context, &engine, &config, &mut cursor).await?;
            }
        }
    }
}

async fn process_opportunity_batch(
    context: &ServiceContext,
    engine: &OpportunityEngineService,
    config: &OpportunityEngineConfig,
    cursor: &mut OpportunityCursor,
) -> Result<()> {
    let events = context.store.replay_opportunity_input_events(
        context.domain,
        cursor.after_sequence,
        config.scan_batch_size,
    )?;
    if events.is_empty() {
        return Ok(());
    }

    let mut event_ids = std::collections::BTreeSet::new();
    let mut stale_markets = Vec::new();

    for event in &events {
        match event.aggregate_type.as_str() {
            "rules_constraint_graph" => {
                if let Ok(snapshot) =
                    serde_json::from_value::<ConstraintGraphSnapshot>(event.payload.clone())
                {
                    if let GraphScope::Event { event_id } = snapshot.scope {
                        event_ids.insert(event_id);
                    }
                }
            }
            "market_snapshot" => {
                if let Some(market_id) = event
                    .payload
                    .get("market_id")
                    .and_then(|value| value.as_str())
                {
                    if let Some(snapshot) = context
                        .store
                        .get_market_snapshot(context.domain, market_id)?
                    {
                        let age_ms =
                            (now() - snapshot.observed_at).num_milliseconds().max(0) as u64;
                        if age_ms > config.book_stale_stop_ms {
                            stale_markets.push(market_id.to_owned());
                        }
                    }
                    if let Some(market) = context.store.get_rules_market(market_id)? {
                        event_ids.insert(market.event_id);
                    }
                }
            }
            "rules_invalidation" => {
                if let Ok(invalidation) =
                    serde_json::from_value::<RulesInvalidation>(event.payload.clone())
                {
                    event_ids.insert(invalidation.event_id);
                    let invalidations = context.store.invalidate_opportunities_for_markets(
                        &invalidation.market_ids,
                        invalidation.reason,
                    )?;
                    for item in invalidations {
                        publish_opportunity_invalidation(context, &item)?;
                    }
                }
            }
            _ => {}
        }
        cursor.after_sequence = Some(event.sequence);
    }

    if !stale_markets.is_empty() {
        let invalidations = context
            .store
            .invalidate_opportunities_for_markets(&stale_markets, "market_data_stale")?;
        for item in invalidations {
            publish_opportunity_invalidation(context, &item)?;
        }
    }

    let settings = build_scan_settings(config)?;
    for event_id in event_ids {
        let Some(graph) = context
            .store
            .get_constraint_graph_snapshot(&GraphScope::Event {
                event_id: event_id.clone(),
            })?
        else {
            continue;
        };
        let market_ids = graph
            .graph
            .markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<_>>();
        let books = context
            .store
            .list_market_snapshots(context.domain, &market_ids)?;
        let output = engine
            .scan(ScanContext {
                graph,
                books: OrderbookSnapshotSet::new(books),
                health: OpportunityRuntimeHealth {
                    domain: context.domain,
                    runtime_mode: context.domain_config.runtime_mode.as_str().to_owned(),
                    now: now(),
                },
                settings: settings.clone(),
            })
            .await?;

        for candidate in output
            .candidates
            .into_iter()
            .filter(|candidate| candidate.event_id == event_id)
            .take(config.max_candidates_per_event)
        {
            publish_opportunity_candidate(context, &candidate)?;
        }
        for report in output.reports {
            context.store.record_scanner_run(report.clone())?;
            context.bus.publish(
                ServiceKind::OpportunityEngine,
                context.domain,
                "opportunity.scan.completed",
                serde_json::to_string(&report)?,
            );
        }
    }

    persist_opportunity_cursor(context, cursor.after_sequence)?;
    Ok(())
}

#[derive(Debug, Clone)]
struct PortfolioLoopConfig {
    evaluation_interval: Duration,
    bootstrap_nav: f64,
    max_candidates: usize,
}

impl PortfolioLoopConfig {
    fn from_env() -> Self {
        let evaluation_interval = env::var("POLYMARKET_PORTFOLIO_EVALUATION_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_millis(1_000));
        let bootstrap_nav = env::var("POLYMARKET_PORTFOLIO_BOOTSTRAP_NAV")
            .ok()
            .and_then(|raw| raw.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(10_000.0);
        let max_candidates = env::var("POLYMARKET_PORTFOLIO_MAX_CANDIDATES")
            .ok()
            .and_then(|raw| raw.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(128);
        Self {
            evaluation_interval,
            bootstrap_nav,
            max_candidates,
        }
    }
}

async fn run_portfolio_engine(
    context: ServiceContext,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = PortfolioLoopConfig::from_env();
    let engine = PortfolioEngineService::default();
    let mut heartbeat = interval(context.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut evaluator = interval(config.evaluation_interval);
    evaluator.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut cursor = restore_portfolio_cursor(&context.store, context.domain)?;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                context.audit.record(
                    Some(context.domain),
                    ServiceKind::PortfolioEngine.as_str(),
                    "service_stopped",
                    "cancellation received",
                )?;
                return Ok(());
            }
            _ = heartbeat.tick() => {
                publish_heartbeat(&context, ServiceKind::PortfolioEngine, "portfolio_engine_running");
            }
            _ = evaluator.tick() => {
                process_portfolio_cycle(&context, &engine, &config, &mut cursor).await?;
            }
        }
    }
}

async fn process_portfolio_cycle(
    context: &ServiceContext,
    engine: &PortfolioEngineService,
    config: &PortfolioLoopConfig,
    cursor: &mut PortfolioCursor,
) -> Result<()> {
    let opportunities = context
        .store
        .list_opportunities(None, None, None, None, config.max_candidates)?
        .into_iter()
        .filter(|candidate| candidate.invalidated_at.is_none())
        .collect::<Vec<_>>();
    let source_sequence = opportunities
        .iter()
        .map(|candidate| candidate.created_at.timestamp_millis())
        .max();
    if source_sequence.is_none() || source_sequence == cursor.source_sequence {
        return Ok(());
    }

    let runtime = load_runtime_health_snapshot(context)?;
    let portfolio = derive_portfolio_snapshot(context, config.bootstrap_nav)?;
    let budgets = derive_risk_budget_snapshot(&portfolio);
    let candidates = build_portfolio_inputs(context, opportunities)?;
    if candidates.is_empty() {
        persist_json_snapshot(context, "portfolio_snapshot", "current", &portfolio)?;
        persist_json_snapshot(context, "risk_budget", "current", &budgets)?;
        cursor.source_sequence = source_sequence;
        persist_portfolio_cursor(context, cursor.source_sequence)?;
        return Ok(());
    }

    let result = engine
        .build_plan(candidates, portfolio.clone(), runtime, budgets.clone())
        .await
        .map_err(|error| anyhow!(error.to_string()))?;

    persist_json_snapshot(context, "portfolio_snapshot", "current", &portfolio)?;
    persist_json_snapshot(context, "risk_budget", "current", &budgets)?;
    persist_json_snapshot(
        context,
        "trade_intent_batch",
        "pending",
        &result.trade_intent_batch,
    )?;
    persist_json_snapshot(
        context,
        "portfolio_audit_trail",
        "latest",
        &result.audit_trail,
    )?;
    context.audit.record(
        Some(context.domain),
        ServiceKind::PortfolioEngine.as_str(),
        "batch_built",
        &format!(
            "opportunities={} intents={} status={:?}",
            result.audit_trail.decisions.len(),
            result.trade_intent_batch.intents.len(),
            result.trade_intent_batch.optimization_status,
        ),
    )?;
    cursor.source_sequence = source_sequence;
    persist_portfolio_cursor(context, cursor.source_sequence)?;
    Ok(())
}

fn load_runtime_health_snapshot(context: &ServiceContext) -> Result<CoreRuntimeHealth> {
    Ok(match context
        .store
        .latest_snapshot(context.domain, "runtime_health", "latest")?
    {
        Some(snapshot) => snapshot_payload(&snapshot, "runtime health snapshot")?,
        None => CoreRuntimeHealth {
            domain: context.domain,
            runtime_mode: context.domain_config.runtime_mode.as_str().to_owned(),
            now: now(),
            market_ws_lag_ms: 0,
            user_ws_ok: true,
            heartbeat_age_ms: 0,
            recent_425_count: 0,
            reject_rate_5m: 0.0,
            reconcile_drift: false,
            capital_buffer_ok: true,
            fill_rate_5m: 1.0,
            open_orders: 0,
            reconcile_lag_ms: 0,
            disputed_capital_ratio: 0.0,
            degradation_reason: None,
            last_alert_at: None,
            stable_since: None,
            shadow_live_drift_bps: None,
        },
    })
}

fn derive_portfolio_snapshot(
    context: &ServiceContext,
    bootstrap_nav: f64,
) -> Result<PortfolioSnapshot> {
    let intents = context.store.list_all_execution_intents(context.domain)?;
    let orders = context.store.list_all_order_lifecycle(context.domain)?;
    let mut intents_by_client = HashMap::new();
    for intent in intents {
        intents_by_client.insert(intent.client_order_id.clone(), intent);
    }

    #[derive(Default)]
    struct AccumulatedPosition {
        market_id: String,
        token_id: String,
        event_id: String,
        category: String,
        quantity: f64,
        notional: f64,
        reserved_notional: f64,
        mark_price: f64,
    }

    let mut positions_by_key: HashMap<(String, String), AccumulatedPosition> = HashMap::new();
    let mut reserved_cash = 0.0_f64;
    for order in &orders {
        let intent = order
            .client_order_id
            .as_ref()
            .and_then(|client_order_id| intents_by_client.get(client_order_id));
        let token_id = intent
            .map(|item| item.token_id.clone())
            .unwrap_or_else(|| "unknown-token".to_owned());
        let rules_market = context.store.get_rules_market(&order.market_id)?;
        let market_snapshot = context.store.get_market_snapshot(context.domain, &order.market_id)?;
        let event_id = intent
            .and_then(|item| item.batch_id.map(|batch_id| batch_id.to_string()))
            .or_else(|| rules_market.as_ref().map(|item| item.event_id.clone()))
            .unwrap_or_else(|| "unknown-event".to_owned());
        let category = rules_market
            .as_ref()
            .map(|item| item.category.clone())
            .unwrap_or_else(|| "uncategorized".to_owned());
        let mark_price = market_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.book.as_ref().and_then(|book| book.mid_price))
            .or(order.average_fill_price)
            .or(order.limit_price)
            .unwrap_or(0.0);

        if !order.status.is_terminal() {
            reserved_cash += order
                .limit_price
                .unwrap_or(0.0)
                * order.order_quantity.unwrap_or(0.0);
        }
        if order.filled_quantity <= 0.0 {
            continue;
        }

        let signed_quantity = match order.side.as_deref().unwrap_or("BUY") {
            "SELL" => -order.filled_quantity,
            _ => order.filled_quantity,
        };
        let entry = positions_by_key
            .entry((order.market_id.clone(), token_id.clone()))
            .or_insert_with(|| AccumulatedPosition {
                market_id: order.market_id.clone(),
                token_id,
                event_id,
                category,
                quantity: 0.0,
                notional: 0.0,
                reserved_notional: 0.0,
                mark_price,
            });
        entry.quantity += signed_quantity;
        entry.notional += order
            .average_fill_price
            .unwrap_or(order.limit_price.unwrap_or(0.0))
            * order.filled_quantity;
        entry.reserved_notional += order
            .limit_price
            .unwrap_or(0.0)
            * (order.order_quantity.unwrap_or(0.0) - order.filled_quantity).max(0.0);
        if mark_price > 0.0 {
            entry.mark_price = mark_price;
        }
    }

    let positions = positions_by_key
        .into_values()
        .map(|position| {
            let average_price = if position.quantity.abs() > f64::EPSILON {
                position.notional / position.quantity.abs()
            } else {
                0.0
            };
            let unrealized_pnl = (position.mark_price - average_price) * position.quantity;
            polymarket_core::PositionSnapshot {
                market_id: position.market_id,
                token_id: position.token_id,
                event_id: position.event_id,
                category: position.category,
                quantity: position.quantity,
                average_price,
                mark_price: position.mark_price,
                reserved_notional: position.reserved_notional,
                unrealized_pnl,
            }
        })
        .collect::<Vec<_>>();
    let invested_notional = positions
        .iter()
        .map(|position| position.quantity.abs() * position.average_price.abs())
        .sum::<f64>();
    let unresolved_capital = positions
        .iter()
        .map(|position| position.quantity.abs() * position.mark_price.abs())
        .sum::<f64>();
    let nav = bootstrap_nav
        .max(invested_notional + reserved_cash + unresolved_capital)
        .max(1.0);
    let cash_available = (nav - invested_notional - reserved_cash).max(0.0);

    Ok(PortfolioSnapshot {
        nav,
        cash_available,
        reserved_cash,
        positions,
        unresolved_capital,
        redeemable_capital: 0.0,
    })
}

fn derive_risk_budget_snapshot(portfolio: &PortfolioSnapshot) -> RiskBudgetSnapshot {
    let nav = portfolio.nav.max(1.0);
    RiskBudgetSnapshot {
        max_structural_arb_notional: nav * 0.40,
        max_neg_risk_notional: nav * 0.20,
        max_rules_driven_notional: nav * 0.25,
        max_fee_mm_notional: nav * 0.15,
        min_cash_buffer: nav * 0.025,
        max_single_intent_notional: nav * 0.15,
        max_event_notional: nav * 0.025,
        max_category_notional: nav * 0.15,
        max_market_notional: nav * 0.035,
        max_unresolved_notional: nav * 0.50,
    }
}

fn build_portfolio_inputs(
    context: &ServiceContext,
    opportunities: Vec<polymarket_core::OpportunityCandidate>,
) -> Result<Vec<PortfolioOpportunity>> {
    let mut results = Vec::new();
    for opportunity in opportunities {
        let Some(market_id) = opportunity.market_refs.first().cloned() else {
            continue;
        };
        let Some(rules_market) = context.store.get_rules_market(&market_id)? else {
            continue;
        };
        let Some(snapshot) = context.store.get_market_snapshot(context.domain, &market_id)? else {
            continue;
        };
        let Some(book) = snapshot.book.as_ref() else {
            continue;
        };
        let token_id = rules_market
            .token_ids
            .first()
            .cloned()
            .unwrap_or_else(|| market_id.clone());
        let limit_price = book
            .best_bid
            .as_ref()
            .map(|level| level.price)
            .or_else(|| book.best_ask.as_ref().map(|level| level.price))
            .or(book.mid_price)
            .unwrap_or(0.0);
        let max_size = book
            .best_bid
            .as_ref()
            .map(|level| level.size)
            .or_else(|| book.best_ask.as_ref().map(|level| level.size))
            .unwrap_or(0.0);
        if limit_price <= 0.0 || max_size <= 0.0 {
            continue;
        }

        results.push(PortfolioOpportunity {
            opportunity,
            account_domain: context.domain,
            market_id,
            token_id,
            side: TradeSide::Buy,
            limit_price,
            max_size,
            category: rules_market.category,
            resolution_source: rules_market.resolution_source,
            fees_enabled: rules_market.fees_enabled,
            neg_risk: rules_market.neg_risk,
            orderbook_depth_score: estimate_orderbook_depth_score(book),
            book_age_ms: (now() - snapshot.observed_at).num_milliseconds().max(0) as u64,
        });
    }
    Ok(results)
}

fn estimate_orderbook_depth_score(book: &MarketBook) -> f64 {
    let bid = book.best_bid.as_ref().map(|level| level.size).unwrap_or(0.0);
    let ask = book.best_ask.as_ref().map(|level| level.size).unwrap_or(0.0);
    ((bid + ask) / 50.0).clamp(0.25, 1.25)
}

async fn process_rules_batch(
    context: &ServiceContext,
    engine: &RulesEngineService,
    config: &RulesEngineConfig,
    cursor: &mut RulesCursor,
) -> Result<()> {
    let events = context.store.replay_rules_market_events(
        context.domain,
        cursor.after_sequence,
        config.replay_batch_size,
    )?;
    if events.is_empty() {
        return Ok(());
    }

    for event in events {
        if let Some(raw) = raw_document_from_event(&event, config.max_rules_text_len)? {
            let previous = context.store.get_rules_market(&raw.market_id)?;
            let canonical = engine.upsert_market(raw).await?;
            publish_rules_market(context, &canonical)?;

            let family = engine.rebuild_event_family(&canonical.event_id).await?;
            publish_event_family(context, &family)?;

            let graph = engine
                .get_constraint_graph(GraphScope::Event {
                    event_id: canonical.event_id.clone(),
                })
                .await?;
            publish_constraint_graph(context, &graph)?;

            if previous
                .as_ref()
                .map(|item| item.rules_version != canonical.rules_version)
                .unwrap_or(false)
            {
                let invalidation = RulesInvalidation {
                    event_id: canonical.event_id.clone(),
                    market_ids: vec![canonical.market_id.clone()],
                    rules_versions: vec![canonical.rules_version.clone()],
                    reason: "rules_version_changed".to_owned(),
                    invalidated_at: now(),
                };
                publish_invalidation(context, &invalidation)?;
            }
        }
        cursor.after_sequence = Some(event.sequence);
    }

    persist_rules_cursor(context, cursor.after_sequence)?;
    let report = engine
        .replay(ReplayRequest {
            domain: context.domain,
            after_sequence: cursor.after_sequence,
            limit: config.replay_batch_size,
            reason: Some("rules-engine periodic replay".to_owned()),
            alert_id: None,
            audit_event_id: None,
        })
        .await?;
    context.bus.publish(
        ServiceKind::RulesEngine,
        context.domain,
        "rules.replay.completed",
        serde_json::to_string(&report)?,
    );
    Ok(())
}

async fn run_risk_engine(context: ServiceContext, cancellation: CancellationToken) -> Result<()> {
    let config = RiskEngineConfig::from_env()?;
    let engine = RiskEngineService::new(config.clone());
    let mut heartbeat = interval(context.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut evaluator = interval(config.evaluation_interval);
    evaluator.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut last_batch_version = -1_i64;

    loop {
        tokio::select! {
            _ = cancellation.cancelled() => {
                context.audit.record(
                    Some(context.domain),
                    ServiceKind::RiskEngine.as_str(),
                    "service_stopped",
                    "cancellation received",
                )?;
                return Ok(());
            }
            _ = heartbeat.tick() => {
                publish_heartbeat(&context, ServiceKind::RiskEngine, "risk_engine_running");
            }
            _ = evaluator.tick() => {
                let Some(risk_context) = load_risk_context(&context)? else {
                    warn!(domain = %context.domain, "risk engine skipped evaluation because required snapshots are missing");
                    continue;
                };

                let runtime = engine.evaluate_runtime(&risk_context).await?;
                persist_json_snapshot(&context, "risk_runtime_decision", "current", &runtime)?;
                context.audit.record(
                    Some(context.domain),
                    ServiceKind::RiskEngine.as_str(),
                    "runtime_evaluated",
                    &format!("mode={}", runtime.mode.as_str()),
                )?;

                if let Some(snapshot) = context
                    .store
                    .latest_snapshot(context.domain, "trade_intent_batch", "pending")
                    .context("failed to load pending trade intent batch snapshot")?
                {
                    if snapshot.version > last_batch_version {
                        let batch: TradeIntentBatch =
                            snapshot_payload(&snapshot, "trade intent batch")?;
                        let evaluation = engine.evaluate_batch(batch, risk_context.clone()).await?;
                        persist_json_snapshot(&context, "risk_evaluation", "latest", &evaluation)?;

                        match &evaluation {
                            RiskEvaluation::Approved(approved) => {
                                persist_json_snapshot(
                                    &context,
                                    "approved_trade_intent_batch",
                                    "latest",
                                    approved,
                                )?;
                                context.bus.publish(
                                    ServiceKind::RiskEngine,
                                    context.domain,
                                    "trade.intent.approved",
                                    serde_json::to_string(&approved.batch)?,
                                );
                                context.audit.record(
                                    Some(context.domain),
                                    ServiceKind::RiskEngine.as_str(),
                                    "batch_approved",
                                    &format!(
                                        "intents={} scale={:.4} mode={}",
                                        approved.batch.intents.len(),
                                        approved.scale,
                                        approved.runtime_mode.as_str()
                                    ),
                                )?;
                            }
                            RiskEvaluation::Rejected(rejection) => {
                                persist_json_snapshot(
                                    &context,
                                    "risk_rejection",
                                    "latest",
                                    rejection,
                                )?;
                                context.audit.record(
                                    Some(context.domain),
                                    ServiceKind::RiskEngine.as_str(),
                                    "batch_rejected",
                                    &format!(
                                        "findings={} mode={}",
                                        rejection.findings.len(),
                                        rejection.runtime_mode.as_str()
                                    ),
                                )?;
                            }
                        }

                        last_batch_version = snapshot.version;
                    }
                }
            }
        }
    }
}

fn load_risk_context(context: &ServiceContext) -> Result<Option<RiskContext>> {
    let Some(portfolio_snapshot) = context
        .store
        .latest_snapshot(context.domain, "portfolio_snapshot", "current")
        .context("failed to load portfolio snapshot")?
    else {
        return Ok(None);
    };
    let Some(budget_snapshot) = context
        .store
        .latest_snapshot(context.domain, "risk_budget", "current")
        .context("failed to load risk budget snapshot")?
    else {
        return Ok(None);
    };

    let portfolio: PortfolioSnapshot = snapshot_payload(&portfolio_snapshot, "portfolio snapshot")?;
    let budgets: RiskBudgetSnapshot = snapshot_payload(&budget_snapshot, "risk budget snapshot")?;
    let runtime = match context
        .store
        .latest_snapshot(context.domain, "risk_runtime_health", "current")
        .context("failed to load runtime health snapshot")?
    {
        Some(snapshot) => snapshot_payload(&snapshot, "runtime health snapshot")?,
        None => CoreRuntimeHealth {
            domain: context.domain,
            runtime_mode: context.domain_config.runtime_mode.as_str().to_owned(),
            now: now(),
            market_ws_lag_ms: 0,
            user_ws_ok: true,
            heartbeat_age_ms: 0,
            recent_425_count: 0,
            reject_rate_5m: 0.0,
            reconcile_drift: false,
            capital_buffer_ok: true,
            fill_rate_5m: 1.0,
            open_orders: 0,
            reconcile_lag_ms: 0,
            disputed_capital_ratio: 0.0,
            degradation_reason: None,
            last_alert_at: None,
            stable_since: None,
            shadow_live_drift_bps: None,
        },
    };
    let market_states = match context
        .store
        .latest_snapshot(context.domain, "risk_market_state_catalog", "current")
        .context("failed to load risk market catalog snapshot")?
    {
        Some(snapshot) => snapshot_payload(&snapshot, "risk market catalog snapshot")?,
        None => BTreeMap::new(),
    };
    let strategy_states = match context
        .store
        .latest_snapshot(context.domain, "risk_strategy_state_catalog", "current")
        .context("failed to load strategy risk catalog snapshot")?
    {
        Some(snapshot) => snapshot_payload(&snapshot, "strategy risk catalog snapshot")?,
        None => BTreeMap::new(),
    };
    let disputed_capital = match context
        .store
        .latest_snapshot(context.domain, "risk_disputed_capital", "current")
        .context("failed to load disputed capital snapshot")?
    {
        Some(snapshot) => snapshot_payload(&snapshot, "disputed capital snapshot")?,
        None => 0.0_f64,
    };
    let runtime_mode_override = match context
        .store
        .latest_snapshot(context.domain, "risk_runtime_mode_override", "current")
        .context("failed to load runtime mode override snapshot")?
    {
        Some(snapshot) => Some(snapshot_payload(
            &snapshot,
            "runtime mode override snapshot",
        )?),
        None => None,
    };

    Ok(Some(RiskContext {
        portfolio,
        budgets,
        runtime,
        market_states,
        strategy_states,
        disputed_capital,
        decision_time: now(),
        runtime_mode_override,
        rollout_stage: context
            .store
            .get_current_rollout_stage(context.domain)?
            .map(|record| record.stage),
        rollout_policy: context.store.load_effective_rollout_policy(context.domain)?,
    }))
}

fn persist_json_snapshot<T: Serialize>(
    context: &ServiceContext,
    aggregate_type: &str,
    aggregate_id: &str,
    payload: &T,
) -> Result<()> {
    let next_version = context
        .store
        .latest_snapshot(context.domain, aggregate_type, aggregate_id)?
        .map(|snapshot| snapshot.version + 1)
        .unwrap_or(0);
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: aggregate_type.to_owned(),
        aggregate_id: aggregate_id.to_owned(),
        version: next_version,
        payload: serde_json::to_value(payload)?,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    Ok(())
}

fn snapshot_payload<T: serde::de::DeserializeOwned>(
    snapshot: &StateSnapshot,
    label: &str,
) -> Result<T> {
    serde_json::from_value(snapshot.payload.clone())
        .with_context(|| format!("failed to decode {label}"))
}

fn restore_rules_cursor(store: &Store, domain: AccountDomain) -> Result<RulesCursor> {
    let after_sequence = store
        .latest_snapshot(domain, "rules_engine_cursor", domain.as_str())?
        .and_then(|snapshot| {
            snapshot
                .payload
                .get("after_sequence")
                .and_then(|value| value.as_i64())
        });
    Ok(RulesCursor { after_sequence })
}

fn persist_rules_cursor(context: &ServiceContext, after_sequence: Option<i64>) -> Result<()> {
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "rules_engine_cursor".to_owned(),
        aggregate_id: context.domain.as_str().to_owned(),
        version: after_sequence.unwrap_or_default(),
        payload: json!({ "after_sequence": after_sequence }),
        derived_from_sequence: after_sequence,
        created_at: now(),
    })?;
    Ok(())
}

fn restore_opportunity_cursor(store: &Store, domain: AccountDomain) -> Result<OpportunityCursor> {
    let after_sequence = store
        .latest_snapshot(domain, "opportunity_engine_cursor", domain.as_str())?
        .and_then(|snapshot| {
            snapshot
                .payload
                .get("after_sequence")
                .and_then(|value| value.as_i64())
        });
    Ok(OpportunityCursor { after_sequence })
}

fn persist_opportunity_cursor(context: &ServiceContext, after_sequence: Option<i64>) -> Result<()> {
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "opportunity_engine_cursor".to_owned(),
        aggregate_id: context.domain.as_str().to_owned(),
        version: after_sequence.unwrap_or_default(),
        payload: json!({ "after_sequence": after_sequence }),
        derived_from_sequence: after_sequence,
        created_at: now(),
    })?;
    Ok(())
}

fn restore_portfolio_cursor(store: &Store, domain: AccountDomain) -> Result<PortfolioCursor> {
    let source_sequence = store
        .latest_snapshot(domain, "portfolio_engine_cursor", domain.as_str())?
        .and_then(|snapshot| {
            snapshot
                .payload
                .get("source_sequence")
                .and_then(|value| value.as_i64())
        });
    Ok(PortfolioCursor { source_sequence })
}

fn persist_portfolio_cursor(context: &ServiceContext, source_sequence: Option<i64>) -> Result<()> {
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "portfolio_engine_cursor".to_owned(),
        aggregate_id: context.domain.as_str().to_owned(),
        version: source_sequence.unwrap_or_default(),
        payload: json!({ "source_sequence": source_sequence }),
        derived_from_sequence: source_sequence,
        created_at: now(),
    })?;
    Ok(())
}

fn build_scan_settings(config: &OpportunityEngineConfig) -> Result<ScanSettings> {
    let mut shadow_only_strategies = std::collections::BTreeSet::new();
    for strategy in &config.shadow_only_strategies {
        match strategy.parse::<StrategyKind>() {
            Ok(strategy) => {
                shadow_only_strategies.insert(strategy);
            }
            Err(_) => {
                warn!(strategy = %strategy, "ignoring invalid shadow-only strategy");
            }
        }
    }
    Ok(ScanSettings {
        min_edge_net_bps: config.min_edge_net_bps,
        max_market_refs_per_candidate: config.max_market_refs_per_candidate,
        max_candidates_per_event: config.max_candidates_per_event,
        max_candidates_total: config.max_candidates_total,
        fee_mm_enabled: config.fee_mm_enabled,
        shadow_only_strategies,
        default_half_life_secs: config.default_half_life_secs,
        book_stale_warn_ms: config.book_stale_warn_ms,
        book_stale_stop_ms: config.book_stale_stop_ms,
        max_book_age_for_multileg_ms: config.max_book_age_for_multileg_ms,
    })
}

fn publish_opportunity_candidate(
    context: &ServiceContext,
    candidate: &polymarket_core::OpportunityCandidate,
) -> Result<()> {
    let payload = serde_json::to_value(candidate)?;
    context
        .store
        .upsert_opportunity_candidate(candidate.clone())?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("opportunity:{}", candidate.opportunity_id),
        aggregate_type: "opportunity_candidate".to_owned(),
        aggregate_id: candidate.opportunity_id.to_string(),
        event_type: "opportunity.candidate.upserted".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({
            "strategy": candidate.strategy,
            "event_id": candidate.event_id,
            "edge_net_bps": candidate.edge_net_bps,
        }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "opportunity_candidate".to_owned(),
        aggregate_id: candidate.opportunity_id.to_string(),
        version: candidate.created_at.timestamp(),
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::OpportunityEngine,
        context.domain,
        "opportunity.candidate.upserted",
        serde_json::to_string(candidate)?,
    );
    Ok(())
}

fn publish_opportunity_invalidation(
    context: &ServiceContext,
    invalidation: &polymarket_core::OpportunityInvalidation,
) -> Result<()> {
    let payload = serde_json::to_value(invalidation)?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("opportunity-invalidation:{}", invalidation.opportunity_id),
        aggregate_type: "opportunity_invalidation".to_owned(),
        aggregate_id: invalidation.opportunity_id.to_string(),
        event_type: "opportunity.candidate.invalidated".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({ "reason": invalidation.reason }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "opportunity_invalidation".to_owned(),
        aggregate_id: invalidation.opportunity_id.to_string(),
        version: invalidation.invalidated_at.timestamp(),
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::OpportunityEngine,
        context.domain,
        "opportunity.candidate.invalidated",
        serde_json::to_string(invalidation)?,
    );
    Ok(())
}

fn publish_rules_market(
    context: &ServiceContext,
    canonical: &polymarket_core::MarketCanonical,
) -> Result<()> {
    let payload = serde_json::to_value(canonical)?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("rules-market:{}", canonical.market_id),
        aggregate_type: "rules_market".to_owned(),
        aggregate_id: canonical.market_id.clone(),
        event_type: "rules.market_canonical.updated".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({ "rules_version": canonical.rules_version }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "rules_market".to_owned(),
        aggregate_id: canonical.market_id.clone(),
        version: now().timestamp(),
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::RulesEngine,
        context.domain,
        "rules.market_canonical.updated",
        serde_json::to_string(canonical)?,
    );
    Ok(())
}

fn publish_event_family(context: &ServiceContext, snapshot: &EventFamilySnapshot) -> Result<()> {
    let payload = serde_json::to_value(snapshot)?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("rules-event:{}", snapshot.event_id),
        aggregate_type: "rules_event_family".to_owned(),
        aggregate_id: snapshot.event_id.clone(),
        event_type: "rules.event_family.updated".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({ "version": snapshot.version }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "rules_event_family".to_owned(),
        aggregate_id: snapshot.event_id.clone(),
        version: snapshot.version,
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::RulesEngine,
        context.domain,
        "rules.event_family.updated",
        serde_json::to_string(snapshot)?,
    );
    Ok(())
}

fn publish_constraint_graph(
    context: &ServiceContext,
    snapshot: &ConstraintGraphSnapshot,
) -> Result<()> {
    let aggregate_id = match &snapshot.scope {
        GraphScope::All => "ALL".to_owned(),
        GraphScope::Event { event_id } => event_id.clone(),
        GraphScope::Market { market_id } => market_id.clone(),
    };
    let payload = serde_json::to_value(snapshot)?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("rules-graph:{aggregate_id}"),
        aggregate_type: "rules_constraint_graph".to_owned(),
        aggregate_id: aggregate_id.clone(),
        event_type: "rules.constraint_graph.updated".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({ "version": snapshot.version }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "rules_constraint_graph".to_owned(),
        aggregate_id,
        version: snapshot.version,
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::RulesEngine,
        context.domain,
        "rules.constraint_graph.updated",
        serde_json::to_string(snapshot)?,
    );
    Ok(())
}

fn publish_invalidation(context: &ServiceContext, invalidation: &RulesInvalidation) -> Result<()> {
    let payload = serde_json::to_value(invalidation)?;
    context.store.append_event(NewDurableEvent {
        domain: context.domain,
        stream: format!("rules-invalidation:{}", invalidation.event_id),
        aggregate_type: "rules_invalidation".to_owned(),
        aggregate_id: invalidation.event_id.clone(),
        event_type: "rules.invalidation.emitted".to_owned(),
        causation_id: None,
        correlation_id: None,
        idempotency_key: None,
        payload: payload.clone(),
        metadata: json!({ "reason": invalidation.reason }),
        created_at: now(),
    })?;
    context.store.upsert_snapshot(NewStateSnapshot {
        domain: context.domain,
        aggregate_type: "rules_invalidation".to_owned(),
        aggregate_id: invalidation.event_id.clone(),
        version: invalidation.invalidated_at.timestamp(),
        payload,
        derived_from_sequence: None,
        created_at: now(),
    })?;
    context.bus.publish(
        ServiceKind::RulesEngine,
        context.domain,
        "rules.invalidation.emitted",
        serde_json::to_string(invalidation)?,
    );
    Ok(())
}

fn raw_document_from_event(
    event: &DurableEvent,
    max_rules_text_len: usize,
) -> Result<Option<RawMarketDocument>> {
    if event.aggregate_type != "market_snapshot" {
        return Ok(None);
    }

    let market_id = event.aggregate_id.clone();
    let event_id = event
        .payload
        .get("event_id")
        .and_then(|value| value.as_str())
        .unwrap_or("unknown-event")
        .to_owned();
    let rules_text = event
        .payload
        .get("rules_text")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .chars()
        .take(max_rules_text_len)
        .collect::<String>();
    let title = event
        .payload
        .get("title")
        .and_then(|value| value.as_str())
        .unwrap_or(&market_id)
        .to_owned();
    let outcomes = event
        .payload
        .get("outcomes")
        .and_then(|value| value.as_array())
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToOwned::to_owned))
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| vec!["YES".to_owned(), "NO".to_owned()]);
    let observed_at = event.created_at;

    Ok(Some(RawMarketDocument {
        market_id,
        event_id,
        condition_id: event
            .payload
            .get("condition_id")
            .and_then(|value| value.as_str())
            .unwrap_or("")
            .to_owned(),
        token_ids: Vec::new(),
        title,
        category: event
            .payload
            .get("category")
            .and_then(|value| value.as_str())
            .unwrap_or("uncategorized")
            .to_owned(),
        outcomes,
        end_time: observed_at,
        resolution_source: event
            .payload
            .get("resolution_source")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned),
        edge_cases: Vec::new(),
        clarifications: Vec::new(),
        fees_enabled: true,
        neg_risk: event
            .payload
            .get("neg_risk")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        neg_risk_augmented: event
            .payload
            .get("neg_risk_augmented")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        tick_size: event
            .payload
            .get("tick_size")
            .and_then(|value| value.as_f64())
            .unwrap_or(0.01),
        raw_rules_text: rules_text,
        market_status: event
            .payload
            .get("status")
            .and_then(|value| value.as_str())
            .unwrap_or("open")
            .to_owned(),
        observed_at,
    }))
}

fn quote_level(price: Option<f64>, size: Option<f64>) -> Result<Option<MarketQuoteLevel>> {
    match (price, size) {
        (Some(price), Some(size)) => {
            if price < 0.0 || size < 0.0 {
                return Err(anyhow!("price and size must be non-negative"));
            }
            Ok(Some(MarketQuoteLevel { price, size }))
        }
        (None, None) => Ok(None),
        _ => Err(anyhow!("price and size must be provided together")),
    }
}

fn log_domain_summary(config: &DomainConfig) {
    let summary = config.summary();
    info!(
        domain = %summary.domain,
        environment = %summary.environment,
        data_dir = %summary.data_dir.display(),
        database_path = %summary.database_path.display(),
        default_runtime_mode = %summary.default_runtime_mode,
        runtime_mode = %summary.runtime_mode,
        allowed_runtime_modes = ?summary.allowed_runtime_modes,
        execution_approved = summary.execution_approved,
        l2_credentials = %summary.l2_credentials_kind,
        signer_credentials = %summary.signer_kind,
        namespace = %summary.namespace,
        audit_prefix = %summary.audit_prefix,
        "domain configuration loaded"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_book_and_trade() {
        let snapshot = normalize_market_event(
            &RawMarketEvent {
                market_id: "m1".to_owned(),
                event_id: Some("evt-1".to_owned()),
                channel: MarketDataChannel::Market,
                status: Some("open".to_owned()),
                best_bid_price: Some(0.42),
                best_bid_size: Some(100.0),
                best_ask_price: Some(0.44),
                best_ask_size: Some(80.0),
                trade_price: Some(0.43),
                trade_size: Some(25.0),
                trade_side: Some("buy".to_owned()),
                trade_id: Some("t1".to_owned()),
                observed_at: None,
                condition_id: None,
                title: None,
                category: None,
                outcomes: None,
                token_ids: None,
                resolution_source: None,
                neg_risk: None,
                neg_risk_augmented: None,
                tick_size: None,
                rules_text: None,
            },
            7,
        )
        .expect("snapshot");

        assert_eq!(snapshot.sequence, 7);
        assert_eq!(snapshot.market_id, "m1");
        assert!(snapshot.book.is_some());
        let book = snapshot.book.expect("book");
        assert_eq!(book.best_bid.expect("bid").price, 0.42);
        assert_eq!(book.best_ask.expect("ask").price, 0.44);
        assert!(book.mid_price.expect("mid") > 0.0);
        assert!(book.spread_bps.expect("spread") > 0.0);
    }

    #[test]
    fn rejects_crossed_book() {
        let error = normalize_market_event(
            &RawMarketEvent {
                market_id: "m1".to_owned(),
                event_id: None,
                channel: MarketDataChannel::Market,
                status: None,
                best_bid_price: Some(0.55),
                best_bid_size: Some(10.0),
                best_ask_price: Some(0.50),
                best_ask_size: Some(9.0),
                trade_price: None,
                trade_size: None,
                trade_side: None,
                trade_id: None,
                observed_at: None,
                condition_id: None,
                title: None,
                category: None,
                outcomes: None,
                token_ids: None,
                resolution_source: None,
                neg_risk: None,
                neg_risk_augmented: None,
                tick_size: None,
                rules_text: None,
            },
            1,
        )
        .expect_err("crossed book must fail");

        assert!(error.to_string().contains("crossed book"));
    }

    #[test]
    fn merged_payload_preserves_rules_metadata() {
        let raw = RawMarketEvent {
            market_id: "m1".to_owned(),
            event_id: Some("evt-1".to_owned()),
            channel: MarketDataChannel::Market,
            status: Some("open".to_owned()),
            best_bid_price: Some(0.42),
            best_bid_size: Some(100.0),
            best_ask_price: Some(0.44),
            best_ask_size: Some(80.0),
            trade_price: None,
            trade_size: None,
            trade_side: None,
            trade_id: None,
            observed_at: None,
            condition_id: Some("cond-1".to_owned()),
            title: Some("Will it rain?".to_owned()),
            category: Some("weather".to_owned()),
            outcomes: Some(vec!["YES".to_owned(), "NO".to_owned()]),
            token_ids: Some(vec!["tok-yes".to_owned(), "tok-no".to_owned()]),
            resolution_source: Some("oracle".to_owned()),
            neg_risk: Some(true),
            neg_risk_augmented: Some(false),
            tick_size: Some(0.01),
            rules_text: Some("Official rules".to_owned()),
        };
        let snapshot = normalize_market_event(&raw, 9).expect("snapshot");
        let payload = merged_market_payload(&raw, &snapshot).expect("payload");

        assert_eq!(payload["condition_id"], "cond-1");
        assert_eq!(payload["title"], "Will it rain?");
        assert_eq!(payload["token_ids"][0], "tok-yes");
        assert_eq!(payload["rules_text"], "Official rules");
    }
}
