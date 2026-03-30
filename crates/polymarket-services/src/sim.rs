use std::collections::VecDeque;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use polymarket_config::SimConfig;
use polymarket_core::{
    now, AccountDomain, ExecutionError, ExecutionIntentRecord, NewReplayCheckpoint, NewSimEvent,
    NewSimFillRecord, NewSimOrderRecord, OrderLifecycleRecord, OrderLifecycleStatus, ReplayCursor,
    SimDriftReport, SimDriftSeverity, SimEventKind, SimEventRecord, SimFillRecord, SimMode,
    SimOrderRecord, SimRunReport,
};
use polymarket_msgbus::MessageBus;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use crate::execution::{ExecutionVenue, VenueHeartbeat, VenueOrderState, VenueSubmitAck};
use crate::ServiceContext;

pub const SIM_EVENT_STREAM: &str = "sim-engine";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillModel {
    pub probability_bps: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeeModel {
    pub fee_bps: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyModel {
    pub latency_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueModel {
    pub depth: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceCalendar {
    pub maintenance_windows: Vec<(u32, u32)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimulatedOrderRequest {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_id: String,
    pub side: String,
    pub limit_price: f64,
    pub quantity: f64,
    pub book_price: f64,
    pub available_quantity: f64,
    pub post_only: bool,
    pub fok: bool,
    pub fak: bool,
    pub expires_at_ms: Option<i64>,
    pub now_ms: i64,
    pub leg_count: usize,
    pub failing_leg_index: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatedOrderOutcome {
    pub ack: VenueSubmitAck,
    pub state: VenueOrderState,
    pub fill: Option<SimFillRecord>,
}

#[derive(Debug, Clone)]
pub struct SimExchangeAdapter {
    pub fill_model: FillModel,
    pub fee_model: FeeModel,
    pub latency_model: LatencyModel,
    pub queue_model: QueueModel,
    pub maintenance_calendar: MaintenanceCalendar,
    heartbeat_failures: Arc<std::sync::atomic::AtomicU64>,
}

impl SimExchangeAdapter {
    pub fn new(config: &SimConfig) -> Self {
        Self {
            fill_model: FillModel {
                probability_bps: config.fill_probability_bps,
            },
            fee_model: FeeModel {
                fee_bps: config.fee_bps,
            },
            latency_model: LatencyModel {
                latency_ms: config.latency_ms,
            },
            queue_model: QueueModel {
                depth: config.queue_depth,
            },
            maintenance_calendar: MaintenanceCalendar {
                maintenance_windows: vec![(425, 425)],
            },
            heartbeat_failures: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    pub fn simulate_order(
        &self,
        request: &SimulatedOrderRequest,
    ) -> Result<SimulatedOrderOutcome, ExecutionError> {
        if self.is_maintenance_window(request.now_ms) {
            return Err(ExecutionError::VenueRestart(
                "425 maintenance window".to_owned(),
            ));
        }
        if request.leg_count > 1 && request.failing_leg_index.is_some() {
            return Ok(self.rejected_outcome(
                request,
                "multi-leg partial failure detected",
                json!({ "failing_leg_index": request.failing_leg_index }),
            ));
        }
        if request.post_only && self.crosses_book(request) {
            return Ok(self.rejected_outcome(
                request,
                "post-only order would cross",
                json!({ "reason": "post_only_cross" }),
            ));
        }
        if let Some(expires_at_ms) = request.expires_at_ms {
            if request.now_ms >= expires_at_ms {
                return Ok(self.expired_outcome(request));
            }
        }

        let fillable_quantity = request
            .available_quantity
            .min(request.quantity)
            .min(self.queue_model.depth as f64)
            .max(0.0);

        if request.fok && fillable_quantity + f64::EPSILON < request.quantity {
            return Ok(self.cancelled_outcome(request, "fok_not_filled"));
        }

        let filled_quantity = if request.fak {
            fillable_quantity
        } else if fillable_quantity + f64::EPSILON >= request.quantity {
            request.quantity
        } else {
            fillable_quantity
        };

        if filled_quantity <= f64::EPSILON {
            return Ok(self.acknowledged_outcome(request));
        }

        let fees_paid =
            filled_quantity * request.limit_price * f64::from(self.fee_model.fee_bps) / 10_000.0;
        let status = if filled_quantity + f64::EPSILON >= request.quantity {
            OrderLifecycleStatus::Filled
        } else {
            OrderLifecycleStatus::PartiallyFilled
        };
        let detail = json!({
            "latency_ms": self.latency_model.latency_ms,
            "fees_paid": fees_paid,
            "queue_depth": self.queue_model.depth,
        });
        let ack = VenueSubmitAck {
            order_id: request.order_id.clone(),
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| request.order_id.clone()),
            external_order_id: Some(format!("sim-{}", request.order_id)),
            status,
            detail: detail.clone(),
        };
        let state = VenueOrderState {
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            market_id: request.market_id.clone(),
            status,
            filled_quantity,
            average_fill_price: Some(request.limit_price),
            detail: detail.clone(),
        };
        let fill = SimFillRecord {
            fill_id: Uuid::new_v4(),
            run_id: Uuid::nil(),
            mode: SimMode::PaperLiveQueue,
            order_id: request.order_id.clone(),
            market_id: request.market_id.clone(),
            quantity: filled_quantity,
            price: request.limit_price,
            fees_paid,
            filled_at: now(),
            detail,
        };
        Ok(SimulatedOrderOutcome {
            ack,
            state,
            fill: Some(fill),
        })
    }

    fn crosses_book(&self, request: &SimulatedOrderRequest) -> bool {
        match request.side.to_ascii_uppercase().as_str() {
            "BUY" => request.limit_price >= request.book_price,
            "SELL" => request.limit_price <= request.book_price,
            _ => false,
        }
    }

    fn is_maintenance_window(&self, now_ms: i64) -> bool {
        self.maintenance_calendar
            .maintenance_windows
            .iter()
            .any(|(start, end)| {
                let marker = (now_ms.rem_euclid(1_000)) as u32;
                marker >= *start && marker <= *end
            })
    }

    fn rejected_outcome(
        &self,
        request: &SimulatedOrderRequest,
        reason: &str,
        detail: Value,
    ) -> SimulatedOrderOutcome {
        let ack = VenueSubmitAck {
            order_id: request.order_id.clone(),
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| request.order_id.clone()),
            external_order_id: None,
            status: OrderLifecycleStatus::Rejected,
            detail: json!({ "reason": reason, "detail": detail }),
        };
        let state = VenueOrderState {
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            market_id: request.market_id.clone(),
            status: OrderLifecycleStatus::Rejected,
            filled_quantity: 0.0,
            average_fill_price: None,
            detail: ack.detail.clone(),
        };
        SimulatedOrderOutcome {
            ack,
            state,
            fill: None,
        }
    }

    fn cancelled_outcome(
        &self,
        request: &SimulatedOrderRequest,
        reason: &str,
    ) -> SimulatedOrderOutcome {
        let ack = VenueSubmitAck {
            order_id: request.order_id.clone(),
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| request.order_id.clone()),
            external_order_id: None,
            status: OrderLifecycleStatus::Cancelled,
            detail: json!({ "reason": reason }),
        };
        let state = VenueOrderState {
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            market_id: request.market_id.clone(),
            status: OrderLifecycleStatus::Cancelled,
            filled_quantity: 0.0,
            average_fill_price: None,
            detail: ack.detail.clone(),
        };
        SimulatedOrderOutcome {
            ack,
            state,
            fill: None,
        }
    }

    fn expired_outcome(&self, request: &SimulatedOrderRequest) -> SimulatedOrderOutcome {
        let ack = VenueSubmitAck {
            order_id: request.order_id.clone(),
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| request.order_id.clone()),
            external_order_id: None,
            status: OrderLifecycleStatus::Expired,
            detail: json!({ "reason": "gtd_expired" }),
        };
        let state = VenueOrderState {
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            market_id: request.market_id.clone(),
            status: OrderLifecycleStatus::Expired,
            filled_quantity: 0.0,
            average_fill_price: None,
            detail: ack.detail.clone(),
        };
        SimulatedOrderOutcome {
            ack,
            state,
            fill: None,
        }
    }

    fn acknowledged_outcome(&self, request: &SimulatedOrderRequest) -> SimulatedOrderOutcome {
        let ack = VenueSubmitAck {
            order_id: request.order_id.clone(),
            client_order_id: request
                .client_order_id
                .clone()
                .unwrap_or_else(|| request.order_id.clone()),
            external_order_id: Some(format!("sim-{}", request.order_id)),
            status: OrderLifecycleStatus::Acknowledged,
            detail: json!({ "reason": "queued" }),
        };
        let state = VenueOrderState {
            order_id: request.order_id.clone(),
            client_order_id: request.client_order_id.clone(),
            market_id: request.market_id.clone(),
            status: OrderLifecycleStatus::Acknowledged,
            filled_quantity: 0.0,
            average_fill_price: None,
            detail: ack.detail.clone(),
        };
        SimulatedOrderOutcome {
            ack,
            state,
            fill: None,
        }
    }
}

#[async_trait]
impl ExecutionVenue for SimExchangeAdapter {
    async fn submit_batch(
        &self,
        _domain: AccountDomain,
        intents: &[ExecutionIntentRecord],
    ) -> std::result::Result<Vec<VenueSubmitAck>, ExecutionError> {
        let mut acks = Vec::with_capacity(intents.len());
        for intent in intents {
            let request = SimulatedOrderRequest {
                order_id: Uuid::new_v4().to_string(),
                client_order_id: Some(intent.client_order_id.clone()),
                market_id: intent.market_id.clone(),
                side: intent.side.as_str().to_owned(),
                limit_price: intent.limit_price,
                quantity: intent.target_size,
                book_price: intent.limit_price,
                available_quantity: intent.target_size,
                post_only: false,
                fok: false,
                fak: false,
                expires_at_ms: Some(intent.expires_at.timestamp_millis()),
                now_ms: intent.created_at.timestamp_millis(),
                leg_count: 1,
                failing_leg_index: None,
            };
            acks.push(self.simulate_order(&request)?.ack);
        }
        Ok(acks)
    }

    async fn cancel_order(
        &self,
        _domain: AccountDomain,
        _order: &OrderLifecycleRecord,
    ) -> std::result::Result<(), ExecutionError> {
        Ok(())
    }

    async fn cancel_market(
        &self,
        _domain: AccountDomain,
        _market_id: &str,
    ) -> std::result::Result<(), ExecutionError> {
        Ok(())
    }

    async fn cancel_all(&self, _domain: AccountDomain) -> std::result::Result<(), ExecutionError> {
        Ok(())
    }

    async fn heartbeat(
        &self,
        _domain: AccountDomain,
    ) -> std::result::Result<VenueHeartbeat, ExecutionError> {
        let failures = self
            .heartbeat_failures
            .load(std::sync::atomic::Ordering::Relaxed);
        Ok(VenueHeartbeat {
            venue_healthy: failures == 0,
            signer_healthy: failures == 0,
            detail: if failures == 0 {
                "sim venue healthy".to_owned()
            } else {
                format!("sim venue degraded failures={failures}")
            },
        })
    }

    async fn reconcile(
        &self,
        _domain: AccountDomain,
        orders: &[OrderLifecycleRecord],
    ) -> std::result::Result<Vec<VenueOrderState>, ExecutionError> {
        Ok(orders
            .iter()
            .map(|order| VenueOrderState {
                order_id: order.order_id.to_string(),
                client_order_id: order.client_order_id.clone(),
                market_id: order.market_id.clone(),
                status: order.status,
                filled_quantity: order.filled_quantity,
                average_fill_price: order.average_fill_price,
                detail: order.detail.clone(),
            })
            .collect())
    }
}

#[derive(Clone)]
pub struct SimEngineService {
    context: ServiceContext,
    config: SimConfig,
    adapter: Arc<SimExchangeAdapter>,
}

impl SimEngineService {
    pub fn new(context: ServiceContext) -> Self {
        let config = SimConfig::from_env(context.domain).expect("sim config");
        let adapter = Arc::new(SimExchangeAdapter::new(&config));
        Self {
            context,
            config,
            adapter,
        }
    }

    async fn run(self, cancellation: CancellationToken) -> Result<()> {
        if self.context.domain != AccountDomain::Sim || !self.config.enabled {
            info!(domain = %self.context.domain, "sim engine skipped");
            return Ok(());
        }

        let run_id = Uuid::new_v4();
        match self.config.mode {
            SimMode::Replay => self.run_replay(run_id).await?,
            SimMode::Shadow => self.run_shadow(run_id, cancellation).await?,
            SimMode::PaperLiveQueue => self.run_paper_live_queue(run_id, cancellation).await?,
        }
        Ok(())
    }

    async fn run_replay(&self, run_id: Uuid) -> Result<()> {
        let cursor = ReplayCursor {
            after_sequence: None,
            limit: 512,
        };
        let historical = self
            .context
            .store
            .replay_events(self.context.domain, cursor)?;
        let started_at = now();
        let mut processed = 0_u64;
        let orders = 0_u64;
        let fills = 0_u64;

        for event in historical {
            processed += 1;
            let sim_event = self.context.store.append_sim_event(NewSimEvent {
                domain: self.context.domain,
                run_id,
                sequence: processed,
                mode: SimMode::Replay,
                event_kind: SimEventKind::MarketSnapshot,
                event_time: event.persisted_at,
                market_id: Some(event.aggregate_id.clone()),
                payload: event.payload.clone(),
            })?;
            self.context
                .bus
                .publish_sim_event(self.context.domain, &sim_event)?;
        }

        self.context
            .store
            .upsert_replay_checkpoint(NewReplayCheckpoint {
                domain: self.context.domain,
                run_id,
                mode: SimMode::Replay,
                cursor: processed.to_string(),
                processed_events: processed,
                updated_at: now(),
                metadata: json!({
                    "data_source": self.config.data_source,
                    "replay_start": self.config.replay_start,
                    "replay_end": self.config.replay_end,
                }),
            })?;

        let drift = build_drift_report(
            SimMode::Replay,
            processed,
            orders,
            fills,
            self.config.drift_alert_bps,
        );
        let report = SimRunReport {
            run_id,
            domain: self.context.domain,
            mode: SimMode::Replay,
            started_at,
            completed_at: Some(now()),
            input_snapshot_hash: format!("replay-{}-{}", self.context.domain, processed),
            rule_version_hash: format!("rules-{}", self.context.domain),
            processed_events: processed,
            orders_emitted: orders,
            fills_emitted: fills,
            drift,
            risk_decisions: vec!["replayed historical risk context".to_owned()],
        };
        self.context.store.finalize_sim_run(&report)?;
        self.context
            .bus
            .publish_sim_report(self.context.domain, &report)?;
        Ok(())
    }

    async fn run_shadow(&self, run_id: Uuid, cancellation: CancellationToken) -> Result<()> {
        self.run_live_like_mode(run_id, SimMode::Shadow, cancellation)
            .await
    }

    async fn run_paper_live_queue(
        &self,
        run_id: Uuid,
        cancellation: CancellationToken,
    ) -> Result<()> {
        self.run_live_like_mode(run_id, SimMode::PaperLiveQueue, cancellation)
            .await
    }

    async fn run_live_like_mode(
        &self,
        run_id: Uuid,
        mode: SimMode,
        cancellation: CancellationToken,
    ) -> Result<()> {
        let started_at = now();
        let mut processed = 0_u64;
        let mut orders = 0_u64;
        let mut fills = 0_u64;
        let mut inbox = self.context.bus.subscribe();
        let mut ticker = interval(Duration::from_millis(self.config.event_step_ms.max(50)));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut recent_topics = VecDeque::with_capacity(32);

        loop {
            select! {
                _ = cancellation.cancelled() => break,
                _ = ticker.tick() => {
                    let heartbeat = self.adapter.heartbeat(self.context.domain).await?;
                    if !heartbeat.venue_healthy {
                        warn!(domain = %self.context.domain, detail = %heartbeat.detail, "sim heartbeat degraded");
                    }
                }
                envelope = inbox.recv() => {
                    let envelope = match envelope {
                        Ok(value) => value,
                        Err(_) => continue,
                    };
                    if envelope.domain != self.context.domain || envelope.service == polymarket_core::ServiceKind::SimEngine {
                        continue;
                    }
                    processed += 1;
                    recent_topics.push_back(envelope.topic.clone());
                    if recent_topics.len() > 16 {
                        recent_topics.pop_front();
                    }
                    let sim_event = self.context.store.append_sim_event(NewSimEvent {
                        domain: self.context.domain,
                        run_id,
                        sequence: processed,
                        mode,
                        event_kind: classify_topic(&envelope.topic),
                        event_time: envelope.emitted_at,
                        market_id: None,
                        payload: json!({
                            "topic": envelope.topic,
                            "payload": envelope.payload,
                            "service": envelope.service.as_str(),
                        }),
                    })?;
                    self.context.bus.publish_sim_event(self.context.domain, &sim_event)?;

                    let request = SimulatedOrderRequest {
                        order_id: Uuid::new_v4().to_string(),
                        client_order_id: Some(format!("{mode}-{processed}")),
                        market_id: "shadow-market".to_owned(),
                        side: "BUY".to_owned(),
                        limit_price: 0.51,
                        quantity: 10.0,
                        book_price: 0.52,
                        available_quantity: 10.0,
                        post_only: false,
                        fok: false,
                        fak: mode == SimMode::PaperLiveQueue,
                        expires_at_ms: None,
                        now_ms: envelope.emitted_at.timestamp_millis(),
                        leg_count: 1,
                        failing_leg_index: None,
                    };
                    let outcome = self.adapter.simulate_order(&request)?;
                    let order = self.context.store.record_sim_order_state(NewSimOrderRecord {
                        domain: self.context.domain,
                        run_id,
                        mode,
                        order_id: outcome.state.order_id.clone(),
                        client_order_id: outcome.state.client_order_id.clone(),
                        market_id: outcome.state.market_id.clone(),
                        status: outcome.state.status,
                        filled_quantity: outcome.state.filled_quantity,
                        average_fill_price: outcome.state.average_fill_price,
                        detail: outcome.state.detail.clone(),
                        updated_at: now(),
                    })?;
                    orders += 1;
                    self.context.bus.publish_sim_order(
                        self.context.domain,
                        run_id,
                        &order.order_id,
                        &order.market_id,
                        order.status.as_str(),
                    )?;
                    if let Some(fill) = outcome.fill {
                        let fill = self.context.store.record_sim_fill(NewSimFillRecord {
                            domain: self.context.domain,
                            run_id,
                            mode,
                            order_id: fill.order_id.clone(),
                            market_id: fill.market_id.clone(),
                            quantity: fill.quantity,
                            price: fill.price,
                            fees_paid: fill.fees_paid,
                            filled_at: fill.filled_at,
                            detail: fill.detail.clone(),
                        })?;
                        fills += 1;
                        self.context.bus.publish_sim_fill(self.context.domain, &fill)?;
                    }
                }
            }
        }

        self.context
            .store
            .upsert_replay_checkpoint(NewReplayCheckpoint {
                domain: self.context.domain,
                run_id,
                mode,
                cursor: processed.to_string(),
                processed_events: processed,
                updated_at: now(),
                metadata: json!({ "recent_topics": recent_topics }),
            })?;

        let drift = build_drift_report(mode, processed, orders, fills, self.config.drift_alert_bps);
        self.context
            .bus
            .publish_sim_drift(self.context.domain, &drift)?;
        let report = SimRunReport {
            run_id,
            domain: self.context.domain,
            mode,
            started_at,
            completed_at: Some(now()),
            input_snapshot_hash: format!("live-{}-{processed}", self.context.domain),
            rule_version_hash: format!("rules-live-{}", self.context.domain),
            processed_events: processed,
            orders_emitted: orders,
            fills_emitted: fills,
            drift,
            risk_decisions: vec!["sim-engine shadow validation".to_owned()],
        };
        self.context.store.finalize_sim_run(&report)?;
        self.context
            .bus
            .publish_sim_report(self.context.domain, &report)?;
        Ok(())
    }
}

pub async fn run_sim_engine(
    context: ServiceContext,
    cancellation: CancellationToken,
) -> Result<()> {
    SimEngineService::new(context).run(cancellation).await
}

fn classify_topic(topic: &str) -> SimEventKind {
    match topic {
        topic if topic.contains("heartbeat") => SimEventKind::Heartbeat,
        topic if topic.contains("rule") => SimEventKind::RuleVersion,
        topic if topic.contains("intent") => SimEventKind::Intent,
        topic if topic.contains("fill") => SimEventKind::BaselineFill,
        topic if topic.contains("order") => SimEventKind::BaselineOrder,
        _ => SimEventKind::MarketSnapshot,
    }
}

fn build_drift_report(
    mode: SimMode,
    processed_events: u64,
    orders_emitted: u64,
    fills_emitted: u64,
    drift_alert_bps: u32,
) -> SimDriftReport {
    let fill_rate_bps = if orders_emitted == 0 {
        0
    } else {
        ((fills_emitted.saturating_mul(10_000)) / orders_emitted) as u32
    };
    let drift_score_bps = if processed_events == 0 {
        0
    } else {
        (((orders_emitted.saturating_sub(fills_emitted)) * 10_000) / processed_events.max(1)) as u32
    };
    SimDriftReport {
        run_id: Uuid::new_v4(),
        mode,
        intent_match_rate_bps: 10_000_u32.saturating_sub(drift_score_bps.min(10_000)),
        reject_rate_bps: if orders_emitted == 0 {
            0
        } else {
            ((orders_emitted.saturating_sub(fills_emitted)) * 10_000 / orders_emitted) as u32
        },
        fill_rate_bps,
        fee_leakage_bps: 0,
        average_latency_ms: 250,
        p95_latency_ms: 250,
        baseline_orders: processed_events,
        simulated_orders: orders_emitted,
        drift_score_bps,
        severity: if drift_score_bps >= drift_alert_bps {
            SimDriftSeverity::Alert
        } else if drift_score_bps > 0 {
            SimDriftSeverity::Warn
        } else {
            SimDriftSeverity::Info
        },
        notes: vec![format!("mode={mode} processed={processed_events}")],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_request() -> SimulatedOrderRequest {
        SimulatedOrderRequest {
            order_id: "order-1".to_owned(),
            client_order_id: Some("client-1".to_owned()),
            market_id: "market-1".to_owned(),
            side: "BUY".to_owned(),
            limit_price: 0.51,
            quantity: 10.0,
            book_price: 0.52,
            available_quantity: 10.0,
            post_only: false,
            fok: false,
            fak: false,
            expires_at_ms: None,
            now_ms: 1_700_000_000_000,
            leg_count: 1,
            failing_leg_index: None,
        }
    }

    fn adapter() -> SimExchangeAdapter {
        SimExchangeAdapter {
            fill_model: FillModel {
                probability_bps: 10_000,
            },
            fee_model: FeeModel { fee_bps: 35 },
            latency_model: LatencyModel { latency_ms: 25 },
            queue_model: QueueModel { depth: 100 },
            maintenance_calendar: MaintenanceCalendar {
                maintenance_windows: Vec::new(),
            },
            heartbeat_failures: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    #[test]
    fn rejects_post_only_crossing_orders() {
        let adapter = adapter();
        let mut request = base_request();
        request.post_only = true;
        request.book_price = 0.50;
        let outcome = adapter.simulate_order(&request).expect("simulate order");
        assert_eq!(outcome.state.status, OrderLifecycleStatus::Rejected);
    }

    #[test]
    fn fok_cancels_when_not_fully_fillable() {
        let adapter = adapter();
        let mut request = base_request();
        request.fok = true;
        request.available_quantity = 2.0;
        let outcome = adapter.simulate_order(&request).expect("simulate order");
        assert_eq!(outcome.state.status, OrderLifecycleStatus::Cancelled);
    }

    #[test]
    fn fak_partially_fills_and_cancels_remainder() {
        let adapter = adapter();
        let mut request = base_request();
        request.fak = true;
        request.available_quantity = 3.0;
        let outcome = adapter.simulate_order(&request).expect("simulate order");
        assert_eq!(outcome.state.status, OrderLifecycleStatus::PartiallyFilled);
        assert_eq!(outcome.state.filled_quantity, 3.0);
    }

    #[test]
    fn gtd_expired_orders_become_expired() {
        let adapter = adapter();
        let mut request = base_request();
        request.expires_at_ms = Some(request.now_ms - 1);
        let outcome = adapter.simulate_order(&request).expect("simulate order");
        assert_eq!(outcome.state.status, OrderLifecycleStatus::Expired);
    }

    #[test]
    fn maintenance_window_returns_error() {
        let mut adapter = adapter();
        adapter.maintenance_calendar.maintenance_windows = vec![(0, 999)];
        let result = adapter.simulate_order(&base_request());
        assert!(matches!(result, Err(ExecutionError::VenueRestart(_))));
    }

    #[test]
    fn multi_leg_failure_rejects_order() {
        let adapter = adapter();
        let mut request = base_request();
        request.leg_count = 2;
        request.failing_leg_index = Some(1);
        let outcome = adapter.simulate_order(&request).expect("simulate order");
        assert_eq!(outcome.state.status, OrderLifecycleStatus::Rejected);
    }
}
