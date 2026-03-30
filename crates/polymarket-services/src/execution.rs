use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use base64::Engine;
use chrono::{DateTime, TimeZone, Utc};
use chrono_tz::Tz;
use hmac::{Hmac, Mac};
use polymarket_config::{CredentialSource, DomainConfig, ExecutionEngineConfig};
use polymarket_core::{
    now, AccountDomain, ExecutionCommand, ExecutionCommandKind, ExecutionError, ExecutionEvent,
    ExecutionEventKind, ExecutionHeartbeat, ExecutionIntentRecord, ExecutionIntentStatus,
    ExecutionReconcileReport, IdempotencyClaimResult, NewDurableEvent, NewIdempotencyKey,
    NewOrderLifecycleRecord, NewStateSnapshot, OrderLifecycleRecord, OrderLifecycleStatus,
    ServiceKind, Timestamp, TradeIntent, TradeIntentBatch, TradeSide,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::{
    publish_heartbeat, ApprovedTradeIntentBatch, ClosePnlNotification, DailyReportNotification,
    NotificationEvent, ServiceContext,
};

const TOPIC_APPROVED_INTENT: &str = "trade.intent.approved";
const TOPIC_EXECUTION_LIFECYCLE: &str = "execution.lifecycle";
const TOPIC_EXECUTION_HEARTBEAT: &str = "execution.heartbeat";
const TOPIC_EXECUTION_RECONCILE: &str = "execution.reconcile";
const TOPIC_EXECUTION_ALERT: &str = "execution.alert";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommandCursor {
    last_sequence: i64,
    updated_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApprovedIntentCursor {
    last_snapshot_version: i64,
    updated_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExecutionVenueCredentials {
    key: String,
    secret: String,
    passphrase: Option<String>,
    wallet: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueSubmitAck {
    pub order_id: String,
    pub client_order_id: String,
    pub external_order_id: Option<String>,
    pub status: OrderLifecycleStatus,
    pub detail: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueOrderState {
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_id: String,
    pub status: OrderLifecycleStatus,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub detail: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueHeartbeat {
    pub venue_healthy: bool,
    pub signer_healthy: bool,
    pub detail: String,
}

#[async_trait]
pub trait ExecutionVenue: Send + Sync {
    async fn submit_batch(
        &self,
        domain: AccountDomain,
        intents: &[ExecutionIntentRecord],
    ) -> std::result::Result<Vec<VenueSubmitAck>, ExecutionError>;

    async fn cancel_order(
        &self,
        domain: AccountDomain,
        order: &OrderLifecycleRecord,
    ) -> std::result::Result<(), ExecutionError>;

    async fn cancel_market(
        &self,
        domain: AccountDomain,
        market_id: &str,
    ) -> std::result::Result<(), ExecutionError>;

    async fn cancel_all(&self, domain: AccountDomain) -> std::result::Result<(), ExecutionError>;

    async fn heartbeat(
        &self,
        domain: AccountDomain,
    ) -> std::result::Result<VenueHeartbeat, ExecutionError>;

    async fn reconcile(
        &self,
        domain: AccountDomain,
        orders: &[OrderLifecycleRecord],
    ) -> std::result::Result<Vec<VenueOrderState>, ExecutionError>;
}

#[derive(Clone)]
struct ExecutionEngineService {
    context: ServiceContext,
    config: ExecutionEngineConfig,
    venue: Arc<dyn ExecutionVenue>,
    operator: String,
    consecutive_heartbeat_failures: u32,
    position_book: PositionBook,
    daily_report_cursor: DailyReportCursor,
    report_timezone: Tz,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PositionBook {
    lots_by_instrument: BTreeMap<String, VecDeque<OpenLot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenLot {
    side: TradeSide,
    quantity: f64,
    average_price: f64,
    remaining_entry_fees: f64,
    opened_at: Timestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct DailyReportCursor {
    last_report_date: Option<String>,
}

#[derive(Debug, Clone)]
struct CloseComputation {
    closed_quantity: f64,
    entry_notional: f64,
    exit_notional: f64,
    gross_realized_pnl: f64,
    entry_fees_allocated: f64,
    exit_fees: f64,
}

impl ExecutionEngineService {
    fn new(
        context: ServiceContext,
        config: ExecutionEngineConfig,
        venue: Arc<dyn ExecutionVenue>,
    ) -> Result<Self> {
        let report_timezone = context
            .telegram
            .timezone
            .parse::<Tz>()
            .with_context(|| {
                format!(
                    "failed to parse POLYMARKET_TELEGRAM_TIMEZONE `{}`",
                    context.telegram.timezone
                )
            })?;
        Ok(Self {
            context,
            config,
            venue,
            operator: "execution-engine".to_owned(),
            consecutive_heartbeat_failures: 0,
            position_book: PositionBook::default(),
            daily_report_cursor: DailyReportCursor::default(),
            report_timezone,
        })
    }

    async fn run(mut self, cancellation: CancellationToken) -> Result<()> {
        self.restore_recovery_state().await?;
        self.restore_latest_approved_batch().await?;
        self.restore_position_book()?;
        self.daily_report_cursor = self.load_daily_report_cursor()?;

        let mut receiver = self.context.bus.subscribe();
        let mut heartbeat = interval(self.config.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut reconcile = interval(self.config.reconcile_interval);
        reconcile.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                _ = cancellation.cancelled() => {
                    if self.config.emergency_cancel_on_shutdown {
                        self.handle_shutdown_cancel().await?;
                    }
                    self.context.audit.record(
                        Some(self.context.domain),
                        ServiceKind::ExecutionEngine.as_str(),
                        "service_stopped",
                        "cancellation requested",
                    )?;
                    return Ok(());
                }
                result = receiver.recv() => {
                    match result {
                        Ok(event) if event.domain == self.context.domain && event.topic == TOPIC_APPROVED_INTENT => {
                            self.process_approved_message(&event.payload).await?;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            warn!(error = %error, "execution bus receive failed");
                        }
                    }
                }
                _ = heartbeat.tick() => {
                    self.process_operator_commands().await?;
                    self.perform_heartbeat().await?;
                    self.maybe_send_daily_report().await?;
                }
                _ = reconcile.tick() => {
                    self.process_operator_commands().await?;
                    self.perform_reconcile().await?;
                    self.maybe_send_daily_report().await?;
                }
            }
        }
    }

    async fn process_approved_message(&mut self, payload: &str) -> Result<()> {
        let batch = parse_trade_intent_batch(payload)?;
        for intent in batch.intents {
            self.process_trade_intent(intent, Some(batch.created_at))
                .await?;
        }
        if let Some(snapshot) = self
            .context
            .store
            .latest_snapshot(self.context.domain, "approved_trade_intent_batch", "latest")?
        {
            self.store_approved_cursor(snapshot.version)?;
        }
        Ok(())
    }

    async fn restore_latest_approved_batch(&mut self) -> Result<()> {
        let cursor = self.load_approved_cursor()?;
        let Some(snapshot) = self
            .context
            .store
            .latest_snapshot(self.context.domain, "approved_trade_intent_batch", "latest")?
        else {
            return Ok(());
        };
        if snapshot.version <= cursor.last_snapshot_version {
            return Ok(());
        }
        let approved: ApprovedTradeIntentBatch = serde_json::from_value(snapshot.payload)
            .context("failed to decode approved trade intent batch")?;
        for intent in approved.batch.intents {
            self.process_trade_intent(intent, Some(approved.batch.created_at))
                .await?;
        }
        self.store_approved_cursor(snapshot.version)?;
        Ok(())
    }

    fn restore_position_book(&mut self) -> Result<()> {
        let intents = self
            .context
            .store
            .list_all_execution_intents(self.context.domain)?;
        let mut intents_by_client_order = BTreeMap::new();
        for intent in intents {
            intents_by_client_order.insert(intent.client_order_id.clone(), intent);
        }

        let mut orders = self.context.store.list_all_order_lifecycle(self.context.domain)?;
        orders.sort_by_key(|order| (order.updated_at, order.order_id.clone()));
        for order in orders.into_iter().filter(|order| order.filled_quantity > 0.0) {
            let Some(client_order_id) = order.client_order_id.as_ref() else {
                continue;
            };
            let Some(intent) = intents_by_client_order.get(client_order_id) else {
                continue;
            };
            let fill_price = order
                .average_fill_price
                .or(order.limit_price)
                .unwrap_or(intent.limit_price);
            let fill_fee = estimate_fill_fee_from_transition(
                None,
                0.0,
                Some(&order.detail),
                order.filled_quantity,
                fill_price,
                self.config.default_fee_bps,
            );
            self.apply_fill_to_position_book(
                intent,
                order.filled_quantity,
                fill_price,
                fill_fee,
                order.updated_at,
            );
        }
        Ok(())
    }

    fn load_daily_report_cursor(&self) -> Result<DailyReportCursor> {
        let snapshot = self.context.store.latest_snapshot(
            self.context.domain,
            "telegram_daily_report_cursor",
            "current",
        )?;
        Ok(match snapshot {
            Some(snapshot) => serde_json::from_value(snapshot.payload)
                .context("failed to decode telegram daily report cursor")?,
            None => DailyReportCursor::default(),
        })
    }

    fn store_daily_report_cursor(&self) -> Result<()> {
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: "telegram_daily_report_cursor".to_owned(),
            aggregate_id: "current".to_owned(),
            version: now().timestamp_millis(),
            payload: serde_json::to_value(&self.daily_report_cursor)?,
            derived_from_sequence: None,
            created_at: now(),
        })?;
        Ok(())
    }

    async fn process_trade_intent(
        &mut self,
        intent: TradeIntent,
        batch_created_at: Option<Timestamp>,
    ) -> Result<()> {
        validate_intent(&self.context, &intent)?;
        let intent_id = intent.opportunity_id;
        let idempotency_key = build_idempotency_key(&intent);
        let client_order_id = build_client_order_id(&intent);
        let request_hash = hash_trade_intent(&intent, &idempotency_key);
        match self
            .context
            .store
            .claim_idempotency_key(NewIdempotencyKey {
                domain: self.context.domain,
                scope: "execution.submit".to_owned(),
                key: idempotency_key.clone(),
                request_hash,
                created_by: self.operator.clone(),
                created_at: now(),
                lock_expires_at: Some(now() + chrono::Duration::seconds(30)),
            })? {
            IdempotencyClaimResult::Claimed(_) => {}
            IdempotencyClaimResult::DuplicateCompleted(_)
            | IdempotencyClaimResult::DuplicateInFlight(_) => {
                return Ok(());
            }
            IdempotencyClaimResult::HashMismatch(existing) => {
                return Err(anyhow!(
                    "idempotency hash mismatch for key `{}` status={}",
                    existing.key,
                    existing.status
                ));
            }
        }

        let record = self.context.store.record_execution_intent(
            &intent,
            intent_id,
            batch_created_at.map(|_| intent.opportunity_id),
            idempotency_key.clone(),
            client_order_id.clone(),
            ExecutionIntentStatus::ReadyToSubmit,
            json!({
                "event_id": intent.event_id,
                "policy": intent.policy,
                "thesis_ref": intent.thesis_ref,
            }),
        )?;
        self.publish_lifecycle(ExecutionEventKind::Accepted, &record, "intent accepted")?;
        let submitted = self.submit_intent(record).await;
        if let Err(error) = submitted {
            self.context.store.mark_idempotency_key(
                self.context.domain,
                "execution.submit",
                &idempotency_key,
                &json!({
                    "error": error.to_string(),
                    "kind": error.kind(),
                }),
                false,
            )?;
            return Err(anyhow!(error.to_string()));
        }
        Ok(())
    }

    async fn submit_intent(
        &mut self,
        record: ExecutionIntentRecord,
    ) -> std::result::Result<(), ExecutionError> {
        let acks = self
            .venue
            .submit_batch(self.context.domain, std::slice::from_ref(&record))
            .await?;
        let ack = acks.into_iter().next().ok_or_else(|| {
            ExecutionError::VenueRejected("submit_batch returned no ack".to_owned())
        })?;
        self.context
            .store
            .upsert_execution_state(ExecutionIntentRecord {
                status: map_order_status_to_intent_status(ack.status),
                updated_at: now(),
                detail: ack.detail.clone(),
                ..record.clone()
            })
            .map_err(|error| ExecutionError::StateConflict(error.to_string()))?;
        let durable = self
            .context
            .store
            .append_order_event(NewDurableEvent {
                domain: self.context.domain,
                stream: format!("execution:{}", ack.client_order_id),
                aggregate_type: "execution_intent".to_owned(),
                aggregate_id: record.intent_id.to_string(),
                event_type: "EXECUTION_SUBMITTED".to_owned(),
                causation_id: None,
                correlation_id: Some(record.intent_id),
                idempotency_key: Some(record.idempotency_key.clone()),
                payload: serde_json::to_value(&ack).unwrap_or_else(|_| json!({})),
                metadata: json!({
                    "market_id": record.market_id,
                    "client_order_id": record.client_order_id,
                }),
                created_at: now(),
            })
            .map_err(|error| ExecutionError::StateConflict(error.to_string()))?;

        self.context
            .store
            .upsert_order_lifecycle(NewOrderLifecycleRecord {
                domain: self.context.domain,
                order_id: ack.order_id.clone(),
                market_id: record.market_id.clone(),
                status: ack.status,
                client_order_id: Some(ack.client_order_id.clone()),
                external_order_id: ack.external_order_id.clone(),
                idempotency_key: Some(record.idempotency_key.clone()),
                side: Some(record.side.as_str().to_owned()),
                limit_price: Some(record.limit_price),
                order_quantity: Some(record.target_size),
                filled_quantity: 0.0,
                average_fill_price: None,
                last_event_sequence: Some(durable.sequence),
                detail: ack.detail.clone(),
                opened_at: record.created_at,
                updated_at: now(),
                closed_at: if ack.status.is_terminal() {
                    Some(now())
                } else {
                    None
                },
            })
            .map_err(|error| ExecutionError::StateConflict(error.to_string()))?;

        self.context
            .store
            .mark_idempotency_key(
                self.context.domain,
                "execution.submit",
                &record.idempotency_key,
                &json!({
                    "order_id": ack.order_id,
                    "client_order_id": ack.client_order_id,
                    "status": ack.status,
                }),
                true,
            )
            .map_err(|error| ExecutionError::StateConflict(error.to_string()))?;

        self.publish_lifecycle(ExecutionEventKind::Submitted, &record, "intent submitted")
            .map_err(|error| ExecutionError::StateConflict(error.to_string()))?;
        Ok(())
    }

    async fn restore_recovery_state(&mut self) -> Result<()> {
        let intents = self
            .context
            .store
            .load_recovery_batch(self.context.domain, self.config.max_concurrent_intents)?;
        if intents.is_empty() {
            return Ok(());
        }

        let orders = self.context.store.list_open_orders(self.context.domain)?;
        let remote = self
            .venue
            .reconcile(self.context.domain, &orders)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        self.apply_reconcile_states(remote).await?;
        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::ExecutionEngine.as_str(),
            "recovery_restored",
            &format!("intents={}", intents.len()),
        )?;
        Ok(())
    }

    async fn perform_heartbeat(&mut self) -> Result<()> {
        match self.venue.heartbeat(self.context.domain).await {
            Ok(status) => {
                self.consecutive_heartbeat_failures = 0;
                let heartbeat = ExecutionHeartbeat {
                    domain: self.context.domain,
                    service: ServiceKind::ExecutionEngine,
                    venue_healthy: status.venue_healthy,
                    signer_healthy: status.signer_healthy,
                    event_loop_healthy: true,
                    consecutive_failures: 0,
                    detail: status.detail.clone(),
                    observed_at: now(),
                };
                self.context.store.upsert_snapshot(NewStateSnapshot {
                    domain: self.context.domain,
                    aggregate_type: "execution_heartbeat".to_owned(),
                    aggregate_id: "latest".to_owned(),
                    version: heartbeat.observed_at.timestamp_millis(),
                    payload: serde_json::to_value(&heartbeat)?,
                    derived_from_sequence: None,
                    created_at: heartbeat.observed_at,
                })?;
                self.context.bus.publish(
                    ServiceKind::ExecutionEngine,
                    self.context.domain,
                    TOPIC_EXECUTION_HEARTBEAT,
                    serde_json::to_string(&heartbeat)?,
                );
                publish_heartbeat(&self.context, ServiceKind::ExecutionEngine, &status.detail);
            }
            Err(error) => {
                self.consecutive_heartbeat_failures += 1;
                self.context.audit.record(
                    Some(self.context.domain),
                    ServiceKind::ExecutionEngine.as_str(),
                    "heartbeat_failed",
                    &error.to_string(),
                )?;
                if self.consecutive_heartbeat_failures
                    >= self.config.heartbeat_failure_cancel_threshold
                {
                    self.venue
                        .cancel_all(self.context.domain)
                        .await
                        .map_err(|cancel_error| anyhow!(cancel_error.to_string()))?;
                    self.raise_alert("heartbeat failure threshold reached; cancel_all sent")?;
                }
            }
        }
        Ok(())
    }

    async fn perform_reconcile(&mut self) -> Result<()> {
        let orders = self
            .context
            .store
            .fetch_reconcile_window(self.context.domain, 250)?;
        let states = self
            .venue
            .reconcile(self.context.domain, &orders)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        let checked_orders = orders.len();
        let mismatches_fixed = self.apply_reconcile_states(states).await?;
        let report = ExecutionReconcileReport {
            domain: self.context.domain,
            generated_at: now(),
            checked_orders,
            mismatches_fixed,
            missing_remote_orders: 0,
            alerts: Vec::new(),
        };
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: "execution_reconcile".to_owned(),
            aggregate_id: "latest".to_owned(),
            version: report.generated_at.timestamp_millis(),
            payload: serde_json::to_value(&report)?,
            derived_from_sequence: None,
            created_at: report.generated_at,
        })?;
        self.context.bus.publish(
            ServiceKind::ExecutionEngine,
            self.context.domain,
            TOPIC_EXECUTION_RECONCILE,
            serde_json::to_string(&report)?,
        );
        Ok(())
    }

    async fn apply_reconcile_states(&mut self, states: Vec<VenueOrderState>) -> Result<usize> {
        let mut changed = 0usize;
        let intents = self
            .context
            .store
            .list_execution_intents(self.context.domain, 1_000)?;
        let mut by_client_order = BTreeMap::new();
        for intent in intents {
            by_client_order.insert(intent.client_order_id.clone(), intent);
        }

        for state in states {
            let Some(client_order_id) = state.client_order_id.clone() else {
                continue;
            };
            let Some(intent) = by_client_order.get(&client_order_id).cloned() else {
                continue;
            };
            let existing_order = self
                .context
                .store
                .order_lifecycle(self.context.domain, &state.order_id)?;
            let new_status = map_order_status_to_intent_status(state.status);
            if intent.status != new_status {
                changed += 1;
                self.context
                    .store
                    .upsert_execution_state(ExecutionIntentRecord {
                        status: new_status,
                        updated_at: now(),
                        detail: state.detail.clone(),
                        ..intent.clone()
                    })?;
                self.publish_lifecycle(
                    ExecutionEventKind::Reconciled,
                    &intent,
                    "state reconciled",
                )?;
            }

            let previous_filled = existing_order
                .as_ref()
                .map(|order| order.filled_quantity)
                .unwrap_or(0.0);
            if state.filled_quantity > previous_filled + f64::EPSILON {
                let fill_price = incremental_fill_price(
                    previous_filled,
                    existing_order.as_ref().and_then(|order| order.average_fill_price),
                    state.filled_quantity,
                    state.average_fill_price,
                )
                .unwrap_or_else(|| {
                    state
                        .average_fill_price
                        .unwrap_or(intent.limit_price)
                });
                let fill_quantity = state.filled_quantity - previous_filled;
                let fill_fee = estimate_fill_fee_from_transition(
                    existing_order.as_ref().map(|order| &order.detail),
                    previous_filled,
                    Some(&state.detail),
                    state.filled_quantity,
                    state.average_fill_price.unwrap_or(fill_price),
                    self.config.default_fee_bps,
                );
                let notifications = self.apply_fill_to_position_book(
                    &intent,
                    fill_quantity,
                    fill_price,
                    fill_fee,
                    now(),
                );
                for notification in notifications {
                    self.send_close_pnl_notification(notification).await?;
                }
            }
            self.context
                .store
                .upsert_order_lifecycle(NewOrderLifecycleRecord {
                    domain: self.context.domain,
                    order_id: state.order_id.clone(),
                    market_id: state.market_id.clone(),
                    status: state.status,
                    client_order_id: Some(client_order_id),
                    external_order_id: None,
                    idempotency_key: Some(intent.idempotency_key.clone()),
                    side: Some(intent.side.as_str().to_owned()),
                    limit_price: Some(intent.limit_price),
                    order_quantity: Some(intent.target_size),
                    filled_quantity: state.filled_quantity,
                    average_fill_price: state.average_fill_price,
                    last_event_sequence: None,
                    detail: state.detail,
                    opened_at: intent.created_at,
                    updated_at: now(),
                    closed_at: if state.status.is_terminal() {
                        Some(now())
                    } else {
                        None
                    },
                })?;
        }

        Ok(changed)
    }

    fn apply_fill_to_position_book(
        &mut self,
        intent: &ExecutionIntentRecord,
        quantity: f64,
        fill_price: f64,
        fill_fee: f64,
        occurred_at: Timestamp,
    ) -> Vec<ClosePnlNotification> {
        if quantity <= f64::EPSILON {
            return Vec::new();
        }

        let key = instrument_key(&intent.market_id, &intent.token_id);
        let lots = self.position_book.lots_by_instrument.entry(key).or_default();
        let incoming_side = intent.side;
        let fee_per_unit = fill_fee / quantity;
        let mut remaining = quantity;
        let mut close = CloseComputation {
            closed_quantity: 0.0,
            entry_notional: 0.0,
            exit_notional: 0.0,
            gross_realized_pnl: 0.0,
            entry_fees_allocated: 0.0,
            exit_fees: 0.0,
        };

        while remaining > f64::EPSILON {
            let Some(front) = lots.front_mut() else {
                break;
            };
            if front.side == incoming_side {
                break;
            }

            let matched_quantity = front.quantity.min(remaining);
            let entry_fee_allocated = if front.quantity > f64::EPSILON {
                front.remaining_entry_fees * (matched_quantity / front.quantity)
            } else {
                0.0
            };
            let gross_realized_pnl = match (front.side, incoming_side) {
                (TradeSide::Buy, TradeSide::Sell) => {
                    (fill_price - front.average_price) * matched_quantity
                }
                (TradeSide::Sell, TradeSide::Buy) => {
                    (front.average_price - fill_price) * matched_quantity
                }
                _ => 0.0,
            };
            close.closed_quantity += matched_quantity;
            close.entry_notional += front.average_price * matched_quantity;
            close.exit_notional += fill_price * matched_quantity;
            close.gross_realized_pnl += gross_realized_pnl;
            close.entry_fees_allocated += entry_fee_allocated;
            close.exit_fees += fee_per_unit * matched_quantity;

            front.quantity -= matched_quantity;
            front.remaining_entry_fees =
                (front.remaining_entry_fees - entry_fee_allocated).max(0.0);
            remaining -= matched_quantity;

            if front.quantity <= f64::EPSILON {
                lots.pop_front();
            }
        }

        if remaining > f64::EPSILON {
            let open_fee = fee_per_unit * remaining;
            lots.push_back(OpenLot {
                side: incoming_side,
                quantity: remaining,
                average_price: fill_price,
                remaining_entry_fees: open_fee,
                opened_at: occurred_at,
            });
        }

        if close.closed_quantity <= f64::EPSILON {
            return Vec::new();
        }

        vec![ClosePnlNotification {
            domain: self.context.domain,
            market_id: intent.market_id.clone(),
            token_id: intent.token_id.clone(),
            side: incoming_side.as_str().to_owned(),
            closed_quantity: close.closed_quantity,
            entry_average_price: close.entry_notional / close.closed_quantity,
            exit_average_price: close.exit_notional / close.closed_quantity,
            gross_realized_pnl: close.gross_realized_pnl,
            entry_fees_allocated: close.entry_fees_allocated,
            exit_fees: close.exit_fees,
            total_fees: close.entry_fees_allocated + close.exit_fees,
            net_realized_pnl: close.gross_realized_pnl
                - close.entry_fees_allocated
                - close.exit_fees,
            occurred_at,
            dedupe_key: format!(
                "{}:{}:{:.8}:{:.8}",
                intent.client_order_id,
                incoming_side.as_str(),
                close.closed_quantity,
                close.exit_notional
            ),
        }]
    }

    async fn send_close_pnl_notification(
        &self,
        notification: ClosePnlNotification,
    ) -> Result<()> {
        let key = format!("close-pnl:{}", notification.dedupe_key);
        if !self.claim_notification(&key, &notification)? {
            return Ok(());
        }
        let event = NotificationEvent::ClosePnl(notification.clone());
        if let Err(error) = self.context.notifier.send(event).await {
            self.finish_notification_claim(&key, &notification, false, Some(error.to_string()))?;
            warn!(error = %error, "failed to send close pnl telegram notification");
            return Ok(());
        }
        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::ExecutionEngine.as_str(),
            "close_pnl_sent",
            &serde_json::to_string(&notification)?,
        )?;
        self.finish_notification_claim(&key, &notification, true, None)?;
        Ok(())
    }

    async fn maybe_send_daily_report(&mut self) -> Result<()> {
        let now_local = Utc::now().with_timezone(&self.report_timezone);
        let today = now_local.date_naive();
        let scheduled = today
            .and_hms_opt(
                self.context.telegram.daily_report_hour,
                self.context.telegram.daily_report_minute,
                0,
            )
            .ok_or_else(|| anyhow!("invalid telegram daily report schedule"))?;
        if now_local.naive_local() < scheduled {
            return Ok(());
        }

        let today_key = today.format("%Y-%m-%d").to_string();
        if self.daily_report_cursor.last_report_date.as_deref() == Some(today_key.as_str()) {
            return Ok(());
        }

        let start_local = today
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid start of day"))?;
        let end_local = (today.succ_opt().unwrap_or(today))
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow!("invalid end of day"))?;
        let start = local_naive_to_utc(self.report_timezone, start_local)?;
        let end = local_naive_to_utc(self.report_timezone, end_local)?;

        let closes = self.context.store.audit_events_in_window(
            Some(self.context.domain),
            Some(ServiceKind::ExecutionEngine.as_str()),
            Some("close_pnl_sent"),
            start,
            end,
            10_000,
        )?;
        let risks = self.context.store.audit_events_in_window(
            Some(self.context.domain),
            Some(ServiceKind::RiskEngine.as_str()),
            Some("severe_risk_alert_sent"),
            start,
            end,
            10_000,
        )?;

        let mut close_count = 0usize;
        let mut win_count = 0usize;
        let mut gross_realized_pnl = 0.0;
        let mut total_fees = 0.0;
        let mut net_realized_pnl = 0.0;
        let mut best_trade_pnl = f64::NEG_INFINITY;
        let mut worst_trade_pnl = f64::INFINITY;
        for event in closes {
            let item: ClosePnlNotification = serde_json::from_str(&event.detail)
                .context("failed to decode close pnl audit event")?;
            close_count += 1;
            if item.net_realized_pnl > 0.0 {
                win_count += 1;
            }
            gross_realized_pnl += item.gross_realized_pnl;
            total_fees += item.total_fees;
            net_realized_pnl += item.net_realized_pnl;
            best_trade_pnl = best_trade_pnl.max(item.net_realized_pnl);
            worst_trade_pnl = worst_trade_pnl.min(item.net_realized_pnl);
        }
        if close_count == 0 {
            best_trade_pnl = 0.0;
            worst_trade_pnl = 0.0;
        }

        let notification = DailyReportNotification {
            domain: self.context.domain,
            report_date: today_key.clone(),
            timezone: self.context.telegram.timezone.clone(),
            close_count,
            win_count,
            gross_realized_pnl,
            total_fees,
            net_realized_pnl,
            best_trade_pnl,
            worst_trade_pnl,
            severe_risk_event_count: risks.len(),
            generated_at: now(),
        };
        let key = format!("daily-report:{}", today_key);
        if !self.claim_notification(&key, &notification)? {
            return Ok(());
        }
        if let Err(error) = self
            .context
            .notifier
            .send(NotificationEvent::DailyReport(notification.clone()))
            .await
        {
            self.finish_notification_claim(&key, &notification, false, Some(error.to_string()))?;
            warn!(error = %error, "failed to send daily telegram report");
            return Ok(());
        }
        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::ExecutionEngine.as_str(),
            "daily_report_sent",
            &serde_json::to_string(&notification)?,
        )?;
        self.finish_notification_claim(&key, &notification, true, None)?;
        self.daily_report_cursor.last_report_date = Some(today_key);
        self.store_daily_report_cursor()?;
        Ok(())
    }

    fn claim_notification<T: Serialize>(&self, key: &str, payload: &T) -> Result<bool> {
        let request_hash = hex_bytes(
            &Sha256::digest(serde_json::to_vec(payload).unwrap_or_default()),
        );
        Ok(match self.context.store.claim_idempotency_key(NewIdempotencyKey {
            domain: self.context.domain,
            scope: "telegram.notification".to_owned(),
            key: key.to_owned(),
            request_hash,
            created_by: self.operator.clone(),
            created_at: now(),
            lock_expires_at: Some(now() + chrono::Duration::seconds(30)),
        })? {
            IdempotencyClaimResult::Claimed(_) => true,
            IdempotencyClaimResult::DuplicateCompleted(_)
            | IdempotencyClaimResult::DuplicateInFlight(_) => false,
            IdempotencyClaimResult::HashMismatch(existing) => {
                return Err(anyhow!(
                    "notification idempotency hash mismatch for key `{}` status={}",
                    existing.key,
                    existing.status
                ));
            }
        })
    }

    fn finish_notification_claim<T: Serialize>(
        &self,
        key: &str,
        payload: &T,
        success: bool,
        error: Option<String>,
    ) -> Result<()> {
        self.context.store.mark_idempotency_key(
            self.context.domain,
            "telegram.notification",
            key,
            &json!({
                "success": success,
                "error": error,
                "payload": payload,
            }),
            success,
        )?;
        Ok(())
    }

    async fn process_operator_commands(&mut self) -> Result<()> {
        let cursor = self.load_command_cursor()?;
        let events = self.context.store.replay_events(
            self.context.domain,
            polymarket_core::ReplayCursor {
                after_sequence: Some(cursor.last_sequence),
                limit: 100,
            },
        )?;
        let mut next_sequence = cursor.last_sequence;
        for event in events {
            next_sequence = next_sequence.max(event.sequence);
            if event.stream != "execution.commands" || event.aggregate_type != "execution_command" {
                continue;
            }
            let command: ExecutionCommand = serde_json::from_value(event.payload.clone())?;
            self.handle_command(command).await?;
        }
        self.store_command_cursor(next_sequence)?;
        Ok(())
    }

    async fn handle_command(&mut self, command: ExecutionCommand) -> Result<()> {
        match command.kind {
            ExecutionCommandKind::CancelAll => {
                self.venue
                    .cancel_all(command.domain)
                    .await
                    .map_err(|error| anyhow!(error.to_string()))?;
                self.raise_alert("operator requested cancel_all")?;
            }
            ExecutionCommandKind::Recover => {
                self.restore_recovery_state().await?;
            }
            ExecutionCommandKind::Reconcile => {
                self.perform_reconcile().await?;
            }
            ExecutionCommandKind::CancelOrder => {
                if let Some(order_id) = command.order_id.as_deref() {
                    if let Some(order) = self
                        .context
                        .store
                        .order_lifecycle(command.domain, order_id)?
                    {
                        self.venue
                            .cancel_order(command.domain, &order)
                            .await
                            .map_err(|error| anyhow!(error.to_string()))?;
                    }
                }
            }
            ExecutionCommandKind::CancelMarket => {
                if let Some(market_id) = command.market_id.as_deref() {
                    self.venue
                        .cancel_market(command.domain, market_id)
                        .await
                        .map_err(|error| anyhow!(error.to_string()))?;
                }
            }
            ExecutionCommandKind::Submit | ExecutionCommandKind::Heartbeat => {}
        }
        Ok(())
    }

    async fn handle_shutdown_cancel(&mut self) -> Result<()> {
        self.venue
            .cancel_all(self.context.domain)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::ExecutionEngine.as_str(),
            "shutdown_cancel_all",
            "emergency cancel_all triggered on shutdown",
        )?;
        Ok(())
    }

    fn publish_lifecycle(
        &self,
        kind: ExecutionEventKind,
        record: &ExecutionIntentRecord,
        detail: &str,
    ) -> Result<()> {
        let event = ExecutionEvent {
            event_id: Uuid::new_v4(),
            domain: record.domain,
            kind,
            intent_id: Some(record.intent_id),
            order_id: None,
            market_id: Some(record.market_id.clone()),
            client_order_id: Some(record.client_order_id.clone()),
            detail: detail.to_owned(),
            payload: record.detail.clone(),
            occurred_at: now(),
        };
        self.context.bus.publish(
            ServiceKind::ExecutionEngine,
            self.context.domain,
            TOPIC_EXECUTION_LIFECYCLE,
            serde_json::to_string(&event)?,
        );
        Ok(())
    }

    fn raise_alert(&self, detail: &str) -> Result<()> {
        self.context.bus.publish(
            ServiceKind::ExecutionEngine,
            self.context.domain,
            TOPIC_EXECUTION_ALERT,
            json!({ "detail": detail, "domain": self.context.domain }).to_string(),
        );
        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::ExecutionEngine.as_str(),
            "alert",
            detail,
        )?;
        Ok(())
    }

    fn load_command_cursor(&self) -> Result<CommandCursor> {
        let snapshot = self.context.store.latest_snapshot(
            self.context.domain,
            "execution_command_cursor",
            "latest",
        )?;
        Ok(match snapshot {
            Some(snapshot) => serde_json::from_value(snapshot.payload)
                .context("failed to decode execution command cursor")?,
            None => CommandCursor {
                last_sequence: 0,
                updated_at: now(),
            },
        })
    }

    fn store_command_cursor(&self, last_sequence: i64) -> Result<()> {
        let cursor = CommandCursor {
            last_sequence,
            updated_at: now(),
        };
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: "execution_command_cursor".to_owned(),
            aggregate_id: "latest".to_owned(),
            version: last_sequence,
            payload: serde_json::to_value(cursor)?,
            derived_from_sequence: Some(last_sequence),
            created_at: now(),
        })?;
        Ok(())
    }

    fn load_approved_cursor(&self) -> Result<ApprovedIntentCursor> {
        let snapshot = self.context.store.latest_snapshot(
            self.context.domain,
            "execution_approved_cursor",
            "latest",
        )?;
        Ok(match snapshot {
            Some(snapshot) => serde_json::from_value(snapshot.payload)
                .context("failed to decode execution approved cursor")?,
            None => ApprovedIntentCursor {
                last_snapshot_version: -1,
                updated_at: now(),
            },
        })
    }

    fn store_approved_cursor(&self, last_snapshot_version: i64) -> Result<()> {
        let cursor = ApprovedIntentCursor {
            last_snapshot_version,
            updated_at: now(),
        };
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: "execution_approved_cursor".to_owned(),
            aggregate_id: "latest".to_owned(),
            version: last_snapshot_version,
            payload: serde_json::to_value(cursor)?,
            derived_from_sequence: Some(last_snapshot_version),
            created_at: now(),
        })?;
        Ok(())
    }
}

pub async fn run_execution_engine(
    context: ServiceContext,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = ExecutionEngineConfig::from_env()?;
    let venue: Arc<dyn ExecutionVenue> = if context.domain == AccountDomain::Sim {
        Arc::new(MockExecutionVenue::default())
    } else {
        Arc::new(PolymarketClobVenue::new(&context.domain_config, &config)?)
    };

    context.audit.record(
        Some(context.domain),
        ServiceKind::ExecutionEngine.as_str(),
        "execution_engine_configured",
        &format!(
            "api_base_url={} heartbeat_ms={} reconcile_ms={} max_concurrent={}",
            config.api_base_url,
            config.heartbeat_interval.as_millis(),
            config.reconcile_interval.as_millis(),
            config.max_concurrent_intents
        ),
    )?;

    ExecutionEngineService::new(context, config, venue)?
        .run(cancellation)
        .await
}

#[derive(Clone)]
struct PolymarketClobVenue {
    http: reqwest::Client,
    base_url: String,
    l2_credentials: ExecutionVenueCredentials,
    signer_secret: String,
    wallet_id: Option<String>,
}

impl PolymarketClobVenue {
    fn new(domain: &DomainConfig, config: &ExecutionEngineConfig) -> Result<Self> {
        let l2_credentials = parse_credentials(&domain.l2_credentials, domain.wallet_id.clone())?;
        let signer_secret = domain
            .signer
            .resolve_string()?
            .ok_or_else(|| anyhow!("signer credentials are required for real execution"))?
            .trim()
            .to_owned();
        let http = reqwest::Client::builder()
            .timeout(config.submit_timeout)
            .build()
            .context("failed to build reqwest client")?;
        Ok(Self {
            http,
            base_url: config.api_base_url.trim_end_matches('/').to_owned(),
            l2_credentials,
            signer_secret,
            wallet_id: domain.wallet_id.clone(),
        })
    }

    async fn signed_request(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Value,
    ) -> std::result::Result<Value, ExecutionError> {
        let body_string = body.to_string();
        let timestamp = now().timestamp_millis().to_string();
        let signature_payload = format!("{}{}{}{}", method.as_str(), path, timestamp, body_string);
        let mut mac = Hmac::<Sha256>::new_from_slice(self.signer_secret.as_bytes())
            .map_err(|error| ExecutionError::Signing(error.to_string()))?;
        mac.update(signature_payload.as_bytes());
        let signature =
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes());
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        insert_header(
            &mut headers,
            "POLY_ADDRESS",
            self.wallet_id
                .as_deref()
                .or(self.l2_credentials.wallet.as_deref()),
        );
        insert_header(&mut headers, "POLY_API_KEY", Some(&self.l2_credentials.key));
        insert_header(
            &mut headers,
            "POLY_PASSPHRASE",
            self.l2_credentials.passphrase.as_deref(),
        );
        insert_header(&mut headers, "POLY_SIGNATURE", Some(&signature));
        insert_header(&mut headers, "POLY_TIMESTAMP", Some(&timestamp));

        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .request(method, url)
            .headers(headers)
            .body(body_string)
            .send()
            .await
            .map_err(|error| ExecutionError::Network(error.to_string()))?;
        let status = response.status();
        let text = response
            .text()
            .await
            .map_err(|error| ExecutionError::Network(error.to_string()))?;
        if status.as_u16() == 425 {
            return Err(ExecutionError::VenueRestart(text));
        }
        if !status.is_success() {
            return Err(ExecutionError::VenueRejected(format!(
                "status={} body={}",
                status, text
            )));
        }
        serde_json::from_str(&text).map_err(|error| ExecutionError::Network(error.to_string()))
    }
}

#[async_trait]
impl ExecutionVenue for PolymarketClobVenue {
    async fn submit_batch(
        &self,
        _domain: AccountDomain,
        intents: &[ExecutionIntentRecord],
    ) -> std::result::Result<Vec<VenueSubmitAck>, ExecutionError> {
        let mut acks = Vec::with_capacity(intents.len());
        for intent in intents {
            let response = self
                .signed_request(
                    reqwest::Method::POST,
                    "/orders",
                    json!({
                        "market": intent.market_id,
                        "token_id": intent.token_id,
                        "side": intent.side,
                        "price": intent.limit_price,
                        "size": intent.target_size,
                        "client_order_id": intent.client_order_id,
                    }),
                )
                .await?;
            acks.push(VenueSubmitAck {
                order_id: response
                    .get("order_id")
                    .and_then(Value::as_str)
                    .unwrap_or(&intent.client_order_id)
                    .to_owned(),
                client_order_id: intent.client_order_id.clone(),
                external_order_id: response
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                status: OrderLifecycleStatus::Submitted,
                detail: response,
            });
        }
        Ok(acks)
    }

    async fn cancel_order(
        &self,
        _domain: AccountDomain,
        order: &OrderLifecycleRecord,
    ) -> std::result::Result<(), ExecutionError> {
        let _ = self
            .signed_request(
                reqwest::Method::POST,
                "/orders/cancel",
                json!({
                    "order_id": order.external_order_id.clone().unwrap_or_else(|| order.order_id.clone()),
                    "client_order_id": order.client_order_id,
                }),
            )
            .await?;
        Ok(())
    }

    async fn cancel_market(
        &self,
        _domain: AccountDomain,
        market_id: &str,
    ) -> std::result::Result<(), ExecutionError> {
        let _ = self
            .signed_request(
                reqwest::Method::POST,
                "/orders/cancel-market",
                json!({ "market_id": market_id }),
            )
            .await?;
        Ok(())
    }

    async fn cancel_all(&self, _domain: AccountDomain) -> std::result::Result<(), ExecutionError> {
        let _ = self
            .signed_request(reqwest::Method::POST, "/orders/cancel-all", json!({}))
            .await?;
        Ok(())
    }

    async fn heartbeat(
        &self,
        _domain: AccountDomain,
    ) -> std::result::Result<VenueHeartbeat, ExecutionError> {
        let response = self
            .signed_request(reqwest::Method::GET, "/health", json!({}))
            .await?;
        Ok(VenueHeartbeat {
            venue_healthy: true,
            signer_healthy: true,
            detail: response.to_string(),
        })
    }

    async fn reconcile(
        &self,
        _domain: AccountDomain,
        orders: &[OrderLifecycleRecord],
    ) -> std::result::Result<Vec<VenueOrderState>, ExecutionError> {
        let response = self
            .signed_request(
                reqwest::Method::POST,
                "/orders/reconcile",
                json!({
                    "orders": orders.iter().map(|order| json!({
                        "order_id": order.external_order_id.clone().unwrap_or_else(|| order.order_id.clone()),
                        "client_order_id": order.client_order_id,
                        "market_id": order.market_id,
                    })).collect::<Vec<_>>()
                }),
            )
            .await?;
        let items = response
            .get("orders")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut states = Vec::with_capacity(items.len());
        for item in items {
            states.push(VenueOrderState {
                order_id: item
                    .get("order_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                client_order_id: item
                    .get("client_order_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                market_id: item
                    .get("market_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                status: item
                    .get("status")
                    .and_then(Value::as_str)
                    .and_then(|status| parse_remote_order_status(status).ok())
                    .unwrap_or(OrderLifecycleStatus::Acknowledged),
                filled_quantity: item
                    .get("filled_quantity")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0),
                average_fill_price: item.get("average_fill_price").and_then(Value::as_f64),
                detail: item,
            });
        }
        Ok(states)
    }
}

#[derive(Default)]
struct MockExecutionVenue;

#[async_trait]
impl ExecutionVenue for MockExecutionVenue {
    async fn submit_batch(
        &self,
        _domain: AccountDomain,
        intents: &[ExecutionIntentRecord],
    ) -> std::result::Result<Vec<VenueSubmitAck>, ExecutionError> {
        Ok(intents
            .iter()
            .map(|intent| VenueSubmitAck {
                order_id: format!("sim-{}", intent.client_order_id),
                client_order_id: intent.client_order_id.clone(),
                external_order_id: None,
                status: OrderLifecycleStatus::Acknowledged,
                detail: json!({"simulated": true}),
            })
            .collect())
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
        Ok(VenueHeartbeat {
            venue_healthy: true,
            signer_healthy: true,
            detail: "simulated venue healthy".to_owned(),
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
                order_id: order.order_id.clone(),
                client_order_id: order.client_order_id.clone(),
                market_id: order.market_id.clone(),
                status: order.status,
                filled_quantity: order.filled_quantity,
                average_fill_price: order.average_fill_price,
                detail: json!({"simulated": true}),
            })
            .collect())
    }
}

fn validate_intent(context: &ServiceContext, intent: &TradeIntent) -> Result<()> {
    if intent.account_domain != context.domain {
        return Err(anyhow!(
            "trade intent domain `{}` does not match execution domain `{}`",
            intent.account_domain,
            context.domain
        ));
    }
    if intent.max_size <= 0.0 {
        return Err(anyhow!("trade intent size must be positive"));
    }
    if !(0.0..=1.0).contains(&intent.limit_price) {
        return Err(anyhow!("trade intent price must be within [0,1]"));
    }
    if intent.expires_at <= now() {
        return Err(anyhow!("trade intent already expired"));
    }
    if context.domain_config.runtime_mode == polymarket_core::RuntimeMode::Disabled {
        return Err(anyhow!("execution engine is disabled for this domain"));
    }
    if context.domain == AccountDomain::Live
        && context.domain_config.runtime_mode != polymarket_core::RuntimeMode::Execute
    {
        return Err(anyhow!("live execution requires EXECUTE runtime mode"));
    }
    Ok(())
}

fn parse_trade_intent_batch(payload: &str) -> Result<TradeIntentBatch> {
    if let Ok(approved) = serde_json::from_str::<ApprovedTradeIntentBatch>(payload) {
        return Ok(approved.batch);
    }
    serde_json::from_str::<TradeIntentBatch>(payload)
        .context("failed to decode approved trade intent batch")
}

fn build_idempotency_key(intent: &TradeIntent) -> String {
    format!(
        "{}:{}:{}:{}:{}",
        intent.account_domain,
        intent.opportunity_id,
        intent.market_id,
        intent.token_id,
        intent.side.as_str()
    )
}

fn build_client_order_id(intent: &TradeIntent) -> String {
    let raw = format!(
        "{}-{}-{}-{}",
        intent.account_domain.namespace(),
        intent.market_id,
        intent.token_id,
        intent.opportunity_id.simple()
    );
    raw.chars().take(64).collect()
}

fn hash_trade_intent(intent: &TradeIntent, idempotency_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(intent).unwrap_or_default());
    hasher.update(idempotency_key.as_bytes());
    hex_bytes(&hasher.finalize())
}

fn map_order_status_to_intent_status(status: OrderLifecycleStatus) -> ExecutionIntentStatus {
    match status {
        OrderLifecycleStatus::Created => ExecutionIntentStatus::PendingValidation,
        OrderLifecycleStatus::Submitted | OrderLifecycleStatus::Acknowledged => {
            ExecutionIntentStatus::Submitted
        }
        OrderLifecycleStatus::PartiallyFilled => ExecutionIntentStatus::PartiallyFilled,
        OrderLifecycleStatus::Filled | OrderLifecycleStatus::Settled => {
            ExecutionIntentStatus::Filled
        }
        OrderLifecycleStatus::CancelRequested => ExecutionIntentStatus::CancelRequested,
        OrderLifecycleStatus::Cancelled | OrderLifecycleStatus::Expired => {
            ExecutionIntentStatus::Cancelled
        }
        OrderLifecycleStatus::Rejected => ExecutionIntentStatus::FailedTerminal,
    }
}

fn parse_credentials(
    source: &CredentialSource,
    wallet_id: Option<String>,
) -> Result<ExecutionVenueCredentials> {
    let raw = source
        .resolve_string()?
        .ok_or_else(|| anyhow!("l2 credentials are required"))?;
    if let Ok(parsed) = serde_json::from_str::<ExecutionVenueCredentials>(raw.trim()) {
        return Ok(parsed);
    }
    let parts: Vec<_> = raw.trim().split(':').collect();
    if parts.len() >= 2 {
        return Ok(ExecutionVenueCredentials {
            key: parts[0].to_owned(),
            secret: parts[1].to_owned(),
            passphrase: parts.get(2).map(|value| (*value).to_owned()),
            wallet: wallet_id,
        });
    }
    Err(anyhow!("unsupported credential format"))
}

fn insert_header(headers: &mut HeaderMap, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(key.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            headers.insert(name, value);
        }
    }
}

fn parse_remote_order_status(
    status: &str,
) -> Result<OrderLifecycleStatus, polymarket_core::ParseEnumError> {
    match status.to_ascii_uppercase().as_str() {
        "LIVE" | "OPEN" | "MATCHED" => Ok(OrderLifecycleStatus::Acknowledged),
        "SUBMITTED" => Ok(OrderLifecycleStatus::Submitted),
        "PARTIALLY_FILLED" => Ok(OrderLifecycleStatus::PartiallyFilled),
        "FILLED" => Ok(OrderLifecycleStatus::Filled),
        "CANCELED" | "CANCELLED" => Ok(OrderLifecycleStatus::Cancelled),
        "REJECTED" => Ok(OrderLifecycleStatus::Rejected),
        "EXPIRED" => Ok(OrderLifecycleStatus::Expired),
        other => OrderLifecycleStatus::from_str(other),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn instrument_key(market_id: &str, token_id: &str) -> String {
    format!("{market_id}::{token_id}")
}

fn incremental_fill_price(
    previous_filled: f64,
    previous_average_fill_price: Option<f64>,
    current_filled: f64,
    current_average_fill_price: Option<f64>,
) -> Option<f64> {
    let delta_quantity = current_filled - previous_filled;
    if delta_quantity <= f64::EPSILON {
        return None;
    }
    match (previous_average_fill_price, current_average_fill_price) {
        (_, Some(current_average)) if previous_filled <= f64::EPSILON => Some(current_average),
        (Some(previous_average), Some(current_average)) => {
            let total_notional_before = previous_filled * previous_average;
            let total_notional_after = current_filled * current_average;
            Some((total_notional_after - total_notional_before) / delta_quantity)
        }
        _ => current_average_fill_price,
    }
}

fn estimate_fill_fee_from_transition(
    previous_detail: Option<&Value>,
    previous_filled: f64,
    current_detail: Option<&Value>,
    current_filled: f64,
    current_average_fill_price: f64,
    fallback_fee_bps: u32,
) -> f64 {
    let previous_total_fee = previous_detail.and_then(extract_total_fee);
    let current_total_fee = current_detail.and_then(extract_total_fee);
    if let Some(current_total_fee) = current_total_fee {
        if let Some(previous_total_fee) = previous_total_fee {
            let delta = current_total_fee - previous_total_fee;
            if delta >= 0.0 {
                return delta;
            }
        }
        if previous_filled <= f64::EPSILON {
            return current_total_fee.max(0.0);
        }
    }
    let delta_quantity = (current_filled - previous_filled).max(0.0);
    delta_quantity * current_average_fill_price * f64::from(fallback_fee_bps) / 10_000.0
}

fn extract_total_fee(detail: &Value) -> Option<f64> {
    const CANDIDATE_PATHS: [&[&str]; 7] = [
        &["fees_paid"],
        &["fee_paid"],
        &["fee"],
        &["total_fee"],
        &["fees", "total"],
        &["fee_amount"],
        &["detail", "fees_paid"],
    ];
    for path in CANDIDATE_PATHS {
        let mut current = detail;
        let mut found = true;
        for key in path {
            match current.get(*key) {
                Some(next) => current = next,
                None => {
                    found = false;
                    break;
                }
            }
        }
        if found {
            if let Some(value) = current.as_f64() {
                return Some(value);
            }
            if let Some(value) = current.as_str().and_then(|item| item.parse::<f64>().ok()) {
                return Some(value);
            }
        }
    }
    None
}

fn local_naive_to_utc(timezone: Tz, local: chrono::NaiveDateTime) -> Result<DateTime<Utc>> {
    timezone
        .from_local_datetime(&local)
        .single()
        .map(|value| value.with_timezone(&Utc))
        .ok_or_else(|| anyhow!("failed to resolve local datetime `{local}` in timezone `{timezone}`"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_msgbus::MessageBus;
    use polymarket_storage::Store;
    use polymarket_core::{IntentPolicy, OptimizationStatus, RuntimeMode, StrategyKind};
    use std::time::Duration;

    fn sample_intent(domain: AccountDomain) -> TradeIntent {
        TradeIntent {
            account_domain: domain,
            market_id: "mkt-1".to_owned(),
            token_id: "yes".to_owned(),
            side: TradeSide::Buy,
            limit_price: 0.43,
            max_size: 10.0,
            policy: IntentPolicy::Passive,
            expires_at: now() + chrono::Duration::seconds(30),
            strategy_kind: StrategyKind::DependencyArb,
            thesis_ref: "thesis".to_owned(),
            opportunity_id: Uuid::new_v4(),
            event_id: "evt-1".to_owned(),
        }
    }

    #[test]
    fn idempotency_key_is_deterministic() {
        let intent = sample_intent(AccountDomain::Canary);
        assert_eq!(
            build_idempotency_key(&intent),
            build_idempotency_key(&intent)
        );
    }

    #[test]
    fn approved_batch_payload_can_be_parsed() {
        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Canary,
            created_at: now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![sample_intent(AccountDomain::Canary)],
        };
        let parsed =
            parse_trade_intent_batch(&serde_json::to_string(&batch).expect("json")).expect("parse");
        assert_eq!(parsed.intents.len(), 1);
    }

    #[test]
    fn live_runtime_requires_execute() {
        let domain = AccountDomain::Live;
        let telegram = polymarket_config::TelegramConfig::from_map(&BTreeMap::new())
            .expect("telegram config");
        let notifier = crate::TelegramNotifier::from_config(&telegram).expect("notifier");
        let context = ServiceContext {
            domain,
            domain_config: DomainConfig {
                descriptor: domain.descriptor(),
                data_dir: std::path::PathBuf::from("./var/live"),
                database_path: std::path::PathBuf::from("./var/live/live.sqlite"),
                audit_prefix: "domain.live".to_owned(),
                wallet_id: Some("wallet".to_owned()),
                credential_alias: Some("alias".to_owned()),
                l2_credentials: CredentialSource::EnvVar {
                    variable: "LIVE_L2".to_owned(),
                },
                signer: CredentialSource::EnvVar {
                    variable: "LIVE_SIGNER".to_owned(),
                },
                execution_approved: true,
                runtime_mode: RuntimeMode::Observe,
            },
            telegram,
            store: Store::new("./var/live/live.sqlite", domain, "live", "domain.live"),
            bus: polymarket_msgbus::MessageBus::new(16),
            audit: Arc::new(polymarket_audit::StorageAuditSink::new(Store::new(
                "./var/live/live.sqlite",
                domain,
                "live",
                "domain.live",
            ))),
            notifier: Arc::new(notifier),
            heartbeat_interval: Duration::from_secs(1),
        };
        let error = validate_intent(&context, &sample_intent(domain)).expect_err("must fail");
        assert!(error.to_string().contains("EXECUTE"));
    }

    fn test_execution_service() -> ExecutionEngineService {
        let db_path = std::env::temp_dir().join(format!("execution-{}.sqlite", Uuid::new_v4()));
        let store = Store::new(db_path.clone(), AccountDomain::Sim, "sim", "domain.sim");
        store.init().expect("store init");
        let telegram = polymarket_config::TelegramConfig::from_map(&BTreeMap::new())
            .expect("telegram config");
        let notifier = crate::TelegramNotifier::from_config(&telegram).expect("notifier");
        let context = ServiceContext {
            domain: AccountDomain::Sim,
            domain_config: polymarket_config::NodeConfig::from_env(AccountDomain::Sim)
                .expect("node config")
                .selected_domain_config,
            telegram,
            store: store.clone(),
            bus: MessageBus::new(16),
            audit: Arc::new(polymarket_audit::StorageAuditSink::new(store)),
            notifier: Arc::new(notifier),
            heartbeat_interval: Duration::from_secs(1),
        };
        let mut config_vars = BTreeMap::new();
        config_vars.insert(
            "POLYMARKET_TELEGRAM_TIMEZONE".to_owned(),
            "Asia/Shanghai".to_owned(),
        );
        ExecutionEngineService::new(
            context,
            ExecutionEngineConfig::from_map(&config_vars).expect("execution config"),
            Arc::new(MockExecutionVenue),
        )
        .expect("service")
    }

    fn execution_record(side: TradeSide) -> ExecutionIntentRecord {
        ExecutionIntentRecord {
            intent_id: Uuid::new_v4(),
            domain: AccountDomain::Sim,
            batch_id: None,
            strategy_kind: StrategyKind::DependencyArb,
            market_id: "mkt-1".to_owned(),
            token_id: "yes".to_owned(),
            side,
            limit_price: 0.50,
            target_size: 10.0,
            idempotency_key: Uuid::new_v4().to_string(),
            client_order_id: Uuid::new_v4().to_string(),
            status: ExecutionIntentStatus::Submitted,
            detail: json!({ "event_id": "evt-1" }),
            created_at: now(),
            updated_at: now(),
            expires_at: now() + chrono::Duration::minutes(5),
        }
    }

    #[test]
    fn partial_close_allocates_entry_and_exit_fees() {
        let mut service = test_execution_service();
        let buy = execution_record(TradeSide::Buy);
        let sell = execution_record(TradeSide::Sell);

        assert!(service
            .apply_fill_to_position_book(&buy, 10.0, 0.40, 0.20, now())
            .is_empty());
        let notifications = service.apply_fill_to_position_book(&sell, 4.0, 0.55, 0.08, now());

        assert_eq!(notifications.len(), 1);
        let item = &notifications[0];
        assert_eq!(item.closed_quantity, 4.0);
        assert!((item.entry_fees_allocated - 0.08).abs() < 1e-9);
        assert!((item.exit_fees - 0.08).abs() < 1e-9);
        assert!((item.gross_realized_pnl - 0.60).abs() < 1e-9);
        assert!((item.net_realized_pnl - 0.44).abs() < 1e-9);
    }

    #[test]
    fn single_close_aggregates_multiple_fifo_lots() {
        let mut service = test_execution_service();
        let buy = execution_record(TradeSide::Buy);
        let sell = execution_record(TradeSide::Sell);

        assert!(service
            .apply_fill_to_position_book(&buy, 3.0, 0.30, 0.03, now())
            .is_empty());
        assert!(service
            .apply_fill_to_position_book(&buy, 2.0, 0.45, 0.02, now())
            .is_empty());
        let notifications = service.apply_fill_to_position_book(&sell, 4.0, 0.60, 0.08, now());

        assert_eq!(notifications.len(), 1);
        let item = &notifications[0];
        assert_eq!(item.closed_quantity, 4.0);
        assert!((item.entry_average_price - 0.3375).abs() < 1e-9);
        assert!((item.gross_realized_pnl - 1.05).abs() < 1e-9);
        assert!((item.total_fees - 0.12).abs() < 1e-9);
        assert!((item.net_realized_pnl - 0.93).abs() < 1e-9);
    }
}
