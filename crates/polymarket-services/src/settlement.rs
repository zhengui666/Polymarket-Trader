use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use polymarket_core::{
    now, AccountDomain, ExecutionIntentRecord, ExecutionIntentStatus, NewDurableEvent,
    NewOrderLifecycleRecord, NewStateSnapshot, OrderLifecycleRecord, OrderLifecycleStatus,
    ServiceKind, SettlementCashRecovery, SettlementCommand, SettlementDiscrepancy,
    SettlementEngineReport, SettlementMarketState, SettlementPhase, SettlementPositionSnapshot,
    SettlementPositionState, SettlementTask, SettlementTaskKind, SettlementTaskStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::select;
use tokio::time::{interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

use crate::{publish_heartbeat, ServiceContext};

pub const TOPIC_SETTLEMENT_REPORT: &str = "settlement.report";
pub const TOPIC_SETTLEMENT_TASK: &str = "settlement.task";
pub const TOPIC_SETTLEMENT_CASH: &str = "settlement.cash_recovery";

const SNAPSHOT_SETTLEMENT_COMMAND_CURSOR: &str = "settlement_command_cursor";
const SNAPSHOT_SETTLEMENT_MARKET: &str = "settlement_market_state";
const SNAPSHOT_SETTLEMENT_POSITION: &str = "settlement_position";
const SNAPSHOT_SETTLEMENT_TASK: &str = "settlement_task";
const SNAPSHOT_SETTLEMENT_CASH: &str = "settlement_cash_recovery";
const SNAPSHOT_SETTLEMENT_REPORT: &str = "settlement_report";

#[derive(Debug, Clone)]
pub struct SettlementConfig {
    pub reconcile_interval: Duration,
    pub stale_order_after: Duration,
}

impl SettlementConfig {
    pub fn from_env() -> Self {
        let reconcile_interval = env::var("POLYMARKET_SETTLEMENT_RECONCILE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(30));
        let stale_order_after = env::var("POLYMARKET_SETTLEMENT_STALE_ORDER_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(300));

        Self {
            reconcile_interval,
            stale_order_after,
        }
    }
}

#[derive(Clone)]
pub struct SettlementEngineService {
    context: ServiceContext,
    config: SettlementConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SettlementCommandCursor {
    last_sequence: i64,
    updated_at: polymarket_core::Timestamp,
}

#[derive(Debug, Clone)]
struct SettlementCycleOutcome {
    positions: Vec<SettlementPositionSnapshot>,
    tasks: Vec<SettlementTask>,
    discrepancies: Vec<SettlementDiscrepancy>,
    converged_orders: usize,
    recovered_cash: f64,
}

impl SettlementEngineService {
    pub fn new(context: ServiceContext, config: SettlementConfig) -> Self {
        Self { context, config }
    }

    pub async fn run(self, cancellation: CancellationToken) -> Result<()> {
        let mut heartbeat = interval(self.context.heartbeat_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut reconcile = interval(self.config.reconcile_interval);
        reconcile.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            select! {
                _ = cancellation.cancelled() => {
                    self.context.audit.record(
                        Some(self.context.domain),
                        ServiceKind::SettlementEngine.as_str(),
                        "service_stopped",
                        "cancellation requested",
                    )?;
                    return Ok(());
                }
                _ = heartbeat.tick() => {
                    publish_heartbeat(&self.context, ServiceKind::SettlementEngine, "settlement loop ready");
                }
                _ = reconcile.tick() => {
                    if let Err(error) = self.run_cycle().await {
                        warn!(error = %error, "settlement cycle failed");
                        self.context.audit.record(
                            Some(self.context.domain),
                            ServiceKind::SettlementEngine.as_str(),
                            "settlement_cycle_failed",
                            &error.to_string(),
                        )?;
                    }
                }
            }
        }
    }

    async fn run_cycle(&self) -> Result<()> {
        self.process_commands()?;

        let market_states = self.load_market_states()?;
        let intents = self
            .context
            .store
            .list_all_execution_intents(self.context.domain)?;
        let orders = self
            .context
            .store
            .list_all_order_lifecycle(self.context.domain)?;

        let positions =
            materialize_positions(self.context.domain, &market_states, &intents, &orders);
        let (discrepancies, converged_orders) =
            self.reconcile_pending_orders(&market_states, &intents, &orders)?;
        let (tasks, tasks_closed) =
            self.materialize_tasks(&market_states, &positions, &discrepancies)?;
        let recovered_cash = self.persist_cash_recovery(&market_states, &positions)?;

        self.persist_positions(&positions)?;
        self.persist_tasks(&tasks)?;

        let report = SettlementEngineReport {
            domain: self.context.domain,
            generated_at: now(),
            positions_updated: positions.len(),
            tasks_open: tasks
                .iter()
                .filter(|task| task.status != SettlementTaskStatus::Completed)
                .count(),
            tasks_closed,
            discrepancies: discrepancies.len(),
            converged_orders,
            recovered_cash,
        };
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: SNAPSHOT_SETTLEMENT_REPORT.to_owned(),
            aggregate_id: "latest".to_owned(),
            version: report.generated_at.timestamp_millis(),
            payload: serde_json::to_value(&report)?,
            derived_from_sequence: None,
            created_at: report.generated_at,
        })?;
        self.context.bus.publish(
            ServiceKind::SettlementEngine,
            self.context.domain,
            TOPIC_SETTLEMENT_REPORT,
            serde_json::to_string(&report)?,
        );

        self.context.audit.record(
            Some(self.context.domain),
            ServiceKind::SettlementEngine.as_str(),
            "settlement_cycle_completed",
            &format!(
                "positions={} tasks={} discrepancies={} converged_orders={} recovered_cash={:.4}",
                positions.len(),
                tasks.len(),
                discrepancies.len(),
                converged_orders,
                recovered_cash
            ),
        )?;
        Ok(())
    }

    fn process_commands(&self) -> Result<()> {
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
            if event.stream != "settlement.commands" || event.aggregate_type != "settlement_command"
            {
                continue;
            }

            let command: SettlementCommand = serde_json::from_value(event.payload.clone())
                .context("failed to decode settlement command")?;
            match command {
                SettlementCommand::UpdateMarketState {
                    market_id,
                    event_id,
                    phase,
                    winning_price,
                    finalizes_at,
                    detail,
                } => {
                    let state = SettlementMarketState {
                        domain: self.context.domain,
                        market_id: market_id.clone(),
                        event_id,
                        phase,
                        winning_price,
                        disputed: phase == SettlementPhase::Disputed,
                        finalizes_at,
                        updated_at: event.created_at,
                        detail,
                    };
                    self.context.store.upsert_snapshot(NewStateSnapshot {
                        domain: self.context.domain,
                        aggregate_type: SNAPSHOT_SETTLEMENT_MARKET.to_owned(),
                        aggregate_id: market_id,
                        version: event.sequence,
                        payload: serde_json::to_value(state)?,
                        derived_from_sequence: Some(event.sequence),
                        created_at: event.created_at,
                    })?;
                }
                SettlementCommand::ConvergeOrder {
                    order_id,
                    status,
                    detail,
                } => {
                    if let Some(order) = self
                        .context
                        .store
                        .order_lifecycle(self.context.domain, &order_id)?
                    {
                        self.context
                            .store
                            .upsert_order_lifecycle(NewOrderLifecycleRecord {
                                domain: self.context.domain,
                                order_id: order.order_id,
                                market_id: order.market_id,
                                status: status.unwrap_or(order.status),
                                client_order_id: order.client_order_id,
                                external_order_id: order.external_order_id,
                                idempotency_key: order.idempotency_key,
                                side: order.side,
                                limit_price: order.limit_price,
                                order_quantity: order.order_quantity,
                                filled_quantity: order.filled_quantity,
                                average_fill_price: order.average_fill_price,
                                last_event_sequence: Some(event.sequence),
                                detail: merge_detail(
                                    order.detail,
                                    json!({ "settlement_note": detail }),
                                ),
                                opened_at: order.opened_at,
                                updated_at: event.created_at,
                                closed_at: if status.unwrap_or(order.status).is_terminal() {
                                    Some(event.created_at)
                                } else {
                                    order.closed_at
                                },
                            })?;
                    }
                }
                SettlementCommand::RecordRedemption {
                    market_id,
                    event_id,
                    amount,
                    detail,
                } => {
                    let recovery = SettlementCashRecovery {
                        domain: self.context.domain,
                        market_id: market_id.clone(),
                        event_id,
                        amount,
                        recovered_at: event.created_at,
                        detail,
                    };
                    self.context.store.upsert_snapshot(NewStateSnapshot {
                        domain: self.context.domain,
                        aggregate_type: SNAPSHOT_SETTLEMENT_CASH.to_owned(),
                        aggregate_id: market_id,
                        version: event.sequence,
                        payload: serde_json::to_value(&recovery)?,
                        derived_from_sequence: Some(event.sequence),
                        created_at: event.created_at,
                    })?;
                }
            }
        }

        self.store_command_cursor(next_sequence)
    }

    fn load_market_states(&self) -> Result<BTreeMap<String, SettlementMarketState>> {
        let snapshots = self
            .context
            .store
            .list_snapshots_by_type(self.context.domain, SNAPSHOT_SETTLEMENT_MARKET)?;
        let mut states = BTreeMap::new();
        for snapshot in snapshots {
            let state: SettlementMarketState = serde_json::from_value(snapshot.payload)
                .context("failed to decode settlement market state")?;
            states.insert(state.market_id.clone(), state);
        }
        Ok(states)
    }

    fn reconcile_pending_orders(
        &self,
        market_states: &BTreeMap<String, SettlementMarketState>,
        intents: &[ExecutionIntentRecord],
        orders: &[OrderLifecycleRecord],
    ) -> Result<(Vec<SettlementDiscrepancy>, usize)> {
        let now_ts = now();
        let mut discrepancies = Vec::new();
        let mut converged_orders = 0usize;

        let orders_by_client_id: BTreeMap<&str, &OrderLifecycleRecord> = orders
            .iter()
            .filter_map(|order| {
                order
                    .client_order_id
                    .as_deref()
                    .map(|client| (client, order))
            })
            .collect();

        for order in orders {
            if !order.status.is_terminal()
                && stale_for(order.updated_at, now_ts) >= self.config.stale_order_after
            {
                discrepancies.push(SettlementDiscrepancy {
                    domain: self.context.domain,
                    order_id: order.order_id.clone(),
                    market_id: order.market_id.clone(),
                    reason: "stale_open_order".to_owned(),
                    detail: format!(
                        "status={} age_secs={}",
                        order.status,
                        stale_for(order.updated_at, now_ts).as_secs()
                    ),
                    detected_at: now_ts,
                });
            }

            if let Some(state) = market_states.get(&order.market_id) {
                if matches!(
                    state.phase,
                    SettlementPhase::Finalized
                        | SettlementPhase::Redeeming
                        | SettlementPhase::Redeemed
                ) && order.filled_quantity > 0.0
                    && order.status != OrderLifecycleStatus::Settled
                {
                    self.context
                        .store
                        .upsert_order_lifecycle(NewOrderLifecycleRecord {
                            domain: order.domain,
                            order_id: order.order_id.clone(),
                            market_id: order.market_id.clone(),
                            status: OrderLifecycleStatus::Settled,
                            client_order_id: order.client_order_id.clone(),
                            external_order_id: order.external_order_id.clone(),
                            idempotency_key: order.idempotency_key.clone(),
                            side: order.side.clone(),
                            limit_price: order.limit_price,
                            order_quantity: order.order_quantity,
                            filled_quantity: order.filled_quantity,
                            average_fill_price: order.average_fill_price,
                            last_event_sequence: order.last_event_sequence,
                            detail: merge_detail(
                                order.detail.clone(),
                                json!({ "settlement_phase": state.phase }),
                            ),
                            opened_at: order.opened_at,
                            updated_at: now_ts,
                            closed_at: Some(now_ts),
                        })?;
                    converged_orders += 1;
                }
            }
        }

        for intent in intents {
            if intent.status.is_terminal() {
                continue;
            }

            match orders_by_client_id.get(intent.client_order_id.as_str()) {
                None => discrepancies.push(SettlementDiscrepancy {
                    domain: self.context.domain,
                    order_id: intent.client_order_id.clone(),
                    market_id: intent.market_id.clone(),
                    reason: "missing_order_lifecycle".to_owned(),
                    detail: "execution intent has no corresponding order lifecycle row".to_owned(),
                    detected_at: now_ts,
                }),
                Some(order) => {
                    let target_status = map_order_status_to_intent_status(order.status);
                    if target_status != intent.status {
                        self.context
                            .store
                            .upsert_execution_state(ExecutionIntentRecord {
                                status: target_status,
                                updated_at: now_ts,
                                detail: merge_detail(
                                    intent.detail.clone(),
                                    json!({
                                        "settlement_reconciled_from_order_status": order.status,
                                    }),
                                ),
                                ..intent.clone()
                            })?;
                        converged_orders += 1;
                    }
                }
            }
        }

        Ok((discrepancies, converged_orders))
    }

    fn materialize_tasks(
        &self,
        market_states: &BTreeMap<String, SettlementMarketState>,
        positions: &[SettlementPositionSnapshot],
        discrepancies: &[SettlementDiscrepancy],
    ) -> Result<(Vec<SettlementTask>, usize)> {
        let existing = self
            .context
            .store
            .list_snapshots_by_type(self.context.domain, SNAPSHOT_SETTLEMENT_TASK)?;
        let mut previous_ids = BTreeSet::new();
        for snapshot in existing {
            previous_ids.insert(snapshot.aggregate_id);
        }

        let now_ts = now();
        let mut tasks = Vec::new();
        let mut current_ids = BTreeSet::new();

        for state in market_states.values() {
            let exposure: f64 = positions
                .iter()
                .filter(|position| position.market_id == state.market_id)
                .map(|position| position.net_quantity.abs())
                .sum();
            if exposure <= 1e-9 {
                continue;
            }

            let base = match state.phase {
                SettlementPhase::Trading => None,
                SettlementPhase::Resolving => Some((
                    SettlementTaskKind::MonitorResolution,
                    SettlementTaskStatus::Pending,
                    "market resolving; inventory moved to settlement watch".to_owned(),
                    None,
                    None,
                )),
                SettlementPhase::Disputed => Some((
                    SettlementTaskKind::AwaitFinalization,
                    SettlementTaskStatus::Blocked,
                    "market disputed; capital remains quarantined".to_owned(),
                    Some("market dispute pending".to_owned()),
                    None,
                )),
                SettlementPhase::Finalized => Some((
                    SettlementTaskKind::RedeemInventory,
                    SettlementTaskStatus::Pending,
                    "market finalized; redemption can start".to_owned(),
                    None,
                    state.winning_price.map(|price| price * exposure),
                )),
                SettlementPhase::Redeeming => Some((
                    SettlementTaskKind::RedeemInventory,
                    SettlementTaskStatus::InProgress,
                    "redemption in progress".to_owned(),
                    None,
                    state.winning_price.map(|price| price * exposure),
                )),
                SettlementPhase::Redeemed => Some((
                    SettlementTaskKind::ReleaseCapital,
                    SettlementTaskStatus::Completed,
                    "redemption settled; capital released".to_owned(),
                    None,
                    state.winning_price.map(|price| price * exposure),
                )),
            };

            if let Some((kind, status, detail, blocked_reason, expected_cash_recovery)) = base {
                let task = build_task(
                    self.context.domain,
                    &state.market_id,
                    &state.event_id,
                    kind,
                    status,
                    detail,
                    blocked_reason,
                    expected_cash_recovery,
                    now_ts,
                );
                current_ids.insert(task_key(&task.market_id, task.kind));
                tasks.push(task);
            }
        }

        for discrepancy in discrepancies {
            let event_id = market_states
                .get(&discrepancy.market_id)
                .map(|state| state.event_id.clone())
                .unwrap_or_else(|| "UNKNOWN".to_owned());
            let task = build_task(
                self.context.domain,
                &discrepancy.market_id,
                &event_id,
                SettlementTaskKind::ReconcileOrders,
                SettlementTaskStatus::Pending,
                discrepancy.detail.clone(),
                None,
                None,
                now_ts,
            );
            current_ids.insert(task_key(&task.market_id, task.kind));
            tasks.push(task);
        }

        let tasks_closed = previous_ids.difference(&current_ids).count();
        Ok((tasks, tasks_closed))
    }

    fn persist_positions(&self, positions: &[SettlementPositionSnapshot]) -> Result<()> {
        for position in positions {
            self.context.store.upsert_snapshot(NewStateSnapshot {
                domain: self.context.domain,
                aggregate_type: SNAPSHOT_SETTLEMENT_POSITION.to_owned(),
                aggregate_id: format!("{}:{}", position.market_id, position.token_id),
                version: position.updated_at.timestamp_millis(),
                payload: serde_json::to_value(position)?,
                derived_from_sequence: None,
                created_at: position.updated_at,
            })?;
        }
        Ok(())
    }

    fn persist_tasks(&self, tasks: &[SettlementTask]) -> Result<()> {
        for task in tasks {
            self.context.store.upsert_snapshot(NewStateSnapshot {
                domain: self.context.domain,
                aggregate_type: SNAPSHOT_SETTLEMENT_TASK.to_owned(),
                aggregate_id: task_key(&task.market_id, task.kind),
                version: task.updated_at.timestamp_millis(),
                payload: serde_json::to_value(task)?,
                derived_from_sequence: None,
                created_at: task.updated_at,
            })?;
            self.context.bus.publish(
                ServiceKind::SettlementEngine,
                self.context.domain,
                TOPIC_SETTLEMENT_TASK,
                serde_json::to_string(task)?,
            );
        }
        Ok(())
    }

    fn persist_cash_recovery(
        &self,
        market_states: &BTreeMap<String, SettlementMarketState>,
        positions: &[SettlementPositionSnapshot],
    ) -> Result<f64> {
        let mut recovered_cash = 0.0;

        for state in market_states.values() {
            if state.phase != SettlementPhase::Redeemed {
                continue;
            }
            let amount: f64 = positions
                .iter()
                .filter(|position| {
                    position.market_id == state.market_id && position.net_quantity > 0.0
                })
                .map(|position| state.winning_price.unwrap_or(1.0) * position.net_quantity)
                .sum();
            if amount <= 0.0 {
                continue;
            }

            let existing = self.context.store.latest_snapshot(
                self.context.domain,
                SNAPSHOT_SETTLEMENT_CASH,
                &state.market_id,
            )?;
            if existing
                .as_ref()
                .map(|snapshot| snapshot.version >= state.updated_at.timestamp_millis())
                .unwrap_or(false)
            {
                continue;
            }

            let recovery = SettlementCashRecovery {
                domain: self.context.domain,
                market_id: state.market_id.clone(),
                event_id: state.event_id.clone(),
                amount,
                recovered_at: now(),
                detail: format!("automatic redemption recovery for phase {:?}", state.phase),
            };
            self.context.store.upsert_snapshot(NewStateSnapshot {
                domain: self.context.domain,
                aggregate_type: SNAPSHOT_SETTLEMENT_CASH.to_owned(),
                aggregate_id: state.market_id.clone(),
                version: state.updated_at.timestamp_millis(),
                payload: serde_json::to_value(&recovery)?,
                derived_from_sequence: None,
                created_at: recovery.recovered_at,
            })?;
            self.context.store.append_event(NewDurableEvent {
                domain: self.context.domain,
                stream: format!("settlement:{}", state.market_id),
                aggregate_type: "settlement_cash_recovery".to_owned(),
                aggregate_id: state.market_id.clone(),
                event_type: "SETTLEMENT_CASH_RECOVERED".to_owned(),
                causation_id: None,
                correlation_id: None,
                idempotency_key: Some(format!(
                    "settlement-recovery:{}:{}",
                    state.market_id,
                    state.updated_at.timestamp_millis()
                )),
                payload: serde_json::to_value(&recovery)?,
                metadata: json!({
                    "event_id": state.event_id,
                }),
                created_at: recovery.recovered_at,
            })?;
            self.context.bus.publish(
                ServiceKind::SettlementEngine,
                self.context.domain,
                TOPIC_SETTLEMENT_CASH,
                serde_json::to_string(&recovery)?,
            );
            recovered_cash += amount;
        }

        Ok(recovered_cash)
    }

    fn load_command_cursor(&self) -> Result<SettlementCommandCursor> {
        let snapshot = self.context.store.latest_snapshot(
            self.context.domain,
            SNAPSHOT_SETTLEMENT_COMMAND_CURSOR,
            "latest",
        )?;
        Ok(match snapshot {
            Some(snapshot) => serde_json::from_value(snapshot.payload)
                .context("failed to decode settlement command cursor")?,
            None => SettlementCommandCursor {
                last_sequence: 0,
                updated_at: now(),
            },
        })
    }

    fn store_command_cursor(&self, last_sequence: i64) -> Result<()> {
        let cursor = SettlementCommandCursor {
            last_sequence,
            updated_at: now(),
        };
        self.context.store.upsert_snapshot(NewStateSnapshot {
            domain: self.context.domain,
            aggregate_type: SNAPSHOT_SETTLEMENT_COMMAND_CURSOR.to_owned(),
            aggregate_id: "latest".to_owned(),
            version: last_sequence,
            payload: serde_json::to_value(cursor)?,
            derived_from_sequence: Some(last_sequence),
            created_at: now(),
        })?;
        Ok(())
    }
}

pub async fn run_settlement_engine(
    context: ServiceContext,
    cancellation: CancellationToken,
) -> Result<()> {
    let config = SettlementConfig::from_env();
    context.audit.record(
        Some(context.domain),
        ServiceKind::SettlementEngine.as_str(),
        "settlement_engine_configured",
        &format!(
            "reconcile_ms={} stale_order_secs={}",
            config.reconcile_interval.as_millis(),
            config.stale_order_after.as_secs()
        ),
    )?;
    SettlementEngineService::new(context, config)
        .run(cancellation)
        .await
}

fn materialize_positions(
    domain: AccountDomain,
    market_states: &BTreeMap<String, SettlementMarketState>,
    intents: &[ExecutionIntentRecord],
    orders: &[OrderLifecycleRecord],
) -> Vec<SettlementPositionSnapshot> {
    #[derive(Default)]
    struct Accumulator {
        event_id: String,
        net_quantity: f64,
        buy_quantity: f64,
        buy_notional: f64,
        gross_cost_basis: f64,
        realized_cash_flow: f64,
        last_trade_at: Option<polymarket_core::Timestamp>,
    }

    let intents_by_client_id: BTreeMap<&str, &ExecutionIntentRecord> = intents
        .iter()
        .map(|intent| (intent.client_order_id.as_str(), intent))
        .collect();
    let mut positions: BTreeMap<(String, String), Accumulator> = BTreeMap::new();

    for order in orders.iter().filter(|order| order.filled_quantity > 0.0) {
        let Some(client_order_id) = order.client_order_id.as_deref() else {
            continue;
        };
        let Some(intent) = intents_by_client_id.get(client_order_id) else {
            continue;
        };

        let key = (intent.market_id.clone(), intent.token_id.clone());
        let accumulator = positions.entry(key).or_default();
        accumulator.event_id = extract_event_id(intent);
        let fill_price = order
            .average_fill_price
            .or(order.limit_price)
            .unwrap_or(intent.limit_price);
        let signed_quantity = match intent.side {
            polymarket_core::TradeSide::Buy => order.filled_quantity,
            polymarket_core::TradeSide::Sell => -order.filled_quantity,
        };
        accumulator.net_quantity += signed_quantity;
        if matches!(intent.side, polymarket_core::TradeSide::Buy) {
            accumulator.buy_quantity += order.filled_quantity;
            accumulator.buy_notional += order.filled_quantity * fill_price;
            accumulator.gross_cost_basis += order.filled_quantity * fill_price;
            accumulator.realized_cash_flow -= order.filled_quantity * fill_price;
        } else {
            accumulator.realized_cash_flow += order.filled_quantity * fill_price;
        }
        accumulator.last_trade_at = Some(
            accumulator
                .last_trade_at
                .map(|current| current.max(order.updated_at))
                .unwrap_or(order.updated_at),
        );
    }

    let now_ts = now();
    positions
        .into_iter()
        .filter_map(|((market_id, token_id), position)| {
            if position.net_quantity.abs() <= 1e-9
                && position.realized_cash_flow.abs() <= 1e-9
                && position.gross_cost_basis.abs() <= 1e-9
            {
                return None;
            }

            let market_state = market_states.get(&market_id);
            let lifecycle_state = market_state
                .map(|state| match state.phase {
                    SettlementPhase::Trading => SettlementPositionState::Trading,
                    SettlementPhase::Resolving => SettlementPositionState::Settling,
                    SettlementPhase::Disputed => SettlementPositionState::Disputed,
                    SettlementPhase::Finalized | SettlementPhase::Redeeming => {
                        SettlementPositionState::Redeemable
                    }
                    SettlementPhase::Redeemed => SettlementPositionState::Redeemed,
                })
                .unwrap_or(SettlementPositionState::Trading);
            let recovered_value = market_state
                .and_then(|state| state.winning_price)
                .unwrap_or(0.0)
                * position.net_quantity.max(0.0);
            let realized_pnl =
                position.realized_cash_flow + recovered_value - position.gross_cost_basis;

            Some(SettlementPositionSnapshot {
                domain,
                market_id,
                token_id,
                event_id: position.event_id,
                net_quantity: position.net_quantity,
                average_open_price: if position.buy_quantity > 0.0 {
                    position.buy_notional / position.buy_quantity
                } else {
                    0.0
                },
                gross_cost_basis: position.gross_cost_basis,
                realized_cash_flow: position.realized_cash_flow,
                realized_pnl,
                lifecycle_state,
                last_trade_at: position.last_trade_at.unwrap_or(now_ts),
                updated_at: now_ts,
            })
        })
        .collect()
}

fn extract_event_id(intent: &ExecutionIntentRecord) -> String {
    intent
        .detail
        .get("event_id")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_owned()
}

fn stale_for(
    previous: polymarket_core::Timestamp,
    current: polymarket_core::Timestamp,
) -> Duration {
    (current - previous)
        .to_std()
        .unwrap_or_else(|_| Duration::from_secs(0))
}

fn merge_detail(mut detail: Value, patch: Value) -> Value {
    match (&mut detail, patch) {
        (Value::Object(existing), Value::Object(patch)) => {
            for (key, value) in patch {
                existing.insert(key, value);
            }
            detail
        }
        (_, patch) => patch,
    }
}

fn task_key(market_id: &str, kind: SettlementTaskKind) -> String {
    format!("{market_id}:{kind:?}")
}

fn build_task(
    domain: AccountDomain,
    market_id: &str,
    event_id: &str,
    kind: SettlementTaskKind,
    status: SettlementTaskStatus,
    detail: String,
    blocked_reason: Option<String>,
    expected_cash_recovery: Option<f64>,
    timestamp: polymarket_core::Timestamp,
) -> SettlementTask {
    SettlementTask {
        task_id: Uuid::new_v4(),
        domain,
        market_id: market_id.to_owned(),
        event_id: event_id.to_owned(),
        kind,
        status,
        detail,
        blocked_reason,
        expected_cash_recovery,
        created_at: timestamp,
        updated_at: timestamp,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_core::{
        AccountDomain, ExecutionIntentStatus, IntentPolicy, NewDurableEvent,
        NewOrderLifecycleRecord, StrategyKind, TradeIntent, TradeSide,
    };
    use polymarket_msgbus::MessageBus;
    use polymarket_storage::Store;
    use std::sync::Arc;

    fn temp_store() -> Store {
        let path =
            std::env::temp_dir().join(format!("polymarket-settlement-{}.db", Uuid::new_v4()));
        let store = Store::new(path, AccountDomain::Sim, "sim", "audit");
        store.init().expect("store init");
        store
    }

    fn test_context(store: Store) -> ServiceContext {
        let domain_config = polymarket_config::NodeConfig::from_env(AccountDomain::Sim)
            .expect("node config")
            .selected_domain_config;
        ServiceContext {
            domain: AccountDomain::Sim,
            domain_config,
            store,
            bus: MessageBus::new(32),
            audit: Arc::new(polymarket_audit::StorageAuditSink::new(temp_store())),
            heartbeat_interval: Duration::from_secs(5),
        }
    }

    fn sample_intent() -> TradeIntent {
        TradeIntent {
            account_domain: AccountDomain::Sim,
            market_id: "mkt-1".to_owned(),
            token_id: "token-yes".to_owned(),
            side: TradeSide::Buy,
            limit_price: 0.42,
            max_size: 10.0,
            policy: IntentPolicy::Passive,
            expires_at: now(),
            strategy_kind: StrategyKind::DependencyArb,
            thesis_ref: "test".to_owned(),
            opportunity_id: Uuid::new_v4(),
            event_id: "event-1".to_owned(),
        }
    }

    #[test]
    fn materialize_positions_marks_redeemable_inventory() {
        let intent = sample_intent();
        let record = ExecutionIntentRecord {
            intent_id: intent.opportunity_id,
            domain: AccountDomain::Sim,
            batch_id: Some(Uuid::new_v4()),
            strategy_kind: intent.strategy_kind,
            market_id: intent.market_id.clone(),
            token_id: intent.token_id.clone(),
            side: intent.side,
            limit_price: intent.limit_price,
            target_size: intent.max_size,
            idempotency_key: "idem".to_owned(),
            client_order_id: "client-1".to_owned(),
            status: ExecutionIntentStatus::Filled,
            detail: json!({ "event_id": intent.event_id }),
            created_at: now(),
            updated_at: now(),
            expires_at: intent.expires_at,
        };
        let order = OrderLifecycleRecord {
            domain: AccountDomain::Sim,
            order_id: "order-1".to_owned(),
            market_id: intent.market_id.clone(),
            status: OrderLifecycleStatus::Filled,
            client_order_id: Some("client-1".to_owned()),
            external_order_id: None,
            idempotency_key: Some("idem".to_owned()),
            side: Some("BUY".to_owned()),
            limit_price: Some(0.42),
            order_quantity: Some(10.0),
            filled_quantity: 10.0,
            average_fill_price: Some(0.40),
            last_event_sequence: None,
            detail: json!({}),
            opened_at: now(),
            updated_at: now(),
            closed_at: Some(now()),
        };
        let market_state = SettlementMarketState {
            domain: AccountDomain::Sim,
            market_id: "mkt-1".to_owned(),
            event_id: "event-1".to_owned(),
            phase: SettlementPhase::Finalized,
            winning_price: Some(1.0),
            disputed: false,
            finalizes_at: None,
            updated_at: now(),
            detail: "finalized".to_owned(),
        };
        let mut states = BTreeMap::new();
        states.insert(market_state.market_id.clone(), market_state);

        let positions = materialize_positions(AccountDomain::Sim, &states, &[record], &[order]);
        assert_eq!(positions.len(), 1);
        assert_eq!(
            positions[0].lifecycle_state,
            SettlementPositionState::Redeemable
        );
        assert!((positions[0].realized_pnl - 2.0).abs() < 1e-9);
    }

    #[test]
    fn process_commands_persists_market_state_snapshot() {
        let store = temp_store();
        let context = test_context(store.clone());
        let service = SettlementEngineService::new(context, SettlementConfig::from_env());
        let created_at = now();
        store
            .append_event(NewDurableEvent {
                domain: AccountDomain::Sim,
                stream: "settlement.commands".to_owned(),
                aggregate_type: "settlement_command".to_owned(),
                aggregate_id: "mkt-1".to_owned(),
                event_type: "SETTLEMENT_COMMAND".to_owned(),
                causation_id: None,
                correlation_id: None,
                idempotency_key: None,
                payload: serde_json::to_value(SettlementCommand::UpdateMarketState {
                    market_id: "mkt-1".to_owned(),
                    event_id: "event-1".to_owned(),
                    phase: SettlementPhase::Resolving,
                    winning_price: None,
                    finalizes_at: None,
                    detail: "resolution pending".to_owned(),
                })
                .expect("serialize command"),
                metadata: json!({}),
                created_at,
            })
            .expect("append event");

        service.process_commands().expect("process commands");

        let snapshot = store
            .latest_snapshot(AccountDomain::Sim, SNAPSHOT_SETTLEMENT_MARKET, "mkt-1")
            .expect("load snapshot")
            .expect("snapshot exists");
        let state: SettlementMarketState =
            serde_json::from_value(snapshot.payload).expect("decode market state");
        assert_eq!(state.phase, SettlementPhase::Resolving);
        assert_eq!(state.event_id, "event-1");
    }

    #[test]
    fn reconcile_pending_orders_converges_terminal_state() {
        let store = temp_store();
        let store_for_audit = store.clone();
        let context = ServiceContext {
            domain: AccountDomain::Sim,
            domain_config: polymarket_config::NodeConfig::from_env(AccountDomain::Sim)
                .expect("node config")
                .selected_domain_config,
            store: store.clone(),
            bus: MessageBus::new(16),
            audit: Arc::new(polymarket_audit::StorageAuditSink::new(store_for_audit)),
            heartbeat_interval: Duration::from_secs(5),
        };
        let service = SettlementEngineService::new(
            context,
            SettlementConfig {
                reconcile_interval: Duration::from_secs(30),
                stale_order_after: Duration::from_secs(60),
            },
        );

        let intent = sample_intent();
        let record = store
            .record_execution_intent(
                &intent,
                intent.opportunity_id,
                Some(Uuid::new_v4()),
                "idem",
                "client-1",
                ExecutionIntentStatus::Submitted,
                json!({ "event_id": intent.event_id }),
            )
            .expect("record intent");
        let order = store
            .upsert_order_lifecycle(NewOrderLifecycleRecord {
                domain: AccountDomain::Sim,
                order_id: "order-1".to_owned(),
                market_id: intent.market_id.clone(),
                status: OrderLifecycleStatus::Filled,
                client_order_id: Some("client-1".to_owned()),
                external_order_id: None,
                idempotency_key: Some("idem".to_owned()),
                side: Some("BUY".to_owned()),
                limit_price: Some(0.42),
                order_quantity: Some(10.0),
                filled_quantity: 10.0,
                average_fill_price: Some(0.40),
                last_event_sequence: None,
                detail: json!({}),
                opened_at: now(),
                updated_at: now(),
                closed_at: Some(now()),
            })
            .expect("insert order");
        let market_state = SettlementMarketState {
            domain: AccountDomain::Sim,
            market_id: order.market_id.clone(),
            event_id: "event-1".to_owned(),
            phase: SettlementPhase::Finalized,
            winning_price: Some(1.0),
            disputed: false,
            finalizes_at: None,
            updated_at: now(),
            detail: "finalized".to_owned(),
        };
        let mut states = BTreeMap::new();
        states.insert(market_state.market_id.clone(), market_state);

        let (discrepancies, converged) = service
            .reconcile_pending_orders(&states, &[record.clone()], &[order.clone()])
            .expect("reconcile pending");
        assert!(discrepancies.is_empty());
        assert!(converged >= 1);

        let updated = store
            .get_execution_intent(record.intent_id)
            .expect("load intent")
            .expect("intent exists");
        assert_eq!(updated.status, ExecutionIntentStatus::Filled);
        let settled_order = store
            .order_lifecycle(AccountDomain::Sim, "order-1")
            .expect("load order")
            .expect("order exists");
        assert_eq!(settled_order.status, OrderLifecycleStatus::Settled);
    }
}
