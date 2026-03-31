use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use polymarket_config::RiskEngineConfig;
use polymarket_core::{
    now, IntentPolicy, OptimizationStatus, PortfolioSnapshot, PromotionStage, RiskBudgetSnapshot,
    RolloutPolicy, RuntimeHealth, StrategyKind, Timestamp, TradeIntent, TradeIntentBatch,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type RiskConfig = RiskEngineConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskRuntimeMode {
    Normal,
    Degraded,
    Safe,
}

impl RiskRuntimeMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Degraded => "DEGRADED",
            Self::Safe => "SAFE",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskCode {
    EmptyBatch,
    DomainMismatch,
    UnknownMarket,
    InvalidTickSize,
    InvalidOrderSize,
    InvalidExpiry,
    MarketUnsafe,
    RuleChurn,
    ClarificationCooldown,
    ResolutionHazard,
    BookDepthInsufficient,
    FeeLeakage,
    StrategyDegraded,
    StrategyBudgetExceeded,
    MarketExposureLimit,
    EventExposureLimit,
    CategoryExposureLimit,
    CashBufferBreach,
    UnresolvedCapitalLimit,
    DisputedCapitalLimit,
    RuntimeDegraded,
    RuntimeUnhealthy,
    EngineRestarting,
    DailyLossLimit,
    RollingDrawdownLimit,
    RolloutStageBlocked,
    RolloutStrategyBlocked,
    RolloutCapabilityBlocked,
    RolloutNotionalExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RiskFinding {
    pub code: RiskCode,
    pub scope: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskMarketState {
    pub market_id: String,
    pub event_id: String,
    pub category: String,
    pub resolution_source: Option<String>,
    pub tradable: bool,
    pub rules_stable: bool,
    pub ambiguous_rules: bool,
    pub recent_rule_change_at: Option<Timestamp>,
    pub recent_clarification_at: Option<Timestamp>,
    pub event_starts_at: Option<Timestamp>,
    pub resolution_window_starts_at: Option<Timestamp>,
    pub book_depth_score: f64,
    pub fee_cost_bps: i32,
    pub tick_size: Option<f64>,
    pub min_order_size: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StrategyRiskState {
    pub strategy: StrategyKind,
    pub live_exposure: f64,
    pub daily_pnl: f64,
    pub rolling_30d_pnl: f64,
    pub consecutive_failures: u32,
    pub reject_rate: f64,
    pub fill_rate: f64,
    pub fee_leakage_bps: i32,
}

impl StrategyRiskState {
    pub fn healthy(strategy: StrategyKind) -> Self {
        Self {
            strategy,
            live_exposure: 0.0,
            daily_pnl: 0.0,
            rolling_30d_pnl: 0.0,
            consecutive_failures: 0,
            reject_rate: 0.0,
            fill_rate: 1.0,
            fee_leakage_bps: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskContext {
    pub portfolio: PortfolioSnapshot,
    pub budgets: RiskBudgetSnapshot,
    pub runtime: RuntimeHealth,
    pub market_states: BTreeMap<String, RiskMarketState>,
    pub strategy_states: BTreeMap<StrategyKind, StrategyRiskState>,
    pub disputed_capital: f64,
    pub decision_time: Timestamp,
    pub runtime_mode_override: Option<RiskRuntimeMode>,
    pub rollout_stage: Option<PromotionStage>,
    pub rollout_policy: Option<RolloutPolicy>,
}

impl RiskContext {
    pub fn with_now(
        portfolio: PortfolioSnapshot,
        budgets: RiskBudgetSnapshot,
        runtime: RuntimeHealth,
    ) -> Self {
        Self {
            portfolio,
            budgets,
            runtime,
            market_states: BTreeMap::new(),
            strategy_states: BTreeMap::new(),
            disputed_capital: 0.0,
            decision_time: now(),
            runtime_mode_override: None,
            rollout_stage: None,
            rollout_policy: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovedTradeIntentBatch {
    pub batch: TradeIntentBatch,
    pub runtime_mode: RiskRuntimeMode,
    pub scale: f64,
    pub approved_at: Timestamp,
    pub findings: Vec<RiskFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskRejection {
    pub runtime_mode: RiskRuntimeMode,
    pub rejected_at: Timestamp,
    pub findings: Vec<RiskFinding>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RiskEvaluation {
    Approved(ApprovedTradeIntentBatch),
    Rejected(RiskRejection),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeDecision {
    pub mode: RiskRuntimeMode,
    pub evaluated_at: Timestamp,
    pub should_cancel_all: bool,
    pub reduce_only: bool,
    pub findings: Vec<RiskFinding>,
}

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("batch contains no trade intents")]
    EmptyBatch,
    #[error("batch contains mixed account domains")]
    MixedDomains,
}

#[async_trait]
pub trait RiskEngine {
    async fn evaluate_batch(
        &self,
        batch: TradeIntentBatch,
        ctx: RiskContext,
    ) -> Result<RiskEvaluation, RiskError>;

    async fn evaluate_runtime(&self, ctx: &RiskContext) -> Result<RuntimeDecision, RiskError>;
}

#[derive(Debug, Clone)]
pub struct RiskEngineService {
    config: RiskConfig,
}

impl RiskEngineService {
    pub fn new(config: RiskConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl RiskEngine for RiskEngineService {
    async fn evaluate_batch(
        &self,
        batch: TradeIntentBatch,
        ctx: RiskContext,
    ) -> Result<RiskEvaluation, RiskError> {
        if batch.intents.is_empty() {
            return Err(RiskError::EmptyBatch);
        }
        if batch
            .intents
            .iter()
            .any(|intent| intent.account_domain != batch.account_domain)
        {
            return Err(RiskError::MixedDomains);
        }

        let runtime = self.evaluate_runtime(&ctx).await?;
        if runtime.mode == RiskRuntimeMode::Safe {
            let mut findings = runtime.findings;
            findings.push(finding(
                RiskCode::RuntimeUnhealthy,
                "batch",
                "runtime is in SAFE mode; only unwind and reconciliation actions are allowed",
            ));
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: RiskRuntimeMode::Safe,
                rejected_at: ctx.decision_time,
                findings,
            }));
        }

        let mut findings = runtime.findings;
        let mut scale = 1.0_f64;
        let portfolio = &ctx.portfolio;
        let budgets = &ctx.budgets;
        let nav = portfolio.nav.max(1.0);
        let batch_notional = batch_notional(&batch);

        if let Some(policy) = ctx.rollout_policy.as_ref() {
            if policy.domain != batch.account_domain {
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::RolloutStageBlocked,
                            "rollout",
                            format!(
                                "rollout policy for domain `{}` cannot be applied to `{}`",
                                policy.domain, batch.account_domain
                            ),
                        ),
                    ),
                }));
            }
            if !policy.allowed_strategies.is_empty()
                && batch
                    .intents
                    .iter()
                    .any(|intent| !policy.allowed_strategies.contains(&intent.strategy_kind))
            {
                let blocked = batch
                    .intents
                    .iter()
                    .find(|intent| !policy.allowed_strategies.contains(&intent.strategy_kind))
                    .expect("blocked strategy exists");
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::RolloutStrategyBlocked,
                            blocked.strategy_kind.as_str(),
                            format!(
                                "strategy `{}` is not allowlisted for rollout stage `{}`",
                                blocked.strategy_kind, policy.stage
                            ),
                        ),
                    ),
                }));
            }
            if batch_notional > policy.max_batch_notional {
                scale = scale.min(policy.max_batch_notional / batch_notional);
                findings.push(finding(
                    RiskCode::RolloutNotionalExceeded,
                    "rollout",
                    format!(
                        "batch notional {:.2} exceeds rollout stage cap {:.2}",
                        batch_notional, policy.max_batch_notional
                    ),
                ));
            }
            if is_complex_multileg(&batch) && !policy.capabilities.allow_multileg {
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::RolloutCapabilityBlocked,
                            "rollout",
                            format!(
                                "multi-leg execution is disabled for rollout stage `{}`",
                                policy.stage
                            ),
                        ),
                    ),
                }));
            }
            if batch
                .intents
                .iter()
                .any(|intent| intent.strategy_kind == StrategyKind::FeeAwareMM)
                && !policy.capabilities.allow_market_making
            {
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::RolloutCapabilityBlocked,
                            "rollout",
                            format!(
                                "market making is disabled for rollout stage `{}`",
                                policy.stage
                            ),
                        ),
                    ),
                }));
            }
        }

        if batch_notional > self.config.max_batch_notional {
            scale = scale.min(self.config.max_batch_notional / batch_notional);
            findings.push(finding(
                RiskCode::StrategyBudgetExceeded,
                "batch",
                &format!(
                    "batch notional {:.2} exceeds cap {:.2}",
                    batch_notional, self.config.max_batch_notional
                ),
            ));
        }

        let required_cash_buffer = budgets
            .min_cash_buffer
            .max(portfolio.nav * self.config.min_cash_buffer_ratio);
        let remaining_cash = portfolio.cash_available - batch_notional;
        if remaining_cash < required_cash_buffer {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::CashBufferBreach,
                        "portfolio",
                        &format!(
                            "cash after execution {:.2} would fall below required buffer {:.2}",
                            remaining_cash, required_cash_buffer
                        ),
                    ),
                ),
            }));
        }

        if portfolio.unresolved_capital + batch_notional
            > budgets
                .max_unresolved_notional
                .max(portfolio.nav * self.config.unresolved_capital_limit_ratio)
        {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::UnresolvedCapitalLimit,
                        "portfolio",
                        "unresolved capital limit would be exceeded",
                    ),
                ),
            }));
        }

        if ctx.disputed_capital > nav * self.config.disputed_capital_limit_ratio {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::DisputedCapitalLimit,
                        "portfolio",
                        "disputed capital share exceeds configured limit",
                    ),
                ),
            }));
        }

        let complex_multileg = is_complex_multileg(&batch);
        if runtime.mode == RiskRuntimeMode::Degraded
            && complex_multileg
            && !batch
                .intents
                .iter()
                .all(|intent| intent.policy == IntentPolicy::ReduceOnly)
        {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::RuntimeDegraded,
                        "batch",
                        "complex multi-leg entries are disabled while runtime is degraded",
                    ),
                ),
            }));
        }

        let mut market_notional = existing_market_exposure(portfolio);
        let mut event_notional = existing_event_exposure(portfolio);
        let mut category_notional = existing_category_exposure(portfolio);
        let mut strategy_notional: BTreeMap<StrategyKind, f64> = ctx
            .strategy_states
            .iter()
            .map(|(strategy, state)| (*strategy, state.live_exposure))
            .collect();

        for intent in &batch.intents {
            let hard_findings = validate_intent(intent, &ctx, &self.config);
            if !hard_findings.is_empty() {
                findings.extend(hard_findings);
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings,
                }));
            }
            let market = ctx
                .market_states
                .get(&intent.market_id)
                .ok_or(RiskError::EmptyBatch)?;
            let notional = intent_notional(intent);

            if market.book_depth_score < self.config.book_depth_reject_threshold {
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::BookDepthInsufficient,
                            &intent.market_id,
                            "order book depth is below hard rejection threshold",
                        ),
                    ),
                }));
            }

            if market.book_depth_score < self.config.book_depth_reduce_threshold {
                let reduce_scale =
                    (market.book_depth_score / self.config.book_depth_reduce_threshold).max(0.1);
                scale = scale.min(reduce_scale);
                findings.push(finding(
                    RiskCode::BookDepthInsufficient,
                    &intent.market_id,
                    "order book depth is shallow; intent size reduced",
                ));
            }

            if market.fee_cost_bps >= self.config.fee_bps_hard_limit {
                return Ok(RiskEvaluation::Rejected(RiskRejection {
                    runtime_mode: runtime.mode,
                    rejected_at: ctx.decision_time,
                    findings: with_extra(
                        findings,
                        finding(
                            RiskCode::FeeLeakage,
                            &intent.market_id,
                            "fee-adjusted edge is below hard execution floor",
                        ),
                    ),
                }));
            }

            if market.fee_cost_bps >= self.config.fee_bps_soft_limit {
                scale = scale.min(0.5);
                findings.push(finding(
                    RiskCode::FeeLeakage,
                    &intent.market_id,
                    "fee-adjusted edge is weak; intent size reduced",
                ));
            }

            let strategy_limit = strategy_budget(intent.strategy_kind, budgets);
            let next_strategy = strategy_notional
                .get(&intent.strategy_kind)
                .copied()
                .unwrap_or_default()
                + notional;
            if next_strategy > strategy_limit {
                scale = scale.min((strategy_limit / next_strategy).max(0.0));
                findings.push(finding(
                    RiskCode::StrategyBudgetExceeded,
                    intent.strategy_kind.as_str(),
                    "strategy live exposure would exceed configured budget",
                ));
            }
            strategy_notional.insert(intent.strategy_kind, next_strategy);

            let next_market = market_notional
                .get(&intent.market_id)
                .copied()
                .unwrap_or_default()
                + notional;
            let market_limit = budgets
                .max_market_notional
                .max(nav * self.config.market_cap_ratio);
            if next_market > market_limit {
                scale = scale.min((market_limit / next_market).max(0.0));
                findings.push(finding(
                    RiskCode::MarketExposureLimit,
                    &intent.market_id,
                    "market exposure would exceed configured cap",
                ));
            }
            market_notional.insert(intent.market_id.clone(), next_market);

            let next_event = event_notional
                .get(&intent.event_id)
                .copied()
                .unwrap_or_default()
                + notional;
            let event_limit = budgets
                .max_event_notional
                .max(nav * self.config.event_cap_ratio);
            if next_event > event_limit {
                scale = scale.min((event_limit / next_event).max(0.0));
                findings.push(finding(
                    RiskCode::EventExposureLimit,
                    &intent.event_id,
                    "event-family exposure would exceed configured cap",
                ));
            }
            event_notional.insert(intent.event_id.clone(), next_event);

            let next_category = category_notional
                .get(&market.category)
                .copied()
                .unwrap_or_default()
                + notional;
            let category_limit = budgets
                .max_category_notional
                .max(nav * self.config.category_cap_ratio);
            if next_category > category_limit {
                scale = scale.min((category_limit / next_category).max(0.0));
                findings.push(finding(
                    RiskCode::CategoryExposureLimit,
                    &market.category,
                    "category exposure would exceed configured cap",
                ));
            }
            category_notional.insert(market.category.clone(), next_category);
        }

        if runtime.mode == RiskRuntimeMode::Degraded {
            scale = scale.min(self.config.reduce_only_scale.max(0.1));
        }

        if scale <= 0.0 {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::StrategyBudgetExceeded,
                        "batch",
                        "no residual size remains after applying risk caps",
                    ),
                ),
            }));
        }

        let scaled = scale_batch(batch, scale, self.config.min_order_size);
        if scaled.intents.is_empty() {
            return Ok(RiskEvaluation::Rejected(RiskRejection {
                runtime_mode: runtime.mode,
                rejected_at: ctx.decision_time,
                findings: with_extra(
                    findings,
                    finding(
                        RiskCode::InvalidOrderSize,
                        "batch",
                        "all intents fell below minimum order size after scaling",
                    ),
                ),
            }));
        }

        Ok(RiskEvaluation::Approved(ApprovedTradeIntentBatch {
            batch: scaled,
            runtime_mode: runtime.mode,
            scale,
            approved_at: ctx.decision_time,
            findings,
        }))
    }

    async fn evaluate_runtime(&self, ctx: &RiskContext) -> Result<RuntimeDecision, RiskError> {
        let mut mode = RiskRuntimeMode::Normal;
        let mut findings = Vec::new();
        let nav = ctx.portfolio.nav.max(1.0);
        let unrealized_pnl = ctx
            .portfolio
            .positions
            .iter()
            .map(|position| position.unrealized_pnl)
            .sum::<f64>();
        let loss_ratio = (-unrealized_pnl).max(0.0) / nav;

        if ctx.runtime.heartbeat_age_ms >= self.config.heartbeat_safe_ms {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::RuntimeUnhealthy,
                "runtime",
                "heartbeat age exceeded safe threshold",
            ));
        } else if ctx.runtime.heartbeat_age_ms >= self.config.heartbeat_degraded_ms {
            mode = RiskRuntimeMode::Degraded;
            findings.push(finding(
                RiskCode::RuntimeDegraded,
                "runtime",
                "heartbeat age exceeded degraded threshold",
            ));
        }

        if !ctx.runtime.user_ws_ok {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::RuntimeDegraded,
                "runtime",
                "user websocket is unhealthy",
            ));
        }

        if ctx.runtime.market_ws_lag_ms >= self.config.market_ws_lag_safe_ms {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::RuntimeUnhealthy,
                "runtime",
                "market websocket lag exceeded safe threshold",
            ));
        } else if ctx.runtime.market_ws_lag_ms >= self.config.market_ws_lag_degraded_ms {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::RuntimeDegraded,
                "runtime",
                "market websocket lag exceeded degraded threshold",
            ));
        }

        if ctx.runtime.recent_425_count >= self.config.recent_425_safe {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::EngineRestarting,
                "runtime",
                "repeated HTTP 425 activity suggests exchange restart window",
            ));
        } else if ctx.runtime.recent_425_count >= self.config.recent_425_degraded {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::EngineRestarting,
                "runtime",
                "elevated HTTP 425 activity detected",
            ));
        }

        if ctx.runtime.reject_rate_5m >= self.config.reject_rate_safe {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::RuntimeUnhealthy,
                "runtime",
                "reject rate exceeded safe threshold",
            ));
        } else if ctx.runtime.reject_rate_5m >= self.config.reject_rate_degraded {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::RuntimeDegraded,
                "runtime",
                "reject rate exceeded degraded threshold",
            ));
        }

        if ctx.runtime.reconcile_drift {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::RuntimeUnhealthy,
                "runtime",
                "reconciliation drift detected between order and position ledgers",
            ));
        }

        if loss_ratio >= self.config.rolling_drawdown_limit_ratio {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::RollingDrawdownLimit,
                "portfolio",
                "rolling drawdown estimate exceeded configured limit",
            ));
        } else if loss_ratio >= self.config.daily_loss_limit_ratio {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::DailyLossLimit,
                "portfolio",
                "daily loss estimate exceeded configured limit",
            ));
        }

        if ctx.portfolio.unresolved_capital > nav * self.config.unresolved_capital_limit_ratio {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::UnresolvedCapitalLimit,
                "portfolio",
                "unresolved capital share is elevated",
            ));
        }

        if ctx.disputed_capital > nav * self.config.disputed_capital_limit_ratio {
            mode = RiskRuntimeMode::Safe;
            findings.push(finding(
                RiskCode::DisputedCapitalLimit,
                "portfolio",
                "disputed capital share exceeded configured limit",
            ));
        }

        if ctx.market_states.values().any(|market| {
            market
                .recent_clarification_at
                .map(|ts| {
                    (ctx.decision_time - ts).num_seconds()
                        <= self.config.clarification_cooldown_secs as i64
                })
                .unwrap_or(false)
                || market
                    .recent_rule_change_at
                    .map(|ts| {
                        (ctx.decision_time - ts).num_seconds()
                            <= self.config.rule_change_cooldown_secs as i64
                    })
                    .unwrap_or(false)
        }) {
            mode = mode.max(RiskRuntimeMode::Degraded);
            findings.push(finding(
                RiskCode::RuleChurn,
                "market",
                "recent clarification or rule churn detected",
            ));
        }

        if let Some(override_mode) = ctx.runtime_mode_override {
            mode = mode.max(override_mode);
        }

        Ok(RuntimeDecision {
            mode,
            evaluated_at: ctx.decision_time,
            should_cancel_all: mode == RiskRuntimeMode::Safe,
            reduce_only: mode != RiskRuntimeMode::Normal,
            findings,
        })
    }
}

fn validate_intent(
    intent: &TradeIntent,
    ctx: &RiskContext,
    config: &RiskConfig,
) -> Vec<RiskFinding> {
    let Some(market) = ctx.market_states.get(&intent.market_id) else {
        return vec![finding(
            RiskCode::UnknownMarket,
            &intent.market_id,
            "market state is missing from the risk input surface",
        )];
    };
    let tick_size = market.tick_size.unwrap_or(config.tick_size);
    let min_order_size = market.min_order_size.unwrap_or(config.min_order_size);
    let mut findings = Vec::new();

    if intent.max_size < min_order_size {
        return vec![finding(
            RiskCode::InvalidOrderSize,
            &intent.market_id,
            "order size is below minimum threshold",
        )];
    }

    if !is_price_aligned(intent.limit_price, tick_size) {
        return vec![finding(
            RiskCode::InvalidTickSize,
            &intent.market_id,
            "limit price is not aligned to tick size",
        )];
    }

    if intent.expires_at <= ctx.decision_time {
        return vec![finding(
            RiskCode::InvalidExpiry,
            &intent.market_id,
            "intent expiration is already in the past",
        )];
    }

    if !market.tradable || !market.rules_stable || market.ambiguous_rules {
        findings.push(finding(
            RiskCode::MarketUnsafe,
            &intent.market_id,
            "market is not currently safe to trade",
        ));
    }

    if market
        .recent_rule_change_at
        .map(|ts| (ctx.decision_time - ts).num_seconds() <= config.rule_change_cooldown_secs as i64)
        .unwrap_or(false)
    {
        findings.push(finding(
            RiskCode::RuleChurn,
            &intent.market_id,
            "recent rule change is still inside cooldown window",
        ));
    }

    if market
        .recent_clarification_at
        .map(|ts| {
            (ctx.decision_time - ts).num_seconds() <= config.clarification_cooldown_secs as i64
        })
        .unwrap_or(false)
    {
        findings.push(finding(
            RiskCode::ClarificationCooldown,
            &intent.market_id,
            "recent clarification is still inside cooldown window",
        ));
    }

    if market
        .event_starts_at
        .map(|ts| {
            (ts - ctx.decision_time).num_seconds() <= config.resolution_hazard_window_secs as i64
        })
        .unwrap_or(false)
        || market
            .resolution_window_starts_at
            .map(|ts| {
                (ts - ctx.decision_time).num_seconds()
                    <= config.resolution_hazard_window_secs as i64
            })
            .unwrap_or(false)
    {
        findings.push(finding(
            RiskCode::ResolutionHazard,
            &intent.market_id,
            "market is inside configured event or resolution hazard window",
        ));
    }

    findings
}

fn scale_batch(batch: TradeIntentBatch, scale: f64, min_order_size: f64) -> TradeIntentBatch {
    let intents = batch
        .intents
        .into_iter()
        .filter_map(|mut intent| {
            intent.max_size *= scale;
            if intent.max_size >= min_order_size {
                Some(intent)
            } else {
                None
            }
        })
        .collect();

    TradeIntentBatch { intents, ..batch }
}

fn batch_notional(batch: &TradeIntentBatch) -> f64 {
    batch.intents.iter().map(intent_notional).sum()
}

fn intent_notional(intent: &TradeIntent) -> f64 {
    intent.limit_price * intent.max_size
}

fn is_price_aligned(price: f64, tick_size: f64) -> bool {
    if tick_size <= 0.0 {
        return false;
    }
    let scaled = price / tick_size;
    (scaled - scaled.round()).abs() <= 1e-9
}

fn is_complex_multileg(batch: &TradeIntentBatch) -> bool {
    if batch.optimization_status == OptimizationStatus::TimedOut {
        return false;
    }
    let unique_markets = batch
        .intents
        .iter()
        .map(|intent| intent.market_id.as_str())
        .collect::<BTreeSet<_>>();
    unique_markets.len() > 1
        || batch
            .intents
            .iter()
            .any(|intent| intent.policy == IntentPolicy::Shrinkable)
}

fn strategy_budget(strategy: StrategyKind, budgets: &RiskBudgetSnapshot) -> f64 {
    match strategy {
        StrategyKind::DependencyArb | StrategyKind::Rebalancing => {
            budgets.max_structural_arb_notional
        }
        StrategyKind::NegRisk => budgets.max_neg_risk_notional,
        StrategyKind::FeeAwareMM => budgets.max_fee_mm_notional,
        StrategyKind::RulesDriven => budgets.max_rules_driven_notional,
    }
}

fn existing_market_exposure(portfolio: &PortfolioSnapshot) -> BTreeMap<String, f64> {
    let mut exposure = BTreeMap::new();
    for position in &portfolio.positions {
        *exposure.entry(position.market_id.clone()).or_insert(0.0) +=
            position.quantity.abs() * position.mark_price + position.reserved_notional;
    }
    exposure
}

fn existing_event_exposure(portfolio: &PortfolioSnapshot) -> BTreeMap<String, f64> {
    let mut exposure = BTreeMap::new();
    for position in &portfolio.positions {
        *exposure.entry(position.event_id.clone()).or_insert(0.0) +=
            position.quantity.abs() * position.mark_price + position.reserved_notional;
    }
    exposure
}

fn existing_category_exposure(portfolio: &PortfolioSnapshot) -> BTreeMap<String, f64> {
    let mut exposure = BTreeMap::new();
    for position in &portfolio.positions {
        *exposure.entry(position.category.clone()).or_insert(0.0) +=
            position.quantity.abs() * position.mark_price + position.reserved_notional;
    }
    exposure
}

fn finding(code: RiskCode, scope: impl Into<String>, message: impl Into<String>) -> RiskFinding {
    RiskFinding {
        code,
        scope: scope.into(),
        message: message.into(),
    }
}

fn with_extra(mut findings: Vec<RiskFinding>, item: RiskFinding) -> Vec<RiskFinding> {
    findings.push(item);
    findings
}

#[cfg(test)]
mod tests {
    use chrono::{Duration as ChronoDuration, Utc};

    use super::*;
    use polymarket_core::{AccountDomain, PositionSnapshot, TradeSide};
    use uuid::Uuid;

    fn config() -> RiskConfig {
        RiskConfig::from_map(&BTreeMap::new()).expect("config")
    }

    fn runtime_health() -> RuntimeHealth {
        RuntimeHealth {
            domain: AccountDomain::Sim,
            runtime_mode: "OBSERVE".to_owned(),
            now: Utc::now(),
            market_ws_lag_ms: 50,
            user_ws_ok: true,
            heartbeat_age_ms: 500,
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
            stable_since: Some(Utc::now() - ChronoDuration::minutes(30)),
            shadow_live_drift_bps: Some(12.0),
        }
    }

    fn portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            nav: 10_000.0,
            cash_available: 8_000.0,
            reserved_cash: 0.0,
            positions: vec![PositionSnapshot {
                market_id: "m-existing".to_owned(),
                token_id: "t-existing".to_owned(),
                event_id: "e-existing".to_owned(),
                category: "Politics".to_owned(),
                quantity: 100.0,
                average_price: 0.45,
                mark_price: 0.47,
                reserved_notional: 0.0,
                unrealized_pnl: 20.0,
            }],
            unresolved_capital: 100.0,
            redeemable_capital: 0.0,
        }
    }

    fn budgets() -> RiskBudgetSnapshot {
        RiskBudgetSnapshot {
            max_structural_arb_notional: 1_000.0,
            max_neg_risk_notional: 1_500.0,
            max_rules_driven_notional: 800.0,
            max_fee_mm_notional: 700.0,
            min_cash_buffer: 1_000.0,
            max_single_intent_notional: 500.0,
            max_event_notional: 2_000.0,
            max_category_notional: 3_000.0,
            max_market_notional: 900.0,
            max_unresolved_notional: 2_000.0,
        }
    }

    fn market_state(depth: f64) -> RiskMarketState {
        RiskMarketState {
            market_id: "m1".to_owned(),
            event_id: "e1".to_owned(),
            category: "Sports".to_owned(),
            resolution_source: Some("oracle-a".to_owned()),
            tradable: true,
            rules_stable: true,
            ambiguous_rules: false,
            recent_rule_change_at: None,
            recent_clarification_at: None,
            event_starts_at: None,
            resolution_window_starts_at: None,
            book_depth_score: depth,
            fee_cost_bps: 10,
            tick_size: Some(0.01),
            min_order_size: Some(5.0),
        }
    }

    fn intent() -> TradeIntent {
        TradeIntent {
            account_domain: AccountDomain::Sim,
            market_id: "m1".to_owned(),
            token_id: "t1".to_owned(),
            side: TradeSide::Buy,
            limit_price: 0.55,
            max_size: 100.0,
            policy: IntentPolicy::Passive,
            expires_at: Utc::now() + ChronoDuration::minutes(10),
            strategy_kind: StrategyKind::RulesDriven,
            thesis_ref: "thesis-1".to_owned(),
            research_ref: None,
            opportunity_id: Uuid::new_v4(),
            event_id: "e1".to_owned(),
        }
    }

    fn ctx(depth: f64) -> RiskContext {
        let mut ctx = RiskContext::with_now(portfolio(), budgets(), runtime_health());
        ctx.market_states
            .insert("m1".to_owned(), market_state(depth));
        ctx.strategy_states.insert(
            StrategyKind::RulesDriven,
            StrategyRiskState::healthy(StrategyKind::RulesDriven),
        );
        ctx
    }

    #[tokio::test]
    async fn approves_healthy_batch() {
        let engine = RiskEngineService::new(config());
        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Sim,
            created_at: Utc::now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![intent()],
        };

        let result = engine.evaluate_batch(batch, ctx(3.0)).await.expect("risk");
        match result {
            RiskEvaluation::Approved(approved) => {
                assert_eq!(approved.runtime_mode, RiskRuntimeMode::Normal);
                assert_eq!(approved.batch.intents.len(), 1);
                assert!((approved.scale - 1.0).abs() < 1e-9);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn reduces_batch_when_depth_is_shallow() {
        let engine = RiskEngineService::new(config());
        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Sim,
            created_at: Utc::now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![intent()],
        };

        let result = engine.evaluate_batch(batch, ctx(1.5)).await.expect("risk");
        match result {
            RiskEvaluation::Approved(approved) => {
                assert!(approved.scale < 1.0);
                assert!(approved
                    .findings
                    .iter()
                    .any(|finding| finding.code == RiskCode::BookDepthInsufficient));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rejects_when_runtime_is_safe() {
        let engine = RiskEngineService::new(config());
        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Sim,
            created_at: Utc::now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![intent()],
        };
        let mut context = ctx(3.0);
        context.runtime.heartbeat_age_ms = 20_000;

        let result = engine.evaluate_batch(batch, context).await.expect("risk");
        match result {
            RiskEvaluation::Rejected(rejection) => {
                assert_eq!(rejection.runtime_mode, RiskRuntimeMode::Safe);
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn runtime_transitions_to_safe_on_drift() {
        let engine = RiskEngineService::new(config());
        let mut context = ctx(3.0);
        context.runtime.reconcile_drift = true;

        let decision = engine.evaluate_runtime(&context).await.expect("runtime");
        assert_eq!(decision.mode, RiskRuntimeMode::Safe);
        assert!(decision.should_cancel_all);
    }

    #[tokio::test]
    async fn degraded_runtime_rejects_complex_multileg_entries() {
        let engine = RiskEngineService::new(config());
        let mut second = intent();
        second.market_id = "m2".to_owned();
        second.event_id = "e2".to_owned();

        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Sim,
            created_at: Utc::now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![intent(), second],
        };

        let mut context = ctx(3.0);
        context.market_states.insert(
            "m2".to_owned(),
            RiskMarketState {
                market_id: "m2".to_owned(),
                event_id: "e2".to_owned(),
                category: "Sports".to_owned(),
                resolution_source: None,
                tradable: true,
                rules_stable: true,
                ambiguous_rules: false,
                recent_rule_change_at: None,
                recent_clarification_at: None,
                event_starts_at: None,
                resolution_window_starts_at: None,
                book_depth_score: 3.0,
                fee_cost_bps: 10,
                tick_size: Some(0.01),
                min_order_size: Some(5.0),
            },
        );
        context.runtime.heartbeat_age_ms = 9_000;

        let result = engine.evaluate_batch(batch, context).await.expect("risk");
        match result {
            RiskEvaluation::Rejected(rejection) => {
                assert_eq!(rejection.runtime_mode, RiskRuntimeMode::Degraded);
                assert!(rejection
                    .findings
                    .iter()
                    .any(|finding| finding.code == RiskCode::RuntimeDegraded));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }

    #[tokio::test]
    async fn rollout_policy_blocks_disallowed_strategy() {
        let engine = RiskEngineService::new(config());
        let batch = TradeIntentBatch {
            account_domain: AccountDomain::Sim,
            created_at: Utc::now(),
            optimization_status: OptimizationStatus::Optimal,
            intents: vec![intent()],
        };
        let mut context = ctx(3.0);
        context.rollout_stage = Some(PromotionStage::Shadow);
        context.rollout_policy = Some(RolloutPolicy {
            stage: PromotionStage::Shadow,
            domain: AccountDomain::Sim,
            allowed_strategies: vec![StrategyKind::Rebalancing],
            max_total_notional: 10_000.0,
            max_batch_notional: 1_000.0,
            max_strategy_notional: 500.0,
            capabilities: polymarket_core::RolloutCapabilityMatrix {
                allow_real_execution: false,
                allow_multileg: false,
                allow_market_making: false,
                allow_auto_redemption: true,
                require_shadow_alignment: true,
            },
            mvp_flags: polymarket_core::MvpCapabilityFlags::default(),
        });

        let result = engine.evaluate_batch(batch, context).await.expect("risk");
        match result {
            RiskEvaluation::Rejected(rejection) => {
                assert!(rejection
                    .findings
                    .iter()
                    .any(|item| item.code == RiskCode::RolloutStrategyBlocked));
            }
            other => panic!("unexpected result: {other:?}"),
        }
    }
}
