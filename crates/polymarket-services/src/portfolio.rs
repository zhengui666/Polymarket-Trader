use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Duration as ChronoDuration;
use polymarket_core::{
    AccountDomain, AllocationPlan, IntentPolicy, OpportunityCandidate, OptimizationStatus,
    PortfolioRejectReason, PortfolioSnapshot, RiskBudgetSnapshot, RuntimeHealth, StrategyKind,
    TradeIntent, TradeIntentBatch, TradeSide,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyBucket {
    StructuralArb,
    NegRisk,
    RulesDriven,
    FeeAwareMm,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioOpportunity {
    pub opportunity: OpportunityCandidate,
    pub account_domain: AccountDomain,
    pub market_id: String,
    pub token_id: String,
    pub side: TradeSide,
    pub limit_price: f64,
    pub max_size: f64,
    pub category: String,
    pub resolution_source: Option<String>,
    pub fees_enabled: bool,
    pub neg_risk: bool,
    pub orderbook_depth_score: f64,
    pub book_age_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PortfolioConfig {
    pub min_edge_net_bps: i32,
    pub min_cash_buffer: f64,
    pub max_single_intent_notional: f64,
    pub max_per_intent_allocation_ratio: f64,
    pub structural_arb_ratio: f64,
    pub neg_risk_ratio: f64,
    pub rules_driven_ratio: f64,
    pub fee_mm_ratio: f64,
    pub max_candidates_for_solver: usize,
    pub solver_timeout: Duration,
    pub heuristic_fallback_enabled: bool,
    pub market_overlap_threshold: usize,
    pub degraded_multileg_scale: f64,
    pub intent_ttl: Duration,
    pub max_category_group_notional: f64,
    pub max_resolution_source_group_notional: f64,
}

impl Default for PortfolioConfig {
    fn default() -> Self {
        Self {
            min_edge_net_bps: 8,
            min_cash_buffer: 250.0,
            max_single_intent_notional: 1_500.0,
            max_per_intent_allocation_ratio: 0.15,
            structural_arb_ratio: 0.40,
            neg_risk_ratio: 0.20,
            rules_driven_ratio: 0.25,
            fee_mm_ratio: 0.15,
            max_candidates_for_solver: 18,
            solver_timeout: Duration::from_millis(25),
            heuristic_fallback_enabled: true,
            market_overlap_threshold: 2,
            degraded_multileg_scale: 0.35,
            intent_ttl: Duration::from_secs(45),
            max_category_group_notional: 3_000.0,
            max_resolution_source_group_notional: 2_500.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BucketUsage {
    pub bucket: StrategyBucket,
    pub budget: f64,
    pub used: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioCandidateDecision {
    pub opportunity_id: uuid::Uuid,
    pub strategy_kind: StrategyKind,
    pub market_id: String,
    pub event_id: String,
    pub selected: bool,
    pub planned_notional: f64,
    pub adjusted_score: f64,
    pub reject_reason: Option<PortfolioRejectReason>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioAuditTrail {
    pub optimization_status: OptimizationStatus,
    pub fallback_reason: Option<String>,
    pub total_selected_notional: f64,
    pub bucket_usage: Vec<BucketUsage>,
    pub decisions: Vec<PortfolioCandidateDecision>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioResult {
    pub allocation_plan: AllocationPlan,
    pub trade_intent_batch: TradeIntentBatch,
    pub audit_trail: PortfolioAuditTrail,
}

#[derive(Debug, Error)]
pub enum PortfolioError {
    #[error("portfolio engine requires at least one opportunity")]
    EmptyInput,
    #[error("portfolio engine received mixed account domains")]
    MixedDomains,
    #[error("portfolio engine budgets are invalid: {0}")]
    InvalidBudget(String),
    #[error("portfolio engine could not produce a feasible plan and fallback is disabled")]
    Infeasible,
}

#[async_trait]
pub trait PortfolioEngine {
    async fn build_plan(
        &self,
        opportunities: Vec<PortfolioOpportunity>,
        portfolio: PortfolioSnapshot,
        runtime: RuntimeHealth,
        budgets: RiskBudgetSnapshot,
    ) -> Result<PortfolioResult, PortfolioError>;
}

#[derive(Debug, Clone)]
pub struct PortfolioEngineService {
    config: PortfolioConfig,
}

impl Default for PortfolioEngineService {
    fn default() -> Self {
        Self::new(PortfolioConfig::default())
    }
}

impl PortfolioEngineService {
    pub fn new(config: PortfolioConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl PortfolioEngine for PortfolioEngineService {
    async fn build_plan(
        &self,
        opportunities: Vec<PortfolioOpportunity>,
        portfolio: PortfolioSnapshot,
        runtime: RuntimeHealth,
        budgets: RiskBudgetSnapshot,
    ) -> Result<PortfolioResult, PortfolioError> {
        let domain = validate_domain(&opportunities)?;
        validate_budgets(&budgets)?;

        let allocation_plan = build_allocation_plan(&self.config, &portfolio, &budgets);
        let baseline_usage = UsageLedger::from_portfolio(&portfolio);
        let normalized = normalize_candidates(
            &opportunities,
            &self.config,
            &runtime,
            &allocation_plan,
            &budgets,
        );

        let (valid_candidates, mut decisions) = split_candidate_decisions(normalized);
        let selection = optimize_selection(
            &valid_candidates,
            &self.config,
            &allocation_plan,
            &budgets,
            &baseline_usage,
        );

        let (selected, status, fallback_reason) = match selection {
            SelectionOutcome::Optimal(indices) => (indices, OptimizationStatus::Optimal, None),
            SelectionOutcome::TimedOutOrInfeasible(reason)
                if self.config.heuristic_fallback_enabled =>
            {
                (
                    greedy_fallback(
                        &valid_candidates,
                        &self.config,
                        &allocation_plan,
                        &budgets,
                        &baseline_usage,
                    ),
                    OptimizationStatus::HeuristicFallback,
                    Some(reason),
                )
            }
            SelectionOutcome::TimedOutOrInfeasible(_) => return Err(PortfolioError::Infeasible),
        };

        let final_state = apply_selected(&selected, &valid_candidates, &baseline_usage);
        let mut selected_set = BTreeSet::new();
        for index in &selected {
            selected_set.insert(*index);
        }

        decisions.extend(
            valid_candidates
                .iter()
                .enumerate()
                .map(|(index, candidate)| {
                    let reject_reason = if selected_set.contains(&index) {
                        None
                    } else {
                        derive_reject_reason(candidate, &final_state)
                    };
                    PortfolioCandidateDecision {
                        opportunity_id: candidate.opportunity.opportunity.opportunity_id,
                        strategy_kind: candidate.opportunity.opportunity.strategy,
                        market_id: candidate.opportunity.market_id.clone(),
                        event_id: candidate.opportunity.opportunity.event_id.clone(),
                        selected: selected_set.contains(&index),
                        planned_notional: if selected_set.contains(&index) {
                            candidate.target_notional
                        } else {
                            0.0
                        },
                        adjusted_score: candidate.adjusted_score,
                        reject_reason,
                    }
                }),
        );
        decisions.sort_by_key(|decision| decision.opportunity_id);

        let intents = selected
            .iter()
            .map(|index| build_intent(domain, runtime.now, &self.config, &valid_candidates[*index]))
            .collect::<Vec<_>>();

        let batch = TradeIntentBatch {
            account_domain: domain,
            created_at: runtime.now,
            optimization_status: status,
            intents,
        };

        let audit_trail = PortfolioAuditTrail {
            optimization_status: status,
            fallback_reason,
            total_selected_notional: final_state.total_used,
            bucket_usage: StrategyBucket::ordered()
                .into_iter()
                .map(|bucket| BucketUsage {
                    bucket,
                    budget: bucket_budget(&allocation_plan, bucket),
                    used: final_state
                        .bucket_usage
                        .get(&bucket)
                        .copied()
                        .unwrap_or_default(),
                })
                .collect(),
            decisions,
        };

        Ok(PortfolioResult {
            allocation_plan,
            trade_intent_batch: batch,
            audit_trail,
        })
    }
}

#[derive(Debug, Clone)]
struct CandidateInput {
    opportunity: PortfolioOpportunity,
    bucket: StrategyBucket,
    adjusted_score: f64,
    target_notional: f64,
    expected_value: f64,
    max_overlap: usize,
}

#[derive(Debug, Clone)]
enum CandidateEval {
    Accepted(CandidateInput),
    Rejected(PortfolioCandidateDecision),
}

#[derive(Debug, Clone)]
enum SelectionOutcome {
    Optimal(Vec<usize>),
    TimedOutOrInfeasible(String),
}

#[derive(Debug, Clone, Default)]
struct UsageLedger {
    total_used: f64,
    bucket_usage: BTreeMap<StrategyBucket, f64>,
    event_usage: HashMap<String, f64>,
    category_usage: HashMap<String, f64>,
    market_usage: HashMap<String, f64>,
    resolution_usage: HashMap<String, f64>,
    selected_events: HashSet<String>,
    selected_markets: HashSet<String>,
}

impl UsageLedger {
    fn from_portfolio(portfolio: &PortfolioSnapshot) -> Self {
        let mut ledger = Self {
            total_used: portfolio.positions.iter().map(position_notional).sum(),
            ..Self::default()
        };

        for position in &portfolio.positions {
            let notional = position_notional(position);
            *ledger
                .event_usage
                .entry(position.event_id.clone())
                .or_default() += notional;
            *ledger
                .category_usage
                .entry(position.category.clone())
                .or_default() += notional;
            *ledger
                .market_usage
                .entry(position.market_id.clone())
                .or_default() += notional;
            ledger.selected_events.insert(position.event_id.clone());
            ledger.selected_markets.insert(position.market_id.clone());
        }

        ledger
    }

    fn can_accept(
        &self,
        candidate: &CandidateInput,
        allocation_plan: &AllocationPlan,
        budgets: &RiskBudgetSnapshot,
    ) -> Option<PortfolioRejectReason> {
        let target = candidate.target_notional;
        let event_id = &candidate.opportunity.opportunity.event_id;
        let category = &candidate.opportunity.category;
        let market_id = &candidate.opportunity.market_id;

        if self.total_used + target > total_budget(allocation_plan) + 1e-9 {
            return Some(PortfolioRejectReason::CashBuffer);
        }

        if self
            .bucket_usage
            .get(&candidate.bucket)
            .copied()
            .unwrap_or_default()
            + target
            > bucket_budget(allocation_plan, candidate.bucket) + 1e-9
        {
            return Some(PortfolioRejectReason::StrategyBudget);
        }

        if self
            .market_usage
            .get(market_id)
            .copied()
            .unwrap_or_default()
            + target
            > budgets.max_market_notional + 1e-9
        {
            return Some(PortfolioRejectReason::MarketCorrelation);
        }

        if self.event_usage.get(event_id).copied().unwrap_or_default() + target
            > budgets.max_event_notional + 1e-9
        {
            return Some(PortfolioRejectReason::EventCorrelation);
        }

        if !candidate.opportunity.neg_risk && self.selected_events.contains(event_id) {
            return Some(PortfolioRejectReason::EventCorrelation);
        }

        if self
            .category_usage
            .get(category)
            .copied()
            .unwrap_or_default()
            + target
            > budgets.max_category_notional + 1e-9
        {
            return Some(PortfolioRejectReason::CategoryCorrelation);
        }

        if let Some(source) = candidate.opportunity.resolution_source.as_ref() {
            if self
                .resolution_usage
                .get(source)
                .copied()
                .unwrap_or_default()
                + target
                > budgets.max_category_notional + 1e-9
            {
                return Some(PortfolioRejectReason::ResolutionSourceCorrelation);
            }
        }

        if self.selected_markets.contains(market_id) {
            return Some(PortfolioRejectReason::MarketCorrelation);
        }

        None
    }

    fn accept(&mut self, candidate: &CandidateInput) {
        let notional = candidate.target_notional;
        *self.bucket_usage.entry(candidate.bucket).or_default() += notional;
        *self
            .event_usage
            .entry(candidate.opportunity.opportunity.event_id.clone())
            .or_default() += notional;
        *self
            .category_usage
            .entry(candidate.opportunity.category.clone())
            .or_default() += notional;
        *self
            .market_usage
            .entry(candidate.opportunity.market_id.clone())
            .or_default() += notional;
        if let Some(source) = candidate.opportunity.resolution_source.clone() {
            *self.resolution_usage.entry(source).or_default() += notional;
        }
        self.selected_events
            .insert(candidate.opportunity.opportunity.event_id.clone());
        self.selected_markets
            .insert(candidate.opportunity.market_id.clone());
        self.total_used += notional;
    }
}

impl StrategyBucket {
    fn from_strategy(strategy: StrategyKind) -> Self {
        match strategy {
            StrategyKind::Rebalancing | StrategyKind::DependencyArb => Self::StructuralArb,
            StrategyKind::NegRisk => Self::NegRisk,
            StrategyKind::RulesDriven => Self::RulesDriven,
            StrategyKind::FeeAwareMM => Self::FeeAwareMm,
        }
    }

    fn ordered() -> [Self; 4] {
        [
            Self::StructuralArb,
            Self::NegRisk,
            Self::RulesDriven,
            Self::FeeAwareMm,
        ]
    }
}

fn validate_domain(
    opportunities: &[PortfolioOpportunity],
) -> Result<AccountDomain, PortfolioError> {
    let first = opportunities.first().ok_or(PortfolioError::EmptyInput)?;
    if opportunities
        .iter()
        .any(|candidate| candidate.account_domain != first.account_domain)
    {
        return Err(PortfolioError::MixedDomains);
    }
    Ok(first.account_domain)
}

fn validate_budgets(budgets: &RiskBudgetSnapshot) -> Result<(), PortfolioError> {
    let values = [
        budgets.max_structural_arb_notional,
        budgets.max_neg_risk_notional,
        budgets.max_rules_driven_notional,
        budgets.max_fee_mm_notional,
        budgets.min_cash_buffer,
        budgets.max_single_intent_notional,
        budgets.max_event_notional,
        budgets.max_category_notional,
        budgets.max_market_notional,
        budgets.max_unresolved_notional,
    ];

    if values
        .iter()
        .any(|value| !value.is_finite() || *value < 0.0)
    {
        return Err(PortfolioError::InvalidBudget(
            "all notional limits must be finite and non-negative".to_owned(),
        ));
    }

    Ok(())
}

fn build_allocation_plan(
    config: &PortfolioConfig,
    portfolio: &PortfolioSnapshot,
    budgets: &RiskBudgetSnapshot,
) -> AllocationPlan {
    let cash_buffer_target = config.min_cash_buffer.max(budgets.min_cash_buffer);
    let allocatable_cash =
        (portfolio.cash_available - portfolio.reserved_cash - cash_buffer_target)
            .max(0.0)
            .min((budgets.max_unresolved_notional - portfolio.unresolved_capital).max(0.0));

    AllocationPlan {
        structural_arb_budget: allocatable_cash
            .mul_add(config.structural_arb_ratio, 0.0)
            .min(budgets.max_structural_arb_notional),
        neg_risk_budget: allocatable_cash
            .mul_add(config.neg_risk_ratio, 0.0)
            .min(budgets.max_neg_risk_notional),
        rules_driven_budget: allocatable_cash
            .mul_add(config.rules_driven_ratio, 0.0)
            .min(budgets.max_rules_driven_notional),
        fee_mm_budget: allocatable_cash
            .mul_add(config.fee_mm_ratio, 0.0)
            .min(budgets.max_fee_mm_notional),
        cash_buffer_target,
    }
}

fn normalize_candidates(
    opportunities: &[PortfolioOpportunity],
    config: &PortfolioConfig,
    runtime: &RuntimeHealth,
    allocation_plan: &AllocationPlan,
    budgets: &RiskBudgetSnapshot,
) -> Vec<CandidateEval> {
    opportunities
        .iter()
        .cloned()
        .map(|opportunity| {
            normalize_candidate(opportunity, config, runtime, allocation_plan, budgets)
        })
        .collect()
}

fn normalize_candidate(
    opportunity: PortfolioOpportunity,
    config: &PortfolioConfig,
    runtime: &RuntimeHealth,
    allocation_plan: &AllocationPlan,
    budgets: &RiskBudgetSnapshot,
) -> CandidateEval {
    let bucket = StrategyBucket::from_strategy(opportunity.opportunity.strategy);
    let decision = |reason, score| {
        CandidateEval::Rejected(PortfolioCandidateDecision {
            opportunity_id: opportunity.opportunity.opportunity_id,
            strategy_kind: opportunity.opportunity.strategy,
            market_id: opportunity.market_id.clone(),
            event_id: opportunity.opportunity.event_id.clone(),
            selected: false,
            planned_notional: 0.0,
            adjusted_score: score,
            reject_reason: Some(reason),
        })
    };

    if opportunity.opportunity.edge_net_bps < config.min_edge_net_bps {
        return decision(PortfolioRejectReason::EdgeTooLow, 0.0);
    }

    if !opportunity.limit_price.is_finite()
        || opportunity.limit_price <= 0.0
        || !opportunity.max_size.is_finite()
        || opportunity.max_size <= 0.0
    {
        return decision(PortfolioRejectReason::InvalidQuote, 0.0);
    }

    let bucket_budget_cap = bucket_budget(allocation_plan, bucket);
    if bucket_budget_cap <= 0.0 {
        return decision(PortfolioRejectReason::StrategyBudget, 0.0);
    }

    let health_penalty = runtime.execution_penalty();
    if opportunity.opportunity.needs_multileg
        && runtime.is_degraded()
        && config.degraded_multileg_scale <= 0.0
    {
        return decision(PortfolioRejectReason::RuntimeDegraded, 0.0);
    }

    let depth_factor = opportunity.orderbook_depth_score.clamp(0.25, 1.25);
    let turnover_factor = 1.0 / (1.0 + opportunity.opportunity.capital_lock_days.max(0.0) as f64);
    let half_life_factor = (opportunity.opportunity.half_life_sec as f64
        / (opportunity.opportunity.half_life_sec as f64 + 600.0))
        .clamp(0.2, 1.0);
    let multileg_penalty = if opportunity.opportunity.needs_multileg {
        0.82
    } else {
        1.0
    };
    let fee_penalty = if !opportunity.fees_enabled && bucket == StrategyBucket::FeeAwareMm {
        0.75
    } else {
        1.0
    };
    let aged_book_penalty = if opportunity.book_age_ms >= 5_000 {
        0.8
    } else {
        1.0
    };
    let adjusted_score = ((opportunity.opportunity.edge_net_bps.max(0) as f64 / 10_000.0)
        * opportunity.opportunity.confidence as f64
        * turnover_factor
        * half_life_factor
        * depth_factor
        * health_penalty
        * multileg_penalty
        * fee_penalty
        * aged_book_penalty)
        .max(0.0);

    let raw_capacity = opportunity.limit_price * opportunity.max_size;
    let per_intent_cap = total_budget(allocation_plan) * config.max_per_intent_allocation_ratio;
    let mut target_notional = raw_capacity
        .min(config.max_single_intent_notional)
        .min(budgets.max_single_intent_notional)
        .min(bucket_budget_cap)
        .min(per_intent_cap.max(0.0));

    if opportunity.opportunity.needs_multileg && runtime.is_degraded() {
        target_notional *= config.degraded_multileg_scale;
    }

    if target_notional <= 0.0 {
        return decision(PortfolioRejectReason::CapacityExceeded, adjusted_score);
    }

    CandidateEval::Accepted(CandidateInput {
        max_overlap: market_overlap(&opportunity),
        expected_value: target_notional * adjusted_score,
        target_notional,
        adjusted_score,
        bucket,
        opportunity,
    })
}

fn split_candidate_decisions(
    normalized: Vec<CandidateEval>,
) -> (Vec<CandidateInput>, Vec<PortfolioCandidateDecision>) {
    let mut accepted = Vec::new();
    let mut rejected = Vec::new();

    for item in normalized {
        match item {
            CandidateEval::Accepted(candidate) => accepted.push(candidate),
            CandidateEval::Rejected(decision) => rejected.push(decision),
        }
    }

    accepted.sort_by(compare_candidates);
    (accepted, rejected)
}

fn optimize_selection(
    candidates: &[CandidateInput],
    config: &PortfolioConfig,
    allocation_plan: &AllocationPlan,
    budgets: &RiskBudgetSnapshot,
    baseline_usage: &UsageLedger,
) -> SelectionOutcome {
    if candidates.is_empty() {
        return SelectionOutcome::Optimal(Vec::new());
    }

    let limit = candidates.len().min(config.max_candidates_for_solver);
    let shortlisted = &candidates[..limit];
    let started_at = Instant::now();
    let value_suffix = build_value_suffix(shortlisted);

    let mut best = SearchBest::default();
    let mut path = Vec::new();
    branch_and_bound(
        0,
        shortlisted,
        baseline_usage.clone(),
        allocation_plan,
        budgets,
        started_at,
        config.solver_timeout,
        &value_suffix,
        0.0,
        &mut path,
        &mut best,
    );

    if best.timed_out {
        return SelectionOutcome::TimedOutOrInfeasible(
            "solver timeout reached, using heuristic fallback".to_owned(),
        );
    }

    if best.indices.is_empty()
        && shortlisted.iter().all(|candidate| {
            baseline_usage
                .can_accept(candidate, allocation_plan, budgets)
                .is_some()
        })
    {
        return SelectionOutcome::TimedOutOrInfeasible(
            "no feasible portfolio under current constraints".to_owned(),
        );
    }

    SelectionOutcome::Optimal(best.indices)
}

#[derive(Debug, Default)]
struct SearchBest {
    value: f64,
    indices: Vec<usize>,
    timed_out: bool,
}

#[allow(clippy::too_many_arguments)]
fn branch_and_bound(
    index: usize,
    candidates: &[CandidateInput],
    usage: UsageLedger,
    allocation_plan: &AllocationPlan,
    budgets: &RiskBudgetSnapshot,
    started_at: Instant,
    timeout: Duration,
    value_suffix: &[f64],
    current_value: f64,
    path: &mut Vec<usize>,
    best: &mut SearchBest,
) {
    if started_at.elapsed() >= timeout {
        best.timed_out = true;
        return;
    }

    if index == candidates.len() {
        if current_value > best.value {
            best.value = current_value;
            best.indices = path.clone();
        }
        return;
    }

    if current_value + value_suffix[index] <= best.value {
        return;
    }

    let candidate = &candidates[index];
    if usage
        .can_accept(candidate, allocation_plan, budgets)
        .is_none()
    {
        let mut next_usage = usage.clone();
        next_usage.accept(candidate);
        path.push(index);
        branch_and_bound(
            index + 1,
            candidates,
            next_usage,
            allocation_plan,
            budgets,
            started_at,
            timeout,
            value_suffix,
            current_value + candidate.expected_value,
            path,
            best,
        );
        path.pop();
    }

    branch_and_bound(
        index + 1,
        candidates,
        usage,
        allocation_plan,
        budgets,
        started_at,
        timeout,
        value_suffix,
        current_value,
        path,
        best,
    );
}

fn greedy_fallback(
    candidates: &[CandidateInput],
    _config: &PortfolioConfig,
    allocation_plan: &AllocationPlan,
    budgets: &RiskBudgetSnapshot,
    baseline_usage: &UsageLedger,
) -> Vec<usize> {
    let mut usage = baseline_usage.clone();
    let mut selected = Vec::new();

    for (index, candidate) in candidates.iter().enumerate() {
        if usage
            .can_accept(candidate, allocation_plan, budgets)
            .is_none()
        {
            usage.accept(candidate);
            selected.push(index);
        }
    }

    selected
}

fn apply_selected(
    selected: &[usize],
    candidates: &[CandidateInput],
    baseline_usage: &UsageLedger,
) -> UsageLedger {
    let mut usage = baseline_usage.clone();
    let mut ordered = selected.to_vec();
    ordered.sort_by(|lhs, rhs| compare_candidates(&candidates[*lhs], &candidates[*rhs]));

    for index in ordered {
        usage.accept(&candidates[index]);
    }
    usage
}

fn derive_reject_reason(
    candidate: &CandidateInput,
    ledger: &UsageLedger,
) -> Option<PortfolioRejectReason> {
    let event_id = &candidate.opportunity.opportunity.event_id;
    let category = &candidate.opportunity.category;
    let market_id = &candidate.opportunity.market_id;

    if ledger.selected_events.contains(event_id) && !candidate.opportunity.neg_risk {
        return Some(PortfolioRejectReason::EventCorrelation);
    }
    if ledger.selected_markets.contains(market_id) || candidate.max_overlap >= 2 {
        return Some(PortfolioRejectReason::MarketCorrelation);
    }
    if ledger
        .category_usage
        .get(category)
        .copied()
        .unwrap_or_default()
        > 0.0
    {
        return Some(PortfolioRejectReason::CategoryCorrelation);
    }
    if candidate.target_notional <= 0.0 {
        return Some(PortfolioRejectReason::CapacityExceeded);
    }
    Some(PortfolioRejectReason::StrategyBudget)
}

fn build_intent(
    domain: AccountDomain,
    reference_time: polymarket_core::Timestamp,
    config: &PortfolioConfig,
    candidate: &CandidateInput,
) -> TradeIntent {
    let expires_at = reference_time
        + ChronoDuration::seconds(config.intent_ttl.as_secs().min(i64::MAX as u64) as i64);

    TradeIntent {
        account_domain: domain,
        market_id: candidate.opportunity.market_id.clone(),
        token_id: candidate.opportunity.token_id.clone(),
        side: candidate.opportunity.side,
        limit_price: candidate.opportunity.limit_price,
        max_size: candidate.target_notional / candidate.opportunity.limit_price,
        policy: if candidate.opportunity.opportunity.needs_multileg {
            IntentPolicy::Shrinkable
        } else {
            IntentPolicy::AggressiveLimit
        },
        expires_at,
        strategy_kind: candidate.opportunity.opportunity.strategy,
        thesis_ref: candidate.opportunity.opportunity.thesis_ref.clone(),
        research_ref: candidate.opportunity.opportunity.research_ref.clone(),
        opportunity_id: candidate.opportunity.opportunity.opportunity_id,
        event_id: candidate.opportunity.opportunity.event_id.clone(),
    }
}

fn build_value_suffix(candidates: &[CandidateInput]) -> Vec<f64> {
    let mut suffix = vec![0.0; candidates.len() + 1];
    for index in (0..candidates.len()).rev() {
        suffix[index] = suffix[index + 1] + candidates[index].expected_value.max(0.0);
    }
    suffix
}

fn compare_candidates(lhs: &CandidateInput, rhs: &CandidateInput) -> Ordering {
    rhs.adjusted_score
        .partial_cmp(&lhs.adjusted_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            rhs.expected_value
                .partial_cmp(&lhs.expected_value)
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| lhs.opportunity.market_id.cmp(&rhs.opportunity.market_id))
}

fn position_notional(position: &polymarket_core::PositionSnapshot) -> f64 {
    (position.quantity.abs() * position.mark_price.abs()) + position.reserved_notional.abs()
}

fn total_budget(plan: &AllocationPlan) -> f64 {
    plan.structural_arb_budget
        + plan.neg_risk_budget
        + plan.rules_driven_budget
        + plan.fee_mm_budget
}

fn bucket_budget(plan: &AllocationPlan, bucket: StrategyBucket) -> f64 {
    match bucket {
        StrategyBucket::StructuralArb => plan.structural_arb_budget,
        StrategyBucket::NegRisk => plan.neg_risk_budget,
        StrategyBucket::RulesDriven => plan.rules_driven_budget,
        StrategyBucket::FeeAwareMm => plan.fee_mm_budget,
    }
}

fn market_overlap(opportunity: &PortfolioOpportunity) -> usize {
    let unique_refs = opportunity
        .opportunity
        .market_refs
        .iter()
        .collect::<HashSet<_>>()
        .len();
    opportunity
        .opportunity
        .market_refs
        .len()
        .saturating_sub(unique_refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use polymarket_core::{
        OpportunityCandidate, PortfolioSnapshot, PositionSnapshot, RuntimeHealth,
    };

    fn sample_runtime_health() -> RuntimeHealth {
        RuntimeHealth {
            domain: AccountDomain::Canary,
            runtime_mode: "SHADOW".to_owned(),
            now: polymarket_core::now(),
            market_ws_lag_ms: 100,
            user_ws_ok: true,
            heartbeat_age_ms: 100,
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
        }
    }

    fn sample_portfolio() -> PortfolioSnapshot {
        PortfolioSnapshot {
            nav: 10_000.0,
            cash_available: 4_000.0,
            reserved_cash: 250.0,
            positions: Vec::new(),
            unresolved_capital: 500.0,
            redeemable_capital: 0.0,
        }
    }

    fn sample_budgets() -> RiskBudgetSnapshot {
        RiskBudgetSnapshot {
            max_structural_arb_notional: 2_000.0,
            max_neg_risk_notional: 1_500.0,
            max_rules_driven_notional: 1_500.0,
            max_fee_mm_notional: 1_000.0,
            min_cash_buffer: 500.0,
            max_single_intent_notional: 1_200.0,
            max_event_notional: 1_200.0,
            max_category_notional: 2_000.0,
            max_market_notional: 1_200.0,
            max_unresolved_notional: 5_000.0,
        }
    }

    fn sample_candidate(
        suffix: &str,
        strategy: StrategyKind,
        event_id: &str,
        category: &str,
        edge_net_bps: i32,
    ) -> PortfolioOpportunity {
        PortfolioOpportunity {
            opportunity: OpportunityCandidate {
                opportunity_id: uuid::Uuid::new_v4(),
                strategy,
                market_refs: vec![format!("m-{suffix}")],
                event_id: event_id.to_owned(),
                edge_gross_bps: edge_net_bps + 10,
                fee_cost_bps: 2,
                slippage_cost_bps: 2,
                failure_risk_bps: 1,
                edge_net_bps,
                confidence: 0.9,
                half_life_sec: 1_200,
                capital_lock_days: 1.0,
                needs_multileg: false,
                thesis_ref: format!("thesis-{suffix}"),
                research_ref: None,
                llm_review: None,
                graph_version: 1,
                book_observed_at: polymarket_core::now(),
                created_at: polymarket_core::now(),
                invalidated_at: None,
                invalidation_reason: None,
            },
            account_domain: AccountDomain::Canary,
            market_id: format!("market-{suffix}"),
            token_id: format!("token-{suffix}"),
            side: TradeSide::Buy,
            limit_price: 0.54,
            max_size: 1_500.0,
            category: category.to_owned(),
            resolution_source: Some("manual".to_owned()),
            fees_enabled: true,
            neg_risk: strategy == StrategyKind::NegRisk,
            orderbook_depth_score: 1.0,
            book_age_ms: 100,
        }
    }

    #[tokio::test]
    async fn enforces_same_event_diversification() {
        let service = PortfolioEngineService::default();
        let result = service
            .build_plan(
                vec![
                    sample_candidate("a", StrategyKind::Rebalancing, "event-1", "sports", 28),
                    sample_candidate("b", StrategyKind::DependencyArb, "event-1", "sports", 24),
                    sample_candidate("c", StrategyKind::RulesDriven, "event-2", "politics", 22),
                ],
                sample_portfolio(),
                sample_runtime_health(),
                sample_budgets(),
            )
            .await
            .expect("portfolio plan");

        let event_one_count = result
            .trade_intent_batch
            .intents
            .iter()
            .filter(|intent| intent.event_id == "event-1")
            .count();
        assert_eq!(event_one_count, 1);
    }

    #[tokio::test]
    async fn degraded_runtime_shrinks_multileg_capacity() {
        let service = PortfolioEngineService::default();
        let mut candidate = sample_candidate("m", StrategyKind::NegRisk, "event-3", "crypto", 35);
        candidate.opportunity.needs_multileg = true;

        let healthy = service
            .build_plan(
                vec![candidate.clone()],
                sample_portfolio(),
                sample_runtime_health(),
                sample_budgets(),
            )
            .await
            .expect("healthy result");

        let mut degraded = sample_runtime_health();
        degraded.market_ws_lag_ms = 3_500;
        degraded.user_ws_ok = false;
        let degraded_result = service
            .build_plan(
                vec![candidate],
                sample_portfolio(),
                degraded,
                sample_budgets(),
            )
            .await
            .expect("degraded result");

        assert!(
            degraded_result.trade_intent_batch.intents[0].max_size
                < healthy.trade_intent_batch.intents[0].max_size
        );
    }

    #[tokio::test]
    async fn respects_cash_buffer_and_bucket_caps() {
        let service = PortfolioEngineService::default();
        let result = service
            .build_plan(
                vec![
                    sample_candidate("x", StrategyKind::Rebalancing, "event-4", "sports", 26),
                    sample_candidate("y", StrategyKind::Rebalancing, "event-5", "sports", 25),
                    sample_candidate("z", StrategyKind::Rebalancing, "event-6", "sports", 24),
                ],
                sample_portfolio(),
                sample_runtime_health(),
                sample_budgets(),
            )
            .await
            .expect("result");

        let total_notional: f64 = result
            .trade_intent_batch
            .intents
            .iter()
            .map(|intent| intent.max_size * intent.limit_price)
            .sum();
        assert!(total_notional <= result.allocation_plan.structural_arb_budget + 1e-9);
    }

    #[tokio::test]
    async fn serializes_audit_output() {
        let service = PortfolioEngineService::default();
        let result = service
            .build_plan(
                vec![sample_candidate(
                    "s",
                    StrategyKind::RulesDriven,
                    "event-7",
                    "news",
                    18,
                )],
                sample_portfolio(),
                sample_runtime_health(),
                sample_budgets(),
            )
            .await
            .expect("result");

        let payload = serde_json::to_string(&result.audit_trail).expect("serialize audit");
        assert!(payload.contains("optimization_status"));
    }

    #[tokio::test]
    async fn accounts_for_existing_exposure() {
        let service = PortfolioEngineService::default();
        let mut portfolio = sample_portfolio();
        portfolio.positions.push(PositionSnapshot {
            market_id: "market-a".to_owned(),
            token_id: "token-a".to_owned(),
            event_id: "event-8".to_owned(),
            category: "sports".to_owned(),
            quantity: 1_000.0,
            average_price: 0.6,
            mark_price: 0.6,
            reserved_notional: 100.0,
            unrealized_pnl: 0.0,
        });

        let result = service
            .build_plan(
                vec![
                    sample_candidate("a", StrategyKind::Rebalancing, "event-8", "sports", 30),
                    sample_candidate("b", StrategyKind::RulesDriven, "event-9", "politics", 21),
                ],
                portfolio,
                sample_runtime_health(),
                sample_budgets(),
            )
            .await
            .expect("result");

        assert!(result
            .audit_trail
            .decisions
            .iter()
            .any(|decision| decision.event_id == "event-8" && !decision.selected));
    }
}
