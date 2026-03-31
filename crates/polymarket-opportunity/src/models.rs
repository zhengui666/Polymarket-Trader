use std::collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap};
use std::hash::{Hash, Hasher};

use chrono::Utc;
use polymarket_config::{LlmTaskKind, ModelTier, ResolvedLlmTaskConfig, SearchConfig};
use polymarket_core::{
    now, AccountDomain, ConstraintGraphSnapshot, MarketCanonical, MarketSnapshot,
    OpportunityCandidate, OpportunityScore, ScannerRunReport, StrategyKind, Timestamp,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ScanSettings {
    pub min_edge_net_bps: i32,
    pub max_market_refs_per_candidate: usize,
    pub max_candidates_per_event: usize,
    pub max_candidates_total: usize,
    pub fee_mm_enabled: bool,
    pub shadow_only_strategies: BTreeSet<StrategyKind>,
    pub default_half_life_secs: u32,
    pub book_stale_warn_ms: u64,
    pub book_stale_stop_ms: u64,
    pub max_book_age_for_multileg_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RuntimeHealth {
    pub domain: AccountDomain,
    pub runtime_mode: String,
    pub now: Timestamp,
}

#[derive(Debug, Clone)]
pub struct OrderbookSnapshotSet {
    books: HashMap<String, MarketSnapshot>,
}

impl OrderbookSnapshotSet {
    pub fn new(books: Vec<MarketSnapshot>) -> Self {
        let books = books
            .into_iter()
            .map(|snapshot| (snapshot.market_id.clone(), snapshot))
            .collect();
        Self { books }
    }

    pub fn get(&self, market_id: &str) -> Option<&MarketSnapshot> {
        self.books.get(market_id)
    }
}

#[derive(Debug, Clone)]
pub struct ScanContext {
    pub graph: ConstraintGraphSnapshot,
    pub books: OrderbookSnapshotSet,
    pub health: RuntimeHealth,
    pub settings: ScanSettings,
    pub llm: Option<LlmEvaluationConfig>,
}

#[derive(Debug, Clone)]
pub struct LlmEvaluationConfig {
    pub task: LlmTaskKind,
    pub tier: ModelTier,
    pub provider: String,
    pub base_url: String,
    pub api_key: String,
    pub model_name: String,
    pub fallback_models: Vec<LlmFallbackConfig>,
    pub max_candidates: usize,
    pub veto_enabled: bool,
    pub search: Option<SearchConfig>,
}

#[derive(Debug, Clone)]
pub struct LlmFallbackConfig {
    pub tier: ModelTier,
    pub provider: String,
    pub base_url: String,
    pub api_key: String,
    pub model_name: String,
}

impl LlmEvaluationConfig {
    pub fn from_resolved(
        resolved: ResolvedLlmTaskConfig,
        max_candidates: usize,
        search: Option<SearchConfig>,
        fallbacks: Vec<LlmFallbackConfig>,
    ) -> Self {
        Self {
            task: resolved.task,
            tier: resolved.tier,
            provider: resolved.provider,
            base_url: resolved.base_url,
            api_key: resolved.api_key,
            model_name: resolved.model_name,
            fallback_models: fallbacks,
            max_candidates,
            veto_enabled: false,
            search,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceEvidence {
    pub title: String,
    pub url: String,
    pub snippet: String,
    pub source_domain: String,
    pub published_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResearchEnvelope {
    pub query: String,
    pub generated_at: Timestamp,
    pub summary: String,
    pub risks: Vec<String>,
    pub contradictions: Vec<String>,
    pub evidence_gaps: Vec<String>,
    pub confidence_note: String,
    pub source_links: Vec<String>,
    pub sources: Vec<SourceEvidence>,
}

#[derive(Debug, Clone)]
pub struct ProposedCandidate {
    pub strategy: StrategyKind,
    pub market_refs: Vec<String>,
    pub event_id: String,
    pub score: OpportunityScore,
    pub needs_multileg: bool,
    pub thesis_ref: String,
    pub book_observed_at: Timestamp,
}

#[derive(Debug, Clone)]
pub struct ScannerOutcome {
    pub strategy: StrategyKind,
    pub candidates: Vec<ProposedCandidate>,
    pub rejection_reasons: Vec<String>,
}

impl ScannerOutcome {
    pub fn empty(strategy: StrategyKind) -> Self {
        Self {
            strategy,
            candidates: Vec::new(),
            rejection_reasons: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScanOutput {
    pub candidates: Vec<OpportunityCandidate>,
    pub reports: Vec<ScannerRunReport>,
}

pub fn market_mid(snapshot: &MarketSnapshot) -> Option<f64> {
    snapshot.book.as_ref()?.mid_price
}

pub fn best_bid(snapshot: &MarketSnapshot) -> Option<f64> {
    snapshot
        .book
        .as_ref()?
        .best_bid
        .as_ref()
        .map(|level| level.price)
}

pub fn best_ask(snapshot: &MarketSnapshot) -> Option<f64> {
    snapshot
        .book
        .as_ref()?
        .best_ask
        .as_ref()
        .map(|level| level.price)
}

pub fn market_map(graph: &ConstraintGraphSnapshot) -> BTreeMap<String, MarketCanonical> {
    graph
        .graph
        .markets
        .iter()
        .cloned()
        .map(|market| (market.market_id.clone(), market))
        .collect()
}

pub fn normalize_market_refs(refs: &[String], max_refs: usize) -> Vec<String> {
    let mut refs = refs.to_vec();
    refs.sort();
    refs.dedup();
    refs.truncate(max_refs);
    refs
}

pub fn candidate_from_proposed(
    proposed: ProposedCandidate,
    graph_version: i64,
) -> OpportunityCandidate {
    let stable_seed = format!(
        "{}:{}:{}:{}",
        proposed.strategy,
        proposed.event_id,
        proposed.market_refs.join(","),
        proposed.thesis_ref
    );
    let mut hasher = DefaultHasher::new();
    stable_seed.hash(&mut hasher);
    let digest = hasher.finish() as u128;
    OpportunityCandidate {
        opportunity_id: Uuid::from_u128(digest),
        strategy: proposed.strategy,
        market_refs: proposed.market_refs,
        event_id: proposed.event_id,
        edge_gross_bps: proposed.score.edge_gross_bps,
        fee_cost_bps: proposed.score.fee_cost_bps,
        slippage_cost_bps: proposed.score.slippage_cost_bps,
        failure_risk_bps: proposed.score.failure_risk_bps,
        edge_net_bps: proposed.score.edge_net_bps,
        confidence: proposed.score.confidence,
        half_life_sec: proposed.score.half_life_sec,
        capital_lock_days: proposed.score.capital_lock_days,
        needs_multileg: proposed.needs_multileg,
        thesis_ref: proposed.thesis_ref,
        research_ref: None,
        llm_review: None,
        graph_version,
        book_observed_at: proposed.book_observed_at,
        created_at: now(),
        invalidated_at: None,
        invalidation_reason: None,
    }
}

pub fn scanner_report(
    strategy: StrategyKind,
    domain: AccountDomain,
    scope_key: String,
    graph_version: i64,
    book_version_hint: Option<Timestamp>,
    candidates_emitted: usize,
    candidates_rejected: usize,
    rejection_reasons: Vec<String>,
    started_at: Timestamp,
) -> ScannerRunReport {
    ScannerRunReport {
        run_id: Uuid::new_v4(),
        domain,
        scope_key,
        scanner: strategy,
        graph_version,
        book_version_hint,
        candidates_emitted,
        candidates_rejected,
        rejection_reasons,
        started_at,
        completed_at: Utc::now(),
    }
}
