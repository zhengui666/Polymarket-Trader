use polymarket_core::{MarketCanonical, OpportunityScore, SemanticTag, StrategyKind, Timestamp};

use crate::models::ScanSettings;

pub fn build_score(
    strategy: StrategyKind,
    gross_bps: i32,
    market_count: usize,
    primary_market: &MarketCanonical,
    book_age_ms: u64,
    settings: &ScanSettings,
) -> OpportunityScore {
    let fee_cost_bps = estimate_fee_cost(primary_market, strategy, market_count);
    let slippage_cost_bps = estimate_slippage_cost(strategy, market_count);
    let failure_risk_bps =
        estimate_failure_risk(primary_market, strategy, market_count, book_age_ms);
    let capital_lock_days =
        estimate_capital_lock_days(primary_market.end_time, settings.default_half_life_secs);
    let capital_lock_penalty = (capital_lock_days * 2.0).round() as i32;
    let edge_net_bps =
        gross_bps - fee_cost_bps - slippage_cost_bps - failure_risk_bps - capital_lock_penalty;
    let confidence = estimate_confidence(
        primary_market,
        gross_bps,
        edge_net_bps,
        book_age_ms,
        settings.book_stale_warn_ms,
    );
    let half_life_sec = estimate_half_life(strategy, book_age_ms, settings.default_half_life_secs);

    OpportunityScore {
        edge_gross_bps: gross_bps,
        fee_cost_bps,
        slippage_cost_bps,
        failure_risk_bps,
        edge_net_bps,
        confidence,
        half_life_sec,
        capital_lock_days,
    }
}

fn estimate_fee_cost(market: &MarketCanonical, strategy: StrategyKind, market_count: usize) -> i32 {
    let base = if market.fees_enabled { 8 } else { 2 };
    let multileg = (market_count.saturating_sub(1) as i32) * 4;
    let strategy_adj = match strategy {
        StrategyKind::FeeAwareMM => 3,
        StrategyKind::NegRisk => 6,
        StrategyKind::RulesDriven => 5,
        StrategyKind::DependencyArb => 4,
        StrategyKind::Rebalancing => 2,
    };
    base + multileg + strategy_adj
}

fn estimate_slippage_cost(strategy: StrategyKind, market_count: usize) -> i32 {
    let base = match strategy {
        StrategyKind::FeeAwareMM => 1,
        StrategyKind::NegRisk => 10,
        StrategyKind::RulesDriven => 8,
        StrategyKind::DependencyArb => 7,
        StrategyKind::Rebalancing => 5,
    };
    base + (market_count.saturating_sub(1) as i32) * 3
}

fn estimate_failure_risk(
    market: &MarketCanonical,
    strategy: StrategyKind,
    market_count: usize,
    book_age_ms: u64,
) -> i32 {
    let mut risk = match strategy {
        StrategyKind::FeeAwareMM => 24,
        StrategyKind::NegRisk => 20,
        StrategyKind::RulesDriven => 16,
        StrategyKind::DependencyArb => 14,
        StrategyKind::Rebalancing => 8,
    };
    risk += (market_count.saturating_sub(1) as i32) * 5;
    risk += ((book_age_ms / 1_000).min(30)) as i32;
    if market.neg_risk_augmented {
        risk += 8;
    }
    if market.semantic_tags.contains(&SemanticTag::Sports) {
        risk += 6;
    }
    risk
}

fn estimate_capital_lock_days(end_time: Timestamp, default_half_life_secs: u32) -> f32 {
    let now = chrono::Utc::now();
    let time_lock_days = (end_time - now).num_seconds().max(0) as f32 / 86_400.0;
    let half_life_days = default_half_life_secs as f32 / 86_400.0;
    (time_lock_days + half_life_days).max(0.0)
}

fn estimate_confidence(
    market: &MarketCanonical,
    gross_bps: i32,
    edge_net_bps: i32,
    book_age_ms: u64,
    stale_warn_ms: u64,
) -> f32 {
    let mut confidence = 0.50;
    confidence += (gross_bps.max(0) as f32 / 10_000.0).min(0.35);
    confidence -= (book_age_ms as f32 / stale_warn_ms.max(1) as f32).min(0.25);
    if market.neg_risk_augmented {
        confidence -= 0.05;
    }
    if edge_net_bps > 0 {
        confidence += 0.10;
    }
    confidence.clamp(0.05, 0.99)
}

fn estimate_half_life(
    strategy: StrategyKind,
    book_age_ms: u64,
    default_half_life_secs: u32,
) -> u32 {
    let strategy_adj = match strategy {
        StrategyKind::FeeAwareMM => 4,
        StrategyKind::NegRisk => 2,
        StrategyKind::RulesDriven => 2,
        StrategyKind::DependencyArb => 1,
        StrategyKind::Rebalancing => 1,
    };
    let decay = ((book_age_ms / 1_000) as u32).saturating_mul(15);
    default_half_life_secs
        .saturating_div(strategy_adj)
        .saturating_sub(decay)
        .max(30)
}
