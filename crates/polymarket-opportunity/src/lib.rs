pub mod dependency_scanner;
pub mod errors;
pub mod fee_mm_scanner;
pub mod models;
pub mod neg_risk_scanner;
pub mod rebalancing_scanner;
pub mod scorer;
pub mod stale_guard;

use std::collections::{BTreeSet, HashSet};

use async_trait::async_trait;
use polymarket_core::{OpportunityCandidate, ScannerRunReport, StrategyKind};

pub use crate::errors::OpportunityError;
pub use crate::models::{
    OrderbookSnapshotSet, RuntimeHealth, ScanContext, ScanOutput, ScanSettings,
};

#[async_trait]
pub trait OpportunityEngine {
    async fn scan(&self, context: ScanContext) -> Result<ScanOutput, OpportunityError>;
}

#[derive(Debug, Default, Clone)]
pub struct OpportunityEngineService;

#[async_trait]
impl OpportunityEngine for OpportunityEngineService {
    async fn scan(&self, context: ScanContext) -> Result<ScanOutput, OpportunityError> {
        let started_at = chrono::Utc::now();
        let scope_key = match &context.graph.scope {
            polymarket_core::GraphScope::All => "ALL".to_owned(),
            polymarket_core::GraphScope::Event { event_id } => format!("EVENT:{event_id}"),
            polymarket_core::GraphScope::Market { market_id } => format!("MARKET:{market_id}"),
        };

        let mut outcomes = vec![
            rebalancing_scanner::scan(&context),
            dependency_scanner::scan(&context),
            neg_risk_scanner::scan(&context),
        ];
        if context.settings.fee_mm_enabled {
            outcomes.push(fee_mm_scanner::scan(&context));
        }

        let mut candidates = Vec::new();
        let mut reports = Vec::new();
        let mut shadow_only = BTreeSet::new();
        for item in &context.settings.shadow_only_strategies {
            shadow_only.insert(*item);
        }

        for outcome in outcomes {
            let strategy = outcome.strategy;
            let emitted_before = candidates.len();
            if !shadow_only.contains(&strategy) || context.health.runtime_mode == "OBSERVE" {
                candidates.extend(outcome.candidates.into_iter().map(|candidate| {
                    models::candidate_from_proposed(candidate, context.graph.version)
                }));
            }

            reports.push(models::scanner_report(
                strategy,
                context.health.domain,
                scope_key.clone(),
                context.graph.version,
                candidates
                    .last()
                    .map(|candidate| candidate.book_observed_at),
                candidates.len().saturating_sub(emitted_before),
                outcome.rejection_reasons.len(),
                outcome.rejection_reasons,
                started_at,
            ));
        }

        let candidates = dedupe_and_rank(candidates, &context);
        Ok(ScanOutput {
            candidates,
            reports,
        })
    }
}

fn dedupe_and_rank(
    mut candidates: Vec<OpportunityCandidate>,
    context: &ScanContext,
) -> Vec<OpportunityCandidate> {
    candidates.sort_by(|left, right| {
        right
            .edge_net_bps
            .cmp(&left.edge_net_bps)
            .then_with(|| right.confidence.total_cmp(&left.confidence))
            .then_with(|| left.market_refs.cmp(&right.market_refs))
            .then_with(|| left.strategy.as_str().cmp(right.strategy.as_str()))
    });

    let mut deduped = Vec::new();
    let mut keys = HashSet::new();
    for candidate in candidates {
        let key = format!(
            "{}:{}:{}:{}",
            candidate.strategy,
            candidate.market_refs.join(","),
            candidate.graph_version,
            candidate.thesis_ref
        );
        if keys.insert(key) {
            deduped.push(candidate);
        }
    }

    deduped.truncate(context.settings.max_candidates_total);
    deduped
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{Duration, Utc};
    use polymarket_core::{
        AccountDomain, ConstraintEdge, ConstraintEdgeType, ConstraintGraph,
        ConstraintGraphSnapshot, GraphScope, MarketBook, MarketCanonical, MarketDataChannel,
        MarketQuoteLevel, MarketSnapshot, SemanticConfidence, SemanticTag,
    };
    use uuid::Uuid;

    use super::*;

    fn sample_market(market_id: &str, event_id: &str, neg_risk: bool) -> MarketCanonical {
        MarketCanonical {
            market_id: market_id.to_owned(),
            event_id: event_id.to_owned(),
            condition_id: format!("cond-{market_id}"),
            token_ids: vec![format!("token-{market_id}")],
            title: market_id.to_owned(),
            category: "crypto".to_owned(),
            outcomes: vec!["YES".to_owned(), "NO".to_owned()],
            end_time: Utc::now() + Duration::days(3),
            resolution_source: None,
            edge_cases: Vec::new(),
            clarifications: Vec::new(),
            fees_enabled: true,
            neg_risk,
            neg_risk_augmented: false,
            tick_size: 0.01,
            rules_version: "v1".to_owned(),
            raw_rules_text: "rules".to_owned(),
            market_status: "open".to_owned(),
            observed_at: Utc::now(),
            updated_at: Utc::now(),
            semantic_tags: vec![SemanticTag::Binary],
            semantic_attributes: Vec::new(),
        }
    }

    fn sample_snapshot(market_id: &str, mid: f64, spread_bps: f64) -> MarketSnapshot {
        let spread = mid * spread_bps / 10_000.0;
        let bid = (mid - spread / 2.0).max(0.01);
        let ask = (mid + spread / 2.0).min(0.99);
        MarketSnapshot {
            market_id: market_id.to_owned(),
            channel: MarketDataChannel::Market,
            status: Some("open".to_owned()),
            book: Some(MarketBook {
                best_bid: Some(MarketQuoteLevel {
                    price: bid,
                    size: 100.0,
                }),
                best_ask: Some(MarketQuoteLevel {
                    price: ask,
                    size: 100.0,
                }),
                last_trade: None,
                mid_price: Some(mid),
                spread_bps: Some(spread_bps),
                observed_at: Utc::now(),
            }),
            sequence: 1,
            source_event_id: None,
            observed_at: Utc::now(),
            received_at: Utc::now(),
        }
    }

    fn settings() -> ScanSettings {
        ScanSettings {
            min_edge_net_bps: 1,
            max_market_refs_per_candidate: 8,
            max_candidates_per_event: 16,
            max_candidates_total: 32,
            fee_mm_enabled: true,
            shadow_only_strategies: BTreeSet::new(),
            default_half_life_secs: 900,
            book_stale_warn_ms: 3_000,
            book_stale_stop_ms: 10_000,
            max_book_age_for_multileg_ms: 2_000,
        }
    }

    #[tokio::test]
    async fn scan_emits_ranked_candidates() {
        let markets = vec![
            sample_market("m1", "evt1", false),
            sample_market("m2", "evt1", false),
            sample_market("m3", "evt1", true),
        ];
        let graph = ConstraintGraphSnapshot {
            scope: GraphScope::Event {
                event_id: "evt1".to_owned(),
            },
            graph: ConstraintGraph {
                event_id: "evt1".to_owned(),
                generated_at: Utc::now(),
                markets,
                edges: vec![ConstraintEdge {
                    edge_id: Uuid::new_v4(),
                    src_market_id: "m1".to_owned(),
                    dst_market_id: "m2".to_owned(),
                    edge_type: ConstraintEdgeType::Equivalent,
                    confidence: SemanticConfidence::High,
                    rules_version_src: "v1".to_owned(),
                    rules_version_dst: "v1".to_owned(),
                    evidence: vec!["same rules".to_owned()],
                    created_at: Utc::now(),
                    invalidated_at: None,
                }],
            },
            version: 1,
            generated_at: Utc::now(),
        };
        let context = ScanContext {
            graph,
            books: OrderbookSnapshotSet::new(vec![
                sample_snapshot("m1", 0.65, 30.0),
                sample_snapshot("m2", 0.40, 30.0),
                sample_snapshot("m3", 0.20, 40.0),
            ]),
            health: RuntimeHealth {
                domain: AccountDomain::Sim,
                runtime_mode: "SIMULATE".to_owned(),
                now: Utc::now(),
            },
            settings: settings(),
        };

        let result = OpportunityEngineService.scan(context).await.expect("scan");
        assert!(!result.candidates.is_empty());
        assert!(!result.reports.is_empty());
        assert!(result
            .candidates
            .windows(2)
            .all(|pair| pair[0].edge_net_bps >= pair[1].edge_net_bps));
    }
}
