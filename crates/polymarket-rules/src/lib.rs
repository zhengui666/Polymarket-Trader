pub mod clarifications;
pub mod classifier;
pub mod equivalence;
pub mod errors;
pub mod graph_builder;
pub mod graph_store;
pub mod models;
pub mod neg_risk;
pub mod normalizer;
pub mod parser;
pub mod replay;

use async_trait::async_trait;
use polymarket_core::{
    ConstraintGraphSnapshot, EventFamilySnapshot, GraphScope, MarketCanonical, RawMarketDocument,
    ReplayReport, ReplayRequest,
};

pub use crate::errors::RulesError;
pub use crate::graph_builder::build_constraint_graph;
pub use crate::models::RulesEngineService;
pub use crate::normalizer::normalize_market;
pub use crate::replay::build_replay_report;

#[async_trait]
pub trait RulesEngine {
    async fn upsert_market(&self, raw: RawMarketDocument) -> Result<MarketCanonical, RulesError>;

    async fn rebuild_event_family(&self, event_id: &str)
        -> Result<EventFamilySnapshot, RulesError>;

    async fn get_constraint_graph(
        &self,
        scope: GraphScope,
    ) -> Result<ConstraintGraphSnapshot, RulesError>;

    async fn replay(&self, request: ReplayRequest) -> Result<ReplayReport, RulesError>;
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use polymarket_core::{ClarificationRecord, ConstraintEdgeType, RawMarketDocument};

    use crate::graph_builder::{build_constraint_graph, build_event_family};
    use crate::normalizer::normalize_market;

    fn sample_market(title: &str, clarification: &str) -> RawMarketDocument {
        RawMarketDocument {
            market_id: format!("m-{title}"),
            event_id: "evt-1".to_owned(),
            condition_id: "cond-1".to_owned(),
            token_ids: vec!["yes".to_owned(), "no".to_owned()],
            title: title.to_owned(),
            category: "politics".to_owned(),
            outcomes: vec!["YES".to_owned(), "NO".to_owned()],
            end_time: Utc::now(),
            resolution_source: Some("Associated Press".to_owned()),
            edge_cases: vec!["runoff".to_owned()],
            clarifications: vec![ClarificationRecord {
                text: clarification.to_owned(),
                occurred_at: Utc::now(),
            }],
            fees_enabled: true,
            neg_risk: false,
            neg_risk_augmented: false,
            tick_size: 0.01,
            raw_rules_text: "Resolved according to AP by Mar 31.".to_owned(),
            market_status: "open".to_owned(),
            observed_at: Utc::now(),
        }
    }

    #[test]
    fn rules_version_changes_when_clarification_changes() {
        let left = normalize_market(sample_market("Will X win by Mar 31?", "first"), 32)
            .expect("normalize");
        let right = normalize_market(sample_market("Will X win by Mar 31?", "second"), 32)
            .expect("normalize");
        assert_ne!(left.rules_version, right.rules_version);
        assert!(left
            .semantic_attributes
            .iter()
            .any(|item| item.key == "deadline"));
    }

    #[test]
    fn graph_builder_emits_equivalence_and_threshold_edges() {
        let mut left = sample_market("BTC above 100k by Mar 31?", "same");
        left.market_id = "m1".to_owned();
        let mut right = sample_market("BTC above 100k by Mar 31", "same");
        right.market_id = "m2".to_owned();
        let left = normalize_market(left, 32).expect("normalize");
        let right = normalize_market(right, 32).expect("normalize");
        let family = build_event_family(vec![left, right]);
        let graph = build_constraint_graph(&family);
        assert!(graph
            .edges
            .iter()
            .any(|edge| edge.edge_type == ConstraintEdgeType::Equivalent));
        assert!(graph
            .edges
            .iter()
            .any(|edge| edge.edge_type == ConstraintEdgeType::ThresholdMonotonicity));
    }
}
