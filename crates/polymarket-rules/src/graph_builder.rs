use polymarket_core::{
    ConstraintEdge, ConstraintEdgeType, ConstraintGraph, ConstraintGraphSnapshot, EventFamily,
    EventFamilySnapshot, GraphScope, MarketCanonical, SemanticConfidence,
};
use uuid::Uuid;

use crate::equivalence::are_equivalent;
use crate::neg_risk::build_neg_risk_edges;

pub fn build_event_family(markets: Vec<MarketCanonical>) -> EventFamily {
    let event_id = markets
        .first()
        .map(|market| market.event_id.clone())
        .unwrap_or_default();

    EventFamily { event_id, markets }
}

pub fn build_constraint_graph(family: &EventFamily) -> ConstraintGraph {
    let mut edges = Vec::new();
    let markets = &family.markets;

    for (index, left) in markets.iter().enumerate() {
        for right in markets.iter().skip(index + 1) {
            if are_equivalent(left, right) {
                edges.push(make_edge(
                    left,
                    right,
                    ConstraintEdgeType::Equivalent,
                    SemanticConfidence::High,
                    &["normalized_title"],
                ));
                edges.push(make_edge(
                    right,
                    left,
                    ConstraintEdgeType::Equivalent,
                    SemanticConfidence::High,
                    &["normalized_title"],
                ));
            }

            if left.outcomes.len() == right.outcomes.len() && left.outcomes.len() > 1 {
                edges.push(make_edge(
                    left,
                    right,
                    ConstraintEdgeType::MutuallyExclusive,
                    SemanticConfidence::Medium,
                    &["same_event"],
                ));
                edges.push(make_edge(
                    right,
                    left,
                    ConstraintEdgeType::MutuallyExclusive,
                    SemanticConfidence::Medium,
                    &["same_event"],
                ));
            }

            if shares_threshold_family(left, right) {
                edges.push(make_edge(
                    left,
                    right,
                    ConstraintEdgeType::ThresholdMonotonicity,
                    SemanticConfidence::Medium,
                    &["threshold"],
                ));
                edges.push(make_edge(
                    right,
                    left,
                    ConstraintEdgeType::ThresholdMonotonicity,
                    SemanticConfidence::Medium,
                    &["threshold"],
                ));
            }

            if shares_time_family(left, right) {
                edges.push(make_edge(
                    left,
                    right,
                    ConstraintEdgeType::TimeImplication,
                    SemanticConfidence::Medium,
                    &["deadline"],
                ));
            }
        }
    }

    edges.extend(build_neg_risk_edges(markets));

    ConstraintGraph {
        event_id: family.event_id.clone(),
        generated_at: chrono::Utc::now(),
        markets: markets.clone(),
        edges,
    }
}

pub fn snapshot_graph(
    scope: GraphScope,
    graph: ConstraintGraph,
    version: i64,
) -> ConstraintGraphSnapshot {
    ConstraintGraphSnapshot {
        scope,
        graph,
        version,
        generated_at: chrono::Utc::now(),
    }
}

pub fn snapshot_family(family: EventFamily, version: i64) -> EventFamilySnapshot {
    EventFamilySnapshot {
        event_id: family.event_id.clone(),
        family,
        version,
        generated_at: chrono::Utc::now(),
    }
}

fn shares_threshold_family(left: &MarketCanonical, right: &MarketCanonical) -> bool {
    left.semantic_attributes
        .iter()
        .find(|item| item.key == "threshold")
        .zip(
            right
                .semantic_attributes
                .iter()
                .find(|item| item.key == "threshold"),
        )
        .is_some()
}

fn shares_time_family(left: &MarketCanonical, right: &MarketCanonical) -> bool {
    left.semantic_attributes
        .iter()
        .find(|item| item.key == "deadline")
        .zip(
            right
                .semantic_attributes
                .iter()
                .find(|item| item.key == "deadline"),
        )
        .is_some()
}

fn make_edge(
    left: &MarketCanonical,
    right: &MarketCanonical,
    edge_type: ConstraintEdgeType,
    confidence: SemanticConfidence,
    evidence: &[&str],
) -> ConstraintEdge {
    ConstraintEdge {
        edge_id: Uuid::new_v4(),
        src_market_id: left.market_id.clone(),
        dst_market_id: right.market_id.clone(),
        edge_type,
        confidence,
        rules_version_src: left.rules_version.clone(),
        rules_version_dst: right.rules_version.clone(),
        evidence: evidence.iter().map(|item| (*item).to_owned()).collect(),
        created_at: chrono::Utc::now(),
        invalidated_at: None,
    }
}
