use polymarket_core::{ConstraintEdge, ConstraintEdgeType, MarketCanonical, SemanticConfidence};
use uuid::Uuid;

pub fn build_neg_risk_edges(markets: &[MarketCanonical]) -> Vec<ConstraintEdge> {
    let mut edges = Vec::new();

    for src in markets {
        if !src.neg_risk {
            continue;
        }

        for dst in markets {
            if src.market_id == dst.market_id {
                continue;
            }

            edges.push(ConstraintEdge {
                edge_id: Uuid::new_v4(),
                src_market_id: src.market_id.clone(),
                dst_market_id: dst.market_id.clone(),
                edge_type: ConstraintEdgeType::NegRiskConversion,
                confidence: SemanticConfidence::High,
                rules_version_src: src.rules_version.clone(),
                rules_version_dst: dst.rules_version.clone(),
                evidence: vec!["neg_risk".to_owned()],
                created_at: src.updated_at,
                invalidated_at: None,
            });
        }
    }

    edges
}
