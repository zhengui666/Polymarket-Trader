use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use polymarket_core::{ClarificationRecord, MarketCanonical, RawMarketDocument};

use crate::clarifications::normalize_clarifications;
use crate::classifier::classify_market;
use crate::errors::RulesError;
use crate::parser::parse_semantic_attributes;

pub fn normalize_market(
    raw: RawMarketDocument,
    max_clarifications: usize,
) -> Result<MarketCanonical, RulesError> {
    if raw.market_id.trim().is_empty() {
        return Err(RulesError::InvalidDocument(
            "market_id is required".to_owned(),
        ));
    }
    if raw.event_id.trim().is_empty() {
        return Err(RulesError::InvalidDocument(
            "event_id is required".to_owned(),
        ));
    }

    let clarifications = normalize_clarifications(&raw.clarifications, max_clarifications);
    let semantic_attributes = parse_semantic_attributes(&raw.title, &raw.raw_rules_text);
    let rules_version =
        compute_rules_version(&raw.raw_rules_text, &clarifications, &raw.edge_cases);

    let mut canonical = MarketCanonical {
        market_id: raw.market_id,
        event_id: raw.event_id,
        condition_id: raw.condition_id,
        token_ids: raw.token_ids,
        title: raw.title,
        category: raw.category,
        outcomes: raw.outcomes,
        end_time: raw.end_time,
        resolution_source: raw.resolution_source,
        edge_cases: raw.edge_cases,
        clarifications,
        fees_enabled: raw.fees_enabled,
        neg_risk: raw.neg_risk,
        neg_risk_augmented: raw.neg_risk_augmented,
        tick_size: raw.tick_size,
        rules_version,
        raw_rules_text: raw.raw_rules_text,
        market_status: raw.market_status,
        observed_at: raw.observed_at,
        updated_at: chrono::Utc::now(),
        semantic_tags: Vec::new(),
        semantic_attributes,
    };

    classify_market(&mut canonical);
    Ok(canonical)
}

fn compute_rules_version(
    rules_text: &str,
    clarifications: &[ClarificationRecord],
    edge_cases: &[String],
) -> String {
    let mut hasher = DefaultHasher::new();
    rules_text.hash(&mut hasher);
    edge_cases.hash(&mut hasher);
    for clarification in clarifications {
        clarification.text.hash(&mut hasher);
        clarification.occurred_at.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}
