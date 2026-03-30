use polymarket_core::{MarketCanonical, SemanticTag};

pub fn classify_market(canonical: &mut MarketCanonical) {
    let title = canonical.title.to_ascii_lowercase();
    let rules = canonical.raw_rules_text.to_ascii_lowercase();

    if title.contains(" vs ") || title.contains("match") {
        canonical.semantic_tags.push(SemanticTag::Sports);
    }
    if title.contains(" by ") || rules.contains("before ") {
        canonical.semantic_tags.push(SemanticTag::TimeBound);
    }
    if canonical.neg_risk || canonical.neg_risk_augmented {
        canonical.semantic_tags.push(SemanticTag::NegRisk);
    }
    if canonical
        .semantic_attributes
        .iter()
        .any(|item| item.key == "threshold")
    {
        canonical.semantic_tags.push(SemanticTag::Threshold);
    }
    if canonical.outcomes.len() == 2 {
        canonical.semantic_tags.push(SemanticTag::Binary);
    }
}
