use polymarket_core::MarketCanonical;

pub fn normalized_title_key(market: &MarketCanonical) -> String {
    market
        .title
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || c.is_ascii_whitespace())
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase()
}

pub fn are_equivalent(left: &MarketCanonical, right: &MarketCanonical) -> bool {
    normalized_title_key(left) == normalized_title_key(right)
        || (left.condition_id == right.condition_id && !left.condition_id.is_empty())
}
