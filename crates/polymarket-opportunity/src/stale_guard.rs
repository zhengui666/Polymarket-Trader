use polymarket_core::{MarketCanonical, MarketSnapshot, RuntimeMode};

use crate::models::ScanSettings;

pub fn is_market_scan_allowed(
    market: &MarketCanonical,
    snapshot: Option<&MarketSnapshot>,
    settings: &ScanSettings,
    runtime_mode: RuntimeMode,
) -> bool {
    if runtime_mode == RuntimeMode::Disabled {
        return false;
    }
    if !market.market_status.eq_ignore_ascii_case("open") {
        return false;
    }
    let Some(snapshot) = snapshot else {
        return false;
    };
    let age_ms = (chrono::Utc::now() - snapshot.observed_at)
        .num_milliseconds()
        .max(0) as u64;
    age_ms <= settings.book_stale_stop_ms
}

pub fn book_age_ms(snapshot: &MarketSnapshot) -> u64 {
    (chrono::Utc::now() - snapshot.observed_at)
        .num_milliseconds()
        .max(0) as u64
}
