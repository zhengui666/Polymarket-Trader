use polymarket_core::StrategyKind;

use crate::models::{market_map, ProposedCandidate, ScanContext, ScannerOutcome};
use crate::scorer::build_score;
use crate::stale_guard::{book_age_ms, is_market_scan_allowed};

pub fn scan(context: &ScanContext) -> ScannerOutcome {
    let markets = market_map(&context.graph);
    let runtime_mode = context
        .health
        .runtime_mode
        .parse()
        .unwrap_or(polymarket_core::RuntimeMode::Observe);
    let mut outcome = ScannerOutcome::empty(StrategyKind::FeeAwareMM);

    if !context.settings.fee_mm_enabled {
        outcome.rejection_reasons.push("fee_mm_disabled".to_owned());
        return outcome;
    }

    for market in markets.values().filter(|market| market.fees_enabled) {
        let Some(snapshot) = context.books.get(&market.market_id) else {
            continue;
        };
        if !is_market_scan_allowed(market, Some(snapshot), &context.settings, runtime_mode) {
            continue;
        }
        let Some(book) = snapshot.book.as_ref() else {
            continue;
        };
        let Some(spread_bps) = book.spread_bps else {
            continue;
        };
        if spread_bps < 25.0 {
            continue;
        }
        let gross_bps = (spread_bps * 0.35).round() as i32;
        let score = build_score(
            StrategyKind::FeeAwareMM,
            gross_bps,
            1,
            market,
            book_age_ms(snapshot),
            &context.settings,
        );
        if score.edge_net_bps < context.settings.min_edge_net_bps {
            continue;
        }
        outcome.candidates.push(ProposedCandidate {
            strategy: StrategyKind::FeeAwareMM,
            market_refs: vec![market.market_id.clone()],
            event_id: market.event_id.clone(),
            score,
            needs_multileg: false,
            thesis_ref: format!("fee_mm:spread_bps:{spread_bps:.1}"),
            book_observed_at: snapshot.observed_at,
        });
    }

    outcome
}
