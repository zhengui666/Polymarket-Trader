use std::collections::BTreeMap;

use polymarket_core::StrategyKind;

use crate::models::{
    best_ask, best_bid, market_map, market_mid, normalize_market_refs, ProposedCandidate,
    ScanContext, ScannerOutcome,
};
use crate::scorer::build_score;
use crate::stale_guard::{book_age_ms, is_market_scan_allowed};

pub fn scan(context: &ScanContext) -> ScannerOutcome {
    let markets = market_map(&context.graph);
    let mut grouped: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for market in markets.values() {
        grouped
            .entry(market.event_id.clone())
            .or_default()
            .push(market.clone());
    }

    let mut outcome = ScannerOutcome::empty(StrategyKind::Rebalancing);
    let runtime_mode = context
        .health
        .runtime_mode
        .parse()
        .unwrap_or(polymarket_core::RuntimeMode::Observe);

    for (event_id, family_markets) in grouped {
        let mut refs = Vec::new();
        let mut mids = Vec::new();
        let mut freshest_book_at: Option<polymarket_core::Timestamp> = None;
        let mut all_open = true;

        for market in &family_markets {
            let snapshot = context.books.get(&market.market_id);
            if !is_market_scan_allowed(market, snapshot, &context.settings, runtime_mode) {
                all_open = false;
                continue;
            }
            let Some(snapshot) = snapshot else {
                all_open = false;
                continue;
            };
            let Some(mid) = market_mid(snapshot) else {
                outcome
                    .rejection_reasons
                    .push(format!("rebalancing_missing_mid:{}", market.market_id));
                continue;
            };
            refs.push(market.market_id.clone());
            mids.push(mid);
            freshest_book_at = Some(
                freshest_book_at
                    .map(|current| current.max(snapshot.observed_at))
                    .unwrap_or(snapshot.observed_at),
            );
        }

        if mids.len() >= 2 {
            let sum = mids.iter().sum::<f64>();
            let gross_bps = ((1.0 - sum).abs() * 10_000.0).round() as i32;
            if gross_bps > 0 {
                let primary = family_markets.first().expect("family markets not empty");
                let age_ms = freshest_book_at
                    .and_then(|_| refs.first().and_then(|id| context.books.get(id)))
                    .map(book_age_ms)
                    .unwrap_or_default();
                let score = build_score(
                    StrategyKind::Rebalancing,
                    gross_bps,
                    refs.len(),
                    primary,
                    age_ms,
                    &context.settings,
                );
                if score.edge_net_bps >= context.settings.min_edge_net_bps {
                    outcome.candidates.push(ProposedCandidate {
                        strategy: StrategyKind::Rebalancing,
                        market_refs: normalize_market_refs(
                            &refs,
                            context.settings.max_market_refs_per_candidate,
                        ),
                        event_id,
                        score,
                        needs_multileg: refs.len() > 1,
                        thesis_ref: format!("rebalancing:completeness:{:.4}", sum),
                        book_observed_at: freshest_book_at.unwrap_or_else(chrono::Utc::now),
                    });
                }
            }
        } else if all_open {
            for market in &family_markets {
                let Some(snapshot) = context.books.get(&market.market_id) else {
                    continue;
                };
                let (Some(bid), Some(ask)) = (best_bid(snapshot), best_ask(snapshot)) else {
                    continue;
                };
                let gross_bps = ((ask - bid).abs() * 10_000.0).round() as i32;
                let score = build_score(
                    StrategyKind::Rebalancing,
                    gross_bps,
                    1,
                    market,
                    book_age_ms(snapshot),
                    &context.settings,
                );
                if score.edge_net_bps >= context.settings.min_edge_net_bps {
                    outcome.candidates.push(ProposedCandidate {
                        strategy: StrategyKind::Rebalancing,
                        market_refs: vec![market.market_id.clone()],
                        event_id: market.event_id.clone(),
                        score,
                        needs_multileg: false,
                        thesis_ref: format!("rebalancing:binary_spread:{bid:.4}:{ask:.4}"),
                        book_observed_at: snapshot.observed_at,
                    });
                }
            }
        }
    }

    outcome
}
