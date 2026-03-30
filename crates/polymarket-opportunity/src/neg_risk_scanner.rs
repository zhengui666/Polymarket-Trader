use std::collections::BTreeMap;

use polymarket_core::StrategyKind;

use crate::models::{
    market_map, market_mid, normalize_market_refs, ProposedCandidate, ScanContext, ScannerOutcome,
};
use crate::scorer::build_score;
use crate::stale_guard::{book_age_ms, is_market_scan_allowed};

pub fn scan(context: &ScanContext) -> ScannerOutcome {
    let markets = market_map(&context.graph);
    let runtime_mode = context
        .health
        .runtime_mode
        .parse()
        .unwrap_or(polymarket_core::RuntimeMode::Observe);
    let mut grouped: BTreeMap<String, Vec<_>> = BTreeMap::new();
    for market in markets.values().filter(|market| market.neg_risk) {
        grouped
            .entry(market.event_id.clone())
            .or_default()
            .push(market.clone());
    }

    let mut outcome = ScannerOutcome::empty(StrategyKind::NegRisk);
    for (event_id, family_markets) in grouped {
        let mut refs = Vec::new();
        let mut mids = Vec::new();
        let mut observed_at: Option<polymarket_core::Timestamp> = None;

        for market in &family_markets {
            if market.neg_risk_augmented {
                let title = market.title.to_ascii_lowercase();
                if title.contains("placeholder") || title.contains(" other ") || title == "other" {
                    continue;
                }
            }
            let Some(snapshot) = context.books.get(&market.market_id) else {
                outcome
                    .rejection_reasons
                    .push(format!("neg_risk_missing_book:{}", market.market_id));
                continue;
            };
            if !is_market_scan_allowed(market, Some(snapshot), &context.settings, runtime_mode) {
                continue;
            }
            let Some(mid) = market_mid(snapshot) else {
                outcome
                    .rejection_reasons
                    .push(format!("neg_risk_missing_mid:{}", market.market_id));
                continue;
            };
            refs.push(market.market_id.clone());
            mids.push(mid);
            observed_at = Some(
                observed_at
                    .map(|ts| ts.max(snapshot.observed_at))
                    .unwrap_or(snapshot.observed_at),
            );
        }

        if mids.len() < 2 {
            continue;
        }

        let sum = mids.iter().sum::<f64>();
        let gross_bps = ((1.0 - sum).abs() * 10_000.0).round() as i32;
        let primary = family_markets.first().expect("family markets not empty");
        let age_ms = refs
            .first()
            .and_then(|market_id| context.books.get(market_id))
            .map(book_age_ms)
            .unwrap_or_default();
        let score = build_score(
            StrategyKind::NegRisk,
            gross_bps,
            refs.len(),
            primary,
            age_ms,
            &context.settings,
        );
        if score.edge_net_bps < context.settings.min_edge_net_bps {
            continue;
        }
        outcome.candidates.push(ProposedCandidate {
            strategy: StrategyKind::NegRisk,
            market_refs: normalize_market_refs(
                &refs,
                context.settings.max_market_refs_per_candidate,
            ),
            event_id,
            score,
            needs_multileg: true,
            thesis_ref: format!("neg_risk:sum_mid:{sum:.4}"),
            book_observed_at: observed_at.unwrap_or_else(chrono::Utc::now),
        });
    }

    outcome
}
