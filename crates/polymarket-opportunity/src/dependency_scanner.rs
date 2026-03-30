use polymarket_core::{ConstraintEdgeType, StrategyKind};

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
    let mut outcome = ScannerOutcome::empty(StrategyKind::DependencyArb);

    for edge in &context.graph.graph.edges {
        let (Some(src_market), Some(dst_market)) = (
            markets.get(&edge.src_market_id),
            markets.get(&edge.dst_market_id),
        ) else {
            outcome
                .rejection_reasons
                .push(format!("dependency_missing_market:{}", edge.edge_id));
            continue;
        };
        let (Some(src_book), Some(dst_book)) = (
            context.books.get(&edge.src_market_id),
            context.books.get(&edge.dst_market_id),
        ) else {
            outcome
                .rejection_reasons
                .push(format!("dependency_missing_book:{}", edge.edge_id));
            continue;
        };
        if !is_market_scan_allowed(src_market, Some(src_book), &context.settings, runtime_mode)
            || !is_market_scan_allowed(dst_market, Some(dst_book), &context.settings, runtime_mode)
        {
            outcome
                .rejection_reasons
                .push(format!("dependency_market_not_scannable:{}", edge.edge_id));
            continue;
        }
        let (Some(src_mid), Some(dst_mid)) = (market_mid(src_book), market_mid(dst_book)) else {
            outcome
                .rejection_reasons
                .push(format!("dependency_missing_mid:{}", edge.edge_id));
            continue;
        };

        let maybe_gross = match edge.edge_type {
            ConstraintEdgeType::TimeImplication
            | ConstraintEdgeType::ThresholdMonotonicity
            | ConstraintEdgeType::Dependency
            | ConstraintEdgeType::Inclusion => {
                if src_mid > dst_mid {
                    Some(((src_mid - dst_mid) * 10_000.0).round() as i32)
                } else {
                    None
                }
            }
            ConstraintEdgeType::Equivalent => {
                Some(((src_mid - dst_mid).abs() * 10_000.0).round() as i32)
            }
            ConstraintEdgeType::MutuallyExclusive => {
                if src_mid + dst_mid > 1.0 {
                    Some((((src_mid + dst_mid) - 1.0) * 10_000.0).round() as i32)
                } else {
                    None
                }
            }
            _ => None,
        };

        let Some(gross_bps) = maybe_gross else {
            continue;
        };

        let strategy = if edge.edge_type == ConstraintEdgeType::Equivalent {
            StrategyKind::RulesDriven
        } else {
            StrategyKind::DependencyArb
        };
        let score = build_score(
            strategy,
            gross_bps,
            2,
            src_market,
            book_age_ms(src_book).max(book_age_ms(dst_book)),
            &context.settings,
        );
        if score.edge_net_bps < context.settings.min_edge_net_bps {
            continue;
        }

        outcome.candidates.push(ProposedCandidate {
            strategy,
            market_refs: normalize_market_refs(
                &[src_market.market_id.clone(), dst_market.market_id.clone()],
                context.settings.max_market_refs_per_candidate,
            ),
            event_id: src_market.event_id.clone(),
            score,
            needs_multileg: true,
            thesis_ref: format!("dependency:{:?}:{}->{}", edge.edge_type, src_mid, dst_mid),
            book_observed_at: src_book.observed_at.max(dst_book.observed_at),
        });
    }

    outcome
}
