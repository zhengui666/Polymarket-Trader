use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Utc};
use polymarket_config::{LlmTaskKind, NodeConfig, OpportunityEngineConfig};
use polymarket_core::{
    ConstraintEdge, ConstraintEdgeType, ConstraintGraph, ConstraintGraphSnapshot, GraphScope,
    MarketBook, MarketCanonical, MarketDataChannel, MarketQuoteLevel, MarketSnapshot,
    SemanticConfidence, SemanticTag,
};
use polymarket_opportunity::{
    LlmEvaluationConfig, OpportunityEngine, OpportunityEngineService, OrderbookSnapshotSet,
    RuntimeHealth, ScanContext, ScanSettings,
};
use polymarket_storage::Store;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    let selected_domain = std::env::var("POLYMARKET_NODE_DOMAIN")
        .ok()
        .unwrap_or_else(|| "SIM".to_owned())
        .parse()?;
    let config = NodeConfig::from_env(selected_domain)?;
    let opportunity = OpportunityEngineConfig::from_env()?;
    let store = Store::new(
        config.selected_domain_config.database_path.clone(),
        config.selected_domain,
        config.selected_domain_config.namespace(),
        config.selected_domain_config.audit_prefix.clone(),
    );

    let engine = OpportunityEngineService;
    for graph in recent_event_graphs(&config.selected_domain_config.database_path)? {
        let market_ids = graph
            .graph
            .markets
            .iter()
            .map(|market| market.market_id.clone())
            .collect::<Vec<_>>();
        let books = store.list_market_snapshots(config.selected_domain, &market_ids)?;
        if books.is_empty() {
            continue;
        }
        let output = engine
            .scan(ScanContext {
                graph,
                books: OrderbookSnapshotSet::new(books),
                health: RuntimeHealth {
                    domain: config.selected_domain,
                    runtime_mode: config
                        .selected_domain_config
                        .runtime_mode
                        .as_str()
                        .to_owned(),
                    now: polymarket_core::now(),
                },
                settings: build_scan_settings(&opportunity)?,
                llm: config
                    .llm
                    .resolve_task(LlmTaskKind::OpportunityReview)
                    .map(|resolved| {
                        LlmEvaluationConfig::from_resolved(
                            resolved,
                            8,
                            config.search.enabled.then(|| config.search.clone()),
                            Vec::new(),
                        )
                    }),
            })
            .await
            .context("opportunity engine scan failed")?;
        if output.candidates.is_empty() {
            continue;
        }
        println!("candidates={}", output.candidates.len());
        for candidate in output.candidates.iter().take(5) {
            println!(
                "{}|{}|{}|{}|{}",
                candidate.opportunity_id,
                candidate.strategy,
                candidate.confidence,
                candidate.edge_net_bps,
                candidate.thesis_ref
            );
        }
        return Ok(());
    }
    let output = engine
        .scan(ScanContext {
            graph: synthetic_graph(),
            books: OrderbookSnapshotSet::new(vec![
                synthetic_snapshot("m1", 0.65, 30.0),
                synthetic_snapshot("m2", 0.40, 30.0),
                synthetic_snapshot("m3", 0.20, 40.0),
            ]),
            health: RuntimeHealth {
                domain: config.selected_domain,
                runtime_mode: config
                    .selected_domain_config
                    .runtime_mode
                    .as_str()
                    .to_owned(),
                now: polymarket_core::now(),
            },
            settings: build_scan_settings(&opportunity)?,
            llm: config
                .llm
                .resolve_task(LlmTaskKind::OpportunityReview)
                .map(|resolved| {
                    LlmEvaluationConfig::from_resolved(
                        resolved,
                        8,
                        config.search.enabled.then(|| config.search.clone()),
                        Vec::new(),
                    )
                }),
        })
        .await
        .context("synthetic opportunity engine scan failed")?;
    if output.candidates.is_empty() {
        return Err(anyhow!("no candidates found to process"));
    }
    println!("synthetic_candidates={}", output.candidates.len());
    for candidate in output.candidates.iter().take(5) {
        println!(
            "{}|{}|{}|{}|{}",
            candidate.opportunity_id,
            candidate.strategy,
            candidate.confidence,
            candidate.edge_net_bps,
            candidate.thesis_ref
        );
    }
    Ok(())
}

fn recent_event_graphs(
    db_path: &std::path::Path,
) -> Result<Vec<polymarket_core::ConstraintGraphSnapshot>> {
    let connection = rusqlite::Connection::open(db_path)
        .with_context(|| format!("failed to open sqlite db at {}", db_path.display()))?;
    let mut statement = connection.prepare(
        "SELECT scope_key, snapshot_json
         FROM constraint_graph_snapshots
         WHERE scope_key LIKE 'EVENT:%'
         ORDER BY rowid DESC
         LIMIT 20",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut graphs = Vec::new();
    for row in rows {
        let (scope_key, snapshot_json) = row?;
        let graph = serde_json::from_str(&snapshot_json)?;
        println!("graph_scope={scope_key}");
        graphs.push(graph);
    }
    Ok(graphs)
}

fn build_scan_settings(config: &OpportunityEngineConfig) -> Result<ScanSettings> {
    Ok(ScanSettings {
        min_edge_net_bps: config.min_edge_net_bps,
        max_market_refs_per_candidate: config.max_market_refs_per_candidate,
        max_candidates_per_event: config.max_candidates_per_event,
        max_candidates_total: config.max_candidates_total,
        fee_mm_enabled: config.fee_mm_enabled,
        shadow_only_strategies: config
            .shadow_only_strategies
            .iter()
            .map(|item| item.parse())
            .collect::<Result<_, _>>()?,
        default_half_life_secs: config.default_half_life_secs,
        book_stale_warn_ms: config.book_stale_warn_ms,
        book_stale_stop_ms: config.book_stale_stop_ms,
        max_book_age_for_multileg_ms: config.max_book_age_for_multileg_ms,
    })
}

fn synthetic_graph() -> ConstraintGraphSnapshot {
    ConstraintGraphSnapshot {
        scope: GraphScope::Event {
            event_id: "evt1".to_owned(),
        },
        graph: ConstraintGraph {
            event_id: "evt1".to_owned(),
            generated_at: Utc::now(),
            markets: vec![
                synthetic_market("m1", "evt1", false),
                synthetic_market("m2", "evt1", false),
                synthetic_market("m3", "evt1", true),
            ],
            edges: vec![ConstraintEdge {
                edge_id: Uuid::new_v4(),
                src_market_id: "m1".to_owned(),
                dst_market_id: "m2".to_owned(),
                edge_type: ConstraintEdgeType::Equivalent,
                confidence: SemanticConfidence::High,
                rules_version_src: "v1".to_owned(),
                rules_version_dst: "v1".to_owned(),
                evidence: vec!["same rules".to_owned()],
                created_at: Utc::now(),
                invalidated_at: None,
            }],
        },
        version: 1,
        generated_at: Utc::now(),
    }
}

fn synthetic_market(market_id: &str, event_id: &str, neg_risk: bool) -> MarketCanonical {
    MarketCanonical {
        market_id: market_id.to_owned(),
        event_id: event_id.to_owned(),
        condition_id: format!("cond-{market_id}"),
        token_ids: vec![format!("token-{market_id}")],
        title: market_id.to_owned(),
        category: "crypto".to_owned(),
        outcomes: vec!["YES".to_owned(), "NO".to_owned()],
        end_time: Utc::now() + Duration::days(3),
        resolution_source: None,
        edge_cases: Vec::new(),
        clarifications: Vec::new(),
        fees_enabled: true,
        neg_risk,
        neg_risk_augmented: false,
        tick_size: 0.01,
        rules_version: "v1".to_owned(),
        raw_rules_text: "rules".to_owned(),
        market_status: "open".to_owned(),
        observed_at: Utc::now(),
        updated_at: Utc::now(),
        semantic_tags: vec![SemanticTag::Binary],
        semantic_attributes: Vec::new(),
    }
}

fn synthetic_snapshot(market_id: &str, mid: f64, spread_bps: f64) -> MarketSnapshot {
    let spread = mid * spread_bps / 10_000.0;
    let bid = (mid - spread / 2.0).max(0.01);
    let ask = (mid + spread / 2.0).min(0.99);
    MarketSnapshot {
        market_id: market_id.to_owned(),
        channel: MarketDataChannel::Market,
        status: Some("open".to_owned()),
        book: Some(MarketBook {
            best_bid: Some(MarketQuoteLevel {
                price: bid,
                size: 100.0,
            }),
            best_ask: Some(MarketQuoteLevel {
                price: ask,
                size: 100.0,
            }),
            last_trade: None,
            mid_price: Some(mid),
            spread_bps: Some(spread_bps),
            observed_at: Utc::now(),
        }),
        sequence: 1,
        source_event_id: None,
        observed_at: Utc::now(),
        received_at: Utc::now(),
    }
}
