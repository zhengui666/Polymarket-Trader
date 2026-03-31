pub mod dependency_scanner;
pub mod errors;
pub mod fee_mm_scanner;
pub mod models;
pub mod neg_risk_scanner;
pub mod rebalancing_scanner;
pub mod scorer;
pub mod stale_guard;

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::time::Duration;

use async_trait::async_trait;
use polymarket_config::SearchConfig;
use polymarket_core::OpportunityCandidate;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::warn;

pub use crate::errors::OpportunityError;
pub use crate::models::{
    LlmEvaluationConfig, LlmFallbackConfig, OrderbookSnapshotSet, ResearchEnvelope, RuntimeHealth,
    ScanContext, ScanOutput, ScanSettings, SourceEvidence,
};

#[async_trait]
pub trait OpportunityEngine {
    async fn scan(&self, context: ScanContext) -> Result<ScanOutput, OpportunityError>;
}

#[derive(Debug, Default, Clone)]
pub struct OpportunityEngineService;

#[async_trait]
impl OpportunityEngine for OpportunityEngineService {
    async fn scan(&self, context: ScanContext) -> Result<ScanOutput, OpportunityError> {
        let started_at = chrono::Utc::now();
        let scope_key = match &context.graph.scope {
            polymarket_core::GraphScope::All => "ALL".to_owned(),
            polymarket_core::GraphScope::Event { event_id } => format!("EVENT:{event_id}"),
            polymarket_core::GraphScope::Market { market_id } => format!("MARKET:{market_id}"),
        };

        let mut outcomes = vec![
            rebalancing_scanner::scan(&context),
            dependency_scanner::scan(&context),
            neg_risk_scanner::scan(&context),
        ];
        if context.settings.fee_mm_enabled {
            outcomes.push(fee_mm_scanner::scan(&context));
        }

        let mut candidates = Vec::new();
        let mut reports = Vec::new();
        let mut shadow_only = BTreeSet::new();
        for item in &context.settings.shadow_only_strategies {
            shadow_only.insert(*item);
        }

        for outcome in outcomes {
            let strategy = outcome.strategy;
            let emitted_before = candidates.len();
            if !shadow_only.contains(&strategy) || context.health.runtime_mode == "OBSERVE" {
                candidates.extend(outcome.candidates.into_iter().map(|candidate| {
                    models::candidate_from_proposed(candidate, context.graph.version)
                }));
            }

            reports.push(models::scanner_report(
                strategy,
                context.health.domain,
                scope_key.clone(),
                context.graph.version,
                candidates
                    .last()
                    .map(|candidate| candidate.book_observed_at),
                candidates.len().saturating_sub(emitted_before),
                outcome.rejection_reasons.len(),
                outcome.rejection_reasons,
                started_at,
            ));
        }

        let candidates = dedupe_and_rank(candidates, &context).await;
        Ok(ScanOutput {
            candidates,
            reports,
        })
    }
}

async fn dedupe_and_rank(
    mut candidates: Vec<OpportunityCandidate>,
    context: &ScanContext,
) -> Vec<OpportunityCandidate> {
    candidates.sort_by(|left, right| {
        right
            .edge_net_bps
            .cmp(&left.edge_net_bps)
            .then_with(|| right.confidence.total_cmp(&left.confidence))
            .then_with(|| left.market_refs.cmp(&right.market_refs))
            .then_with(|| left.strategy.as_str().cmp(right.strategy.as_str()))
    });

    let mut deduped = Vec::new();
    let mut keys = HashSet::new();
    for candidate in candidates {
        let key = format!(
            "{}:{}:{}:{}",
            candidate.strategy,
            candidate.market_refs.join(","),
            candidate.graph_version,
            candidate.thesis_ref
        );
        if keys.insert(key) {
            deduped.push(candidate);
        }
    }

    deduped = apply_llm_evaluation(deduped, context).await;
    deduped.truncate(context.settings.max_candidates_total);
    deduped
}

async fn apply_llm_evaluation(
    candidates: Vec<OpportunityCandidate>,
    context: &ScanContext,
) -> Vec<OpportunityCandidate> {
    let Some(llm) = context.llm.as_ref() else {
        return candidates;
    };
    let head_count = llm.max_candidates.min(candidates.len());
    if head_count == 0 {
        return candidates;
    }
    let head = candidates[..head_count].to_vec();
    let tail = candidates[head_count..].to_vec();
    let research_cache = collect_research(llm, &head).await;
    match request_llm_reviews(llm, &head, &research_cache).await {
        Ok(reviews) => {
            let review_map = reviews
                .into_iter()
                .map(|review| (review.opportunity_id.clone(), review))
                .collect::<std::collections::HashMap<_, _>>();
            let mut merged = Vec::with_capacity(head.len() + tail.len());
            for mut candidate in head {
                let Some(review) = review_map.get(&candidate.opportunity_id.to_string()) else {
                    merged.push(candidate);
                    continue;
                };
                apply_review(
                    candidate.event_id.clone(),
                    &mut candidate,
                    review,
                    llm,
                    &research_cache,
                );
                if llm.veto_enabled && !review.approve {
                    continue;
                }
                merged.push(candidate);
            }
            merged.extend(tail);
            merged
        }
        Err(error) => {
            warn!(error = %error, "llm opportunity evaluation failed; falling back to rule-based ranking");
            let mut merged = head;
            merged.extend(tail);
            merged
        }
    }
}

fn apply_review(
    event_id: String,
    candidate: &mut OpportunityCandidate,
    review: &LlmCandidateReview,
    llm: &LlmEvaluationConfig,
    research_cache: &BTreeMap<String, ResearchEnvelope>,
) {
    candidate.confidence = review.confidence.clamp(0.0, 1.0) as f32;
    let research = research_cache.get(&event_id);
    let research_ref = review
        .research_ref
        .clone()
        .or_else(|| research.as_ref().map(|_| build_research_ref(candidate)));
    if let Some(reference) = research_ref.clone() {
        candidate.research_ref = Some(reference.clone());
        candidate.thesis_ref = reference;
    } else if !review.explanation.trim().is_empty() {
        candidate.thesis_ref = format!("llm:{}", truncate_text(review.explanation.trim(), 180));
    }
    candidate.llm_review = Some(json!({
        "task": llm.task,
        "tier": llm.tier,
        "approve": review.approve,
        "confidence": review.confidence,
        "explanation": review.explanation,
        "risk_note": review.risk_note,
        "novelty": review.novelty,
        "evidence_gap": review.evidence_gap,
        "contradiction_summary": review.contradiction_summary,
        "research_ref": research_ref,
        "research": research,
    }));
}

async fn collect_research(
    llm: &LlmEvaluationConfig,
    candidates: &[OpportunityCandidate],
) -> BTreeMap<String, ResearchEnvelope> {
    let Some(search) = llm.search.as_ref().filter(|config| config.enabled) else {
        return BTreeMap::new();
    };
    let mut cache = BTreeMap::new();
    for candidate in candidates {
        if !should_trigger_search(candidate, &cache) {
            continue;
        }
        match search_candidate(search, candidate).await {
            Ok(Some(research)) => {
                cache.insert(candidate.event_id.clone(), research);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(event_id = %candidate.event_id, error = %error, "searxng enrichment failed");
            }
        }
    }
    cache
}

fn should_trigger_search(
    candidate: &OpportunityCandidate,
    cache: &BTreeMap<String, ResearchEnvelope>,
) -> bool {
    if cache.contains_key(&candidate.event_id) {
        return false;
    }
    candidate.edge_net_bps >= 60
        || candidate.needs_multileg
        || candidate.market_refs.len() > 1
        || candidate.confidence < 0.6
        || candidate.thesis_ref.trim().is_empty()
}

async fn search_candidate(
    search: &SearchConfig,
    candidate: &OpportunityCandidate,
) -> Result<Option<ResearchEnvelope>, OpportunityError> {
    let client = Client::builder()
        .timeout(Duration::from_millis(search.timeout_ms))
        .build()
        .map_err(|error| OpportunityError::Llm(error.to_string()))?;
    let url = format!("{}/search", search.base_url.trim_end_matches('/'));
    let query = build_search_query(candidate);
    let mut request = client.get(url).query(&[
        ("q", query.as_str()),
        ("format", "json"),
        ("language", "zh-CN"),
        ("safesearch", "0"),
    ]);
    if !search.api_key.trim().is_empty() {
        request = request
            .header("Authorization", format!("Bearer {}", search.api_key))
            .header("X-API-Key", search.api_key.clone());
    }
    let payload = request
        .send()
        .await
        .map_err(|error| OpportunityError::Llm(error.to_string()))?
        .error_for_status()
        .map_err(|error| OpportunityError::Llm(error.to_string()))?
        .json::<Value>()
        .await
        .map_err(|error| OpportunityError::Llm(error.to_string()))?;

    let mut sources = Vec::new();
    let mut seen = HashSet::new();
    for item in payload
        .get("results")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let Some(url) = item.get("url").and_then(Value::as_str) else {
            continue;
        };
        if !seen.insert(url.to_owned()) {
            continue;
        }
        let domain = extract_domain(url);
        let normalized_domain = domain.to_ascii_lowercase();
        if !search.allowed_domains.is_empty()
            && !search.allowed_domains.contains(&normalized_domain)
        {
            continue;
        }
        if search.blocked_domains.contains(&normalized_domain) {
            continue;
        }
        sources.push(SourceEvidence {
            title: item
                .get("title")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned(),
            url: url.to_owned(),
            snippet: item
                .get("content")
                .or_else(|| item.get("snippet"))
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_owned(),
            source_domain: domain,
            published_at: item
                .get("publishedDate")
                .or_else(|| item.get("published_date"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        });
        if sources.len() >= search.max_results {
            break;
        }
    }
    if sources.is_empty() {
        return Ok(None);
    }
    let summary = sources
        .iter()
        .take(3)
        .map(|source| {
            if source.snippet.is_empty() {
                source.title.clone()
            } else {
                format!("{}: {}", source.title, truncate_text(&source.snippet, 140))
            }
        })
        .collect::<Vec<_>>()
        .join(" | ");
    Ok(Some(ResearchEnvelope {
        query,
        generated_at: chrono::Utc::now(),
        summary,
        risks: vec!["外部搜索结果仅作研究增强，不直接作为执行放行条件".to_owned()],
        contradictions: if sources.len() > 1 {
            vec!["需要人工确认多来源时间线是否一致".to_owned()]
        } else {
            Vec::new()
        },
        evidence_gaps: if sources.iter().all(|source| source.snippet.is_empty()) {
            vec!["搜索结果缺少可直接消费的摘要片段".to_owned()]
        } else {
            Vec::new()
        },
        confidence_note: format!("searxng returned {} sources", sources.len()),
        source_links: sources.iter().map(|source| source.url.clone()).collect(),
        sources,
    }))
}

fn build_search_query(candidate: &OpportunityCandidate) -> String {
    format!(
        "Polymarket {} {} {}",
        candidate.event_id,
        candidate.strategy,
        candidate.market_refs.join(" ")
    )
}

fn build_research_ref(candidate: &OpportunityCandidate) -> String {
    format!(
        "research:{}:{}",
        candidate.event_id, candidate.opportunity_id
    )
}

fn extract_domain(url: &str) -> String {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn truncate_text(value: &str, max_len: usize) -> String {
    let trimmed = value.trim();
    if trimmed.chars().count() <= max_len {
        return trimmed.to_owned();
    }
    let mut output = trimmed
        .chars()
        .take(max_len.saturating_sub(1))
        .collect::<String>();
    output.push('…');
    output
}

async fn request_llm_reviews(
    llm: &LlmEvaluationConfig,
    candidates: &[OpportunityCandidate],
    research_cache: &BTreeMap<String, ResearchEnvelope>,
) -> Result<Vec<LlmCandidateReview>, OpportunityError> {
    let mut attempts = Vec::with_capacity(1 + llm.fallback_models.len());
    attempts.push((
        llm.tier,
        llm.base_url.clone(),
        llm.api_key.clone(),
        llm.model_name.clone(),
    ));
    attempts.extend(llm.fallback_models.iter().map(|fallback| {
        (
            fallback.tier,
            fallback.base_url.clone(),
            fallback.api_key.clone(),
            fallback.model_name.clone(),
        )
    }));

    let mut last_error = None;
    for (tier, base_url, api_key, model_name) in attempts {
        match request_llm_reviews_for_route(
            tier,
            &base_url,
            &api_key,
            &model_name,
            candidates,
            research_cache,
        )
        .await
        {
            Ok(reviews) => return Ok(reviews),
            Err(error) => {
                warn!(
                    tier = ?tier,
                    error = %error,
                    "llm route failed; trying next fallback if available"
                );
                last_error = Some(error);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| OpportunityError::Llm("no llm route configured".to_owned())))
}

async fn request_llm_reviews_for_route(
    tier: polymarket_config::ModelTier,
    base_url: &str,
    api_key: &str,
    model_name: &str,
    candidates: &[OpportunityCandidate],
    research_cache: &BTreeMap<String, ResearchEnvelope>,
) -> Result<Vec<LlmCandidateReview>, OpportunityError> {
    let client = Client::new();
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let response = client
        .post(url)
        .bearer_auth(api_key)
        .json(&json!({
            "model": model_name,
            "temperature": 0,
            "response_format": { "type": "json_object" },
            "messages": [
                {
                    "role": "system",
                    "content": concat!(
                        "You are a strict Polymarket execution reviewer. ",
                        "Return valid JSON only and exactly match the requested schema. ",
                        "Review candidates like a professional event trader and risk manager, not like a general assistant. ",
                        "Your goal is to approve only opportunities with credible executable edge after realistic frictions. ",
                        "Default to enrich-first review: only set approve=false when the edge looks materially weak or structurally suspect."
                    )
                },
                {
                    "role": "user",
                    "content": json!({
                        "tier": tier,
                        "required_schema": {
                            "reviews": [{
                                "opportunity_id": "uuid string",
                                "approve": true,
                                "confidence": 0.0,
                                "explanation": "short explanation",
                                "risk_note": "optional",
                                "novelty": "optional",
                                "evidence_gap": "optional",
                                "contradiction_summary": "optional",
                                "research_ref": "optional"
                            }]
                        },
                        "candidates": candidates.iter().map(|candidate| {
                            json!({
                                "opportunity_id": candidate.opportunity_id.to_string(),
                                "strategy": candidate.strategy.as_str(),
                                "event_id": candidate.event_id,
                                "market_refs": candidate.market_refs,
                                "edge_net_bps": candidate.edge_net_bps,
                                "edge_gross_bps": candidate.edge_gross_bps,
                                "fee_cost_bps": candidate.fee_cost_bps,
                                "slippage_cost_bps": candidate.slippage_cost_bps,
                                "failure_risk_bps": candidate.failure_risk_bps,
                                "confidence": finite_f32(candidate.confidence),
                                "half_life_sec": candidate.half_life_sec,
                                "capital_lock_days": candidate.capital_lock_days,
                                "needs_multileg": candidate.needs_multileg,
                                "book_observed_at": candidate.book_observed_at,
                                "thesis_ref": candidate.thesis_ref,
                                "research": research_cache.get(&candidate.event_id),
                            })
                        }).collect::<Vec<_>>()
                    }).to_string()
                }
            ]
        }))
        .send()
        .await
        .map_err(|error| OpportunityError::Llm(error.to_string()))?
        .error_for_status()
        .map_err(|error| OpportunityError::Llm(error.to_string()))?;
    let body = response
        .json::<Value>()
        .await
        .map_err(|error| OpportunityError::Llm(error.to_string()))?;
    let content = body
        .get("choices")
        .and_then(|choices| choices.get(0))
        .and_then(|choice| choice.get("message"))
        .and_then(|message| message.get("content"))
        .and_then(|content| content.as_str())
        .ok_or_else(|| OpportunityError::Llm("missing llm response content".to_owned()))?;
    let envelope = serde_json::from_str::<LlmReviewEnvelope>(content)
        .or_else(|_| {
            let start = content.find('{').ok_or_else(|| {
                serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing json body",
                ))
            })?;
            let end = content.rfind('}').ok_or_else(|| {
                serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "missing json body",
                ))
            })?;
            serde_json::from_str::<LlmReviewEnvelope>(&content[start..=end])
        })
        .map_err(|error| OpportunityError::Llm(error.to_string()))?;
    Ok(envelope.reviews)
}

fn finite_f32(value: f32) -> f32 {
    if value.is_finite() {
        value
    } else {
        0.0
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LlmReviewEnvelope {
    reviews: Vec<LlmCandidateReview>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct LlmCandidateReview {
    opportunity_id: String,
    approve: bool,
    confidence: f64,
    explanation: String,
    #[serde(default)]
    risk_note: Option<String>,
    #[serde(default)]
    novelty: Option<String>,
    #[serde(default)]
    evidence_gap: Option<String>,
    #[serde(default)]
    contradiction_summary: Option<String>,
    #[serde(default)]
    research_ref: Option<String>,
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use chrono::{Duration, Utc};
    use polymarket_core::{
        AccountDomain, ConstraintEdge, ConstraintEdgeType, ConstraintGraph,
        ConstraintGraphSnapshot, GraphScope, MarketBook, MarketCanonical, MarketDataChannel,
        MarketQuoteLevel, MarketSnapshot, SemanticConfidence, SemanticTag,
    };
    use uuid::Uuid;

    use super::*;

    fn sample_market(market_id: &str, event_id: &str, neg_risk: bool) -> MarketCanonical {
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

    fn sample_snapshot(market_id: &str, mid: f64, spread_bps: f64) -> MarketSnapshot {
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

    fn settings() -> ScanSettings {
        ScanSettings {
            min_edge_net_bps: 1,
            max_market_refs_per_candidate: 8,
            max_candidates_per_event: 16,
            max_candidates_total: 32,
            fee_mm_enabled: true,
            shadow_only_strategies: BTreeSet::new(),
            default_half_life_secs: 900,
            book_stale_warn_ms: 3_000,
            book_stale_stop_ms: 10_000,
            max_book_age_for_multileg_ms: 2_000,
        }
    }

    #[tokio::test]
    async fn scan_emits_ranked_candidates() {
        let markets = vec![
            sample_market("m1", "evt1", false),
            sample_market("m2", "evt1", false),
            sample_market("m3", "evt1", true),
        ];
        let graph = ConstraintGraphSnapshot {
            scope: GraphScope::Event {
                event_id: "evt1".to_owned(),
            },
            graph: ConstraintGraph {
                event_id: "evt1".to_owned(),
                generated_at: Utc::now(),
                markets,
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
        };
        let context = ScanContext {
            graph,
            books: OrderbookSnapshotSet::new(vec![
                sample_snapshot("m1", 0.65, 30.0),
                sample_snapshot("m2", 0.40, 30.0),
                sample_snapshot("m3", 0.20, 40.0),
            ]),
            health: RuntimeHealth {
                domain: AccountDomain::Sim,
                runtime_mode: "SIMULATE".to_owned(),
                now: Utc::now(),
            },
            settings: settings(),
            llm: None,
        };

        let result = OpportunityEngineService.scan(context).await.expect("scan");
        assert!(!result.candidates.is_empty());
        assert!(!result.reports.is_empty());
        assert!(result
            .candidates
            .windows(2)
            .all(|pair| pair[0].edge_net_bps >= pair[1].edge_net_bps));
    }
}
