use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{ensure, Result};
use axum::body::Body;
use axum::extract::{Extension, Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use polymarket_api_types::{
    AlertAckResponse, AlertQueryResponse, ApiEnvelope, AuditEventsResponse, ConstraintGraphResponse,
    EventFamilyResponse, ExecutionActionResponse, ExecutionHealthResponse,
    ExecutionIntentsResponse, ExecutionOrdersResponse, ExecutionReconcileResponse, HealthResponse,
    OpportunitiesResponse, OpportunityResponse, ReplayJobStatusResponse, ReplayRunResponse,
    ReplayRunsResponse, ReplayTraceResponse, ReplayTriggerRequest, ReplayTriggerResponse,
    RolloutEvaluationResponse, RolloutEvaluationsResponse, RolloutIncidentsResponse,
    RolloutPolicyResponse, RolloutPromoteRequest, RolloutRollbackRequest, RolloutStatusResponse,
    RuleVersionsResponse, RulesMarketResponse, RuntimeModeUpdateRequest, RuntimeModesResponse,
    ScannerRunResponse, ScannerRunsResponse, ServicesResponse,
};
use polymarket_common::init_tracing_with_sampling;
use polymarket_config::{MonitoringConfig, OpsApiConfig, RolloutConfig};
use polymarket_core::{
    now, AccountDomain, AlertEvent, AlertRuleKind, AlertSeverity, AlertStatus, ExecutionCommand,
    ExecutionCommandKind, ExecutionHeartbeat, ExecutionReconcileReport, GraphScope,
    MdGatewayRuntime,
    MetricSample, NewDurableEvent, NewStateSnapshot, PromotionCandidate, PromotionStage,
    ReplayJob, ReplayRequest, RolloutEvaluation, RolloutEvidence, RolloutGuardrail,
    RolloutIncident, RolloutIncidentSeverity, RolloutPolicy, RuntimeHealth, RuntimeMode,
    StrategyKind,
};
use polymarket_rules::build_replay_report;
use polymarket_storage::Store;
use serde::Deserialize;
use tower_http::trace::TraceLayer;
use uuid::Uuid;

#[derive(Clone)]
struct AppState {
    store: Store,
    authenticator: Arc<OperatorAuthenticator>,
    target_domain: AccountDomain,
    replay_worker_ready: Arc<AtomicBool>,
    monitoring_ready: Arc<AtomicBool>,
    monitoring: MonitoringConfig,
    rollout: RolloutConfig,
    alert_webhook: Option<String>,
    prometheus_enabled: bool,
    webhook_timeout: Duration,
    webhook_retries: usize,
    http_client: reqwest::Client,
}

#[derive(Debug, Clone)]
struct OperatorIdentity {
    name: Arc<str>,
}

#[derive(Debug, Clone)]
struct OperatorAuthenticator {
    operators: Vec<(Arc<str>, Arc<str>)>,
    legacy_token: Option<Arc<str>>,
}

#[derive(Debug, Deserialize)]
struct AuditQuery {
    limit: Option<usize>,
    domain: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GraphQuery {
    event_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ReplayQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct AlertQuery {
    limit: Option<usize>,
    status: Option<String>,
    severity: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpportunitiesQuery {
    strategy: Option<String>,
    market_id: Option<String>,
    event_id: Option<String>,
    min_edge_net_bps: Option<i32>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ExecutionIntentsQuery {
    limit: Option<usize>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = OpsApiConfig::from_env()?;
    let allow_non_loopback = std::env::var("POLYMARKET_OPS_ALLOW_NON_LOOPBACK")
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false);
    ensure!(
        config.bind_addr.ip().is_loopback() || allow_non_loopback,
        "refusing to bind ops API to non-loopback address {}; set POLYMARKET_OPS_ALLOW_NON_LOOPBACK=true to override",
        config.bind_addr
    );
    init_tracing_with_sampling(
        &config.shared.log_filter,
        config.telemetry.trace_sample_ratio,
    );

    let domain_config = config.target_domain_config.clone();
    let store = Store::new(
        domain_config.database_path,
        config.target_domain,
        domain_config.descriptor.namespace.clone(),
        domain_config.audit_prefix,
    );
    store.init()?;

    let state = AppState {
        store,
        authenticator: Arc::new(OperatorAuthenticator::from_config(&config)),
        target_domain: config.target_domain,
        replay_worker_ready: Arc::new(AtomicBool::new(false)),
        monitoring_ready: Arc::new(AtomicBool::new(false)),
        monitoring: config.monitoring.clone(),
        rollout: config.rollout.clone(),
        alert_webhook: config.alerting.webhook_url.clone(),
        prometheus_enabled: config.alerting.prometheus_enabled,
        webhook_timeout: config.alerting.webhook_timeout,
        webhook_retries: config.alerting.webhook_retries,
        http_client: reqwest::Client::builder()
            .timeout(config.alerting.webhook_timeout)
            .build()?,
    };
    sync_rollout_policies(&state)?;
    spawn_replay_worker(state.clone());
    spawn_monitoring_worker(state.clone());

    let app = build_app(state);
    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_app(state: AppState) -> Router {
    let v1 = Router::new()
        .route("/runtime-modes", get(list_runtime_modes))
        .route("/runtime-modes/:domain", put(set_runtime_mode))
        .route("/services/:domain", get(list_services))
        .route("/audit-events", get(list_audit_events))
        .route("/health/:domain", get(health_snapshot))
        .route("/alerts", get(list_alerts))
        .route("/alerts/:alert_id/ack", post(acknowledge_alert))
        .route("/execution/:domain/health", get(get_execution_health))
        .route("/execution/:domain/intents", get(list_execution_intents))
        .route("/execution/:domain/orders", get(list_execution_orders))
        .route("/execution/:domain/reconcile", get(get_execution_reconcile))
        .route("/execution/:domain/cancel-all", post(cancel_all))
        .route("/execution/:domain/reconcile", post(trigger_reconcile))
        .route("/execution/:domain/recover", post(trigger_recover))
        .route("/rules/markets/:market_id", get(get_rules_market))
        .route(
            "/rules/markets/:market_id/versions",
            get(list_rule_versions),
        )
        .route("/rules/events/:event_id", get(get_event_family))
        .route("/rules/graph", get(get_constraint_graph))
        .route(
            "/rules/graph/:market_id",
            get(get_constraint_graph_for_market),
        )
        .route(
            "/rules/replay/runs",
            get(list_replay_runs).post(trigger_replay),
        )
        .route("/rules/replay/runs/:run_id", get(get_replay_run))
        .route("/rules/replay/runs/:run_id/trace", get(get_replay_trace))
        .route("/rules/replay/jobs/:job_id", get(get_replay_job))
        .route("/opportunities", get(list_opportunities))
        .route("/opportunities/:opportunity_id", get(get_opportunity))
        .route("/opportunities/scanner-runs", get(list_scanner_runs))
        .route("/opportunities/scanner-runs/:run_id", get(get_scanner_run))
        .route("/rollout/:domain", get(get_rollout_status))
        .route("/rollout/:domain/policy", get(get_rollout_policy))
        .route("/rollout/:domain/evaluations", get(list_rollout_evaluations))
        .route("/rollout/:domain/incidents", get(list_rollout_incidents))
        .route("/rollout/:domain/evaluate", post(trigger_rollout_evaluation))
        .route("/rollout/:domain/promote", post(promote_rollout_stage))
        .route("/rollout/:domain/rollback", post(rollback_rollout_stage))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            authorize_v1_request,
        ));

    Router::new()
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .nest("/v1", v1)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

fn sync_rollout_policies(state: &AppState) -> Result<()> {
    for stage in PromotionStage::PATH {
        let mut policy = state.rollout.policy(stage)?.clone();
        policy.domain = state.target_domain;
        state.store.upsert_rollout_policy(state.target_domain, &policy)?;
    }
    Ok(())
}

fn current_rollout_stage(state: &AppState, domain: AccountDomain) -> Result<polymarket_core::RolloutStageRecord> {
    Ok(state
        .store
        .get_current_rollout_stage(domain)?
        .unwrap_or_else(|| polymarket_core::RolloutStageRecord {
            domain,
            stage: default_rollout_stage(domain),
            previous_stage: None,
            approved_by: None,
            approved_at: None,
            reason: "default rollout stage".to_owned(),
            updated_at: now(),
        }))
}

fn default_rollout_stage(domain: AccountDomain) -> PromotionStage {
    match domain {
        AccountDomain::Sim => PromotionStage::Replay,
        AccountDomain::Canary => PromotionStage::Canary,
        AccountDomain::Live => PromotionStage::Live,
    }
}

fn build_rollout_guardrail(
    code: &str,
    summary: &str,
    detail: impl Into<String>,
) -> RolloutGuardrail {
    RolloutGuardrail {
        code: code.to_owned(),
        summary: summary.to_owned(),
        detail: detail.into(),
    }
}

fn evaluate_rollout_for_domain(state: &AppState, domain: AccountDomain) -> Result<RolloutEvaluation> {
    sync_rollout_policies(state)?;
    let current_stage = current_rollout_stage(state, domain)?;
    let policy = state
        .store
        .load_rollout_policy(domain, current_stage.stage)?
        .unwrap_or_else(|| {
            let mut policy = state
                .rollout
                .policy(current_stage.stage)
                .expect("rollout policy")
                .clone();
            policy.domain = domain;
            policy
        });
    let health = state
        .store
        .health_snapshot(domain)?
        .runtime_health
        .unwrap_or(collect_runtime_health(state)?);
    let critical_alerts = state
        .store
        .list_alerts(domain, Some(AlertStatus::Open), Some(AlertSeverity::Critical), 200)?
        .len();
    let recent_alert = state
        .store
        .list_alerts(domain, None, None, 1)?
        .into_iter()
        .next();
    let unresolved_incidents = state.store.list_rollout_incidents(domain, false, 200)?.len();
    let threshold = state.rollout.threshold(current_stage.stage)?;
    let stable_window_secs = health
        .stable_since
        .map(|value| (health.now - value).num_seconds().max(0) as u64)
        .unwrap_or_default();
    let evidence = RolloutEvidence {
        runtime_mode: health.runtime_mode.clone(),
        heartbeat_age_ms: health.heartbeat_age_ms,
        reject_rate_5m: health.reject_rate_5m,
        fill_rate_5m: health.fill_rate_5m,
        recent_425_count: health.recent_425_count,
        market_ws_lag_ms: health.market_ws_lag_ms,
        reconcile_drift: health.reconcile_drift,
        disputed_capital_ratio: health.disputed_capital_ratio,
        latest_alert_severity: recent_alert.as_ref().map(|alert| alert.severity),
        open_alerts: critical_alerts,
        unresolved_incidents,
        stable_window_secs,
        shadow_live_drift_bps: health.shadow_live_drift_bps,
    };

    let mut blocking_reasons = Vec::new();
    let mut warnings = Vec::new();
    if health.heartbeat_age_ms > threshold.heartbeat_max_ms {
        blocking_reasons.push(build_rollout_guardrail(
            "HEARTBEAT_STALE",
            "heartbeat exceeded rollout threshold",
            format!(
                "heartbeat_age_ms={} > {}",
                health.heartbeat_age_ms, threshold.heartbeat_max_ms
            ),
        ));
    }
    if health.reject_rate_5m > threshold.reject_rate_max {
        blocking_reasons.push(build_rollout_guardrail(
            "REJECT_RATE_HIGH",
            "reject rate exceeded rollout threshold",
            format!(
                "reject_rate_5m={:.4} > {:.4}",
                health.reject_rate_5m, threshold.reject_rate_max
            ),
        ));
    }
    if health.fill_rate_5m < threshold.fill_rate_min {
        blocking_reasons.push(build_rollout_guardrail(
            "FILL_RATE_LOW",
            "fill rate below rollout threshold",
            format!(
                "fill_rate_5m={:.4} < {:.4}",
                health.fill_rate_5m, threshold.fill_rate_min
            ),
        ));
    }
    if health.market_ws_lag_ms > threshold.market_ws_lag_max_ms {
        blocking_reasons.push(build_rollout_guardrail(
            "MARKET_WS_LAG",
            "market websocket lag exceeded rollout threshold",
            format!(
                "market_ws_lag_ms={} > {}",
                health.market_ws_lag_ms, threshold.market_ws_lag_max_ms
            ),
        ));
    }
    if health.reconcile_drift {
        blocking_reasons.push(build_rollout_guardrail(
            "RECONCILE_DRIFT",
            "reconciliation drift detected",
            "execution ledger diverged from remote state",
        ));
    }
    if health.disputed_capital_ratio > threshold.disputed_capital_ratio_max {
        blocking_reasons.push(build_rollout_guardrail(
            "DISPUTED_CAPITAL",
            "disputed capital exceeded rollout threshold",
            format!(
                "disputed_capital_ratio={:.4} > {:.4}",
                health.disputed_capital_ratio, threshold.disputed_capital_ratio_max
            ),
        ));
    }
    if critical_alerts > threshold.max_open_critical_alerts {
        blocking_reasons.push(build_rollout_guardrail(
            "CRITICAL_ALERTS_OPEN",
            "open critical alerts exceed limit",
            format!(
                "open_critical_alerts={} > {}",
                critical_alerts, threshold.max_open_critical_alerts
            ),
        ));
    }
    if unresolved_incidents > 0 {
        blocking_reasons.push(build_rollout_guardrail(
            "UNRESOLVED_INCIDENT",
            "unresolved rollout incidents exist",
            format!("unresolved_incidents={unresolved_incidents}"),
        ));
    }
    if stable_window_secs < threshold.stable_window.as_secs() {
        blocking_reasons.push(build_rollout_guardrail(
            "STABILITY_WINDOW",
            "stability window is not yet satisfied",
            format!(
                "stable_window_secs={} < {}",
                stable_window_secs,
                threshold.stable_window.as_secs()
            ),
        ));
    }
    if policy.capabilities.require_shadow_alignment {
        if let Some(drift) = health.shadow_live_drift_bps {
            if drift > threshold.max_shadow_live_drift_bps {
                blocking_reasons.push(build_rollout_guardrail(
                    "SHADOW_DRIFT",
                    "shadow/live drift exceeded threshold",
                    format!(
                        "shadow_live_drift_bps={:.2} > {:.2}",
                        drift, threshold.max_shadow_live_drift_bps
                    ),
                ));
            }
        } else {
            warnings.push(build_rollout_guardrail(
                "MISSING_SHADOW_DRIFT",
                "shadow/live drift metric is unavailable",
                "shadow alignment is required but no drift metric is present",
            ));
        }
    }
    if health.runtime_mode.eq_ignore_ascii_case("SAFE") {
        blocking_reasons.push(build_rollout_guardrail(
            "RUNTIME_SAFE",
            "runtime is already in safe mode",
            "promotion is blocked while runtime mode is SAFE",
        ));
    }

    let target_stage = current_stage.stage.next();
    let eligible = target_stage.is_some() && blocking_reasons.is_empty();
    let evaluation = RolloutEvaluation {
        evaluation_id: Uuid::new_v4(),
        domain,
        current_stage: current_stage.stage,
        target_stage,
        eligible,
        blocking_reasons,
        warnings,
        evidence,
        evaluated_at: health.now,
    };
    state.store.record_rollout_evaluation(evaluation.clone())?;
    if evaluation.eligible {
        let target_stage = evaluation.target_stage.expect("eligible target stage");
        state
            .store
            .upsert_promotion_candidate(PromotionCandidate {
                candidate_id: Uuid::new_v4(),
                domain,
                current_stage: current_stage.stage,
                target_stage,
                evidence_summary: format!(
                    "stable_window={}s heartbeat={}ms reject_rate={:.4} fill_rate={:.4}",
                    evaluation.evidence.stable_window_secs,
                    evaluation.evidence.heartbeat_age_ms,
                    evaluation.evidence.reject_rate_5m,
                    evaluation.evidence.fill_rate_5m
                ),
                blocking_reasons: evaluation.warnings.clone(),
                valid_until: evaluation.evaluated_at
                    + chrono::Duration::from_std(threshold.candidate_ttl)
                        .unwrap_or_else(|_| chrono::Duration::minutes(5)),
                created_at: evaluation.evaluated_at,
                invalidated_at: None,
            })?;
    } else {
        state
            .store
            .invalidate_promotion_candidates(domain, evaluation.evaluated_at)?;
    }
    maybe_auto_rollback(state, &current_stage, &evaluation)?;
    Ok(evaluation)
}

fn maybe_auto_rollback(
    state: &AppState,
    current_stage: &polymarket_core::RolloutStageRecord,
    evaluation: &RolloutEvaluation,
) -> Result<()> {
    let Some(previous_stage) = current_stage.stage.previous() else {
        return Ok(());
    };
    let should_rollback = matches!(
        current_stage.stage,
        PromotionStage::Canary | PromotionStage::Live
    ) && evaluation.blocking_reasons.iter().any(|item| {
        matches!(
            item.code.as_str(),
            "RECONCILE_DRIFT" | "HEARTBEAT_STALE" | "REJECT_RATE_HIGH" | "CRITICAL_ALERTS_OPEN"
        )
    });
    if !should_rollback {
        return Ok(());
    }

    state.store.set_rollout_stage(
        current_stage.domain,
        previous_stage,
        None,
        None,
        format!("automatic rollback from {} due to rollout blockers", current_stage.stage),
    )?;
    state
        .store
        .invalidate_promotion_candidates(current_stage.domain, evaluation.evaluated_at)?;
    let incident = RolloutIncident {
        incident_id: Uuid::new_v4(),
        domain: current_stage.domain,
        stage: current_stage.stage,
        severity: RolloutIncidentSeverity::Critical,
        code: "AUTO_ROLLBACK".to_owned(),
        summary: "rollout stage automatically rolled back".to_owned(),
        detail: serde_json::to_string(&evaluation.blocking_reasons)?,
        auto_rollback_stage: Some(previous_stage),
        created_at: evaluation.evaluated_at,
        resolved_at: None,
    };
    state.store.record_rollout_incident(incident)?;
    let runtime_mode = if state.rollout.auto_rollback_to_safe {
        RuntimeMode::Observe
    } else {
        RuntimeMode::Simulate
    };
    state.store.set_runtime_mode(
        current_stage.domain,
        runtime_mode,
        format!("rollout auto rollback from {} to {}", current_stage.stage, previous_stage),
    )?;
    let _ = enqueue_execution_command(
        &state.store,
        current_stage.domain,
        ExecutionCommandKind::CancelAll,
        "ops-api.monitor",
        "automatic rollout rollback",
    );
    state.store.append_audit(
        Some(current_stage.domain),
        "ops-api",
        "rollout.auto-rollback",
        serde_json::json!({
            "from_stage": current_stage.stage,
            "to_stage": previous_stage,
            "runtime_mode": runtime_mode,
            "blocking_reasons": evaluation.blocking_reasons,
        })
        .to_string(),
    )?;
    Ok(())
}

fn spawn_replay_worker(state: AppState) {
    tokio::spawn(async move {
        state.replay_worker_ready.store(true, Ordering::Release);
        let worker_id = format!("ops-api-{}", std::process::id());
        loop {
            let result = run_replay_worker_iteration(&state, &worker_id).await;
            let idle = matches!(result, Ok(false));
            if result.is_err() {
                let _ = result;
            }
            tokio::time::sleep(if idle {
                Duration::from_millis(250)
            } else {
                Duration::from_millis(25)
            })
            .await;
        }
    });
}

fn spawn_monitoring_worker(state: AppState) {
    tokio::spawn(async move {
        state.monitoring_ready.store(true, Ordering::Release);
        loop {
            let _ = run_monitoring_iteration(&state).await;
            tokio::time::sleep(state.monitoring.poll_interval).await;
        }
    });
}

async fn run_replay_worker_iteration(state: &AppState, worker_id: &str) -> Result<bool> {
    let Some(job) = state.store.claim_pending_replay_job(worker_id)? else {
        return Ok(false);
    };
    process_replay_job(state, &job)?;
    Ok(true)
}

fn process_replay_job(state: &AppState, job: &ReplayJob) -> Result<()> {
    let request = ReplayRequest {
        domain: job.domain,
        after_sequence: job.after_sequence,
        limit: job.limit,
        reason: job.reason.clone(),
        alert_id: job.alert_id,
        audit_event_id: job.audit_event_id,
    };
    let outcome = (|| -> Result<()> {
        let events =
            state
                .store
                .replay_rules_market_events(job.domain, job.after_sequence, job.limit)?;
        let report = build_replay_report(&request, events.len(), Vec::new());
        state.store.record_rules_replay_run(report.clone())?;
        state.store.complete_replay_job(job.job_id, report.run_id)?;
        state.store.append_audit(
            Some(job.domain),
            "ops-api",
            "rules.replay.completed",
            serde_json::json!({
                "source": "ops-api",
                "operator": job.requested_by,
                "job_id": job.job_id,
                "run_id": report.run_id,
                "processed_events": report.processed_events,
                "limit": job.limit,
            })
            .to_string(),
        )?;
        Ok(())
    })();

    if let Err(error) = outcome {
        let detail = error.to_string();
        state.store.fail_replay_job(job.job_id, &detail)?;
        state.store.upsert_alert(AlertEvent {
            alert_id: job.alert_id.unwrap_or_else(Uuid::new_v4),
            domain: job.domain,
            rule_kind: AlertRuleKind::ReplayFailure,
            severity: AlertSeverity::Critical,
            status: AlertStatus::Open,
            dedupe_key: format!("REPLAY_FAILURE:{}", job.job_id),
            source: "ops-api.replay".to_owned(),
            summary: "replay job failed".to_owned(),
            detail: detail.clone(),
            metric_key: None,
            metric_value: None,
            threshold: None,
            trigger_value: None,
            labels: BTreeMap::from([("job_id".to_owned(), job.job_id.to_string())]),
            created_at: now(),
            updated_at: now(),
            acknowledged_at: None,
            acknowledged_by: None,
            resolved_at: None,
            audit_event_id: job.audit_event_id,
            replay_job_id: Some(job.job_id),
            replay_run_id: None,
            last_sent_at: None,
        })?;
        state.store.append_audit(
            Some(job.domain),
            "ops-api",
            "rules.replay.failed",
            serde_json::json!({
                "source": "ops-api",
                "operator": job.requested_by,
                "job_id": job.job_id,
                "limit": job.limit,
                "error": detail,
            })
            .to_string(),
        )?;
        return Err(error);
    }

    Ok(())
}

async fn run_monitoring_iteration(state: &AppState) -> Result<()> {
    let health = collect_runtime_health(state)?;
    persist_runtime_health(state, &health)?;
    let alerts = evaluate_runtime_alerts(state, &health)?;
    for alert in alerts {
        let saved = state.store.upsert_alert(alert)?;
        if matches!(saved.status, AlertStatus::Open | AlertStatus::Acknowledged) {
            dispatch_alert_webhook(state, &saved).await?;
        }
    }
    let _ = evaluate_rollout_for_domain(state, state.target_domain)?;
    Ok(())
}

fn collect_runtime_health(state: &AppState) -> Result<RuntimeHealth> {
    let domain = state.target_domain;
    let runtime_mode = state
        .store
        .runtime_mode(domain)?
        .map(|record| record.mode.to_string())
        .unwrap_or_else(|| domain.default_runtime_mode().to_string());
    let execution_heartbeat = state
        .store
        .latest_snapshot(domain, "execution_heartbeat", "latest")?
        .map(|snapshot| serde_json::from_value::<ExecutionHeartbeat>(snapshot.payload))
        .transpose()?;
    let reconcile = state
        .store
        .latest_snapshot(domain, "execution_reconcile", "latest")?
        .map(|snapshot| serde_json::from_value::<ExecutionReconcileReport>(snapshot.payload))
        .transpose()?;
    let md_runtime = state
        .store
        .latest_snapshot(domain, "md_gateway_runtime", "latest")?
        .map(|snapshot| serde_json::from_value::<MdGatewayRuntime>(snapshot.payload))
        .transpose()?;
    let latest_alert = state
        .store
        .list_alerts(domain, None, None, 1)?
        .into_iter()
        .next();
    let latest_rollout = state.store.latest_rollout_evaluation(domain)?;
    let open_orders = state.store.list_open_orders(domain)?.len();
    let recent_orders = state
        .store
        .list_all_order_lifecycle(domain)?
        .into_iter()
        .filter(|order| (now() - order.updated_at).num_seconds() <= 300)
        .collect::<Vec<_>>();
    let now_ts = now();
    let heartbeat_age_ms = execution_heartbeat
        .as_ref()
        .map(|heartbeat| (now_ts - heartbeat.observed_at).num_milliseconds().max(0) as u64)
        .unwrap_or(state.monitoring.heartbeat_safe_ms + 1);
    let recent_425_count = execution_heartbeat
        .as_ref()
        .and_then(|heartbeat| {
            heartbeat
                .detail
                .to_ascii_lowercase()
                .contains("425")
                .then_some(1)
        })
        .unwrap_or_default();
    let user_ws_ok = execution_heartbeat
        .as_ref()
        .map(|heartbeat| {
            heartbeat.venue_healthy
                && heartbeat.signer_healthy
                && md_runtime
                    .as_ref()
                    .map(|runtime| runtime.user_stream_connected)
                    .unwrap_or(false)
        })
        .unwrap_or(false);
    let market_ws_lag_ms = md_runtime
        .as_ref()
        .and_then(|runtime| runtime.last_market_event_at)
        .map(|timestamp| (now_ts - timestamp).num_milliseconds().max(0) as u64)
        .unwrap_or(state.monitoring.market_ws_lag_safe_ms + 1);
    let reconcile_drift = reconcile
        .as_ref()
        .map(|report| report.mismatches_fixed > 0 || report.missing_remote_orders > 0)
        .unwrap_or(false);
    let terminal_recent = recent_orders
        .iter()
        .filter(|order| order.status.is_terminal())
        .count();
    let rejected_recent = recent_orders
        .iter()
        .filter(|order| {
            matches!(
                order.status,
                polymarket_core::OrderLifecycleStatus::Rejected
                    | polymarket_core::OrderLifecycleStatus::Expired
            )
        })
        .count();
    let filled_recent = recent_orders
        .iter()
        .filter(|order| {
            matches!(
                order.status,
                polymarket_core::OrderLifecycleStatus::Filled
                    | polymarket_core::OrderLifecycleStatus::PartiallyFilled
                    | polymarket_core::OrderLifecycleStatus::Settled
            )
        })
        .count();
    let reject_rate_5m = if terminal_recent == 0 {
        0.0
    } else {
        rejected_recent as f64 / terminal_recent as f64
    };
    let fill_rate_5m = if terminal_recent > 0 {
        filled_recent as f64 / terminal_recent as f64
    } else if open_orders == 0 && !reconcile_drift {
        1.0
    } else if reconcile_drift {
        0.5
    } else {
        0.8
    };
    let degradation_reason = if heartbeat_age_ms >= state.monitoring.heartbeat_safe_ms {
        Some("heartbeat exceeded safe threshold".to_owned())
    } else if !user_ws_ok {
        Some("venue or signer health is degraded".to_owned())
    } else if reconcile_drift {
        Some("reconcile drift detected".to_owned())
    } else if market_ws_lag_ms >= state.monitoring.market_ws_lag_degraded_ms {
        Some("market websocket lag elevated".to_owned())
    } else {
        None
    };
    Ok(RuntimeHealth {
        domain,
        runtime_mode,
        now: now_ts,
        market_ws_lag_ms,
        user_ws_ok,
        heartbeat_age_ms,
        recent_425_count,
        reject_rate_5m,
        reconcile_drift,
        capital_buffer_ok: !reconcile_drift,
        fill_rate_5m,
        open_orders,
        reconcile_lag_ms: reconcile
            .as_ref()
            .map(|report| (now_ts - report.generated_at).num_milliseconds().max(0) as u64)
            .unwrap_or_default(),
        disputed_capital_ratio: 0.0,
        degradation_reason,
        last_alert_at: latest_alert.map(|alert| alert.updated_at),
        stable_since: latest_rollout
            .as_ref()
            .filter(|evaluation| evaluation.blocking_reasons.is_empty())
            .map(|evaluation| evaluation.evaluated_at),
        shadow_live_drift_bps: Some(if reconcile_drift { 250.0 } else { 25.0 }),
    })
}

fn persist_runtime_health(state: &AppState, health: &RuntimeHealth) -> Result<()> {
    state.store.upsert_snapshot(NewStateSnapshot {
        domain: health.domain,
        aggregate_type: "runtime_health".to_owned(),
        aggregate_id: "latest".to_owned(),
        version: health.now.timestamp_millis(),
        payload: serde_json::to_value(health)?,
        derived_from_sequence: None,
        created_at: health.now,
    })?;
    for (metric_key, metric_kind, value) in [
        ("heartbeat_age_ms", "gauge", health.heartbeat_age_ms as f64),
        ("market_ws_lag_ms", "gauge", health.market_ws_lag_ms as f64),
        ("reject_rate_5m", "gauge", health.reject_rate_5m),
        ("fill_rate_5m", "gauge", health.fill_rate_5m),
        ("open_orders", "gauge", health.open_orders as f64),
        ("reconcile_lag_ms", "gauge", health.reconcile_lag_ms as f64),
        (
            "disputed_capital_ratio",
            "gauge",
            health.disputed_capital_ratio,
        ),
    ] {
        state.store.record_metric_sample(MetricSample {
            sample_id: Uuid::new_v4(),
            domain: health.domain,
            metric_key: metric_key.to_owned(),
            metric_kind: metric_kind.to_owned(),
            value,
            labels: BTreeMap::new(),
            observed_at: health.now,
        })?;
    }
    Ok(())
}

fn evaluate_runtime_alerts(state: &AppState, health: &RuntimeHealth) -> Result<Vec<AlertEvent>> {
    let mut alerts = Vec::new();
    let now_ts = health.now;
    alerts.extend(build_threshold_alert(
        state,
        health,
        AlertRuleKind::HeartbeatStale,
        "heartbeat_age_ms",
        state.monitoring.heartbeat_degraded_ms as f64,
        state.monitoring.heartbeat_safe_ms as f64,
        health.heartbeat_age_ms as f64,
        "execution heartbeat is stale",
    )?);
    alerts.extend(build_threshold_alert(
        state,
        health,
        AlertRuleKind::MarketWsLag,
        "market_ws_lag_ms",
        state.monitoring.market_ws_lag_degraded_ms as f64,
        state.monitoring.market_ws_lag_safe_ms as f64,
        health.market_ws_lag_ms as f64,
        "market websocket lag is elevated",
    )?);
    alerts.extend(build_threshold_alert(
        state,
        health,
        AlertRuleKind::RejectRate,
        "reject_rate_5m",
        (state.monitoring.reject_rate_degraded_bps as f64) / 10_000.0,
        (state.monitoring.reject_rate_safe_bps as f64) / 10_000.0,
        health.reject_rate_5m,
        "execution reject rate is elevated",
    )?);
    if !health.user_ws_ok {
        alerts.push(new_alert(
            health,
            AlertRuleKind::UserWsDisconnected,
            AlertSeverity::Critical,
            "user websocket is unhealthy",
            "venue or signer connectivity failed health checks",
            None,
            None,
            now_ts,
        ));
    } else {
        let _ = state
            .store
            .resolve_alert(health.domain, "USER_WS_DISCONNECTED", "user websocket recovered");
    }
    if health.reconcile_drift {
        alerts.push(new_alert(
            health,
            AlertRuleKind::ReconcileDrift,
            AlertSeverity::Critical,
            "reconciliation drift detected",
            "remote order state diverged from local lifecycle state",
            None,
            None,
            now_ts,
        ));
    } else {
        let _ = state
            .store
            .resolve_alert(health.domain, "RECONCILE_DRIFT", "reconcile drift cleared");
    }
    Ok(alerts)
}

fn build_threshold_alert(
    state: &AppState,
    health: &RuntimeHealth,
    rule_kind: AlertRuleKind,
    metric_key: &str,
    degraded_threshold: f64,
    safe_threshold: f64,
    actual: f64,
    summary: &str,
) -> Result<Vec<AlertEvent>> {
    let now_ts = health.now;
    if actual >= safe_threshold {
        Ok(vec![new_alert(
            health,
            rule_kind,
            AlertSeverity::Critical,
            summary,
            &format!("{metric_key}={actual} exceeded safe threshold {safe_threshold}"),
            Some(metric_key),
            Some(safe_threshold),
            now_ts,
        )])
    } else if actual >= degraded_threshold {
        Ok(vec![new_alert(
            health,
            rule_kind,
            AlertSeverity::Warning,
            summary,
            &format!("{metric_key}={actual} exceeded degraded threshold {degraded_threshold}"),
            Some(metric_key),
            Some(degraded_threshold),
            now_ts,
        )])
    } else {
        let _ = state.store.resolve_alert(
            health.domain,
            rule_kind.as_str(),
            &format!("{metric_key} recovered below degraded threshold"),
        )?;
        Ok(Vec::new())
    }
}

fn new_alert(
    health: &RuntimeHealth,
    rule_kind: AlertRuleKind,
    severity: AlertSeverity,
    summary: &str,
    detail: &str,
    metric_key: Option<&str>,
    threshold: Option<f64>,
    now_ts: polymarket_core::Timestamp,
) -> AlertEvent {
    AlertEvent {
        alert_id: Uuid::new_v4(),
        domain: health.domain,
        rule_kind,
        severity,
        status: AlertStatus::Open,
        dedupe_key: rule_kind.as_str().to_owned(),
        source: "ops-api.monitor".to_owned(),
        summary: summary.to_owned(),
        detail: detail.to_owned(),
        metric_key: metric_key.map(str::to_owned),
        metric_value: metric_key.map(|_| match rule_kind {
            AlertRuleKind::HeartbeatStale => health.heartbeat_age_ms as f64,
            AlertRuleKind::MarketWsLag => health.market_ws_lag_ms as f64,
            AlertRuleKind::RejectRate => health.reject_rate_5m,
            _ => 0.0,
        }),
        threshold,
        trigger_value: metric_key.map(|_| match rule_kind {
            AlertRuleKind::HeartbeatStale => health.heartbeat_age_ms as f64,
            AlertRuleKind::MarketWsLag => health.market_ws_lag_ms as f64,
            AlertRuleKind::RejectRate => health.reject_rate_5m,
            _ => 0.0,
        }),
        labels: BTreeMap::from([("runtime_mode".to_owned(), health.runtime_mode.clone())]),
        created_at: now_ts,
        updated_at: now_ts,
        acknowledged_at: None,
        acknowledged_by: None,
        resolved_at: None,
        audit_event_id: None,
        replay_job_id: None,
        replay_run_id: None,
        last_sent_at: None,
    }
}

async fn dispatch_alert_webhook(state: &AppState, alert: &AlertEvent) -> Result<()> {
    let Some(url) = state.alert_webhook.as_ref() else {
        return Ok(());
    };
    for attempt in 0..state.webhook_retries.max(1) {
        let response = state
            .http_client
            .post(url)
            .json(alert)
            .send()
            .await;
        match response {
            Ok(result) if result.status().is_success() => return Ok(()),
            Ok(_) | Err(_) if attempt + 1 < state.webhook_retries.max(1) => continue,
            Ok(result) => {
                return Err(anyhow::anyhow!(
                    "alert webhook returned status {}",
                    result.status()
                ));
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

async fn authorize_v1_request(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let operator = state
        .authenticator
        .authenticate(request.headers())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    request.extensions_mut().insert(operator);
    Ok(next.run(request).await)
}

async fn livez() -> StatusCode {
    StatusCode::OK
}

async fn metrics(State(state): State<AppState>) -> Response {
    if !state.prometheus_enabled {
        return StatusCode::NOT_FOUND.into_response();
    }
    match render_metrics(&state) {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn readyz(State(state): State<AppState>) -> StatusCode {
    if !state.replay_worker_ready.load(Ordering::Acquire)
        || !state.monitoring_ready.load(Ordering::Acquire)
    {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let Ok(services) = state.store.list_service_heartbeats(state.target_domain) else {
        return StatusCode::SERVICE_UNAVAILABLE;
    };
    let now_ts = now();
    for service in [
        polymarket_core::ServiceKind::MdGateway,
        polymarket_core::ServiceKind::RiskEngine,
        polymarket_core::ServiceKind::ExecutionEngine,
        polymarket_core::ServiceKind::SettlementEngine,
    ] {
        let Some(heartbeat) = services.iter().find(|item| item.service == service) else {
            return StatusCode::SERVICE_UNAVAILABLE;
        };
        if !heartbeat.healthy
            || (now_ts - heartbeat.last_seen_at).num_milliseconds()
                > state.monitoring.heartbeat_safe_ms as i64
        {
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }
    let md_ready = state
        .store
        .latest_snapshot(state.target_domain, "md_gateway_runtime", "latest")
        .ok()
        .flatten()
        .is_some();
    let risk_ready = state
        .store
        .latest_snapshot(state.target_domain, "risk_runtime_decision", "current")
        .ok()
        .flatten()
        .is_some();
    let execution_ready = state
        .store
        .latest_snapshot(state.target_domain, "execution_heartbeat", "latest")
        .ok()
        .flatten()
        .is_some();
    let settlement_ready = state
        .store
        .latest_snapshot(state.target_domain, "settlement_report", "latest")
        .ok()
        .flatten()
        .is_some();
    if md_ready && risk_ready && execution_ready && settlement_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn list_runtime_modes(
    State(state): State<AppState>,
) -> Result<Json<ApiEnvelope<RuntimeModesResponse>>, StatusCode> {
    let runtime_modes = state.store.list_runtime_modes().map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RuntimeModesResponse { runtime_modes },
    }))
}

async fn set_runtime_mode(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Json(payload): Json<RuntimeModeUpdateRequest>,
) -> Result<Json<ApiEnvelope<RuntimeModesResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    state
        .store
        .set_runtime_mode(domain, payload.mode, payload.reason.clone())
        .map_err(internal_error)?;
    append_ops_audit(
        &state.store,
        domain,
        &operator,
        "runtime-mode.updated",
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "mode": payload.mode,
            "reason": payload.reason,
        }),
    )
    .map_err(internal_error)?;
    list_runtime_modes(State(state)).await
}

async fn list_services(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ServicesResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let services = state
        .store
        .list_service_heartbeats(domain)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ServicesResponse { services },
    }))
}

async fn list_audit_events(
    State(state): State<AppState>,
    Query(query): Query<AuditQuery>,
) -> Result<Json<ApiEnvelope<AuditEventsResponse>>, StatusCode> {
    let limit = query.limit.unwrap_or(100).min(500);
    let domain = query.domain.as_deref().map(parse_domain).transpose()?;
    let events = state
        .store
        .recent_audit_events_for_domain(domain, limit)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: AuditEventsResponse { events },
    }))
}

async fn health_snapshot(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<HealthResponse>>, StatusCode> {
    let snapshot = state
        .store
        .health_snapshot(ensure_target_domain(&state, &domain)?)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: HealthResponse { snapshot },
    }))
}

async fn list_alerts(
    State(state): State<AppState>,
    Query(query): Query<AlertQuery>,
) -> Result<Json<ApiEnvelope<AlertQueryResponse>>, StatusCode> {
    let status = query
        .status
        .as_deref()
        .map(parse_alert_status)
        .transpose()?;
    let severity = query
        .severity
        .as_deref()
        .map(parse_alert_severity)
        .transpose()?;
    let alerts = state
        .store
        .list_alerts(
            state.target_domain,
            status,
            severity,
            query.limit.unwrap_or(100).min(500),
        )
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: AlertQueryResponse {
            status,
            severity,
            alerts,
        },
    }))
}

async fn acknowledge_alert(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(alert_id): Path<String>,
) -> Result<Json<ApiEnvelope<AlertAckResponse>>, StatusCode> {
    let alert_id = Uuid::parse_str(&alert_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let alert = state
        .store
        .acknowledge_alert(alert_id, operator.name.as_ref())
        .map_err(internal_error)?;
    if let Some(alert) = &alert {
        append_ops_audit(
            &state.store,
            alert.domain,
            &operator,
            "alert.acknowledged",
            serde_json::json!({
                "source": "ops-api",
                "operator": operator.name.as_ref(),
                "alert_id": alert.alert_id,
                "rule_kind": alert.rule_kind,
            }),
        )
        .map_err(internal_error)?;
    }
    Ok(Json(ApiEnvelope {
        data: AlertAckResponse { alert },
    }))
}

async fn get_execution_health(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionHealthResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let heartbeat = state
        .store
        .latest_snapshot(domain, "execution_heartbeat", "latest")
        .map_err(internal_error)?
        .map(|snapshot| serde_json::from_value::<ExecutionHeartbeat>(snapshot.payload))
        .transpose()
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ExecutionHealthResponse { heartbeat },
    }))
}

async fn list_execution_intents(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<ExecutionIntentsQuery>,
) -> Result<Json<ApiEnvelope<ExecutionIntentsResponse>>, StatusCode> {
    let intents = state
        .store
        .list_execution_intents(ensure_target_domain(&state, &domain)?, query.limit.unwrap_or(100).min(500))
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ExecutionIntentsResponse { intents },
    }))
}

async fn list_execution_orders(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionOrdersResponse>>, StatusCode> {
    let orders = state
        .store
        .list_open_orders(ensure_target_domain(&state, &domain)?)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ExecutionOrdersResponse { orders },
    }))
}

async fn get_execution_reconcile(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionReconcileResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let report = state
        .store
        .latest_snapshot(domain, "execution_reconcile", "latest")
        .map_err(internal_error)?
        .map(|snapshot| serde_json::from_value::<ExecutionReconcileReport>(snapshot.payload))
        .transpose()
        .map_err(internal_error)?
        .unwrap_or(ExecutionReconcileReport {
            domain,
            generated_at: now(),
            checked_orders: 0,
            mismatches_fixed: 0,
            missing_remote_orders: 0,
            alerts: Vec::new(),
        });
    Ok(Json(ApiEnvelope {
        data: ExecutionReconcileResponse { report },
    }))
}

async fn cancel_all(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionActionResponse>>, StatusCode> {
    enqueue_execution_action(
        &state,
        ensure_target_domain(&state, &domain)?,
        &operator,
        ExecutionCommandKind::CancelAll,
        "execution.cancel-all.requested",
        "ops-api cancel_all",
    )
}

async fn trigger_reconcile(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionActionResponse>>, StatusCode> {
    enqueue_execution_action(
        &state,
        ensure_target_domain(&state, &domain)?,
        &operator,
        ExecutionCommandKind::Reconcile,
        "execution.reconcile.requested",
        "ops-api reconcile",
    )
}

async fn trigger_recover(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<ExecutionActionResponse>>, StatusCode> {
    enqueue_execution_action(
        &state,
        ensure_target_domain(&state, &domain)?,
        &operator,
        ExecutionCommandKind::Recover,
        "execution.recover.requested",
        "ops-api recover",
    )
}

async fn get_rules_market(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
) -> Result<Json<ApiEnvelope<RulesMarketResponse>>, StatusCode> {
    let market = state
        .store
        .get_rules_market(&market_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RulesMarketResponse { market },
    }))
}

async fn list_rule_versions(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
) -> Result<Json<ApiEnvelope<RuleVersionsResponse>>, StatusCode> {
    let versions = state
        .store
        .list_rule_versions(&market_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RuleVersionsResponse { versions },
    }))
}

async fn get_event_family(
    State(state): State<AppState>,
    Path(event_id): Path<String>,
) -> Result<Json<ApiEnvelope<EventFamilyResponse>>, StatusCode> {
    let snapshot = state
        .store
        .get_event_family_snapshot(&event_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: EventFamilyResponse { snapshot },
    }))
}

async fn get_constraint_graph(
    State(state): State<AppState>,
    Query(query): Query<GraphQuery>,
) -> Result<Json<ApiEnvelope<ConstraintGraphResponse>>, StatusCode> {
    let scope = match query.event_id {
        Some(event_id) => GraphScope::Event { event_id },
        None => GraphScope::All,
    };
    let snapshot = state
        .store
        .get_constraint_graph_snapshot(&scope)
        .map_err(internal_error)?;
    let edges = state
        .store
        .list_constraint_edges(&scope, None)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ConstraintGraphResponse { snapshot, edges },
    }))
}

async fn get_constraint_graph_for_market(
    State(state): State<AppState>,
    Path(market_id): Path<String>,
) -> Result<Json<ApiEnvelope<ConstraintGraphResponse>>, StatusCode> {
    let scope = GraphScope::Market { market_id };
    let snapshot = state
        .store
        .get_constraint_graph_snapshot(&scope)
        .map_err(internal_error)?;
    let edges = state
        .store
        .list_constraint_edges(&scope, None)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ConstraintGraphResponse { snapshot, edges },
    }))
}

async fn list_replay_runs(
    State(state): State<AppState>,
    Query(query): Query<ReplayQuery>,
) -> Result<Json<ApiEnvelope<ReplayRunsResponse>>, StatusCode> {
    let runs = state
        .store
        .list_rules_replay_runs(query.limit.unwrap_or(50).min(200))
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ReplayRunsResponse { runs },
    }))
}

async fn get_replay_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiEnvelope<ReplayRunResponse>>, StatusCode> {
    let run_id = Uuid::parse_str(&run_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let run = state
        .store
        .get_rules_replay_run(run_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ReplayRunResponse { run },
    }))
}

async fn get_replay_trace(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiEnvelope<ReplayTraceResponse>>, StatusCode> {
    let run_id = Uuid::parse_str(&run_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let trace = state
        .store
        .replay_trace_by_run(run_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ReplayTraceResponse { trace },
    }))
}

async fn trigger_replay(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Json(payload): Json<ReplayTriggerRequest>,
) -> Result<Json<ApiEnvelope<ReplayTriggerResponse>>, StatusCode> {
    let limit = clamp_limit(payload.limit, 1);
    if limit == 0 {
        return Err(StatusCode::BAD_REQUEST);
    }
    let request = ReplayRequest {
        domain: payload.domain,
        after_sequence: payload.after_sequence,
        limit,
        reason: payload.reason.clone(),
        alert_id: payload
            .alert_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|_| StatusCode::BAD_REQUEST)?,
        audit_event_id: payload
            .audit_event_id
            .as_deref()
            .map(Uuid::parse_str)
            .transpose()
            .map_err(|_| StatusCode::BAD_REQUEST)?,
    };
    let job = state
        .store
        .enqueue_replay_job(request, operator.name.as_ref())
        .map_err(internal_error)?;
    append_ops_audit(
        &state.store,
        job.domain,
        &operator,
        "rules.replay.requested",
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "job_id": job.job_id,
            "after_sequence": job.after_sequence,
            "limit": job.limit,
        }),
    )
    .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ReplayTriggerResponse {
            accepted: true,
            job_id: job.job_id.to_string(),
            detail: format!("replay job queued: {}", job.job_id),
        },
    }))
}

async fn get_replay_job(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<Json<ApiEnvelope<ReplayJobStatusResponse>>, StatusCode> {
    let job_id = Uuid::parse_str(&job_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let job = state.store.get_replay_job(job_id).map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ReplayJobStatusResponse { job },
    }))
}

async fn list_opportunities(
    State(state): State<AppState>,
    Query(query): Query<OpportunitiesQuery>,
) -> Result<Json<ApiEnvelope<OpportunitiesResponse>>, StatusCode> {
    let strategy = query.strategy.as_deref().map(parse_strategy).transpose()?;
    let opportunities = state
        .store
        .list_opportunities(
            strategy,
            query.market_id.as_deref(),
            query.event_id.as_deref(),
            query.min_edge_net_bps,
            query.limit.unwrap_or(100).min(500),
        )
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: OpportunitiesResponse { opportunities },
    }))
}

async fn get_opportunity(
    State(state): State<AppState>,
    Path(opportunity_id): Path<String>,
) -> Result<Json<ApiEnvelope<OpportunityResponse>>, StatusCode> {
    let opportunity_id = Uuid::parse_str(&opportunity_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let opportunity = state
        .store
        .get_opportunity(opportunity_id)
        .map_err(internal_error)?;
    let invalidations = state
        .store
        .list_opportunity_invalidations(Some(opportunity_id), 100)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: OpportunityResponse {
            opportunity,
            invalidations,
        },
    }))
}

async fn list_scanner_runs(
    State(state): State<AppState>,
    Query(query): Query<OpportunitiesQuery>,
) -> Result<Json<ApiEnvelope<ScannerRunsResponse>>, StatusCode> {
    let strategy = query.strategy.as_deref().map(parse_strategy).transpose()?;
    let runs = state
        .store
        .list_scanner_runs(strategy, query.limit.unwrap_or(100).min(500))
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ScannerRunsResponse { runs },
    }))
}

async fn get_scanner_run(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<ApiEnvelope<ScannerRunResponse>>, StatusCode> {
    let run_id = Uuid::parse_str(&run_id).map_err(|_| StatusCode::BAD_REQUEST)?;
    let run = state
        .store
        .get_scanner_run(run_id)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ScannerRunResponse { run },
    }))
}

async fn get_rollout_status(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<RolloutStatusResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    sync_rollout_policies(&state).map_err(internal_error)?;
    let stage = state
        .store
        .get_current_rollout_stage(domain)
        .map_err(internal_error)?;
    let policy = state
        .store
        .load_effective_rollout_policy(domain)
        .map_err(internal_error)?;
    let latest_evaluation = state
        .store
        .latest_rollout_evaluation(domain)
        .map_err(internal_error)?;
    let promotion_candidate = state
        .store
        .get_active_promotion_candidate(domain)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RolloutStatusResponse {
            stage,
            policy,
            latest_evaluation,
            promotion_candidate,
        },
    }))
}

async fn get_rollout_policy(
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<RolloutPolicyResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    sync_rollout_policies(&state).map_err(internal_error)?;
    let stage = state
        .store
        .get_current_rollout_stage(domain)
        .map_err(internal_error)?
        .map(|item| item.stage);
    let policy = state
        .store
        .load_effective_rollout_policy(domain)
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RolloutPolicyResponse { stage, policy },
    }))
}

async fn list_rollout_evaluations(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<ApiEnvelope<RolloutEvaluationsResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let evaluations = state
        .store
        .list_rollout_evaluations(domain, query.limit.unwrap_or(50).min(500))
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RolloutEvaluationsResponse { evaluations },
    }))
}

async fn list_rollout_incidents(
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Query(query): Query<LimitQuery>,
) -> Result<Json<ApiEnvelope<RolloutIncidentsResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let incidents = state
        .store
        .list_rollout_incidents(domain, true, query.limit.unwrap_or(50).min(500))
        .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RolloutIncidentsResponse { incidents },
    }))
}

async fn trigger_rollout_evaluation(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
) -> Result<Json<ApiEnvelope<RolloutEvaluationResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let evaluation = evaluate_rollout_for_domain(&state, domain).map_err(internal_error)?;
    append_ops_audit(
        &state.store,
        domain,
        &operator,
        "rollout.evaluate",
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "evaluation_id": evaluation.evaluation_id,
            "current_stage": evaluation.current_stage,
            "target_stage": evaluation.target_stage,
            "eligible": evaluation.eligible,
        }),
    )
    .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: RolloutEvaluationResponse {
            evaluation: Some(evaluation),
        },
    }))
}

async fn promote_rollout_stage(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Json(payload): Json<RolloutPromoteRequest>,
) -> Result<Json<ApiEnvelope<RolloutStatusResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let current = current_rollout_stage(&state, domain).map_err(internal_error)?;
    let candidate = state
        .store
        .get_active_promotion_candidate(domain)
        .map_err(internal_error)?
        .ok_or(StatusCode::CONFLICT)?;
    if candidate.current_stage != current.stage || current.stage.next() != Some(candidate.target_stage)
    {
        return Err(StatusCode::CONFLICT);
    }
    state
        .store
        .set_rollout_stage(
            domain,
            candidate.target_stage,
            Some(operator.name.as_ref()),
            Some(now()),
            payload.reason.clone(),
        )
        .map_err(internal_error)?;
    state
        .store
        .invalidate_promotion_candidates(domain, now())
        .map_err(internal_error)?;
    append_ops_audit(
        &state.store,
        domain,
        &operator,
        "rollout.promote",
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "from_stage": current.stage,
            "to_stage": candidate.target_stage,
            "reason": payload.reason,
            "candidate_id": candidate.candidate_id,
        }),
    )
    .map_err(internal_error)?;
    get_rollout_status(State(state), Path(domain.to_string())).await
}

async fn rollback_rollout_stage(
    Extension(operator): Extension<OperatorIdentity>,
    State(state): State<AppState>,
    Path(domain): Path<String>,
    Json(payload): Json<RolloutRollbackRequest>,
) -> Result<Json<ApiEnvelope<RolloutStatusResponse>>, StatusCode> {
    let domain = ensure_target_domain(&state, &domain)?;
    let current = current_rollout_stage(&state, domain).map_err(internal_error)?;
    if payload.target_stage == current.stage {
        return Err(StatusCode::CONFLICT);
    }
    let mut cursor = current.stage;
    let mut allowed = false;
    while let Some(previous) = cursor.previous() {
        if previous == payload.target_stage {
            allowed = true;
            break;
        }
        cursor = previous;
    }
    if !allowed {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .store
        .set_rollout_stage(
            domain,
            payload.target_stage,
            Some(operator.name.as_ref()),
            Some(now()),
            payload.reason.clone(),
        )
        .map_err(internal_error)?;
    state
        .store
        .invalidate_promotion_candidates(domain, now())
        .map_err(internal_error)?;
    state
        .store
        .record_rollout_incident(RolloutIncident {
            incident_id: Uuid::new_v4(),
            domain,
            stage: current.stage,
            severity: RolloutIncidentSeverity::Warning,
            code: "MANUAL_ROLLBACK".to_owned(),
            summary: "rollout stage manually rolled back".to_owned(),
            detail: payload.reason.clone(),
            auto_rollback_stage: Some(payload.target_stage),
            created_at: now(),
            resolved_at: Some(now()),
        })
        .map_err(internal_error)?;
    let _ = enqueue_execution_command(
        &state.store,
        domain,
        ExecutionCommandKind::CancelAll,
        operator.name.as_ref(),
        &payload.reason,
    );
    append_ops_audit(
        &state.store,
        domain,
        &operator,
        "rollout.rollback",
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "from_stage": current.stage,
            "to_stage": payload.target_stage,
            "reason": payload.reason,
        }),
    )
    .map_err(internal_error)?;
    get_rollout_status(State(state), Path(domain.to_string())).await
}

fn enqueue_execution_action(
    state: &AppState,
    domain: AccountDomain,
    operator: &OperatorIdentity,
    kind: ExecutionCommandKind,
    audit_action: &str,
    reason: &str,
) -> Result<Json<ApiEnvelope<ExecutionActionResponse>>, StatusCode> {
    let command_id =
        enqueue_execution_command(&state.store, domain, kind, operator.name.as_ref(), reason)
            .map_err(internal_error)?;
    append_ops_audit(
        &state.store,
        domain,
        operator,
        audit_action,
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "command_id": command_id,
            "kind": format!("{kind:?}"),
            "reason": reason,
        }),
    )
    .map_err(internal_error)?;
    Ok(Json(ApiEnvelope {
        data: ExecutionActionResponse {
            accepted: true,
            detail: format!("command persisted: {command_id}"),
        },
    }))
}

fn parse_domain(domain: &str) -> Result<AccountDomain, StatusCode> {
    domain
        .parse::<AccountDomain>()
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn ensure_target_domain(state: &AppState, domain: &str) -> Result<AccountDomain, StatusCode> {
    let domain = parse_domain(domain)?;
    if domain != state.target_domain {
        return Err(StatusCode::CONFLICT);
    }
    Ok(domain)
}

fn parse_alert_status(value: &str) -> Result<AlertStatus, StatusCode> {
    value
        .parse::<AlertStatus>()
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn parse_alert_severity(value: &str) -> Result<AlertSeverity, StatusCode> {
    value
        .parse::<AlertSeverity>()
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn parse_strategy(strategy: &str) -> Result<StrategyKind, StatusCode> {
    strategy
        .parse::<StrategyKind>()
        .map_err(|_| StatusCode::BAD_REQUEST)
}

fn internal_error(error: impl std::fmt::Display) -> StatusCode {
    let _ = error;
    StatusCode::INTERNAL_SERVER_ERROR
}

fn render_metrics(state: &AppState) -> Result<String> {
    let metrics = state
        .store
        .latest_metric_samples(state.target_domain, 128)?
        .into_iter()
        .fold(BTreeMap::<String, MetricSample>::new(), |mut acc, sample| {
            acc.entry(sample.metric_key.clone()).or_insert(sample);
            acc
        });
    let open_alerts = state
        .store
        .list_alerts(state.target_domain, Some(AlertStatus::Open), None, 500)?
        .len();
    let mut lines = vec![
        "# HELP polymarket_alerts_open Number of open alerts".to_owned(),
        "# TYPE polymarket_alerts_open gauge".to_owned(),
        format!(
            "polymarket_alerts_open{{domain=\"{}\"}} {}",
            state.target_domain,
            open_alerts
        ),
    ];
    for (key, sample) in metrics {
        lines.push(format!("# TYPE polymarket_{key} gauge"));
        lines.push(format!(
            "polymarket_{key}{{domain=\"{}\"}} {}",
            sample.domain, sample.value
        ));
    }
    Ok(lines.join("\n") + "\n")
}

fn enqueue_execution_command(
    store: &Store,
    domain: AccountDomain,
    kind: ExecutionCommandKind,
    requested_by: &str,
    reason: &str,
) -> Result<Uuid, anyhow::Error> {
    let command = ExecutionCommand {
        command_id: Uuid::new_v4(),
        domain,
        kind,
        intent_id: None,
        order_id: None,
        market_id: None,
        requested_by: requested_by.to_owned(),
        requested_at: now(),
        reason: Some(reason.to_owned()),
    };
    let command_id = command.command_id;
    let event_type = format!("EXECUTION_COMMAND_{:?}", command.kind).to_ascii_uppercase();
    store.append_event(NewDurableEvent {
        domain,
        stream: "execution.commands".to_owned(),
        aggregate_type: "execution_command".to_owned(),
        aggregate_id: command_id.to_string(),
        event_type,
        causation_id: None,
        correlation_id: Some(command_id),
        idempotency_key: Some(command_id.to_string()),
        payload: serde_json::to_value(command)?,
        metadata: serde_json::json!({ "source": "ops-api" }),
        created_at: now(),
    })?;
    Ok(command_id)
}

fn append_ops_audit(
    store: &Store,
    domain: AccountDomain,
    operator: &OperatorIdentity,
    action: &str,
    detail: serde_json::Value,
) -> Result<()> {
    store.append_audit(
        Some(domain),
        "ops-api",
        action,
        serde_json::json!({
            "source": "ops-api",
            "operator": operator.name.as_ref(),
            "detail": detail,
        })
        .to_string(),
    )?;
    Ok(())
}

fn constant_time_eq(left: &str, right: &str) -> bool {
    let left_bytes = left.as_bytes();
    let right_bytes = right.as_bytes();
    let max_len = left_bytes.len().max(right_bytes.len());
    let mut diff = left_bytes.len() ^ right_bytes.len();
    for index in 0..max_len {
        let left_byte = *left_bytes.get(index).unwrap_or(&0);
        let right_byte = *right_bytes.get(index).unwrap_or(&0);
        diff |= usize::from(left_byte ^ right_byte);
    }
    diff == 0
}

fn clamp_limit(limit: usize, default_limit: usize) -> usize {
    match limit {
        0 => default_limit,
        value => value.min(10_000),
    }
}

impl OperatorAuthenticator {
    fn from_config(config: &OpsApiConfig) -> Self {
        let operators = config
            .auth_tokens
            .iter()
            .map(|(name, token)| {
                (
                    Arc::<str>::from(name.as_str()),
                    Arc::<str>::from(token.as_str()),
                )
            })
            .collect();
        Self {
            operators,
            legacy_token: config.auth_token.as_deref().map(Arc::<str>::from),
        }
    }

    fn from_parts(auth_tokens: BTreeMap<String, String>, legacy_token: Option<String>) -> Self {
        Self {
            operators: auth_tokens
                .into_iter()
                .map(|(name, token)| (Arc::<str>::from(name), Arc::<str>::from(token)))
                .collect(),
            legacy_token: legacy_token.map(Arc::<str>::from),
        }
    }

    fn authenticate(&self, headers: &HeaderMap) -> Option<OperatorIdentity> {
        let actual = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))?;
        if !self.operators.is_empty() {
            return self
                .operators
                .iter()
                .find(|(_, token)| constant_time_eq(token, actual))
                .map(|(name, _)| OperatorIdentity { name: name.clone() });
        }
        self.legacy_token
            .as_ref()
            .filter(|token| constant_time_eq(token, actual))
            .map(|_| OperatorIdentity {
                name: Arc::<str>::from("legacy-operator"),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::body::to_bytes;
    use axum::http::Request;
    use polymarket_core::RuntimeMode;
    use tempfile::tempdir;
    use tower::ServiceExt;

    fn test_state() -> AppState {
        let dir = tempdir().expect("tempdir");
        let root = dir.into_path();
        let store = Store::new(root.join("ops.sqlite"), AccountDomain::Sim, "SIM", "SIM");
        store.init().expect("store init");
        let mut auth = BTreeMap::new();
        auth.insert("alice".to_owned(), "token-a".to_owned());
        auth.insert("bob".to_owned(), "token-b".to_owned());
        AppState {
            store,
            authenticator: Arc::new(OperatorAuthenticator::from_parts(auth, None)),
            target_domain: AccountDomain::Sim,
            replay_worker_ready: Arc::new(AtomicBool::new(true)),
            monitoring_ready: Arc::new(AtomicBool::new(true)),
            monitoring: MonitoringConfig {
                poll_interval: Duration::from_millis(10),
                alert_debounce: Duration::from_millis(10),
                heartbeat_degraded_ms: 12_000,
                heartbeat_safe_ms: 30_000,
                market_ws_lag_degraded_ms: 2_500,
                market_ws_lag_safe_ms: 10_000,
                reject_rate_degraded_bps: 1_000,
                reject_rate_safe_bps: 2_500,
                disputed_capital_safe_bps: 2_000,
            },
            rollout: RolloutConfig::from_map(&BTreeMap::new()).expect("rollout config"),
            alert_webhook: None,
            prometheus_enabled: true,
            webhook_timeout: Duration::from_secs(1),
            webhook_retries: 1,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()
                .expect("http client"),
        }
    }

    async fn request(app: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = app.oneshot(request).await.expect("response");
        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let json = if body.is_empty() {
            serde_json::json!(null)
        } else {
            serde_json::from_slice(&body).expect("json body")
        };
        (status, json)
    }

    #[tokio::test]
    async fn livez_is_open() {
        let app = build_app(test_state());
        let (status, _) = request(
            app,
            Request::builder()
                .uri("/livez")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn v1_requires_auth() {
        let app = build_app(test_state());
        let (status, _) = request(
            app,
            Request::builder()
                .uri("/v1/runtime-modes")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn set_runtime_mode_records_audit() {
        let state = test_state();
        let store = state.store.clone();
        let app = build_app(state);
        let (status, _) = request(
            app,
            Request::builder()
                .method("PUT")
                .uri("/v1/runtime-modes/SIM")
                .header(header::AUTHORIZATION, "Bearer token-a")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "mode": RuntimeMode::Disabled,
                        "reason": "manual"
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let mode = store
            .runtime_mode(AccountDomain::Sim)
            .expect("mode")
            .expect("runtime mode");
        assert_eq!(mode.mode, RuntimeMode::Disabled);
        let audit = store.recent_audit_events(10).expect("audit");
        assert!(audit
            .iter()
            .any(|event| event.action == "runtime-mode.updated"));
    }

    #[tokio::test]
    async fn execution_actions_are_audited() {
        let state = test_state();
        let store = state.store.clone();
        let app = build_app(state);
        for path in [
            "/v1/execution/SIM/cancel-all",
            "/v1/execution/SIM/reconcile",
            "/v1/execution/SIM/recover",
        ] {
            let (status, _) = request(
                app.clone(),
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header(header::AUTHORIZATION, "Bearer token-b")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }
        let audit = store.recent_audit_events(20).expect("audit");
        assert!(audit
            .iter()
            .any(|event| event.action == "execution.cancel-all.requested"));
        assert!(audit
            .iter()
            .any(|event| event.action == "execution.reconcile.requested"));
        assert!(audit
            .iter()
            .any(|event| event.action == "execution.recover.requested"));
    }

    #[tokio::test]
    async fn foreign_domain_requests_are_rejected() {
        let app = build_app(test_state());
        let (status, _) = request(
            app,
            Request::builder()
                .uri("/v1/health/CANARY")
                .header(header::AUTHORIZATION, "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn replay_job_can_be_triggered_and_completed() {
        let state = test_state();
        let store = state.store.clone();
        spawn_replay_worker(state.clone());
        let app = build_app(state);

        let (status, body) = request(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri("/v1/rules/replay/runs")
                .header(header::AUTHORIZATION, "Bearer token-a")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "domain": "SIM",
                        "after_sequence": null,
                        "limit": 25
                    })
                    .to_string(),
                ))
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let job_id = body["data"]["job_id"].as_str().expect("job id").to_owned();

        let mut completed = false;
        for _ in 0..40 {
            let job = store
                .get_replay_job(Uuid::parse_str(&job_id).expect("uuid"))
                .expect("get replay job")
                .expect("job");
            if job.run_id.is_some() {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(completed);

        let (status, body) = request(
            app,
            Request::builder()
                .uri(format!("/v1/rules/replay/jobs/{job_id}"))
                .header(header::AUTHORIZATION, "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"]["job"]["status"], "COMPLETED");
    }

    #[tokio::test]
    async fn rollout_evaluation_endpoint_creates_candidate() {
        let state = test_state();
        state
            .store
            .upsert_snapshot(NewStateSnapshot {
                domain: AccountDomain::Sim,
                aggregate_type: "execution_heartbeat".to_owned(),
                aggregate_id: "latest".to_owned(),
                version: 1,
                payload: serde_json::to_value(ExecutionHeartbeat {
                    domain: AccountDomain::Sim,
                    service: polymarket_core::ServiceKind::ExecutionEngine,
                    venue_healthy: true,
                    signer_healthy: true,
                    event_loop_healthy: true,
                    consecutive_failures: 0,
                    detail: "ok".to_owned(),
                    observed_at: now(),
                })
                .expect("heartbeat"),
                derived_from_sequence: None,
                created_at: now(),
            })
            .expect("snapshot");
        let app = build_app(state.clone());
        let (status, body) = request(
            app,
            Request::builder()
                .method("POST")
                .uri("/v1/rollout/SIM/evaluate")
                .header(header::AUTHORIZATION, "Bearer token-a")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["data"]["evaluation"]["evaluation_id"].is_string());
        let evaluation = state
            .store
            .latest_rollout_evaluation(AccountDomain::Sim)
            .expect("evaluation");
        assert!(evaluation.is_some());
    }

    #[tokio::test]
    async fn rollout_promote_requires_candidate() {
        let state = test_state();
        let app = build_app(state);
        let (status, _) = request(
            app,
            Request::builder()
                .method("POST")
                .uri("/v1/rollout/SIM/promote")
                .header(header::AUTHORIZATION, "Bearer token-a")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::json!({ "reason": "advance" }).to_string(),
                ))
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }
}
