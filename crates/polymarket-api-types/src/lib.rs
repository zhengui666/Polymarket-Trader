use polymarket_core::{
    AccountDomain, AlertEvent, AlertSeverity, AlertStatus, AuditEvent, ConstraintEdge,
    ConstraintGraphSnapshot, EventFamilySnapshot, ExecutionHeartbeat, ExecutionIntentRecord,
    ExecutionReconcileReport, HealthSnapshot, MarketCanonical, MetricSample,
    OpportunityCandidate, OpportunityInvalidation, OrderLifecycleRecord, PromotionCandidate,
    PromotionStage, ReplayJob, ReplayReport, ReplayTrace, RolloutEvaluation, RolloutIncident,
    RolloutPolicy, RolloutStageRecord, RuleVersion, RuntimeMode, RuntimeModeRecord,
    ScannerRunReport, ServiceHeartbeat,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiEnvelope<T> {
    pub data: T,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeModeUpdateRequest {
    pub mode: RuntimeMode,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeModesResponse {
    pub runtime_modes: Vec<RuntimeModeRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicesResponse {
    pub services: Vec<ServiceHeartbeat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEventsResponse {
    pub events: Vec<AuditEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub snapshot: HealthSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertsResponse {
    pub alerts: Vec<AlertEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertAckResponse {
    pub alert: Option<AlertEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub metrics: Vec<MetricSample>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertQueryResponse {
    pub status: Option<AlertStatus>,
    pub severity: Option<AlertSeverity>,
    pub alerts: Vec<AlertEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulesMarketResponse {
    pub market: Option<MarketCanonical>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleVersionsResponse {
    pub versions: Vec<RuleVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventFamilyResponse {
    pub snapshot: Option<EventFamilySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintGraphResponse {
    pub snapshot: Option<ConstraintGraphSnapshot>,
    pub edges: Vec<ConstraintEdge>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRunsResponse {
    pub runs: Vec<ReplayReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRunResponse {
    pub run: Option<ReplayReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayTraceResponse {
    pub trace: ReplayTrace,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayTriggerRequest {
    pub domain: AccountDomain,
    pub after_sequence: Option<i64>,
    pub limit: usize,
    pub reason: Option<String>,
    pub alert_id: Option<String>,
    pub audit_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayTriggerResponse {
    pub accepted: bool,
    pub job_id: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayJobStatusResponse {
    pub job: Option<ReplayJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityResponse {
    pub opportunity: Option<OpportunityCandidate>,
    pub invalidations: Vec<OpportunityInvalidation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunitiesResponse {
    pub opportunities: Vec<OpportunityCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerRunsResponse {
    pub runs: Vec<ScannerRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerRunResponse {
    pub run: Option<ScannerRunReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionHealthResponse {
    pub heartbeat: Option<ExecutionHeartbeat>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionIntentsResponse {
    pub intents: Vec<ExecutionIntentRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionOrdersResponse {
    pub orders: Vec<OrderLifecycleRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReconcileResponse {
    pub report: ExecutionReconcileReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionActionResponse {
    pub accepted: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutStatusResponse {
    pub stage: Option<RolloutStageRecord>,
    pub policy: Option<RolloutPolicy>,
    pub latest_evaluation: Option<RolloutEvaluation>,
    pub promotion_candidate: Option<PromotionCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutPolicyResponse {
    pub stage: Option<PromotionStage>,
    pub policy: Option<RolloutPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutEvaluationResponse {
    pub evaluation: Option<RolloutEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutEvaluationsResponse {
    pub evaluations: Vec<RolloutEvaluation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromotionCandidateResponse {
    pub candidate: Option<PromotionCandidate>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutPromoteRequest {
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutRollbackRequest {
    pub target_stage: PromotionStage,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RolloutIncidentsResponse {
    pub incidents: Vec<RolloutIncident>,
}
