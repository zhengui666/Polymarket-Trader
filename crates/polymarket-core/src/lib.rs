use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub type Timestamp = DateTime<Utc>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AccountDomain {
    Sim,
    Canary,
    Live,
}

impl AccountDomain {
    pub const ALL: [Self; 3] = [Self::Sim, Self::Canary, Self::Live];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sim => "SIM",
            Self::Canary => "CANARY",
            Self::Live => "LIVE",
        }
    }

    pub const fn namespace(self) -> &'static str {
        match self {
            Self::Sim => "sim",
            Self::Canary => "canary",
            Self::Live => "live",
        }
    }

    pub const fn audit_label(self) -> &'static str {
        match self {
            Self::Sim => "domain.sim",
            Self::Canary => "domain.canary",
            Self::Live => "domain.live",
        }
    }

    pub const fn environment(self) -> DomainEnvironment {
        match self {
            Self::Sim => DomainEnvironment::Simulation,
            Self::Canary => DomainEnvironment::Canary,
            Self::Live => DomainEnvironment::Production,
        }
    }

    pub const fn allows_real_capital(self) -> bool {
        !matches!(self, Self::Sim)
    }

    pub const fn allows_external_trading_credentials(self) -> bool {
        !matches!(self, Self::Sim)
    }

    pub const fn default_runtime_mode(self) -> RuntimeMode {
        match self {
            Self::Sim => RuntimeMode::Simulate,
            Self::Canary => RuntimeMode::Observe,
            Self::Live => RuntimeMode::Disabled,
        }
    }

    pub const fn allowed_runtime_modes(self) -> &'static [RuntimeMode] {
        match self {
            Self::Sim => &[
                RuntimeMode::Disabled,
                RuntimeMode::Observe,
                RuntimeMode::Simulate,
            ],
            Self::Canary | Self::Live => &[
                RuntimeMode::Disabled,
                RuntimeMode::Observe,
                RuntimeMode::Simulate,
                RuntimeMode::Execute,
            ],
        }
    }

    pub fn descriptor(self) -> AccountDomainDescriptor {
        AccountDomainDescriptor {
            domain: self,
            environment: self.environment(),
            allows_real_capital: self.allows_real_capital(),
            default_runtime_mode: self.default_runtime_mode(),
            allowed_runtime_modes: self.allowed_runtime_modes().to_vec(),
            namespace: self.namespace().to_owned(),
            audit_label: self.audit_label().to_owned(),
            allows_external_trading_credentials: self.allows_external_trading_credentials(),
        }
    }
}

impl Display for AccountDomain {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AccountDomain {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "SIM" => Ok(Self::Sim),
            "CANARY" => Ok(Self::Canary),
            "LIVE" => Ok(Self::Live),
            _ => Err(ParseEnumError::new("AccountDomain", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DomainEnvironment {
    Simulation,
    Canary,
    Production,
}

impl DomainEnvironment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Simulation => "SIMULATION",
            Self::Canary => "CANARY",
            Self::Production => "PRODUCTION",
        }
    }
}

impl Display for DomainEnvironment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PromotionStage {
    Replay,
    Shadow,
    Canary,
    Live,
}

impl PromotionStage {
    pub const PATH: [Self; 4] = [Self::Replay, Self::Shadow, Self::Canary, Self::Live];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "REPLAY",
            Self::Shadow => "SHADOW",
            Self::Canary => "CANARY",
            Self::Live => "LIVE",
        }
    }
}

impl Display for PromotionStage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PromotionStage {
    pub const fn domain(self) -> AccountDomain {
        match self {
            Self::Replay | Self::Shadow => AccountDomain::Sim,
            Self::Canary => AccountDomain::Canary,
            Self::Live => AccountDomain::Live,
        }
    }

    pub fn next(self) -> Option<Self> {
        match self {
            Self::Replay => Some(Self::Shadow),
            Self::Shadow => Some(Self::Canary),
            Self::Canary => Some(Self::Live),
            Self::Live => None,
        }
    }

    pub fn previous(self) -> Option<Self> {
        match self {
            Self::Replay => None,
            Self::Shadow => Some(Self::Replay),
            Self::Canary => Some(Self::Shadow),
            Self::Live => Some(Self::Canary),
        }
    }

    pub fn is_reachable_from(self, current: Self) -> bool {
        current.next() == Some(self)
    }
}

impl FromStr for PromotionStage {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "REPLAY" => Ok(Self::Replay),
            "SHADOW" => Ok(Self::Shadow),
            "CANARY" => Ok(Self::Canary),
            "LIVE" => Ok(Self::Live),
            _ => Err(ParseEnumError::new("PromotionStage", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDomainDescriptor {
    pub domain: AccountDomain,
    pub environment: DomainEnvironment,
    pub allows_real_capital: bool,
    pub default_runtime_mode: RuntimeMode,
    pub allowed_runtime_modes: Vec<RuntimeMode>,
    pub namespace: String,
    pub audit_label: String,
    pub allows_external_trading_credentials: bool,
}

impl AccountDomainDescriptor {
    pub fn validate(&self) -> Result<(), DomainModelError> {
        if self.namespace.trim().is_empty() {
            return Err(DomainModelError::EmptyField {
                domain: self.domain,
                field: "namespace",
            });
        }
        if self.audit_label.trim().is_empty() {
            return Err(DomainModelError::EmptyField {
                domain: self.domain,
                field: "audit_label",
            });
        }
        if !self
            .allowed_runtime_modes
            .contains(&self.default_runtime_mode)
        {
            return Err(DomainModelError::DefaultModeNotAllowed {
                domain: self.domain,
                mode: self.default_runtime_mode,
            });
        }
        if self.domain == AccountDomain::Sim {
            if self.allows_real_capital {
                return Err(DomainModelError::SimCannotAllowRealCapital);
            }
            if self.allows_external_trading_credentials {
                return Err(DomainModelError::SimCannotAllowExternalCredentials);
            }
            if self.allowed_runtime_modes.contains(&RuntimeMode::Execute) {
                return Err(DomainModelError::SimCannotAllowExecute);
            }
        }
        Ok(())
    }

    pub fn allows_runtime_mode(&self, mode: RuntimeMode) -> bool {
        self.allowed_runtime_modes.contains(&mode)
    }
}

pub fn required_domain_descriptors() -> Vec<AccountDomainDescriptor> {
    AccountDomain::ALL
        .iter()
        .map(|domain| domain.descriptor())
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum DomainModelError {
    #[error("domain `{domain}` has an empty `{field}` field")]
    EmptyField {
        domain: AccountDomain,
        field: &'static str,
    },
    #[error("domain `{domain}` default runtime mode `{mode}` is not included in its allowed runtime modes")]
    DefaultModeNotAllowed {
        domain: AccountDomain,
        mode: RuntimeMode,
    },
    #[error("SIM cannot allow real capital")]
    SimCannotAllowRealCapital,
    #[error("SIM cannot allow external trading credentials")]
    SimCannotAllowExternalCredentials,
    #[error("SIM cannot allow EXECUTE runtime mode")]
    SimCannotAllowExecute,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeMode {
    Disabled,
    Observe,
    Simulate,
    Execute,
}

impl RuntimeMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "DISABLED",
            Self::Observe => "OBSERVE",
            Self::Simulate => "SIMULATE",
            Self::Execute => "EXECUTE",
        }
    }

    pub const fn requires_execution_approval(self) -> bool {
        matches!(self, Self::Execute)
    }
}

impl Display for RuntimeMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RuntimeMode {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "DISABLED" => Ok(Self::Disabled),
            "OBSERVE" => Ok(Self::Observe),
            "SIMULATE" => Ok(Self::Simulate),
            "EXECUTE" => Ok(Self::Execute),
            _ => Err(ParseEnumError::new("RuntimeMode", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ServiceKind {
    MdGateway,
    RulesEngine,
    OpportunityEngine,
    PortfolioEngine,
    RiskEngine,
    SimEngine,
    ExecutionEngine,
    SettlementEngine,
}

impl ServiceKind {
    pub const ALL: [Self; 8] = [
        Self::MdGateway,
        Self::RulesEngine,
        Self::OpportunityEngine,
        Self::PortfolioEngine,
        Self::RiskEngine,
        Self::SimEngine,
        Self::ExecutionEngine,
        Self::SettlementEngine,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MdGateway => "MD_GATEWAY",
            Self::RulesEngine => "RULES_ENGINE",
            Self::OpportunityEngine => "OPPORTUNITY_ENGINE",
            Self::PortfolioEngine => "PORTFOLIO_ENGINE",
            Self::RiskEngine => "RISK_ENGINE",
            Self::SimEngine => "SIM_ENGINE",
            Self::ExecutionEngine => "EXECUTION_ENGINE",
            Self::SettlementEngine => "SETTLEMENT_ENGINE",
        }
    }
}

impl Display for ServiceKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ServiceKind {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "MD_GATEWAY" => Ok(Self::MdGateway),
            "RULES_ENGINE" => Ok(Self::RulesEngine),
            "OPPORTUNITY_ENGINE" => Ok(Self::OpportunityEngine),
            "PORTFOLIO_ENGINE" => Ok(Self::PortfolioEngine),
            "RISK_ENGINE" => Ok(Self::RiskEngine),
            "SIM_ENGINE" => Ok(Self::SimEngine),
            "EXECUTION_ENGINE" => Ok(Self::ExecutionEngine),
            "SETTLEMENT_ENGINE" => Ok(Self::SettlementEngine),
            _ => Err(ParseEnumError::new("ServiceKind", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceDefinition {
    pub kind: ServiceKind,
    pub enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeModeRecord {
    pub domain: AccountDomain,
    pub mode: RuntimeMode,
    pub reason: String,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceHeartbeat {
    pub service: ServiceKind,
    pub domain: AccountDomain,
    pub healthy: bool,
    pub detail: String,
    pub last_seen_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub id: Uuid,
    pub occurred_at: Timestamp,
    pub service: String,
    pub domain: Option<AccountDomain>,
    pub action: String,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

impl AlertSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warning => "WARNING",
            Self::Critical => "CRITICAL",
        }
    }
}

impl Display for AlertSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AlertSeverity {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "INFO" => Ok(Self::Info),
            "WARNING" => Ok(Self::Warning),
            "CRITICAL" => Ok(Self::Critical),
            _ => Err(ParseEnumError::new("AlertSeverity", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlertStatus {
    Open,
    Acknowledged,
    Resolved,
}

impl AlertStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "OPEN",
            Self::Acknowledged => "ACKNOWLEDGED",
            Self::Resolved => "RESOLVED",
        }
    }
}

impl Display for AlertStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AlertStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "OPEN" => Ok(Self::Open),
            "ACKNOWLEDGED" => Ok(Self::Acknowledged),
            "RESOLVED" => Ok(Self::Resolved),
            _ => Err(ParseEnumError::new("AlertStatus", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AlertRuleKind {
    HeartbeatStale,
    RuntimeModeSwitch,
    UserWsDisconnected,
    MarketWsLag,
    CapitalBuffer,
    RejectRate,
    DisputedCapital,
    ReconcileDrift,
    CancelAllTriggered,
    ReplayFailure,
}

impl AlertRuleKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HeartbeatStale => "HEARTBEAT_STALE",
            Self::RuntimeModeSwitch => "RUNTIME_MODE_SWITCH",
            Self::UserWsDisconnected => "USER_WS_DISCONNECTED",
            Self::MarketWsLag => "MARKET_WS_LAG",
            Self::CapitalBuffer => "CAPITAL_BUFFER",
            Self::RejectRate => "REJECT_RATE",
            Self::DisputedCapital => "DISPUTED_CAPITAL",
            Self::ReconcileDrift => "RECONCILE_DRIFT",
            Self::CancelAllTriggered => "CANCEL_ALL_TRIGGERED",
            Self::ReplayFailure => "REPLAY_FAILURE",
        }
    }
}

impl Display for AlertRuleKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for AlertRuleKind {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "HEARTBEAT_STALE" => Ok(Self::HeartbeatStale),
            "RUNTIME_MODE_SWITCH" => Ok(Self::RuntimeModeSwitch),
            "USER_WS_DISCONNECTED" => Ok(Self::UserWsDisconnected),
            "MARKET_WS_LAG" => Ok(Self::MarketWsLag),
            "CAPITAL_BUFFER" => Ok(Self::CapitalBuffer),
            "REJECT_RATE" => Ok(Self::RejectRate),
            "DISPUTED_CAPITAL" => Ok(Self::DisputedCapital),
            "RECONCILE_DRIFT" => Ok(Self::ReconcileDrift),
            "CANCEL_ALL_TRIGGERED" => Ok(Self::CancelAllTriggered),
            "REPLAY_FAILURE" => Ok(Self::ReplayFailure),
            _ => Err(ParseEnumError::new("AlertRuleKind", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertEvent {
    pub alert_id: Uuid,
    pub domain: AccountDomain,
    pub rule_kind: AlertRuleKind,
    pub severity: AlertSeverity,
    pub status: AlertStatus,
    pub dedupe_key: String,
    pub source: String,
    pub summary: String,
    pub detail: String,
    pub metric_key: Option<String>,
    pub metric_value: Option<f64>,
    pub threshold: Option<f64>,
    pub trigger_value: Option<f64>,
    pub labels: BTreeMap<String, String>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub acknowledged_at: Option<Timestamp>,
    pub acknowledged_by: Option<String>,
    pub resolved_at: Option<Timestamp>,
    pub audit_event_id: Option<Uuid>,
    pub replay_job_id: Option<Uuid>,
    pub replay_run_id: Option<Uuid>,
    pub last_sent_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    pub sample_id: Uuid,
    pub domain: AccountDomain,
    pub metric_key: String,
    pub metric_kind: String,
    pub value: f64,
    pub labels: BTreeMap<String, String>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthMetricsSummary {
    pub heartbeat_age_ms: u64,
    pub market_ws_lag_ms: u64,
    pub user_ws_ok: bool,
    pub capital_buffer_ok: bool,
    pub fill_rate_5m: f64,
    pub open_orders: usize,
    pub reconcile_lag_ms: u64,
    pub disputed_capital_ratio: f64,
    pub last_alert_at: Option<Timestamp>,
    pub degradation_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub generated_at: Timestamp,
    pub runtime_modes: Vec<RuntimeModeRecord>,
    pub services: Vec<ServiceHeartbeat>,
    pub audit_events: Vec<AuditEvent>,
    pub recent_alerts: Vec<AlertEvent>,
    pub runtime_health: Option<RuntimeHealth>,
    pub metrics_summary: Option<HealthMetricsSummary>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartupManifest {
    pub services: Vec<ServiceDefinition>,
}

impl Default for StartupManifest {
    fn default() -> Self {
        Self {
            services: ServiceKind::ALL
                .iter()
                .copied()
                .map(|kind| ServiceDefinition {
                    kind,
                    enabled: true,
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("failed to parse {kind} from `{value}`")]
pub struct ParseEnumError {
    kind: &'static str,
    value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotencyStatus {
    InProgress,
    Completed,
    Failed,
}

impl IdempotencyStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl Display for IdempotencyStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for IdempotencyStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "IN_PROGRESS" => Ok(Self::InProgress),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            _ => Err(ParseEnumError::new("IdempotencyStatus", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderLifecycleStatus {
    Created,
    Submitted,
    Acknowledged,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    Rejected,
    Expired,
    Settled,
}

impl OrderLifecycleStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "CREATED",
            Self::Submitted => "SUBMITTED",
            Self::Acknowledged => "ACKNOWLEDGED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::CancelRequested => "CANCEL_REQUESTED",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
            Self::Expired => "EXPIRED",
            Self::Settled => "SETTLED",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled | Self::Cancelled | Self::Rejected | Self::Expired | Self::Settled
        )
    }
}

impl Display for OrderLifecycleStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for OrderLifecycleStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "CREATED" => Ok(Self::Created),
            "SUBMITTED" => Ok(Self::Submitted),
            "ACKNOWLEDGED" => Ok(Self::Acknowledged),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCEL_REQUESTED" => Ok(Self::CancelRequested),
            "CANCELLED" => Ok(Self::Cancelled),
            "REJECTED" => Ok(Self::Rejected),
            "EXPIRED" => Ok(Self::Expired),
            "SETTLED" => Ok(Self::Settled),
            _ => Err(ParseEnumError::new("OrderLifecycleStatus", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewDurableEvent {
    pub domain: AccountDomain,
    pub stream: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketDataChannel {
    Market,
    User,
    Sports,
    Rtds,
}

impl MarketDataChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Market => "market",
            Self::User => "user",
            Self::Sports => "sports",
            Self::Rtds => "rtds",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketQuoteLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketTrade {
    pub price: f64,
    pub size: f64,
    pub side: Option<String>,
    pub trade_id: Option<String>,
    pub occurred_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketBook {
    pub best_bid: Option<MarketQuoteLevel>,
    pub best_ask: Option<MarketQuoteLevel>,
    pub last_trade: Option<MarketTrade>,
    pub mid_price: Option<f64>,
    pub spread_bps: Option<f64>,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub market_id: String,
    pub channel: MarketDataChannel,
    pub status: Option<String>,
    pub book: Option<MarketBook>,
    pub sequence: i64,
    pub source_event_id: Option<String>,
    pub observed_at: Timestamp,
    pub received_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdGatewayCursor {
    pub source_path: String,
    pub line_number: u64,
    pub checksum: Option<u64>,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DurableEvent {
    pub sequence: i64,
    pub event_id: Uuid,
    pub domain: AccountDomain,
    pub stream: String,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub event_type: String,
    pub causation_id: Option<Uuid>,
    pub correlation_id: Option<Uuid>,
    pub idempotency_key: Option<String>,
    pub payload: serde_json::Value,
    pub metadata: serde_json::Value,
    pub created_at: Timestamp,
    pub persisted_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayCursor {
    pub after_sequence: Option<i64>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewStateSnapshot {
    pub domain: AccountDomain,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: i64,
    pub payload: serde_json::Value,
    pub derived_from_sequence: Option<i64>,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub snapshot_id: Uuid,
    pub domain: AccountDomain,
    pub aggregate_type: String,
    pub aggregate_id: String,
    pub version: i64,
    pub payload: serde_json::Value,
    pub derived_from_sequence: Option<i64>,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewOrderLifecycleRecord {
    pub domain: AccountDomain,
    pub order_id: String,
    pub market_id: String,
    pub status: OrderLifecycleStatus,
    pub client_order_id: Option<String>,
    pub external_order_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub side: Option<String>,
    pub limit_price: Option<f64>,
    pub order_quantity: Option<f64>,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub last_event_sequence: Option<i64>,
    pub detail: serde_json::Value,
    pub opened_at: Timestamp,
    pub updated_at: Timestamp,
    pub closed_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderLifecycleRecord {
    pub domain: AccountDomain,
    pub order_id: String,
    pub market_id: String,
    pub status: OrderLifecycleStatus,
    pub client_order_id: Option<String>,
    pub external_order_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub side: Option<String>,
    pub limit_price: Option<f64>,
    pub order_quantity: Option<f64>,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub last_event_sequence: Option<i64>,
    pub detail: serde_json::Value,
    pub opened_at: Timestamp,
    pub updated_at: Timestamp,
    pub closed_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewIdempotencyKey {
    pub domain: AccountDomain,
    pub scope: String,
    pub key: String,
    pub request_hash: String,
    pub created_by: String,
    pub created_at: Timestamp,
    pub lock_expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyKeyRecord {
    pub domain: AccountDomain,
    pub scope: String,
    pub key: String,
    pub request_hash: String,
    pub status: IdempotencyStatus,
    pub response_payload: Option<serde_json::Value>,
    pub created_by: String,
    pub created_at: Timestamp,
    pub last_seen_at: Timestamp,
    pub lock_expires_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IdempotencyClaimResult {
    Claimed(IdempotencyKeyRecord),
    DuplicateCompleted(IdempotencyKeyRecord),
    DuplicateInFlight(IdempotencyKeyRecord),
    HashMismatch(IdempotencyKeyRecord),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecoveryState {
    pub domain: AccountDomain,
    pub generated_at: Timestamp,
    pub runtime_mode: Option<RuntimeModeRecord>,
    pub services: Vec<ServiceHeartbeat>,
    pub recent_audit_events: Vec<AuditEvent>,
    pub recent_events: Vec<DurableEvent>,
    pub snapshots: Vec<StateSnapshot>,
    pub open_orders: Vec<OrderLifecycleRecord>,
}

impl ParseEnumError {
    pub fn new(kind: &'static str, value: impl Into<String>) -> Self {
        Self {
            kind,
            value: value.into(),
        }
    }
}

pub fn now() -> Timestamp {
    Utc::now()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimMode {
    Replay,
    Shadow,
    PaperLiveQueue,
}

impl SimMode {
    pub const ALL: [Self; 3] = [Self::Replay, Self::Shadow, Self::PaperLiveQueue];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Replay => "REPLAY",
            Self::Shadow => "SHADOW",
            Self::PaperLiveQueue => "PAPER_LIVE_QUEUE",
        }
    }
}

impl Display for SimMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SimMode {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "REPLAY" => Ok(Self::Replay),
            "SHADOW" => Ok(Self::Shadow),
            "PAPER_LIVE_QUEUE" | "PAPERLIVEQUEUE" | "PAPER_LIVE" | "PAPER" => {
                Ok(Self::PaperLiveQueue)
            }
            other => Err(ParseEnumError {
                kind: "SimMode",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimEventKind {
    MarketSnapshot,
    RuleVersion,
    Intent,
    BaselineOrder,
    BaselineFill,
    Heartbeat,
}

impl SimEventKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MarketSnapshot => "MARKET_SNAPSHOT",
            Self::RuleVersion => "RULE_VERSION",
            Self::Intent => "INTENT",
            Self::BaselineOrder => "BASELINE_ORDER",
            Self::BaselineFill => "BASELINE_FILL",
            Self::Heartbeat => "HEARTBEAT",
        }
    }
}

impl Display for SimEventKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SimEventKind {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "MARKET_SNAPSHOT" => Ok(Self::MarketSnapshot),
            "RULE_VERSION" => Ok(Self::RuleVersion),
            "INTENT" => Ok(Self::Intent),
            "BASELINE_ORDER" => Ok(Self::BaselineOrder),
            "BASELINE_FILL" => Ok(Self::BaselineFill),
            "HEARTBEAT" => Ok(Self::Heartbeat),
            other => Err(ParseEnumError {
                kind: "SimEventKind",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SimDriftSeverity {
    Info,
    Warn,
    Alert,
}

impl SimDriftSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Warn => "WARN",
            Self::Alert => "ALERT",
        }
    }
}

impl Display for SimDriftSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SimDriftSeverity {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "INFO" => Ok(Self::Info),
            "WARN" => Ok(Self::Warn),
            "ALERT" => Ok(Self::Alert),
            other => Err(ParseEnumError {
                kind: "SimDriftSeverity",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimEventRecord {
    pub run_id: Uuid,
    pub sequence: u64,
    pub mode: SimMode,
    pub event_kind: SimEventKind,
    pub event_time: Timestamp,
    pub market_id: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewSimEvent {
    pub domain: AccountDomain,
    pub run_id: Uuid,
    pub sequence: u64,
    pub mode: SimMode,
    pub event_kind: SimEventKind,
    pub event_time: Timestamp,
    pub market_id: Option<String>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimOrderRecord {
    pub run_id: Uuid,
    pub mode: SimMode,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_id: String,
    pub status: OrderLifecycleStatus,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub detail: serde_json::Value,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewSimOrderRecord {
    pub domain: AccountDomain,
    pub run_id: Uuid,
    pub mode: SimMode,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub market_id: String,
    pub status: OrderLifecycleStatus,
    pub filled_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub detail: serde_json::Value,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimFillRecord {
    pub fill_id: Uuid,
    pub run_id: Uuid,
    pub mode: SimMode,
    pub order_id: String,
    pub market_id: String,
    pub quantity: f64,
    pub price: f64,
    pub fees_paid: f64,
    pub filled_at: Timestamp,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewSimFillRecord {
    pub domain: AccountDomain,
    pub run_id: Uuid,
    pub mode: SimMode,
    pub order_id: String,
    pub market_id: String,
    pub quantity: f64,
    pub price: f64,
    pub fees_paid: f64,
    pub filled_at: Timestamp,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayCheckpoint {
    pub run_id: Uuid,
    pub mode: SimMode,
    pub cursor: String,
    pub processed_events: u64,
    pub updated_at: Timestamp,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewReplayCheckpoint {
    pub domain: AccountDomain,
    pub run_id: Uuid,
    pub mode: SimMode,
    pub cursor: String,
    pub processed_events: u64,
    pub updated_at: Timestamp,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimDriftReport {
    pub run_id: Uuid,
    pub mode: SimMode,
    pub intent_match_rate_bps: u32,
    pub reject_rate_bps: u32,
    pub fill_rate_bps: u32,
    pub fee_leakage_bps: i32,
    pub average_latency_ms: u64,
    pub p95_latency_ms: u64,
    pub baseline_orders: u64,
    pub simulated_orders: u64,
    pub drift_score_bps: u32,
    pub severity: SimDriftSeverity,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SimRunReport {
    pub run_id: Uuid,
    pub domain: AccountDomain,
    pub mode: SimMode,
    pub started_at: Timestamp,
    pub completed_at: Option<Timestamp>,
    pub input_snapshot_hash: String,
    pub rule_version_hash: String,
    pub processed_events: u64,
    pub orders_emitted: u64,
    pub fills_emitted: u64,
    pub drift: SimDriftReport,
    pub risk_decisions: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticValueKind {
    Text,
    Number,
    Timestamp,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticValue {
    pub kind: SemanticValueKind,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SemanticAttribute {
    pub key: String,
    pub value: SemanticValue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticTag {
    Binary,
    TimeBound,
    Threshold,
    NegRisk,
    Sports,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SemanticConfidence {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClarificationRecord {
    pub text: String,
    pub occurred_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RawMarketDocument {
    pub market_id: String,
    pub event_id: String,
    pub condition_id: String,
    pub token_ids: Vec<String>,
    pub title: String,
    pub category: String,
    pub outcomes: Vec<String>,
    pub end_time: Timestamp,
    pub resolution_source: Option<String>,
    pub edge_cases: Vec<String>,
    pub clarifications: Vec<ClarificationRecord>,
    pub fees_enabled: bool,
    pub neg_risk: bool,
    pub neg_risk_augmented: bool,
    pub tick_size: f64,
    pub raw_rules_text: String,
    pub market_status: String,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarketCanonical {
    pub market_id: String,
    pub event_id: String,
    pub condition_id: String,
    pub token_ids: Vec<String>,
    pub title: String,
    pub category: String,
    pub outcomes: Vec<String>,
    pub end_time: Timestamp,
    pub resolution_source: Option<String>,
    pub edge_cases: Vec<String>,
    pub clarifications: Vec<ClarificationRecord>,
    pub fees_enabled: bool,
    pub neg_risk: bool,
    pub neg_risk_augmented: bool,
    pub tick_size: f64,
    pub rules_version: String,
    pub raw_rules_text: String,
    pub market_status: String,
    pub observed_at: Timestamp,
    pub updated_at: Timestamp,
    pub semantic_tags: Vec<SemanticTag>,
    pub semantic_attributes: Vec<SemanticAttribute>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleVersion {
    pub market_id: String,
    pub rules_version: String,
    pub created_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventFamily {
    pub event_id: String,
    pub markets: Vec<MarketCanonical>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventFamilySnapshot {
    pub event_id: String,
    pub family: EventFamily,
    pub version: i64,
    pub generated_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConstraintEdgeType {
    TimeImplication,
    ThresholdMonotonicity,
    Equivalent,
    MutuallyExclusive,
    Inclusion,
    Dependency,
    NegRiskConversion,
    RuleVersionInvalidation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConstraintEdge {
    pub edge_id: Uuid,
    pub src_market_id: String,
    pub dst_market_id: String,
    pub edge_type: ConstraintEdgeType,
    pub confidence: SemanticConfidence,
    pub rules_version_src: String,
    pub rules_version_dst: String,
    pub evidence: Vec<String>,
    pub created_at: Timestamp,
    pub invalidated_at: Option<Timestamp>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConstraintGraph {
    pub event_id: String,
    pub generated_at: Timestamp,
    pub markets: Vec<MarketCanonical>,
    pub edges: Vec<ConstraintEdge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphScope {
    All,
    Event { event_id: String },
    Market { market_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConstraintGraphSnapshot {
    pub scope: GraphScope,
    pub graph: ConstraintGraph,
    pub version: i64,
    pub generated_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum StrategyKind {
    Rebalancing,
    DependencyArb,
    NegRisk,
    FeeAwareMM,
    RulesDriven,
}

impl StrategyKind {
    pub const ALL: [Self; 5] = [
        Self::Rebalancing,
        Self::DependencyArb,
        Self::NegRisk,
        Self::FeeAwareMM,
        Self::RulesDriven,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rebalancing => "REBALANCING",
            Self::DependencyArb => "DEPENDENCY_ARB",
            Self::NegRisk => "NEG_RISK",
            Self::FeeAwareMM => "FEE_AWARE_MM",
            Self::RulesDriven => "RULES_DRIVEN",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MvpCapabilityFlags {
    pub directional_positions_allowed: bool,
    pub sports_latency_trading_allowed: bool,
    pub broad_market_making_allowed: bool,
    pub human_trade_approval_allowed: bool,
    pub model_driven_execution_allowed: bool,
}

impl Default for MvpCapabilityFlags {
    fn default() -> Self {
        Self {
            directional_positions_allowed: false,
            sports_latency_trading_allowed: false,
            broad_market_making_allowed: false,
            human_trade_approval_allowed: false,
            model_driven_execution_allowed: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RolloutCapabilityMatrix {
    pub allow_real_execution: bool,
    pub allow_multileg: bool,
    pub allow_market_making: bool,
    pub allow_auto_redemption: bool,
    pub require_shadow_alignment: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutGuardrail {
    pub code: String,
    pub summary: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutPolicy {
    pub stage: PromotionStage,
    pub domain: AccountDomain,
    pub allowed_strategies: Vec<StrategyKind>,
    pub max_total_notional: f64,
    pub max_batch_notional: f64,
    pub max_strategy_notional: f64,
    pub capabilities: RolloutCapabilityMatrix,
    pub mvp_flags: MvpCapabilityFlags,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutEvidence {
    pub runtime_mode: String,
    pub heartbeat_age_ms: u64,
    pub reject_rate_5m: f64,
    pub fill_rate_5m: f64,
    pub recent_425_count: u32,
    pub market_ws_lag_ms: u64,
    pub reconcile_drift: bool,
    pub disputed_capital_ratio: f64,
    pub latest_alert_severity: Option<AlertSeverity>,
    pub open_alerts: usize,
    pub unresolved_incidents: usize,
    pub stable_window_secs: u64,
    pub shadow_live_drift_bps: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutEvaluation {
    pub evaluation_id: Uuid,
    pub domain: AccountDomain,
    pub current_stage: PromotionStage,
    pub target_stage: Option<PromotionStage>,
    pub eligible: bool,
    pub blocking_reasons: Vec<RolloutGuardrail>,
    pub warnings: Vec<RolloutGuardrail>,
    pub evidence: RolloutEvidence,
    pub evaluated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutStageRecord {
    pub domain: AccountDomain,
    pub stage: PromotionStage,
    pub previous_stage: Option<PromotionStage>,
    pub approved_by: Option<String>,
    pub approved_at: Option<Timestamp>,
    pub reason: String,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PromotionCandidate {
    pub candidate_id: Uuid,
    pub domain: AccountDomain,
    pub current_stage: PromotionStage,
    pub target_stage: PromotionStage,
    pub evidence_summary: String,
    pub blocking_reasons: Vec<RolloutGuardrail>,
    pub valid_until: Timestamp,
    pub created_at: Timestamp,
    pub invalidated_at: Option<Timestamp>,
}

impl PromotionCandidate {
    pub fn is_active(&self, now: Timestamp) -> bool {
        self.invalidated_at.is_none() && self.valid_until >= now
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RolloutIncidentSeverity {
    Warning,
    Critical,
}

impl RolloutIncidentSeverity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "WARNING",
            Self::Critical => "CRITICAL",
        }
    }
}

impl Display for RolloutIncidentSeverity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for RolloutIncidentSeverity {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "WARNING" => Ok(Self::Warning),
            "CRITICAL" => Ok(Self::Critical),
            _ => Err(ParseEnumError::new("RolloutIncidentSeverity", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutIncident {
    pub incident_id: Uuid,
    pub domain: AccountDomain,
    pub stage: PromotionStage,
    pub severity: RolloutIncidentSeverity,
    pub code: String,
    pub summary: String,
    pub detail: String,
    pub auto_rollback_stage: Option<PromotionStage>,
    pub created_at: Timestamp,
    pub resolved_at: Option<Timestamp>,
}

impl Display for StrategyKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for StrategyKind {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "REBALANCING" => Ok(Self::Rebalancing),
            "DEPENDENCY_ARB" => Ok(Self::DependencyArb),
            "NEG_RISK" => Ok(Self::NegRisk),
            "FEE_AWARE_MM" => Ok(Self::FeeAwareMM),
            "RULES_DRIVEN" => Ok(Self::RulesDriven),
            _ => Err(ParseEnumError::new("StrategyKind", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityScore {
    pub edge_gross_bps: i32,
    pub fee_cost_bps: i32,
    pub slippage_cost_bps: i32,
    pub failure_risk_bps: i32,
    pub edge_net_bps: i32,
    pub confidence: f32,
    pub half_life_sec: u32,
    pub capital_lock_days: f32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpportunityCandidate {
    pub opportunity_id: Uuid,
    pub strategy: StrategyKind,
    pub market_refs: Vec<String>,
    pub event_id: String,
    pub edge_gross_bps: i32,
    pub fee_cost_bps: i32,
    pub slippage_cost_bps: i32,
    pub failure_risk_bps: i32,
    pub edge_net_bps: i32,
    pub confidence: f32,
    pub half_life_sec: u32,
    pub capital_lock_days: f32,
    pub needs_multileg: bool,
    pub thesis_ref: String,
    pub research_ref: Option<String>,
    pub llm_review: Option<serde_json::Value>,
    pub graph_version: i64,
    pub book_observed_at: Timestamp,
    pub created_at: Timestamp,
    pub invalidated_at: Option<Timestamp>,
    pub invalidation_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScannerRunReport {
    pub run_id: Uuid,
    pub domain: AccountDomain,
    pub scope_key: String,
    pub scanner: StrategyKind,
    pub graph_version: i64,
    pub book_version_hint: Option<Timestamp>,
    pub candidates_emitted: usize,
    pub candidates_rejected: usize,
    pub rejection_reasons: Vec<String>,
    pub started_at: Timestamp,
    pub completed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpportunityInvalidation {
    pub invalidation_id: Uuid,
    pub opportunity_id: Uuid,
    pub reason: String,
    pub invalidated_at: Timestamp,
    pub affected_market_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RulesInvalidation {
    pub event_id: String,
    pub market_ids: Vec<String>,
    pub rules_versions: Vec<String>,
    pub reason: String,
    pub invalidated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub domain: AccountDomain,
    pub after_sequence: Option<i64>,
    pub limit: usize,
    pub reason: Option<String>,
    pub alert_id: Option<Uuid>,
    pub audit_event_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReplayJobStatus {
    Pending,
    Running,
    Completed,
    Failed,
}

impl ReplayJobStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Running => "RUNNING",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
        }
    }
}

impl Display for ReplayJobStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ReplayJobStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().as_str() {
            "PENDING" => Ok(Self::Pending),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            _ => Err(ParseEnumError::new("ReplayJobStatus", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayJob {
    pub job_id: Uuid,
    pub domain: AccountDomain,
    pub requested_by: String,
    pub requested_at: Timestamp,
    pub after_sequence: Option<i64>,
    pub limit: usize,
    pub status: ReplayJobStatus,
    pub started_at: Option<Timestamp>,
    pub completed_at: Option<Timestamp>,
    pub error: Option<String>,
    pub run_id: Option<Uuid>,
    pub worker_id: Option<String>,
    pub reason: Option<String>,
    pub alert_id: Option<Uuid>,
    pub audit_event_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub run_id: Uuid,
    pub requested_at: Timestamp,
    pub completed_at: Timestamp,
    pub event_limit: usize,
    pub processed_events: usize,
    pub graph_versions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ReplayTrace {
    pub job: Option<ReplayJob>,
    pub run: Option<ReplayReport>,
    pub alerts: Vec<AlertEvent>,
    pub audit_events: Vec<AuditEvent>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PositionSnapshot {
    pub market_id: String,
    pub token_id: String,
    pub event_id: String,
    pub category: String,
    pub quantity: f64,
    pub average_price: f64,
    pub mark_price: f64,
    pub reserved_notional: f64,
    pub unrealized_pnl: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementPhase {
    Trading,
    Resolving,
    Disputed,
    Finalized,
    Redeeming,
    Redeemed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementPositionState {
    Trading,
    Settling,
    Disputed,
    Redeemable,
    Redeemed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementTaskKind {
    MonitorResolution,
    ReconcileOrders,
    AwaitFinalization,
    RedeemInventory,
    ReleaseCapital,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SettlementTaskStatus {
    Pending,
    InProgress,
    Blocked,
    Completed,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementMarketState {
    pub domain: AccountDomain,
    pub market_id: String,
    pub event_id: String,
    pub phase: SettlementPhase,
    pub winning_price: Option<f64>,
    pub disputed: bool,
    pub finalizes_at: Option<Timestamp>,
    pub updated_at: Timestamp,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementPositionSnapshot {
    pub domain: AccountDomain,
    pub market_id: String,
    pub token_id: String,
    pub event_id: String,
    pub net_quantity: f64,
    pub average_open_price: f64,
    pub gross_cost_basis: f64,
    pub realized_cash_flow: f64,
    pub realized_pnl: f64,
    pub lifecycle_state: SettlementPositionState,
    pub last_trade_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementTask {
    pub task_id: Uuid,
    pub domain: AccountDomain,
    pub market_id: String,
    pub event_id: String,
    pub kind: SettlementTaskKind,
    pub status: SettlementTaskStatus,
    pub detail: String,
    pub blocked_reason: Option<String>,
    pub expected_cash_recovery: Option<f64>,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementDiscrepancy {
    pub domain: AccountDomain,
    pub order_id: String,
    pub market_id: String,
    pub reason: String,
    pub detail: String,
    pub detected_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementCashRecovery {
    pub domain: AccountDomain,
    pub market_id: String,
    pub event_id: String,
    pub amount: f64,
    pub recovered_at: Timestamp,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SettlementEngineReport {
    pub domain: AccountDomain,
    pub generated_at: Timestamp,
    pub positions_updated: usize,
    pub tasks_open: usize,
    pub tasks_closed: usize,
    pub discrepancies: usize,
    pub converged_orders: usize,
    pub recovered_cash: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SettlementCommand {
    UpdateMarketState {
        market_id: String,
        event_id: String,
        phase: SettlementPhase,
        winning_price: Option<f64>,
        finalizes_at: Option<Timestamp>,
        detail: String,
    },
    ConvergeOrder {
        order_id: String,
        status: Option<OrderLifecycleStatus>,
        detail: String,
    },
    RecordRedemption {
        market_id: String,
        event_id: String,
        amount: f64,
        detail: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RuntimeHealth {
    pub domain: AccountDomain,
    pub runtime_mode: String,
    pub now: Timestamp,
    pub market_ws_lag_ms: u64,
    pub user_ws_ok: bool,
    pub heartbeat_age_ms: u64,
    pub recent_425_count: u32,
    pub reject_rate_5m: f64,
    pub reconcile_drift: bool,
    pub capital_buffer_ok: bool,
    pub fill_rate_5m: f64,
    pub open_orders: usize,
    pub reconcile_lag_ms: u64,
    pub disputed_capital_ratio: f64,
    pub degradation_reason: Option<String>,
    pub last_alert_at: Option<Timestamp>,
    pub stable_since: Option<Timestamp>,
    pub shadow_live_drift_bps: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MdGatewayRuntime {
    pub domain: AccountDomain,
    pub mode: String,
    pub market_stream_connected: bool,
    pub user_stream_connected: bool,
    pub last_market_event_at: Option<Timestamp>,
    pub last_user_event_at: Option<Timestamp>,
    pub market_messages_total: u64,
    pub user_messages_total: u64,
    pub observed_at: Timestamp,
}

impl RuntimeHealth {
    pub fn is_degraded(&self) -> bool {
        !self.user_ws_ok
            || self.market_ws_lag_ms >= 2_500
            || self.heartbeat_age_ms >= 5_000
            || self.recent_425_count >= 3
            || self.reject_rate_5m >= 0.10
            || self.reconcile_drift
    }

    pub fn execution_penalty(&self) -> f64 {
        let mut penalty: f64 = 1.0;
        if self.market_ws_lag_ms >= 1_000 {
            penalty *= 0.9;
        }
        if self.market_ws_lag_ms >= 2_500 {
            penalty *= 0.7;
        }
        if !self.user_ws_ok {
            penalty *= 0.6;
        }
        if self.heartbeat_age_ms >= 5_000 {
            penalty *= 0.75;
        }
        if self.recent_425_count >= 3 {
            penalty *= 0.8;
        }
        if self.reject_rate_5m >= 0.10 {
            penalty *= 0.75;
        }
        if self.reconcile_drift {
            penalty *= 0.6;
        }
        penalty.clamp(0.0, 1.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub nav: f64,
    pub cash_available: f64,
    pub reserved_cash: f64,
    pub positions: Vec<PositionSnapshot>,
    pub unresolved_capital: f64,
    pub redeemable_capital: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RiskBudgetSnapshot {
    pub max_structural_arb_notional: f64,
    pub max_neg_risk_notional: f64,
    pub max_rules_driven_notional: f64,
    pub max_fee_mm_notional: f64,
    pub min_cash_buffer: f64,
    pub max_single_intent_notional: f64,
    pub max_event_notional: f64,
    pub max_category_notional: f64,
    pub max_market_notional: f64,
    pub max_unresolved_notional: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AllocationPlan {
    pub structural_arb_budget: f64,
    pub neg_risk_budget: f64,
    pub rules_driven_budget: f64,
    pub fee_mm_budget: f64,
    pub cash_buffer_target: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeSide {
    Buy,
    Sell,
}

impl TradeSide {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl Display for TradeSide {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TradeSide {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            _ => Err(ParseEnumError::new("TradeSide", value)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IntentPolicy {
    Passive,
    AggressiveLimit,
    ReduceOnly,
    Shrinkable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PortfolioRejectReason {
    EdgeTooLow,
    CashBuffer,
    CapacityExceeded,
    EventCorrelation,
    CategoryCorrelation,
    MarketCorrelation,
    ResolutionSourceCorrelation,
    RuntimeDegraded,
    StrategyBudget,
    UnresolvedCapital,
    InvalidQuote,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OptimizationStatus {
    Optimal,
    HeuristicFallback,
    TimedOut,
    Infeasible,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntent {
    pub account_domain: AccountDomain,
    pub market_id: String,
    pub token_id: String,
    pub side: TradeSide,
    pub limit_price: f64,
    pub max_size: f64,
    pub policy: IntentPolicy,
    pub expires_at: Timestamp,
    pub strategy_kind: StrategyKind,
    pub thesis_ref: String,
    pub research_ref: Option<String>,
    pub opportunity_id: Uuid,
    pub event_id: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TradeIntentBatch {
    pub account_domain: AccountDomain,
    pub created_at: Timestamp,
    pub optimization_status: OptimizationStatus,
    pub intents: Vec<TradeIntent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionCommandKind {
    Submit,
    CancelOrder,
    CancelMarket,
    CancelAll,
    Recover,
    Reconcile,
    Heartbeat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionCommand {
    pub command_id: Uuid,
    pub domain: AccountDomain,
    pub kind: ExecutionCommandKind,
    pub intent_id: Option<Uuid>,
    pub order_id: Option<String>,
    pub market_id: Option<String>,
    pub requested_by: String,
    pub requested_at: Timestamp,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionEventKind {
    Accepted,
    Submitted,
    Acknowledged,
    FillObserved,
    CancelRequested,
    Cancelled,
    FailedRecoverable,
    FailedTerminal,
    HeartbeatHealthy,
    HeartbeatDegraded,
    RecoveryTriggered,
    Reconciled,
    AlertRaised,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub event_id: Uuid,
    pub domain: AccountDomain,
    pub kind: ExecutionEventKind,
    pub intent_id: Option<Uuid>,
    pub order_id: Option<String>,
    pub market_id: Option<String>,
    pub client_order_id: Option<String>,
    pub detail: String,
    pub payload: serde_json::Value,
    pub occurred_at: Timestamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionFailureKind {
    Config,
    Signing,
    VenueRejected,
    Network,
    VenueRestart,
    StateConflict,
    Recovery,
    Reconcile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionIntentStatus {
    PendingValidation,
    ReadyToSubmit,
    Submitted,
    PartiallyFilled,
    Filled,
    CancelRequested,
    Cancelled,
    FailedRecoverable,
    FailedTerminal,
    Reconciling,
    Reconciled,
}

impl ExecutionIntentStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PendingValidation => "PENDING_VALIDATION",
            Self::ReadyToSubmit => "READY_TO_SUBMIT",
            Self::Submitted => "SUBMITTED",
            Self::PartiallyFilled => "PARTIALLY_FILLED",
            Self::Filled => "FILLED",
            Self::CancelRequested => "CANCEL_REQUESTED",
            Self::Cancelled => "CANCELLED",
            Self::FailedRecoverable => "FAILED_RECOVERABLE",
            Self::FailedTerminal => "FAILED_TERMINAL",
            Self::Reconciling => "RECONCILING",
            Self::Reconciled => "RECONCILED",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Filled
                | Self::Cancelled
                | Self::FailedTerminal
                | Self::FailedRecoverable
                | Self::Reconciled
        )
    }
}

impl Display for ExecutionIntentStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ExecutionIntentStatus {
    type Err = ParseEnumError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_uppercase().as_str() {
            "PENDING_VALIDATION" => Ok(Self::PendingValidation),
            "READY_TO_SUBMIT" => Ok(Self::ReadyToSubmit),
            "SUBMITTED" => Ok(Self::Submitted),
            "PARTIALLY_FILLED" => Ok(Self::PartiallyFilled),
            "FILLED" => Ok(Self::Filled),
            "CANCEL_REQUESTED" => Ok(Self::CancelRequested),
            "CANCELLED" => Ok(Self::Cancelled),
            "FAILED_RECOVERABLE" => Ok(Self::FailedRecoverable),
            "FAILED_TERMINAL" => Ok(Self::FailedTerminal),
            "RECONCILING" => Ok(Self::Reconciling),
            "RECONCILED" => Ok(Self::Reconciled),
            _ => Err(ParseEnumError::new("ExecutionIntentStatus", value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionIntentRecord {
    pub intent_id: Uuid,
    pub domain: AccountDomain,
    pub batch_id: Option<Uuid>,
    pub strategy_kind: StrategyKind,
    pub market_id: String,
    pub token_id: String,
    pub side: TradeSide,
    pub limit_price: f64,
    pub target_size: f64,
    pub idempotency_key: String,
    pub client_order_id: String,
    pub status: ExecutionIntentStatus,
    pub detail: serde_json::Value,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub expires_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionHeartbeat {
    pub domain: AccountDomain,
    pub service: ServiceKind,
    pub venue_healthy: bool,
    pub signer_healthy: bool,
    pub event_loop_healthy: bool,
    pub consecutive_failures: u32,
    pub detail: String,
    pub observed_at: Timestamp,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionReconcileReport {
    pub domain: AccountDomain,
    pub generated_at: Timestamp,
    pub checked_orders: usize,
    pub mismatches_fixed: usize,
    pub missing_remote_orders: usize,
    pub alerts: Vec<String>,
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("execution configuration invalid: {0}")]
    Config(String),
    #[error("signing failed: {0}")]
    Signing(String),
    #[error("venue rejected request: {0}")]
    VenueRejected(String),
    #[error("transient network error: {0}")]
    Network(String),
    #[error("venue restart in progress: {0}")]
    VenueRestart(String),
    #[error("state conflict: {0}")]
    StateConflict(String),
    #[error("recovery failed: {0}")]
    Recovery(String),
    #[error("reconciliation failed: {0}")]
    Reconcile(String),
}

impl ExecutionError {
    pub const fn kind(&self) -> ExecutionFailureKind {
        match self {
            Self::Config(_) => ExecutionFailureKind::Config,
            Self::Signing(_) => ExecutionFailureKind::Signing,
            Self::VenueRejected(_) => ExecutionFailureKind::VenueRejected,
            Self::Network(_) => ExecutionFailureKind::Network,
            Self::VenueRestart(_) => ExecutionFailureKind::VenueRestart,
            Self::StateConflict(_) => ExecutionFailureKind::StateConflict,
            Self::Recovery(_) => ExecutionFailureKind::Recovery,
            Self::Reconcile(_) => ExecutionFailureKind::Reconcile,
        }
    }

    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            Self::Network(_) | Self::VenueRestart(_) | Self::Recovery(_) | Self::Reconcile(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn startup_manifest_contains_all_processes() {
        let manifest = StartupManifest::default();
        assert_eq!(manifest.services.len(), ServiceKind::ALL.len());
        assert!(manifest
            .services
            .iter()
            .any(|svc| svc.kind == ServiceKind::MdGateway));
        assert!(manifest
            .services
            .iter()
            .any(|svc| svc.kind == ServiceKind::SettlementEngine));
    }

    #[test]
    fn required_domains_are_complete_and_valid() {
        let descriptors = required_domain_descriptors();
        assert_eq!(descriptors.len(), 3);
        for descriptor in descriptors {
            descriptor.validate().expect("descriptor should be valid");
        }
    }

    #[test]
    fn sim_cannot_execute() {
        let sim = AccountDomain::Sim.descriptor();
        assert!(!sim.allowed_runtime_modes.contains(&RuntimeMode::Execute));
        assert_eq!(sim.default_runtime_mode, RuntimeMode::Simulate);
    }

    #[test]
    fn promotion_path_is_fixed() {
        assert_eq!(
            PromotionStage::PATH,
            [
                PromotionStage::Replay,
                PromotionStage::Shadow,
                PromotionStage::Canary,
                PromotionStage::Live,
            ]
        );
    }

    #[test]
    fn promotion_stage_neighbors_are_stable() {
        assert_eq!(PromotionStage::Replay.next(), Some(PromotionStage::Shadow));
        assert_eq!(
            PromotionStage::Shadow.previous(),
            Some(PromotionStage::Replay)
        );
        assert!(PromotionStage::Canary.is_reachable_from(PromotionStage::Shadow));
        assert!(!PromotionStage::Live.is_reachable_from(PromotionStage::Replay));
        assert_eq!(PromotionStage::Live.next(), None);
    }

    #[test]
    fn strategy_kind_all_covers_every_strategy() {
        assert_eq!(StrategyKind::ALL.len(), 5);
        assert!(StrategyKind::ALL.contains(&StrategyKind::Rebalancing));
        assert!(StrategyKind::ALL.contains(&StrategyKind::FeeAwareMM));
    }

    #[test]
    fn promotion_candidate_active_requires_no_invalidation_and_future_expiry() {
        let now_ts = now();
        let candidate = PromotionCandidate {
            candidate_id: Uuid::new_v4(),
            domain: AccountDomain::Canary,
            current_stage: PromotionStage::Canary,
            target_stage: PromotionStage::Live,
            evidence_summary: "stable".to_owned(),
            blocking_reasons: Vec::new(),
            valid_until: now_ts + chrono::Duration::minutes(10),
            created_at: now_ts,
            invalidated_at: None,
        };
        assert!(candidate.is_active(now_ts));
        assert!(!PromotionCandidate {
            invalidated_at: Some(now_ts),
            ..candidate.clone()
        }
        .is_active(now_ts));
        assert!(!PromotionCandidate {
            valid_until: now_ts - chrono::Duration::seconds(1),
            ..candidate
        }
        .is_active(now_ts));
    }

    #[test]
    fn descriptor_round_trip_serde() {
        let descriptor = AccountDomain::Canary.descriptor();
        let payload = serde_json::to_string(&descriptor).expect("serialize");
        let restored: AccountDomainDescriptor =
            serde_json::from_str(&payload).expect("deserialize");
        assert_eq!(restored, descriptor);
    }

    #[test]
    fn domain_validation_error_is_readable() {
        let mut descriptor = AccountDomain::Live.descriptor();
        descriptor.allowed_runtime_modes = vec![RuntimeMode::Observe];
        let error = descriptor.validate().expect_err("validation should fail");
        assert!(error
            .to_string()
            .contains("default runtime mode `DISABLED` is not included"));
    }
}
