use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use polymarket_core::{
    required_domain_descriptors, AccountDomain, AccountDomainDescriptor, MvpCapabilityFlags,
    PromotionStage, RolloutCapabilityMatrix, RolloutPolicy, RuntimeMode, SimMode, StrategyKind,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MdGatewayMode {
    Realtime,
    ReplayFile,
}

impl MdGatewayMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Realtime => "realtime",
            Self::ReplayFile => "replay_file",
        }
    }
}

impl FromStr for MdGatewayMode {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "realtime" => Ok(Self::Realtime),
            "replay_file" | "replay" | "file" => Ok(Self::ReplayFile),
            other => bail!("unsupported md-gateway mode `{other}`"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MdGatewayConfig {
    pub mode: MdGatewayMode,
    pub replay_input_path: PathBuf,
    pub replay_poll_interval: Duration,
    pub market_ws_url: String,
    pub user_ws_url: String,
    pub metadata_api_base_url: String,
    pub metadata_refresh_interval: Duration,
    pub reconnect_backoff: Duration,
    pub heartbeat_timeout: Duration,
    pub market_subscription_chunk_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedConfig {
    pub app_name: String,
    pub environment: String,
    pub data_root: PathBuf,
    pub log_filter: String,
    pub heartbeat_interval: Duration,
}

impl SharedConfig {
    pub fn from_env(app_name: &str) -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(app_name, &vars)
    }

    pub fn from_map(app_name: &str, vars: &BTreeMap<String, String>) -> Result<Self> {
        let config = Self {
            app_name: app_name.to_owned(),
            environment: env_or(vars, "POLYMARKET_ENV", "dev"),
            data_root: PathBuf::from(env_or(vars, "POLYMARKET_DATA_DIR", "./var")),
            log_filter: env_or(vars, "POLYMARKET_LOG_FILTER", "info"),
            heartbeat_interval: Duration::from_secs(parse_u64(
                vars,
                "POLYMARKET_HEARTBEAT_INTERVAL_SECS",
                5,
            )?),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.app_name.trim().is_empty(),
            "app_name must not be empty"
        );
        ensure!(
            !self.environment.trim().is_empty(),
            "environment must not be empty"
        );
        ensure!(
            self.data_root.is_absolute() || self.data_root.starts_with("."),
            "data_root must be absolute or relative to the workspace"
        );
        ensure!(
            self.heartbeat_interval.as_secs() > 0,
            "heartbeat_interval must be positive"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CredentialSource {
    Unconfigured,
    EnvVar { variable: String },
    FilePath { path: PathBuf },
}

impl CredentialSource {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Unconfigured => "UNCONFIGURED",
            Self::EnvVar { .. } => "ENV_VAR",
            Self::FilePath { .. } => "FILE_PATH",
        }
    }

    pub fn is_configured(&self) -> bool {
        !matches!(self, Self::Unconfigured)
    }

    pub fn resolve_string(&self) -> Result<Option<String>> {
        match self {
            Self::Unconfigured => Ok(None),
            Self::EnvVar { variable } => {
                Ok(Some(env::var(variable).with_context(|| {
                    format!("failed to read credential env var `{variable}`")
                })?))
            }
            Self::FilePath { path } => {
                Ok(Some(std::fs::read_to_string(path).with_context(|| {
                    format!("failed to read credential file `{}`", path.display())
                })?))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainConfig {
    pub descriptor: AccountDomainDescriptor,
    pub data_dir: PathBuf,
    pub database_path: PathBuf,
    pub audit_prefix: String,
    pub wallet_id: Option<String>,
    pub credential_alias: Option<String>,
    pub l2_credentials: CredentialSource,
    pub signer: CredentialSource,
    pub execution_approved: bool,
    pub runtime_mode: RuntimeMode,
}

impl DomainConfig {
    pub fn domain(&self) -> AccountDomain {
        self.descriptor.domain
    }

    pub fn namespace(&self) -> &str {
        &self.descriptor.namespace
    }

    pub fn allows_runtime_mode(&self, mode: RuntimeMode) -> bool {
        self.descriptor.allowed_runtime_modes.contains(&mode)
    }

    pub fn credentials_summary(&self) -> (&'static str, &'static str) {
        (self.l2_credentials.kind(), self.signer.kind())
    }

    pub fn validate(&self, shared: &SharedConfig) -> Result<()> {
        self.descriptor.validate().map_err(anyhow::Error::from)?;
        ensure!(
            self.data_dir.starts_with(&shared.data_root),
            "domain `{}` data_dir `{}` must stay under shared data_root `{}`",
            self.domain(),
            self.data_dir.display(),
            shared.data_root.display()
        );
        ensure!(
            path_contains_namespace(&self.data_dir, self.namespace()),
            "domain `{}` data_dir `{}` must include namespace `{}`",
            self.domain(),
            self.data_dir.display(),
            self.namespace()
        );
        ensure!(
            self.database_path.starts_with(&self.data_dir),
            "domain `{}` database_path `{}` must stay under data_dir `{}`",
            self.domain(),
            self.database_path.display(),
            self.data_dir.display()
        );
        ensure!(
            file_name_contains_namespace(&self.database_path, self.namespace()),
            "domain `{}` database_path `{}` must include namespace `{}` in the filename",
            self.domain(),
            self.database_path.display(),
            self.namespace()
        );
        ensure!(
            !self.audit_prefix.trim().is_empty(),
            "domain `{}` audit_prefix must not be empty",
            self.domain()
        );
        ensure!(
            self.audit_prefix.contains(self.namespace()),
            "domain `{}` audit_prefix `{}` must include namespace `{}`",
            self.domain(),
            self.audit_prefix,
            self.namespace()
        );
        ensure!(
            self.allows_runtime_mode(self.runtime_mode),
            "domain `{}` runtime mode `{}` is not permitted",
            self.domain(),
            self.runtime_mode
        );
        if self.domain() == AccountDomain::Sim {
            ensure!(
                !self.l2_credentials.is_configured(),
                "domain `SIM` cannot configure L2 trading credentials"
            );
            ensure!(
                !self.signer.is_configured(),
                "domain `SIM` cannot configure signer credentials"
            );
            ensure!(
                self.wallet_id.is_none(),
                "domain `SIM` cannot set wallet_id"
            );
            ensure!(
                self.credential_alias.is_none(),
                "domain `SIM` cannot set credential_alias"
            );
            ensure!(
                !self.execution_approved,
                "domain `SIM` cannot enable execution approval"
            );
        }
        if self.domain() == AccountDomain::Live {
            ensure!(
                self.runtime_mode != RuntimeMode::Execute || self.execution_approved,
                "domain `LIVE` cannot enter EXECUTE without explicit execution approval"
            );
        }
        if self.runtime_mode.requires_execution_approval() {
            ensure!(
                self.execution_approved,
                "domain `{}` runtime mode `EXECUTE` requires explicit execution approval",
                self.domain()
            );
            ensure!(
                self.descriptor.allows_real_capital,
                "domain `{}` runtime mode `EXECUTE` requires a real-capital domain",
                self.domain()
            );
            ensure!(
                self.l2_credentials.is_configured(),
                "domain `{}` runtime mode `EXECUTE` requires L2 credential source configuration",
                self.domain()
            );
            ensure!(
                self.signer.is_configured(),
                "domain `{}` runtime mode `EXECUTE` requires signer credential source configuration",
                self.domain()
            );
        }
        Ok(())
    }

    pub fn summary(&self) -> DomainRuntimeSummary {
        DomainRuntimeSummary {
            domain: self.domain(),
            environment: self.descriptor.environment.as_str().to_owned(),
            data_dir: self.data_dir.clone(),
            database_path: self.database_path.clone(),
            default_runtime_mode: self.descriptor.default_runtime_mode,
            runtime_mode: self.runtime_mode,
            allowed_runtime_modes: self.descriptor.allowed_runtime_modes.clone(),
            execution_approved: self.execution_approved,
            l2_credentials_kind: self.l2_credentials.kind().to_owned(),
            signer_kind: self.signer.kind().to_owned(),
            namespace: self.namespace().to_owned(),
            audit_prefix: self.audit_prefix.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainRuntimeSummary {
    pub domain: AccountDomain,
    pub environment: String,
    pub data_dir: PathBuf,
    pub database_path: PathBuf,
    pub default_runtime_mode: RuntimeMode,
    pub runtime_mode: RuntimeMode,
    pub allowed_runtime_modes: Vec<RuntimeMode>,
    pub execution_approved: bool,
    pub l2_credentials_kind: String,
    pub signer_kind: String,
    pub namespace: String,
    pub audit_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainRegistry {
    pub domains: BTreeMap<AccountDomain, DomainConfig>,
}

impl DomainRegistry {
    pub fn from_env(shared: &SharedConfig) -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(shared, &vars)
    }

    pub fn from_map(shared: &SharedConfig, vars: &BTreeMap<String, String>) -> Result<Self> {
        let mut domains = BTreeMap::new();
        for descriptor in required_domain_descriptors() {
            let config = DomainConfigBuilder::new(shared, vars, descriptor).build()?;
            domains.insert(config.domain(), config);
        }
        let registry = Self { domains };
        registry.validate(shared)?;
        Ok(registry)
    }

    pub fn validate(&self, shared: &SharedConfig) -> Result<()> {
        ensure!(
            self.domains.len() == AccountDomain::ALL.len(),
            "domain registry must contain all required domains"
        );
        for domain in AccountDomain::ALL {
            ensure!(
                self.domains.contains_key(&domain),
                "domain registry is missing required domain `{domain}`"
            );
        }

        let mut seen_dirs = BTreeMap::<PathBuf, AccountDomain>::new();
        let mut seen_dbs = BTreeMap::<PathBuf, AccountDomain>::new();
        let mut seen_wallets = BTreeMap::<String, AccountDomain>::new();
        let mut seen_aliases = BTreeMap::<String, AccountDomain>::new();

        for domain in AccountDomain::ALL {
            let config = self
                .domains
                .get(&domain)
                .with_context(|| format!("missing domain config for `{domain}`"))?;
            config.validate(shared)?;

            if let Some(previous) = seen_dirs.insert(config.data_dir.clone(), domain) {
                bail!(
                    "domain `{domain}` data_dir `{}` conflicts with domain `{previous}`",
                    config.data_dir.display()
                );
            }
            if let Some(previous) = seen_dbs.insert(config.database_path.clone(), domain) {
                bail!(
                    "domain `{domain}` database_path `{}` conflicts with domain `{previous}`",
                    config.database_path.display()
                );
            }

            if config.descriptor.allows_real_capital {
                if let Some(wallet_id) = config.wallet_id.as_ref() {
                    if let Some(previous) =
                        seen_wallets.insert(wallet_id.to_ascii_lowercase(), domain)
                    {
                        bail!(
                            "domain `{domain}` wallet_id `{wallet_id}` conflicts with real-capital domain `{previous}`"
                        );
                    }
                }
                if let Some(alias) = config.credential_alias.as_ref() {
                    if let Some(previous) = seen_aliases.insert(alias.to_ascii_lowercase(), domain)
                    {
                        bail!(
                            "domain `{domain}` credential_alias `{alias}` conflicts with real-capital domain `{previous}`"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get(&self, domain: AccountDomain) -> Option<&DomainConfig> {
        self.domains.get(&domain)
    }

    pub fn visible_domains(&self) -> Vec<AccountDomain> {
        self.domains.keys().copied().collect()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub shared: SharedConfig,
    pub domain_registry: DomainRegistry,
    pub selected_domain: AccountDomain,
    pub selected_domain_config: DomainConfig,
    pub sim: SimConfig,
    pub rollout: RolloutConfig,
}

impl NodeConfig {
    pub fn from_env(default_domain: AccountDomain) -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(default_domain, &vars)
    }

    pub fn from_map(
        default_domain: AccountDomain,
        vars: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let shared = SharedConfig::from_map("polymarket-node", vars)?;
        let mut domain_registry = DomainRegistry::from_map(&shared, vars)?;
        let selected_domain = vars
            .get("POLYMARKET_NODE_DOMAIN")
            .map(|value| AccountDomain::from_str(value))
            .transpose()?
            .unwrap_or(default_domain);
        let runtime_mode = vars
            .get("POLYMARKET_NODE_RUNTIME_MODE")
            .map(|value| RuntimeMode::from_str(value))
            .transpose()?;
        let selected = domain_registry
            .domains
            .get_mut(&selected_domain)
            .with_context(|| format!("missing selected domain config for `{selected_domain}`"))?;
        if let Some(runtime_mode) = runtime_mode {
            selected.runtime_mode = runtime_mode;
        }
        selected.validate(&shared)?;
        let selected_domain_config = selected.clone();
        Ok(Self {
            shared,
            domain_registry,
            selected_domain,
            selected_domain_config,
            sim: SimConfig::from_map(selected_domain, vars)?,
            rollout: RolloutConfig::from_map(vars)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimConfig {
    pub enabled: bool,
    pub mode: SimMode,
    pub data_source: String,
    pub replay_start: Option<String>,
    pub replay_end: Option<String>,
    pub event_step_ms: u64,
    pub fill_probability_bps: u32,
    pub latency_ms: u64,
    pub queue_depth: usize,
    pub fee_bps: u32,
    pub drift_alert_bps: u32,
    pub heartbeat_timeout_ms: u64,
    pub persist_outputs: bool,
    pub output_dir: PathBuf,
}

impl SimConfig {
    pub fn from_env(selected_domain: AccountDomain) -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(selected_domain, &vars)
    }

    pub fn from_map(
        selected_domain: AccountDomain,
        vars: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let mode = vars
            .get("POLYMARKET_SIM_MODE")
            .map(|value| SimMode::from_str(value))
            .transpose()?
            .unwrap_or(SimMode::Replay);
        let enabled = if selected_domain == AccountDomain::Sim {
            parse_bool(vars, "POLYMARKET_SIM_ENABLED", true)?
        } else {
            false
        };
        Ok(Self {
            enabled,
            mode,
            data_source: env_or(vars, "POLYMARKET_SIM_DATA_SOURCE", "store"),
            replay_start: vars.get("POLYMARKET_SIM_REPLAY_START").cloned(),
            replay_end: vars.get("POLYMARKET_SIM_REPLAY_END").cloned(),
            event_step_ms: parse_u64(vars, "POLYMARKET_SIM_EVENT_STEP_MS", 250)?,
            fill_probability_bps: parse_u32(
                vars.get("POLYMARKET_SIM_FILL_PROBABILITY_BPS"),
                8_500,
            )?,
            latency_ms: parse_u64(vars, "POLYMARKET_SIM_LATENCY_MS", 250)?,
            queue_depth: parse_usize(vars.get("POLYMARKET_SIM_QUEUE_DEPTH"), 32)?,
            fee_bps: parse_u32(vars.get("POLYMARKET_SIM_FEE_BPS"), 35)?,
            drift_alert_bps: parse_u32(vars.get("POLYMARKET_SIM_DRIFT_ALERT_BPS"), 250)?,
            heartbeat_timeout_ms: parse_u64(vars, "POLYMARKET_SIM_HEARTBEAT_TIMEOUT_MS", 5_000)?,
            persist_outputs: parse_bool(vars, "POLYMARKET_SIM_PERSIST_OUTPUTS", true)?,
            output_dir: PathBuf::from(env_or(vars, "POLYMARKET_SIM_OUTPUT_DIR", "./var/sim")),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpsApiConfig {
    pub shared: SharedConfig,
    pub bind_addr: SocketAddr,
    pub auth_tokens: BTreeMap<String, String>,
    pub auth_token: Option<String>,
    pub visible_domains: Vec<AccountDomain>,
    pub target_domain: AccountDomain,
    pub target_domain_config: DomainConfig,
    pub monitoring: MonitoringConfig,
    pub alerting: AlertingConfig,
    pub telemetry: TelemetryConfig,
    pub rollout: RolloutConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutThresholds {
    pub stable_window: Duration,
    pub candidate_ttl: Duration,
    pub heartbeat_max_ms: u64,
    pub reject_rate_max: f64,
    pub fill_rate_min: f64,
    pub market_ws_lag_max_ms: u64,
    pub disputed_capital_ratio_max: f64,
    pub max_open_critical_alerts: usize,
    pub max_shadow_live_drift_bps: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RolloutConfig {
    pub policies: BTreeMap<PromotionStage, RolloutPolicy>,
    pub thresholds: BTreeMap<PromotionStage, RolloutThresholds>,
    pub auto_rollback_to_safe: bool,
}

impl RolloutConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        let mut policies = BTreeMap::new();
        let mut thresholds = BTreeMap::new();
        for stage in PromotionStage::PATH {
            let policy = rollout_policy_from_map(vars, stage)?;
            validate_rollout_policy(&policy)?;
            policies.insert(stage, policy);
            thresholds.insert(stage, rollout_thresholds_from_map(vars, stage)?);
        }
        let config = Self {
            policies,
            thresholds,
            auto_rollback_to_safe: parse_bool(
                vars,
                "POLYMARKET_ROLLOUT_AUTO_ROLLBACK_TO_SAFE",
                true,
            )?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        for stage in PromotionStage::PATH {
            ensure!(
                self.policies.contains_key(&stage),
                "missing rollout policy for stage `{stage}`"
            );
            ensure!(
                self.thresholds.contains_key(&stage),
                "missing rollout thresholds for stage `{stage}`"
            );
        }
        let replay = self.policy(PromotionStage::Replay)?;
        ensure!(
            replay.domain == AccountDomain::Sim,
            "REPLAY stage must map to SIM domain"
        );
        ensure!(
            !replay.capabilities.allow_real_execution,
            "REPLAY cannot allow real execution"
        );
        let shadow = self.policy(PromotionStage::Shadow)?;
        ensure!(
            shadow.domain == AccountDomain::Sim,
            "SHADOW stage must map to SIM domain"
        );
        ensure!(
            !shadow.capabilities.allow_real_execution,
            "SHADOW cannot allow real execution"
        );
        ensure!(
            shadow.capabilities.require_shadow_alignment,
            "SHADOW must require shadow alignment"
        );
        let canary = self.policy(PromotionStage::Canary)?;
        ensure!(
            canary.domain == AccountDomain::Canary,
            "CANARY stage must map to CANARY domain"
        );
        ensure!(
            canary.capabilities.allow_real_execution,
            "CANARY must allow real execution"
        );
        let live = self.policy(PromotionStage::Live)?;
        ensure!(
            live.domain == AccountDomain::Live,
            "LIVE stage must map to LIVE domain"
        );
        ensure!(
            live.capabilities.allow_real_execution,
            "LIVE must allow real execution"
        );
        Ok(())
    }

    pub fn policy(&self, stage: PromotionStage) -> Result<&RolloutPolicy> {
        self.policies
            .get(&stage)
            .with_context(|| format!("missing rollout policy for `{stage}`"))
    }

    pub fn threshold(&self, stage: PromotionStage) -> Result<&RolloutThresholds> {
        self.thresholds
            .get(&stage)
            .with_context(|| format!("missing rollout thresholds for `{stage}`"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub poll_interval: Duration,
    pub alert_debounce: Duration,
    pub heartbeat_degraded_ms: u64,
    pub heartbeat_safe_ms: u64,
    pub market_ws_lag_degraded_ms: u64,
    pub market_ws_lag_safe_ms: u64,
    pub reject_rate_degraded_bps: u64,
    pub reject_rate_safe_bps: u64,
    pub disputed_capital_safe_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AlertingConfig {
    pub webhook_url: Option<String>,
    pub webhook_timeout: Duration,
    pub webhook_retries: usize,
    pub prometheus_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub trace_sample_ratio: f64,
}

impl OpsApiConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        let shared = SharedConfig::from_map("polymarket-ops-api", vars)?;
        let registry = DomainRegistry::from_map(&shared, vars)?;
        let bind_addr = vars
            .get("POLYMARKET_OPS_BIND_ADDR")
            .map(|value| value.parse::<SocketAddr>())
            .transpose()
            .context("failed to parse POLYMARKET_OPS_BIND_ADDR as SocketAddr")?
            .unwrap_or_else(|| "127.0.0.1:8080".parse().expect("default addr"));
        let target_domain = vars
            .get("POLYMARKET_OPS_TARGET_DOMAIN")
            .map(|value| AccountDomain::from_str(value))
            .transpose()?
            .unwrap_or(AccountDomain::Sim);
        let requested_visible = vars
            .get("POLYMARKET_OPS_VISIBLE_DOMAINS")
            .map(|value| parse_domain_csv(value))
            .transpose()?;
        let mut visible_domains = requested_visible.unwrap_or_else(|| vec![target_domain]);
        if !visible_domains.contains(&target_domain) {
            visible_domains.push(target_domain);
        }
        visible_domains.sort_unstable_by_key(|domain| domain.as_str().to_owned());
        visible_domains.dedup();
        for domain in &visible_domains {
            ensure!(
                registry.domains.contains_key(domain),
                "ops-api visible domain `{domain}` is not present in the domain registry"
            );
        }
        let target_domain_config = registry
            .domains
            .get(&target_domain)
            .cloned()
            .with_context(|| format!("missing target domain config for `{target_domain}`"))?;
        let auth_tokens = vars
            .get("POLYMARKET_OPS_AUTH_TOKENS_JSON")
            .map(|raw| {
                serde_json::from_str::<BTreeMap<String, String>>(raw)
                    .context("failed to parse POLYMARKET_OPS_AUTH_TOKENS_JSON as JSON object")
            })
            .transpose()?
            .unwrap_or_default();
        for (operator, token) in &auth_tokens {
            ensure!(
                !operator.trim().is_empty(),
                "POLYMARKET_OPS_AUTH_TOKENS_JSON contains an empty operator name"
            );
            ensure!(
                !token.trim().is_empty(),
                "POLYMARKET_OPS_AUTH_TOKENS_JSON contains an empty token for operator `{operator}`"
            );
        }
        let legacy_auth_token = vars.get("POLYMARKET_OPS_AUTH_TOKEN").cloned();
        ensure!(
            !auth_tokens.is_empty() || legacy_auth_token.is_some(),
            "ops-api requires POLYMARKET_OPS_AUTH_TOKENS_JSON or POLYMARKET_OPS_AUTH_TOKEN"
        );
        Ok(Self {
            shared,
            bind_addr,
            auth_tokens,
            auth_token: legacy_auth_token,
            visible_domains,
            target_domain,
            target_domain_config,
            monitoring: MonitoringConfig {
                poll_interval: parse_duration_millis(vars.get("POLYMARKET_MONITOR_POLL_MS"), 5_000)?,
                alert_debounce: parse_duration_millis(
                    vars.get("POLYMARKET_MONITOR_ALERT_DEBOUNCE_MS"),
                    15_000,
                )?,
                heartbeat_degraded_ms: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_HEARTBEAT_DEGRADED_MS",
                    12_000,
                )?,
                heartbeat_safe_ms: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_HEARTBEAT_SAFE_MS",
                    30_000,
                )?,
                market_ws_lag_degraded_ms: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_MARKET_WS_LAG_DEGRADED_MS",
                    2_500,
                )?,
                market_ws_lag_safe_ms: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_MARKET_WS_LAG_SAFE_MS",
                    10_000,
                )?,
                reject_rate_degraded_bps: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_REJECT_RATE_DEGRADED_BPS",
                    1_000,
                )?,
                reject_rate_safe_bps: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_REJECT_RATE_SAFE_BPS",
                    2_500,
                )?,
                disputed_capital_safe_bps: parse_u64(
                    vars,
                    "POLYMARKET_MONITOR_DISPUTED_CAPITAL_SAFE_BPS",
                    2_000,
                )?,
            },
            alerting: AlertingConfig {
                webhook_url: vars.get("POLYMARKET_ALERT_WEBHOOK_URL").cloned(),
                webhook_timeout: parse_duration_millis(
                    vars.get("POLYMARKET_ALERT_WEBHOOK_TIMEOUT_MS"),
                    3_000,
                )?,
                webhook_retries: parse_usize(
                    vars.get("POLYMARKET_ALERT_WEBHOOK_RETRIES"),
                    3,
                )?,
                prometheus_enabled: parse_bool(
                    vars,
                    "POLYMARKET_PROMETHEUS_ENABLED",
                    true,
                )?,
            },
            telemetry: TelemetryConfig {
                trace_sample_ratio: parse_f64(vars, "POLYMARKET_TRACE_SAMPLE_RATIO", 1.0)?,
            },
            rollout: RolloutConfig::from_map(vars)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesEngineConfig {
    pub batch_window: Duration,
    pub graph_rebuild_debounce: Duration,
    pub replay_batch_size: usize,
    pub max_rules_text_len: usize,
    pub max_clarification_history: usize,
    pub confidence_threshold_bps: u32,
    pub concurrency: usize,
}

impl RulesEngineConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        Ok(Self {
            batch_window: parse_duration_millis(vars.get("POLYMARKET_RULES_BATCH_WINDOW_MS"), 750)?,
            graph_rebuild_debounce: parse_duration_millis(
                vars.get("POLYMARKET_RULES_GRAPH_REBUILD_DEBOUNCE_MS"),
                2_000,
            )?,
            replay_batch_size: parse_usize(vars.get("POLYMARKET_RULES_REPLAY_BATCH_SIZE"), 250)?,
            max_rules_text_len: parse_usize(vars.get("POLYMARKET_RULES_MAX_TEXT_LEN"), 64_000)?,
            max_clarification_history: parse_usize(
                vars.get("POLYMARKET_RULES_MAX_CLARIFICATIONS"),
                32,
            )?,
            confidence_threshold_bps: parse_u32(
                vars.get("POLYMARKET_RULES_CONFIDENCE_THRESHOLD_BPS"),
                5_000,
            )?,
            concurrency: parse_usize(vars.get("POLYMARKET_RULES_CONCURRENCY"), 4)?,
        })
    }
}

fn parse_duration_millis(value: Option<&String>, default_ms: u64) -> Result<Duration> {
    Ok(Duration::from_millis(match value {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("failed to parse `{value}` as u64 milliseconds"))?,
        None => default_ms,
    }))
}

fn parse_usize(value: Option<&String>, default_value: usize) -> Result<usize> {
    match value {
        Some(value) => value
            .parse::<usize>()
            .with_context(|| format!("failed to parse `{value}` as usize")),
        None => Ok(default_value),
    }
}

fn parse_u32(value: Option<&String>, default_value: u32) -> Result<u32> {
    match value {
        Some(value) => value
            .parse::<u32>()
            .with_context(|| format!("failed to parse `{value}` as u32")),
        None => Ok(default_value),
    }
}

fn parse_i32(value: Option<&String>, default_value: i32) -> Result<i32> {
    match value {
        Some(value) => value
            .parse::<i32>()
            .with_context(|| format!("failed to parse `{value}` as i32")),
        None => Ok(default_value),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpportunityEngineConfig {
    pub batch_window: Duration,
    pub scan_batch_size: usize,
    pub book_stale_warn_ms: u64,
    pub book_stale_stop_ms: u64,
    pub min_edge_net_bps: i32,
    pub max_market_refs_per_candidate: usize,
    pub max_candidates_per_event: usize,
    pub max_candidates_total: usize,
    pub fee_mm_enabled: bool,
    pub shadow_only_strategies: Vec<String>,
    pub default_half_life_secs: u32,
    pub max_book_age_for_multileg_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEngineConfig {
    pub api_base_url: String,
    pub heartbeat_interval: Duration,
    pub submit_timeout: Duration,
    pub cancel_timeout: Duration,
    pub retry_budget: usize,
    pub venue_restart_backoff: Duration,
    pub max_concurrent_intents: usize,
    pub reconcile_interval: Duration,
    pub stale_order_threshold: Duration,
    pub emergency_cancel_on_shutdown: bool,
    pub heartbeat_failure_cancel_threshold: u32,
}

impl ExecutionEngineConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        Ok(Self {
            api_base_url: vars
                .get("POLYMARKET_EXECUTION_API_BASE_URL")
                .cloned()
                .unwrap_or_else(|| "https://clob.polymarket.com".to_owned()),
            heartbeat_interval: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_HEARTBEAT_INTERVAL_MS"),
                5_000,
            )?,
            submit_timeout: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_SUBMIT_TIMEOUT_MS"),
                4_000,
            )?,
            cancel_timeout: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_CANCEL_TIMEOUT_MS"),
                4_000,
            )?,
            retry_budget: parse_usize(vars.get("POLYMARKET_EXECUTION_RETRY_BUDGET"), 3)?,
            venue_restart_backoff: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_425_BACKOFF_MS"),
                10_000,
            )?,
            max_concurrent_intents: parse_usize(
                vars.get("POLYMARKET_EXECUTION_MAX_CONCURRENT_INTENTS"),
                16,
            )?,
            reconcile_interval: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_RECONCILE_INTERVAL_MS"),
                30_000,
            )?,
            stale_order_threshold: parse_duration_millis(
                vars.get("POLYMARKET_EXECUTION_STALE_ORDER_MS"),
                120_000,
            )?,
            emergency_cancel_on_shutdown: parse_bool(
                vars,
                "POLYMARKET_EXECUTION_EMERGENCY_CANCEL_ON_SHUTDOWN",
                true,
            )?,
            heartbeat_failure_cancel_threshold: parse_u32(
                vars.get("POLYMARKET_EXECUTION_HEARTBEAT_FAILURE_CANCEL_THRESHOLD"),
                3,
            )?,
        })
    }
}

impl MdGatewayConfig {
    pub fn from_env(domain: &DomainConfig) -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(domain, &vars)
    }

    pub fn from_map(domain: &DomainConfig, vars: &BTreeMap<String, String>) -> Result<Self> {
        let mode = vars
            .get("POLYMARKET_MD_GATEWAY_MODE")
            .map(|value| MdGatewayMode::from_str(value))
            .transpose()?
            .unwrap_or(MdGatewayMode::Realtime);
        Ok(Self {
            mode,
            replay_input_path: PathBuf::from(
                vars.get("POLYMARKET_MD_GATEWAY_INPUT_PATH")
                    .cloned()
                    .unwrap_or_else(|| {
                        domain
                            .data_dir
                            .join("ingest")
                            .join("market-data.jsonl")
                            .display()
                            .to_string()
                    }),
            ),
            replay_poll_interval: parse_duration_millis(
                vars.get("POLYMARKET_MD_GATEWAY_POLL_MS"),
                500,
            )?,
            market_ws_url: vars
                .get("POLYMARKET_MD_GATEWAY_MARKET_WS_URL")
                .cloned()
                .unwrap_or_else(|| {
                    "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_owned()
                }),
            user_ws_url: vars
                .get("POLYMARKET_MD_GATEWAY_USER_WS_URL")
                .cloned()
                .unwrap_or_else(|| {
                    "wss://ws-subscriptions-clob.polymarket.com/ws/user".to_owned()
                }),
            metadata_api_base_url: vars
                .get("POLYMARKET_MD_GATEWAY_METADATA_API_BASE_URL")
                .cloned()
                .unwrap_or_else(|| "https://gamma-api.polymarket.com".to_owned()),
            metadata_refresh_interval: parse_duration_millis(
                vars.get("POLYMARKET_MD_GATEWAY_METADATA_REFRESH_MS"),
                60_000,
            )?,
            reconnect_backoff: parse_duration_millis(
                vars.get("POLYMARKET_MD_GATEWAY_RECONNECT_BACKOFF_MS"),
                5_000,
            )?,
            heartbeat_timeout: parse_duration_millis(
                vars.get("POLYMARKET_MD_GATEWAY_HEARTBEAT_TIMEOUT_MS"),
                30_000,
            )?,
            market_subscription_chunk_size: parse_usize(
                vars.get("POLYMARKET_MD_GATEWAY_MARKET_SUBSCRIPTION_CHUNK"),
                250,
            )?
            .max(1),
        })
    }
}

impl OpportunityEngineConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        Ok(Self {
            batch_window: parse_duration_millis(
                vars.get("POLYMARKET_OPPORTUNITY_BATCH_WINDOW_MS"),
                1_000,
            )?,
            scan_batch_size: parse_usize(vars.get("POLYMARKET_OPPORTUNITY_SCAN_BATCH_SIZE"), 250)?,
            book_stale_warn_ms: parse_u64(
                vars,
                "POLYMARKET_OPPORTUNITY_BOOK_STALE_WARN_MS",
                3_000,
            )?,
            book_stale_stop_ms: parse_u64(
                vars,
                "POLYMARKET_OPPORTUNITY_BOOK_STALE_STOP_MS",
                10_000,
            )?,
            min_edge_net_bps: parse_i32(vars.get("POLYMARKET_OPPORTUNITY_MIN_EDGE_NET_BPS"), 15)?,
            max_market_refs_per_candidate: parse_usize(
                vars.get("POLYMARKET_OPPORTUNITY_MAX_MARKET_REFS"),
                8,
            )?,
            max_candidates_per_event: parse_usize(
                vars.get("POLYMARKET_OPPORTUNITY_MAX_CANDIDATES_PER_EVENT"),
                64,
            )?,
            max_candidates_total: parse_usize(
                vars.get("POLYMARKET_OPPORTUNITY_MAX_CANDIDATES_TOTAL"),
                512,
            )?,
            fee_mm_enabled: parse_bool(vars, "POLYMARKET_OPPORTUNITY_FEE_MM_ENABLED", true)?,
            shadow_only_strategies: vars
                .get("POLYMARKET_OPPORTUNITY_SHADOW_ONLY_STRATEGIES")
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(ToOwned::to_owned)
                        .collect()
                })
                .unwrap_or_else(|| vec!["FEE_AWARE_MM".to_owned()]),
            default_half_life_secs: parse_u32(
                vars.get("POLYMARKET_OPPORTUNITY_DEFAULT_HALF_LIFE_SECS"),
                900,
            )?,
            max_book_age_for_multileg_ms: parse_u64(
                vars,
                "POLYMARKET_OPPORTUNITY_MAX_BOOK_AGE_FOR_MULTILEG_MS",
                2_000,
            )?,
        })
    }
}

struct DomainConfigBuilder<'a> {
    shared: &'a SharedConfig,
    vars: &'a BTreeMap<String, String>,
    descriptor: AccountDomainDescriptor,
}

impl<'a> DomainConfigBuilder<'a> {
    fn new(
        shared: &'a SharedConfig,
        vars: &'a BTreeMap<String, String>,
        descriptor: AccountDomainDescriptor,
    ) -> Self {
        Self {
            shared,
            vars,
            descriptor,
        }
    }

    fn build(&self) -> Result<DomainConfig> {
        let prefix = format!("POLYMARKET_DOMAIN_{}", self.descriptor.domain.as_str());
        let namespace = self.descriptor.namespace.clone();
        let data_dir = self
            .vars
            .get(&format!("{prefix}_DATA_DIR"))
            .map(PathBuf::from)
            .unwrap_or_else(|| self.shared.data_root.join(&namespace));
        let database_path = self
            .vars
            .get(&format!("{prefix}_DATABASE_PATH"))
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join(format!("polymarket-{namespace}.sqlite")));
        let audit_prefix = self
            .vars
            .get(&format!("{prefix}_AUDIT_PREFIX"))
            .cloned()
            .unwrap_or_else(|| self.descriptor.audit_label.clone());
        let wallet_id = self
            .vars
            .get(&format!("{prefix}_WALLET_ID"))
            .cloned()
            .filter(|value| !value.trim().is_empty());
        let credential_alias = self
            .vars
            .get(&format!("{prefix}_CREDENTIAL_ALIAS"))
            .cloned()
            .filter(|value| !value.trim().is_empty());
        let l2_credentials = parse_credential_source(
            self.vars,
            &format!("{prefix}_L2_CREDENTIALS_ENV"),
            &format!("{prefix}_L2_CREDENTIALS_FILE"),
        )?;
        let signer = parse_credential_source(
            self.vars,
            &format!("{prefix}_SIGNER_ENV"),
            &format!("{prefix}_SIGNER_FILE"),
        )?;
        let execution_approved = parse_bool(self.vars, &format!("{prefix}_ALLOW_EXECUTE"), false)?;
        let allowed_runtime_modes = self
            .vars
            .get(&format!("{prefix}_ALLOWED_RUNTIME_MODES"))
            .map(|value| parse_runtime_modes_csv(value))
            .transpose()?
            .unwrap_or_else(|| self.descriptor.allowed_runtime_modes.clone());
        let default_runtime_mode = self
            .vars
            .get(&format!("{prefix}_DEFAULT_RUNTIME_MODE"))
            .map(|value| RuntimeMode::from_str(value))
            .transpose()?
            .unwrap_or(self.descriptor.default_runtime_mode);
        let runtime_mode = self
            .vars
            .get(&format!("{prefix}_RUNTIME_MODE"))
            .map(|value| RuntimeMode::from_str(value))
            .transpose()?
            .unwrap_or(default_runtime_mode);

        let mut descriptor = self.descriptor.clone();
        descriptor.allowed_runtime_modes = allowed_runtime_modes;
        descriptor.default_runtime_mode = default_runtime_mode;

        let config = DomainConfig {
            descriptor,
            data_dir,
            database_path,
            audit_prefix,
            wallet_id,
            credential_alias,
            l2_credentials,
            signer,
            execution_approved,
            runtime_mode,
        };
        config.validate(self.shared)?;
        Ok(config)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskEngineConfig {
    pub evaluation_interval: Duration,
    pub max_batch_notional: f64,
    pub min_order_size: f64,
    pub tick_size: f64,
    pub default_gtd_secs: u64,
    pub market_ws_lag_degraded_ms: u64,
    pub market_ws_lag_safe_ms: u64,
    pub heartbeat_degraded_ms: u64,
    pub heartbeat_safe_ms: u64,
    pub reject_rate_degraded: f64,
    pub reject_rate_safe: f64,
    pub recent_425_degraded: u32,
    pub recent_425_safe: u32,
    pub clarification_cooldown_secs: u64,
    pub rule_change_cooldown_secs: u64,
    pub resolution_hazard_window_secs: u64,
    pub book_depth_reduce_threshold: f64,
    pub book_depth_reject_threshold: f64,
    pub fee_bps_soft_limit: i32,
    pub fee_bps_hard_limit: i32,
    pub daily_loss_limit_ratio: f64,
    pub rolling_drawdown_limit_ratio: f64,
    pub disputed_capital_limit_ratio: f64,
    pub unresolved_capital_limit_ratio: f64,
    pub market_cap_ratio: f64,
    pub event_cap_ratio: f64,
    pub category_cap_ratio: f64,
    pub min_cash_buffer_ratio: f64,
    pub reduce_only_scale: f64,
}

impl RiskEngineConfig {
    pub fn from_env() -> Result<Self> {
        let mut vars = BTreeMap::new();
        for (key, value) in env::vars() {
            vars.insert(key, value);
        }
        Self::from_map(&vars)
    }

    pub fn from_map(vars: &BTreeMap<String, String>) -> Result<Self> {
        Ok(Self {
            evaluation_interval: parse_duration_millis(
                vars.get("POLYMARKET_RISK_EVALUATION_INTERVAL_MS"),
                500,
            )?,
            max_batch_notional: parse_f64(vars, "POLYMARKET_RISK_MAX_BATCH_NOTIONAL", 1_500.0)?,
            min_order_size: parse_f64(vars, "POLYMARKET_RISK_MIN_ORDER_SIZE", 5.0)?,
            tick_size: parse_f64(vars, "POLYMARKET_RISK_TICK_SIZE", 0.01)?,
            default_gtd_secs: parse_u64(vars, "POLYMARKET_RISK_DEFAULT_GTD_SECS", 300)?,
            market_ws_lag_degraded_ms: parse_u64(
                vars,
                "POLYMARKET_RISK_MARKET_WS_LAG_DEGRADED_MS",
                2_500,
            )?,
            market_ws_lag_safe_ms: parse_u64(vars, "POLYMARKET_RISK_MARKET_WS_LAG_SAFE_MS", 5_000)?,
            heartbeat_degraded_ms: parse_u64(vars, "POLYMARKET_RISK_HEARTBEAT_DEGRADED_MS", 8_000)?,
            heartbeat_safe_ms: parse_u64(vars, "POLYMARKET_RISK_HEARTBEAT_SAFE_MS", 12_000)?,
            reject_rate_degraded: parse_f64(vars, "POLYMARKET_RISK_REJECT_RATE_DEGRADED", 0.10)?,
            reject_rate_safe: parse_f64(vars, "POLYMARKET_RISK_REJECT_RATE_SAFE", 0.20)?,
            recent_425_degraded: parse_u32(vars.get("POLYMARKET_RISK_RECENT_425_DEGRADED"), 3)?,
            recent_425_safe: parse_u32(vars.get("POLYMARKET_RISK_RECENT_425_SAFE"), 6)?,
            clarification_cooldown_secs: parse_u64(
                vars,
                "POLYMARKET_RISK_CLARIFICATION_COOLDOWN_SECS",
                900,
            )?,
            rule_change_cooldown_secs: parse_u64(
                vars,
                "POLYMARKET_RISK_RULE_CHANGE_COOLDOWN_SECS",
                900,
            )?,
            resolution_hazard_window_secs: parse_u64(
                vars,
                "POLYMARKET_RISK_RESOLUTION_HAZARD_WINDOW_SECS",
                1_800,
            )?,
            book_depth_reduce_threshold: parse_f64(
                vars,
                "POLYMARKET_RISK_BOOK_DEPTH_REDUCE_THRESHOLD",
                2.0,
            )?,
            book_depth_reject_threshold: parse_f64(
                vars,
                "POLYMARKET_RISK_BOOK_DEPTH_REJECT_THRESHOLD",
                1.0,
            )?,
            fee_bps_soft_limit: parse_i32(vars.get("POLYMARKET_RISK_FEE_BPS_SOFT_LIMIT"), 120)?,
            fee_bps_hard_limit: parse_i32(vars.get("POLYMARKET_RISK_FEE_BPS_HARD_LIMIT"), 200)?,
            daily_loss_limit_ratio: parse_f64(
                vars,
                "POLYMARKET_RISK_DAILY_LOSS_LIMIT_RATIO",
                0.015,
            )?,
            rolling_drawdown_limit_ratio: parse_f64(
                vars,
                "POLYMARKET_RISK_ROLLING_DRAWDOWN_LIMIT_RATIO",
                0.03,
            )?,
            disputed_capital_limit_ratio: parse_f64(
                vars,
                "POLYMARKET_RISK_DISPUTED_CAPITAL_LIMIT_RATIO",
                0.10,
            )?,
            unresolved_capital_limit_ratio: parse_f64(
                vars,
                "POLYMARKET_RISK_UNRESOLVED_CAPITAL_LIMIT_RATIO",
                0.20,
            )?,
            market_cap_ratio: parse_f64(vars, "POLYMARKET_RISK_MARKET_CAP_RATIO", 0.0035)?,
            event_cap_ratio: parse_f64(vars, "POLYMARKET_RISK_EVENT_CAP_RATIO", 0.025)?,
            category_cap_ratio: parse_f64(vars, "POLYMARKET_RISK_CATEGORY_CAP_RATIO", 0.15)?,
            min_cash_buffer_ratio: parse_f64(vars, "POLYMARKET_RISK_MIN_CASH_BUFFER_RATIO", 0.25)?,
            reduce_only_scale: parse_f64(vars, "POLYMARKET_RISK_REDUCE_ONLY_SCALE", 0.35)?,
        })
    }
}

fn parse_credential_source(
    vars: &BTreeMap<String, String>,
    env_key: &str,
    file_key: &str,
) -> Result<CredentialSource> {
    let env_var = vars.get(env_key).filter(|value| !value.trim().is_empty());
    let file_path = vars.get(file_key).filter(|value| !value.trim().is_empty());
    match (env_var, file_path) {
        (Some(_), Some(_)) => {
            bail!("credential source keys `{env_key}` and `{file_key}` are mutually exclusive")
        }
        (Some(variable), None) => Ok(CredentialSource::EnvVar {
            variable: variable.clone(),
        }),
        (None, Some(path)) => Ok(CredentialSource::FilePath {
            path: PathBuf::from(path),
        }),
        (None, None) => Ok(CredentialSource::Unconfigured),
    }
}

fn parse_runtime_modes_csv(value: &str) -> Result<Vec<RuntimeMode>> {
    let mut seen = BTreeSet::new();
    let mut modes = Vec::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mode = RuntimeMode::from_str(trimmed)?;
        if seen.insert(mode.as_str().to_owned()) {
            modes.push(mode);
        }
    }
    ensure!(
        !modes.is_empty(),
        "runtime mode CSV must contain at least one mode"
    );
    Ok(modes)
}

fn parse_domain_csv(value: &str) -> Result<Vec<AccountDomain>> {
    let mut domains = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let domain = AccountDomain::from_str(trimmed)?;
        if seen.insert(domain.as_str().to_owned()) {
            domains.push(domain);
        }
    }
    ensure!(
        !domains.is_empty(),
        "domain CSV must contain at least one domain"
    );
    Ok(domains)
}

fn parse_strategy_csv(value: &str) -> Result<Vec<StrategyKind>> {
    let mut strategies = Vec::new();
    let mut seen = BTreeSet::new();
    for raw in value.split(',') {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let strategy = StrategyKind::from_str(trimmed)?;
        if seen.insert(strategy.as_str().to_owned()) {
            strategies.push(strategy);
        }
    }
    ensure!(
        !strategies.is_empty(),
        "strategy CSV must contain at least one strategy"
    );
    Ok(strategies)
}

fn rollout_policy_from_map(
    vars: &BTreeMap<String, String>,
    stage: PromotionStage,
) -> Result<RolloutPolicy> {
    let stage_key = stage.as_str();
    let allow_strategies_key = format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOWED_STRATEGIES");
    let max_total_key = format!("POLYMARKET_ROLLOUT_{stage_key}_MAX_TOTAL_NOTIONAL");
    let max_batch_key = format!("POLYMARKET_ROLLOUT_{stage_key}_MAX_BATCH_NOTIONAL");
    let max_strategy_key = format!("POLYMARKET_ROLLOUT_{stage_key}_MAX_STRATEGY_NOTIONAL");
    let allow_real_execution = parse_bool(
        vars,
        &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_REAL_EXECUTION"),
        matches!(stage, PromotionStage::Canary | PromotionStage::Live),
    )?;
    let allow_multileg = parse_bool(
        vars,
        &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_MULTILEG"),
        matches!(stage, PromotionStage::Live),
    )?;
    let allow_market_making = parse_bool(
        vars,
        &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_MARKET_MAKING"),
        false,
    )?;
    let allow_auto_redemption = parse_bool(
        vars,
        &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_AUTO_REDEMPTION"),
        !matches!(stage, PromotionStage::Replay),
    )?;
    let require_shadow_alignment = parse_bool(
        vars,
        &format!("POLYMARKET_ROLLOUT_{stage_key}_REQUIRE_SHADOW_ALIGNMENT"),
        !matches!(stage, PromotionStage::Replay),
    )?;
    let allowed_strategies = vars
        .get(&allow_strategies_key)
        .map(|raw| parse_strategy_csv(raw))
        .transpose()?
        .unwrap_or_else(|| default_stage_strategies(stage));
    let mvp_flags = MvpCapabilityFlags {
        directional_positions_allowed: parse_bool(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_DIRECTIONAL_POSITIONS"),
            false,
        )?,
        sports_latency_trading_allowed: parse_bool(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_SPORTS_LATENCY"),
            false,
        )?,
        broad_market_making_allowed: parse_bool(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_BROAD_MARKET_MAKING"),
            false,
        )?,
        human_trade_approval_allowed: parse_bool(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_HUMAN_TRADE_APPROVAL"),
            false,
        )?,
        model_driven_execution_allowed: parse_bool(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_ALLOW_MODEL_DRIVEN_EXECUTION"),
            false,
        )?,
    };

    Ok(RolloutPolicy {
        stage,
        domain: stage.domain(),
        allowed_strategies,
        max_total_notional: parse_f64(vars, &max_total_key, default_stage_total_notional(stage))?,
        max_batch_notional: parse_f64(vars, &max_batch_key, default_stage_batch_notional(stage))?,
        max_strategy_notional: parse_f64(
            vars,
            &max_strategy_key,
            default_stage_strategy_notional(stage),
        )?,
        capabilities: RolloutCapabilityMatrix {
            allow_real_execution,
            allow_multileg,
            allow_market_making,
            allow_auto_redemption,
            require_shadow_alignment,
        },
        mvp_flags,
    })
}

fn rollout_thresholds_from_map(
    vars: &BTreeMap<String, String>,
    stage: PromotionStage,
) -> Result<RolloutThresholds> {
    let stage_key = stage.as_str();
    Ok(RolloutThresholds {
        stable_window: parse_duration_millis(
            vars.get(&format!("POLYMARKET_ROLLOUT_{stage_key}_STABLE_WINDOW_MS")),
            default_stage_stable_window_ms(stage),
        )?,
        candidate_ttl: parse_duration_millis(
            vars.get(&format!("POLYMARKET_ROLLOUT_{stage_key}_CANDIDATE_TTL_MS")),
            300_000,
        )?,
        heartbeat_max_ms: parse_u64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_HEARTBEAT_MAX_MS"),
            default_stage_heartbeat_max(stage),
        )?,
        reject_rate_max: parse_f64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_REJECT_RATE_MAX"),
            default_stage_reject_rate_max(stage),
        )?,
        fill_rate_min: parse_f64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_FILL_RATE_MIN"),
            default_stage_fill_rate_min(stage),
        )?,
        market_ws_lag_max_ms: parse_u64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_MARKET_WS_LAG_MAX_MS"),
            default_stage_market_ws_max(stage),
        )?,
        disputed_capital_ratio_max: parse_f64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_DISPUTED_CAPITAL_RATIO_MAX"),
            default_stage_disputed_capital_max(stage),
        )?,
        max_open_critical_alerts: parse_usize(
            vars.get(&format!("POLYMARKET_ROLLOUT_{stage_key}_MAX_OPEN_CRITICAL_ALERTS")),
            0,
        )?,
        max_shadow_live_drift_bps: parse_f64(
            vars,
            &format!("POLYMARKET_ROLLOUT_{stage_key}_MAX_SHADOW_LIVE_DRIFT_BPS"),
            default_stage_shadow_drift_bps(stage),
        )?,
    })
}

fn validate_rollout_policy(policy: &RolloutPolicy) -> Result<()> {
    ensure!(
        !policy.allowed_strategies.is_empty(),
        "rollout policy `{}` must allow at least one strategy",
        policy.stage
    );
    ensure!(
        policy.max_total_notional > 0.0,
        "rollout policy `{}` max_total_notional must be positive",
        policy.stage
    );
    ensure!(
        policy.max_batch_notional > 0.0,
        "rollout policy `{}` max_batch_notional must be positive",
        policy.stage
    );
    ensure!(
        policy.max_strategy_notional > 0.0,
        "rollout policy `{}` max_strategy_notional must be positive",
        policy.stage
    );
    ensure!(
        policy.max_total_notional >= policy.max_batch_notional,
        "rollout policy `{}` total notional must be >= batch notional",
        policy.stage
    );
    ensure!(
        policy.max_batch_notional >= policy.max_strategy_notional,
        "rollout policy `{}` batch notional must be >= strategy notional",
        policy.stage
    );
    if matches!(policy.stage, PromotionStage::Replay | PromotionStage::Shadow) {
        ensure!(
            !policy.capabilities.allow_real_execution,
            "rollout policy `{}` cannot allow real execution",
            policy.stage
        );
    }
    if policy.capabilities.allow_market_making {
        ensure!(
            policy.allowed_strategies.contains(&StrategyKind::FeeAwareMM),
            "rollout policy `{}` enables market making but does not allow FEE_AWARE_MM",
            policy.stage
        );
    }
    Ok(())
}

fn default_stage_strategies(stage: PromotionStage) -> Vec<StrategyKind> {
    match stage {
        PromotionStage::Replay => StrategyKind::ALL.to_vec(),
        PromotionStage::Shadow => StrategyKind::ALL.to_vec(),
        PromotionStage::Canary => vec![
            StrategyKind::Rebalancing,
            StrategyKind::DependencyArb,
            StrategyKind::NegRisk,
        ],
        PromotionStage::Live => vec![
            StrategyKind::Rebalancing,
            StrategyKind::DependencyArb,
            StrategyKind::NegRisk,
            StrategyKind::RulesDriven,
        ],
    }
}

fn default_stage_total_notional(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 10_000.0,
        PromotionStage::Shadow => 15_000.0,
        PromotionStage::Canary => 2_500.0,
        PromotionStage::Live => 25_000.0,
    }
}

fn default_stage_batch_notional(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 2_000.0,
        PromotionStage::Shadow => 2_500.0,
        PromotionStage::Canary => 500.0,
        PromotionStage::Live => 3_000.0,
    }
}

fn default_stage_strategy_notional(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 1_000.0,
        PromotionStage::Shadow => 1_200.0,
        PromotionStage::Canary => 250.0,
        PromotionStage::Live => 1_500.0,
    }
}

fn default_stage_stable_window_ms(stage: PromotionStage) -> u64 {
    match stage {
        PromotionStage::Replay => 60_000,
        PromotionStage::Shadow => 300_000,
        PromotionStage::Canary => 900_000,
        PromotionStage::Live => 1_800_000,
    }
}

fn default_stage_heartbeat_max(stage: PromotionStage) -> u64 {
    match stage {
        PromotionStage::Replay => 30_000,
        PromotionStage::Shadow => 15_000,
        PromotionStage::Canary => 10_000,
        PromotionStage::Live => 8_000,
    }
}

fn default_stage_reject_rate_max(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 0.20,
        PromotionStage::Shadow => 0.15,
        PromotionStage::Canary => 0.08,
        PromotionStage::Live => 0.05,
    }
}

fn default_stage_fill_rate_min(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 0.40,
        PromotionStage::Shadow => 0.55,
        PromotionStage::Canary => 0.70,
        PromotionStage::Live => 0.80,
    }
}

fn default_stage_market_ws_max(stage: PromotionStage) -> u64 {
    match stage {
        PromotionStage::Replay => 10_000,
        PromotionStage::Shadow => 5_000,
        PromotionStage::Canary => 2_500,
        PromotionStage::Live => 1_500,
    }
}

fn default_stage_disputed_capital_max(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 0.25,
        PromotionStage::Shadow => 0.15,
        PromotionStage::Canary => 0.08,
        PromotionStage::Live => 0.05,
    }
}

fn default_stage_shadow_drift_bps(stage: PromotionStage) -> f64 {
    match stage {
        PromotionStage::Replay => 500.0,
        PromotionStage::Shadow => 300.0,
        PromotionStage::Canary => 150.0,
        PromotionStage::Live => 100.0,
    }
}

fn parse_bool(vars: &BTreeMap<String, String>, key: &str, default: bool) -> Result<bool> {
    match vars.get(key) {
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => bail!("failed to parse `{key}` as bool"),
        },
        None => Ok(default),
    }
}

fn parse_f64(vars: &BTreeMap<String, String>, key: &str, default: f64) -> Result<f64> {
    match vars.get(key) {
        Some(value) => value
            .trim()
            .parse::<f64>()
            .with_context(|| format!("failed to parse `{key}` as f64")),
        None => Ok(default),
    }
}

fn parse_u64(vars: &BTreeMap<String, String>, key: &str, default: u64) -> Result<u64> {
    match vars.get(key) {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("failed to parse `{key}` as u64")),
        None => Ok(default),
    }
}

fn env_or(vars: &BTreeMap<String, String>, key: &str, default: &str) -> String {
    vars.get(key).cloned().unwrap_or_else(|| default.to_owned())
}

fn path_contains_namespace(path: &Path, namespace: &str) -> bool {
    path.components()
        .any(|component| component.as_os_str() == namespace)
}

fn file_name_contains_namespace(path: &Path, namespace: &str) -> bool {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase().contains(namespace))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_env(root: &Path) -> BTreeMap<String, String> {
        let mut vars = BTreeMap::new();
        vars.insert("POLYMARKET_ENV".to_owned(), "test".to_owned());
        vars.insert(
            "POLYMARKET_DATA_DIR".to_owned(),
            root.to_string_lossy().to_string(),
        );
        vars.insert("POLYMARKET_LOG_FILTER".to_owned(), "debug".to_owned());
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_WALLET_ID".to_owned(),
            "wallet-canary".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_CREDENTIAL_ALIAS".to_owned(),
            "cred-canary".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_L2_CREDENTIALS_ENV".to_owned(),
            "CANARY_L2".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_SIGNER_ENV".to_owned(),
            "CANARY_SIGNER".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_WALLET_ID".to_owned(),
            "wallet-live".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_CREDENTIAL_ALIAS".to_owned(),
            "cred-live".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_L2_CREDENTIALS_ENV".to_owned(),
            "LIVE_L2".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_SIGNER_ENV".to_owned(),
            "LIVE_SIGNER".to_owned(),
        );
        vars
    }

    #[test]
    fn shared_config_validates_defaults() {
        let vars = BTreeMap::new();
        let shared = SharedConfig::from_map("polymarket-node", &vars).expect("shared config");
        assert_eq!(shared.environment, "dev");
        assert_eq!(shared.data_root, PathBuf::from("./var"));
        assert_eq!(shared.heartbeat_interval.as_secs(), 5);
    }

    #[test]
    fn registry_builds_all_domains_with_isolated_paths() {
        let dir = tempfile::tempdir().expect("tempdir");
        let vars = base_env(dir.path());
        let shared = SharedConfig::from_map("polymarket-node", &vars).expect("shared");
        let registry = DomainRegistry::from_map(&shared, &vars).expect("registry");
        assert_eq!(registry.visible_domains(), AccountDomain::ALL.to_vec());
        let sim = registry.get(AccountDomain::Sim).expect("sim");
        let canary = registry.get(AccountDomain::Canary).expect("canary");
        let live = registry.get(AccountDomain::Live).expect("live");
        assert_ne!(sim.data_dir, canary.data_dir);
        assert_ne!(canary.data_dir, live.data_dir);
        assert!(sim.database_path.to_string_lossy().contains("sim"));
        assert!(canary.database_path.to_string_lossy().contains("canary"));
        assert!(live.database_path.to_string_lossy().contains("live"));
    }

    #[test]
    fn node_config_rejects_execute_for_sim() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert("POLYMARKET_NODE_DOMAIN".to_owned(), "SIM".to_owned());
        vars.insert(
            "POLYMARKET_NODE_RUNTIME_MODE".to_owned(),
            "EXECUTE".to_owned(),
        );
        let error = NodeConfig::from_map(AccountDomain::Sim, &vars).expect_err("must fail");
        assert!(error
            .to_string()
            .contains("runtime mode `EXECUTE` is not permitted"));
    }

    #[test]
    fn node_config_allows_canary_execute_when_explicitly_approved() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert("POLYMARKET_NODE_DOMAIN".to_owned(), "CANARY".to_owned());
        vars.insert(
            "POLYMARKET_NODE_RUNTIME_MODE".to_owned(),
            "EXECUTE".to_owned(),
        );
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_ALLOW_EXECUTE".to_owned(),
            "true".to_owned(),
        );
        let config = NodeConfig::from_map(AccountDomain::Sim, &vars).expect("node config");
        assert_eq!(config.selected_domain, AccountDomain::Canary);
        assert_eq!(
            config.selected_domain_config.runtime_mode,
            RuntimeMode::Execute
        );
    }

    #[test]
    fn live_cannot_default_to_execute_without_approval() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_DEFAULT_RUNTIME_MODE".to_owned(),
            "EXECUTE".to_owned(),
        );
        let shared = SharedConfig::from_map("polymarket-node", &vars).expect("shared");
        let error = DomainRegistry::from_map(&shared, &vars).expect_err("must fail");
        assert!(error
            .to_string()
            .contains("cannot enter EXECUTE without explicit execution approval"));
    }

    #[test]
    fn real_capital_domains_cannot_share_wallet_or_alias() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert(
            "POLYMARKET_DOMAIN_LIVE_WALLET_ID".to_owned(),
            "wallet-canary".to_owned(),
        );
        let shared = SharedConfig::from_map("polymarket-node", &vars).expect("shared");
        let error = DomainRegistry::from_map(&shared, &vars).expect_err("must fail");
        assert!(error
            .to_string()
            .contains("wallet_id `wallet-canary` conflicts"));
    }

    #[test]
    fn registry_rejects_paths_without_namespace() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert(
            "POLYMARKET_DOMAIN_CANARY_DATA_DIR".to_owned(),
            dir.path().join("shared").to_string_lossy().to_string(),
        );
        let shared = SharedConfig::from_map("polymarket-node", &vars).expect("shared");
        let error = DomainRegistry::from_map(&shared, &vars).expect_err("must fail");
        assert!(error
            .to_string()
            .contains("must include namespace `canary`"));
    }

    #[test]
    fn ops_api_config_keeps_visible_domains_read_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut vars = base_env(dir.path());
        vars.insert(
            "POLYMARKET_OPS_VISIBLE_DOMAINS".to_owned(),
            "SIM,CANARY".to_owned(),
        );
        vars.insert(
            "POLYMARKET_OPS_TARGET_DOMAIN".to_owned(),
            "CANARY".to_owned(),
        );
        vars.insert(
            "POLYMARKET_OPS_AUTH_TOKEN".to_owned(),
            "test-token".to_owned(),
        );
        let config = OpsApiConfig::from_map(&vars).expect("ops config");
        assert_eq!(config.target_domain, AccountDomain::Canary);
        assert_eq!(
            config.visible_domains,
            vec![AccountDomain::Canary, AccountDomain::Sim]
        );
    }

    #[test]
    fn rollout_config_defaults_are_valid() {
        let config = RolloutConfig::from_map(&BTreeMap::new()).expect("rollout config");
        let replay = config.policy(PromotionStage::Replay).expect("replay");
        assert_eq!(replay.domain, AccountDomain::Sim);
        assert!(!replay.capabilities.allow_real_execution);
        let canary = config.policy(PromotionStage::Canary).expect("canary");
        assert_eq!(canary.domain, AccountDomain::Canary);
        assert!(canary.capabilities.allow_real_execution);
        assert!(!canary
            .mvp_flags
            .broad_market_making_allowed);
    }

    #[test]
    fn rollout_config_rejects_invalid_shadow_execution_override() {
        let mut vars = BTreeMap::new();
        vars.insert(
            "POLYMARKET_ROLLOUT_SHADOW_ALLOW_REAL_EXECUTION".to_owned(),
            "true".to_owned(),
        );
        let error = RolloutConfig::from_map(&vars).expect_err("must fail");
        assert!(error
            .to_string()
            .contains("cannot allow real execution"));
    }
}
