use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use anyhow::{anyhow, bail, ensure, Context, Result};
use polymarket_core::{
    now, AccountDomain, AlertEvent, AlertRuleKind, AlertSeverity, AlertStatus, AuditEvent,
    ConstraintEdge, ConstraintGraphSnapshot, DurableEvent, EventFamilySnapshot,
    ExecutionIntentRecord, ExecutionIntentStatus, GraphScope, HealthMetricsSummary,
    HealthSnapshot, IdempotencyClaimResult, IdempotencyKeyRecord, IdempotencyStatus,
    MarketCanonical, MarketSnapshot, MetricSample, NewDurableEvent, NewIdempotencyKey,
    NewOrderLifecycleRecord, NewReplayCheckpoint, NewSimEvent, NewSimFillRecord, NewSimOrderRecord,
    NewStateSnapshot, OpportunityCandidate, OpportunityInvalidation, OrderLifecycleRecord,
    OrderLifecycleStatus, PromotionCandidate, PromotionStage, RecoveryState, ReplayCheckpoint,
    ReplayCursor, ReplayJob, ReplayJobStatus, ReplayReport, ReplayRequest, ReplayTrace,
    RolloutEvaluation, RolloutIncident, RolloutPolicy,
    RolloutStageRecord, RuleVersion, RuntimeHealth, RuntimeMode, RuntimeModeRecord,
    ScannerRunReport, ServiceHeartbeat, ServiceKind, SimEventRecord, SimFillRecord, SimMode,
    SimOrderRecord, SimRunReport,
    StateSnapshot, StrategyKind, TradeIntent, TradeSide,
};
use rusqlite::types::Type;
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, Row, TransactionBehavior};
use serde_json::Value;
use uuid::Uuid;

const SQLITE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RECOVERY_EVENT_LIMIT: usize = 200;
const DEFAULT_RECOVERY_SNAPSHOT_LIMIT: usize = 64;
const DEFAULT_RECOVERY_AUDIT_LIMIT: usize = 50;

const MIGRATIONS: &[(i64, &str)] = &[
    (
        1,
        r#"
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS domain_metadata (
            owner_domain TEXT PRIMARY KEY,
            namespace TEXT NOT NULL,
            audit_label TEXT NOT NULL,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS runtime_modes (
            domain TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            reason TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS service_heartbeats (
            service TEXT NOT NULL,
            domain TEXT NOT NULL,
            healthy INTEGER NOT NULL,
            detail TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            PRIMARY KEY (service, domain)
        );

        CREATE TABLE IF NOT EXISTS audit_events (
            id TEXT PRIMARY KEY,
            occurred_at TEXT NOT NULL,
            service TEXT NOT NULL,
            domain TEXT,
            action TEXT NOT NULL,
            detail TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_audit_events_occurred_at
        ON audit_events (occurred_at DESC);

        CREATE INDEX IF NOT EXISTS idx_audit_events_domain_occurred_at
        ON audit_events (domain, occurred_at DESC);
        "#,
    ),
    (
        2,
        r#"
        CREATE TABLE IF NOT EXISTS durable_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            domain TEXT NOT NULL,
            stream TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            causation_id TEXT,
            correlation_id TEXT,
            idempotency_key TEXT,
            payload_json TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            persisted_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_durable_events_domain_sequence
        ON durable_events (domain, sequence DESC);

        CREATE INDEX IF NOT EXISTS idx_durable_events_stream_sequence
        ON durable_events (stream, sequence DESC);

        CREATE INDEX IF NOT EXISTS idx_durable_events_aggregate
        ON durable_events (domain, aggregate_type, aggregate_id, sequence DESC);

        CREATE INDEX IF NOT EXISTS idx_durable_events_idempotency
        ON durable_events (domain, idempotency_key);

        CREATE TABLE IF NOT EXISTS state_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            version INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            derived_from_sequence INTEGER,
            created_at TEXT NOT NULL,
            UNIQUE(domain, aggregate_type, aggregate_id)
        );

        CREATE INDEX IF NOT EXISTS idx_state_snapshots_created_at
        ON state_snapshots (domain, created_at DESC);

        CREATE TABLE IF NOT EXISTS order_lifecycle (
            domain TEXT NOT NULL,
            order_id TEXT NOT NULL,
            market_id TEXT NOT NULL,
            status TEXT NOT NULL,
            client_order_id TEXT,
            external_order_id TEXT,
            idempotency_key TEXT,
            side TEXT,
            limit_price REAL,
            order_quantity REAL,
            filled_quantity REAL NOT NULL,
            average_fill_price REAL,
            last_event_sequence INTEGER,
            detail_json TEXT NOT NULL,
            opened_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            closed_at TEXT,
            PRIMARY KEY (domain, order_id)
        );

        CREATE INDEX IF NOT EXISTS idx_order_lifecycle_market_status
        ON order_lifecycle (domain, market_id, status, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_order_lifecycle_open_orders
        ON order_lifecycle (domain, closed_at, updated_at DESC);

        CREATE TABLE IF NOT EXISTS idempotency_keys (
            domain TEXT NOT NULL,
            scope TEXT NOT NULL,
            key TEXT NOT NULL,
            request_hash TEXT NOT NULL,
            status TEXT NOT NULL,
            response_json TEXT,
            created_by TEXT NOT NULL,
            created_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            lock_expires_at TEXT,
            PRIMARY KEY (domain, scope, key)
        );

        CREATE INDEX IF NOT EXISTS idx_idempotency_status
        ON idempotency_keys (domain, status, last_seen_at DESC);
        "#,
    ),
    (
        3,
        r#"
        CREATE TABLE IF NOT EXISTS execution_intents (
            intent_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            batch_id TEXT,
            strategy_kind TEXT NOT NULL,
            market_id TEXT NOT NULL,
            token_id TEXT NOT NULL,
            side TEXT NOT NULL,
            limit_price REAL NOT NULL,
            target_size REAL NOT NULL,
            idempotency_key TEXT NOT NULL,
            client_order_id TEXT NOT NULL,
            status TEXT NOT NULL,
            detail_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_execution_intents_domain_status
        ON execution_intents (domain, status, updated_at DESC);

        CREATE INDEX IF NOT EXISTS idx_execution_intents_market
        ON execution_intents (domain, market_id, updated_at DESC);
        "#,
    ),
    (
        4,
        r#"
        CREATE TABLE IF NOT EXISTS rollout_stage_state (
            domain TEXT PRIMARY KEY,
            stage TEXT NOT NULL,
            previous_stage TEXT,
            approved_by TEXT,
            approved_at TEXT,
            reason TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rollout_evaluations (
            evaluation_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            current_stage TEXT NOT NULL,
            target_stage TEXT,
            eligible INTEGER NOT NULL,
            blocking_count INTEGER NOT NULL,
            warning_count INTEGER NOT NULL,
            evaluation_json TEXT NOT NULL,
            evaluated_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_rollout_evaluations_domain_time
        ON rollout_evaluations (domain, evaluated_at DESC);

        CREATE TABLE IF NOT EXISTS promotion_candidates (
            candidate_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            current_stage TEXT NOT NULL,
            target_stage TEXT NOT NULL,
            candidate_json TEXT NOT NULL,
            is_active INTEGER NOT NULL,
            valid_until TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_promotion_candidates_domain_active
        ON promotion_candidates (domain, is_active, created_at DESC);

        CREATE TABLE IF NOT EXISTS strategy_stage_policies (
            domain TEXT NOT NULL,
            stage TEXT NOT NULL,
            policy_json TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (domain, stage)
        );

        CREATE TABLE IF NOT EXISTS rollout_incidents (
            incident_id TEXT PRIMARY KEY,
            domain TEXT NOT NULL,
            stage TEXT NOT NULL,
            severity TEXT NOT NULL,
            code TEXT NOT NULL,
            summary TEXT NOT NULL,
            detail TEXT NOT NULL,
            auto_rollback_stage TEXT,
            created_at TEXT NOT NULL,
            resolved_at TEXT,
            incident_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_rollout_incidents_domain_created_at
        ON rollout_incidents (domain, created_at DESC);
        CREATE INDEX IF NOT EXISTS idx_rollout_incidents_domain_resolved_at
        ON rollout_incidents (domain, resolved_at, created_at DESC);
        "#,
    ),
];

#[derive(Debug, Clone)]
pub struct Store {
    db_path: PathBuf,
    owner_domain: AccountDomain,
    namespace: String,
    audit_label: String,
}

impl Store {
    pub fn new(
        path: impl Into<PathBuf>,
        owner_domain: AccountDomain,
        namespace: impl Into<String>,
        audit_label: impl Into<String>,
    ) -> Self {
        Self {
            db_path: path.into(),
            owner_domain,
            namespace: namespace.into(),
            audit_label: audit_label.into(),
        }
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn owner_domain(&self) -> AccountDomain {
        self.owner_domain
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn init(&self) -> Result<()> {
        if let Some(parent) = self.db_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create database directory `{}`", parent.display())
            })?;
        }

        let mut connection = self.connection()?;
        connection.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA foreign_keys = ON;
            PRAGMA temp_store = MEMORY;
            PRAGMA wal_autocheckpoint = 1000;
            PRAGMA journal_size_limit = 67108864;
            "#,
        )?;

        let transaction = connection.transaction()?;
        for (version, sql) in MIGRATIONS {
            transaction.execute_batch(sql)?;
            transaction.execute(
                r#"
                INSERT INTO schema_migrations (version, applied_at)
                VALUES (?1, ?2)
                ON CONFLICT(version) DO NOTHING
                "#,
                params![version, now().to_rfc3339()],
            )?;
        }

        let created_at = now().to_rfc3339();
        transaction.execute(
            r#"
            INSERT INTO domain_metadata (owner_domain, namespace, audit_label, created_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(owner_domain) DO NOTHING
            "#,
            params![
                self.owner_domain.as_str(),
                self.namespace.as_str(),
                self.audit_label.as_str(),
                created_at
            ],
        )?;
        transaction.commit()?;

        self.ensure_rules_schema()?;
        self.ensure_replay_jobs_schema()?;
        self.ensure_observability_schema()?;
        self.ensure_rollout_schema()?;
        self.verify_metadata()?;

        let existing = self.runtime_mode(self.owner_domain)?;
        if existing.is_none() {
            self.set_runtime_mode(
                self.owner_domain,
                self.owner_domain.default_runtime_mode(),
                "default runtime mode",
            )?;
        }

        if self.get_current_rollout_stage(self.owner_domain)?.is_none() {
            self.set_rollout_stage(
                self.owner_domain,
                default_rollout_stage_for_domain(self.owner_domain),
                None,
                None,
                "default rollout stage",
            )?;
        }

        Ok(())
    }

    fn ensure_replay_jobs_schema(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS replay_jobs (
                job_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                requested_by TEXT NOT NULL,
                requested_at TEXT NOT NULL,
                after_sequence INTEGER,
                event_limit INTEGER NOT NULL,
                status TEXT NOT NULL,
                started_at TEXT,
                completed_at TEXT,
                error TEXT,
                run_id TEXT,
                worker_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_replay_jobs_domain_status_requested_at
                ON replay_jobs (domain, status, requested_at ASC);
            "#,
        )?;
        self.ensure_column_exists("replay_jobs", "reason", "TEXT")?;
        self.ensure_column_exists("replay_jobs", "alert_id", "TEXT")?;
        self.ensure_column_exists("replay_jobs", "audit_event_id", "TEXT")?;
        Ok(())
    }

    fn ensure_observability_schema(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS alert_events (
                alert_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                rule_kind TEXT NOT NULL,
                severity TEXT NOT NULL,
                status TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                source TEXT NOT NULL,
                summary TEXT NOT NULL,
                detail TEXT NOT NULL,
                metric_key TEXT,
                metric_value REAL,
                threshold REAL,
                trigger_value REAL,
                labels_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                acknowledged_at TEXT,
                acknowledged_by TEXT,
                resolved_at TEXT,
                audit_event_id TEXT,
                replay_job_id TEXT,
                replay_run_id TEXT,
                last_sent_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_alert_events_domain_updated_at
                ON alert_events (domain, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_alert_events_domain_status_updated_at
                ON alert_events (domain, status, updated_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS idx_alert_events_domain_dedupe
                ON alert_events (domain, dedupe_key);

            CREATE TABLE IF NOT EXISTS metric_samples (
                sample_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                metric_key TEXT NOT NULL,
                metric_kind TEXT NOT NULL,
                value REAL NOT NULL,
                labels_json TEXT NOT NULL,
                observed_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_metric_samples_domain_metric_observed_at
                ON metric_samples (domain, metric_key, observed_at DESC);
            "#,
        )?;
        Ok(())
    }

    fn ensure_rollout_schema(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS rollout_stage_state (
                domain TEXT PRIMARY KEY,
                stage TEXT NOT NULL,
                previous_stage TEXT,
                approved_by TEXT,
                approved_at TEXT,
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS rollout_evaluations (
                evaluation_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                current_stage TEXT NOT NULL,
                target_stage TEXT,
                eligible INTEGER NOT NULL,
                blocking_count INTEGER NOT NULL,
                warning_count INTEGER NOT NULL,
                evaluation_json TEXT NOT NULL,
                evaluated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rollout_evaluations_domain_time
                ON rollout_evaluations (domain, evaluated_at DESC);

            CREATE TABLE IF NOT EXISTS promotion_candidates (
                candidate_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                current_stage TEXT NOT NULL,
                target_stage TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                is_active INTEGER NOT NULL,
                valid_until TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_promotion_candidates_domain_active
                ON promotion_candidates (domain, is_active, created_at DESC);

            CREATE TABLE IF NOT EXISTS strategy_stage_policies (
                domain TEXT NOT NULL,
                stage TEXT NOT NULL,
                policy_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (domain, stage)
            );

            CREATE TABLE IF NOT EXISTS rollout_incidents (
                incident_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                stage TEXT NOT NULL,
                severity TEXT NOT NULL,
                code TEXT NOT NULL,
                summary TEXT NOT NULL,
                detail TEXT NOT NULL,
                auto_rollback_stage TEXT,
                created_at TEXT NOT NULL,
                resolved_at TEXT,
                incident_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rollout_incidents_domain_created_at
                ON rollout_incidents (domain, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_rollout_incidents_domain_resolved_at
                ON rollout_incidents (domain, resolved_at, created_at DESC);
            "#,
        )?;
        Ok(())
    }

    fn ensure_column_exists(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        let connection = self.connection()?;
        let pragma = format!("PRAGMA table_info({table})");
        let mut statement = connection.prepare(&pragma)?;
        let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
        let existing = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        if existing.iter().any(|name| name == column) {
            return Ok(());
        }
        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        connection.execute(&sql, [])?;
        Ok(())
    }

    pub fn set_runtime_mode(
        &self,
        domain: AccountDomain,
        mode: RuntimeMode,
        reason: impl Into<String>,
    ) -> Result<RuntimeModeRecord> {
        self.ensure_domain_matches(domain)?;
        let reason = reason.into();
        let updated_at = now();
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO runtime_modes (domain, mode, reason, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(domain) DO UPDATE SET
                mode = excluded.mode,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            "#,
            params![
                domain.as_str(),
                mode.as_str(),
                reason,
                updated_at.to_rfc3339()
            ],
        )?;
        Ok(RuntimeModeRecord {
            domain,
            mode,
            reason,
            updated_at,
        })
    }

    pub fn runtime_mode(&self, domain: AccountDomain) -> Result<Option<RuntimeModeRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT domain, mode, reason, updated_at
                FROM runtime_modes
                WHERE domain = ?1
                "#,
                params![domain.as_str()],
                map_runtime_mode_record,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn list_runtime_modes(&self) -> Result<Vec<RuntimeModeRecord>> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT domain, mode, reason, updated_at
            FROM runtime_modes
            ORDER BY domain
            "#,
        )?;
        let rows = statement.query_map([], map_runtime_mode_record)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn get_current_rollout_stage(
        &self,
        domain: AccountDomain,
    ) -> Result<Option<RolloutStageRecord>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT domain, stage, previous_stage, approved_by, approved_at, reason, updated_at
                FROM rollout_stage_state
                WHERE domain = ?1
                "#,
                params![domain.as_str()],
                map_rollout_stage_record,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn set_rollout_stage(
        &self,
        domain: AccountDomain,
        stage: PromotionStage,
        approved_by: Option<&str>,
        approved_at: Option<polymarket_core::Timestamp>,
        reason: impl Into<String>,
    ) -> Result<RolloutStageRecord> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let previous_stage = self.get_current_rollout_stage(domain)?.map(|record| record.stage);
        let reason = reason.into();
        let updated_at = now();
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO rollout_stage_state (
                domain, stage, previous_stage, approved_by, approved_at, reason, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(domain) DO UPDATE SET
                stage = excluded.stage,
                previous_stage = excluded.previous_stage,
                approved_by = excluded.approved_by,
                approved_at = excluded.approved_at,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            "#,
            params![
                domain.as_str(),
                stage.as_str(),
                previous_stage.map(|value| value.as_str()),
                approved_by,
                approved_at.map(|value| value.to_rfc3339()),
                reason,
                updated_at.to_rfc3339(),
            ],
        )?;
        self.get_current_rollout_stage(domain)?
            .ok_or_else(|| anyhow!("rollout stage disappeared after upsert"))
    }

    pub fn record_rollout_evaluation(
        &self,
        evaluation: RolloutEvaluation,
    ) -> Result<RolloutEvaluation> {
        self.ensure_domain_matches(evaluation.domain)?;
        self.ensure_rollout_schema()?;
        let evaluation_json = serde_json::to_string(&evaluation)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO rollout_evaluations (
                evaluation_id, domain, current_stage, target_stage, eligible, blocking_count,
                warning_count, evaluation_json, evaluated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                evaluation.evaluation_id.to_string(),
                evaluation.domain.as_str(),
                evaluation.current_stage.as_str(),
                evaluation.target_stage.map(|value| value.as_str()),
                bool_to_sqlite(evaluation.eligible),
                evaluation.blocking_reasons.len() as i64,
                evaluation.warnings.len() as i64,
                evaluation_json,
                evaluation.evaluated_at.to_rfc3339(),
            ],
        )?;
        Ok(evaluation)
    }

    pub fn latest_rollout_evaluation(
        &self,
        domain: AccountDomain,
    ) -> Result<Option<RolloutEvaluation>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT evaluation_json
                FROM rollout_evaluations
                WHERE domain = ?1
                ORDER BY evaluated_at DESC
                LIMIT 1
                "#,
                params![domain.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(anyhow::Error::from)?
            .map(|json| serde_json::from_str(&json).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn list_rollout_evaluations(
        &self,
        domain: AccountDomain,
        limit: usize,
    ) -> Result<Vec<RolloutEvaluation>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT evaluation_json
            FROM rollout_evaluations
            WHERE domain = ?1
            ORDER BY evaluated_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str(), limit as i64], |row| {
            let json: String = row.get(0)?;
            serde_json::from_str::<RolloutEvaluation>(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn upsert_promotion_candidate(
        &self,
        candidate: PromotionCandidate,
    ) -> Result<PromotionCandidate> {
        self.ensure_domain_matches(candidate.domain)?;
        self.ensure_rollout_schema()?;
        let is_active = candidate.is_active(now());
        let candidate_json = serde_json::to_string(&candidate)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO promotion_candidates (
                candidate_id, domain, current_stage, target_stage, candidate_json,
                is_active, valid_until, created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(candidate_id) DO UPDATE SET
                candidate_json = excluded.candidate_json,
                is_active = excluded.is_active,
                valid_until = excluded.valid_until,
                created_at = excluded.created_at
            "#,
            params![
                candidate.candidate_id.to_string(),
                candidate.domain.as_str(),
                candidate.current_stage.as_str(),
                candidate.target_stage.as_str(),
                candidate_json,
                bool_to_sqlite(is_active),
                candidate.valid_until.to_rfc3339(),
                candidate.created_at.to_rfc3339(),
            ],
        )?;
        Ok(candidate)
    }

    pub fn get_active_promotion_candidate(
        &self,
        domain: AccountDomain,
    ) -> Result<Option<PromotionCandidate>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        let raw = connection
            .query_row(
                r#"
                SELECT candidate_json
                FROM promotion_candidates
                WHERE domain = ?1 AND is_active = 1
                ORDER BY created_at DESC
                LIMIT 1
                "#,
                params![domain.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(anyhow::Error::from)?;
        raw.map(|json| serde_json::from_str::<PromotionCandidate>(&json).map_err(anyhow::Error::from))
            .transpose()
            .map(|candidate| candidate.filter(|value| value.is_active(now())))
    }

    pub fn invalidate_promotion_candidates(
        &self,
        domain: AccountDomain,
        invalidated_at: polymarket_core::Timestamp,
    ) -> Result<()> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            UPDATE promotion_candidates
            SET is_active = 0
            WHERE domain = ?1
            "#,
            params![domain.as_str()],
        )?;
        let candidates = self.list_all_promotion_candidates(domain)?;
        for mut candidate in candidates {
            if candidate.invalidated_at.is_none() {
                candidate.invalidated_at = Some(invalidated_at);
                let candidate_json = serde_json::to_string(&candidate)?;
                connection.execute(
                    r#"
                    UPDATE promotion_candidates
                    SET candidate_json = ?2, is_active = 0
                    WHERE candidate_id = ?1
                    "#,
                    params![candidate.candidate_id.to_string(), candidate_json],
                )?;
            }
        }
        Ok(())
    }

    fn list_all_promotion_candidates(
        &self,
        domain: AccountDomain,
    ) -> Result<Vec<PromotionCandidate>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT candidate_json
            FROM promotion_candidates
            WHERE domain = ?1
            ORDER BY created_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], |row| {
            let json: String = row.get(0)?;
            serde_json::from_str::<PromotionCandidate>(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn upsert_rollout_policy(
        &self,
        domain: AccountDomain,
        policy: &RolloutPolicy,
    ) -> Result<()> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        ensure!(
            policy.domain == domain,
            "rollout policy domain `{}` does not match store domain `{domain}`",
            policy.domain
        );
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO strategy_stage_policies (domain, stage, policy_json, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(domain, stage) DO UPDATE SET
                policy_json = excluded.policy_json,
                updated_at = excluded.updated_at
            "#,
            params![
                domain.as_str(),
                policy.stage.as_str(),
                serde_json::to_string(policy)?,
                now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn load_rollout_policy(
        &self,
        domain: AccountDomain,
        stage: PromotionStage,
    ) -> Result<Option<RolloutPolicy>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT policy_json
                FROM strategy_stage_policies
                WHERE domain = ?1 AND stage = ?2
                "#,
                params![domain.as_str(), stage.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(anyhow::Error::from)?
            .map(|json| serde_json::from_str::<RolloutPolicy>(&json).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn load_effective_rollout_policy(&self, domain: AccountDomain) -> Result<Option<RolloutPolicy>> {
        let stage = self.get_current_rollout_stage(domain)?.map(|record| record.stage);
        match stage {
            Some(stage) => self.load_rollout_policy(domain, stage),
            None => Ok(None),
        }
    }

    pub fn record_rollout_incident(&self, incident: RolloutIncident) -> Result<RolloutIncident> {
        self.ensure_domain_matches(incident.domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO rollout_incidents (
                incident_id, domain, stage, severity, code, summary, detail,
                auto_rollback_stage, created_at, resolved_at, incident_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                incident.incident_id.to_string(),
                incident.domain.as_str(),
                incident.stage.as_str(),
                incident.severity.as_str(),
                incident.code,
                incident.summary,
                incident.detail,
                incident.auto_rollback_stage.map(|value| value.as_str()),
                incident.created_at.to_rfc3339(),
                incident.resolved_at.map(|value| value.to_rfc3339()),
                serde_json::to_string(&incident)?,
            ],
        )?;
        Ok(incident)
    }

    pub fn resolve_rollout_incident(
        &self,
        domain: AccountDomain,
        incident_id: Uuid,
        resolved_at: polymarket_core::Timestamp,
    ) -> Result<()> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        let Some(mut incident) = self
            .list_rollout_incidents(domain, true, 500)?
            .into_iter()
            .find(|item| item.incident_id == incident_id)
        else {
            return Ok(());
        };
        incident.resolved_at = Some(resolved_at);
        connection.execute(
            r#"
            UPDATE rollout_incidents
            SET resolved_at = ?3, incident_json = ?4
            WHERE incident_id = ?1 AND domain = ?2
            "#,
            params![
                incident_id.to_string(),
                domain.as_str(),
                resolved_at.to_rfc3339(),
                serde_json::to_string(&incident)?,
            ],
        )?;
        Ok(())
    }

    pub fn list_rollout_incidents(
        &self,
        domain: AccountDomain,
        include_resolved: bool,
        limit: usize,
    ) -> Result<Vec<RolloutIncident>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_rollout_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT incident_json
            FROM rollout_incidents
            WHERE domain = ?1
              AND (?2 = 1 OR resolved_at IS NULL)
            ORDER BY created_at DESC
            LIMIT ?3
            "#,
        )?;
        let rows = statement.query_map(
            params![domain.as_str(), bool_to_sqlite(include_resolved), limit as i64],
            |row| {
                let json: String = row.get(0)?;
                serde_json::from_str::<RolloutIncident>(&json).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
                })
            },
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn heartbeat_service(
        &self,
        service: ServiceKind,
        domain: AccountDomain,
        healthy: bool,
        detail: &str,
    ) -> Result<ServiceHeartbeat> {
        self.ensure_domain_matches(domain)?;
        let heartbeat = ServiceHeartbeat {
            service,
            domain,
            healthy,
            detail: detail.to_owned(),
            last_seen_at: now(),
        };
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO service_heartbeats (service, domain, healthy, detail, last_seen_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(service, domain) DO UPDATE SET
                healthy = excluded.healthy,
                detail = excluded.detail,
                last_seen_at = excluded.last_seen_at
            "#,
            params![
                heartbeat.service.as_str(),
                heartbeat.domain.as_str(),
                bool_to_sqlite(heartbeat.healthy),
                heartbeat.detail,
                heartbeat.last_seen_at.to_rfc3339()
            ],
        )?;
        Ok(heartbeat)
    }

    pub fn list_service_heartbeats(&self, domain: AccountDomain) -> Result<Vec<ServiceHeartbeat>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT service, domain, healthy, detail, last_seen_at
            FROM service_heartbeats
            WHERE domain = ?1
            ORDER BY service
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], map_service_heartbeat)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn append_audit(
        &self,
        domain: Option<AccountDomain>,
        service: impl Into<String>,
        action: impl Into<String>,
        detail: impl Into<String>,
    ) -> Result<AuditEvent> {
        let domain = domain.unwrap_or(self.owner_domain);
        self.ensure_domain_matches(domain)?;
        let event = AuditEvent {
            id: Uuid::new_v4(),
            occurred_at: now(),
            service: service.into(),
            domain: Some(domain),
            action: action.into(),
            detail: detail.into(),
        };

        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO audit_events (id, occurred_at, service, domain, action, detail)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                event.id.to_string(),
                event.occurred_at.to_rfc3339(),
                event.service,
                event.domain.map(AccountDomain::as_str),
                event.action,
                event.detail
            ],
        )?;
        Ok(event)
    }

    pub fn upsert_alert(&self, alert: AlertEvent) -> Result<AlertEvent> {
        self.ensure_domain_matches(alert.domain)?;
        self.ensure_observability_schema()?;
        let labels_json = serde_json::to_string(&alert.labels)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO alert_events (
                alert_id, domain, rule_kind, severity, status, dedupe_key, source, summary, detail,
                metric_key, metric_value, threshold, trigger_value, labels_json, created_at,
                updated_at, acknowledged_at, acknowledged_by, resolved_at, audit_event_id,
                replay_job_id, replay_run_id, last_sent_at
            )
            VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17,
                ?18, ?19, ?20, ?21, ?22, ?23
            )
            ON CONFLICT(domain, dedupe_key) DO UPDATE SET
                rule_kind = excluded.rule_kind,
                severity = excluded.severity,
                status = excluded.status,
                source = excluded.source,
                summary = excluded.summary,
                detail = excluded.detail,
                metric_key = excluded.metric_key,
                metric_value = excluded.metric_value,
                threshold = excluded.threshold,
                trigger_value = excluded.trigger_value,
                labels_json = excluded.labels_json,
                updated_at = excluded.updated_at,
                acknowledged_at = excluded.acknowledged_at,
                acknowledged_by = excluded.acknowledged_by,
                resolved_at = excluded.resolved_at,
                audit_event_id = excluded.audit_event_id,
                replay_job_id = excluded.replay_job_id,
                replay_run_id = excluded.replay_run_id,
                last_sent_at = excluded.last_sent_at
            "#,
            params![
                alert.alert_id.to_string(),
                alert.domain.as_str(),
                alert.rule_kind.as_str(),
                alert.severity.as_str(),
                alert.status.as_str(),
                alert.dedupe_key,
                alert.source,
                alert.summary,
                alert.detail,
                alert.metric_key,
                alert.metric_value,
                alert.threshold,
                alert.trigger_value,
                labels_json,
                alert.created_at.to_rfc3339(),
                alert.updated_at.to_rfc3339(),
                alert.acknowledged_at.map(|value| value.to_rfc3339()),
                alert.acknowledged_by,
                alert.resolved_at.map(|value| value.to_rfc3339()),
                alert.audit_event_id.map(|value| value.to_string()),
                alert.replay_job_id.map(|value| value.to_string()),
                alert.replay_run_id.map(|value| value.to_string()),
                alert.last_sent_at.map(|value| value.to_rfc3339()),
            ],
        )?;
        self.get_alert_by_dedupe(alert.domain, &alert.dedupe_key)?
            .ok_or_else(|| anyhow!("alert disappeared after upsert"))
    }

    pub fn get_alert_by_dedupe(
        &self,
        domain: AccountDomain,
        dedupe_key: &str,
    ) -> Result<Option<AlertEvent>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_observability_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT alert_id, domain, rule_kind, severity, status, dedupe_key, source, summary,
                       detail, metric_key, metric_value, threshold, trigger_value, labels_json,
                       created_at, updated_at, acknowledged_at, acknowledged_by, resolved_at,
                       audit_event_id, replay_job_id, replay_run_id, last_sent_at
                FROM alert_events
                WHERE domain = ?1 AND dedupe_key = ?2
                "#,
                params![domain.as_str(), dedupe_key],
                map_alert_event,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn get_alert(&self, alert_id: Uuid) -> Result<Option<AlertEvent>> {
        self.ensure_observability_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT alert_id, domain, rule_kind, severity, status, dedupe_key, source, summary,
                       detail, metric_key, metric_value, threshold, trigger_value, labels_json,
                       created_at, updated_at, acknowledged_at, acknowledged_by, resolved_at,
                       audit_event_id, replay_job_id, replay_run_id, last_sent_at
                FROM alert_events
                WHERE alert_id = ?1 AND domain = ?2
                "#,
                params![alert_id.to_string(), self.owner_domain.as_str()],
                map_alert_event,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn list_alerts(
        &self,
        domain: AccountDomain,
        status: Option<AlertStatus>,
        severity: Option<AlertSeverity>,
        limit: usize,
    ) -> Result<Vec<AlertEvent>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_observability_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT alert_id, domain, rule_kind, severity, status, dedupe_key, source, summary,
                   detail, metric_key, metric_value, threshold, trigger_value, labels_json,
                   created_at, updated_at, acknowledged_at, acknowledged_by, resolved_at,
                   audit_event_id, replay_job_id, replay_run_id, last_sent_at
            FROM alert_events
            WHERE domain = ?1
              AND (?2 IS NULL OR status = ?2)
              AND (?3 IS NULL OR severity = ?3)
            ORDER BY updated_at DESC
            LIMIT ?4
            "#,
        )?;
        let rows = statement.query_map(
            params![
                domain.as_str(),
                status.map(|value| value.as_str()),
                severity.map(|value| value.as_str()),
                limit as i64
            ],
            map_alert_event,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn acknowledge_alert(&self, alert_id: Uuid, acknowledged_by: &str) -> Result<Option<AlertEvent>> {
        self.ensure_observability_schema()?;
        let now = now();
        let connection = self.connection()?;
        connection.execute(
            r#"
            UPDATE alert_events
            SET status = ?2, acknowledged_at = ?3, acknowledged_by = ?4, updated_at = ?3
            WHERE alert_id = ?1 AND domain = ?5 AND status != ?6
            "#,
            params![
                alert_id.to_string(),
                AlertStatus::Acknowledged.as_str(),
                now.to_rfc3339(),
                acknowledged_by,
                self.owner_domain.as_str(),
                AlertStatus::Resolved.as_str(),
            ],
        )?;
        self.get_alert(alert_id)
    }

    pub fn record_metric_sample(&self, sample: MetricSample) -> Result<MetricSample> {
        self.ensure_domain_matches(sample.domain)?;
        self.ensure_observability_schema()?;
        let labels_json = serde_json::to_string(&sample.labels)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO metric_samples (
                sample_id, domain, metric_key, metric_kind, value, labels_json, observed_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                sample.sample_id.to_string(),
                sample.domain.as_str(),
                sample.metric_key,
                sample.metric_kind,
                sample.value,
                labels_json,
                sample.observed_at.to_rfc3339(),
            ],
        )?;
        Ok(sample)
    }

    pub fn latest_metric_samples(&self, domain: AccountDomain, limit: usize) -> Result<Vec<MetricSample>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_observability_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT sample_id, domain, metric_key, metric_kind, value, labels_json, observed_at
            FROM metric_samples
            WHERE domain = ?1
            ORDER BY observed_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str(), limit as i64], map_metric_sample)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn enqueue_replay_job(
        &self,
        request: ReplayRequest,
        requested_by: impl Into<String>,
    ) -> Result<ReplayJob> {
        self.ensure_domain_matches(request.domain)?;
        self.ensure_replay_jobs_schema()?;
        let requested_by = requested_by.into();
        let job = ReplayJob {
            job_id: Uuid::new_v4(),
            domain: request.domain,
            requested_by,
            requested_at: now(),
            after_sequence: request.after_sequence,
            limit: request.limit,
            status: ReplayJobStatus::Pending,
            started_at: None,
            completed_at: None,
            error: None,
            run_id: None,
            worker_id: None,
            reason: request.reason,
            alert_id: request.alert_id,
            audit_event_id: request.audit_event_id,
        };
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO replay_jobs (
                job_id,
                domain,
                requested_by,
                requested_at,
                after_sequence,
                event_limit,
                status,
                started_at,
                completed_at,
                error,
                run_id,
                worker_id,
                reason,
                alert_id,
                audit_event_id
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL, NULL, NULL, NULL, ?8, ?9, ?10)
            "#,
            params![
                job.job_id.to_string(),
                job.domain.as_str(),
                job.requested_by,
                job.requested_at.to_rfc3339(),
                job.after_sequence,
                job.limit as i64,
                job.status.as_str(),
                job.reason,
                job.alert_id.map(|value| value.to_string()),
                job.audit_event_id.map(|value| value.to_string()),
            ],
        )?;
        Ok(job)
    }

    pub fn get_replay_job(&self, job_id: Uuid) -> Result<Option<ReplayJob>> {
        self.ensure_replay_jobs_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT
                    job_id,
                    domain,
                    requested_by,
                    requested_at,
                    after_sequence,
                    event_limit,
                    status,
                    started_at,
                    completed_at,
                    error,
                    run_id,
                    worker_id,
                    reason,
                    alert_id,
                    audit_event_id
                FROM replay_jobs
                WHERE job_id = ?1 AND domain = ?2
                "#,
                params![job_id.to_string(), self.owner_domain.as_str()],
                map_replay_job,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn claim_pending_replay_job(&self, worker_id: &str) -> Result<Option<ReplayJob>> {
        self.ensure_replay_jobs_schema()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let job = transaction
            .query_row(
                r#"
                SELECT
                    job_id,
                    domain,
                    requested_by,
                    requested_at,
                    after_sequence,
                    event_limit,
                    status,
                    started_at,
                    completed_at,
                    error,
                    run_id,
                    worker_id,
                    reason,
                    alert_id,
                    audit_event_id
                FROM replay_jobs
                WHERE domain = ?1 AND status = ?2
                ORDER BY requested_at ASC
                LIMIT 1
                "#,
                params![
                    self.owner_domain.as_str(),
                    ReplayJobStatus::Pending.as_str()
                ],
                map_replay_job,
            )
            .optional()?;
        let Some(mut job) = job else {
            transaction.commit()?;
            return Ok(None);
        };
        let started_at = now();
        let updated = transaction.execute(
            r#"
            UPDATE replay_jobs
            SET status = ?2, started_at = ?3, worker_id = ?4, error = NULL
            WHERE job_id = ?1 AND status = ?5
            "#,
            params![
                job.job_id.to_string(),
                ReplayJobStatus::Running.as_str(),
                started_at.to_rfc3339(),
                worker_id,
                ReplayJobStatus::Pending.as_str(),
            ],
        )?;
        transaction.commit()?;
        if updated != 1 {
            return Ok(None);
        }
        job.status = ReplayJobStatus::Running;
        job.started_at = Some(started_at);
        job.worker_id = Some(worker_id.to_owned());
        job.error = None;
        Ok(Some(job))
    }

    pub fn complete_replay_job(&self, job_id: Uuid, run_id: Uuid) -> Result<()> {
        self.ensure_replay_jobs_schema()?;
        let completed_at = now();
        let connection = self.connection()?;
        connection.execute(
            r#"
            UPDATE replay_jobs
            SET status = ?2, completed_at = ?3, error = NULL, run_id = ?4
            WHERE job_id = ?1 AND domain = ?5
            "#,
            params![
                job_id.to_string(),
                ReplayJobStatus::Completed.as_str(),
                completed_at.to_rfc3339(),
                run_id.to_string(),
                self.owner_domain.as_str(),
            ],
        )?;
        Ok(())
    }

    pub fn fail_replay_job(&self, job_id: Uuid, error: impl Into<String>) -> Result<()> {
        self.ensure_replay_jobs_schema()?;
        let completed_at = now();
        let error = error.into();
        let connection = self.connection()?;
        connection.execute(
            r#"
            UPDATE replay_jobs
            SET status = ?2, completed_at = ?3, error = ?4
            WHERE job_id = ?1 AND domain = ?5
            "#,
            params![
                job_id.to_string(),
                ReplayJobStatus::Failed.as_str(),
                completed_at.to_rfc3339(),
                error,
                self.owner_domain.as_str(),
            ],
        )?;
        Ok(())
    }

    pub fn recent_audit_events(&self, limit: usize) -> Result<Vec<AuditEvent>> {
        self.recent_audit_events_for_domain(None, limit)
    }

    pub fn recent_audit_events_for_domain(
        &self,
        domain: Option<AccountDomain>,
        limit: usize,
    ) -> Result<Vec<AuditEvent>> {
        if let Some(domain) = domain {
            self.ensure_domain_matches(domain)?;
        }

        let connection = self.connection()?;
        if let Some(domain) = domain {
            let mut statement = connection.prepare(
                r#"
                SELECT id, occurred_at, service, domain, action, detail
                FROM audit_events
                WHERE domain = ?1
                ORDER BY occurred_at DESC
                LIMIT ?2
                "#,
            )?;
            let rows =
                statement.query_map(params![domain.as_str(), limit as i64], map_audit_event)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(anyhow::Error::from)
        } else {
            let mut statement = connection.prepare(
                r#"
                SELECT id, occurred_at, service, domain, action, detail
                FROM audit_events
                ORDER BY occurred_at DESC
                LIMIT ?1
                "#,
            )?;
            let rows = statement.query_map(params![limit as i64], map_audit_event)?;
            rows.collect::<rusqlite::Result<Vec<_>>>()
                .map_err(anyhow::Error::from)
        }
    }

    pub fn audit_events_in_window(
        &self,
        domain: Option<AccountDomain>,
        service: Option<&str>,
        action: Option<&str>,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
        limit: usize,
    ) -> Result<Vec<AuditEvent>> {
        if let Some(domain) = domain {
            self.ensure_domain_matches(domain)?;
        }

        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT id, occurred_at, service, domain, action, detail
            FROM audit_events
            WHERE (?1 IS NULL OR domain = ?1)
              AND (?2 IS NULL OR service = ?2)
              AND (?3 IS NULL OR action = ?3)
              AND occurred_at >= ?4
              AND occurred_at < ?5
            ORDER BY occurred_at DESC
            LIMIT ?6
            "#,
        )?;
        let rows = statement.query_map(
            params![
                domain.map(|value| value.as_str()),
                service,
                action,
                start.to_rfc3339(),
                end.to_rfc3339(),
                limit as i64,
            ],
            map_audit_event,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn resolve_alert(
        &self,
        domain: AccountDomain,
        dedupe_key: &str,
        detail: &str,
    ) -> Result<Option<AlertEvent>> {
        self.ensure_domain_matches(domain)?;
        self.ensure_observability_schema()?;
        let updated_at = now();
        let connection = self.connection()?;
        connection.execute(
            r#"
            UPDATE alert_events
            SET status = ?3, detail = ?4, resolved_at = ?5, updated_at = ?5
            WHERE domain = ?1 AND dedupe_key = ?2 AND status != ?3
            "#,
            params![
                domain.as_str(),
                dedupe_key,
                AlertStatus::Resolved.as_str(),
                detail,
                updated_at.to_rfc3339()
            ],
        )?;
        self.get_alert_by_dedupe(domain, dedupe_key)
    }

    pub fn replay_trace_by_run(&self, run_id: Uuid) -> Result<ReplayTrace> {
        self.ensure_replay_jobs_schema()?;
        self.ensure_observability_schema()?;
        let connection = self.connection()?;
        let job = connection
            .query_row(
                r#"
                SELECT
                    job_id, domain, requested_by, requested_at, after_sequence, event_limit, status,
                    started_at, completed_at, error, run_id, worker_id, reason, alert_id, audit_event_id
                FROM replay_jobs
                WHERE run_id = ?1 AND domain = ?2
                ORDER BY requested_at DESC
                LIMIT 1
                "#,
                params![run_id.to_string(), self.owner_domain.as_str()],
                map_replay_job,
            )
            .optional()?;
        let run = self.get_rules_replay_run(run_id)?;
        let alerts = if let Some(job) = &job {
            self.list_alerts(self.owner_domain, None, None, 200)?
                .into_iter()
                .filter(|alert| {
                    alert.replay_run_id == Some(run_id) || alert.replay_job_id == Some(job.job_id)
                })
                .collect()
        } else {
            self.list_alerts(self.owner_domain, None, None, 200)?
                .into_iter()
                .filter(|alert| alert.replay_run_id == Some(run_id))
                .collect()
        };
        let audit_events = self
            .recent_audit_events_for_domain(Some(self.owner_domain), 200)?
            .into_iter()
            .filter(|event| {
                event.detail.contains(&run_id.to_string())
                    || job
                        .as_ref()
                        .map(|value| event.detail.contains(&value.job_id.to_string()))
                        .unwrap_or(false)
            })
            .collect();
        Ok(ReplayTrace {
            job,
            run,
            alerts,
            audit_events,
        })
    }

    pub fn append_event(&self, event: NewDurableEvent) -> Result<DurableEvent> {
        self.ensure_domain_matches(event.domain)?;
        let event_id = Uuid::new_v4();
        let persisted_at = now();
        let payload_json = serde_json::to_string(&event.payload)?;
        let metadata_json = serde_json::to_string(&event.metadata)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO durable_events (
                event_id, domain, stream, aggregate_type, aggregate_id, event_type,
                causation_id, correlation_id, idempotency_key, payload_json, metadata_json,
                created_at, persisted_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            "#,
            params![
                event_id.to_string(),
                event.domain.as_str(),
                event.stream,
                event.aggregate_type,
                event.aggregate_id,
                event.event_type,
                event.causation_id.map(|id| id.to_string()),
                event.correlation_id.map(|id| id.to_string()),
                event.idempotency_key,
                payload_json,
                metadata_json,
                event.created_at.to_rfc3339(),
                persisted_at.to_rfc3339()
            ],
        )?;
        let sequence = connection.last_insert_rowid();
        Ok(DurableEvent {
            sequence,
            event_id,
            domain: event.domain,
            stream: event.stream,
            aggregate_type: event.aggregate_type,
            aggregate_id: event.aggregate_id,
            event_type: event.event_type,
            causation_id: event.causation_id,
            correlation_id: event.correlation_id,
            idempotency_key: event.idempotency_key,
            payload: event.payload,
            metadata: event.metadata,
            created_at: event.created_at,
            persisted_at,
        })
    }

    pub fn replay_events(
        &self,
        domain: AccountDomain,
        cursor: ReplayCursor,
    ) -> Result<Vec<DurableEvent>> {
        self.ensure_domain_matches(domain)?;
        let limit = clamp_limit(cursor.limit, DEFAULT_RECOVERY_EVENT_LIMIT);
        let connection = self.connection()?;
        let mut statement = if cursor.after_sequence.is_some() {
            connection.prepare(
                r#"
                SELECT sequence, event_id, domain, stream, aggregate_type, aggregate_id, event_type,
                       causation_id, correlation_id, idempotency_key, payload_json, metadata_json,
                       created_at, persisted_at
                FROM durable_events
                WHERE domain = ?1 AND sequence > ?2
                ORDER BY sequence ASC
                LIMIT ?3
                "#,
            )?
        } else {
            connection.prepare(
                r#"
                SELECT sequence, event_id, domain, stream, aggregate_type, aggregate_id, event_type,
                       causation_id, correlation_id, idempotency_key, payload_json, metadata_json,
                       created_at, persisted_at
                FROM durable_events
                WHERE domain = ?1
                ORDER BY sequence ASC
                LIMIT ?2
                "#,
            )?
        };

        let rows = if let Some(after_sequence) = cursor.after_sequence {
            statement.query_map(
                params![domain.as_str(), after_sequence, limit as i64],
                map_durable_event,
            )?
        } else {
            statement.query_map(params![domain.as_str(), limit as i64], map_durable_event)?
        };

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn upsert_snapshot(&self, snapshot: NewStateSnapshot) -> Result<StateSnapshot> {
        self.ensure_domain_matches(snapshot.domain)?;
        ensure!(
            snapshot.version >= 0,
            "snapshot version must be non-negative"
        );
        let record = StateSnapshot {
            snapshot_id: Uuid::new_v4(),
            domain: snapshot.domain,
            aggregate_type: snapshot.aggregate_type,
            aggregate_id: snapshot.aggregate_id,
            version: snapshot.version,
            payload: snapshot.payload,
            derived_from_sequence: snapshot.derived_from_sequence,
            created_at: snapshot.created_at,
        };
        let payload_json = serde_json::to_string(&record.payload)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO state_snapshots (
                snapshot_id, domain, aggregate_type, aggregate_id, version,
                payload_json, derived_from_sequence, created_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(domain, aggregate_type, aggregate_id) DO UPDATE SET
                snapshot_id = excluded.snapshot_id,
                version = excluded.version,
                payload_json = excluded.payload_json,
                derived_from_sequence = excluded.derived_from_sequence,
                created_at = excluded.created_at
            WHERE excluded.version >= state_snapshots.version
            "#,
            params![
                record.snapshot_id.to_string(),
                record.domain.as_str(),
                record.aggregate_type,
                record.aggregate_id,
                record.version,
                payload_json,
                record.derived_from_sequence,
                record.created_at.to_rfc3339()
            ],
        )?;
        self.latest_snapshot(record.domain, &record.aggregate_type, &record.aggregate_id)?
            .ok_or_else(|| anyhow!("snapshot disappeared after upsert"))
    }

    pub fn latest_snapshot(
        &self,
        domain: AccountDomain,
        aggregate_type: &str,
        aggregate_id: &str,
    ) -> Result<Option<StateSnapshot>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT snapshot_id, domain, aggregate_type, aggregate_id, version,
                       payload_json, derived_from_sequence, created_at
                FROM state_snapshots
                WHERE domain = ?1 AND aggregate_type = ?2 AND aggregate_id = ?3
                "#,
                params![domain.as_str(), aggregate_type, aggregate_id],
                map_snapshot,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn recent_snapshots(
        &self,
        domain: AccountDomain,
        limit: usize,
    ) -> Result<Vec<StateSnapshot>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT snapshot_id, domain, aggregate_type, aggregate_id, version,
                   payload_json, derived_from_sequence, created_at
            FROM state_snapshots
            WHERE domain = ?1
            ORDER BY created_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(
            params![
                domain.as_str(),
                clamp_limit(limit, DEFAULT_RECOVERY_SNAPSHOT_LIMIT) as i64
            ],
            map_snapshot,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn list_snapshots_by_type(
        &self,
        domain: AccountDomain,
        aggregate_type: &str,
    ) -> Result<Vec<StateSnapshot>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT snapshot_id, domain, aggregate_type, aggregate_id, version,
                   payload_json, derived_from_sequence, created_at
            FROM state_snapshots
            WHERE domain = ?1 AND aggregate_type = ?2
            ORDER BY aggregate_id ASC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str(), aggregate_type], map_snapshot)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn upsert_order_lifecycle(
        &self,
        record: NewOrderLifecycleRecord,
    ) -> Result<OrderLifecycleRecord> {
        self.ensure_domain_matches(record.domain)?;
        ensure!(
            record.filled_quantity >= 0.0,
            "filled_quantity must be non-negative"
        );
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO order_lifecycle (
                domain, order_id, market_id, status, client_order_id, external_order_id,
                idempotency_key, side, limit_price, order_quantity, filled_quantity,
                average_fill_price, last_event_sequence, detail_json, opened_at, updated_at, closed_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
            ON CONFLICT(domain, order_id) DO UPDATE SET
                market_id = excluded.market_id,
                status = excluded.status,
                client_order_id = excluded.client_order_id,
                external_order_id = excluded.external_order_id,
                idempotency_key = excluded.idempotency_key,
                side = excluded.side,
                limit_price = excluded.limit_price,
                order_quantity = excluded.order_quantity,
                filled_quantity = excluded.filled_quantity,
                average_fill_price = excluded.average_fill_price,
                last_event_sequence = excluded.last_event_sequence,
                detail_json = excluded.detail_json,
                opened_at = MIN(order_lifecycle.opened_at, excluded.opened_at),
                updated_at = excluded.updated_at,
                closed_at = excluded.closed_at
            "#,
            params![
                record.domain.as_str(),
                record.order_id,
                record.market_id,
                record.status.as_str(),
                record.client_order_id,
                record.external_order_id,
                record.idempotency_key,
                record.side,
                record.limit_price,
                record.order_quantity,
                record.filled_quantity,
                record.average_fill_price,
                record.last_event_sequence,
                serde_json::to_string(&record.detail)?,
                record.opened_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
                record.closed_at.map(|ts| ts.to_rfc3339())
            ],
        )?;
        self.order_lifecycle(record.domain, &record.order_id)?
            .ok_or_else(|| anyhow!("order lifecycle disappeared after upsert"))
    }

    pub fn order_lifecycle(
        &self,
        domain: AccountDomain,
        order_id: &str,
    ) -> Result<Option<OrderLifecycleRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT domain, order_id, market_id, status, client_order_id, external_order_id,
                       idempotency_key, side, limit_price, order_quantity, filled_quantity,
                       average_fill_price, last_event_sequence, detail_json, opened_at, updated_at, closed_at
                FROM order_lifecycle
                WHERE domain = ?1 AND order_id = ?2
                "#,
                params![domain.as_str(), order_id],
                map_order_lifecycle,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn list_open_orders(&self, domain: AccountDomain) -> Result<Vec<OrderLifecycleRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT domain, order_id, market_id, status, client_order_id, external_order_id,
                   idempotency_key, side, limit_price, order_quantity, filled_quantity,
                   average_fill_price, last_event_sequence, detail_json, opened_at, updated_at, closed_at
            FROM order_lifecycle
            WHERE domain = ?1 AND closed_at IS NULL
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], map_order_lifecycle)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn list_all_order_lifecycle(
        &self,
        domain: AccountDomain,
    ) -> Result<Vec<OrderLifecycleRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT domain, order_id, market_id, status, client_order_id, external_order_id,
                   idempotency_key, side, limit_price, order_quantity, filled_quantity,
                   average_fill_price, last_event_sequence, detail_json, opened_at, updated_at, closed_at
            FROM order_lifecycle
            WHERE domain = ?1
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], map_order_lifecycle)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn record_execution_intent(
        &self,
        intent: &TradeIntent,
        intent_id: Uuid,
        batch_id: Option<Uuid>,
        idempotency_key: impl Into<String>,
        client_order_id: impl Into<String>,
        status: ExecutionIntentStatus,
        detail: Value,
    ) -> Result<ExecutionIntentRecord> {
        self.ensure_domain_matches(intent.account_domain)?;
        let idempotency_key = idempotency_key.into();
        let client_order_id = client_order_id.into();
        let now_ts = now();
        let record = ExecutionIntentRecord {
            intent_id,
            domain: intent.account_domain,
            batch_id,
            strategy_kind: intent.strategy_kind,
            market_id: intent.market_id.clone(),
            token_id: intent.token_id.clone(),
            side: intent.side,
            limit_price: intent.limit_price,
            target_size: intent.max_size,
            idempotency_key,
            client_order_id,
            status,
            detail,
            created_at: now_ts,
            updated_at: now_ts,
            expires_at: intent.expires_at,
        };
        self.upsert_execution_state(record)
    }

    pub fn append_order_event(&self, event: NewDurableEvent) -> Result<DurableEvent> {
        self.append_event(event)
    }

    pub fn upsert_execution_state(
        &self,
        record: ExecutionIntentRecord,
    ) -> Result<ExecutionIntentRecord> {
        self.ensure_domain_matches(record.domain)?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO execution_intents (
                intent_id, domain, batch_id, strategy_kind, market_id, token_id, side,
                limit_price, target_size, idempotency_key, client_order_id, status, detail_json,
                created_at, updated_at, expires_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)
            ON CONFLICT(intent_id) DO UPDATE SET
                batch_id = excluded.batch_id,
                strategy_kind = excluded.strategy_kind,
                market_id = excluded.market_id,
                token_id = excluded.token_id,
                side = excluded.side,
                limit_price = excluded.limit_price,
                target_size = excluded.target_size,
                idempotency_key = excluded.idempotency_key,
                client_order_id = excluded.client_order_id,
                status = excluded.status,
                detail_json = excluded.detail_json,
                updated_at = excluded.updated_at,
                expires_at = excluded.expires_at
            "#,
            params![
                record.intent_id.to_string(),
                record.domain.as_str(),
                record.batch_id.map(|value| value.to_string()),
                record.strategy_kind.as_str(),
                record.market_id,
                record.token_id,
                record.side.as_str(),
                record.limit_price,
                record.target_size,
                record.idempotency_key,
                record.client_order_id,
                record.status.as_str(),
                serde_json::to_string(&record.detail)?,
                record.created_at.to_rfc3339(),
                record.updated_at.to_rfc3339(),
                record.expires_at.to_rfc3339(),
            ],
        )?;
        self.get_execution_intent(record.intent_id)?
            .ok_or_else(|| anyhow!("execution intent disappeared after upsert"))
    }

    pub fn get_execution_intent(&self, intent_id: Uuid) -> Result<Option<ExecutionIntentRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT intent_id, domain, batch_id, strategy_kind, market_id, token_id, side,
                       limit_price, target_size, idempotency_key, client_order_id, status,
                       detail_json, created_at, updated_at, expires_at
                FROM execution_intents
                WHERE intent_id = ?1
                "#,
                params![intent_id.to_string()],
                map_execution_intent_record,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    pub fn list_execution_intents(
        &self,
        domain: AccountDomain,
        limit: usize,
    ) -> Result<Vec<ExecutionIntentRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT intent_id, domain, batch_id, strategy_kind, market_id, token_id, side,
                   limit_price, target_size, idempotency_key, client_order_id, status,
                   detail_json, created_at, updated_at, expires_at
            FROM execution_intents
            WHERE domain = ?1
            ORDER BY updated_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(
            params![domain.as_str(), limit as i64],
            map_execution_intent_record,
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn list_all_execution_intents(
        &self,
        domain: AccountDomain,
    ) -> Result<Vec<ExecutionIntentRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT intent_id, domain, batch_id, strategy_kind, market_id, token_id, side,
                   limit_price, target_size, idempotency_key, client_order_id, status,
                   detail_json, created_at, updated_at, expires_at
            FROM execution_intents
            WHERE domain = ?1
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], map_execution_intent_record)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn load_open_execution_intents(
        &self,
        domain: AccountDomain,
    ) -> Result<Vec<ExecutionIntentRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT intent_id, domain, batch_id, strategy_kind, market_id, token_id, side,
                   limit_price, target_size, idempotency_key, client_order_id, status,
                   detail_json, created_at, updated_at, expires_at
            FROM execution_intents
            WHERE domain = ?1 AND status NOT IN ('FILLED', 'CANCELLED', 'FAILED_TERMINAL', 'RECONCILED')
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![domain.as_str()], map_execution_intent_record)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn load_recovery_batch(
        &self,
        domain: AccountDomain,
        limit: usize,
    ) -> Result<Vec<ExecutionIntentRecord>> {
        let mut intents = self.load_open_execution_intents(domain)?;
        intents.truncate(limit.min(intents.len()));
        Ok(intents)
    }

    pub fn mark_idempotency_key(
        &self,
        domain: AccountDomain,
        scope: &str,
        key: &str,
        response_payload: &Value,
        succeeded: bool,
    ) -> Result<IdempotencyKeyRecord> {
        if succeeded {
            self.complete_idempotency_key(domain, scope, key, response_payload)
        } else {
            self.fail_idempotency_key(domain, scope, key, response_payload)
        }
    }

    pub fn fetch_reconcile_window(
        &self,
        domain: AccountDomain,
        limit: usize,
    ) -> Result<Vec<OrderLifecycleRecord>> {
        self.ensure_domain_matches(domain)?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT domain, order_id, market_id, status, client_order_id, external_order_id,
                   idempotency_key, side, limit_price, order_quantity, filled_quantity,
                   average_fill_price, last_event_sequence, detail_json, opened_at, updated_at, closed_at
            FROM order_lifecycle
            WHERE domain = ?1
            ORDER BY updated_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows =
            statement.query_map(params![domain.as_str(), limit as i64], map_order_lifecycle)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn claim_idempotency_key(
        &self,
        claim: NewIdempotencyKey,
    ) -> Result<IdempotencyClaimResult> {
        self.ensure_domain_matches(claim.domain)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;

        let existing = transaction
            .query_row(
                r#"
                SELECT domain, scope, key, request_hash, status, response_json, created_by,
                       created_at, last_seen_at, lock_expires_at
                FROM idempotency_keys
                WHERE domain = ?1 AND scope = ?2 AND key = ?3
                "#,
                params![claim.domain.as_str(), claim.scope, claim.key],
                map_idempotency_record,
            )
            .optional()?;

        let now_ts = now();

        let result = match existing {
            None => {
                transaction.execute(
                    r#"
                    INSERT INTO idempotency_keys (
                        domain, scope, key, request_hash, status, response_json,
                        created_by, created_at, last_seen_at, lock_expires_at
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9)
                    "#,
                    params![
                        claim.domain.as_str(),
                        claim.scope,
                        claim.key,
                        claim.request_hash,
                        IdempotencyStatus::InProgress.as_str(),
                        claim.created_by,
                        claim.created_at.to_rfc3339(),
                        now_ts.to_rfc3339(),
                        claim.lock_expires_at.map(|ts| ts.to_rfc3339())
                    ],
                )?;
                IdempotencyClaimResult::Claimed(IdempotencyKeyRecord {
                    domain: claim.domain,
                    scope: claim.scope,
                    key: claim.key,
                    request_hash: claim.request_hash,
                    status: IdempotencyStatus::InProgress,
                    response_payload: None,
                    created_by: claim.created_by,
                    created_at: claim.created_at,
                    last_seen_at: now_ts,
                    lock_expires_at: claim.lock_expires_at,
                })
            }
            Some(existing) if existing.request_hash != claim.request_hash => {
                IdempotencyClaimResult::HashMismatch(existing)
            }
            Some(existing) if existing.status == IdempotencyStatus::Completed => {
                transaction.execute(
                    r#"
                    UPDATE idempotency_keys
                    SET last_seen_at = ?4
                    WHERE domain = ?1 AND scope = ?2 AND key = ?3
                    "#,
                    params![
                        existing.domain.as_str(),
                        existing.scope,
                        existing.key,
                        now_ts.to_rfc3339()
                    ],
                )?;
                let mut existing = existing;
                existing.last_seen_at = now_ts;
                IdempotencyClaimResult::DuplicateCompleted(existing)
            }
            Some(existing)
                if existing.status == IdempotencyStatus::InProgress
                    && lock_is_active(existing.lock_expires_at, now_ts) =>
            {
                transaction.execute(
                    r#"
                    UPDATE idempotency_keys
                    SET last_seen_at = ?4
                    WHERE domain = ?1 AND scope = ?2 AND key = ?3
                    "#,
                    params![
                        existing.domain.as_str(),
                        existing.scope,
                        existing.key,
                        now_ts.to_rfc3339()
                    ],
                )?;
                let mut existing = existing;
                existing.last_seen_at = now_ts;
                IdempotencyClaimResult::DuplicateInFlight(existing)
            }
            Some(existing) => {
                transaction.execute(
                    r#"
                    UPDATE idempotency_keys
                    SET request_hash = ?4,
                        status = ?5,
                        response_json = NULL,
                        created_by = ?6,
                        last_seen_at = ?7,
                        lock_expires_at = ?8
                    WHERE domain = ?1 AND scope = ?2 AND key = ?3
                    "#,
                    params![
                        claim.domain.as_str(),
                        claim.scope,
                        claim.key,
                        claim.request_hash,
                        IdempotencyStatus::InProgress.as_str(),
                        claim.created_by,
                        now_ts.to_rfc3339(),
                        claim.lock_expires_at.map(|ts| ts.to_rfc3339())
                    ],
                )?;
                IdempotencyClaimResult::Claimed(IdempotencyKeyRecord {
                    domain: claim.domain,
                    scope: claim.scope,
                    key: claim.key,
                    request_hash: claim.request_hash,
                    status: IdempotencyStatus::InProgress,
                    response_payload: None,
                    created_by: claim.created_by,
                    created_at: existing.created_at,
                    last_seen_at: now_ts,
                    lock_expires_at: claim.lock_expires_at,
                })
            }
        };

        transaction.commit()?;
        Ok(result)
    }

    pub fn complete_idempotency_key(
        &self,
        domain: AccountDomain,
        scope: &str,
        key: &str,
        response_payload: &Value,
    ) -> Result<IdempotencyKeyRecord> {
        self.update_idempotency_key(
            domain,
            scope,
            key,
            IdempotencyStatus::Completed,
            Some(response_payload),
        )
    }

    pub fn fail_idempotency_key(
        &self,
        domain: AccountDomain,
        scope: &str,
        key: &str,
        response_payload: &Value,
    ) -> Result<IdempotencyKeyRecord> {
        self.update_idempotency_key(
            domain,
            scope,
            key,
            IdempotencyStatus::Failed,
            Some(response_payload),
        )
    }

    pub fn recovery_state(&self, domain: AccountDomain) -> Result<RecoveryState> {
        self.ensure_domain_matches(domain)?;
        Ok(RecoveryState {
            domain,
            generated_at: now(),
            runtime_mode: self.runtime_mode(domain)?,
            services: self.list_service_heartbeats(domain)?,
            recent_audit_events: self
                .recent_audit_events_for_domain(Some(domain), DEFAULT_RECOVERY_AUDIT_LIMIT)?,
            recent_events: self.replay_events(
                domain,
                ReplayCursor {
                    after_sequence: None,
                    limit: DEFAULT_RECOVERY_EVENT_LIMIT,
                },
            )?,
            snapshots: self.recent_snapshots(domain, DEFAULT_RECOVERY_SNAPSHOT_LIMIT)?,
            open_orders: self.list_open_orders(domain)?,
        })
    }

    pub fn create_backup(&self, target_path: impl AsRef<Path>) -> Result<()> {
        let target_path = target_path.as_ref();
        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create backup directory `{}`", parent.display())
            })?;
        }
        let connection = self.connection()?;
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)", [])?;
        connection.execute(
            "VACUUM INTO ?1",
            params![target_path.to_string_lossy().to_string()],
        )?;
        Ok(())
    }

    pub fn health_snapshot(&self, domain: AccountDomain) -> Result<HealthSnapshot> {
        self.ensure_domain_matches(domain)?;
        let runtime_health = self
            .latest_snapshot(domain, "runtime_health", "latest")?
            .map(|snapshot| serde_json::from_value::<RuntimeHealth>(snapshot.payload))
            .transpose()?;
        let recent_alerts = self.list_alerts(domain, None, None, 25)?;
        let metrics_summary = runtime_health.as_ref().map(|runtime| HealthMetricsSummary {
            heartbeat_age_ms: runtime.heartbeat_age_ms,
            market_ws_lag_ms: runtime.market_ws_lag_ms,
            user_ws_ok: runtime.user_ws_ok,
            capital_buffer_ok: runtime.capital_buffer_ok,
            fill_rate_5m: runtime.fill_rate_5m,
            open_orders: runtime.open_orders,
            reconcile_lag_ms: runtime.reconcile_lag_ms,
            disputed_capital_ratio: runtime.disputed_capital_ratio,
            last_alert_at: runtime.last_alert_at,
            degradation_reason: runtime.degradation_reason.clone(),
        });
        Ok(HealthSnapshot {
            generated_at: now(),
            runtime_modes: self.list_runtime_modes()?,
            services: self.list_service_heartbeats(domain)?,
            audit_events: self.recent_audit_events_for_domain(Some(domain), 25)?,
            recent_alerts,
            runtime_health,
            metrics_summary,
        })
    }

    pub fn upsert_rules_market(&self, canonical: MarketCanonical) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let canonical_json = serde_json::to_string(&canonical)?;
        transaction.execute(
            r#"
            INSERT INTO rules_markets (market_id, event_id, rules_version, canonical_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(market_id) DO UPDATE SET
                event_id = excluded.event_id,
                rules_version = excluded.rules_version,
                canonical_json = excluded.canonical_json,
                updated_at = excluded.updated_at
            "#,
            params![
                canonical.market_id,
                canonical.event_id,
                canonical.rules_version,
                canonical_json,
                canonical.updated_at.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            r#"
            INSERT INTO rule_versions (market_id, rules_version, created_at, raw_rules_text)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(market_id, rules_version) DO NOTHING
            "#,
            params![
                canonical.market_id,
                canonical.rules_version,
                canonical.updated_at.to_rfc3339(),
                canonical.raw_rules_text,
            ],
        )?;
        transaction.execute(
            r#"DELETE FROM clarification_timeline WHERE market_id = ?1"#,
            params![canonical.market_id],
        )?;
        for clarification in &canonical.clarifications {
            transaction.execute(
                r#"
                INSERT INTO clarification_timeline (market_id, rules_version, occurred_at, text)
                VALUES (?1, ?2, ?3, ?4)
                "#,
                params![
                    canonical.market_id,
                    canonical.rules_version,
                    clarification.occurred_at.to_rfc3339(),
                    clarification.text,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_rules_market(&self, market_id: &str) -> Result<Option<MarketCanonical>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT canonical_json
                FROM rules_markets
                WHERE market_id = ?1
                "#,
                params![market_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn list_all_rules_markets(&self) -> Result<Vec<MarketCanonical>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT canonical_json
            FROM rules_markets
            ORDER BY event_id, market_id
            "#,
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            let value = row?;
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(anyhow::Error::from)
    }

    pub fn list_rules_markets_for_event(&self, event_id: &str) -> Result<Vec<MarketCanonical>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT canonical_json
            FROM rules_markets
            WHERE event_id = ?1
            ORDER BY market_id
            "#,
        )?;
        let rows = statement.query_map(params![event_id], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            let value = row?;
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(anyhow::Error::from)
    }

    pub fn list_rule_versions(&self, market_id: &str) -> Result<Vec<RuleVersion>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT market_id, rules_version, created_at
            FROM rule_versions
            WHERE market_id = ?1
            ORDER BY created_at DESC
            "#,
        )?;
        let rows = statement.query_map(params![market_id], |row| {
            Ok(RuleVersion {
                market_id: row.get(0)?,
                rules_version: row.get(1)?,
                created_at: parse_timestamp(&row.get::<_, String>(2)?)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn upsert_event_family_snapshot(&self, snapshot: EventFamilySnapshot) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO event_families (event_id, version, snapshot_json, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(event_id) DO UPDATE SET
                version = excluded.version,
                snapshot_json = excluded.snapshot_json,
                updated_at = excluded.updated_at
            "#,
            params![
                snapshot.event_id,
                snapshot.version,
                serde_json::to_string(&snapshot)?,
                snapshot.generated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_event_family_snapshot(&self, event_id: &str) -> Result<Option<EventFamilySnapshot>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT snapshot_json
                FROM event_families
                WHERE event_id = ?1
                "#,
                params![event_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn upsert_constraint_graph_snapshot(
        &self,
        snapshot: ConstraintGraphSnapshot,
    ) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let scope_key = graph_scope_key(&snapshot.scope);
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            r#"
            INSERT INTO constraint_graph_snapshots (scope_key, version, snapshot_json, updated_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(scope_key) DO UPDATE SET
                version = excluded.version,
                snapshot_json = excluded.snapshot_json,
                updated_at = excluded.updated_at
            "#,
            params![
                scope_key,
                snapshot.version,
                serde_json::to_string(&snapshot)?,
                snapshot.generated_at.to_rfc3339(),
            ],
        )?;
        transaction.execute(
            r#"DELETE FROM constraint_edges WHERE scope_key = ?1"#,
            params![scope_key],
        )?;
        for edge in &snapshot.graph.edges {
            transaction.execute(
                r#"
                INSERT INTO constraint_edges (
                    edge_id, scope_key, src_market_id, dst_market_id, edge_type, confidence,
                    rules_version_src, rules_version_dst, evidence_json, created_at, invalidated_at
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
                "#,
                params![
                    edge.edge_id.to_string(),
                    scope_key,
                    edge.src_market_id,
                    edge.dst_market_id,
                    serde_json::to_string(&edge.edge_type)?,
                    serde_json::to_string(&edge.confidence)?,
                    edge.rules_version_src,
                    edge.rules_version_dst,
                    serde_json::to_string(&edge.evidence)?,
                    edge.created_at.to_rfc3339(),
                    edge.invalidated_at.map(|value| value.to_rfc3339()),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_constraint_graph_snapshot(
        &self,
        scope: &GraphScope,
    ) -> Result<Option<ConstraintGraphSnapshot>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let scope_key = graph_scope_key(scope);
        connection
            .query_row(
                r#"
                SELECT snapshot_json
                FROM constraint_graph_snapshots
                WHERE scope_key = ?1
                "#,
                params![scope_key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn list_constraint_edges(
        &self,
        scope: &GraphScope,
        market_id: Option<&str>,
    ) -> Result<Vec<ConstraintEdge>> {
        let snapshot = match self.get_constraint_graph_snapshot(scope)? {
            Some(snapshot) => snapshot,
            None => return Ok(Vec::new()),
        };
        Ok(snapshot
            .graph
            .edges
            .into_iter()
            .filter(|edge| {
                market_id
                    .map(|value| edge.src_market_id == value || edge.dst_market_id == value)
                    .unwrap_or(true)
            })
            .collect())
    }

    pub fn get_market_snapshot(
        &self,
        domain: AccountDomain,
        market_id: &str,
    ) -> Result<Option<MarketSnapshot>> {
        self.ensure_domain_matches(domain)?;
        self.latest_snapshot(domain, "market_snapshot", market_id)?
            .map(|snapshot| serde_json::from_value(snapshot.payload).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn list_market_snapshots(
        &self,
        domain: AccountDomain,
        market_ids: &[String],
    ) -> Result<Vec<MarketSnapshot>> {
        let mut snapshots = Vec::new();
        for market_id in market_ids {
            if let Some(snapshot) = self.get_market_snapshot(domain, market_id)? {
                snapshots.push(snapshot);
            }
        }
        Ok(snapshots)
    }

    pub fn upsert_opportunity_candidate(&self, candidate: OpportunityCandidate) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            r#"
            INSERT INTO opportunities (
                opportunity_id, strategy, event_id, market_refs_json, candidate_json, edge_net_bps, created_at, invalidated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(opportunity_id) DO UPDATE SET
                strategy = excluded.strategy,
                event_id = excluded.event_id,
                market_refs_json = excluded.market_refs_json,
                candidate_json = excluded.candidate_json,
                edge_net_bps = excluded.edge_net_bps,
                created_at = excluded.created_at,
                invalidated_at = excluded.invalidated_at
            "#,
            params![
                candidate.opportunity_id.to_string(),
                candidate.strategy.as_str(),
                candidate.event_id,
                serde_json::to_string(&candidate.market_refs)?,
                serde_json::to_string(&candidate)?,
                candidate.edge_net_bps,
                candidate.created_at.to_rfc3339(),
                candidate.invalidated_at.map(|value| value.to_rfc3339()),
            ],
        )?;
        transaction.execute(
            r#"DELETE FROM opportunity_links WHERE opportunity_id = ?1"#,
            params![candidate.opportunity_id.to_string()],
        )?;
        for market_id in &candidate.market_refs {
            transaction.execute(
                r#"
                INSERT INTO opportunity_links (opportunity_id, event_id, market_id)
                VALUES (?1, ?2, ?3)
                "#,
                params![
                    candidate.opportunity_id.to_string(),
                    candidate.event_id,
                    market_id,
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn get_opportunity(&self, opportunity_id: Uuid) -> Result<Option<OpportunityCandidate>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT candidate_json
                FROM opportunities
                WHERE opportunity_id = ?1
                "#,
                params![opportunity_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn list_opportunities(
        &self,
        strategy: Option<StrategyKind>,
        market_id: Option<&str>,
        event_id: Option<&str>,
        min_edge_net_bps: Option<i32>,
        limit: usize,
    ) -> Result<Vec<OpportunityCandidate>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT DISTINCT o.candidate_json
            FROM opportunities o
            LEFT JOIN opportunity_links l ON l.opportunity_id = o.opportunity_id
            WHERE (?1 IS NULL OR o.strategy = ?1)
              AND (?2 IS NULL OR l.market_id = ?2)
              AND (?3 IS NULL OR o.event_id = ?3)
              AND (?4 IS NULL OR o.edge_net_bps >= ?4)
            ORDER BY o.invalidated_at IS NOT NULL, o.edge_net_bps DESC, o.created_at DESC
            LIMIT ?5
            "#,
        )?;
        let rows = statement.query_map(
            params![
                strategy.map(|value| value.as_str()),
                market_id,
                event_id,
                min_edge_net_bps,
                limit.min(500),
            ],
            |row| row.get::<_, String>(0),
        )?;
        rows.map(|row| {
            let value = row?;
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(anyhow::Error::from)
    }

    pub fn record_scanner_run(&self, report: ScannerRunReport) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO scanner_runs (
                run_id, domain, scope_key, scanner, graph_version, book_version_hint,
                candidates_emitted, candidates_rejected, rejection_reasons_json,
                started_at, completed_at, report_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(run_id) DO UPDATE SET
                domain = excluded.domain,
                scope_key = excluded.scope_key,
                scanner = excluded.scanner,
                graph_version = excluded.graph_version,
                book_version_hint = excluded.book_version_hint,
                candidates_emitted = excluded.candidates_emitted,
                candidates_rejected = excluded.candidates_rejected,
                rejection_reasons_json = excluded.rejection_reasons_json,
                started_at = excluded.started_at,
                completed_at = excluded.completed_at,
                report_json = excluded.report_json
            "#,
            params![
                report.run_id.to_string(),
                report.domain.as_str(),
                report.scope_key,
                report.scanner.as_str(),
                report.graph_version,
                report.book_version_hint.map(|value| value.to_rfc3339()),
                report.candidates_emitted as i64,
                report.candidates_rejected as i64,
                serde_json::to_string(&report.rejection_reasons)?,
                report.started_at.to_rfc3339(),
                report.completed_at.to_rfc3339(),
                serde_json::to_string(&report)?,
            ],
        )?;
        Ok(())
    }

    pub fn list_scanner_runs(
        &self,
        strategy: Option<StrategyKind>,
        limit: usize,
    ) -> Result<Vec<ScannerRunReport>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT report_json
            FROM scanner_runs
            WHERE (?1 IS NULL OR scanner = ?1)
            ORDER BY completed_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(
            params![strategy.map(|value| value.as_str()), limit.min(500)],
            |row| row.get::<_, String>(0),
        )?;
        rows.map(|row| {
            let value = row?;
            serde_json::from_str(&value).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error))
            })
        })
        .collect::<rusqlite::Result<Vec<_>>>()
        .map_err(anyhow::Error::from)
    }

    pub fn get_scanner_run(&self, run_id: Uuid) -> Result<Option<ScannerRunReport>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT report_json
                FROM scanner_runs
                WHERE run_id = ?1
                "#,
                params![run_id.to_string()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(anyhow::Error::from))
            .transpose()
    }

    pub fn invalidate_opportunity(
        &self,
        mut candidate: OpportunityCandidate,
        reason: impl Into<String>,
    ) -> Result<OpportunityInvalidation> {
        let reason = reason.into();
        candidate.invalidated_at = Some(now());
        candidate.invalidation_reason = Some(reason.clone());
        self.upsert_opportunity_candidate(candidate.clone())?;
        let invalidation = OpportunityInvalidation {
            invalidation_id: Uuid::new_v4(),
            opportunity_id: candidate.opportunity_id,
            reason,
            invalidated_at: candidate.invalidated_at.expect("invalidated_at set"),
            affected_market_ids: candidate.market_refs.clone(),
        };
        self.record_opportunity_invalidation(invalidation.clone())?;
        Ok(invalidation)
    }

    pub fn invalidate_opportunities_for_markets(
        &self,
        market_ids: &[String],
        reason: impl Into<String>,
    ) -> Result<Vec<OpportunityInvalidation>> {
        let reason = reason.into();
        let mut invalidations = Vec::new();
        for market_id in market_ids {
            for candidate in self.list_opportunities(None, Some(market_id), None, None, 1_000)? {
                if candidate.invalidated_at.is_some() {
                    continue;
                }
                invalidations.push(self.invalidate_opportunity(candidate, reason.clone())?);
            }
        }
        invalidations.sort_by_key(|item| item.opportunity_id);
        invalidations.dedup_by_key(|item| item.opportunity_id);
        Ok(invalidations)
    }

    pub fn record_opportunity_invalidation(
        &self,
        invalidation: OpportunityInvalidation,
    ) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO opportunity_invalidations (
                invalidation_id, opportunity_id, reason, invalidated_at, affected_market_ids_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(invalidation_id) DO UPDATE SET
                opportunity_id = excluded.opportunity_id,
                reason = excluded.reason,
                invalidated_at = excluded.invalidated_at,
                affected_market_ids_json = excluded.affected_market_ids_json
            "#,
            params![
                invalidation.invalidation_id.to_string(),
                invalidation.opportunity_id.to_string(),
                invalidation.reason,
                invalidation.invalidated_at.to_rfc3339(),
                serde_json::to_string(&invalidation.affected_market_ids)?,
            ],
        )?;
        Ok(())
    }

    pub fn list_opportunity_invalidations(
        &self,
        opportunity_id: Option<Uuid>,
        limit: usize,
    ) -> Result<Vec<OpportunityInvalidation>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT invalidation_id, opportunity_id, reason, invalidated_at, affected_market_ids_json
            FROM opportunity_invalidations
            WHERE (?1 IS NULL OR opportunity_id = ?1)
            ORDER BY invalidated_at DESC
            LIMIT ?2
            "#,
        )?;
        let rows = statement.query_map(
            params![
                opportunity_id.map(|value| value.to_string()),
                limit.min(500)
            ],
            |row| {
                Ok(OpportunityInvalidation {
                    invalidation_id: parse_uuid(&row.get::<_, String>(0)?)?,
                    opportunity_id: parse_uuid(&row.get::<_, String>(1)?)?,
                    reason: row.get(2)?,
                    invalidated_at: parse_timestamp(&row.get::<_, String>(3)?)?,
                    affected_market_ids: serde_json::from_str(&row.get::<_, String>(4)?).map_err(
                        |error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                4,
                                Type::Text,
                                Box::new(error),
                            )
                        },
                    )?,
                })
            },
        )?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn replay_opportunity_input_events(
        &self,
        domain: AccountDomain,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<Vec<DurableEvent>> {
        let events = self.replay_events(
            domain,
            ReplayCursor {
                after_sequence,
                limit: limit.saturating_mul(3).max(limit),
            },
        )?;
        Ok(events
            .into_iter()
            .filter(|event| {
                event.aggregate_type == "market_snapshot"
                    || event.aggregate_type == "rules_constraint_graph"
                    || event.aggregate_type == "rules_invalidation"
            })
            .take(limit)
            .collect())
    }

    pub fn replay_rules_market_events(
        &self,
        domain: AccountDomain,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<Vec<DurableEvent>> {
        let events = self.replay_events(
            domain,
            ReplayCursor {
                after_sequence,
                limit,
            },
        )?;
        Ok(events
            .into_iter()
            .filter(|event| event.aggregate_type == "market_snapshot")
            .collect())
    }

    pub fn record_rules_replay_run(&self, report: ReplayReport) -> Result<()> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection.execute(
            r#"
            INSERT INTO rules_replay_runs (
                run_id, requested_at, completed_at, event_limit, processed_events, graph_versions_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
            params![
                report.run_id.to_string(),
                report.requested_at.to_rfc3339(),
                report.completed_at.to_rfc3339(),
                report.event_limit as i64,
                report.processed_events as i64,
                serde_json::to_string(&report.graph_versions)?,
            ],
        )?;
        Ok(())
    }

    pub fn list_rules_replay_runs(&self, limit: usize) -> Result<Vec<ReplayReport>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            r#"
            SELECT run_id, requested_at, completed_at, event_limit, processed_events, graph_versions_json
            FROM rules_replay_runs
            ORDER BY requested_at DESC
            LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map(params![limit as i64], map_rules_replay_report)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(anyhow::Error::from)
    }

    pub fn get_rules_replay_run(&self, run_id: Uuid) -> Result<Option<ReplayReport>> {
        self.ensure_rules_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT run_id, requested_at, completed_at, event_limit, processed_events, graph_versions_json
                FROM rules_replay_runs
                WHERE run_id = ?1
                "#,
                params![run_id.to_string()],
                map_rules_replay_report,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    fn ensure_rules_schema(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS rules_markets (
                market_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                rules_version TEXT NOT NULL,
                canonical_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rules_markets_event_id
                ON rules_markets (event_id, updated_at DESC);

            CREATE TABLE IF NOT EXISTS rule_versions (
                market_id TEXT NOT NULL,
                rules_version TEXT NOT NULL,
                created_at TEXT NOT NULL,
                raw_rules_text TEXT NOT NULL,
                PRIMARY KEY (market_id, rules_version)
            );
            CREATE INDEX IF NOT EXISTS idx_rule_versions_market_created_at
                ON rule_versions (market_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS clarification_timeline (
                market_id TEXT NOT NULL,
                rules_version TEXT NOT NULL,
                occurred_at TEXT NOT NULL,
                text TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_clarification_timeline_market
                ON clarification_timeline (market_id, occurred_at DESC);

            CREATE TABLE IF NOT EXISTS event_families (
                event_id TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS constraint_graph_snapshots (
                scope_key TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS constraint_edges (
                edge_id TEXT PRIMARY KEY,
                scope_key TEXT NOT NULL,
                src_market_id TEXT NOT NULL,
                dst_market_id TEXT NOT NULL,
                edge_type TEXT NOT NULL,
                confidence TEXT NOT NULL,
                rules_version_src TEXT NOT NULL,
                rules_version_dst TEXT NOT NULL,
                evidence_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                invalidated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_constraint_edges_scope_key
                ON constraint_edges (scope_key, src_market_id, dst_market_id);

            CREATE TABLE IF NOT EXISTS rules_replay_runs (
                run_id TEXT PRIMARY KEY,
                requested_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                event_limit INTEGER NOT NULL,
                processed_events INTEGER NOT NULL,
                graph_versions_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_rules_replay_runs_requested_at
                ON rules_replay_runs (requested_at DESC);

            CREATE TABLE IF NOT EXISTS opportunities (
                opportunity_id TEXT PRIMARY KEY,
                strategy TEXT NOT NULL,
                event_id TEXT NOT NULL,
                market_refs_json TEXT NOT NULL,
                candidate_json TEXT NOT NULL,
                edge_net_bps INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                invalidated_at TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_opportunities_strategy_edge
                ON opportunities (strategy, edge_net_bps DESC, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_opportunities_event_created
                ON opportunities (event_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS opportunity_links (
                opportunity_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                PRIMARY KEY (opportunity_id, market_id)
            );
            CREATE INDEX IF NOT EXISTS idx_opportunity_links_market
                ON opportunity_links (market_id, event_id);

            CREATE TABLE IF NOT EXISTS opportunity_invalidations (
                invalidation_id TEXT PRIMARY KEY,
                opportunity_id TEXT NOT NULL,
                reason TEXT NOT NULL,
                invalidated_at TEXT NOT NULL,
                affected_market_ids_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_opportunity_invalidations_opportunity
                ON opportunity_invalidations (opportunity_id, invalidated_at DESC);

            CREATE TABLE IF NOT EXISTS scanner_runs (
                run_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                scanner TEXT NOT NULL,
                graph_version INTEGER NOT NULL,
                book_version_hint TEXT,
                candidates_emitted INTEGER NOT NULL,
                candidates_rejected INTEGER NOT NULL,
                rejection_reasons_json TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT NOT NULL,
                report_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_scanner_runs_scope_completed
                ON scanner_runs (scope_key, completed_at DESC);
            CREATE INDEX IF NOT EXISTS idx_scanner_runs_scanner_completed
                ON scanner_runs (scanner, completed_at DESC);
            "#,
        )?;
        Ok(())
    }

    fn update_idempotency_key(
        &self,
        domain: AccountDomain,
        scope: &str,
        key: &str,
        status: IdempotencyStatus,
        response_payload: Option<&Value>,
    ) -> Result<IdempotencyKeyRecord> {
        self.ensure_domain_matches(domain)?;
        let response_json = response_payload.map(serde_json::to_string).transpose()?;
        let connection = self.connection()?;
        let updated = connection.execute(
            r#"
            UPDATE idempotency_keys
            SET status = ?4,
                response_json = ?5,
                last_seen_at = ?6,
                lock_expires_at = NULL
            WHERE domain = ?1 AND scope = ?2 AND key = ?3
            "#,
            params![
                domain.as_str(),
                scope,
                key,
                status.as_str(),
                response_json,
                now().to_rfc3339()
            ],
        )?;
        if updated == 0 {
            bail!(
                "idempotency key `{}/{}` not found for domain `{}`",
                scope,
                key,
                domain
            );
        }

        self.fetch_idempotency_key(domain, scope, key)?
            .ok_or_else(|| anyhow!("idempotency key disappeared after update"))
    }

    fn fetch_idempotency_key(
        &self,
        domain: AccountDomain,
        scope: &str,
        key: &str,
    ) -> Result<Option<IdempotencyKeyRecord>> {
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT domain, scope, key, request_hash, status, response_json, created_by,
                       created_at, last_seen_at, lock_expires_at
                FROM idempotency_keys
                WHERE domain = ?1 AND scope = ?2 AND key = ?3
                "#,
                params![domain.as_str(), scope, key],
                map_idempotency_record,
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    fn verify_metadata(&self) -> Result<()> {
        let connection = self.connection()?;
        let metadata = connection
            .query_row(
                r#"
                SELECT owner_domain, namespace, audit_label
                FROM domain_metadata
                LIMIT 1
                "#,
                [],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;

        let Some((owner_domain, namespace, audit_label)) = metadata else {
            bail!(
                "database `{}` is missing domain metadata",
                self.db_path.display()
            );
        };

        ensure!(
            owner_domain == self.owner_domain.as_str(),
            "database `{}` belongs to domain `{}`, not `{}`",
            self.db_path.display(),
            owner_domain,
            self.owner_domain
        );
        ensure!(
            namespace == self.namespace,
            "database `{}` namespace `{}` does not match expected namespace `{}`",
            self.db_path.display(),
            namespace,
            self.namespace
        );
        ensure!(
            audit_label == self.audit_label,
            "database `{}` audit label `{}` does not match expected audit label `{}`",
            self.db_path.display(),
            audit_label,
            self.audit_label
        );
        Ok(())
    }

    fn ensure_domain_matches(&self, domain: AccountDomain) -> Result<()> {
        ensure!(
            domain == self.owner_domain,
            "store at `{}` is bound to domain `{}`, not `{}`",
            self.db_path.display(),
            self.owner_domain,
            domain
        );
        Ok(())
    }

    fn connection(&self) -> Result<Connection> {
        let connection = Connection::open_with_flags(
            &self.db_path,
            OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_FULL_MUTEX,
        )
        .with_context(|| {
            format!(
                "failed to open sqlite database `{}`",
                self.db_path.display()
            )
        })?;
        connection.busy_timeout(SQLITE_BUSY_TIMEOUT)?;
        Ok(connection)
    }
}

fn clamp_limit(limit: usize, default_limit: usize) -> usize {
    match limit {
        0 => default_limit,
        value => value.min(10_000),
    }
}

fn bool_to_sqlite(value: bool) -> i64 {
    if value {
        1
    } else {
        0
    }
}

fn default_rollout_stage_for_domain(domain: AccountDomain) -> PromotionStage {
    match domain {
        AccountDomain::Sim => PromotionStage::Replay,
        AccountDomain::Canary => PromotionStage::Canary,
        AccountDomain::Live => PromotionStage::Live,
    }
}

fn lock_is_active(
    lock_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    now_ts: chrono::DateTime<chrono::Utc>,
) -> bool {
    match lock_expires_at {
        Some(expires_at) => expires_at > now_ts,
        None => true,
    }
}

fn map_runtime_mode_record(row: &Row<'_>) -> rusqlite::Result<RuntimeModeRecord> {
    Ok(RuntimeModeRecord {
        domain: parse_account_domain(&row.get::<_, String>(0)?)?,
        mode: parse_runtime_mode(&row.get::<_, String>(1)?)?,
        reason: row.get(2)?,
        updated_at: parse_timestamp(&row.get::<_, String>(3)?)?,
    })
}

fn map_rollout_stage_record(row: &Row<'_>) -> rusqlite::Result<RolloutStageRecord> {
    Ok(RolloutStageRecord {
        domain: parse_account_domain(&row.get::<_, String>(0)?)?,
        stage: parse_promotion_stage(&row.get::<_, String>(1)?)?,
        previous_stage: row
            .get::<_, Option<String>>(2)?
            .map(|value| parse_promotion_stage(&value))
            .transpose()?,
        approved_by: row.get(3)?,
        approved_at: row
            .get::<_, Option<String>>(4)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        reason: row.get(5)?,
        updated_at: parse_timestamp(&row.get::<_, String>(6)?)?,
    })
}

fn map_service_heartbeat(row: &Row<'_>) -> rusqlite::Result<ServiceHeartbeat> {
    Ok(ServiceHeartbeat {
        service: parse_service_kind(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        healthy: row.get::<_, i64>(2)? != 0,
        detail: row.get(3)?,
        last_seen_at: parse_timestamp(&row.get::<_, String>(4)?)?,
    })
}

fn map_audit_event(row: &Row<'_>) -> rusqlite::Result<AuditEvent> {
    Ok(AuditEvent {
        id: parse_uuid(&row.get::<_, String>(0)?)?,
        occurred_at: parse_timestamp(&row.get::<_, String>(1)?)?,
        service: row.get(2)?,
        domain: row
            .get::<_, Option<String>>(3)?
            .map(|value| parse_account_domain(&value))
            .transpose()?,
        action: row.get(4)?,
        detail: row.get(5)?,
    })
}

fn map_alert_event(row: &Row<'_>) -> rusqlite::Result<AlertEvent> {
    Ok(AlertEvent {
        alert_id: parse_uuid(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        rule_kind: parse_alert_rule_kind(&row.get::<_, String>(2)?)?,
        severity: parse_alert_severity(&row.get::<_, String>(3)?)?,
        status: parse_alert_status(&row.get::<_, String>(4)?)?,
        dedupe_key: row.get(5)?,
        source: row.get(6)?,
        summary: row.get(7)?,
        detail: row.get(8)?,
        metric_key: row.get(9)?,
        metric_value: row.get(10)?,
        threshold: row.get(11)?,
        trigger_value: row.get(12)?,
        labels: serde_json::from_str(&row.get::<_, String>(13)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(13, Type::Text, Box::new(error))
        })?,
        created_at: parse_timestamp(&row.get::<_, String>(14)?)?,
        updated_at: parse_timestamp(&row.get::<_, String>(15)?)?,
        acknowledged_at: row
            .get::<_, Option<String>>(16)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        acknowledged_by: row.get(17)?,
        resolved_at: row
            .get::<_, Option<String>>(18)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        audit_event_id: row
            .get::<_, Option<String>>(19)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        replay_job_id: row
            .get::<_, Option<String>>(20)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        replay_run_id: row
            .get::<_, Option<String>>(21)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        last_sent_at: row
            .get::<_, Option<String>>(22)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
    })
}

fn map_metric_sample(row: &Row<'_>) -> rusqlite::Result<MetricSample> {
    Ok(MetricSample {
        sample_id: parse_uuid(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        metric_key: row.get(2)?,
        metric_kind: row.get(3)?,
        value: row.get(4)?,
        labels: serde_json::from_str(&row.get::<_, String>(5)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
        })?,
        observed_at: parse_timestamp(&row.get::<_, String>(6)?)?,
    })
}

fn map_durable_event(row: &Row<'_>) -> rusqlite::Result<DurableEvent> {
    Ok(DurableEvent {
        sequence: row.get(0)?,
        event_id: parse_uuid(&row.get::<_, String>(1)?)?,
        domain: parse_account_domain(&row.get::<_, String>(2)?)?,
        stream: row.get(3)?,
        aggregate_type: row.get(4)?,
        aggregate_id: row.get(5)?,
        event_type: row.get(6)?,
        causation_id: row
            .get::<_, Option<String>>(7)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        correlation_id: row
            .get::<_, Option<String>>(8)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        idempotency_key: row.get(9)?,
        payload: parse_json_value(&row.get::<_, String>(10)?)?,
        metadata: parse_json_value(&row.get::<_, String>(11)?)?,
        created_at: parse_timestamp(&row.get::<_, String>(12)?)?,
        persisted_at: parse_timestamp(&row.get::<_, String>(13)?)?,
    })
}

fn map_snapshot(row: &Row<'_>) -> rusqlite::Result<StateSnapshot> {
    Ok(StateSnapshot {
        snapshot_id: parse_uuid(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        aggregate_type: row.get(2)?,
        aggregate_id: row.get(3)?,
        version: row.get(4)?,
        payload: parse_json_value(&row.get::<_, String>(5)?)?,
        derived_from_sequence: row.get(6)?,
        created_at: parse_timestamp(&row.get::<_, String>(7)?)?,
    })
}

fn map_order_lifecycle(row: &Row<'_>) -> rusqlite::Result<OrderLifecycleRecord> {
    Ok(OrderLifecycleRecord {
        domain: parse_account_domain(&row.get::<_, String>(0)?)?,
        order_id: row.get(1)?,
        market_id: row.get(2)?,
        status: parse_order_lifecycle_status(&row.get::<_, String>(3)?)?,
        client_order_id: row.get(4)?,
        external_order_id: row.get(5)?,
        idempotency_key: row.get(6)?,
        side: row.get(7)?,
        limit_price: row.get(8)?,
        order_quantity: row.get(9)?,
        filled_quantity: row.get(10)?,
        average_fill_price: row.get(11)?,
        last_event_sequence: row.get(12)?,
        detail: parse_json_value(&row.get::<_, String>(13)?)?,
        opened_at: parse_timestamp(&row.get::<_, String>(14)?)?,
        updated_at: parse_timestamp(&row.get::<_, String>(15)?)?,
        closed_at: row
            .get::<_, Option<String>>(16)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
    })
}

fn map_idempotency_record(row: &Row<'_>) -> rusqlite::Result<IdempotencyKeyRecord> {
    Ok(IdempotencyKeyRecord {
        domain: parse_account_domain(&row.get::<_, String>(0)?)?,
        scope: row.get(1)?,
        key: row.get(2)?,
        request_hash: row.get(3)?,
        status: parse_idempotency_status(&row.get::<_, String>(4)?)?,
        response_payload: row
            .get::<_, Option<String>>(5)?
            .map(|value| parse_json_value(&value))
            .transpose()?,
        created_by: row.get(6)?,
        created_at: parse_timestamp(&row.get::<_, String>(7)?)?,
        last_seen_at: parse_timestamp(&row.get::<_, String>(8)?)?,
        lock_expires_at: row
            .get::<_, Option<String>>(9)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
    })
}

fn map_execution_intent_record(row: &Row<'_>) -> rusqlite::Result<ExecutionIntentRecord> {
    Ok(ExecutionIntentRecord {
        intent_id: parse_uuid(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        batch_id: row
            .get::<_, Option<String>>(2)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        strategy_kind: StrategyKind::from_str(&row.get::<_, String>(3)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(3, Type::Text, Box::new(error))
        })?,
        market_id: row.get(4)?,
        token_id: row.get(5)?,
        side: TradeSide::from_str(&row.get::<_, String>(6)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(6, Type::Text, Box::new(error))
        })?,
        limit_price: row.get(7)?,
        target_size: row.get(8)?,
        idempotency_key: row.get(9)?,
        client_order_id: row.get(10)?,
        status: parse_execution_intent_status(&row.get::<_, String>(11)?)?,
        detail: parse_json_value(&row.get::<_, String>(12)?)?,
        created_at: parse_timestamp(&row.get::<_, String>(13)?)?,
        updated_at: parse_timestamp(&row.get::<_, String>(14)?)?,
        expires_at: parse_timestamp(&row.get::<_, String>(15)?)?,
    })
}

fn parse_timestamp(value: &str) -> rusqlite::Result<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc))
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_uuid(value: &str) -> rusqlite::Result<Uuid> {
    Uuid::parse_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_json_value(value: &str) -> rusqlite::Result<Value> {
    serde_json::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_replay_job_status(value: &str) -> rusqlite::Result<ReplayJobStatus> {
    ReplayJobStatus::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_alert_rule_kind(value: &str) -> rusqlite::Result<AlertRuleKind> {
    AlertRuleKind::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_alert_severity(value: &str) -> rusqlite::Result<AlertSeverity> {
    AlertSeverity::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_alert_status(value: &str) -> rusqlite::Result<AlertStatus> {
    AlertStatus::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn graph_scope_key(scope: &GraphScope) -> String {
    match scope {
        GraphScope::All => "ALL".to_owned(),
        GraphScope::Event { event_id } => format!("EVENT:{event_id}"),
        GraphScope::Market { market_id } => format!("MARKET:{market_id}"),
    }
}

fn map_rules_replay_report(row: &Row<'_>) -> rusqlite::Result<ReplayReport> {
    Ok(ReplayReport {
        run_id: parse_uuid(&row.get::<_, String>(0)?)?,
        requested_at: parse_timestamp(&row.get::<_, String>(1)?)?,
        completed_at: parse_timestamp(&row.get::<_, String>(2)?)?,
        event_limit: row.get::<_, i64>(3)? as usize,
        processed_events: row.get::<_, i64>(4)? as usize,
        graph_versions: serde_json::from_str(&row.get::<_, String>(5)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
        })?,
    })
}

fn map_replay_job(row: &Row<'_>) -> rusqlite::Result<ReplayJob> {
    Ok(ReplayJob {
        job_id: parse_uuid(&row.get::<_, String>(0)?)?,
        domain: parse_account_domain(&row.get::<_, String>(1)?)?,
        requested_by: row.get(2)?,
        requested_at: parse_timestamp(&row.get::<_, String>(3)?)?,
        after_sequence: row.get(4)?,
        limit: row.get::<_, i64>(5)? as usize,
        status: parse_replay_job_status(&row.get::<_, String>(6)?)?,
        started_at: row
            .get::<_, Option<String>>(7)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        completed_at: row
            .get::<_, Option<String>>(8)?
            .map(|value| parse_timestamp(&value))
            .transpose()?,
        error: row.get(9)?,
        run_id: row
            .get::<_, Option<String>>(10)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        worker_id: row.get(11)?,
        reason: row.get(12)?,
        alert_id: row
            .get::<_, Option<String>>(13)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
        audit_event_id: row
            .get::<_, Option<String>>(14)?
            .map(|value| parse_uuid(&value))
            .transpose()?,
    })
}

fn parse_account_domain(value: &str) -> rusqlite::Result<AccountDomain> {
    AccountDomain::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_runtime_mode(value: &str) -> rusqlite::Result<RuntimeMode> {
    RuntimeMode::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_promotion_stage(value: &str) -> rusqlite::Result<PromotionStage> {
    PromotionStage::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_idempotency_status(value: &str) -> rusqlite::Result<IdempotencyStatus> {
    IdempotencyStatus::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_order_lifecycle_status(value: &str) -> rusqlite::Result<OrderLifecycleStatus> {
    OrderLifecycleStatus::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_execution_intent_status(value: &str) -> rusqlite::Result<ExecutionIntentStatus> {
    ExecutionIntentStatus::from_str(value)
        .map_err(|error| rusqlite::Error::FromSqlConversionFailure(0, Type::Text, Box::new(error)))
}

fn parse_service_kind(value: &str) -> rusqlite::Result<ServiceKind> {
    match value {
        "MD_GATEWAY" => Ok(ServiceKind::MdGateway),
        "RULES_ENGINE" => Ok(ServiceKind::RulesEngine),
        "OPPORTUNITY_ENGINE" => Ok(ServiceKind::OpportunityEngine),
        "PORTFOLIO_ENGINE" => Ok(ServiceKind::PortfolioEngine),
        "RISK_ENGINE" => Ok(ServiceKind::RiskEngine),
        "SIM_ENGINE" => Ok(ServiceKind::SimEngine),
        "EXECUTION_ENGINE" => Ok(ServiceKind::ExecutionEngine),
        "SETTLEMENT_ENGINE" => Ok(ServiceKind::SettlementEngine),
        _ => Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            Type::Text,
            Box::new(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown service kind `{value}`"),
            )),
        )),
    }
}

impl Store {
    pub fn append_sim_event(&self, event: NewSimEvent) -> Result<SimEventRecord> {
        self.ensure_domain_matches(event.domain)?;
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        let payload_json = serde_json::to_string(&event.payload)?;
        connection.execute(
            r#"
            INSERT INTO sim_events (
                run_id, domain, mode, sequence, event_kind, event_time, market_id, payload_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                event.run_id.to_string(),
                event.domain.as_str(),
                event.mode.as_str(),
                event.sequence as i64,
                event.event_kind.as_str(),
                event.event_time.to_rfc3339(),
                event.market_id,
                payload_json,
            ],
        )?;
        Ok(SimEventRecord {
            run_id: event.run_id,
            sequence: event.sequence,
            mode: event.mode,
            event_kind: event.event_kind,
            event_time: event.event_time,
            market_id: event.market_id,
            payload: event.payload,
        })
    }

    pub fn record_sim_order_state(&self, order: NewSimOrderRecord) -> Result<SimOrderRecord> {
        self.ensure_domain_matches(order.domain)?;
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        let detail_json = serde_json::to_string(&order.detail)?;
        connection.execute(
            r#"
            INSERT INTO sim_orders (
                run_id, domain, mode, order_id, client_order_id, market_id, status,
                filled_quantity, average_fill_price, detail_json, updated_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(run_id, order_id) DO UPDATE SET
                mode = excluded.mode,
                client_order_id = excluded.client_order_id,
                market_id = excluded.market_id,
                status = excluded.status,
                filled_quantity = excluded.filled_quantity,
                average_fill_price = excluded.average_fill_price,
                detail_json = excluded.detail_json,
                updated_at = excluded.updated_at
            "#,
            params![
                order.run_id.to_string(),
                order.domain.as_str(),
                order.mode.as_str(),
                order.order_id,
                order.client_order_id,
                order.market_id,
                order.status.as_str(),
                order.filled_quantity,
                order.average_fill_price,
                detail_json,
                order.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(SimOrderRecord {
            run_id: order.run_id,
            mode: order.mode,
            order_id: order.order_id,
            client_order_id: order.client_order_id,
            market_id: order.market_id,
            status: order.status,
            filled_quantity: order.filled_quantity,
            average_fill_price: order.average_fill_price,
            detail: order.detail,
            updated_at: order.updated_at,
        })
    }

    pub fn record_sim_fill(&self, fill: NewSimFillRecord) -> Result<SimFillRecord> {
        self.ensure_domain_matches(fill.domain)?;
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        let detail_json = serde_json::to_string(&fill.detail)?;
        let fill_id = Uuid::new_v4();
        connection.execute(
            r#"
            INSERT INTO sim_fills (
                fill_id, run_id, domain, mode, order_id, market_id, quantity, price,
                fees_paid, filled_at, detail_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                fill_id.to_string(),
                fill.run_id.to_string(),
                fill.domain.as_str(),
                fill.mode.as_str(),
                fill.order_id,
                fill.market_id,
                fill.quantity,
                fill.price,
                fill.fees_paid,
                fill.filled_at.to_rfc3339(),
                detail_json,
            ],
        )?;
        Ok(SimFillRecord {
            fill_id,
            run_id: fill.run_id,
            mode: fill.mode,
            order_id: fill.order_id,
            market_id: fill.market_id,
            quantity: fill.quantity,
            price: fill.price,
            fees_paid: fill.fees_paid,
            filled_at: fill.filled_at,
            detail: fill.detail,
        })
    }

    pub fn upsert_replay_checkpoint(
        &self,
        checkpoint: NewReplayCheckpoint,
    ) -> Result<ReplayCheckpoint> {
        self.ensure_domain_matches(checkpoint.domain)?;
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        let metadata_json = serde_json::to_string(&checkpoint.metadata)?;
        connection.execute(
            r#"
            INSERT INTO replay_checkpoints (
                run_id, domain, mode, cursor, processed_events, updated_at, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(run_id) DO UPDATE SET
                mode = excluded.mode,
                cursor = excluded.cursor,
                processed_events = excluded.processed_events,
                updated_at = excluded.updated_at,
                metadata_json = excluded.metadata_json
            "#,
            params![
                checkpoint.run_id.to_string(),
                checkpoint.domain.as_str(),
                checkpoint.mode.as_str(),
                checkpoint.cursor,
                checkpoint.processed_events as i64,
                checkpoint.updated_at.to_rfc3339(),
                metadata_json,
            ],
        )?;
        Ok(ReplayCheckpoint {
            run_id: checkpoint.run_id,
            mode: checkpoint.mode,
            cursor: checkpoint.cursor,
            processed_events: checkpoint.processed_events,
            updated_at: checkpoint.updated_at,
            metadata: checkpoint.metadata,
        })
    }

    pub fn finalize_sim_run(&self, report: &SimRunReport) -> Result<()> {
        self.ensure_domain_matches(report.domain)?;
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        let drift_notes_json = serde_json::to_string(&report.drift.notes)?;
        let risk_decisions_json = serde_json::to_string(&report.risk_decisions)?;
        connection.execute(
            r#"
            INSERT INTO sim_runs (
                run_id, domain, mode, started_at, completed_at, input_snapshot_hash,
                rule_version_hash, processed_events, orders_emitted, fills_emitted,
                intent_match_rate_bps, reject_rate_bps, fill_rate_bps, fee_leakage_bps,
                average_latency_ms, p95_latency_ms, baseline_orders, simulated_orders,
                drift_score_bps, severity, notes_json, risk_decisions_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22)
            ON CONFLICT(run_id) DO UPDATE SET
                completed_at = excluded.completed_at,
                processed_events = excluded.processed_events,
                orders_emitted = excluded.orders_emitted,
                fills_emitted = excluded.fills_emitted,
                intent_match_rate_bps = excluded.intent_match_rate_bps,
                reject_rate_bps = excluded.reject_rate_bps,
                fill_rate_bps = excluded.fill_rate_bps,
                fee_leakage_bps = excluded.fee_leakage_bps,
                average_latency_ms = excluded.average_latency_ms,
                p95_latency_ms = excluded.p95_latency_ms,
                baseline_orders = excluded.baseline_orders,
                simulated_orders = excluded.simulated_orders,
                drift_score_bps = excluded.drift_score_bps,
                severity = excluded.severity,
                notes_json = excluded.notes_json,
                risk_decisions_json = excluded.risk_decisions_json
            "#,
            params![
                report.run_id.to_string(),
                report.domain.as_str(),
                report.mode.as_str(),
                report.started_at.to_rfc3339(),
                report.completed_at.map(|value| value.to_rfc3339()),
                report.input_snapshot_hash,
                report.rule_version_hash,
                report.processed_events as i64,
                report.orders_emitted as i64,
                report.fills_emitted as i64,
                report.drift.intent_match_rate_bps as i64,
                report.drift.reject_rate_bps as i64,
                report.drift.fill_rate_bps as i64,
                report.drift.fee_leakage_bps,
                report.drift.average_latency_ms as i64,
                report.drift.p95_latency_ms as i64,
                report.drift.baseline_orders as i64,
                report.drift.simulated_orders as i64,
                report.drift.drift_score_bps as i64,
                report.drift.severity.as_str(),
                drift_notes_json,
                risk_decisions_json,
            ],
        )?;
        connection.execute(
            r#"
            INSERT INTO sim_drift_reports (
                run_id, domain, mode, intent_match_rate_bps, reject_rate_bps, fill_rate_bps,
                fee_leakage_bps, average_latency_ms, p95_latency_ms, baseline_orders,
                simulated_orders, drift_score_bps, severity, notes_json, recorded_at
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            ON CONFLICT(run_id) DO UPDATE SET
                intent_match_rate_bps = excluded.intent_match_rate_bps,
                reject_rate_bps = excluded.reject_rate_bps,
                fill_rate_bps = excluded.fill_rate_bps,
                fee_leakage_bps = excluded.fee_leakage_bps,
                average_latency_ms = excluded.average_latency_ms,
                p95_latency_ms = excluded.p95_latency_ms,
                baseline_orders = excluded.baseline_orders,
                simulated_orders = excluded.simulated_orders,
                drift_score_bps = excluded.drift_score_bps,
                severity = excluded.severity,
                notes_json = excluded.notes_json,
                recorded_at = excluded.recorded_at
            "#,
            params![
                report.run_id.to_string(),
                report.domain.as_str(),
                report.mode.as_str(),
                report.drift.intent_match_rate_bps as i64,
                report.drift.reject_rate_bps as i64,
                report.drift.fill_rate_bps as i64,
                report.drift.fee_leakage_bps,
                report.drift.average_latency_ms as i64,
                report.drift.p95_latency_ms as i64,
                report.drift.baseline_orders as i64,
                report.drift.simulated_orders as i64,
                report.drift.drift_score_bps as i64,
                report.drift.severity.as_str(),
                drift_notes_json,
                now().to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn load_replay_checkpoint(&self, run_id: Uuid) -> Result<Option<ReplayCheckpoint>> {
        self.ensure_sim_schema()?;
        let connection = self.connection()?;
        connection
            .query_row(
                r#"
                SELECT run_id, mode, cursor, processed_events, updated_at, metadata_json
                FROM replay_checkpoints
                WHERE run_id = ?1
                "#,
                params![run_id.to_string()],
                |row| {
                    let mode = SimMode::from_str(row.get_ref(1)?.as_str()?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(1, Type::Text, Box::new(error))
                    })?;
                    let metadata_json: String = row.get(5)?;
                    Ok(ReplayCheckpoint {
                        run_id: parse_uuid(&row.get::<_, String>(0)?)?,
                        mode,
                        cursor: row.get(2)?,
                        processed_events: row.get::<_, i64>(3)? as u64,
                        updated_at: parse_timestamp(&row.get::<_, String>(4)?)?,
                        metadata: serde_json::from_str(&metadata_json).map_err(|error| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                Type::Text,
                                Box::new(error),
                            )
                        })?,
                    })
                },
            )
            .optional()
            .map_err(anyhow::Error::from)
    }

    fn ensure_sim_schema(&self) -> Result<()> {
        let connection = self.connection()?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sim_runs (
                run_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                input_snapshot_hash TEXT NOT NULL,
                rule_version_hash TEXT NOT NULL,
                processed_events INTEGER NOT NULL,
                orders_emitted INTEGER NOT NULL,
                fills_emitted INTEGER NOT NULL,
                intent_match_rate_bps INTEGER NOT NULL,
                reject_rate_bps INTEGER NOT NULL,
                fill_rate_bps INTEGER NOT NULL,
                fee_leakage_bps INTEGER NOT NULL,
                average_latency_ms INTEGER NOT NULL,
                p95_latency_ms INTEGER NOT NULL,
                baseline_orders INTEGER NOT NULL,
                simulated_orders INTEGER NOT NULL,
                drift_score_bps INTEGER NOT NULL,
                severity TEXT NOT NULL,
                notes_json TEXT NOT NULL,
                risk_decisions_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sim_events (
                run_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                event_kind TEXT NOT NULL,
                event_time TEXT NOT NULL,
                market_id TEXT,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (run_id, sequence)
            );
            CREATE TABLE IF NOT EXISTS sim_orders (
                run_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                order_id TEXT NOT NULL,
                client_order_id TEXT,
                market_id TEXT NOT NULL,
                status TEXT NOT NULL,
                filled_quantity REAL NOT NULL,
                average_fill_price REAL,
                detail_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (run_id, order_id)
            );
            CREATE TABLE IF NOT EXISTS sim_fills (
                fill_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                order_id TEXT NOT NULL,
                market_id TEXT NOT NULL,
                quantity REAL NOT NULL,
                price REAL NOT NULL,
                fees_paid REAL NOT NULL,
                filled_at TEXT NOT NULL,
                detail_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sim_drift_reports (
                run_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                intent_match_rate_bps INTEGER NOT NULL,
                reject_rate_bps INTEGER NOT NULL,
                fill_rate_bps INTEGER NOT NULL,
                fee_leakage_bps INTEGER NOT NULL,
                average_latency_ms INTEGER NOT NULL,
                p95_latency_ms INTEGER NOT NULL,
                baseline_orders INTEGER NOT NULL,
                simulated_orders INTEGER NOT NULL,
                drift_score_bps INTEGER NOT NULL,
                severity TEXT NOT NULL,
                notes_json TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS replay_checkpoints (
                run_id TEXT PRIMARY KEY,
                domain TEXT NOT NULL,
                mode TEXT NOT NULL,
                cursor TEXT NOT NULL,
                processed_events INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                metadata_json TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sim_events_domain_run_sequence
                ON sim_events (domain, run_id, sequence);
            CREATE INDEX IF NOT EXISTS idx_sim_orders_domain_market
                ON sim_orders (domain, market_id, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_sim_fills_domain_market
                ON sim_fills (domain, market_id, filled_at DESC);
            "#,
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn new_store(dir: &Path, domain: AccountDomain) -> Store {
        Store::new(
            dir.join(format!("{}.sqlite", domain.namespace())),
            domain,
            domain.namespace(),
            domain.audit_label(),
        )
    }

    fn sample_market() -> MarketCanonical {
        MarketCanonical {
            market_id: "m1".to_owned(),
            event_id: "evt-1".to_owned(),
            condition_id: "cond-1".to_owned(),
            token_ids: vec!["yes".to_owned(), "no".to_owned()],
            title: "Will BTC be above 100k by Mar 31?".to_owned(),
            category: "crypto".to_owned(),
            outcomes: vec!["YES".to_owned(), "NO".to_owned()],
            end_time: now(),
            resolution_source: Some("AP".to_owned()),
            edge_cases: vec!["rounding".to_owned()],
            clarifications: Vec::new(),
            fees_enabled: true,
            neg_risk: false,
            neg_risk_augmented: false,
            tick_size: 0.01,
            rules_version: "rv1".to_owned(),
            raw_rules_text: "Resolved according to AP by Mar 31".to_owned(),
            market_status: "open".to_owned(),
            observed_at: now(),
            updated_at: now(),
            semantic_tags: Vec::new(),
            semantic_attributes: Vec::new(),
        }
    }

    #[test]
    fn store_initializes_and_tracks_modes() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Sim);
        store.init().expect("init");

        let record = store
            .set_runtime_mode(AccountDomain::Sim, RuntimeMode::Observe, "test")
            .expect("set mode");
        assert_eq!(record.mode, RuntimeMode::Observe);

        let modes = store.list_runtime_modes().expect("list modes");
        assert_eq!(modes.len(), 1);
        assert_eq!(modes[0].domain, AccountDomain::Sim);
    }

    #[test]
    fn stores_rules_market_and_versions() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Sim);
        store.init().expect("init");

        let market = sample_market();
        store
            .upsert_rules_market(market.clone())
            .expect("upsert rules market");

        let loaded = store
            .get_rules_market(&market.market_id)
            .expect("get rules market")
            .expect("market exists");
        assert_eq!(loaded.rules_version, "rv1");

        let versions = store
            .list_rule_versions(&market.market_id)
            .expect("list versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].rules_version, "rv1");
    }

    #[test]
    fn store_rejects_cross_domain_mutations() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Canary);
        store.init().expect("init");

        let error = store
            .set_runtime_mode(AccountDomain::Live, RuntimeMode::Observe, "oops")
            .expect_err("must reject");
        assert!(error.to_string().contains("is bound to domain `CANARY`"));
    }

    #[test]
    fn event_snapshot_and_order_recovery_are_replayable() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Canary);
        store.init().expect("init");

        let event = store
            .append_event(NewDurableEvent {
                domain: AccountDomain::Canary,
                stream: "execution:market-1".to_owned(),
                aggregate_type: "order".to_owned(),
                aggregate_id: "ord-1".to_owned(),
                event_type: "ORDER_SUBMITTED".to_owned(),
                causation_id: None,
                correlation_id: None,
                idempotency_key: Some("submit-1".to_owned()),
                payload: serde_json::json!({"market_id":"market-1","price":0.42}),
                metadata: serde_json::json!({"source":"test"}),
                created_at: now(),
            })
            .expect("append event");

        let snapshot = store
            .upsert_snapshot(NewStateSnapshot {
                domain: AccountDomain::Canary,
                aggregate_type: "portfolio".to_owned(),
                aggregate_id: "default".to_owned(),
                version: 2,
                payload: serde_json::json!({"cash":1000,"reserved":125}),
                derived_from_sequence: Some(event.sequence),
                created_at: now(),
            })
            .expect("upsert snapshot");

        let order = store
            .upsert_order_lifecycle(NewOrderLifecycleRecord {
                domain: AccountDomain::Canary,
                order_id: "ord-1".to_owned(),
                market_id: "market-1".to_owned(),
                status: OrderLifecycleStatus::Acknowledged,
                client_order_id: Some("client-1".to_owned()),
                external_order_id: Some("ext-1".to_owned()),
                idempotency_key: Some("submit-1".to_owned()),
                side: Some("BUY".to_owned()),
                limit_price: Some(0.42),
                order_quantity: Some(25.0),
                filled_quantity: 0.0,
                average_fill_price: None,
                last_event_sequence: Some(event.sequence),
                detail: serde_json::json!({"venue":"polymarket"}),
                opened_at: now(),
                updated_at: now(),
                closed_at: None,
            })
            .expect("upsert order");

        let replay = store
            .replay_events(
                AccountDomain::Canary,
                ReplayCursor {
                    after_sequence: None,
                    limit: 10,
                },
            )
            .expect("replay");
        assert_eq!(replay.len(), 1);
        assert_eq!(replay[0].event_type, "ORDER_SUBMITTED");
        assert_eq!(snapshot.derived_from_sequence, Some(event.sequence));
        assert_eq!(order.last_event_sequence, Some(event.sequence));

        let recovery = store
            .recovery_state(AccountDomain::Canary)
            .expect("recovery state");
        assert_eq!(recovery.recent_events.len(), 1);
        assert_eq!(recovery.snapshots.len(), 1);
        assert_eq!(recovery.open_orders.len(), 1);
    }

    #[test]
    fn idempotency_keys_enforce_deduplication_and_completion() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Live);
        store.init().expect("init");

        let claim = NewIdempotencyKey {
            domain: AccountDomain::Live,
            scope: "execution.submit".to_owned(),
            key: "submit-1".to_owned(),
            request_hash: "hash-a".to_owned(),
            created_by: "execution-engine".to_owned(),
            created_at: now(),
            lock_expires_at: None,
        };

        let first = store
            .claim_idempotency_key(claim.clone())
            .expect("claim idempotency");
        assert!(matches!(first, IdempotencyClaimResult::Claimed(_)));

        let second = store
            .claim_idempotency_key(claim.clone())
            .expect("dedupe idempotency");
        assert!(matches!(
            second,
            IdempotencyClaimResult::DuplicateInFlight(_)
        ));

        let completed = store
            .complete_idempotency_key(
                AccountDomain::Live,
                "execution.submit",
                "submit-1",
                &serde_json::json!({"order_id":"ord-99"}),
            )
            .expect("complete idempotency");
        assert_eq!(completed.status, IdempotencyStatus::Completed);

        let third = store
            .claim_idempotency_key(claim)
            .expect("replay completed idempotency");
        assert!(matches!(
            third,
            IdempotencyClaimResult::DuplicateCompleted(_)
        ));
    }

    #[test]
    fn health_snapshot_reads_only_owner_domain() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Canary);
        store.init().expect("init");
        store
            .heartbeat_service(
                ServiceKind::ExecutionEngine,
                AccountDomain::Canary,
                true,
                "running",
            )
            .expect("heartbeat");
        store
            .append_audit(Some(AccountDomain::Canary), "test", "start", "ok")
            .expect("audit");

        let snapshot = store
            .health_snapshot(AccountDomain::Canary)
            .expect("health snapshot");
        assert_eq!(snapshot.services.len(), 1);
        assert_eq!(snapshot.audit_events.len(), 1);
    }

    #[test]
    fn rollout_state_and_candidate_round_trip() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Canary);
        store.init().expect("init");

        let record = store
            .set_rollout_stage(
                AccountDomain::Canary,
                PromotionStage::Canary,
                Some("alice"),
                Some(now()),
                "initial canary",
            )
            .expect("set rollout stage");
        assert_eq!(record.stage, PromotionStage::Canary);

        let policy = RolloutPolicy {
            stage: PromotionStage::Canary,
            domain: AccountDomain::Canary,
            allowed_strategies: vec![StrategyKind::Rebalancing],
            max_total_notional: 1_000.0,
            max_batch_notional: 250.0,
            max_strategy_notional: 125.0,
            capabilities: polymarket_core::RolloutCapabilityMatrix {
                allow_real_execution: true,
                allow_multileg: false,
                allow_market_making: false,
                allow_auto_redemption: true,
                require_shadow_alignment: true,
            },
            mvp_flags: polymarket_core::MvpCapabilityFlags::default(),
        };
        store
            .upsert_rollout_policy(AccountDomain::Canary, &policy)
            .expect("policy");
        let loaded = store
            .load_effective_rollout_policy(AccountDomain::Canary)
            .expect("load policy")
            .expect("policy");
        assert_eq!(loaded.stage, PromotionStage::Canary);

        let evaluation = RolloutEvaluation {
            evaluation_id: Uuid::new_v4(),
            domain: AccountDomain::Canary,
            current_stage: PromotionStage::Canary,
            target_stage: Some(PromotionStage::Live),
            eligible: true,
            blocking_reasons: Vec::new(),
            warnings: Vec::new(),
            evidence: polymarket_core::RolloutEvidence {
                runtime_mode: "OBSERVE".to_owned(),
                heartbeat_age_ms: 100,
                reject_rate_5m: 0.0,
                fill_rate_5m: 1.0,
                recent_425_count: 0,
                market_ws_lag_ms: 50,
                reconcile_drift: false,
                disputed_capital_ratio: 0.0,
                latest_alert_severity: None,
                open_alerts: 0,
                unresolved_incidents: 0,
                stable_window_secs: 600,
                shadow_live_drift_bps: Some(10.0),
            },
            evaluated_at: now(),
        };
        store
            .record_rollout_evaluation(evaluation.clone())
            .expect("evaluation");
        assert_eq!(
            store
                .latest_rollout_evaluation(AccountDomain::Canary)
                .expect("latest")
                .expect("evaluation")
                .evaluation_id,
            evaluation.evaluation_id
        );

        let candidate = PromotionCandidate {
            candidate_id: Uuid::new_v4(),
            domain: AccountDomain::Canary,
            current_stage: PromotionStage::Canary,
            target_stage: PromotionStage::Live,
            evidence_summary: "stable".to_owned(),
            blocking_reasons: Vec::new(),
            valid_until: now() + chrono::Duration::minutes(5),
            created_at: now(),
            invalidated_at: None,
        };
        store
            .upsert_promotion_candidate(candidate.clone())
            .expect("candidate");
        assert_eq!(
            store
                .get_active_promotion_candidate(AccountDomain::Canary)
                .expect("active")
                .expect("candidate")
                .candidate_id,
            candidate.candidate_id
        );
    }

    #[test]
    fn replay_job_lifecycle_round_trips() {
        let dir = tempdir().expect("tempdir");
        let store = new_store(dir.path(), AccountDomain::Sim);
        store.init().expect("init");

        let job = store
            .enqueue_replay_job(
                ReplayRequest {
                    domain: AccountDomain::Sim,
                    after_sequence: Some(42),
                    limit: 25,
                    reason: None,
                    alert_id: None,
                    audit_event_id: None,
                },
                "alice",
            )
            .expect("enqueue replay job");
        assert_eq!(job.status, ReplayJobStatus::Pending);

        let claimed = store
            .claim_pending_replay_job("worker-a")
            .expect("claim replay job")
            .expect("claimed job");
        assert_eq!(claimed.job_id, job.job_id);
        assert_eq!(claimed.status, ReplayJobStatus::Running);
        assert_eq!(claimed.worker_id.as_deref(), Some("worker-a"));

        let run_id = Uuid::new_v4();
        store
            .complete_replay_job(job.job_id, run_id)
            .expect("complete replay job");
        let completed = store
            .get_replay_job(job.job_id)
            .expect("fetch replay job")
            .expect("job exists");
        assert_eq!(completed.status, ReplayJobStatus::Completed);
        assert_eq!(completed.run_id, Some(run_id));
        assert!(completed.completed_at.is_some());
    }

    #[test]
    fn replay_job_claim_is_atomic() {
        let dir = tempdir().expect("tempdir");
        let store_a = new_store(dir.path(), AccountDomain::Sim);
        let store_b = new_store(dir.path(), AccountDomain::Sim);
        store_a.init().expect("init");

        store_a
            .enqueue_replay_job(
                ReplayRequest {
                    domain: AccountDomain::Sim,
                    after_sequence: None,
                    limit: 10,
                    reason: None,
                    alert_id: None,
                    audit_event_id: None,
                },
                "alice",
            )
            .expect("enqueue replay job");

        let first = store_a
            .claim_pending_replay_job("worker-a")
            .expect("claim first");
        let second = store_b
            .claim_pending_replay_job("worker-b")
            .expect("claim second");

        assert!(first.is_some());
        assert!(second.is_none());
    }
}
