use anyhow::Result;
use async_trait::async_trait;
use polymarket_core::{
    ConstraintGraphSnapshot, EventFamilySnapshot, GraphScope, MarketCanonical, RawMarketDocument,
    ReplayReport, ReplayRequest,
};
use polymarket_storage::Store;

use crate::errors::RulesError;
use crate::graph_builder::{
    build_constraint_graph, build_event_family, snapshot_family, snapshot_graph,
};
use crate::normalizer::normalize_market;
use crate::replay::build_replay_report;
use crate::RulesEngine;

#[derive(Clone)]
pub struct RulesEngineService {
    pub store: Store,
    pub max_clarifications: usize,
}

impl RulesEngineService {
    pub fn new(store: Store, max_clarifications: usize) -> Self {
        Self {
            store,
            max_clarifications,
        }
    }
}

#[async_trait]
impl RulesEngine for RulesEngineService {
    async fn upsert_market(&self, raw: RawMarketDocument) -> Result<MarketCanonical, RulesError> {
        let canonical = normalize_market(raw, self.max_clarifications)?;
        self.store
            .upsert_rules_market(canonical.clone())
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        Ok(canonical)
    }

    async fn rebuild_event_family(
        &self,
        event_id: &str,
    ) -> Result<EventFamilySnapshot, RulesError> {
        let markets = self
            .store
            .list_rules_markets_for_event(event_id)
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        let family = build_event_family(markets);
        let snapshot = snapshot_family(family, chrono::Utc::now().timestamp());
        self.store
            .upsert_event_family_snapshot(snapshot.clone())
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        Ok(snapshot)
    }

    async fn get_constraint_graph(
        &self,
        scope: GraphScope,
    ) -> Result<ConstraintGraphSnapshot, RulesError> {
        let markets = match &scope {
            GraphScope::All => self
                .store
                .list_all_rules_markets()
                .map_err(|error| RulesError::Storage(error.to_string()))?,
            GraphScope::Event { event_id } => self
                .store
                .list_rules_markets_for_event(event_id)
                .map_err(|error| RulesError::Storage(error.to_string()))?,
            GraphScope::Market { market_id } => {
                let market = self
                    .store
                    .get_rules_market(market_id)
                    .map_err(|error| RulesError::Storage(error.to_string()))?;
                match market {
                    Some(market) => self
                        .store
                        .list_rules_markets_for_event(&market.event_id)
                        .map_err(|error| RulesError::Storage(error.to_string()))?,
                    None => Vec::new(),
                }
            }
        };
        let family = build_event_family(markets);
        let graph = build_constraint_graph(&family);
        let snapshot = snapshot_graph(scope, graph, chrono::Utc::now().timestamp());
        self.store
            .upsert_constraint_graph_snapshot(snapshot.clone())
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        Ok(snapshot)
    }

    async fn replay(&self, request: ReplayRequest) -> Result<ReplayReport, RulesError> {
        let events = self
            .store
            .replay_rules_market_events(request.domain, request.after_sequence, request.limit)
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        let versions = events
            .iter()
            .filter_map(|event| {
                event
                    .payload
                    .get("rules_version")
                    .and_then(|value| value.as_str())
            })
            .map(ToOwned::to_owned)
            .collect();
        let report = build_replay_report(&request, events.len(), versions);
        self.store
            .record_rules_replay_run(report.clone())
            .map_err(|error| RulesError::Storage(error.to_string()))?;
        Ok(report)
    }
}
