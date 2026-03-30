use polymarket_core::{
    now, AccountDomain, ServiceKind, SimDriftReport, SimEventRecord, SimFillRecord, SimRunReport,
    Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::broadcast;

pub const TOPIC_SIM_EVENT: &str = "sim.event";
pub const TOPIC_SIM_ORDER: &str = "sim.order";
pub const TOPIC_SIM_FILL: &str = "sim.fill";
pub const TOPIC_SIM_DRIFT: &str = "sim.drift";
pub const TOPIC_SIM_REPORT: &str = "sim.report";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub emitted_at: Timestamp,
    pub service: ServiceKind,
    pub domain: AccountDomain,
    pub topic: String,
    pub payload: String,
}

#[derive(Clone)]
pub struct MessageBus {
    tx: broadcast::Sender<EventEnvelope>,
}

impl MessageBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity.max(16));
        Self { tx }
    }

    pub fn publish(
        &self,
        service: ServiceKind,
        domain: AccountDomain,
        topic: impl Into<String>,
        payload: impl Into<String>,
    ) {
        let _ = self.tx.send(EventEnvelope {
            emitted_at: now(),
            service,
            domain,
            topic: topic.into(),
            payload: payload.into(),
        });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventEnvelope> {
        self.tx.subscribe()
    }

    pub fn publish_sim_event(
        &self,
        domain: AccountDomain,
        event: &SimEventRecord,
    ) -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(event)?;
        self.publish(ServiceKind::SimEngine, domain, TOPIC_SIM_EVENT, payload);
        Ok(())
    }

    pub fn publish_sim_order(
        &self,
        domain: AccountDomain,
        run_id: uuid::Uuid,
        order_id: &str,
        market_id: &str,
        status: &str,
    ) -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(&json!({
            "run_id": run_id,
            "order_id": order_id,
            "market_id": market_id,
            "status": status,
        }))?;
        self.publish(ServiceKind::SimEngine, domain, TOPIC_SIM_ORDER, payload);
        Ok(())
    }

    pub fn publish_sim_fill(
        &self,
        domain: AccountDomain,
        fill: &SimFillRecord,
    ) -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(fill)?;
        self.publish(ServiceKind::SimEngine, domain, TOPIC_SIM_FILL, payload);
        Ok(())
    }

    pub fn publish_sim_drift(
        &self,
        domain: AccountDomain,
        drift: &SimDriftReport,
    ) -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(drift)?;
        self.publish(ServiceKind::SimEngine, domain, TOPIC_SIM_DRIFT, payload);
        Ok(())
    }

    pub fn publish_sim_report(
        &self,
        domain: AccountDomain,
        report: &SimRunReport,
    ) -> Result<(), serde_json::Error> {
        let payload = serde_json::to_string(report)?;
        self.publish(ServiceKind::SimEngine, domain, TOPIC_SIM_REPORT, payload);
        Ok(())
    }
}
