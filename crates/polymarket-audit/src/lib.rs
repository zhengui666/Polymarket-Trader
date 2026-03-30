use anyhow::Result;
use polymarket_core::{AccountDomain, AuditEvent};
use polymarket_storage::Store;

pub trait AuditSink: Send + Sync {
    fn record(
        &self,
        domain: Option<AccountDomain>,
        service: &str,
        action: &str,
        detail: &str,
    ) -> Result<AuditEvent>;
}

#[derive(Clone)]
pub struct StorageAuditSink {
    store: Store,
}

impl StorageAuditSink {
    pub fn new(store: Store) -> Self {
        Self { store }
    }
}

impl AuditSink for StorageAuditSink {
    fn record(
        &self,
        domain: Option<AccountDomain>,
        service: &str,
        action: &str,
        detail: &str,
    ) -> Result<AuditEvent> {
        self.store.append_audit(domain, service, action, detail)
    }
}
