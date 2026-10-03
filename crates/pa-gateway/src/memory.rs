use std::collections::BTreeMap;

use pa_types::gateway::StoredSession;
use tokio::sync::RwLock;

use crate::{Error, Result, SessionStore};

/// Ephemeral reference adapter for tests and local integration examples.
/// Metadata is lost on process exit. Use a durable `SessionStore` in production.
#[derive(Default)]
pub struct MemoryStore {
    records: RwLock<BTreeMap<(String, String), StoredSession>>,
}

impl SessionStore for MemoryStore {
    async fn insert(&self, record: StoredSession) -> Result<()> {
        let key = (
            record.session.workspace.tenant_id.clone(),
            record.session.id.clone(),
        );
        let mut records = self.records.write().await;
        if records.contains_key(&key) {
            return Err(Error::Conflict);
        }
        records.insert(key, record);
        Ok(())
    }

    async fn get(&self, tenant: &str, id: &str) -> Result<StoredSession> {
        self.records
            .read()
            .await
            .get(&(tenant.to_owned(), id.to_owned()))
            .cloned()
            .ok_or(Error::NotFound)
    }

    async fn list(&self, tenant: &str) -> Result<Vec<StoredSession>> {
        Ok(self
            .records
            .read()
            .await
            .values()
            .filter(|record| record.session.workspace.tenant_id == tenant)
            .cloned()
            .collect())
    }

    async fn replace(&self, record: StoredSession, expected_revision: u64) -> Result<()> {
        let key = (
            record.session.workspace.tenant_id.clone(),
            record.session.id.clone(),
        );
        let mut records = self.records.write().await;
        let previous = records.get(&key).ok_or(Error::NotFound)?;
        if previous.session.revision != expected_revision
            || record.session.revision != expected_revision.checked_add(1).ok_or(Error::Conflict)?
            || record.session.workspace != previous.session.workspace
        {
            return Err(Error::Conflict);
        }
        records.insert(key, record);
        Ok(())
    }
}
