use std::{collections::BTreeMap, ops::Bound};

use pa_types::gateway::{
    PageRequest, PromptKey, PromptOutcome, PromptReservation, StoredPage, StoredSession, Workspace,
};
use tokio::sync::RwLock;

use crate::{Error, Result, SessionStore};

/// Ephemeral reference adapter for tests and local integration examples.
/// Metadata and idempotency keys are lost on process exit, and prompt keys are
/// never expired. Use a durable `SessionStore` in production.
#[derive(Default)]
pub struct MemoryStore {
    records: RwLock<BTreeMap<(String, String), StoredSession>>,
    prompts: RwLock<BTreeMap<PromptKey, (String, PromptState)>>,
}

enum PromptState {
    Pending,
    Admitted,
}

impl MemoryStore {
    async fn page(
        &self,
        tenant: &str,
        page: &PageRequest,
        include: impl Fn(&StoredSession) -> bool,
    ) -> StoredPage {
        let records = self.records.read().await;
        let start = page.after.as_ref().map_or(
            Bound::Included((tenant.to_owned(), String::new())),
            |after| Bound::Excluded((tenant.to_owned(), after.clone())),
        );
        let mut matching = records
            .range((start, Bound::Unbounded))
            .take_while(|((record_tenant, _), _)| record_tenant == tenant)
            .map(|(_, record)| record)
            .filter(|record| include(record));
        let records: Vec<_> = matching.by_ref().take(page.limit).cloned().collect();
        let next = if matching.next().is_some() {
            records.last().map(|record| record.session.id.clone())
        } else {
            None
        };
        StoredPage { records, next }
    }
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

    async fn list_member(
        &self,
        tenant: &str,
        user_id: &str,
        page: &PageRequest,
    ) -> Result<StoredPage> {
        Ok(self
            .page(tenant, page, |record| {
                record.session.members.contains_key(user_id)
            })
            .await)
    }

    async fn list_workspace(
        &self,
        workspace: &Workspace,
        page: &PageRequest,
    ) -> Result<StoredPage> {
        Ok(self
            .page(&workspace.tenant_id, page, |record| {
                record.session.workspace == *workspace
            })
            .await)
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

    async fn reserve_prompt(&self, key: &PromptKey, request_id: &str) -> Result<PromptReservation> {
        let mut prompts = self.prompts.write().await;
        Ok(match prompts.get(key) {
            None => {
                prompts.insert(key.clone(), (request_id.to_owned(), PromptState::Pending));
                PromptReservation::Reserved
            }
            Some((request_id, PromptState::Admitted)) => PromptReservation::Admitted {
                request_id: request_id.clone(),
            },
            Some((request_id, PromptState::Pending)) => PromptReservation::Unresolved {
                request_id: request_id.clone(),
            },
        })
    }

    async fn settle_prompt(&self, key: &PromptKey, outcome: PromptOutcome) -> Result<()> {
        let mut prompts = self.prompts.write().await;
        match outcome {
            PromptOutcome::Admitted => {
                prompts.get_mut(key).ok_or(Error::NotFound)?.1 = PromptState::Admitted;
            }
            PromptOutcome::NotDelivered => {
                prompts.remove(key);
            }
        }
        Ok(())
    }
}
