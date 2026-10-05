#![allow(dead_code)] // Each integration test binary uses a different subset.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use futures::{stream, StreamExt};
use pa_gateway::{Error, EventStream, Gateway, MemoryStore, Result, Runtime, WorkspacePolicy};
use pa_types::gateway::{
    AttributedPrompt, EventCursor, GatewayAction, Principal, RuntimeEvent, RuntimeEventKind,
    StoredSession, Workspace,
};
use serde_json::{json, Value};
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};

#[derive(Default)]
pub struct Policy {
    pub denied: RwLock<BTreeSet<String>>,
    pub checks: AtomicUsize,
}

impl WorkspacePolicy for Policy {
    async fn check(
        &self,
        principal: &Principal,
        workspace: &Workspace,
        _action: GatewayAction,
    ) -> Result<()> {
        self.checks.fetch_add(1, Ordering::Relaxed);
        if principal.tenant_id == workspace.tenant_id
            && workspace.workspace_id == "project"
            && !self.denied.read().await.contains(&principal.user_id)
        {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
}

#[derive(Default)]
pub struct Agent {
    pub prompts: Mutex<Vec<AttributedPrompt>>,
    pub events: Mutex<BTreeMap<String, broadcast::Sender<RuntimeEvent>>>,
    pub closed: Mutex<Vec<String>>,
    pub admission_gate: Mutex<Option<AdmissionGate>>,
}

pub struct AdmissionGate {
    pub entered: oneshot::Sender<()>,
    pub release: oneshot::Receiver<()>,
    pub committed: oneshot::Sender<()>,
}

impl Runtime for Agent {
    async fn create(&self, _workspace: &Workspace, id: &str) -> Result<String> {
        self.events
            .lock()
            .await
            .insert(id.to_owned(), broadcast::channel(256).0);
        Ok(id.to_owned())
    }

    async fn prompt(&self, _record: &StoredSession, prompt: AttributedPrompt) -> Result<()> {
        let gate = self.admission_gate.lock().await.take();
        let committed = if let Some(gate) = gate {
            gate.entered.send(()).unwrap();
            gate.release.await.unwrap();
            Some(gate.committed)
        } else {
            None
        };
        self.prompts.lock().await.push(prompt);
        if let Some(committed) = committed {
            committed.send(()).unwrap();
        }
        Ok(())
    }

    async fn cancel(&self, record: &StoredSession) -> Result<()> {
        self.closed
            .lock()
            .await
            .push(format!("cancel:{}", record.session.id));
        Ok(())
    }

    async fn close(&self, record: &StoredSession) -> Result<()> {
        self.closed.lock().await.push(record.session.id.clone());
        Ok(())
    }

    async fn subscribe(&self, record: &StoredSession) -> Result<EventStream> {
        let rx = self
            .events
            .lock()
            .await
            .get(&record.session.id)
            .unwrap()
            .subscribe();
        let events = stream::unfold(rx, |mut rx| async move {
            Some((
                rx.recv()
                    .await
                    .map_err(|error| Error::Runtime(error.into())),
                rx,
            ))
        });
        Ok(Box::pin(
            stream::once(async { Ok(snapshot()) }).chain(events),
        ))
    }
}

pub type Service = Gateway<MemoryStore, Policy, Agent>;

pub fn service() -> (Service, Arc<MemoryStore>, Arc<Policy>, Arc<Agent>) {
    let store = Arc::new(MemoryStore::default());
    let policy = Arc::new(Policy::default());
    let agent = Arc::new(Agent::default());
    (
        Gateway::new(Arc::clone(&store), Arc::clone(&policy), Arc::clone(&agent)),
        store,
        policy,
        agent,
    )
}

pub fn user(tenant: &str, user: &str) -> Principal {
    Principal {
        tenant_id: tenant.to_owned(),
        user_id: user.to_owned(),
    }
}

pub fn snapshot() -> RuntimeEvent {
    RuntimeEvent {
        kind: RuntimeEventKind::Snapshot,
        cursor: None,
        data: json!({"type": "snapshot"}),
    }
}

/// A runtime event without a cursor.
pub fn event(data: Value) -> RuntimeEvent {
    RuntimeEvent {
        kind: RuntimeEventKind::Event,
        cursor: None,
        data,
    }
}

/// A runtime event at `sequence` in generation `g1`.
pub fn sequenced(sequence: u64, data: Value) -> RuntimeEvent {
    RuntimeEvent {
        kind: RuntimeEventKind::Event,
        cursor: Some(EventCursor {
            generation: "g1".into(),
            sequence,
        }),
        data,
    }
}
