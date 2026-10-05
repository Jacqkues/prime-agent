//! Opt-in, process-local diagnostics. The host owns operator access; never mount
//! the inspector with an ordinary tenant authenticator. Content capture requires
//! explicit trace sources; no observer operation can stop or modify an agent.

pub(crate) mod http;
mod roster;
mod traces;

#[cfg(test)]
mod tests;

use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use pa_types::gateway::{
    debug::{DebugEvent, DebugRequest, DebugSnapshot, DebugWorkspace},
    Principal, Workspace,
};

pub use http::router;
pub use roster::DebugWatch;
pub use traces::{TraceSource, TraceWatch};

const REQUEST_LIMIT: usize = 1000;
const EVENT_LIMIT: usize = 500;

/// Share this handle between `Gateway::with_inspector`, the operator router and
/// runtime observers. At most 1,000 requests and 500 transitions are retained.
#[derive(Clone)]
pub struct Inspector(Arc<Mutex<State>>);

struct State {
    started_at_ms: u64,
    total_requests: u64,
    in_flight: u64,
    open_streams: u64,
    evicted_requests: u64,
    requests: VecDeque<DebugRequest>,
    workspaces: BTreeMap<Workspace, DebugWorkspace>,
    observers: BTreeMap<Workspace, uuid::Uuid>,
    events: VecDeque<DebugEvent>,
    traces: traces::TraceStore,
}

impl Default for Inspector {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(State {
            started_at_ms: now_ms(),
            total_requests: 0,
            in_flight: 0,
            open_streams: 0,
            evicted_requests: 0,
            requests: VecDeque::new(),
            workspaces: BTreeMap::new(),
            observers: BTreeMap::new(),
            events: VecDeque::new(),
            traces: traces::TraceStore::default(),
        })))
    }
}

impl Inspector {
    /// Read a consistent snapshot, also usable without the supplied web UI.
    #[must_use]
    pub fn snapshot(&self) -> DebugSnapshot {
        let state = self.lock();
        DebugSnapshot {
            started_at_ms: state.started_at_ms,
            captured_at_ms: now_ms(),
            total_requests: state.total_requests,
            in_flight: state.in_flight,
            open_streams: state.open_streams,
            evicted_requests: state.evicted_requests,
            requests: state.requests.iter().rev().cloned().collect(),
            workspaces: state.workspaces.values().cloned().collect(),
            events: state.events.iter().rev().cloned().collect(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // Debug collection must not take the application down after a panic.
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn begin(&self, method: String, route: String, session_id: Option<String>) -> RequestGuard {
        let id = uuid::Uuid::new_v4().to_string();
        let mut state = self.lock();
        state.total_requests += 1;
        state.in_flight += 1;
        if state.requests.len() == REQUEST_LIMIT {
            let completed = state
                .requests
                .iter()
                .position(|entry| entry.ended_at_ms.is_some());
            state.requests.remove(completed.unwrap_or(0));
            state.evicted_requests += 1;
        }
        state.requests.push_back(DebugRequest {
            id: id.clone(),
            method,
            route,
            session_id,
            principal: None,
            started_at_ms: now_ms(),
            ended_at_ms: None,
            status: None,
            response_ms: None,
            duration_ms: None,
            streaming: false,
        });
        RequestGuard {
            trace: RequestTrace {
                inspector: self.clone(),
                id,
            },
            started: Instant::now(),
            streaming: false,
            status: None,
            body: Vec::new(),
            body_truncated: false,
        }
    }
}

impl State {
    fn event(&mut self, workspace: Workspace, agent_id: Option<String>, activity: String) {
        if self.events.len() == EVENT_LIMIT {
            self.events.pop_front();
        }
        self.events.push_back(DebugEvent {
            at_ms: now_ms(),
            workspace,
            agent_id,
            activity,
        });
    }
}

#[derive(Clone)]
pub(crate) struct RequestTrace {
    inspector: Inspector,
    id: String,
}

impl RequestTrace {
    pub(crate) fn capturing(&self) -> bool {
        self.inspector.traces_enabled()
    }

    pub(crate) fn capture_request(
        &self,
        method: &str,
        path: &str,
        bytes: &[u8],
        principal: &Principal,
    ) {
        self.inspector.capture_http(pa_types::diagnostics::TracePoint::HttpRequest, &self.id,
            serde_json::json!({"method":method,"path":path,"principal":principal,"body":serde_json::from_slice::<serde_json::Value>(bytes).unwrap_or_else(|_| serde_json::json!(String::from_utf8_lossy(bytes)))}));
    }
    pub(crate) fn identify(&self, principal: &Principal) {
        if let Some(entry) = self
            .inspector
            .lock()
            .requests
            .iter_mut()
            .find(|entry| entry.id == self.id)
        {
            entry.principal = Some(Principal {
                tenant_id: capped(&principal.tenant_id),
                user_id: capped(&principal.user_id),
            });
        }
    }
}

struct RequestGuard {
    trace: RequestTrace,
    started: Instant,
    streaming: bool,
    status: Option<u16>,
    body: Vec<u8>,
    body_truncated: bool,
}

impl RequestGuard {
    fn chunk(&mut self, bytes: &[u8]) {
        if !self.streaming && self.trace.capturing() {
            let retained = bytes
                .len()
                .min((128 * 1024_usize).saturating_sub(self.body.len()));
            self.body.extend_from_slice(&bytes[..retained]);
            self.body_truncated |= retained < bytes.len();
        }
    }

    fn responded(&mut self, status: u16, streaming: bool) {
        self.status = Some(status);
        let mut state = self.trace.inspector.lock();
        self.streaming = streaming;
        if streaming {
            state.open_streams += 1;
        }
        if let Some(entry) = state
            .requests
            .iter_mut()
            .find(|entry| entry.id == self.trace.id)
        {
            entry.status = Some(status);
            entry.response_ms = Some(elapsed_ms(self.started));
            entry.streaming = streaming;
        }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.trace.inspector.capture_http(pa_types::diagnostics::TracePoint::HttpResponse, &self.trace.id,
            serde_json::json!({"status":self.status,"streaming":self.streaming,"body_truncated":self.body_truncated,"duration_ms":elapsed_ms(self.started),"body":serde_json::from_slice::<serde_json::Value>(&self.body).unwrap_or_else(|_| serde_json::json!(String::from_utf8_lossy(&self.body)))}));
        let mut state = self.trace.inspector.lock();
        state.in_flight -= 1;
        if self.streaming {
            state.open_streams -= 1;
        }
        if let Some(entry) = state
            .requests
            .iter_mut()
            .find(|entry| entry.id == self.trace.id)
        {
            entry.ended_at_ms = Some(now_ms());
            entry.duration_ms = Some(elapsed_ms(self.started));
        }
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn capped(value: &str) -> String {
    value.chars().take(256).collect()
}
