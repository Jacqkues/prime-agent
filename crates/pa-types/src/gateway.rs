//! Application-facing vocabulary for shared gateway sessions.
//!
//! Tenant and user identities are supplied by the embedding application's
//! authentication, never by a gateway request body.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod debug;

/// Verified application identity. An identifier is scoped to its tenant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub tenant_id: String,
    pub user_id: String,
}

/// A workspace names an execution and credential boundary within a tenant.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Workspace {
    pub tenant_id: String,
    pub workspace_id: String,
}

/// Session permissions, additionally restricted by the application's policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionRole {
    Owner,
    Contributor,
    Viewer,
}

/// Provisioning is recorded before external execution resources are created.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Provisioning,
    Ready,
    Failed,
    Closed,
}

/// Public session metadata. Execution addresses and credentials stay server-side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub workspace: Workspace,
    pub members: BTreeMap<String, SessionRole>,
    pub status: SessionStatus,
    pub revision: u64,
}

/// Storage record for the gateway's session-to-runtime binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredSession {
    pub session: Session,
    pub runtime_id: Option<String>,
}

/// Operations checked against the application's current workspace policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayAction {
    Create,
    Read,
    Prompt,
    Share,
    Cancel,
    Close,
    /// Workspace administration: owner-only operations on any session in the
    /// workspace and listing every session for reconciliation. Never grants
    /// reading session content, subscribing or prompting.
    Administer,
}

/// A prompt attributed by the server to an authenticated participant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttributedPrompt {
    pub request_id: String,
    pub author: Principal,
    pub text: String,
}

/// Receipt for admission to the runtime's ordered input queue, not completion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptReceipt {
    pub request_id: String,
}

/// A participant's prompt. With an idempotency key, a retry of an admitted
/// prompt returns the original receipt instead of submitting it again.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PromptSubmission {
    pub text: String,
    pub idempotency_key: Option<String>,
}

/// Durable idempotency scope: one key per author and session.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PromptKey {
    pub tenant_id: String,
    pub session_id: String,
    pub user_id: String,
    pub idempotency_key: String,
}

/// Result of atomically reserving a prompt key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum PromptReservation {
    /// The key was free and now belongs to the caller's request.
    Reserved,
    /// An earlier attempt was admitted; its receipt is authoritative.
    Admitted { request_id: String },
    /// An earlier attempt is in flight or ended with an unknown outcome.
    Unresolved { request_id: String },
}

/// How a reserved prompt attempt settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptOutcome {
    /// The runtime admitted the prompt.
    Admitted,
    /// The runtime certainly never received it; the key may be reused.
    NotDelivered,
}

/// Opaque position in a runtime's ordered event stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventCursor {
    pub generation: String,
    pub sequence: u64,
}

/// Whether a runtime frame replaces client state or applies on top of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEventKind {
    Snapshot,
    Event,
}

/// One frame from a runtime subscription. `data` is the runtime's native
/// payload; `cursor` orders and deduplicates events when the runtime has one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeEvent {
    pub kind: RuntimeEventKind,
    pub cursor: Option<EventCursor>,
    pub data: serde_json::Value,
}

/// Version of the [`GatewayEvent`] envelope delivered to clients.
pub const GATEWAY_EVENT_VERSION: u32 = 1;

/// Versioned client envelope for subscription frames.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayEvent {
    pub v: u32,
    #[serde(flatten)]
    pub event: RuntimeEvent,
}

/// Where a subscription starts. A snapshot is always delivered first;
/// resuming additionally skips events at or before the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeFrom {
    Start,
    After(EventCursor),
}

/// Keyset pagination ordered by session ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    /// Exclusive lower bound: the `next` value of the previous page.
    pub after: Option<String>,
    pub limit: usize,
}

/// One page of stored records. `next` is the last scanned session ID, present
/// when more records may follow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredPage {
    pub records: Vec<StoredSession>,
    pub next: Option<String>,
}

/// One page of visible sessions. Access filtering can make a page shorter
/// than requested; continue while `next` is present.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionPage {
    pub sessions: Vec<Session>,
    pub next: Option<String>,
}

/// Process-local operational counters for the host's metrics exporter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayMetrics {
    pub sessions_created: u64,
    pub sessions_failed: u64,
    pub prompts_admitted: u64,
    pub prompts_replayed: u64,
    pub prompts_failed: u64,
    pub runtime_errors: u64,
    pub active_subscriptions: u64,
    pub access_rechecks: u64,
    pub streams_revoked: u64,
}
