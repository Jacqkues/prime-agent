//! Application-facing vocabulary for shared gateway sessions.
//!
//! Tenant and user identities are supplied by the embedding application's
//! authentication, never by a gateway request body.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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
