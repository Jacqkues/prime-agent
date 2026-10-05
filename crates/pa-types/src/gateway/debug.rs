//! Content-free operational snapshots for an explicitly enabled gateway inspector.

use serde::{Deserialize, Serialize};

use super::{Principal, Workspace};
use crate::daemon::agent_roster::AgentRosterStatus;

/// An HTTP exchange. A missing end time means its response body is still open.
/// Duration includes streaming; response time measures time to response headers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugRequest {
    pub id: String,
    pub method: String,
    pub route: String,
    pub session_id: Option<String>,
    pub principal: Option<Principal>,
    pub started_at_ms: u64,
    pub ended_at_ms: Option<u64>,
    pub status: Option<u16>,
    pub response_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub streaming: bool,
}

/// Allowlisted roster metadata, excluding messages, code, paths and credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugAgent {
    pub id: String,
    pub session_id: Option<String>,
    pub active_session_id: Option<String>,
    pub gateway_session_id: Option<String>,
    pub parent_active_session_id: Option<String>,
    pub kind: String,
    pub status: AgentRosterStatus,
    pub activity: String,
    pub model: Option<String>,
    pub provider: Option<String>,
}

/// Connectivity of the inspector's own runtime observation, not client presence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DebugConnectionStatus {
    Connecting,
    Connected,
    Disconnected,
    Stopped,
}

/// Last known state is retained on disconnection and must be displayed as stale.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugWorkspace {
    pub workspace: Workspace,
    pub connection: DebugConnectionStatus,
    pub updated_at_ms: u64,
    pub agents: Vec<DebugAgent>,
    /// True once an observation exceeded its bound; reset by a fresh snapshot.
    pub truncated: bool,
}

/// A bounded operational transition log, separate from adoption telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugEvent {
    pub at_ms: u64,
    pub workspace: Workspace,
    pub agent_id: Option<String>,
    pub activity: String,
}

/// Process-local state since inspector activation. Never a durable audit log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugSnapshot {
    pub started_at_ms: u64,
    pub captured_at_ms: u64,
    pub total_requests: u64,
    pub in_flight: u64,
    pub open_streams: u64,
    pub evicted_requests: u64,
    pub requests: Vec<DebugRequest>,
    pub workspaces: Vec<DebugWorkspace>,
    pub events: Vec<DebugEvent>,
}

/// Lightweight index entry; the payload is fetched separately on selection.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceIndexEntry {
    pub id: String,
    pub at_ms: u64,
    pub point: crate::diagnostics::TracePoint,
    pub workspace: Option<Workspace>,
    pub active_session_id: Option<String>,
    pub correlation_id: Option<String>,
    pub pid: u32,
    pub label: String,
    pub bytes: usize,
    pub truncated: bool,
}

/// Source health distinguishes an empty trace from a broken collector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSourceStatus {
    pub workspace: Workspace,
    pub last_scan_ms: u64,
    pub error: Option<String>,
}

/// Bounded trace index. `evicted` and source errors make gaps explicit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceIndex {
    pub enabled: bool,
    pub entries: Vec<TraceIndexEntry>,
    pub sources: Vec<TraceSourceStatus>,
    pub evicted: u64,
    pub dropped: u64,
    pub retained_bytes: usize,
}
