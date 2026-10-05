use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use pa_types::{
    daemon::agent_roster::{session_activity_detail, AgentRosterEntry, SessionActivityOptions},
    gateway::{
        debug::{DebugAgent, DebugConnectionStatus, DebugWorkspace},
        Workspace,
    },
};
use serde_json::Value;

use super::{capped, now_ms, Inspector};
use crate::{DaemonRuntime, Error, Result};

const AGENT_LIMIT: usize = 2000;
const WORKSPACE_LIMIT: usize = 64;

/// Owns observation connections. Keep this guard alive while serving the
/// inspector. Dropping it closes only observation, never agent execution.
#[must_use]
pub struct DebugWatch {
    tasks: Vec<tokio::task::JoinHandle<()>>,
    inspector: Inspector,
    workspaces: Vec<Workspace>,
    observer_id: uuid::Uuid,
}

impl Drop for DebugWatch {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        for workspace in &self.workspaces {
            self.inspector
                .connection(workspace, self.observer_id, DebugConnectionStatus::Stopped);
        }
    }
}

impl Inspector {
    /// Observe every configured daemon, including child agents and sessions
    /// created before this process started. Reconnects use capped backoff; state
    /// is explicitly stale while disconnected. Keep the returned guard alive.
    ///
    /// # Errors
    /// Rejects overlapping active observers or more than 64 distinct workspaces.
    /// # Panics
    /// Must be called within a Tokio runtime.
    pub fn watch_daemon(&self, runtime: &Arc<DaemonRuntime>) -> Result<DebugWatch> {
        let observer_id = uuid::Uuid::new_v4();
        let workspaces = runtime.workspaces();
        {
            let mut state = self.lock();
            let extra = workspaces
                .iter()
                .filter(|key| !state.workspaces.contains_key(*key))
                .count();
            if state.workspaces.len() + extra > WORKSPACE_LIMIT {
                return Err(Error::LimitExceeded);
            }
            if workspaces.iter().any(|key| {
                state
                    .workspaces
                    .get(key)
                    .is_some_and(|value| value.connection != DebugConnectionStatus::Stopped)
            }) {
                return Err(Error::Conflict);
            }
            for workspace in &workspaces {
                state.observers.insert(workspace.clone(), observer_id);
                state.workspaces.insert(
                    workspace.clone(),
                    DebugWorkspace {
                        workspace: workspace.clone(),
                        connection: DebugConnectionStatus::Connecting,
                        updated_at_ms: now_ms(),
                        agents: Vec::new(),
                        truncated: false,
                    },
                );
            }
        }
        let tasks = workspaces
            .iter()
            .map(|workspace| {
                tokio::spawn(observe(
                    self.clone(),
                    Arc::clone(runtime),
                    workspace.clone(),
                    observer_id,
                ))
            })
            .collect();
        Ok(DebugWatch {
            tasks,
            inspector: self.clone(),
            workspaces,
            observer_id,
        })
    }

    fn connection(
        &self,
        workspace: &Workspace,
        observer_id: uuid::Uuid,
        connection: DebugConnectionStatus,
    ) {
        let mut state = self.lock();
        if state.observers.get(workspace) != Some(&observer_id) {
            return;
        }
        if let Some(entry) = state.workspaces.get_mut(workspace).filter(|entry| {
            entry.connection != connection && entry.connection != DebugConnectionStatus::Stopped
        }) {
            entry.connection = connection;
            entry.updated_at_ms = now_ms();
            state.event(
                workspace.clone(),
                None,
                format!("observer {connection:?}").to_lowercase(),
            );
        }
    }

    fn apply_roster(
        &self,
        workspace: &Workspace,
        observer_id: uuid::Uuid,
        frame: &Value,
    ) -> Result<()> {
        let snapshot = frame["type"] == "roster_snapshot";
        if frame["resync"] == true {
            return Err(Error::NotReady);
        }
        let rows = if snapshot {
            &frame["roster"]
        } else {
            &frame["changed"]
        };
        let rows = rows.as_array().ok_or(Error::InvalidRequest)?;
        let removed: Vec<String> = serde_json::from_value(
            frame
                .get("removed")
                .cloned()
                .unwrap_or_else(|| Value::Array(Vec::new())),
        )
        .map_err(|_| Error::InvalidRequest)?;
        let mut state = self.lock();
        if state.observers.get(workspace) != Some(&observer_id) {
            return Err(Error::NotReady);
        }
        let entry = state.workspaces.get_mut(workspace).ok_or(Error::NotFound)?;
        if entry.connection == DebugConnectionStatus::Stopped {
            return Err(Error::NotReady);
        }
        let mut agents: BTreeMap<_, _> = if snapshot {
            BTreeMap::new()
        } else {
            entry
                .agents
                .iter()
                .map(|agent| (agent.id.clone(), agent.clone()))
                .collect()
        };
        let mut changes = Vec::new();
        for id in removed {
            let id = opaque_agent_id(&id);
            if agents.remove(&id).is_some() {
                changes.push((Some(id), "removed".into()));
            }
        }
        let mut truncated = !snapshot && entry.truncated;
        for value in rows {
            let row: AgentRosterEntry =
                serde_json::from_value(value.clone()).map_err(|_| Error::InvalidRequest)?;
            let agent = agent(&row);
            if agents.len() >= AGENT_LIMIT && !agents.contains_key(&agent.id) {
                truncated = true;
                continue;
            }
            if agents.get(&agent.id) != Some(&agent) {
                changes.push((Some(agent.id.clone()), agent.activity.clone()));
            }
            agents.insert(agent.id.clone(), agent);
        }
        entry.agents = agents.into_values().collect();
        entry.truncated = truncated;
        entry.updated_at_ms = now_ms();
        for (id, activity) in changes {
            state.event(workspace.clone(), id, activity);
        }
        Ok(())
    }
}

#[tracing::instrument(skip_all)]
async fn observe(
    inspector: Inspector,
    runtime: Arc<DaemonRuntime>,
    workspace: Workspace,
    observer_id: uuid::Uuid,
) {
    let mut backoff = Duration::from_secs(1);
    loop {
        match runtime.roster(&workspace).await {
            Ok(mut events) => {
                while let Some(event) = events.next().await {
                    match event {
                        Ok(frame) => {
                            if inspector
                                .apply_roster(&workspace, observer_id, &frame)
                                .is_err()
                            {
                                break;
                            }
                            inspector.connection(
                                &workspace,
                                observer_id,
                                DebugConnectionStatus::Connected,
                            );
                            backoff = Duration::from_secs(1);
                        }
                        Err(_) => break,
                    }
                }
            }
            Err(error) => tracing::debug!(%error, "inspector observation unavailable"),
        }
        inspector.connection(&workspace, observer_id, DebugConnectionStatus::Disconnected);
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn agent(row: &AgentRosterEntry) -> DebugAgent {
    let summary = &row.summary;
    let field = |name| summary.get(name).and_then(Value::as_str).map(capped);
    let model = &summary["model"];
    let activity = match row.status_label.as_deref() {
        Some("queued") => "queued".into(),
        Some("recovering") => "recovering".into(),
        Some("failed") => "failed".into(),
        _ => session_activity_detail(
            summary,
            &SessionActivityOptions {
                heartbeat_label: "scheduled".into(),
                idle_label: "needs input".into(),
            },
        ),
    };
    DebugAgent {
        id: opaque_agent_id(&row.agent_id),
        session_id: field("sessionId"),
        active_session_id: field("activeSessionId"),
        gateway_session_id: summary
            .get("name")
            .and_then(Value::as_str)
            .filter(|value| uuid::Uuid::parse_str(value).is_ok())
            .map(str::to_owned),
        parent_active_session_id: field("parentActiveSessionId"),
        kind: if summary["runtimeKind"] == "subagent" {
            "subagent"
        } else {
            "top-level"
        }
        .into(),
        status: row.status,
        activity: capped(&activity),
        model: model
            .get("modelId")
            .or_else(|| model.get("id"))
            .and_then(Value::as_str)
            .map(capped),
        provider: model.get("provider").and_then(Value::as_str).map(capped),
    }
}

fn opaque_agent_id(value: &str) -> String {
    // Roster child ids can embed absolute session paths. Keep stable equality
    // without publishing those paths. This is an identifier, not authentication.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in value.bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("agent-{hash:016x}")
}

#[cfg(test)]
mod tests;
