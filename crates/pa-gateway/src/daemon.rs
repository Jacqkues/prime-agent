use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    time::Duration,
};

use pa_types::{
    daemon::{DaemonCommand, DaemonResponse, DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION},
    gateway::{
        AttributedPrompt, EventCursor, RuntimeEvent, RuntimeEventKind, StoredSession, Workspace,
    },
    platform::transport::{connect_transport, AsyncReadHalf, AsyncWriteHalf},
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::{Error, EventStream, Result, Runtime};

const FRAME_LIMIT: u64 = 8 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Host-controlled daemon location and session configuration. Never populate
/// either field from an untrusted HTTP body. Isolation is provisioned by the host.
#[derive(Debug, Clone)]
pub struct DaemonEndpoint {
    pub socket_path: PathBuf,
    pub create_config: Value,
}

/// Native JSONL daemon adapter, independent of `pa-core` and `pa-daemon` linkage.
/// Each workspace must be provisioned by the host with an isolated daemon,
/// agent directory, filesystem, network policy and credential scope.
pub struct DaemonRuntime {
    endpoints: BTreeMap<Workspace, DaemonEndpoint>,
}

impl DaemonRuntime {
    /// Register trusted workspace routes. Endpoint changes require a new adapter.
    ///
    /// # Errors
    /// Rejects empty identities, non-object configuration and duplicate socket
    /// paths. Hosts must also avoid aliases pointing at the same daemon.
    pub fn new(endpoints: BTreeMap<Workspace, DaemonEndpoint>) -> Result<Self> {
        let mut paths = std::collections::BTreeSet::new();
        for (workspace, endpoint) in &endpoints {
            if workspace.tenant_id.is_empty()
                || workspace.workspace_id.is_empty()
                || !endpoint.create_config.is_object()
                || !endpoint.socket_path.is_absolute()
                || !paths.insert(endpoint.socket_path.clone())
            {
                return Err(Error::InvalidRequest);
            }
        }
        Ok(Self { endpoints })
    }

    async fn connect(&self, workspace: &Workspace) -> Result<Connection> {
        let endpoint = self.endpoints.get(workspace).ok_or(Error::Forbidden)?;
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let stream = connect_transport(&endpoint.socket_path)
                .await
                .map_err(Error::Runtime)?;
            let (reader, writer) = stream.split();
            let mut connection = Connection {
                reader: BufReader::new(reader),
                writer,
            };
            let hello = connection.read().await?;
            if hello["type"] != "daemon_hello"
                || hello["protocol"]["name"] != DAEMON_PROTOCOL_NAME
                || hello["protocol"]["version"].as_u64() != Some(DAEMON_PROTOCOL_VERSION)
            {
                return Err(Error::Runtime(anyhow::anyhow!(
                    "incompatible daemon handshake"
                )));
            }
            Ok(connection)
        })
        .await
        .map_err(|error| Error::Runtime(error.into()))?
    }
}

impl Runtime for DaemonRuntime {
    #[tracing::instrument(skip_all)]
    async fn create(&self, workspace: &Workspace, session_id: &str) -> Result<String> {
        let endpoint = self.endpoints.get(workspace).ok_or(Error::Forbidden)?;
        let mut connection = self.connect(workspace).await?;
        let (data, _) = connection
            .exchange(json!({
                "type": "create", "name": session_id,
                "config": endpoint.create_config, "lifecycle": "resident",
            }))
            .await?;
        data["activeSessionId"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| {
                Error::Runtime(anyhow::anyhow!("create response lacks session identity"))
            })
    }

    #[tracing::instrument(skip_all)]
    async fn prompt(&self, record: &StoredSession, prompt: AttributedPrompt) -> Result<()> {
        let mut connection = self.connect(&record.session.workspace).await?;
        // This is ordinary user content, not a system instruction. The envelope
        // preserves attribution in the daemon's durable conversation and context.
        let message =
            serde_json::to_string(&prompt).map_err(|error| Error::Runtime(error.into()))?;
        connection
            .exchange(json!({
                "type": "prompt", "activeSessionId": runtime_id(record)?,
                "message": message, "streamingBehavior": "followUp", "queueIfBusy": true,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn cancel(&self, record: &StoredSession) -> Result<()> {
        self.connect(&record.session.workspace)
            .await?
            .exchange(json!({
                "type": "abort", "activeSessionId": runtime_id(record)?,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn close(&self, record: &StoredSession) -> Result<()> {
        self.connect(&record.session.workspace)
            .await?
            .exchange(json!({
                "type": "kill", "activeSessionId": runtime_id(record)?,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn subscribe(&self, record: &StoredSession) -> Result<EventStream> {
        let id = runtime_id(record)?.to_owned();
        let mut connection = self.connect(&record.session.workspace).await?;
        let (snapshot, mut pending) = connection
            .exchange(json!({
                "type": "attach", "activeSessionId": id,
                "capabilities": ["attach_snapshot", "event_sequence"],
            }))
            .await?;
        // The attach response may resolve a durable id to a replacement worker.
        let id = snapshot["activeSessionId"]
            .as_str()
            .unwrap_or(&id)
            .to_owned();
        pending.retain(|frame| frame["activeSessionId"].as_str() == Some(&id));
        let snapshot = RuntimeEvent {
            kind: RuntimeEventKind::Snapshot,
            cursor: cursor(snapshot.get("lastEventCursor")),
            data: snapshot,
        };
        Ok(Box::pin(futures::stream::unfold(
            Some((connection, pending, id, Some(snapshot))),
            |state| async move {
                let (mut connection, mut pending, id, snapshot) = state?;
                if let Some(snapshot) = snapshot {
                    return Some((Ok(snapshot), Some((connection, pending, id, None))));
                }
                loop {
                    let frame = match pending.pop_front() {
                        Some(frame) => frame,
                        None => match connection.read().await {
                            Ok(frame) => frame,
                            Err(error) => return Some((Err(error), None)),
                        },
                    };
                    if frame["activeSessionId"].as_str() == Some(&id) {
                        let closed = frame["type"] == "session_closed";
                        let event = RuntimeEvent {
                            kind: RuntimeEventKind::Event,
                            cursor: cursor(frame.pointer("/meta/cursor")),
                            data: frame,
                        };
                        return Some((
                            Ok(event),
                            (!closed).then_some((connection, pending, id, None)),
                        ));
                    }
                    if frame["type"] == "daemon_closing" {
                        return Some((
                            Err(Error::Runtime(anyhow::anyhow!("daemon closing"))),
                            None,
                        ));
                    }
                }
            },
        )))
    }
}

fn runtime_id(record: &StoredSession) -> Result<&str> {
    record.runtime_id.as_deref().ok_or(Error::NotReady)
}

/// The daemon's `{generation, sequence}` event cursor, when present.
fn cursor(value: Option<&Value>) -> Option<EventCursor> {
    value.and_then(|value| serde_json::from_value(value.clone()).ok())
}

struct Connection {
    reader: BufReader<Box<dyn AsyncReadHalf>>,
    writer: Box<dyn AsyncWriteHalf>,
}

impl Connection {
    async fn read(&mut self) -> Result<Value> {
        let mut line = Vec::new();
        let size = (&mut self.reader)
            .take(FRAME_LIMIT + 1)
            .read_until(b'\n', &mut line)
            .await
            .map_err(|error| Error::Runtime(error.into()))?;
        if size == 0 || size as u64 > FRAME_LIMIT || line.last() != Some(&b'\n') {
            return Err(Error::Runtime(anyhow::anyhow!(
                "daemon closed or exceeded frame limit"
            )));
        }
        serde_json::from_slice(&line).map_err(|error| Error::Runtime(error.into()))
    }

    async fn exchange(&mut self, mut command: Value) -> Result<(Value, VecDeque<Value>)> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let id = uuid::Uuid::new_v4().to_string();
            command["id"] = json!(id);
            let command: DaemonCommand =
                serde_json::from_value(command).map_err(|error| Error::Runtime(error.into()))?;
            let mut bytes = serde_json::to_vec(&json!({
                "type": "command", "id": id,
                "protocol": {"name": DAEMON_PROTOCOL_NAME, "version": DAEMON_PROTOCOL_VERSION},
                "command": command,
            }))
            .map_err(|error| Error::Runtime(error.into()))?;
            bytes.push(b'\n');
            self.writer
                .write_all(&bytes)
                .await
                .map_err(|error| Error::Runtime(error.into()))?;
            let mut pending = VecDeque::new();
            let mut pending_bytes = 0;
            loop {
                let frame = self.read().await?;
                if frame["type"] == "response" && frame["id"].as_str() == Some(&id) {
                    let response: DaemonResponse = serde_json::from_value(frame)
                        .map_err(|error| Error::Runtime(error.into()))?;
                    if !response.success {
                        return Err(Error::Runtime(anyhow::anyhow!(response
                            .error
                            .unwrap_or_else(|| "daemon refused command".to_owned()))));
                    }
                    return Ok((response.data.unwrap_or(Value::Null), pending));
                }
                if frame
                    .get("activeSessionId")
                    .and_then(Value::as_str)
                    .is_some()
                {
                    pending_bytes += serde_json::to_vec(&frame)
                        .map_err(|error| Error::Runtime(error.into()))?
                        .len();
                    if pending_bytes as u64 > FRAME_LIMIT || pending.len() >= 1024 {
                        return Err(Error::Runtime(anyhow::anyhow!(
                            "daemon event backlog exceeded"
                        )));
                    }
                    pending.push_back(frame);
                }
            }
        })
        .await
        .map_err(|error| Error::Runtime(error.into()))?
    }
}
