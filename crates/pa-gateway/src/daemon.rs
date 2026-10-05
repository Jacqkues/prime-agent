use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    sync::{Arc, Mutex, PoisonError, RwLock},
    time::{Duration, Instant},
};

use pa_types::{
    daemon::{DaemonCommand, DaemonResponse, DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION},
    gateway::{
        AttributedPrompt, EventCursor, RuntimeEvent, RuntimeEventKind, StoredSession, Workspace,
    },
    platform::transport::{connect_transport, AsyncReadHalf, AsyncWriteHalf},
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    sync::Semaphore,
};

use crate::{Error, EventStream, Result, Runtime};

const FRAME_LIMIT: u64 = 8 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Idle command connections kept per workspace for reuse.
const POOL_IDLE: usize = 4;
/// Idle connections older than this are discarded rather than reused.
const POOL_IDLE_TTL: Duration = Duration::from_secs(30);

/// Host-controlled daemon location and session configuration. Never populate
/// any field from an untrusted HTTP body. Isolation is provisioned by the host.
#[derive(Debug, Clone)]
pub struct DaemonEndpoint {
    pub socket_path: PathBuf,
    pub create_config: Value,
    /// Concurrent event subscriptions allowed for this workspace. Each holds
    /// one daemon connection; further subscriptions fail with `LimitExceeded`.
    pub max_subscriptions: usize,
}

/// Native JSONL daemon adapter, independent of `pa-core` and `pa-daemon` linkage.
/// Each workspace must be provisioned by the host with an isolated daemon,
/// agent directory, filesystem, network policy and credential scope.
///
/// Workspaces can be registered and unregistered while the gateway runs.
/// Commands reuse a small pool of idle connections; subscriptions hold their
/// own connection for their lifetime.
pub struct DaemonRuntime {
    routes: RwLock<BTreeMap<Workspace, Arc<Route>>>,
}

struct Route {
    endpoint: DaemonEndpoint,
    idle: Mutex<Vec<(Instant, Connection)>>,
    subscriptions: Arc<Semaphore>,
}

impl DaemonRuntime {
    /// Register trusted workspace routes.
    ///
    /// # Errors
    /// Same validation as [`DaemonRuntime::register`].
    pub fn new(endpoints: BTreeMap<Workspace, DaemonEndpoint>) -> Result<Self> {
        let runtime = Self {
            routes: RwLock::default(),
        };
        for (workspace, endpoint) in endpoints {
            runtime.register(workspace, endpoint)?;
        }
        Ok(runtime)
    }

    /// Add a workspace route while running. Existing sessions in that
    /// workspace become reachable again once it is registered.
    ///
    /// # Errors
    /// Rejects empty identities, non-object configuration, relative or
    /// duplicate socket paths and a zero subscription limit with
    /// `InvalidRequest`, and an already registered workspace with `Conflict`.
    /// Hosts must also avoid aliases pointing at the same daemon.
    pub fn register(&self, workspace: Workspace, endpoint: DaemonEndpoint) -> Result<()> {
        if workspace.tenant_id.is_empty()
            || workspace.workspace_id.is_empty()
            || !endpoint.create_config.is_object()
            || !endpoint.socket_path.is_absolute()
            || endpoint.max_subscriptions == 0
        {
            return Err(Error::InvalidRequest);
        }
        let mut routes = self.routes.write().unwrap_or_else(PoisonError::into_inner);
        if routes.contains_key(&workspace) {
            return Err(Error::Conflict);
        }
        if routes
            .values()
            .any(|route| route.endpoint.socket_path == endpoint.socket_path)
        {
            return Err(Error::InvalidRequest);
        }
        routes.insert(
            workspace,
            Arc::new(Route {
                subscriptions: Arc::new(Semaphore::new(endpoint.max_subscriptions)),
                endpoint,
                idle: Mutex::default(),
            }),
        );
        Ok(())
    }

    /// Remove a workspace route. New operations on its sessions fail with
    /// `Forbidden`; open subscriptions continue until they end. Returns whether
    /// the workspace was registered.
    pub fn unregister(&self, workspace: &Workspace) -> bool {
        self.routes
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(workspace)
            .is_some()
    }

    fn route(&self, workspace: &Workspace) -> Result<Arc<Route>> {
        self.routes
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(workspace)
            .cloned()
            .ok_or(Error::Forbidden)
    }
}

impl Route {
    async fn connect(&self) -> Result<Connection> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let stream = connect_transport(&self.endpoint.socket_path)
                .await
                .map_err(Error::NotDelivered)?;
            let (reader, writer) = stream.split();
            let mut connection = Connection {
                reader: BufReader::new(reader),
                writer,
            };
            let hello = connection.read().await.map_err(|error| match error {
                Error::Runtime(error) => Error::NotDelivered(error),
                error => error,
            })?;
            if hello["type"] != "daemon_hello"
                || hello["protocol"]["name"] != DAEMON_PROTOCOL_NAME
                || hello["protocol"]["version"].as_u64() != Some(DAEMON_PROTOCOL_VERSION)
            {
                return Err(Error::NotDelivered(anyhow::anyhow!(
                    "incompatible daemon handshake"
                )));
            }
            Ok(connection)
        })
        .await
        .map_err(|error| Error::NotDelivered(error.into()))?
    }

    /// Run one request/response command on a pooled connection. A pooled
    /// connection that turns out stale fails before delivery and is replaced.
    async fn command(&self, command: Value) -> Result<Value> {
        let pooled = {
            let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
            idle.retain(|(since, _)| since.elapsed() < POOL_IDLE_TTL);
            idle.pop()
        };
        if let Some((_, mut connection)) = pooled {
            match connection.exchange(command.clone()).await {
                Err(Error::NotDelivered(error)) => {
                    tracing::debug!(%error, "discarding stale daemon connection");
                }
                result => return self.release(connection, result),
            }
        }
        let mut connection = self.connect().await?;
        let result = connection.exchange(command).await;
        self.release(connection, result)
    }

    fn release(
        &self,
        connection: Connection,
        result: Result<(Value, VecDeque<Value>)>,
    ) -> Result<Value> {
        let (data, _) = result?;
        let mut idle = self.idle.lock().unwrap_or_else(PoisonError::into_inner);
        if idle.len() < POOL_IDLE {
            idle.push((Instant::now(), connection));
        }
        Ok(data)
    }
}

impl Runtime for DaemonRuntime {
    #[tracing::instrument(skip_all)]
    async fn create(&self, workspace: &Workspace, session_id: &str) -> Result<String> {
        let route = self.route(workspace)?;
        let data = route
            .command(json!({
                "type": "create", "name": session_id,
                "config": route.endpoint.create_config, "lifecycle": "resident",
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
        // This is ordinary user content, not a system instruction. The envelope
        // preserves attribution in the daemon's durable conversation and context.
        let message =
            serde_json::to_string(&prompt).map_err(|error| Error::Runtime(error.into()))?;
        self.route(&record.session.workspace)?
            .command(json!({
                "type": "prompt", "activeSessionId": runtime_id(record)?,
                "message": message, "streamingBehavior": "followUp", "queueIfBusy": true,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn cancel(&self, record: &StoredSession) -> Result<()> {
        self.route(&record.session.workspace)?
            .command(json!({
                "type": "abort", "activeSessionId": runtime_id(record)?,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn close(&self, record: &StoredSession) -> Result<()> {
        self.route(&record.session.workspace)?
            .command(json!({
                "type": "kill", "activeSessionId": runtime_id(record)?,
            }))
            .await?;
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn subscribe(&self, record: &StoredSession) -> Result<EventStream> {
        let id = runtime_id(record)?.to_owned();
        let route = self.route(&record.session.workspace)?;
        let permit = Arc::clone(&route.subscriptions)
            .try_acquire_owned()
            .map_err(|_| Error::LimitExceeded)?;
        let mut connection = route.connect().await?;
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
            Some((connection, pending, id, Some(snapshot), permit)),
            |state| async move {
                let (mut connection, mut pending, id, snapshot, permit) = state?;
                if let Some(snapshot) = snapshot {
                    return Some((Ok(snapshot), Some((connection, pending, id, None, permit))));
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
                            (!closed).then_some((connection, pending, id, None, permit)),
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

    /// Send one command and wait for its response. A failed write means the
    /// daemon never received a complete line, so it reports `NotDelivered`, as
    /// does an explicit refusal; later failures have an unknown outcome.
    async fn exchange(&mut self, mut command: Value) -> Result<(Value, VecDeque<Value>)> {
        tokio::time::timeout(REQUEST_TIMEOUT, async {
            let roster = command["type"] == "roster_subscribe";
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
                .map_err(|error| Error::NotDelivered(error.into()))?;
            let mut pending = VecDeque::new();
            let mut pending_bytes = 0;
            loop {
                let frame = self.read().await?;
                if frame["type"] == "response" && frame["id"].as_str() == Some(&id) {
                    let response: DaemonResponse = serde_json::from_value(frame)
                        .map_err(|error| Error::Runtime(error.into()))?;
                    if !response.success {
                        return Err(Error::NotDelivered(anyhow::anyhow!(response
                            .error
                            .unwrap_or_else(|| "daemon refused command".to_owned()))));
                    }
                    return Ok((response.data.unwrap_or(Value::Null), pending));
                }
                // Only an explicit operator subscription may receive roster frames.
                if frame
                    .get("activeSessionId")
                    .and_then(Value::as_str)
                    .is_some()
                    || (roster && frame["type"] == "roster_update")
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
