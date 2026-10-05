//! Application-owned kernel using the same `rlm.repl` process as Prime Agent.
//! A bounded queue owns execution, so a disconnected caller cannot release a
//! half-finished cell to another session. No conversation is stored here.

use anyhow::{anyhow, Context};
use pa_gateway::debug::Inspector;
use pa_types::{
    diagnostics::{ExecutionTrace, TracePoint},
    gateway::Workspace,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    path::Path,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout},
    sync::{mpsc, oneshot, RwLock},
};

use super::catalog::{Catalog, Snapshot};

const PROJECTION: &str = include_str!("projection.py");

#[derive(Clone, Serialize)]
pub struct View {
    pub kernel_id: String,
    pub revision: u64,
    pub state: Value,
    pub error: Option<String>,
    pub catalog: Catalog,
}
#[derive(Default, Serialize)]
pub struct Execution {
    pub request_id: String,
    pub stdout: String,
    pub stderr: String,
    pub result: Option<String>,
    pub status: String,
    pub error: Option<Value>,
    pub truncated: bool,
    pub catalog: Option<Snapshot>,
}
struct Job {
    code: String,
    caller: String,
    reply: oneshot::Sender<anyhow::Result<Execution>>,
}
struct Owner {
    queue: mpsc::Sender<Job>,
    view: Arc<RwLock<View>>,
    task: tokio::task::AbortHandle,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[derive(Clone)]
pub struct SharedKernel(Arc<Owner>);

struct Process {
    _child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    inspector: Inspector,
    workspace: Workspace,
    id: String,
}
impl SharedKernel {
    // Keep process startup and the sole queue owner together so cancellation,
    // projection refresh and kill-on-drop share an explicit lifetime.
    #[allow(clippy::too_many_lines)]
    pub async fn start(
        python: &Path,
        inspector: Inspector,
        workspace: Workspace,
        telemetry: &pa_telemetry::TelemetryClient,
    ) -> anyhow::Result<Self> {
        let mut child = tokio::process::Command::new(python)
            .args(["-m", "rlm.repl"])
            .env(
                "PRIME_AGENT_KERNEL_OWNER_PID",
                std::process::id().to_string(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .context("start application kernel")?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| anyhow!("kernel stdin missing"))?;
        let output = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| anyhow!("kernel stdout missing"))?,
        );
        let id = format!("application-kernel:{}", uuid::Uuid::new_v4());
        let mut process = Process {
            _child: child,
            input,
            output,
            inspector,
            workspace,
            id: id.clone(),
        };
        let ready = tokio::time::timeout(Duration::from_secs(30), process.read()).await??;
        anyhow::ensure!(
            ready["event"] == "ready" && ready["protocol"] == 3,
            "application kernel handshake is incompatible"
        );
        let (init, _) = process
            .execute("app = {'counter': 0, 'tasks': []}", "application")
            .await?;
        anyhow::ensure!(
            init.status == "ok",
            "application kernel initialization failed"
        );
        let view = Arc::new(RwLock::new(View {
            kernel_id: id,
            revision: 0,
            state: json!({"counter":0,"tasks":[]}),
            error: None,
            catalog: Catalog::default(),
        }));
        let (queue, mut jobs) = mpsc::channel::<Job>(16);
        let latest = Arc::clone(&view);
        let task = tokio::spawn(async move {
            while let Some(job) = jobs.recv().await {
                let outcome = tokio::time::timeout(Duration::from_secs(60), async {
                    let (mut execution, _) = process.execute(&job.code, &job.caller).await?;
                    let (projection, state) = process.execute(PROJECTION, "application projection").await?;
                    let mut current = latest.write().await;
                    current.revision += 1;
                    match state {
                        Some(state) if projection.status == "ok" => {
                            current.error = state["state_error"].as_str().map(str::to_owned);
                            if current.error.is_none() { current.state = state["state"].clone(); }
                            current.catalog = serde_json::from_value(state["catalog"].clone())?;
                        }
                        Some(_) | None => {
                            let error = format!("Application projection failed: {}", projection.error.unwrap_or(Value::Null));
                            current.error = Some(error.clone());
                            current.catalog = Catalog { error: Some(error), ..Catalog::default() };
                        }
                    }
                    execution.catalog = Some(Snapshot {
                        kernel_id: current.kernel_id.clone(),
                        revision: current.revision,
                        catalog: current.catalog.clone(),
                    });
                    Ok::<_, anyhow::Error>(execution)
                }).await.unwrap_or_else(|_| Err(anyhow!("Application kernel timed out; stopped without retrying the cell. Restart the demo to create a new kernel.")));
                let failed = outcome.is_err();
                if let Err(error) = &outcome {
                    let mut current = latest.write().await;
                    current.error = Some(error.to_string());
                    current.catalog = Catalog {
                        error: Some(error.to_string()),
                        ..Catalog::default()
                    };
                }
                // A disconnected caller does not undo an admitted execution.
                let _ = job.reply.send(outcome);
                if failed {
                    break;
                }
            }
            // Child's kill_on_drop owns abnormal exit too; never reuse a partial frame.
        });
        for feature_name in ["gateway_shared_kernel", "gateway_kernel_catalog"] {
            pa_telemetry::AgentFeatureOutcome {
                feature_id: uuid::Uuid::new_v4().to_string(),
                feature_name,
                outcome: "completed",
                duration_ms: None,
                configuration_choice: None,
            }
            .track(telemetry);
        }
        Ok(Self(Arc::new(Owner {
            queue,
            view,
            task: task.abort_handle(),
        })))
    }

    pub async fn execute(&self, code: String, caller: String) -> pa_gateway::Result<Execution> {
        if code.trim().is_empty() {
            return Err(pa_gateway::Error::InvalidRequest);
        }
        if code.len() > 64 * 1024 {
            return Err(pa_gateway::Error::TooLarge);
        }
        let (reply, result) = oneshot::channel();
        self.0
            .queue
            .try_send(Job {
                code,
                caller,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => pa_gateway::Error::LimitExceeded,
                mpsc::error::TrySendError::Closed(_) => pa_gateway::Error::NotReady,
            })?;
        result
            .await
            .map_err(|error| pa_gateway::Error::Runtime(error.into()))?
            .map_err(pa_gateway::Error::Runtime)
    }

    pub async fn view(&self) -> View {
        self.0.view.read().await.clone()
    }

    pub async fn catalog(&self) -> Snapshot {
        let view = self.0.view.read().await;
        Snapshot {
            kernel_id: view.kernel_id.clone(),
            revision: view.revision,
            catalog: view.catalog.clone(),
        }
    }
}

impl Process {
    async fn read(&mut self) -> anyhow::Result<Value> {
        let mut bytes = Vec::new();
        (&mut self.output)
            .take(4 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)
            .await?;
        anyhow::ensure!(
            !bytes.is_empty() && bytes.len() <= 4 * 1024 * 1024 && bytes.last() == Some(&b'\n'),
            "application kernel closed or exceeded its frame limit"
        );
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn observe(&self, point: TracePoint, correlation: &str, payload: Value) {
        let record = ExecutionTrace {
            version: 1,
            id: uuid::Uuid::new_v4().to_string(),
            at_ms: u64::try_from(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis(),
            )
            .unwrap_or(u64::MAX),
            pid: std::process::id(),
            point,
            active_session_id: Some(self.id.clone()),
            correlation_id: Some(correlation.into()),
            payload,
            truncated: false,
            dropped_total: 0,
        };
        if let Err(error) = self.inspector.observe(self.workspace.clone(), record) {
            eprintln!("Application kernel trace rejected: {error}");
        }
    }

    async fn execute(
        &mut self,
        code: &str,
        caller: &str,
    ) -> anyhow::Result<(Execution, Option<Value>)> {
        let id = uuid::Uuid::new_v4().to_string();
        let request = json!({"type":"execute","id":id,"code":code});
        self.input
            .write_all(format!("{request}\n").as_bytes())
            .await?;
        self.input.flush().await?;
        self.observe(
            TracePoint::ApplicationKernelSend,
            &id,
            json!({"caller":caller,"frame":request,"code":code}),
        );
        let mut execution = Execution {
            request_id: id.clone(),
            ..Execution::default()
        };
        let mut state = None;
        loop {
            let event = self.read().await?;
            self.observe(TracePoint::ApplicationKernelReceive, &id, event.clone());
            if event["event"] == "host_request" {
                let reply = json!({"type":"host_reply","id":event["id"],"data":{"status":"error","error":"The application kernel has no agent conversation or RLM host. Use the private agent kernel for agent operations."}});
                self.input
                    .write_all(format!("{reply}\n").as_bytes())
                    .await?;
                self.input.flush().await?;
                continue;
            }
            if event["id"].as_str() != Some(&id) {
                continue;
            }
            let text = event["text"].as_str().unwrap_or_default();
            match event["event"].as_str() {
                Some("stdout" | "stderr") => {
                    let target = if event["event"] == "stdout" {
                        &mut execution.stdout
                    } else {
                        &mut execution.stderr
                    };
                    let remaining = (256 * 1024_usize).saturating_sub(target.len());
                    let retained: String = text
                        .chars()
                        .scan(0, |bytes, ch| {
                            *bytes += ch.len_utf8();
                            (*bytes <= remaining).then_some(ch)
                        })
                        .collect();
                    execution.truncated |= retained.len() < text.len();
                    target.push_str(&retained);
                }
                Some("result") => {
                    execution.result = Some(text.chars().take(64 * 1024).collect());
                    execution.truncated |= text.chars().count() > 64 * 1024;
                }
                Some("error") => execution.error = Some(event.clone()),
                Some("display") => {
                    if let Some(value) =
                        event.pointer("/data/application~1vnd.prime-agent.application-state+json")
                    {
                        state = Some(value.clone());
                    }
                }
                Some("done") => {
                    execution.status = event["status"].as_str().unwrap_or("error").into();
                    return Ok((execution, state));
                }
                // Other version-3 frames are recorded but do not mutate this cell's result.
                Some(_) | None => {}
            }
        }
    }
}
