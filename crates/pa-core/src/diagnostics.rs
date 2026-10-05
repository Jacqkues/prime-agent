//! Explicit local execution recording for the engine and its daemon host.
//! `PRIME_AGENT_DEBUG_TRACE_DIR` must name an existing private directory. A
//! bounded writer owns disk IO; disabled recording never clones a payload.

use pa_types::diagnostics::{redact_trace, ExecutionTrace, TracePoint};
use serde_json::{json, Value};
use std::{
    io::Write,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, SyncSender},
        Arc, OnceLock,
    },
};

const PAYLOAD_LIMIT: usize = 4 * 1024 * 1024;
const FILE_LIMIT: u64 = 32 * 1024 * 1024;
const QUEUE_LIMIT: usize = 16;
static WRITER: OnceLock<Option<Recorder>> = OnceLock::new();

pub(crate) fn model_event(id: &str, event: &pa_agent::stream::AssistantMessageEvent) {
    use pa_agent::stream::AssistantMessageEvent as E;
    let body = match event {
        E::Start { partial } => json!({"event":"start","message":partial}),
        E::TextStart {
            content_index,
            partial,
        } => json!({"event":"text_start","index":content_index,"message":partial}),
        E::TextDelta {
            content_index,
            delta,
            ..
        } => json!({"event":"text_delta","index":content_index,"delta":delta}),
        E::TextEnd {
            content_index,
            content,
            ..
        } => json!({"event":"text_end","index":content_index,"content":content}),
        E::ThinkingStart {
            content_index,
            partial,
        } => json!({"event":"thinking_start","index":content_index,"message":partial}),
        E::ThinkingDelta {
            content_index,
            delta,
            ..
        } => json!({"event":"thinking_delta","index":content_index,"delta":delta}),
        E::ThinkingEnd {
            content_index,
            partial,
        } => json!({"event":"thinking_end","index":content_index,"message":partial}),
        E::ToolCallStart {
            content_index,
            partial,
        } => json!({"event":"tool_call_start","index":content_index,"message":partial}),
        E::ToolCallDelta {
            content_index,
            delta,
            ..
        } => json!({"event":"tool_call_delta","index":content_index,"delta":delta}),
        E::ToolCallEnd {
            content_index,
            tool_call,
            ..
        } => json!({"event":"tool_call_end","index":content_index,"tool_call":tool_call}),
        E::Done { reason, message } => json!({"event":"done","reason":reason,"message":message}),
        E::Error { reason, error } => json!({"event":"error","reason":reason,"message":error}),
    };
    record(TracePoint::ModelResponse, &json!({"id":id,"body":body}));
}

struct Recorder {
    sender: SyncSender<ExecutionTrace>,
    queued: Arc<AtomicUsize>,
    dropped: Arc<AtomicU64>,
    seq: AtomicU64,
    instance: String,
    active_session_id: Option<String>,
}

/// Whether this process has explicitly enabled content-bearing local recording.
#[must_use]
pub fn enabled() -> bool {
    recorder().is_some()
}

fn recorder() -> Option<&'static Recorder> {
    WRITER
        .get_or_init(|| {
            let path = PathBuf::from(std::env::var_os("PRIME_AGENT_DEBUG_TRACE_DIR")?);
            match Recorder::start(path) {
                Ok(writer) => Some(writer),
                Err(error) => {
                    eprintln!("Prime Agent execution capture disabled: {error}");
                    None
                }
            }
        })
        .as_ref()
}

/// Observe an existing JSON value; credentials are removed before persistence.
/// Recording is bounded and best-effort, never changes the execution outcome.
pub fn record(point: TracePoint, payload: &Value) {
    if let Some(recorder) = recorder() {
        recorder.record(point, payload);
    }
}

/// Observe a JSON protocol frame without parsing anything on the disabled path.
/// Invalid JSON is represented as text, so protocol failures remain inspectable.
pub fn record_bytes(point: TracePoint, bytes: &[u8]) {
    if let Some(recorder) = recorder() {
        if bytes.len() > PAYLOAD_LIMIT {
            recorder.record(
                point,
                &json!({"capture_omitted":true,"bytes":bytes.len(),"reason":"frame exceeds 4 MiB"}),
            );
        } else {
            let value = serde_json::from_slice(bytes)
                .unwrap_or_else(|_| json!({"invalid_json":String::from_utf8_lossy(bytes)}));
            recorder.record(point, &value);
        }
    }
}

impl Recorder {
    fn start(path: PathBuf) -> std::io::Result<Self> {
        check_private_directory(&path)?;
        let instance = uuid::Uuid::new_v4().to_string();
        let file = path.join(format!("trace-{instance}-0.jsonl"));
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        crate::platform::perms::set_private_mode(&mut options);
        let mut output = options.open(file)?;
        let (sender, receiver) = mpsc::sync_channel::<ExecutionTrace>(QUEUE_LIMIT);
        let queued = Arc::new(AtomicUsize::new(0));
        let writer_queued = Arc::clone(&queued);
        let dropped = Arc::new(AtomicU64::new(0));
        let writer_dropped = Arc::clone(&dropped);
        let writer_instance = instance.clone();
        std::thread::Builder::new()
            .name("execution-capture".into())
            .spawn(move || {
                let mut bytes = 0_u64;
                let mut generation = 0_u64;
                while let Ok(mut record) = receiver.recv() {
                    writer_queued.fetch_sub(1, Ordering::Relaxed);
                    redact_trace(&mut record.payload);
                    let write = || -> std::io::Result<Vec<u8>> { Ok(serde_json::to_vec(&record)?) };
                    let result = write().and_then(|mut line| {
                        line.push(b'\n');
                        if bytes + line.len() as u64 > FILE_LIMIT {
                            generation += 1;
                            check_private_directory(&path)?;
                            output = options.open(
                                path.join(format!("trace-{writer_instance}-{generation}.jsonl")),
                            )?;
                            bytes = 0;
                            if generation >= 2 {
                                std::fs::remove_file(path.join(format!(
                                    "trace-{writer_instance}-{}.jsonl",
                                    generation - 2
                                )))?;
                            }
                        }
                        output.write_all(&line)?;
                        output.flush()?;
                        bytes += line.len() as u64;
                        Ok(())
                    });
                    if let Err(error) = result {
                        writer_dropped.fetch_add(1, Ordering::Relaxed);
                        eprintln!("Prime Agent execution capture write failed: {error}");
                    }
                }
            })?;
        Ok(Self {
            sender,
            queued,
            dropped,
            seq: AtomicU64::new(0),
            instance,
            active_session_id: std::env::var(
                "PRIME_AGENT_INTERNAL_DAEMON_WORKER_ACTIVE_SESSION_ID",
            )
            .ok(),
        })
    }

    fn record(&self, point: TracePoint, payload: &Value) {
        if self
            .queued
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                (value < QUEUE_LIMIT).then_some(value + 1)
            })
            .is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut counter = SizeLimit(0);
        let truncated = serde_json::to_writer(&mut counter, payload).is_err()
            || payload["capture_omitted"] == true;
        let payload = if truncated {
            json!({"capture_omitted":true,"reason":"frame exceeds 4 MiB"})
        } else {
            payload.clone()
        };
        let sequence = self.seq.fetch_add(1, Ordering::Relaxed);
        let record = ExecutionTrace {
            version: 1,
            id: format!("{}:{sequence}", self.instance),
            at_ms: now_ms(),
            pid: std::process::id(),
            point,
            active_session_id: payload
                .get("activeSessionId")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| self.active_session_id.clone()),
            correlation_id: payload.get("id").and_then(Value::as_str).map(str::to_owned),
            payload,
            truncated,
            dropped_total: self.dropped.load(Ordering::Relaxed),
        };
        if self.sender.try_send(record).is_err() {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct SizeLimit(usize);
impl Write for SizeLimit {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len());
        if self.0 > PAYLOAD_LIMIT {
            return Err(std::io::Error::other("capture bound"));
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn check_private_directory(path: &std::path::Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::symlink_metadata(path)?;
        if path.is_absolute()
            && meta.is_dir()
            && !meta.file_type().is_symlink()
            && meta.mode().trailing_zeros() >= 6
            && meta.uid() == nix::unistd::Uid::effective().as_raw()
        {
            return Ok(());
        }
    }
    Err(std::io::Error::other(
        "trace directory must be absolute, owner-only and not a symlink (Unix required)",
    ))
}

#[cfg(test)]
mod tests;
