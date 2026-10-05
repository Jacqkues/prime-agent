use super::{now_ms, Inspector};
use crate::{Error, Result};
use pa_types::{
    diagnostics::{redact_trace, ExecutionTrace, TracePoint},
    gateway::{
        debug::{TraceIndex, TraceIndexEntry, TraceSourceStatus},
        Workspace,
    },
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Duration,
};

const BYTE_LIMIT: usize = 64 * 1024 * 1024;
const ENTRY_LIMIT: usize = 2000;
const READ_LIMIT: usize = 8 * 1024 * 1024;
const FILE_LIMIT: usize = 128;

/// A trusted host-configured recorder directory for one isolated workspace.
/// Paths never come from an inspector HTTP request.
pub struct TraceSource {
    pub workspace: Workspace,
    pub directory: PathBuf,
}

/// Keep this guard alive while collecting. Drop ends collection, not execution.
#[must_use]
pub struct TraceWatch {
    task: tokio::task::JoinHandle<()>,
    inspector: Inspector,
    generation: uuid::Uuid,
}

impl Drop for TraceWatch {
    fn drop(&mut self) {
        self.task.abort();
        let mut state = self.inspector.lock();
        if state.traces.generation != Some(self.generation) {
            return;
        }
        state.traces.generation = None;
        for source in &mut state.traces.sources {
            source.error = Some("Collection stopped".into());
        }
    }
}

#[derive(Default)]
pub(super) struct TraceStore {
    generation: Option<uuid::Uuid>,
    entries: VecDeque<(TraceIndexEntry, ExecutionTrace)>,
    sources: Vec<TraceSourceStatus>,
    bytes: usize,
    evicted: u64,
    dropped: BTreeMap<String, u64>,
    archived_dropped: u64,
}

impl Inspector {
    /// Collect the engine/daemon's explicitly enabled content-bearing capture.
    /// Directory scans run off the async executor. No raw content enters the
    /// metadata snapshot; operator-authenticated trace routes expose it.
    ///
    /// # Errors
    /// Refuses duplicate collectors, public/symlinked directories,
    /// duplicate workspace roots, or more than 64 sources. Unix only.
    /// # Panics
    /// Requires a Tokio runtime.
    pub fn watch_traces(&self, sources: Vec<TraceSource>) -> Result<TraceWatch> {
        if sources.len() > 64 {
            return Err(Error::InvalidRequest);
        }
        let mut roots = std::collections::BTreeSet::new();
        let mut workspaces = std::collections::BTreeSet::new();
        for source in &sources {
            private_directory(&source.directory).map_err(Error::Storage)?;
            if !roots.insert(
                source
                    .directory
                    .canonicalize()
                    .map_err(|error| Error::Storage(error.into()))?,
            ) || !workspaces.insert(source.workspace.clone())
            {
                return Err(Error::Conflict);
            }
        }
        let generation = uuid::Uuid::new_v4();
        {
            let mut state = self.lock();
            if state.traces.generation.is_some() {
                return Err(Error::Conflict);
            }
            state.traces.generation = Some(generation);
            state.traces.sources = sources
                .iter()
                .map(|source| TraceSourceStatus {
                    workspace: source.workspace.clone(),
                    last_scan_ms: 0,
                    error: None,
                })
                .collect();
        }
        let inspector = self.clone();
        let task = tokio::spawn(async move {
            let mut readers: Vec<_> = sources
                .into_iter()
                .map(|source| TraceReader {
                    source,
                    offsets: BTreeMap::new(),
                })
                .collect();
            let mut interval = tokio::time::interval(Duration::from_millis(250));
            loop {
                interval.tick().await;
                let result = tokio::task::spawn_blocking(move || {
                    let batches: Vec<_> = readers.iter_mut().map(TraceReader::scan).collect();
                    (readers, batches)
                })
                .await;
                match result {
                    Ok((next, batches)) => {
                        readers = next;
                        let mut state = inspector.lock();
                        if state.traces.generation != Some(generation) {
                            return;
                        }
                        for (index, batch) in batches.into_iter().enumerate() {
                            let source = &mut state.traces.sources[index];
                            source.last_scan_ms = now_ms();
                            source.error = batch.error;
                            let workspace = source.workspace.clone();
                            for record in batch.records {
                                state.traces.insert(Some(workspace.clone()), record);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::error!(%error, "execution trace collector failed");
                        let mut state = inspector.lock();
                        if state.traces.generation != Some(generation) {
                            return;
                        }
                        for source in &mut state.traces.sources {
                            source.error = Some("Trace collector failed".into());
                        }
                        return;
                    }
                }
            }
        });
        Ok(TraceWatch {
            task,
            inspector: self.clone(),
            generation,
        })
    }

    /// Observe a host-owned runtime boundary (for example an application kernel).
    /// Explicitly enable capture with `watch_traces` first; an empty source list
    /// enables in-memory host capture without collecting files. Content is redacted
    /// and retained under the same bounds and operator authorization as file traces.
    ///
    /// # Errors
    /// Returns `NotReady` when capture is disabled, `InvalidRequest` for an unknown
    /// schema version and `TooLarge` for payloads above 4 MiB.
    pub fn observe(&self, workspace: Workspace, record: ExecutionTrace) -> Result<()> {
        if record.version != 1 {
            return Err(Error::InvalidRequest);
        }
        if record.payload.to_string().len() > 4 * 1024 * 1024 {
            return Err(Error::TooLarge);
        }
        let mut state = self.lock();
        if state.traces.generation.is_none() {
            return Err(Error::NotReady);
        }
        state.traces.insert(Some(workspace), record);
        Ok(())
    }

    /// Read the index without copying large payloads.
    #[must_use]
    pub fn trace_index(&self) -> TraceIndex {
        let state = self.lock();
        let traces = &state.traces;
        let mut entries: Vec<_> = traces
            .entries
            .iter()
            .map(|(index, _)| index.clone())
            .collect();
        // Stable sorting preserves a producer's recorded order for same-ms frames.
        entries.sort_by_key(|entry| entry.at_ms);
        TraceIndex {
            enabled: traces.generation.is_some(),
            entries,
            sources: traces.sources.clone(),
            evicted: traces.evicted,
            dropped: traces.archived_dropped + traces.dropped.values().sum::<u64>(),
            retained_bytes: traces.bytes,
        }
    }

    /// Fetch one retained, already-redacted payload by its opaque capture ID.
    #[must_use]
    pub fn trace(&self, id: &str) -> Option<ExecutionTrace> {
        self.lock()
            .traces
            .entries
            .iter()
            .find(|(index, _)| index.id == id)
            .map(|(_, record)| record.clone())
    }

    pub(super) fn capture_http(&self, point: TracePoint, id: &str, payload: Value) {
        let mut state = self.lock();
        if state.traces.generation.is_none() {
            return;
        }
        state.traces.insert(
            None,
            ExecutionTrace {
                version: 1,
                id: uuid::Uuid::new_v4().to_string(),
                at_ms: now_ms(),
                pid: std::process::id(),
                point,
                active_session_id: None,
                correlation_id: Some(id.to_owned()),
                payload,
                truncated: false,
                dropped_total: 0,
            },
        );
    }

    pub(crate) fn traces_enabled(&self) -> bool {
        self.lock().traces.generation.is_some()
    }
}

impl TraceStore {
    fn insert(&mut self, workspace: Option<Workspace>, mut record: ExecutionTrace) {
        if record.version != 1 || self.entries.iter().any(|(index, _)| index.id == record.id) {
            return;
        }
        redact_trace(&mut record.payload);
        if record.active_session_id.is_none() {
            record.active_session_id = record
                .payload
                .pointer("/command/activeSessionId")
                .or_else(|| record.payload.pointer("/data/activeSessionId"))
                .and_then(Value::as_str)
                .map(str::to_owned);
        }
        let bytes = record.payload.to_string().len();
        if bytes > BYTE_LIMIT {
            self.evicted += 1;
            return;
        }
        while self.bytes + bytes > BYTE_LIMIT || self.entries.len() >= ENTRY_LIMIT {
            if let Some((index, _)) = self.entries.pop_front() {
                self.bytes -= index.bytes;
                self.evicted += 1;
            }
        }
        let instance = record.id.split_once(':').map_or_else(
            || record.pid.to_string(),
            |(instance, _)| instance.to_owned(),
        );
        if !self.dropped.contains_key(&instance) && self.dropped.len() >= ENTRY_LIMIT {
            if let Some((_, count)) = self.dropped.pop_first() {
                self.archived_dropped += count;
            }
        }
        self.dropped
            .entry(instance)
            .and_modify(|count| *count = (*count).max(record.dropped_total))
            .or_insert(record.dropped_total);
        let data = &record.payload;
        let label = data
            .pointer("/command/type")
            .or_else(|| data.get("command"))
            .or_else(|| data.pointer("/event/type"))
            .or_else(|| data.pointer("/body/event"))
            .or_else(|| data.get("type"))
            .or_else(|| data.get("method"))
            .and_then(Value::as_str)
            .unwrap_or("exchange")
            .chars()
            .take(120)
            .collect();
        let index = TraceIndexEntry {
            id: record.id.clone(),
            at_ms: record.at_ms,
            point: record.point,
            workspace,
            active_session_id: record.active_session_id.clone(),
            correlation_id: record.correlation_id.clone(),
            pid: record.pid,
            label,
            bytes,
            truncated: record.truncated,
        };
        self.bytes += bytes;
        self.entries.push_back((index, record));
    }
}

struct TraceReader {
    source: TraceSource,
    offsets: BTreeMap<PathBuf, u64>,
}
#[derive(Default)]
struct Batch {
    records: Vec<ExecutionTrace>,
    error: Option<String>,
}

impl TraceReader {
    fn scan(&mut self) -> Batch {
        let mut batch = Batch::default();
        if let Err(error) = self.read(&mut batch.records) {
            batch.error = Some(error.to_string());
        }
        batch
    }

    fn read(&mut self, records: &mut Vec<ExecutionTrace>) -> anyhow::Result<()> {
        private_directory(&self.source.directory)?;
        let mut files =
            std::fs::read_dir(&self.source.directory)?.collect::<std::io::Result<Vec<_>>>()?;
        files.retain(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.starts_with("trace-")
                    && Path::new(name)
                        .extension()
                        .is_some_and(|ext| ext == "jsonl")
            })
        });
        files.sort_by_key(|entry| entry.metadata().and_then(|meta| meta.modified()).ok());
        anyhow::ensure!(
            files.len() <= FILE_LIMIT,
            "Trace directory has more than 128 segments; archive older captures"
        );
        self.offsets
            .retain(|path, _| files.iter().any(|entry| entry.path() == *path));
        let mut budget = 16 * 1024 * 1024;
        for entry in files {
            if budget == 0 {
                break;
            }
            let path = entry.path();
            let metadata = std::fs::symlink_metadata(&path)?;
            anyhow::ensure!(
                metadata.is_file() && !metadata.file_type().is_symlink(),
                "Trace segment is not a regular file"
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                anyhow::ensure!(
                    metadata.mode().trailing_zeros() >= 6,
                    "Trace segment is not owner-only"
                );
            }
            let mut file = std::fs::File::open(&path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let opened = file.metadata()?;
                anyhow::ensure!(
                    opened.ino() == metadata.ino() && opened.dev() == metadata.dev(),
                    "Trace segment changed while opening"
                );
            }
            let offset = self.offsets.entry(path).or_default();
            if metadata.len() < *offset {
                *offset = 0;
            }
            file.seek(SeekFrom::Start(*offset))?;
            let mut bytes = Vec::new();
            file.take(READ_LIMIT.min(budget) as u64)
                .read_to_end(&mut bytes)?;
            budget -= bytes.len();
            let mut consumed = 0;
            for line in bytes.split_inclusive(|byte| *byte == b'\n') {
                if line.last() != Some(&b'\n') {
                    break;
                }
                let record: ExecutionTrace = serde_json::from_slice(line)?;
                records.push(record);
                consumed += line.len();
            }
            *offset += consumed as u64;
            anyhow::ensure!(
                consumed != 0 || bytes.len() < READ_LIMIT,
                "Trace frame exceeds reader limit"
            );
        }
        Ok(())
    }
}

fn private_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = std::fs::symlink_metadata(path)?;
        anyhow::ensure!(
            path.is_absolute()
                && metadata.is_dir()
                && !metadata.file_type().is_symlink()
                && metadata.mode().trailing_zeros() >= 6,
            "Trace directory must be absolute, owner-only and not a symlink"
        );
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        anyhow::bail!("Trace file collection requires Unix permissions")
    }
}

#[cfg(test)]
mod tests;
