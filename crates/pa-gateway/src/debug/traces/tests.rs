use super::*;
use serde_json::json;

fn record(id: &str) -> ExecutionTrace {
    ExecutionTrace {
        version: 1,
        id: id.into(),
        at_ms: 1,
        pid: 42,
        point: TracePoint::ModelRequest,
        active_session_id: None,
        correlation_id: Some("request-1".into()),
        payload: json!({"body":{"messages":[{"role":"user","content":"real prompt"}],"api_key":"secret"}}),
        truncated: false,
        dropped_total: 0,
    }
}

#[test]
fn collection_redacts_payloads_deduplicates_and_bounds_retention() {
    let mut store = TraceStore::default();
    store.insert(None, record("first"));
    store.insert(None, record("first"));
    assert_eq!(store.entries.len(), 1);
    assert_eq!(
        store.entries[0].1.payload,
        json!({"body":{"messages":[{"role":"user","content":"real prompt"}],"api_key":"[redacted]"}})
    );
    for index in 0..ENTRY_LIMIT {
        store.insert(None, record(&format!("capture:{index}")));
    }
    assert_eq!((store.entries.len(), store.evicted), (ENTRY_LIMIT, 1));
    assert_eq!(
        store.bytes,
        store
            .entries
            .iter()
            .map(|(index, _)| index.bytes)
            .sum::<usize>()
    );
    assert!(!store.entries.iter().any(|(index, _)| index.id == "first"));
}

#[tokio::test]
async fn host_runtime_capture_requires_opt_in_and_redacts_before_serving() {
    let inspector = Inspector::default();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "app".into(),
    };
    assert!(matches!(
        inspector.observe(workspace.clone(), record("host:1")),
        Err(Error::NotReady)
    ));
    let guard = inspector.watch_traces(Vec::new()).unwrap();
    inspector
        .observe(workspace.clone(), record("host:1"))
        .unwrap();
    let mut expected = record("host:1");
    expected.payload["body"]["api_key"] = "[redacted]".into();
    assert_eq!(inspector.trace("host:1"), Some(expected));
    let mut oversized = record("host:2");
    oversized.payload = Value::String("x".repeat(4 * 1024 * 1024));
    assert!(matches!(
        inspector.observe(workspace.clone(), oversized),
        Err(Error::TooLarge)
    ));
    drop(guard);
    assert!(matches!(
        inspector.observe(workspace, record("host:3")),
        Err(Error::NotReady)
    ));
}

#[cfg(unix)]
#[test]
fn reader_waits_for_complete_lines_and_resumes_without_duplicates() {
    use std::{
        io::Write,
        os::unix::fs::{OpenOptionsExt, PermissionsExt},
    };
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(dir.path().join("trace-test.jsonl"))
        .unwrap();
    let expected = record("capture:1");
    let bytes = serde_json::to_vec(&expected).unwrap();
    let mut reader = TraceReader {
        source: TraceSource {
            workspace: Workspace {
                tenant_id: "team".into(),
                workspace_id: "app".into(),
            },
            directory: dir.path().to_path_buf(),
        },
        offsets: BTreeMap::new(),
    };
    file.write_all(&bytes[..bytes.len() / 2]).unwrap();
    let partial = reader.scan();
    assert!(partial.error.is_none());
    assert!(partial.records.is_empty());
    file.write_all(&bytes[bytes.len() / 2..]).unwrap();
    file.write_all(b"\n").unwrap();
    assert_eq!(reader.scan().records, vec![expected]);
    assert!(reader.scan().records.is_empty());
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(reader.scan().error.is_some());
}

#[cfg(unix)]
#[tokio::test]
async fn only_one_collector_runs_and_dropped_guards_disable_http_capture() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let source = || {
        vec![TraceSource {
            workspace: Workspace {
                tenant_id: "team".into(),
                workspace_id: "app".into(),
            },
            directory: dir.path().to_path_buf(),
        }]
    };
    let inspector = Inspector::default();
    let guard = inspector.watch_traces(source()).unwrap();
    assert!(inspector.watch_traces(source()).is_err());
    inspector.capture_http(TracePoint::HttpRequest, "http-1", json!({"prompt":"hello"}));
    assert_eq!(inspector.trace_index().entries.len(), 1);
    drop(guard);
    inspector.capture_http(TracePoint::HttpRequest, "http-2", json!({"prompt":"bye"}));
    assert_eq!(inspector.trace_index().entries.len(), 1);
    assert!(!inspector.trace_index().enabled);
    let _next = inspector.watch_traces(source()).unwrap();
    assert!(inspector.trace_index().enabled);
}

#[tokio::test]
async fn same_timestamp_keeps_wire_order_instead_of_sorting_sequence_ids_lexically() {
    let inspector = Inspector::default();
    let _capture = inspector.watch_traces(Vec::new()).unwrap();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "app".into(),
    };
    inspector
        .observe(workspace.clone(), record("process:9"))
        .unwrap();
    inspector.observe(workspace, record("process:10")).unwrap();
    assert_eq!(
        inspector
            .trace_index()
            .entries
            .into_iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        vec!["process:9", "process:10"]
    );
}
