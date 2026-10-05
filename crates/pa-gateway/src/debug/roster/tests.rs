use super::*;
use pa_types::daemon::agent_roster::AgentRosterStatus;
use serde_json::json;

#[test]
fn roster_applies_child_updates_removals_and_redacts_non_operational_fields() {
    let inspector = Inspector::default();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "app".into(),
    };
    let observer_id = uuid::Uuid::new_v4();
    inspector
        .lock()
        .observers
        .insert(workspace.clone(), observer_id);
    inspector.lock().workspaces.insert(
        workspace.clone(),
        DebugWorkspace {
            workspace: workspace.clone(),
            connection: DebugConnectionStatus::Connecting,
            updated_at_ms: 0,
            agents: Vec::new(),
            truncated: false,
        },
    );
    let row = json!({"agentId":"/private/path/session.jsonl#child", "status":"running", "summary":{
        "sessionId":"child-session", "activeSessionId":"child-worker", "runtimeKind":"subagent", "parentActiveSessionId":"parent-worker", "isStreaming":true,
        "model":{"id":"model-1","provider":"provider-1","apiKey":"secret-token"},
        "systemPrompt":"private prompt", "streamingMessage":"private output", "cwd":"/private/path", "name":"private user title"
    }});
    inspector
        .apply_roster(
            &workspace,
            observer_id,
            &json!({"type":"roster_snapshot","roster":[row]}),
        )
        .unwrap();
    inspector.connection(&workspace, observer_id, DebugConnectionStatus::Connected);
    let expected = DebugAgent {
        id: opaque_agent_id("/private/path/session.jsonl#child"),
        session_id: Some("child-session".into()),
        active_session_id: Some("child-worker".into()),
        gateway_session_id: None,
        parent_active_session_id: Some("parent-worker".into()),
        kind: "subagent".into(),
        status: AgentRosterStatus::Running,
        activity: "thinking".into(),
        model: Some("model-1".into()),
        provider: Some("provider-1".into()),
    };
    assert_eq!(
        inspector.snapshot().workspaces[0].agents,
        vec![expected.clone()]
    );
    let serialized = serde_json::to_string(&inspector.snapshot()).unwrap();
    for forbidden in [
        "private",
        "secret",
        "systemPrompt",
        "streamingMessage",
        "apiKey",
    ] {
        assert!(!serialized.contains(forbidden));
    }
    let before = inspector.snapshot().events.len();
    inspector
        .apply_roster(
            &workspace,
            observer_id,
            &json!({"type":"roster_update","changed":[row],"removed":[]}),
        )
        .unwrap();
    assert_eq!(inspector.snapshot().events.len(), before);
    inspector.connection(&workspace, observer_id, DebugConnectionStatus::Disconnected);
    assert_eq!(inspector.snapshot().workspaces[0].agents, vec![expected]);
    assert_eq!(
        inspector.snapshot().workspaces[0].connection,
        DebugConnectionStatus::Disconnected
    );
    inspector.apply_roster(&workspace, observer_id, &json!({"type":"roster_update","changed":[],"removed":["/private/path/session.jsonl#child"]})).unwrap();
    assert!(inspector.snapshot().workspaces[0].agents.is_empty());
    inspector.connection(&workspace, observer_id, DebugConnectionStatus::Stopped);
    inspector.connection(&workspace, observer_id, DebugConnectionStatus::Connected);
    assert_eq!(
        inspector.snapshot().workspaces[0].connection,
        DebugConnectionStatus::Stopped
    );
    assert!(inspector
        .apply_roster(
            &workspace,
            observer_id,
            &json!({"type":"roster_snapshot","roster":[]})
        )
        .is_err());
}

#[cfg(unix)]
#[tokio::test]
async fn observation_uses_the_native_roster_and_drop_releases_its_connection() {
    use pa_types::{
        daemon::{DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION},
        platform::transport::bind_transport,
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let dir = tempfile::tempdir().unwrap();
    let socket = dir.path().join("daemon.sock");
    let listener = bind_transport(&socket).await.unwrap();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "project".into(),
    };
    let runtime = Arc::new(
        DaemonRuntime::new(BTreeMap::from([(
            workspace,
            crate::DaemonEndpoint {
                socket_path: socket,
                create_config: json!({}),
                max_subscriptions: 1,
            },
        )]))
        .unwrap(),
    );
    let server = tokio::spawn(async move {
        let stream = listener.accept().await.unwrap();
        let (reader, mut writer) = stream.split();
        writer.write_all(format!("{}\n", json!({"type":"daemon_hello","protocol":{"name":DAEMON_PROTOCOL_NAME,"version":DAEMON_PROTOCOL_VERSION}})).as_bytes()).await.unwrap();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let command: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(command["command"]["type"], "roster_subscribe");
        writer.write_all(format!("{}\n", json!({"type":"response","id":command["id"],"command":"roster_subscribe","success":true,"data":{"roster":[{"agentId":"main","status":"idle","summary":{"activeSessionId":"worker","sessionId":"session"}}]}})).as_bytes()).await.unwrap();
        line.clear();
        assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
    });
    let inspector = Inspector::default();
    let watch = inspector.watch_daemon(&runtime).unwrap();
    assert!(matches!(
        inspector.watch_daemon(&runtime),
        Err(Error::Conflict)
    ));
    tokio::time::timeout(Duration::from_secs(3), async {
        while inspector.snapshot().workspaces[0].connection != DebugConnectionStatus::Connected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        inspector.snapshot().workspaces[0].agents[0]
            .active_session_id
            .as_deref(),
        Some("worker")
    );
    drop(watch);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inspector.snapshot().workspaces[0].connection,
        DebugConnectionStatus::Stopped
    );
}

#[test]
fn replaced_observer_cannot_overwrite_a_new_observers_state() {
    let inspector = Inspector::default();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "app".into(),
    };
    let retired = uuid::Uuid::new_v4();
    let current = uuid::Uuid::new_v4();
    let expected = DebugWorkspace {
        workspace: workspace.clone(),
        connection: DebugConnectionStatus::Connecting,
        updated_at_ms: 42,
        agents: Vec::new(),
        truncated: false,
    };
    inspector
        .lock()
        .observers
        .insert(workspace.clone(), current);
    inspector
        .lock()
        .workspaces
        .insert(workspace.clone(), expected.clone());
    inspector.connection(&workspace, retired, DebugConnectionStatus::Stopped);
    assert!(matches!(
        inspector.apply_roster(
            &workspace,
            retired,
            &json!({"type":"roster_snapshot","roster":[]})
        ),
        Err(Error::NotReady)
    ));
    assert_eq!(inspector.snapshot().workspaces, vec![expected]);
}
