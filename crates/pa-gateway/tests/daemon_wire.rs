#![cfg(unix)]

use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use futures::StreamExt;
use pa_gateway::{
    DaemonEndpoint, DaemonRuntime, Error, Gateway, MemoryStore, Result, WorkspacePolicy,
};
use pa_types::{
    daemon::{DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION},
    gateway::{
        EventCursor, GatewayAction, Principal, PromptSubmission, RuntimeEvent, RuntimeEventKind,
        SubscribeFrom, Workspace,
    },
    platform::transport::{bind_transport, AsyncWriteHalf},
};
use serde_json::{json, Value};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    sync::mpsc,
};

struct Policy;
impl WorkspacePolicy for Policy {
    fn check(
        &self,
        principal: &Principal,
        workspace: &Workspace,
        _action: GatewayAction,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(if principal.tenant_id == workspace.tenant_id {
            Ok(())
        } else {
            Err(Error::Forbidden)
        })
    }
}

async fn send(writer: &mut dyn AsyncWriteHalf, value: Value) {
    writer
        .write_all(format!("{value}\n").as_bytes())
        .await
        .unwrap();
}

fn workspace() -> Workspace {
    Workspace {
        tenant_id: "team".into(),
        workspace_id: "project".into(),
    }
}

fn endpoint(socket_path: PathBuf, max_subscriptions: usize) -> DaemonEndpoint {
    DaemonEndpoint {
        socket_path,
        create_config: json!({}),
        max_subscriptions,
    }
}

fn alice() -> Principal {
    Principal {
        tenant_id: "team".into(),
        user_id: "alice".into(),
    }
}

fn text(text: &str) -> PromptSubmission {
    PromptSubmission {
        text: text.into(),
        idempotency_key: None,
    }
}

/// How the scripted daemon treats each accepted connection.
#[derive(Clone, Copy)]
enum Daemon {
    /// Serve commands until the client disconnects.
    Persistent,
    /// Close each connection after its first response.
    OneShot,
}

/// Scripted daemon: answers every command and reports `(connection, command)`.
/// An `attach` response is followed by one foreign and one owned session event.
async fn serve(path: PathBuf, daemon: Daemon) -> mpsc::UnboundedReceiver<(usize, Value)> {
    let (commands, received) = mpsc::unbounded_channel();
    let listener = bind_transport(&path).await.unwrap();
    tokio::spawn(async move {
        for connection in 0..usize::MAX {
            let stream = listener.accept().await.unwrap();
            let commands = commands.clone();
            tokio::spawn(async move {
                let (reader, mut writer) = stream.split();
                send(&mut *writer, json!({"type": "daemon_hello", "protocol": {"name": DAEMON_PROTOCOL_NAME, "version": DAEMON_PROTOCOL_VERSION}})).await;
                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                while reader.read_line(&mut line).await.unwrap() > 0 {
                    let request: Value = serde_json::from_str(&line).unwrap();
                    line.clear();
                    let command = request["command"]["type"].as_str().unwrap().to_owned();
                    assert_eq!(request["id"], request["command"]["id"]);
                    assert_eq!(
                        request["protocol"],
                        json!({"name": DAEMON_PROTOCOL_NAME, "version": DAEMON_PROTOCOL_VERSION})
                    );
                    let data = if command == "create" || command == "attach" {
                        json!({"activeSessionId": "worker-1", "messages": [], "lastEventCursor": {"generation": "g1", "sequence": 4}})
                    } else {
                        Value::Null
                    };
                    send(&mut *writer, json!({"type": "response", "id": request["id"], "command": command, "success": true, "data": data})).await;
                    if command == "attach" {
                        send(&mut *writer, json!({"type": "session_event", "activeSessionId": "foreign", "event": {"secret": true}})).await;
                        for sequence in [4, 5] {
                            send(&mut *writer, json!({"type": "session_event", "activeSessionId": "worker-1", "event": {"n": sequence}, "meta": {"cursor": {"generation": "g1", "sequence": sequence}}})).await;
                        }
                    }
                    if matches!(daemon, Daemon::OneShot) {
                        // Report only after closing, so the client's next write
                        // deterministically meets a closed peer.
                        drop((reader, writer));
                        commands
                            .send((connection, request["command"].clone()))
                            .unwrap();
                        return;
                    }
                    commands
                        .send((connection, request["command"].clone()))
                        .unwrap();
                }
            });
        }
    });
    received
}

#[tokio::test]
async fn native_daemon_commands_preserve_attribution_queueing_and_reuse_connections() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let mut received = serve(path.clone(), Daemon::Persistent).await;
    let runtime = DaemonRuntime::new(BTreeMap::from([(workspace(), endpoint(path, 4))])).unwrap();
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::new(runtime),
    );
    let session = gateway.create(alice(), "project".into()).await.unwrap();
    let receipt = gateway
        .prompt(alice(), session.id.clone(), text("hello"))
        .await
        .unwrap();
    let mut stream = gateway
        .subscribe(alice(), session.id.clone(), SubscribeFrom::Start)
        .await
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        RuntimeEvent {
            kind: RuntimeEventKind::Snapshot,
            cursor: Some(EventCursor {
                generation: "g1".into(),
                sequence: 4
            }),
            data: json!({"activeSessionId": "worker-1", "messages": [], "lastEventCursor": {"generation": "g1", "sequence": 4}}),
        }
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap().data,
        json!({"type": "session_event", "activeSessionId": "worker-1", "event": {"n": 4}, "meta": {"cursor": {"generation": "g1", "sequence": 4}}})
    );
    gateway.cancel(alice(), session.id.clone()).await.unwrap();
    gateway.close(alice(), session.id).await.unwrap();
    let mut commands = Vec::new();
    for _ in 0..5 {
        commands.push(received.recv().await.unwrap());
    }
    let shape: Vec<_> = commands
        .iter()
        .map(|(connection, command)| (*connection, command["type"].as_str().unwrap()))
        .collect();
    assert_eq!(
        shape,
        vec![
            (0, "create"),
            (0, "prompt"),
            (1, "attach"),
            (0, "abort"),
            (0, "kill")
        ]
    );
    assert_eq!(commands[0].1["lifecycle"], "resident");
    assert_eq!(commands[1].1["streamingBehavior"], "followUp");
    assert_eq!(commands[1].1["queueIfBusy"], true);
    let content: Value = serde_json::from_str(commands[1].1["message"].as_str().unwrap()).unwrap();
    assert_eq!(
        content,
        json!({"request_id": receipt.request_id, "author": {"tenant_id": "team", "user_id": "alice"}, "text": "hello"})
    );
}

#[tokio::test]
async fn a_closed_pooled_connection_is_replaced_without_losing_the_command() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let mut received = serve(path.clone(), Daemon::OneShot).await;
    let runtime = DaemonRuntime::new(BTreeMap::from([(workspace(), endpoint(path, 4))])).unwrap();
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::new(runtime),
    );
    let session = gateway.create(alice(), "project".into()).await.unwrap();
    assert_eq!(received.recv().await.unwrap().0, 0);
    gateway
        .prompt(alice(), session.id, text("after reconnect"))
        .await
        .unwrap();
    let (connection, command) = received.recv().await.unwrap();
    assert_eq!((connection, command["type"].as_str()), (1, Some("prompt")));
}

#[tokio::test]
async fn resumed_subscriptions_skip_applied_events_and_respect_the_limit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let _received = serve(path.clone(), Daemon::Persistent).await;
    let runtime = DaemonRuntime::new(BTreeMap::from([(workspace(), endpoint(path, 1))])).unwrap();
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::new(runtime),
    );
    let session = gateway.create(alice(), "project".into()).await.unwrap();
    let resume = SubscribeFrom::After(EventCursor {
        generation: "g1".into(),
        sequence: 4,
    });
    let mut stream = gateway
        .subscribe(alice(), session.id.clone(), resume.clone())
        .await
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap().kind,
        RuntimeEventKind::Snapshot
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap().data["event"],
        json!({"n": 5})
    );
    assert!(matches!(
        gateway
            .subscribe(alice(), session.id.clone(), SubscribeFrom::Start)
            .await,
        Err(Error::LimitExceeded)
    ));
    drop(stream);
    let _reopened = gateway
        .subscribe(alice(), session.id, resume)
        .await
        .unwrap();
}

#[tokio::test]
async fn workspaces_can_be_registered_and_unregistered_while_running() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let _received = serve(path.clone(), Daemon::Persistent).await;
    let runtime = Arc::new(DaemonRuntime::new(BTreeMap::new()).unwrap());
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::clone(&runtime),
    );
    assert!(matches!(
        gateway.create(alice(), "project".into()).await,
        Err(Error::Forbidden)
    ));
    runtime
        .register(workspace(), endpoint(path.clone(), 1))
        .unwrap();
    assert!(matches!(
        runtime.register(workspace(), endpoint(path.clone(), 1)),
        Err(Error::Conflict)
    ));
    let other = Workspace {
        tenant_id: "team".into(),
        workspace_id: "other".into(),
    };
    assert!(matches!(
        runtime.register(other, endpoint(path, 1)),
        Err(Error::InvalidRequest)
    ));
    let session = gateway.create(alice(), "project".into()).await.unwrap();
    assert!(runtime.unregister(&workspace()));
    assert!(matches!(
        gateway.prompt(alice(), session.id, text("gone")).await,
        Err(Error::Forbidden)
    ));
}

#[tokio::test]
async fn incompatible_handshake_never_receives_a_command() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let listener = bind_transport(&path).await.unwrap();
    let runtime = DaemonRuntime::new(BTreeMap::from([(workspace(), endpoint(path, 1))])).unwrap();
    let server = tokio::spawn(async move {
        let (reader, mut writer) = listener.accept().await.unwrap().split();
        send(&mut *writer, json!({"type": "daemon_hello", "protocol": {"name": "wrong", "version": DAEMON_PROTOCOL_VERSION}})).await;
        let mut reader = BufReader::new(reader);
        assert_eq!(reader.read_line(&mut String::new()).await.unwrap(), 0);
    });
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::new(runtime),
    );
    assert!(matches!(
        gateway.create(alice(), "project".into()).await,
        Err(Error::NotDelivered(_))
    ));
    let page = pa_types::gateway::PageRequest {
        after: None,
        limit: 10,
    };
    assert_eq!(
        gateway.list(&alice(), page).await.unwrap().sessions[0].status,
        pa_types::gateway::SessionStatus::Failed
    );
    server.await.unwrap();
}
