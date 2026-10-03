#![cfg(unix)]

use std::{collections::BTreeMap, sync::Arc};

use futures::StreamExt;
use pa_gateway::{
    DaemonEndpoint, DaemonRuntime, Error, Gateway, MemoryStore, Result, WorkspacePolicy,
};
use pa_types::{
    daemon::{DAEMON_PROTOCOL_NAME, DAEMON_PROTOCOL_VERSION},
    gateway::{GatewayAction, Principal, Workspace},
    platform::transport::{bind_transport, AsyncWriteHalf},
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

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

#[tokio::test]
async fn native_daemon_commands_preserve_attribution_queueing_and_snapshot_order() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let listener = bind_transport(&path).await.unwrap();
    let workspace = Workspace {
        tenant_id: "team".into(),
        workspace_id: "project".into(),
    };
    let runtime = DaemonRuntime::new(BTreeMap::from([(
        workspace,
        DaemonEndpoint {
            socket_path: path,
            create_config: json!({"cwd": dir.path()}),
        },
    )]))
    .unwrap();
    let server = tokio::spawn(async move {
        let mut received = Vec::new();
        for expected in ["create", "prompt", "attach", "abort", "kill"] {
            let stream = listener.accept().await.unwrap();
            let (reader, mut writer) = stream.split();
            send(&mut *writer, json!({"type": "daemon_hello", "protocol": {"name": DAEMON_PROTOCOL_NAME, "version": DAEMON_PROTOCOL_VERSION}})).await;
            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["command"]["type"], expected);
            assert_eq!(request["id"], request["command"]["id"]);
            assert_eq!(
                request["protocol"],
                json!({"name": DAEMON_PROTOCOL_NAME, "version": DAEMON_PROTOCOL_VERSION})
            );
            let data = if expected == "create" || expected == "attach" {
                json!({"activeSessionId": "worker-1", "messages": []})
            } else {
                Value::Null
            };
            send(&mut *writer, json!({"type": "response", "id": request["id"], "command": expected, "success": true, "data": data})).await;
            if expected == "attach" {
                send(&mut *writer, json!({"type": "session_event", "activeSessionId": "foreign", "event": {"secret": true}})).await;
                send(&mut *writer, json!({"type": "session_event", "activeSessionId": "worker-1", "event": {"text": "answer"}})).await;
            }
            received.push(request["command"].clone());
        }
        received
    });
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::new(Policy),
        Arc::new(runtime),
    );
    let alice = Principal {
        tenant_id: "team".into(),
        user_id: "alice".into(),
    };
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let receipt = gateway
        .prompt(alice.clone(), session.id.clone(), "hello".into())
        .await
        .unwrap();
    let mut stream = gateway
        .subscribe(alice.clone(), session.id.clone())
        .await
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        json!({"type": "snapshot", "data": {"activeSessionId": "worker-1", "messages": []}})
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        json!({"type": "session_event", "activeSessionId": "worker-1", "event": {"text": "answer"}})
    );
    assert!(matches!(stream.next().await, Some(Err(Error::Runtime(_)))));
    assert!(stream.next().await.is_none());
    gateway
        .cancel(alice.clone(), session.id.clone())
        .await
        .unwrap();
    gateway.close(alice, session.id).await.unwrap();
    let received = server.await.unwrap();
    assert_eq!(received[0]["lifecycle"], "resident");
    assert_eq!(received[1]["streamingBehavior"], "followUp");
    assert_eq!(received[1]["queueIfBusy"], true);
    let content: Value = serde_json::from_str(received[1]["message"].as_str().unwrap()).unwrap();
    assert_eq!(
        content,
        json!({"request_id": receipt.request_id, "author": {"tenant_id": "team", "user_id": "alice"}, "text": "hello"})
    );
}

#[tokio::test]
async fn incompatible_handshake_never_receives_a_command() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("daemon.sock");
    let listener = bind_transport(&path).await.unwrap();
    let runtime = DaemonRuntime::new(BTreeMap::from([(
        Workspace {
            tenant_id: "team".into(),
            workspace_id: "project".into(),
        },
        DaemonEndpoint {
            socket_path: path,
            create_config: json!({}),
        },
    )]))
    .unwrap();
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
    let alice = Principal {
        tenant_id: "team".into(),
        user_id: "alice".into(),
    };
    assert!(matches!(
        gateway.create(alice.clone(), "project".into()).await,
        Err(Error::Runtime(_))
    ));
    assert_eq!(
        gateway.list(&alice).await.unwrap()[0].status,
        pa_types::gateway::SessionStatus::Failed
    );
    server.await.unwrap();
}
