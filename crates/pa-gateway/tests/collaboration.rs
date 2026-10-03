mod support;

use futures::StreamExt;
use pa_gateway::{Error, SessionStore};
use pa_types::gateway::{AttributedPrompt, SessionRole, SessionStatus};
use serde_json::json;
use support::{service, user};

#[tokio::test]
async fn disconnect_during_admission_does_not_cancel_the_mutation() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, proceed) = tokio::sync::oneshot::channel();
    let (committed, done) = tokio::sync::oneshot::channel();
    *agent.admission_gate.lock().await = Some(support::AdmissionGate {
        entered,
        release: proceed,
        committed,
    });
    let request = tokio::spawn(async move {
        gateway
            .prompt(alice, session.id, "keep working".into())
            .await
    });
    ready.await.unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    done.await.unwrap();
    let prompts = agent.prompts.lock().await;
    assert_eq!(prompts.len(), 1);
    assert_eq!(prompts[0].text, "keep working");
}

#[tokio::test]
async fn collaborators_share_events_and_reconnect_without_stopping_work() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let bob = user("team", "bob");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            SessionRole::Contributor,
        )
        .await
        .unwrap();
    let mut first = gateway
        .subscribe(alice.clone(), session.id.clone())
        .await
        .unwrap();
    let mut second = gateway
        .subscribe(bob.clone(), session.id.clone())
        .await
        .unwrap();
    assert_eq!(
        first.next().await.unwrap().unwrap(),
        second.next().await.unwrap().unwrap()
    );
    let event = json!({"type": "token", "text": "shared"});
    agent.events.lock().await[&session.id]
        .send(event.clone())
        .unwrap();
    assert_eq!(first.next().await.unwrap().unwrap(), event);
    assert_eq!(second.next().await.unwrap().unwrap(), event);
    drop(first);
    drop(second);
    let (a, b) = tokio::join!(
        gateway.prompt(alice, session.id.clone(), "a".into()),
        gateway.prompt(bob.clone(), session.id.clone(), "b".into())
    );
    let expected = [
        AttributedPrompt {
            request_id: a.unwrap().request_id,
            author: user("team", "alice"),
            text: "a".into(),
        },
        AttributedPrompt {
            request_id: b.unwrap().request_id,
            author: bob.clone(),
            text: "b".into(),
        },
    ];
    let prompts = agent.prompts.lock().await;
    assert_eq!(prompts.len(), 2);
    assert!(expected.iter().all(|prompt| prompts.contains(prompt)));
    drop(prompts);
    let mut reconnected = gateway.subscribe(bob, session.id).await.unwrap();
    assert_eq!(
        reconnected.next().await.unwrap().unwrap(),
        json!({"type": "snapshot"})
    );
    assert!(agent.closed.lock().await.is_empty());
}

#[tokio::test]
async fn shared_sessions_check_tenant_membership_and_role_before_runtime_access() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let bob = user("team", "bob");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    assert!(matches!(
        gateway.get(&bob, &session.id).await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        gateway.get(&user("other", "alice"), &session.id).await,
        Err(Error::NotFound)
    ));
    assert!(gateway.list(&bob).await.unwrap().is_empty());

    let shared = gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            SessionRole::Viewer,
        )
        .await
        .unwrap();
    assert_eq!(gateway.list(&bob).await.unwrap(), vec![shared.clone()]);
    assert_eq!(gateway.get(&bob, &session.id).await.unwrap(), shared);
    assert!(matches!(
        gateway
            .prompt(bob.clone(), session.id.clone(), "work".into())
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway.cancel(bob.clone(), session.id.clone()).await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway.close(bob.clone(), session.id.clone()).await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway
            .set_member(
                bob.clone(),
                session.id.clone(),
                "eve".into(),
                SessionRole::Contributor
            )
            .await,
        Err(Error::Forbidden)
    ));
    assert!(agent.prompts.lock().await.is_empty());
    assert!(agent.closed.lock().await.is_empty());

    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            SessionRole::Contributor,
        )
        .await
        .unwrap();
    let receipt = gateway
        .prompt(bob.clone(), session.id.clone(), "investigate".into())
        .await
        .unwrap();
    assert_eq!(
        *agent.prompts.lock().await,
        vec![AttributedPrompt {
            request_id: receipt.request_id,
            author: bob.clone(),
            text: "investigate".into(),
        }]
    );
}

#[tokio::test]
async fn workspace_revocation_and_session_close_fence_further_work() {
    let (gateway, _, policy, _) = service();
    let alice = user("team", "alice");
    let bob = user("team", "bob");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            SessionRole::Contributor,
        )
        .await
        .unwrap();
    policy.denied.write().await.insert("bob".into());
    assert!(matches!(
        gateway
            .prompt(bob.clone(), session.id.clone(), "again".into())
            .await,
        Err(Error::Forbidden)
    ));
    assert!(gateway.list(&bob).await.unwrap().is_empty());
    assert!(matches!(
        gateway
            .set_member(
                alice.clone(),
                session.id.clone(),
                "bob".into(),
                SessionRole::Viewer
            )
            .await,
        Err(Error::Forbidden)
    ));
    gateway
        .close(alice.clone(), session.id.clone())
        .await
        .unwrap();
    assert_eq!(
        gateway.get(&alice, &session.id).await.unwrap().status,
        SessionStatus::Closed
    );
    assert!(matches!(
        gateway.prompt(alice, session.id, "again".into()).await,
        Err(Error::NotReady)
    ));
}

#[tokio::test]
async fn revoke_stops_existing_stream_without_delivering_the_next_event() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let bob = user("team", "bob");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            SessionRole::Viewer,
        )
        .await
        .unwrap();
    let mut stream = gateway.subscribe(bob, session.id.clone()).await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        json!({"type": "snapshot"})
    );
    gateway
        .remove_member(alice, session.id.clone(), "bob".into())
        .await
        .unwrap();
    agent.events.lock().await[&session.id]
        .send(json!({"secret": "after revocation"}))
        .unwrap();
    assert!(matches!(stream.next().await, Some(Err(Error::NotFound))));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn concurrent_membership_writes_cannot_overwrite_each_other() {
    let (gateway, store, _, _) = service();
    let session = gateway
        .create(user("team", "alice"), "project".into())
        .await
        .unwrap();
    let mut first = store.get("team", &session.id).await.unwrap();
    let mut second = first.clone();
    first
        .session
        .members
        .insert("bob".into(), SessionRole::Viewer);
    second
        .session
        .members
        .insert("eve".into(), SessionRole::Viewer);
    first.session.revision += 1;
    second.session.revision += 1;
    let (a, b) = tokio::join!(
        store.replace(first.clone(), session.revision),
        store.replace(second.clone(), session.revision)
    );
    let winner = match (a, b) {
        (Ok(()), Err(Error::Conflict)) => first,
        (Err(Error::Conflict), Ok(())) => second,
        results => panic!("expected one successful CAS: {results:?}"),
    };
    assert_eq!(store.get("team", &session.id).await.unwrap(), winner);
}

#[tokio::test]
async fn owner_cannot_be_removed_or_duplicated_and_inputs_are_bounded() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    assert!(matches!(
        gateway
            .remove_member(alice.clone(), session.id.clone(), "alice".into())
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway
            .set_member(
                alice.clone(),
                session.id.clone(),
                "alice".into(),
                SessionRole::Viewer
            )
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway
            .set_member(
                alice.clone(),
                session.id.clone(),
                "bob".into(),
                SessionRole::Owner
            )
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway
            .prompt(alice.clone(), session.id.clone(), " ".into())
            .await,
        Err(Error::InvalidRequest)
    ));
    assert!(matches!(
        gateway.prompt(alice, session.id, "x".repeat(65_537)).await,
        Err(Error::LimitExceeded)
    ));
    assert!(agent.prompts.lock().await.is_empty());
}

#[tokio::test]
async fn adoption_events_do_not_contain_session_or_participant_content() {
    let (gateway, _, _, _) = service();
    let sink = std::sync::Arc::new(pa_telemetry::MockSink::new());
    let mut config = pa_telemetry::TelemetryClientConfig::new("host-installation");
    config.sinks = vec![sink.clone()];
    let client = pa_telemetry::TelemetryClient::spawn(config).unwrap();
    let gateway = gateway.with_telemetry(client.clone());
    let alice = user("private-tenant", "private-user");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "private-colleague".into(),
            SessionRole::Contributor,
        )
        .await
        .unwrap();
    gateway
        .prompt(alice, session.id, "secret prompt".into())
        .await
        .unwrap();
    client.shutdown().await.unwrap();
    let events = sink.events();
    let features: Vec<_> = events
        .iter()
        .map(|event| event.properties.get("feature_name").unwrap().clone())
        .collect();
    assert_eq!(
        features,
        vec![
            json!("gateway_session"),
            json!("gateway_share"),
            json!("gateway_prompt")
        ]
    );
    for event in events {
        assert_eq!(event.name, "agent feature outcome");
        let serialized = serde_json::to_string(&event).unwrap();
        assert!(!serialized.contains("private-"));
        assert!(!serialized.contains("secret prompt"));
    }
}
