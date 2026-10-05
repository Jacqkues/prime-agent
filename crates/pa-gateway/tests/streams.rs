mod support;

use std::{sync::atomic::Ordering, time::Duration};

use futures::StreamExt;
use pa_gateway::Error;
use pa_types::gateway::{EventCursor, SessionRole, SubscribeFrom};
use serde_json::json;
use support::{event, sequenced, service, snapshot, user};

#[tokio::test]
async fn delivering_events_does_not_reauthorize_each_one() {
    let (gateway, _, policy, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let mut stream = gateway
        .subscribe(alice, session.id.clone(), SubscribeFrom::Start)
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), snapshot());
    let before = policy.checks.load(Ordering::Relaxed);
    let sender = agent.events.lock().await[&session.id].clone();
    for n in 0..100 {
        sender.send(event(json!({"n": n}))).unwrap();
    }
    for n in 0..100 {
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            event(json!({"n": n}))
        );
    }
    assert_eq!(policy.checks.load(Ordering::Relaxed), before);
    assert_eq!(gateway.metrics().active_subscriptions, 1);
    drop(stream);
    assert_eq!(gateway.metrics().active_subscriptions, 0);
}

#[tokio::test(start_paused = true)]
async fn workspace_policy_revocation_ends_an_idle_stream_within_the_recheck_bound() {
    let (gateway, _, policy, _) = service();
    let alice = user("team", "alice");
    let bob = user("team", "bob");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(alice, session.id.clone(), "bob".into(), SessionRole::Viewer)
        .await
        .unwrap();
    let mut stream = gateway
        .subscribe(bob, session.id, SubscribeFrom::Start)
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), snapshot());
    // A policy change outside the gateway sends no signal; only the bound applies.
    policy.denied.write().await.insert("bob".into());
    let started = tokio::time::Instant::now();
    assert!(matches!(stream.next().await, Some(Err(Error::Forbidden))));
    assert!(started.elapsed() <= Duration::from_secs(5));
    assert!(stream.next().await.is_none());
    assert_eq!(gateway.metrics().streams_revoked, 1);
}

#[tokio::test]
async fn resuming_skips_events_at_or_before_the_cursor() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let mut stream = gateway
        .subscribe(
            alice,
            session.id.clone(),
            SubscribeFrom::After(EventCursor {
                generation: "g1".into(),
                sequence: 2,
            }),
        )
        .await
        .unwrap();
    let sender = agent.events.lock().await[&session.id].clone();
    for sequence in 1..=3 {
        sender
            .send(sequenced(sequence, json!({"n": sequence})))
            .unwrap();
    }
    let mut other = sequenced(1, json!({"restarted": true}));
    other.cursor.as_mut().unwrap().generation = "g2".into();
    sender.send(other.clone()).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), snapshot());
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        sequenced(3, json!({"n": 3}))
    );
    assert_eq!(stream.next().await.unwrap().unwrap(), other);
}
