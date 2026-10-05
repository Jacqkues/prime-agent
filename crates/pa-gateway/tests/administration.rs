mod support;

use std::sync::atomic::Ordering;

use pa_gateway::{Error, SessionStore};
use pa_types::gateway::{PageRequest, SessionRole, SessionStatus};
use support::{page, service, user};

#[tokio::test]
async fn listing_pages_by_session_and_checks_policy_once_per_workspace() {
    let (gateway, _, policy, _) = service();
    let alice = user("team", "alice");
    let mut ids = Vec::new();
    for _ in 0..5 {
        ids.push(
            gateway
                .create(alice.clone(), "project".into())
                .await
                .unwrap()
                .id,
        );
    }
    ids.sort();
    let before = policy.checks.load(Ordering::Relaxed);
    let first = gateway.list(&alice, page(2)).await.unwrap();
    assert_eq!(policy.checks.load(Ordering::Relaxed), before + 1);
    let mut seen: Vec<_> = first.sessions.iter().map(|s| s.id.clone()).collect();
    let mut next = first.next;
    while let Some(after) = next {
        let page = gateway
            .list(
                &alice,
                PageRequest {
                    after: Some(after),
                    limit: 2,
                },
            )
            .await
            .unwrap();
        seen.extend(page.sessions.into_iter().map(|session| session.id));
        next = page.next;
    }
    assert_eq!(seen, ids);
    assert!(matches!(
        gateway.list(&alice, page(0)).await,
        Err(Error::InvalidRequest)
    ));
}

#[tokio::test]
async fn a_failed_runtime_shutdown_can_be_retried() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    *agent.close_error.lock().await = Some(Error::Runtime(anyhow::anyhow!("lost response")));
    assert!(matches!(
        gateway.close(alice.clone(), session.id.clone()).await,
        Err(Error::Runtime(_))
    ));
    assert_eq!(
        gateway.get(&alice, &session.id).await.unwrap().status,
        SessionStatus::Closed
    );
    gateway
        .close(alice.clone(), session.id.clone())
        .await
        .unwrap();
    assert_eq!(*agent.closed.lock().await, vec![session.id.clone()]);
    let closed = gateway.get(&alice, &session.id).await.unwrap();
    assert_eq!(closed.revision, session.revision + 1);
}

#[tokio::test]
async fn administrators_recover_sessions_whose_owner_left_the_workspace() {
    let (gateway, store, policy, agent) = service();
    let alice = user("team", "alice");
    let admin = user("team", "admin");
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
    policy.denied.write().await.insert("alice".into());
    assert!(matches!(
        gateway.close(alice, session.id.clone()).await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway
            .list_workspace(&admin, "project".into(), page(10))
            .await,
        Err(Error::Forbidden)
    ));
    assert!(matches!(
        gateway.close(admin.clone(), session.id.clone()).await,
        Err(Error::NotFound)
    ));

    policy.admins.write().await.insert("admin".into());
    let listed = gateway
        .list_workspace(&admin, "project".into(), page(10))
        .await
        .unwrap();
    assert_eq!(listed.sessions.len(), 1);
    // Administration never grants reading or prompting someone else's session.
    assert!(matches!(
        gateway.get(&admin, &session.id).await,
        Err(Error::NotFound)
    ));
    let transferred = gateway
        .transfer_ownership(admin.clone(), session.id.clone(), "bob".into())
        .await
        .unwrap();
    assert_eq!(
        transferred.members,
        [
            ("alice".to_owned(), SessionRole::Contributor),
            ("bob".to_owned(), SessionRole::Owner),
        ]
        .into()
    );
    gateway
        .close(user("team", "bob"), session.id.clone())
        .await
        .unwrap();
    assert_eq!(*agent.closed.lock().await, vec![session.id.clone()]);

    let mut stuck = store.get("team", &session.id).await.unwrap();
    stuck.session.id = "stuck".into();
    stuck.session.status = SessionStatus::Provisioning;
    stuck.runtime_id = None;
    store.insert(stuck).await.unwrap();
    gateway.close(admin, "stuck".into()).await.unwrap();
    assert_eq!(
        store.get("team", "stuck").await.unwrap().session.status,
        SessionStatus::Closed
    );
    assert_eq!(agent.closed.lock().await.len(), 1);
}
