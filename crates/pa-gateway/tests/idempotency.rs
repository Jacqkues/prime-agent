mod support;

use pa_gateway::Error;
use pa_types::gateway::{PromptReceipt, PromptSubmission};
use support::{service, user};

fn keyed(text: &str, key: &str) -> PromptSubmission {
    PromptSubmission {
        text: text.into(),
        idempotency_key: Some(key.into()),
    }
}

#[tokio::test]
async fn retrying_an_admitted_prompt_returns_its_receipt_without_resubmitting() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let first = gateway
        .prompt(alice.clone(), session.id.clone(), keyed("once", "k1"))
        .await
        .unwrap();
    let retry = gateway
        .prompt(alice.clone(), session.id.clone(), keyed("once", "k1"))
        .await
        .unwrap();
    assert_eq!(retry, first);
    assert_eq!(agent.prompts.lock().await.len(), 1);
    let other = gateway
        .prompt(alice, session.id, keyed("twice", "k2"))
        .await
        .unwrap();
    assert_ne!(other, first);
    assert_eq!(agent.prompts.lock().await.len(), 2);
    assert_eq!(gateway.metrics().prompts_replayed, 1);
}

#[tokio::test]
async fn keys_are_scoped_to_their_author() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    gateway
        .set_member(
            alice.clone(),
            session.id.clone(),
            "bob".into(),
            pa_types::gateway::SessionRole::Contributor,
        )
        .await
        .unwrap();
    let PromptReceipt { request_id: a } = gateway
        .prompt(alice, session.id.clone(), keyed("a", "shared"))
        .await
        .unwrap();
    let PromptReceipt { request_id: b } = gateway
        .prompt(user("team", "bob"), session.id, keyed("b", "shared"))
        .await
        .unwrap();
    assert_ne!(a, b);
    assert_eq!(agent.prompts.lock().await.len(), 2);
}

#[tokio::test]
async fn an_unknown_outcome_blocks_retries_but_a_certain_failure_frees_the_key() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();

    *agent.prompt_error.lock().await = Some(Error::NotDelivered(anyhow::anyhow!("refused")));
    assert!(matches!(
        gateway
            .prompt(alice.clone(), session.id.clone(), keyed("x", "free"))
            .await,
        Err(Error::NotDelivered(_))
    ));
    gateway
        .prompt(alice.clone(), session.id.clone(), keyed("x", "free"))
        .await
        .unwrap();

    *agent.prompt_error.lock().await = Some(Error::Runtime(anyhow::anyhow!("timed out")));
    assert!(matches!(
        gateway
            .prompt(alice.clone(), session.id.clone(), keyed("y", "unknown"))
            .await,
        Err(Error::Runtime(_))
    ));
    assert!(matches!(
        gateway
            .prompt(alice, session.id, keyed("y", "unknown"))
            .await,
        Err(Error::IdempotencyUnresolved)
    ));
    assert_eq!(agent.prompts.lock().await.len(), 1);
    assert_eq!(gateway.metrics().prompts_failed, 2);
}
