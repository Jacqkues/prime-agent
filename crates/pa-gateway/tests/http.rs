#![cfg(feature = "http")]

mod support;

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
    response::Response,
};
use futures::StreamExt;
use pa_gateway::{
    http::{router, Authenticator},
    Error, Result,
};
use pa_types::gateway::{Principal, Session, SessionRole};
use serde_json::{json, Value};
use tokio::sync::RwLock;
use tower::ServiceExt;

use support::{sequenced, service, user};

struct Auth {
    tokens: RwLock<BTreeMap<String, Principal>>,
    revalidation: Duration,
    calls: AtomicUsize,
}

impl Auth {
    fn new(tokens: BTreeMap<String, Principal>, revalidation: Duration) -> Arc<Self> {
        Arc::new(Self {
            tokens: RwLock::new(tokens),
            revalidation,
            calls: AtomicUsize::new(0),
        })
    }
}

impl Authenticator for Auth {
    fn stream_revalidation(&self) -> Duration {
        self.revalidation
    }

    async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let token = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or(Error::Unauthenticated)?;
        self.tokens
            .read()
            .await
            .get(token)
            .cloned()
            .ok_or(Error::Unauthenticated)
    }
}

fn request(method: &str, uri: &str, token: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header("authorization", token)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn body(response: Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
}

#[tokio::test]
async fn embedding_preserves_host_identity_and_enforces_shared_session_permissions() {
    let (gateway, _, _, agent) = service();
    let auth = Auth::new(
        BTreeMap::from([
            ("Bearer alice-key".into(), user("team", "alice")),
            ("Bearer bob-key".into(), user("team", "bob")),
            ("Bearer outsider-key".into(), user("other", "alice")),
        ]),
        Duration::ZERO,
    );
    let app = axum::Router::new().nest("/my-app/v1", router(gateway, auth));
    let base = "/my-app/v1/sessions";
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            base,
            "bad",
            &json!({"workspace_id": "project"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            base,
            "Bearer alice-key",
            &json!({"workspace_id": "project"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let session: Session = serde_json::from_value(body(response).await).unwrap();
    let path = format!("{base}/{}", session.id);
    for token in ["Bearer bob-key", "Bearer outsider-key"] {
        let response = app
            .clone()
            .oneshot(request("GET", &path, token, &Value::Null))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let response = app
        .clone()
        .oneshot(request(
            "PUT",
            &format!("{path}/members/bob"),
            "Bearer alice-key",
            &json!({"role": "contributor"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut shared = session;
    shared
        .members
        .insert("bob".into(), SessionRole::Contributor);
    shared.revision += 1;
    assert_eq!(body(response).await, serde_json::to_value(shared).unwrap());

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("{path}/prompts"),
            "Bearer bob-key",
            &json!({"text": "hello", "author": "alice"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    assert!(agent.prompts.lock().await.is_empty());
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("{path}/prompts"),
            "Bearer bob-key",
            &json!({"text": "hello"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let receipt = body(response).await;
    assert_eq!(
        serde_json::to_value(&*agent.prompts.lock().await).unwrap(),
        json!([{
            "request_id": receipt["request_id"], "author": {"tenant_id": "team", "user_id": "bob"}, "text": "hello",
        }])
    );
    let response = app
        .oneshot(request("DELETE", &path, "Bearer bob-key", &Value::Null))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn revoked_credentials_terminate_an_open_sse_stream() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let auth = Auth::new(
        BTreeMap::from([("Bearer key".into(), alice)]),
        Duration::ZERO,
    );
    let app = router(gateway, Arc::clone(&auth));
    let response = app
        .oneshot(request(
            "GET",
            &format!("/sessions/{}/events", session.id),
            "Bearer key",
            &Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let mut stream = response.into_body().into_data_stream();
    let snapshot = stream.next().await.unwrap().unwrap();
    assert!(std::str::from_utf8(&snapshot).unwrap().contains("snapshot"));
    auth.tokens.write().await.clear();
    agent.events.lock().await[&session.id]
        .send(support::event(json!({"private": "new data"})))
        .unwrap();
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&frame).unwrap(),
        "event: error\ndata: {\"error\":\"authentication required\",\"code\":\"unauthenticated\"}\n\n"
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn http_contract_versions_events_resumes_and_reports_stable_codes() {
    let (gateway, _, _, agent) = service();
    let alice = user("team", "alice");
    let session = gateway
        .create(alice.clone(), "project".into())
        .await
        .unwrap();
    let auth = Auth::new(
        BTreeMap::from([("Bearer key".into(), alice)]),
        Duration::from_secs(3600),
    );
    let app = router(gateway, Arc::clone(&auth));
    let base = format!("/sessions/{}", session.id);

    let response = app
        .clone()
        .oneshot(request(
            "POST",
            &format!("{base}/prompts"),
            "Bearer key",
            &json!({"text": "x".repeat(65_537)}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        body(response).await,
        json!({"error": "request is too large", "code": "too_large"})
    );

    let mut resume = request("GET", &format!("{base}/events"), "Bearer key", &Value::Null);
    resume
        .headers_mut()
        .insert("last-event-id", "g1:1".parse().unwrap());
    let response = app.oneshot(resume).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let calls = auth.calls.load(Ordering::Relaxed);
    let mut stream = response.into_body().into_data_stream();
    let snapshot = stream.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&snapshot).unwrap(),
        "event: runtime\ndata: {\"v\":1,\"kind\":\"snapshot\",\"cursor\":null,\"data\":{\"type\":\"snapshot\"}}\n\n"
    );
    let sender = agent.events.lock().await[&session.id].clone();
    for sequence in 1..=2 {
        sender
            .send(sequenced(sequence, json!({"n": sequence})))
            .unwrap();
    }
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&frame).unwrap(),
        "event: runtime\ndata: {\"v\":1,\"kind\":\"event\",\"cursor\":{\"generation\":\"g1\",\"sequence\":2},\"data\":{\"n\":2}}\nid: g1:2\n\n"
    );
    assert_eq!(auth.calls.load(Ordering::Relaxed), calls);
}
