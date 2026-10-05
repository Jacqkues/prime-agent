#![cfg(feature = "http")]

mod support;

use std::{collections::BTreeMap, sync::Arc};

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

use support::{service, user};

struct Auth(RwLock<BTreeMap<String, Principal>>);

impl Authenticator for Auth {
    async fn authenticate(&self, headers: &HeaderMap) -> Result<Principal> {
        let token = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .ok_or(Error::Unauthenticated)?;
        self.0
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
    let auth = Arc::new(Auth(RwLock::new(BTreeMap::from([
        ("Bearer alice-key".into(), user("team", "alice")),
        ("Bearer bob-key".into(), user("team", "bob")),
        ("Bearer outsider-key".into(), user("other", "alice")),
    ]))));
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
    let auth = Arc::new(Auth(RwLock::new(BTreeMap::from([(
        "Bearer key".into(),
        alice,
    )]))));
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
    auth.0.write().await.clear();
    agent.events.lock().await[&session.id]
        .send(json!({"private": "new data"}))
        .unwrap();
    let frame = stream.next().await.unwrap().unwrap();
    assert_eq!(
        std::str::from_utf8(&frame).unwrap(),
        "event: error\ndata: {\"error\":\"authentication required\"}\n\n"
    );
    assert!(stream.next().await.is_none());
}
