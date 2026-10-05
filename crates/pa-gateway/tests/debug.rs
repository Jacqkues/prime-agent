#![cfg(feature = "debug")]

mod support;

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use futures::StreamExt;
use pa_gateway::{
    debug::{self, Inspector},
    http::{self, Authenticator},
    Error, Result,
};
use pa_types::gateway::Principal;
use serde_json::{json, Value};
use tower::ServiceExt;

struct UserAuth;
impl Authenticator for UserAuth {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl std::future::Future<Output = Result<Principal>> + Send {
        std::future::ready(
            if headers
                .get("authorization")
                .is_some_and(|value| value == "Bearer user-secret")
            {
                Ok(support::user("team", "alice"))
            } else {
                Err(Error::Unauthenticated)
            },
        )
    }
}

struct OperatorAuth;
impl Authenticator for OperatorAuth {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl std::future::Future<Output = Result<Principal>> + Send {
        std::future::ready(
            if headers
                .get("authorization")
                .is_some_and(|value| value == "Bearer operator-secret")
            {
                Ok(support::user("operations", "operator"))
            } else {
                Err(Error::Unauthenticated)
            },
        )
    }
}

fn request(method: &str, path: &str, token: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", token)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

// Keep the open stream and its lifetime assertions in one end-to-end test.
#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn inspector_tracks_stream_lifetime_identity_errors_and_excludes_secrets() {
    let (gateway, _, _, _) = support::service();
    let inspector = Inspector::default();
    let gateway = gateway.with_inspector(inspector.clone());
    let session = gateway
        .create(support::user("team", "alice"), "project".into())
        .await
        .unwrap();
    let app = axum::Router::new().nest("/host/agents", http::router(gateway, Arc::new(UserAuth)));
    let response = app
        .clone()
        .oneshot(request(
            "POST",
            &format!(
                "/host/agents/sessions/{}/prompts?token=query-secret",
                session.id
            ),
            "Bearer user-secret",
            &json!({"text":"private prompt content"}),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let request_id = response.headers()["x-prime-agent-request-id"]
        .to_str()
        .unwrap()
        .to_owned();
    to_bytes(response.into_body(), 1024).await.unwrap();
    let stream = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/host/agents/sessions/{}/events", session.id),
            "Bearer user-secret",
            &Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    let mut body = stream.into_body().into_data_stream();
    body.next().await.unwrap().unwrap();
    let snapshot = inspector.snapshot();
    assert_eq!(
        (
            snapshot.total_requests,
            snapshot.in_flight,
            snapshot.open_streams
        ),
        (2, 1, 1)
    );
    let completed = snapshot
        .requests
        .iter()
        .find(|entry| entry.id == request_id)
        .unwrap();
    assert_eq!(
        (
            &completed.principal,
            completed.route.as_str(),
            &completed.session_id,
            completed.status,
            completed.streaming
        ),
        (
            &Some(support::user("team", "alice")),
            "/host/agents/sessions/{id}/prompts",
            &Some(session.id),
            Some(202),
            false
        )
    );
    assert!(completed.ended_at_ms.is_some());
    drop(body);
    let denied = app
        .oneshot(request(
            "GET",
            "/host/agents/sessions?token=another-secret",
            "Bearer wrong-secret",
            &Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);
    drop(denied);
    let snapshot = inspector.snapshot();
    assert_eq!(
        (
            snapshot.total_requests,
            snapshot.in_flight,
            snapshot.open_streams
        ),
        (3, 0, 0)
    );
    assert_eq!(
        (
            snapshot.requests[0].status,
            snapshot.requests[0].principal.clone()
        ),
        (Some(401), None)
    );
    let data = serde_json::to_string(&snapshot).unwrap();
    for forbidden in [
        "secret",
        "private prompt content",
        "authorization",
        "token=",
    ] {
        assert!(!data.contains(forbidden), "captured {forbidden}");
    }
}

#[tokio::test]
async fn debug_snapshot_requires_separate_operator_access_and_never_caches() {
    let inspector = Inspector::default();
    let app = axum::Router::new().nest(
        "/inspect",
        debug::router(inspector.clone(), Arc::new(OperatorAuth)),
    );
    for token in ["", "Bearer user-secret"] {
        let response = app
            .clone()
            .oneshot(request("GET", "/inspect/snapshot", token, &Value::Null))
            .await
            .unwrap();
        assert_eq!(
            (
                response.status(),
                response.headers()["cache-control"].to_str().unwrap()
            ),
            (StatusCode::UNAUTHORIZED, "no-store")
        );
    }
    let response = app
        .oneshot(request(
            "GET",
            "/inspect/snapshot",
            "Bearer operator-secret",
            &Value::Null,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()["content-security-policy"]
        .to_str()
        .unwrap()
        .contains("frame-ancestors 'none'"));
    let data: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
            .unwrap();
    assert_eq!(data["total_requests"], 0);
    assert_eq!(data["requests"], json!([]));
    assert_eq!(inspector.snapshot().total_requests, 0);
}
