#![cfg(all(feature = "debug", unix))]

use std::{os::unix::fs::PermissionsExt, sync::Arc};

use axum::{
    body::{to_bytes, Body},
    http::{HeaderMap, Request, StatusCode},
};
use pa_gateway::{
    debug::{self, Inspector, TraceSource},
    http::Authenticator,
    Error, Result,
};
use pa_types::gateway::{Principal, Workspace};
use tower::ServiceExt;

struct Operator;
impl Authenticator for Operator {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl std::future::Future<Output = Result<Principal>> + Send {
        std::future::ready(
            if headers
                .get("authorization")
                .is_some_and(|value| value == "Bearer operator")
            {
                Ok(Principal {
                    tenant_id: "operations".into(),
                    user_id: "operator".into(),
                })
            } else {
                Err(Error::Unauthenticated)
            },
        )
    }
}

#[tokio::test]
async fn trace_routes_reauthenticate_even_for_missing_payloads() {
    let inspector = Inspector::default();
    let directory = tempfile::tempdir().unwrap();
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let _capture = inspector
        .watch_traces(vec![TraceSource {
            workspace: Workspace {
                tenant_id: "team".into(),
                workspace_id: "app".into(),
            },
            directory: directory.path().to_path_buf(),
        }])
        .unwrap();
    let app = debug::router(inspector, Arc::new(Operator));
    for (path, token, expected) in [
        ("/trace/events", "", StatusCode::UNAUTHORIZED),
        ("/trace/events", "Bearer user", StatusCode::UNAUTHORIZED),
        (
            "/trace/events/missing",
            "Bearer user",
            StatusCode::UNAUTHORIZED,
        ),
        (
            "/trace/events/missing",
            "Bearer operator",
            StatusCode::NOT_FOUND,
        ),
        ("/trace/events", "Bearer operator", StatusCode::OK),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("authorization", token)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            (
                response.status(),
                response.headers()["cache-control"].to_str().unwrap()
            ),
            (expected, "no-store")
        );
        assert!(response
            .headers()
            .get("access-control-allow-origin")
            .is_none());
        if expected == StatusCode::OK {
            let body: serde_json::Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                    .unwrap();
            assert_eq!(
                (body["enabled"].clone(), body["entries"].clone()),
                (true.into(), serde_json::json!([]))
            );
        }
    }
}
