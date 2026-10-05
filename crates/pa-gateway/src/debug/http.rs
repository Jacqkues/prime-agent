use std::sync::Arc;

use axum::{
    body::Body,
    extract::{MatchedPath, Path, Request, State},
    http::{header, HeaderMap},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
    Json, Router,
};
use futures::StreamExt;

use super::{capped, Inspector};
use crate::http::Authenticator;

/// Mount a read-only dashboard with an **operator-only** authenticator. It can
/// see all configured tenants, so ordinary session/user authentication is not
/// sufficient. The HTML/assets contain no data; `/snapshot` reauthenticates on
/// every poll. The browser keeps an optional bearer token only in memory.
/// Requests to this router are deliberately excluded from its own journal.
pub fn router<A: Authenticator>(inspector: Inspector, auth: Arc<A>) -> Router {
    let index_inspector = inspector.clone();
    let payload_inspector = inspector.clone();
    let index_auth = Arc::clone(&auth);
    let payload_auth = Arc::clone(&auth);
    Router::new()
        .route("/", get(|| async { Html(include_str!("index.html")) }))
        .route(
            "/inspector.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("inspector.js"),
                )
            }),
        )
        .route(
            "/inspector.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("inspector.css"),
                )
            }),
        )
        .route(
            "/trace.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
                    include_str!("trace.js"),
                )
            }),
        )
        .route(
            "/trace.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
                    include_str!("trace.css"),
                )
            }),
        )
        .route(
            "/trace/events",
            get(move |headers: HeaderMap| {
                let inspector = index_inspector.clone();
                let auth = Arc::clone(&index_auth);
                async move {
                    auth.authenticate(&headers).await?;
                    Ok::<_, crate::Error>(Json(inspector.trace_index()))
                }
            }),
        )
        .route(
            "/trace/events/{id}",
            get(move |headers: HeaderMap, Path(id): Path<String>| {
                let inspector = payload_inspector.clone();
                let auth = Arc::clone(&payload_auth);
                async move {
                    auth.authenticate(&headers).await?;
                    inspector.trace(&id).map(Json).ok_or(crate::Error::NotFound)
                }
            }),
        )
        .route(
            "/snapshot",
            get(move |headers: HeaderMap| {
                let auth = Arc::clone(&auth);
                let inspector = inspector.clone();
                async move {
                    auth.authenticate(&headers).await?;
                    Ok::<_, crate::Error>(Json(inspector.snapshot()))
                }
            }),
        )
        .layer(middleware::from_fn(headers))
}

async fn headers(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        header::CACHE_CONTROL,
        "no-store".parse().expect("static header"),
    );
    headers.insert(header::CONTENT_SECURITY_POLICY, "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'".parse().expect("static header"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        "nosniff".parse().expect("static header"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        "no-referrer".parse().expect("static header"),
    );
    response
}

pub(crate) async fn trace(
    State(inspector): State<Inspector>,
    mut request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or("<unmatched>", MatchedPath::as_str)
        .to_owned();
    let session_id = request
        .uri()
        .path()
        .split("/sessions/")
        .nth(1)
        .and_then(|path| path.split('/').next())
        .and_then(|id| uuid::Uuid::parse_str(id).ok())
        .map(|id| id.to_string());
    let mut guard = inspector.begin(capped(request.method().as_str()), route, session_id);
    request.extensions_mut().insert(guard.trace.clone());
    let response = next.run(request).await;
    let streaming = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    guard.responded(response.status().as_u16(), streaming);
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        "x-prime-agent-request-id",
        guard.trace.id.parse().expect("UUID header"),
    );
    let stream = futures::stream::unfold(
        (body.into_data_stream(), guard),
        |(mut body, mut guard)| async move {
            body.next().await.map(|frame| {
                if let Ok(bytes) = &frame {
                    guard.chunk(bytes);
                }
                (frame, (body, guard))
            })
        },
    );
    Response::from_parts(parts, Body::from_stream(stream)).into_response()
}
