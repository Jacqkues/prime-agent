//! Optional Axum router. Mount it beneath any path in the host application.
//! Authentication, TLS, CORS, request concurrency limits and deployment remain
//! host-owned. Bearer credentials are never accepted in query strings.

use std::{convert::Infallible, future::Future, sync::Arc, time::Duration};

use axum::{
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post, put},
    Extension, Json, Router,
};
use futures::StreamExt;
use pa_types::gateway::{
    EventCursor, GatewayEvent, PageRequest, Principal, PromptSubmission, SessionRole,
    SubscribeFrom, GATEWAY_EVENT_VERSION,
};
use serde::Deserialize;
use serde_json::json;
use tokio::time::Instant;

use crate::{Error, Gateway, Result, Runtime, SessionStore, WorkspacePolicy};

const DEFAULT_PAGE: usize = 50;

/// Verify the host application's credentials, including expiration, audience,
/// issuer and revocation as appropriate. Never trust a caller-supplied user or
/// tenant header without verification. Called for every HTTP request and again
/// on open SSE streams; returning an error denies access and terminates a stream.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, headers: &HeaderMap) -> impl Future<Output = Result<Principal>> + Send;

    /// Longest time an open SSE stream keeps delivering events before its
    /// credentials are verified again. Return `Duration::ZERO` to verify before
    /// every event, at the cost of one authentication per event per subscriber.
    fn stream_revalidation(&self) -> Duration {
        Duration::from_secs(5)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateBody {
    workspace_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptBody {
    text: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberBody {
    role: SessionRole,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OwnerBody {
    user_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageQuery {
    after: Option<String>,
    limit: Option<usize>,
}

impl From<PageQuery> for PageRequest {
    fn from(query: PageQuery) -> Self {
        Self {
            after: query.after,
            limit: query.limit.unwrap_or(DEFAULT_PAGE),
        }
    }
}

/// Build an authenticated router with session, membership, prompt and SSE routes.
/// No listener is bound and no global middleware or environment state is changed.
// Keep the route table together; session policy and execution live in Gateway.
#[allow(clippy::too_many_lines)]
pub fn router<S, P, R, A>(gateway: Gateway<S, P, R>, auth: Arc<A>) -> Router
where
    S: SessionStore,
    P: WorkspacePolicy,
    R: Runtime,
    A: Authenticator,
{
    #[cfg(feature = "debug")]
    let inspector = gateway.inspector.clone();
    let create = gateway.clone();
    let list = gateway.clone();
    let administer = gateway.clone();
    let read = gateway.clone();
    let close = gateway.clone();
    let share = gateway.clone();
    let revoke = gateway.clone();
    let transfer = gateway.clone();
    let prompt = gateway.clone();
    let cancel = gateway.clone();
    let stream_auth = Arc::clone(&auth);
    let router = Router::new()
        .route(
            "/sessions",
            post(
                move |Extension(principal): Extension<Principal>, Json(body): Json<CreateBody>| {
                    let gateway = create.clone();
                    async move {
                        gateway
                            .create(principal, body.workspace_id)
                            .await
                            .map(|session| (StatusCode::CREATED, Json(session)))
                    }
                },
            )
            .get(
                move |Extension(principal): Extension<Principal>, Query(page): Query<PageQuery>| {
                    let gateway = list.clone();
                    async move { gateway.list(&principal, page.into()).await.map(Json) }
                },
            ),
        )
        .route(
            "/workspaces/{workspace_id}/sessions",
            get(
                move |Extension(principal): Extension<Principal>,
                      Path(workspace_id): Path<String>,
                      Query(page): Query<PageQuery>| {
                    let gateway = administer.clone();
                    async move {
                        gateway
                            .list_workspace(&principal, workspace_id, page.into())
                            .await
                            .map(Json)
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}",
            get(
                move |Extension(principal): Extension<Principal>, Path(id): Path<String>| {
                    let gateway = read.clone();
                    async move { gateway.get(&principal, &id).await.map(Json) }
                },
            )
            .delete(
                move |Extension(principal): Extension<Principal>, Path(id): Path<String>| {
                    let gateway = close.clone();
                    async move {
                        gateway
                            .close(principal, id)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}/members/{user_id}",
            put(
                move |Extension(principal): Extension<Principal>,
                      Path((id, user_id)): Path<(String, String)>,
                      Json(body): Json<MemberBody>| {
                    let gateway = share.clone();
                    async move {
                        gateway
                            .set_member(principal, id, user_id, body.role)
                            .await
                            .map(Json)
                    }
                },
            )
            .delete(
                move |Extension(principal): Extension<Principal>,
                      Path((id, user_id)): Path<(String, String)>| {
                    let gateway = revoke.clone();
                    async move {
                        gateway
                            .remove_member(principal, id, user_id)
                            .await
                            .map(Json)
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}/owner",
            put(
                move |Extension(principal): Extension<Principal>,
                      Path(id): Path<String>,
                      Json(body): Json<OwnerBody>| {
                    let gateway = transfer.clone();
                    async move {
                        gateway
                            .transfer_ownership(principal, id, body.user_id)
                            .await
                            .map(Json)
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}/prompts",
            post(
                move |Extension(principal): Extension<Principal>,
                      headers: HeaderMap,
                      Path(id): Path<String>,
                      Json(body): Json<PromptBody>| {
                    let gateway = prompt.clone();
                    async move {
                        let idempotency_key = match headers.get("idempotency-key") {
                            Some(value) => Some(
                                value
                                    .to_str()
                                    .map_err(|_| Error::InvalidRequest)?
                                    .to_owned(),
                            ),
                            None => None,
                        };
                        gateway
                            .prompt(
                                principal,
                                id,
                                PromptSubmission {
                                    text: body.text,
                                    idempotency_key,
                                },
                            )
                            .await
                            .map(|receipt| (StatusCode::ACCEPTED, Json(receipt)))
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}/cancel",
            post(
                move |Extension(principal): Extension<Principal>, Path(id): Path<String>| {
                    let gateway = cancel.clone();
                    async move {
                        gateway
                            .cancel(principal, id)
                            .await
                            .map(|()| StatusCode::NO_CONTENT)
                    }
                },
            ),
        )
        .route(
            "/sessions/{id}/events",
            get(
                move |Extension(principal): Extension<Principal>,
                      headers: HeaderMap,
                      Path(id): Path<String>| {
                    let gateway = gateway.clone();
                    let auth = Arc::clone(&stream_auth);
                    async move {
                        let from = match headers.get("last-event-id") {
                            Some(value) => {
                                let (generation, sequence) = value
                                    .to_str()
                                    .ok()
                                    .and_then(|value| value.rsplit_once(':'))
                                    .ok_or(Error::InvalidRequest)?;
                                SubscribeFrom::After(EventCursor {
                                    generation: generation.to_owned(),
                                    sequence: sequence.parse().map_err(|_| Error::InvalidRequest)?,
                                })
                            }
                            None => SubscribeFrom::Start,
                        };
                        let events = gateway.subscribe(principal.clone(), id, from).await?;
                        let revalidation = auth.stream_revalidation();
                        let stream = futures::stream::unfold(
                            Some((events, auth, headers, principal, Instant::now() + revalidation)),
                            move |state| async move {
                                let (mut events, auth, headers, principal, mut verify_at) = state?;
                                let event = events.next().await?;
                                let event = if Instant::now() >= verify_at {
                                    verify_at = Instant::now() + revalidation;
                                    match auth.authenticate(&headers).await {
                                        Ok(identity) if identity == principal => event,
                                        Ok(_) => Err(Error::Unauthenticated),
                                        Err(error) => Err(error),
                                    }
                                } else {
                                    event
                                };
                                let frame = event.and_then(|event| {
                                    let id = event.cursor.as_ref().map(|cursor| {
                                        format!("{}:{}", cursor.generation, cursor.sequence)
                                    });
                                    let data = serde_json::to_string(&GatewayEvent {
                                        v: GATEWAY_EVENT_VERSION,
                                        event,
                                    })
                                    .map_err(|error| Error::Runtime(error.into()))?;
                                    let frame = Event::default().event("runtime").data(data);
                                    Ok(match id {
                                        Some(id) => frame.id(id),
                                        None => frame,
                                    })
                                });
                                Some(match frame {
                                    Ok(frame) => (
                                        Ok::<_, Infallible>(frame),
                                        Some((events, auth, headers, principal, verify_at)),
                                    ),
                                    Err(error) => (
                                        Ok(Event::default().event("error").data(
                                            json!({"error": error.to_string(), "code": error.code()})
                                                .to_string(),
                                        )),
                                        None,
                                    ),
                                })
                            },
                        );
                        Ok::<_, Error>(Sse::new(stream).keep_alive(KeepAlive::default()))
                    }
                },
            ),
        )
        .layer(DefaultBodyLimit::max(128 * 1024))
        .route_layer(middleware::from_fn_with_state(auth, authenticate::<A>));
    #[cfg(feature = "debug")]
    let router = if let Some(inspector) = inspector {
        router.layer(middleware::from_fn_with_state(
            inspector,
            crate::debug::http::trace,
        ))
    } else {
        router
    };
    router
}

async fn authenticate<A: Authenticator>(
    State(auth): State<Arc<A>>,
    mut request: Request,
    next: Next,
) -> Result<Response> {
    let principal = auth.authenticate(request.headers()).await?;
    #[cfg(feature = "debug")]
    if let Some(trace) = request.extensions().get::<crate::debug::RequestTrace>() {
        trace.identify(&principal);
    }
    #[cfg(feature = "debug")]
    if let Some(trace) = request
        .extensions()
        .get::<crate::debug::RequestTrace>()
        .cloned()
        .filter(crate::debug::RequestTrace::capturing)
    {
        let (parts, body) = request.into_parts();
        let Ok(bytes) = axum::body::to_bytes(body, 128 * 1024).await else {
            return Ok(StatusCode::PAYLOAD_TOO_LARGE.into_response());
        };
        trace.capture_request(parts.method.as_str(), parts.uri.path(), &bytes, &principal);
        request = Request::from_parts(parts, axum::body::Body::from(bytes));
    }
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Conflict | Self::NotReady | Self::IdempotencyUnresolved => StatusCode::CONFLICT,
            Self::LimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::InvalidRequest => StatusCode::BAD_REQUEST,
            Self::Storage(_) | Self::NotDelivered(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Runtime(_) => StatusCode::BAD_GATEWAY,
        };
        (
            status,
            Json(json!({"error": self.to_string(), "code": self.code()})),
        )
            .into_response()
    }
}
