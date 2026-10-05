//! Optional Axum router. Mount it beneath any path in the host application.
//! Authentication, TLS, CORS, request concurrency limits and deployment remain
//! host-owned. Bearer credentials are never accepted in query strings.

use std::{convert::Infallible, future::Future, sync::Arc};

use axum::{
    extract::{DefaultBodyLimit, Path, Request, State},
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
use pa_types::gateway::{Principal, SessionRole};
use serde::Deserialize;
use serde_json::json;

use crate::{Error, Gateway, Result, Runtime, SessionStore, WorkspacePolicy};

/// Verify the host application's credentials, including expiration, audience,
/// issuer and revocation as appropriate. Never trust a caller-supplied user or
/// tenant header without verification. Called for every HTTP request and before
/// each SSE event; returning an error denies access and terminates a stream.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, headers: &HeaderMap) -> impl Future<Output = Result<Principal>> + Send;
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
    let create = gateway.clone();
    let list = gateway.clone();
    let read = gateway.clone();
    let close = gateway.clone();
    let share = gateway.clone();
    let revoke = gateway.clone();
    let prompt = gateway.clone();
    let cancel = gateway.clone();
    let stream_auth = Arc::clone(&auth);
    Router::new()
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
            .get(move |Extension(principal): Extension<Principal>| {
                let gateway = list.clone();
                async move { gateway.list(&principal).await.map(Json) }
            }),
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
            "/sessions/{id}/prompts",
            post(
                move |Extension(principal): Extension<Principal>,
                      Path(id): Path<String>,
                      Json(body): Json<PromptBody>| {
                    let gateway = prompt.clone();
                    async move {
                        gateway
                            .prompt(principal, id, body.text)
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
                        let events = gateway.subscribe(principal.clone(), id).await?;
                        let stream = futures::stream::unfold(
                            Some((events, auth, headers, principal)),
                            |state| async move {
                                let (mut events, auth, headers, principal) = state?;
                                let event = events.next().await?;
                                let verified = auth.authenticate(&headers).await;
                                let event = match verified {
                                    Ok(identity) if identity == principal => event,
                                    Ok(_) => Err(Error::Unauthenticated),
                                    Err(error) => Err(error),
                                };
                                let terminal = event.is_err();
                                let (name, data) = match event {
                                    Ok(value) => ("runtime", value),
                                    Err(error) => ("error", json!({"error": error.to_string()})),
                                };
                                let frame = Event::default().event(name).data(data.to_string());
                                Some((
                                    Ok::<_, Infallible>(frame),
                                    (!terminal).then_some((events, auth, headers, principal)),
                                ))
                            },
                        );
                        Ok::<_, Error>(Sse::new(stream).keep_alive(KeepAlive::default()))
                    }
                },
            ),
        )
        .layer(DefaultBodyLimit::max(128 * 1024))
        .route_layer(middleware::from_fn_with_state(auth, authenticate::<A>))
}

async fn authenticate<A: Authenticator>(
    State(auth): State<Arc<A>>,
    mut request: Request,
    next: Next,
) -> Result<Response> {
    let principal = auth.authenticate(request.headers()).await?;
    request.extensions_mut().insert(principal);
    Ok(next.run(request).await)
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Unauthenticated => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Conflict | Self::NotReady => StatusCode::CONFLICT,
            Self::LimitExceeded => StatusCode::TOO_MANY_REQUESTS,
            Self::InvalidRequest => StatusCode::BAD_REQUEST,
            Self::Storage(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Runtime(_) => StatusCode::BAD_GATEWAY,
        };
        (status, Json(json!({"error": self.to_string()}))).into_response()
    }
}
