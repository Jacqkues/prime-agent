//! Host routes enrich private prompts with current shared application context.

use std::sync::Arc;

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use pa_gateway::{http::Authenticator, DaemonRuntime, Error, Gateway, MemoryStore, Result};
use pa_types::gateway::{Principal, PromptReceipt, PromptSubmission, SessionRole, SessionStatus};
use serde::Deserialize;

use super::{catalog, kernel, Identities};

type Service = Gateway<MemoryStore, Identities, DaemonRuntime>;

#[derive(Clone)]
struct Application {
    gateway: Service,
    identity: Arc<Identities>,
    kernel: kernel::SharedKernel,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KernelBody {
    code: String,
    session_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionQuery {
    session_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PromptBody {
    text: String,
}

pub fn router(gateway: Service, identity: Arc<Identities>, kernel: kernel::SharedKernel) -> Router {
    Router::new()
        .route("/state", get(view))
        .route("/kernel", post(execute))
        .route("/kernel/catalog", get(catalog))
        .route("/sessions/{id}/prompts", post(prompt))
        .layer(DefaultBodyLimit::max(128 * 1024))
        .with_state(Application {
            gateway,
            identity,
            kernel,
        })
}

impl Application {
    async fn authorize_kernel(&self, headers: &HeaderMap, session_id: &str) -> Result<Principal> {
        let principal = self.identity.authenticate(headers).await?;
        let session = self.gateway.get(&principal, session_id).await?;
        if session.status != SessionStatus::Ready
            || !session
                .members
                .get(&principal.user_id)
                .is_some_and(|role| matches!(role, SessionRole::Owner | SessionRole::Contributor))
        {
            return Err(Error::Forbidden);
        }
        Ok(principal)
    }
}

async fn view(State(app): State<Application>, headers: HeaderMap) -> Result<Json<kernel::View>> {
    app.identity.authenticate(&headers).await?;
    Ok(Json(app.kernel.view().await))
}

async fn execute(
    State(app): State<Application>,
    headers: HeaderMap,
    Json(body): Json<KernelBody>,
) -> Result<Json<kernel::Execution>> {
    let principal = app.authorize_kernel(&headers, &body.session_id).await?;
    Ok(Json(
        app.kernel
            .execute(
                body.code,
                format!("{} / {}", principal.user_id, body.session_id),
            )
            .await?,
    ))
}

async fn catalog(
    State(app): State<Application>,
    headers: HeaderMap,
    Query(query): Query<SessionQuery>,
) -> Result<Json<catalog::Snapshot>> {
    app.authorize_kernel(&headers, &query.session_id).await?;
    Ok(Json(app.kernel.catalog().await))
}

async fn prompt(
    State(app): State<Application>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Json(body): Json<PromptBody>,
) -> Result<(StatusCode, Json<PromptReceipt>)> {
    let principal = app.authorize_kernel(&headers, &id).await?;
    if body.text.trim().is_empty() {
        return Err(Error::InvalidRequest);
    }
    // Reserve room under the gateway's 64 KiB prompt cap for host context.
    if body.text.len() > 48 * 1024 {
        return Err(Error::TooLarge);
    }
    let identity = app
        .identity
        .users
        .iter()
        .find(|user| user.principal == principal)
        .ok_or(Error::Unauthenticated)?;
    let catalog = app.kernel.catalog().await.prompt_context();
    let context = &identity.kernel_context;
    let text = format!("Application kernel connection file: {context}\nPrivate gateway session ID: {id}\nUse app_kernel.connect(path, session_id) for application work; keep this conversation private.\n{catalog}\n\nMessage utilisateur:\n{}", body.text);
    let idempotency_key = headers
        .get("idempotency-key")
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| Error::InvalidRequest)
        })
        .transpose()?;
    let receipt = app
        .gateway
        .prompt(
            principal,
            id,
            PromptSubmission {
                text,
                idempotency_key,
            },
        )
        .await?;
    Ok((StatusCode::ACCEPTED, Json(receipt)))
}
