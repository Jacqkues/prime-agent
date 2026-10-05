//! Local integration example using explicit host configuration and an ephemeral
//! store. Replace `Identity` and `MemoryStore` with the application's adapters.

use std::{collections::BTreeMap, sync::Arc};

use axum::http::HeaderMap;
use pa_gateway::{
    http::{self, Authenticator},
    DaemonEndpoint, DaemonRuntime, Error, Gateway, MemoryStore, Result, WorkspacePolicy,
};
use pa_types::gateway::{GatewayAction, Principal, Workspace};
use serde::Deserialize;

#[derive(Deserialize)]
struct Configuration {
    workspaces: Vec<WorkspaceConfiguration>,
    users: Vec<User>,
}

#[derive(Deserialize)]
struct WorkspaceConfiguration {
    workspace: Workspace,
    socket_path: std::path::PathBuf,
    create_config: serde_json::Value,
    #[serde(default = "default_max_subscriptions")]
    max_subscriptions: usize,
}

fn default_max_subscriptions() -> usize {
    64
}

#[derive(Deserialize)]
struct User {
    token: String,
    principal: Principal,
    workspaces: Vec<String>,
    /// Workspaces where this user may administer every session.
    #[serde(default)]
    administers: Vec<String>,
}

struct Identity(Vec<User>);

impl Authenticator for Identity {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl std::future::Future<Output = Result<Principal>> + Send {
        let token = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .filter(|value| !value.is_empty())
            .ok_or(Error::Unauthenticated);
        std::future::ready(token.and_then(|token| {
            self.0
                .iter()
                .find(|user| user.token == token)
                .map(|user| user.principal.clone())
                .ok_or(Error::Unauthenticated)
        }))
    }
}

impl WorkspacePolicy for Identity {
    fn check(
        &self,
        principal: &Principal,
        workspace: &Workspace,
        action: GatewayAction,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(
            if principal.tenant_id == workspace.tenant_id
                && self.0.iter().any(|user| {
                    let granted = match action {
                        GatewayAction::Administer => &user.administers,
                        GatewayAction::Create
                        | GatewayAction::Read
                        | GatewayAction::Prompt
                        | GatewayAction::Share
                        | GatewayAction::Cancel
                        | GatewayAction::Close => &user.workspaces,
                    };
                    user.principal == *principal && granted.contains(&workspace.workspace_id)
                })
            {
                Ok(())
            } else {
                Err(Error::Forbidden)
            },
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args
        .next()
        .ok_or_else(|| anyhow::anyhow!("usage: embedded <host-config.json> [listen-address]"))?;
    let address = args.next().unwrap_or_else(|| "127.0.0.1:3000".to_owned());
    anyhow::ensure!(args.next().is_none(), "unexpected argument");
    let config: Configuration = serde_json::from_slice(&std::fs::read(path)?)?;
    let endpoints: BTreeMap<_, _> = config
        .workspaces
        .into_iter()
        .map(|config| {
            (
                config.workspace,
                DaemonEndpoint {
                    socket_path: config.socket_path,
                    create_config: config.create_config,
                    max_subscriptions: config.max_subscriptions,
                },
            )
        })
        .collect();
    let identity = Arc::new(Identity(config.users));
    let runtime = Arc::new(DaemonRuntime::new(endpoints)?);
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::clone(&identity),
        runtime,
    );
    let app = axum::Router::new().nest("/agents", http::router(gateway, identity));
    let listener = tokio::net::TcpListener::bind(address).await?;
    let address = listener.local_addr()?;
    eprintln!("Prime Agent gateway example: http://{address}/agents (ephemeral metadata)");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("shutdown signal failed: {error}");
            }
        })
        .await?;
    Ok(())
}
