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
    #[serde(default)]
    traces: Vec<TraceConfiguration>,
}

// Parsed only to reject capture configuration when the debug feature is absent.
#[cfg_attr(not(feature = "debug"), allow(dead_code))]
#[derive(Deserialize)]
struct TraceConfiguration {
    workspace: Workspace,
    directory: std::path::PathBuf,
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

#[cfg(feature = "debug")]
struct LocalOperator {
    address: std::net::SocketAddr,
}

#[cfg(feature = "debug")]
impl Authenticator for LocalOperator {
    fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> impl std::future::Future<Output = Result<Principal>> + Send {
        std::future::ready((|| {
            let host = headers
                .get("host")
                .and_then(|value| value.to_str().ok())
                .ok_or(Error::Unauthenticated)?;
            let port = self.address.port();
            let valid_host =
                host == self.address.to_string() || host == format!("localhost:{port}");
            let origin_matches = headers
                .get("origin")
                .is_none_or(|value| value == format!("http://{host}").as_str());
            let same_origin = headers
                .get("sec-fetch-site")
                .is_none_or(|value| value == "same-origin" || value == "none");
            if !valid_host
                || !origin_matches
                || !same_origin
                || headers
                    .get("x-prime-agent-debug")
                    .is_none_or(|value| value != "1")
            {
                return Err(Error::Unauthenticated);
            }
            Ok(Principal {
                tenant_id: "local-operator".into(),
                user_id: "developer".into(),
            })
        })())
    }
}

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
// The example is a composition root; keeping setup in one place makes opt-ins explicit.
#[allow(clippy::too_many_lines)]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().ok_or_else(|| {
        anyhow::anyhow!(
            "usage: embedded <host-config.json> [listen-address] [--debug 127.0.0.1:3031]"
        )
    })?;
    let address = args.next().unwrap_or_else(|| "127.0.0.1:3000".to_owned());
    let debug_address = match args.next().as_deref() {
        None => None,
        Some("--debug") => Some(
            args.next()
                .ok_or_else(|| anyhow::anyhow!("--debug requires a loopback address"))?,
        ),
        Some(_) => anyhow::bail!("expected --debug <loopback-address>"),
    };
    anyhow::ensure!(args.next().is_none(), "unexpected argument");
    #[cfg(not(feature = "debug"))]
    anyhow::ensure!(
        debug_address.is_none(),
        "build with --features debug to enable the inspector"
    );
    let config: Configuration = serde_json::from_slice(&std::fs::read(path)?)?;
    #[cfg(not(feature = "debug"))]
    anyhow::ensure!(
        config.traces.is_empty(),
        "trace collection requires --features debug"
    );
    #[cfg(feature = "debug")]
    let trace_sources = config
        .traces
        .into_iter()
        .map(|source| pa_gateway::debug::TraceSource {
            workspace: source.workspace,
            directory: source.directory,
        })
        .collect::<Vec<_>>();
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
    #[cfg(feature = "debug")]
    let inspector = debug_address
        .as_ref()
        .map(|_| pa_gateway::debug::Inspector::default());
    #[cfg(feature = "debug")]
    let observation = inspector
        .as_ref()
        .map(|inspector| inspector.watch_daemon(&runtime))
        .transpose()?;
    #[cfg(feature = "debug")]
    let trace_observation = if trace_sources.is_empty() {
        None
    } else {
        Some(
            inspector
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("traces require --debug"))?
                .watch_traces(trace_sources)?,
        )
    };
    let gateway = Gateway::new(
        Arc::new(MemoryStore::default()),
        Arc::clone(&identity),
        runtime,
    );
    #[cfg(feature = "debug")]
    let gateway = if let Some(inspector) = &inspector {
        gateway.with_inspector(inspector.clone())
    } else {
        gateway
    };
    #[cfg(feature = "debug")]
    let debug_server = if let Some(debug_address) = debug_address {
        let address: std::net::SocketAddr = debug_address.parse()?;
        anyhow::ensure!(
            address.ip().is_loopback(),
            "the example inspector only accepts a loopback listener"
        );
        let listener = tokio::net::TcpListener::bind(address).await?;
        let address = listener.local_addr()?;
        let app = pa_gateway::debug::router(
            inspector.expect("enabled inspector"),
            Arc::new(LocalOperator { address }),
        );
        eprintln!("Prime Agent Gateway Inspector: http://{address}/ (local operator access)");
        Some(tokio::spawn(async move {
            if let Err(error) = axum::serve(listener, app).await {
                eprintln!("inspector server failed: {error}");
            }
        }))
    } else {
        None
    };
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
    #[cfg(feature = "debug")]
    {
        if let Some(server) = debug_server {
            server.abort();
        }
        drop(observation);
        drop(trace_observation);
    }
    Ok(())
}
