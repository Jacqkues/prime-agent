use std::{collections::BTreeMap, sync::Arc, time::Duration};

use futures::StreamExt;
use pa_telemetry::{AgentFeatureOutcome, TelemetryClient};
use pa_types::gateway::{
    AttributedPrompt, GatewayAction, Principal, PromptReceipt, Session, SessionRole, SessionStatus,
    StoredSession, Workspace,
};

use crate::{Error, EventStream, Result, Runtime, SessionStore, WorkspacePolicy};

/// Shared-session service. Clone it to share the same host adapters.
///
/// Mutations continue if their caller disconnects. Process crashes are a
/// different boundary: hosts must reconcile provisioning records and uncertain
/// runtime outcomes rather than replaying side effects automatically.
pub struct Gateway<S, P, R> {
    store: Arc<S>,
    policy: Arc<P>,
    runtime: Arc<R>,
    telemetry: Option<TelemetryClient>,
}

impl<S, P, R> Clone for Gateway<S, P, R> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            policy: Arc::clone(&self.policy),
            runtime: Arc::clone(&self.runtime),
            telemetry: self.telemetry.clone(),
        }
    }
}

impl<S: SessionStore, P: WorkspacePolicy, R: Runtime> Gateway<S, P, R> {
    #[must_use]
    pub fn new(store: Arc<S>, policy: Arc<P>, runtime: Arc<R>) -> Self {
        Self {
            store,
            policy,
            runtime,
            telemetry: None,
        }
    }

    /// Opt in to adoption events using the embedding application's own sinks.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TelemetryClient) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Create a private session; sharing is an explicit subsequent operation.
    ///
    /// # Errors
    /// Returns policy, persistence or runtime errors. A failed or interrupted
    /// provision remains visible to its owner for host-side reconciliation.
    #[tracing::instrument(skip_all)]
    pub async fn create(&self, principal: Principal, workspace_id: String) -> Result<Session> {
        let gateway = self.clone();
        complete(async move {
            validate_identity(&principal)?;
            if workspace_id.is_empty() || workspace_id.len() > 256 {
                return Err(Error::InvalidRequest);
            }
            let workspace = Workspace {
                tenant_id: principal.tenant_id.clone(),
                workspace_id,
            };
            gateway
                .policy
                .check(&principal, &workspace, GatewayAction::Create)
                .await?;
            let mut record = StoredSession {
                session: Session {
                    id: uuid::Uuid::new_v4().to_string(),
                    workspace,
                    members: BTreeMap::from([(principal.user_id, SessionRole::Owner)]),
                    status: SessionStatus::Provisioning,
                    revision: 0,
                },
                runtime_id: None,
            };
            gateway.store.insert(record.clone()).await?;
            let provisioned = gateway
                .runtime
                .create(&record.session.workspace, &record.session.id)
                .await;
            record.session.revision = 1;
            match provisioned {
                Ok(runtime_id) => {
                    record.runtime_id = Some(runtime_id);
                    record.session.status = SessionStatus::Ready;
                    if let Err(error) = gateway.store.replace(record.clone(), 0).await {
                        // Keep the provisioning record, and retire the unbound resource.
                        gateway.runtime.close(&record).await?;
                        return Err(error);
                    }
                    gateway.track("gateway_session");
                    Ok(record.session)
                }
                Err(error) => {
                    record.session.status = SessionStatus::Failed;
                    gateway.store.replace(record, 0).await?;
                    Err(error)
                }
            }
        })
        .await
    }

    /// Read metadata after checking both workspace and session membership.
    ///
    /// # Errors
    /// Unknown and inaccessible sessions both return `Error::NotFound`.
    #[tracing::instrument(skip_all)]
    pub async fn get(&self, principal: &Principal, id: &str) -> Result<Session> {
        Ok(self
            .authorize(principal, id, GatewayAction::Read)
            .await?
            .session)
    }

    /// List only this principal's currently accessible sessions.
    ///
    /// # Errors
    /// Returns identity, storage and policy-service failures.
    #[tracing::instrument(skip_all)]
    pub async fn list(&self, principal: &Principal) -> Result<Vec<Session>> {
        validate_identity(principal)?;
        let records = self.store.list(&principal.tenant_id).await?;
        let mut sessions = Vec::new();
        for record in records {
            if record.session.workspace.tenant_id != principal.tenant_id
                || !record.session.members.contains_key(&principal.user_id)
            {
                continue;
            }
            match self
                .policy
                .check(principal, &record.session.workspace, GatewayAction::Read)
                .await
            {
                Ok(()) => sessions.push(record.session),
                Err(Error::Forbidden | Error::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(sessions)
    }

    /// Invite an existing workspace member or update their session role.
    /// The original owner cannot be removed or demoted.
    ///
    /// # Errors
    /// Only an owner can share. Concurrent changes return `Error::Conflict`.
    #[tracing::instrument(skip_all)]
    pub async fn set_member(
        &self,
        principal: Principal,
        id: String,
        user_id: String,
        role: SessionRole,
    ) -> Result<Session> {
        let gateway = self.clone();
        complete(async move {
            let mut record = gateway
                .authorize(&principal, &id, GatewayAction::Share)
                .await?;
            let member = Principal {
                tenant_id: principal.tenant_id.clone(),
                user_id: user_id.clone(),
            };
            validate_identity(&member)?;
            if role == SessionRole::Owner
                || record.session.members.get(&user_id) == Some(&SessionRole::Owner)
            {
                return Err(Error::Forbidden);
            }
            gateway
                .policy
                .check(&member, &record.session.workspace, GatewayAction::Read)
                .await?;
            record.session.members.insert(user_id, role);
            let revision = record.session.revision;
            record.session.revision = revision.checked_add(1).ok_or(Error::Conflict)?;
            gateway.store.replace(record.clone(), revision).await?;
            gateway.track("gateway_share");
            Ok(record.session)
        })
        .await
    }

    /// Revoke session access. Existing subscriptions check membership before
    /// each event and at least every 15 seconds while idle.
    ///
    /// # Errors
    /// Only the owner may revoke a member, and the owner cannot be removed.
    #[tracing::instrument(skip_all)]
    pub async fn remove_member(
        &self,
        principal: Principal,
        id: String,
        user_id: String,
    ) -> Result<Session> {
        let gateway = self.clone();
        complete(async move {
            let mut record = gateway
                .authorize(&principal, &id, GatewayAction::Share)
                .await?;
            if record.session.members.get(&user_id) == Some(&SessionRole::Owner) {
                return Err(Error::Forbidden);
            }
            record.session.members.remove(&user_id);
            let revision = record.session.revision;
            record.session.revision = revision.checked_add(1).ok_or(Error::Conflict)?;
            gateway.store.replace(record.clone(), revision).await?;
            Ok(record.session)
        })
        .await
    }

    /// Admit a text prompt to the shared runtime queue. Attribution is derived
    /// from the principal; HTTP callers cannot supply another author.
    ///
    /// # Errors
    /// Read-only participants are denied. Text must contain 1–65536 UTF-8 bytes
    /// and not be only whitespace. Runtime errors may mean uncertain delivery.
    #[tracing::instrument(skip_all)]
    pub async fn prompt(
        &self,
        principal: Principal,
        id: String,
        text: String,
    ) -> Result<PromptReceipt> {
        let gateway = self.clone();
        complete(async move {
            if text.trim().is_empty() {
                return Err(Error::InvalidRequest);
            }
            if text.len() > 65_536 {
                return Err(Error::LimitExceeded);
            }
            let record = gateway
                .authorize(&principal, &id, GatewayAction::Prompt)
                .await?;
            let request_id = uuid::Uuid::new_v4().to_string();
            gateway
                .runtime
                .prompt(
                    &record,
                    AttributedPrompt {
                        request_id: request_id.clone(),
                        author: principal,
                        text,
                    },
                )
                .await?;
            gateway.track("gateway_prompt");
            Ok(PromptReceipt { request_id })
        })
        .await
    }

    /// Stop the current run. Only the owner can interrupt shared work.
    ///
    /// # Errors
    /// Returns authorization or runtime failures.
    #[tracing::instrument(skip_all)]
    pub async fn cancel(&self, principal: Principal, id: String) -> Result<()> {
        let gateway = self.clone();
        complete(async move {
            let record = gateway
                .authorize(&principal, &id, GatewayAction::Cancel)
                .await?;
            gateway.runtime.cancel(&record).await
        })
        .await
    }

    /// Fence new gateway actions and stop the runtime session. A runtime error
    /// leaves the session closed to users; the host must reconcile shutdown.
    ///
    /// # Errors
    /// Only the owner may close. Returns storage or runtime failures.
    #[tracing::instrument(skip_all)]
    pub async fn close(&self, principal: Principal, id: String) -> Result<()> {
        let gateway = self.clone();
        complete(async move {
            let mut record = gateway
                .authorize(&principal, &id, GatewayAction::Close)
                .await?;
            let revision = record.session.revision;
            record.session.status = SessionStatus::Closed;
            record.session.revision = revision.checked_add(1).ok_or(Error::Conflict)?;
            gateway.store.replace(record.clone(), revision).await?;
            gateway.runtime.close(&record).await
        })
        .await
    }

    /// Subscribe to a current runtime snapshot followed by live events.
    /// Reconnect by subscribing again; snapshots replace client state.
    ///
    /// # Errors
    /// Returns access or runtime failures. Revocation terminates the stream.
    #[tracing::instrument(skip_all)]
    pub async fn subscribe(&self, principal: Principal, id: String) -> Result<EventStream> {
        let record = self.authorize(&principal, &id, GatewayAction::Read).await?;
        if record.session.status != SessionStatus::Ready {
            return Err(Error::NotReady);
        }
        let events = self.runtime.subscribe(&record).await?;
        let state = Some((
            self.clone(),
            principal,
            id,
            events,
            tokio::time::interval(Duration::from_secs(15)),
        ));
        Ok(Box::pin(futures::stream::unfold(
            state,
            |state| async move {
                let (gateway, principal, id, mut events, mut interval) = state?;
                let event = {
                    let next = events.next();
                    tokio::pin!(next);
                    loop {
                        tokio::select! {
                            event = &mut next => break event?,
                            _ = interval.tick() => {
                                match gateway.authorize(&principal, &id, GatewayAction::Read).await {
                                    Ok(record) if record.session.status == SessionStatus::Ready => {},
                                    Ok(_) => return Some((Err(Error::NotReady), None)),
                                    Err(error) => return Some((Err(error), None)),
                                }
                            }
                        }
                    }
                };
                let access = gateway
                    .authorize(&principal, &id, GatewayAction::Read)
                    .await;
                match access {
                    Ok(record) if record.session.status == SessionStatus::Ready => {}
                    Ok(_) => return Some((Err(Error::NotReady), None)),
                    Err(error) => return Some((Err(error), None)),
                }
                let next = event
                    .is_ok()
                    .then_some((gateway, principal, id, events, interval));
                Some((event, next))
            },
        )))
    }

    async fn authorize(
        &self,
        principal: &Principal,
        id: &str,
        action: GatewayAction,
    ) -> Result<StoredSession> {
        validate_identity(principal)?;
        let record = self.store.get(&principal.tenant_id, id).await?;
        if record.session.workspace.tenant_id != principal.tenant_id || record.session.id != id {
            return Err(Error::NotFound);
        }
        let role = record
            .session
            .members
            .get(&principal.user_id)
            .ok_or(Error::NotFound)?;
        let permitted = match action {
            GatewayAction::Read => true,
            GatewayAction::Prompt => matches!(role, SessionRole::Owner | SessionRole::Contributor),
            GatewayAction::Create
            | GatewayAction::Share
            | GatewayAction::Cancel
            | GatewayAction::Close => *role == SessionRole::Owner,
        };
        if !permitted {
            return Err(Error::Forbidden);
        }
        self.policy
            .check(principal, &record.session.workspace, action)
            .await?;
        if action != GatewayAction::Read && record.session.status != SessionStatus::Ready {
            return Err(Error::NotReady);
        }
        Ok(record)
    }

    fn track(&self, feature_name: &'static str) {
        if let Some(client) = &self.telemetry {
            AgentFeatureOutcome {
                feature_id: uuid::Uuid::new_v4().to_string(),
                feature_name,
                outcome: "completed",
                duration_ms: None,
                configuration_choice: None,
            }
            .track(client);
        }
    }
}

fn validate_identity(principal: &Principal) -> Result<()> {
    if [&principal.tenant_id, &principal.user_id]
        .iter()
        .any(|id| id.is_empty() || id.len() > 256)
    {
        return Err(Error::Unauthenticated);
    }
    Ok(())
}

async fn complete<T: Send + 'static>(
    work: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    tokio::spawn(work)
        .await
        .map_err(|error| Error::Runtime(error.into()))?
}
