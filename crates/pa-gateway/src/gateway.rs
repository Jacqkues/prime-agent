use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::StreamExt;
use pa_telemetry::{AgentFeatureOutcome, TelemetryClient};
use pa_types::gateway::{
    AttributedPrompt, EventCursor, GatewayAction, GatewayMetrics, Principal, PromptReceipt,
    RuntimeEvent, RuntimeEventKind, Session, SessionRole, SessionStatus, StoredSession,
    SubscribeFrom, Workspace,
};
use tokio::{sync::watch, time::Instant};

use crate::{Error, EventStream, Result, Runtime, SessionStore, WorkspacePolicy};

/// Upper bound between access checks on an open subscription. Membership
/// changes made through this process are applied before the next event.
const ACCESS_RECHECK: Duration = Duration::from_secs(5);

/// Shared-session service. Clone it to share the same host adapters.
///
/// Mutations continue if their caller disconnects. Process crashes are a
/// different boundary: hosts must reconcile provisioning records and uncertain
/// runtime outcomes rather than replaying side effects automatically.
pub struct Gateway<S, P, R> {
    store: Arc<S>,
    policy: Arc<P>,
    runtime: Arc<R>,
    shared: Arc<Shared>,
    telemetry: Option<TelemetryClient>,
}

/// Process-local state shared by every clone of one gateway.
struct Shared {
    /// Bumped after every membership or status change so open subscriptions
    /// re-authorize before delivering their next event.
    access_changes: watch::Sender<u64>,
    sessions_created: AtomicU64,
    sessions_failed: AtomicU64,
    prompts_admitted: AtomicU64,
    prompts_failed: AtomicU64,
    runtime_errors: AtomicU64,
    active_subscriptions: AtomicU64,
    access_rechecks: AtomicU64,
    streams_revoked: AtomicU64,
}

impl Default for Shared {
    fn default() -> Self {
        Self {
            access_changes: watch::Sender::new(0),
            sessions_created: AtomicU64::default(),
            sessions_failed: AtomicU64::default(),
            prompts_admitted: AtomicU64::default(),
            prompts_failed: AtomicU64::default(),
            runtime_errors: AtomicU64::default(),
            active_subscriptions: AtomicU64::default(),
            access_rechecks: AtomicU64::default(),
            streams_revoked: AtomicU64::default(),
        }
    }
}

impl Shared {
    fn count<T>(&self, result: Result<T>) -> Result<T> {
        if matches!(result, Err(Error::Runtime(_))) {
            self.runtime_errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

impl<S, P, R> Clone for Gateway<S, P, R> {
    fn clone(&self) -> Self {
        Self {
            store: Arc::clone(&self.store),
            policy: Arc::clone(&self.policy),
            runtime: Arc::clone(&self.runtime),
            shared: Arc::clone(&self.shared),
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
            shared: Arc::default(),
            telemetry: None,
        }
    }

    /// Opt in to adoption events using the embedding application's own sinks.
    #[must_use]
    pub fn with_telemetry(mut self, telemetry: TelemetryClient) -> Self {
        self.telemetry = Some(telemetry);
        self
    }

    /// Process-local counters for the host's metrics exporter. They reset on
    /// restart and are not shared between gateway instances.
    #[must_use]
    pub fn metrics(&self) -> GatewayMetrics {
        let shared = &self.shared;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        GatewayMetrics {
            sessions_created: load(&shared.sessions_created),
            sessions_failed: load(&shared.sessions_failed),
            prompts_admitted: load(&shared.prompts_admitted),
            prompts_failed: load(&shared.prompts_failed),
            runtime_errors: load(&shared.runtime_errors),
            active_subscriptions: load(&shared.active_subscriptions),
            access_rechecks: load(&shared.access_rechecks),
            streams_revoked: load(&shared.streams_revoked),
        }
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
            match gateway.shared.count(provisioned) {
                Ok(runtime_id) => {
                    record.runtime_id = Some(runtime_id);
                    record.session.status = SessionStatus::Ready;
                    if let Err(error) = gateway.store.replace(record.clone(), 0).await {
                        // Keep the provisioning record, and retire the unbound resource.
                        gateway.runtime.close(&record).await?;
                        gateway
                            .shared
                            .sessions_failed
                            .fetch_add(1, Ordering::Relaxed);
                        return Err(error);
                    }
                    gateway
                        .shared
                        .sessions_created
                        .fetch_add(1, Ordering::Relaxed);
                    gateway.track("gateway_session");
                    Ok(record.session)
                }
                Err(error) => {
                    gateway
                        .shared
                        .sessions_failed
                        .fetch_add(1, Ordering::Relaxed);
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
            let session = gateway.save(record).await?;
            gateway.track("gateway_share");
            Ok(session)
        })
        .await
    }

    /// Revoke session access. Open subscriptions in this process stop before
    /// their next event; other gateway instances within `ACCESS_RECHECK`.
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
            gateway.save(record).await
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
                return Err(Error::TooLarge);
            }
            let record = gateway
                .authorize(&principal, &id, GatewayAction::Prompt)
                .await?;
            let request_id = uuid::Uuid::new_v4().to_string();
            let admitted = gateway
                .runtime
                .prompt(
                    &record,
                    AttributedPrompt {
                        request_id: request_id.clone(),
                        author: principal,
                        text,
                    },
                )
                .await;
            if let Err(error) = gateway.shared.count(admitted) {
                gateway
                    .shared
                    .prompts_failed
                    .fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
            gateway
                .shared
                .prompts_admitted
                .fetch_add(1, Ordering::Relaxed);
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
            gateway.shared.count(gateway.runtime.cancel(&record).await)
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
            record.session.status = SessionStatus::Closed;
            gateway.save(record.clone()).await?;
            gateway.shared.count(gateway.runtime.close(&record).await)
        })
        .await
    }

    /// Subscribe to a current runtime snapshot followed by live events.
    /// Reconnect by subscribing again; snapshots replace client state. Resuming
    /// after a cursor skips events the client already applied.
    ///
    /// # Errors
    /// Returns access or runtime failures. Revocation terminates the stream.
    #[tracing::instrument(skip_all)]
    pub async fn subscribe(
        &self,
        principal: Principal,
        id: String,
        from: SubscribeFrom,
    ) -> Result<EventStream> {
        let record = self.authorize(&principal, &id, GatewayAction::Read).await?;
        if record.session.status != SessionStatus::Ready {
            return Err(Error::NotReady);
        }
        let access_changes = self.shared.access_changes.subscribe();
        let events = self.shared.count(self.runtime.subscribe(&record).await)?;
        self.shared
            .active_subscriptions
            .fetch_add(1, Ordering::Relaxed);
        let subscription = Subscription {
            _active: ActiveSubscription(Arc::clone(&self.shared)),
            gateway: self.clone(),
            principal,
            id,
            events,
            access_changes,
            next_check: Instant::now() + ACCESS_RECHECK,
            resume: match from {
                SubscribeFrom::Start => None,
                SubscribeFrom::After(cursor) => Some(cursor),
            },
        };
        Ok(Box::pin(futures::stream::unfold(
            Some(subscription),
            |subscription| async move {
                let mut subscription = subscription?;
                let event = subscription.next().await?;
                let next = event.is_ok().then_some(subscription);
                Some((event, next))
            },
        )))
    }

    async fn save(&self, mut record: StoredSession) -> Result<Session> {
        let revision = record.session.revision;
        record.session.revision = revision.checked_add(1).ok_or(Error::Conflict)?;
        self.store.replace(record.clone(), revision).await?;
        self.shared
            .access_changes
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(record.session)
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

/// One open subscription: re-authorizes on local access changes and at least
/// every `ACCESS_RECHECK`, never per event.
struct Subscription<S, P, R> {
    _active: ActiveSubscription,
    gateway: Gateway<S, P, R>,
    principal: Principal,
    id: String,
    events: EventStream,
    access_changes: watch::Receiver<u64>,
    next_check: Instant,
    resume: Option<EventCursor>,
}

enum Wake {
    Recheck,
    Event(Option<Result<RuntimeEvent>>),
}

impl<S: SessionStore, P: WorkspacePolicy, R: Runtime> Subscription<S, P, R> {
    async fn next(&mut self) -> Option<Result<RuntimeEvent>> {
        loop {
            let wake = tokio::select! {
                biased;
                Ok(()) = self.access_changes.changed() => Wake::Recheck,
                () = tokio::time::sleep_until(self.next_check) => Wake::Recheck,
                event = self.events.next() => Wake::Event(event),
            };
            let event = match wake {
                Wake::Recheck => {
                    if let Err(error) = self.recheck().await {
                        return Some(Err(error));
                    }
                    continue;
                }
                Wake::Event(event) => event?,
            };
            if self.access_changes.has_changed().unwrap_or(false)
                || Instant::now() >= self.next_check
            {
                if let Err(error) = self.recheck().await {
                    return Some(Err(error));
                }
            }
            let event = match event {
                Ok(event) => event,
                Err(error) => return Some(Err(error)),
            };
            if let (Some(after), RuntimeEventKind::Event, Some(cursor)) =
                (&self.resume, event.kind, &event.cursor)
            {
                if cursor.generation == after.generation && cursor.sequence <= after.sequence {
                    continue;
                }
            }
            return Some(Ok(event));
        }
    }

    async fn recheck(&mut self) -> Result<()> {
        self.access_changes.borrow_and_update();
        let shared = &self.gateway.shared;
        shared.access_rechecks.fetch_add(1, Ordering::Relaxed);
        let record = self
            .gateway
            .authorize(&self.principal, &self.id, GatewayAction::Read)
            .await
            .and_then(|record| {
                (record.session.status == SessionStatus::Ready)
                    .then_some(record)
                    .ok_or(Error::NotReady)
            });
        if let Err(error) = record {
            shared.streams_revoked.fetch_add(1, Ordering::Relaxed);
            return Err(error);
        }
        self.next_check = Instant::now() + ACCESS_RECHECK;
        Ok(())
    }
}

/// Decrements the active-subscription gauge however a stream ends.
struct ActiveSubscription(Arc<Shared>);

impl Drop for ActiveSubscription {
    fn drop(&mut self) {
        self.0.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
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
