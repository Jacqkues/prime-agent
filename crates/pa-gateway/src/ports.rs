use std::{future::Future, pin::Pin};

use futures::Stream;
use pa_types::gateway::{
    AttributedPrompt, GatewayAction, Principal, RuntimeEvent, StoredSession, Workspace,
};

use crate::Result;

/// Ordered runtime events. Dropping a subscription must release its resources
/// without cancelling the session. Errors terminate a subscription.
pub type EventStream = Pin<Box<dyn Stream<Item = Result<RuntimeEvent>> + Send>>;

/// Application-owned session metadata persistence.
///
/// All keys include the tenant. `insert` must reject existing keys; `replace`
/// must atomically compare the stored revision with `expected_revision` and
/// reject mismatches with `Error::Conflict`. Successful writes must be visible
/// to subsequent reads. Production adapters must persist before returning.
/// Implementations must not log session content or silently recover failed writes.
pub trait SessionStore: Send + Sync + 'static {
    fn insert(&self, record: StoredSession) -> impl Future<Output = Result<()>> + Send;
    fn get(&self, tenant: &str, id: &str) -> impl Future<Output = Result<StoredSession>> + Send;
    fn list(&self, tenant: &str) -> impl Future<Output = Result<Vec<StoredSession>>> + Send;
    fn replace(
        &self,
        record: StoredSession,
        expected_revision: u64,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Application-owned workspace authorization and admission policy.
///
/// Validate the principal's current workspace membership on every call. `Create`
/// and `Prompt` are also the host's admission/quota seams. Gateway session roles
/// are an additional restriction, never a replacement for this check. Inviting a
/// member checks `Read` for that member too. Return an error to deny access.
pub trait WorkspacePolicy: Send + Sync + 'static {
    fn check(
        &self,
        principal: &Principal,
        workspace: &Workspace,
        action: GatewayAction,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Execution adapter for shared sessions.
///
/// Implementations isolate workspaces, keep author attribution in durable input,
/// serialize admitted prompts, and keep work alive after HTTP disconnection.
/// A successful `prompt` means admitted, not completed. No operation is retried
/// automatically once it may have been delivered; return `Error::NotDelivered`
/// only when the operation certainly did not take effect. Subscriptions
/// begin with a `Snapshot` and then ordered `Event`s carrying cursors when the
/// runtime has them; slow consumers must fail explicitly instead of silently
/// losing data. Credentials and endpoint selection come exclusively from the host.
pub trait Runtime: Send + Sync + 'static {
    fn create(
        &self,
        workspace: &Workspace,
        session_id: &str,
    ) -> impl Future<Output = Result<String>> + Send;
    fn prompt(
        &self,
        record: &StoredSession,
        prompt: AttributedPrompt,
    ) -> impl Future<Output = Result<()>> + Send;
    fn cancel(&self, record: &StoredSession) -> impl Future<Output = Result<()>> + Send;
    fn close(&self, record: &StoredSession) -> impl Future<Output = Result<()>> + Send;
    fn subscribe(&self, record: &StoredSession)
        -> impl Future<Output = Result<EventStream>> + Send;
}
