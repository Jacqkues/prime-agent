use std::{future::Future, pin::Pin};

use futures::Stream;
use pa_types::gateway::{
    AttributedPrompt, GatewayAction, PageRequest, Principal, PromptKey, PromptOutcome,
    PromptReservation, RuntimeEvent, StoredPage, StoredSession, Workspace,
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
///
/// Listing is keyset-paginated by ascending session ID: return at most
/// `page.limit` records with IDs greater than `page.after`, and set `next` to
/// the last returned ID when more may follow. Index membership and workspace so
/// a page never scans the whole tenant.
///
/// Prompt keys are idempotency records. `reserve_prompt` must atomically insert
/// the key with `request_id` when absent, or report the existing record. A key
/// settled as `NotDelivered` must be removed so it can be reserved again. The
/// host chooses a retention period for settled keys.
pub trait SessionStore: Send + Sync + 'static {
    fn insert(&self, record: StoredSession) -> impl Future<Output = Result<()>> + Send;
    fn get(&self, tenant: &str, id: &str) -> impl Future<Output = Result<StoredSession>> + Send;
    /// Sessions in `tenant` whose members include `user_id`.
    fn list_member(
        &self,
        tenant: &str,
        user_id: &str,
        page: &PageRequest,
    ) -> impl Future<Output = Result<StoredPage>> + Send;
    /// Every session in `workspace`, whatever its status or members.
    fn list_workspace(
        &self,
        workspace: &Workspace,
        page: &PageRequest,
    ) -> impl Future<Output = Result<StoredPage>> + Send;
    fn replace(
        &self,
        record: StoredSession,
        expected_revision: u64,
    ) -> impl Future<Output = Result<()>> + Send;
    fn reserve_prompt(
        &self,
        key: &PromptKey,
        request_id: &str,
    ) -> impl Future<Output = Result<PromptReservation>> + Send;
    fn settle_prompt(
        &self,
        key: &PromptKey,
        outcome: PromptOutcome,
    ) -> impl Future<Output = Result<()>> + Send;
}

/// Application-owned workspace authorization and admission policy.
///
/// Validate the principal's current workspace membership on every call. `Create`
/// and `Prompt` are also the host's admission/quota seams. Gateway session roles
/// are an additional restriction, never a replacement for this check. Inviting a
/// member checks `Read` for that member too. Grant `Administer` only to
/// workspace administrators. Return an error to deny access.
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
