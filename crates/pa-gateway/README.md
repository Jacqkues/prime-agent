# pa-gateway

Embed collaborative agent sessions in your own application. Use the Rust service
directly, or mount its optional HTTP router for JavaScript, Python and other
clients. Your application keeps its authentication, database and infrastructure.

## Scope

Tenant-scoped session access, many-to-many membership, owner/contributor/viewer
permissions, attributed prompt admission, cancellation, closure and authorized
event subscriptions. The native daemon adapter uses its existing JSONL protocol
and follow-up queue. ACP is a separate stdio interface.

## Non-goals

No agent loop, tools, model providers, daemon supervision, account system,
billing, database migrations or sandbox provisioner. Session roles control
gateway access; they do not sandbox code executed by an agent.

## Public API and dependencies

- `Gateway<S, P, R>`: create/list/read/share/revoke/prompt/cancel/close/subscribe
  and `metrics()` counters.
- `SessionStore`: host persistence with atomic revision checks.
- `WorkspacePolicy`: current workspace authorization and admission checks.
- `Runtime`: host execution boundary, with `DaemonRuntime` supplied (workspace
  routes can be registered and unregistered while running).
- `DaemonEndpoint`: trusted socket, create configuration and subscription limit per workspace.
- `MemoryStore`: ephemeral reference adapter for local development and tests.
- `EventStream`, `Error`, `Result`: streams and service outcomes.
- Optional `http::{Authenticator, router}`: credential validation and Axum routes.

Shared vocabulary lives in `pa-types::gateway`, without re-exports here. Internals
stay private. Workspace dependencies are only `pa-types` and `pa-telemetry`.
There is no linkage to `pa-core`, `pa-daemon` or `pa-cli`.

Axum is optional: HTTP routing belongs here, outside the engine and agent loop.
Tokio/futures implement transport, serde the existing wire, and UUIDs identify
sessions/requests. No identity-provider, database or Docker dependency is imposed.

## Integration example

The core has no default features. Enable `http` only when you need its router:

```sh
cargo run -p pa-gateway --features http --example embedded -- /absolute/host-config.json
```

The example listens on `127.0.0.1:3000`, mounts `/agents`, and uses ephemeral
metadata. Its local token lookup illustrates the authentication seam; replace it
with your application's verified identity before deployment. Example host config:

```json
{
  "workspaces": [{
    "workspace": {"tenant_id": "acme", "workspace_id": "engineering"},
    "socket_path": "/absolute/isolated-runtime/daemon.sock",
    "create_config": {"cwd": "/workspace"},
    "max_subscriptions": 64
  }],
  "users": [
    {"token": "replace-with-alice-secret", "principal": {"tenant_id": "acme", "user_id": "alice"}, "workspaces": ["engineering"]},
    {"token": "replace-with-bob-secret", "principal": {"tenant_id": "acme", "user_id": "bob"}, "workspaces": ["engineering"]}
  ]
}
```

An optional second argument changes the example's listening address; use
`127.0.0.1:0` for an OS-assigned local port. Startup prints the bound address.

The host provisions and runs the daemon. Never expose your personal daemon or
agent directory to untrusted users. Paths, configuration and credentials are host
inputs, never HTTP inputs. To embed, construct
`Gateway::new(Arc<Store>, Arc<Policy>, Arc<Runtime>)` and mount
`http::router(gateway, Arc<Authenticator>)` under your existing router. Rust-only
applications call the service methods with a verified `Principal`.

## Executable application extensions

Hosts can supply their own code through Prime Agent's existing Python skill
mechanism. A skill directory contains `SKILL.md`, `pyproject.toml` and
`src/<skill_import_name>/__init__.py`; register its absolute path in the trusted
daemon endpoint's `create_config.skills` array. The kernel installs and pre-imports
the package. Agents can compose its functions with arbitrary Python calculations
and keep intermediate objects in the persistent kernel. The gateway's access and
session APIs do not depend on the application's tools, programming language or UI.

When application state or a specialized renderer lives in the host, the package
can call an authenticated host endpoint. The host scopes that capability to the
current request/document, validates results, and emits application events into
its UI stream. Keep credentials outside prompts, revoke capabilities when a
request ends, fence late publication after cancellation, and make mutations
idempotent. Kernel stdout alone is not a trusted artifact publication protocol.
Application-specific schemas, renderers and event adapters stay in the host.

Provision skill dependencies and capability mounts within the workspace's
execution boundary. A shared directory or process is not isolation between
untrusted users. This extension path adds no gateway route or native model tool;
it uses the existing `ipython`/Python-skill contract.

## HTTP contract

Paths are relative to the host mount point. JSON uses snake_case. All routes
require authentication; request bodies reject unknown fields.

| Method and path | Behavior |
| --- | --- |
| `POST /sessions` | `{"workspace_id":"engineering"}` → 201 session |
| `GET /sessions` | Only the caller's currently accessible sessions |
| `GET /sessions/{id}` | Metadata without the internal runtime binding |
| `PUT /sessions/{id}/members/{user_id}` | `{"role":"contributor"}` or `viewer`; owner only |
| `DELETE /sessions/{id}/members/{user_id}` | Revoke member; owner only |
| `POST /sessions/{id}/prompts` | `{"text":"Investigate this bug"}` → 202 admission receipt |
| `POST /sessions/{id}/cancel` | Stop current run; owner only; queued inputs follow daemon semantics |
| `DELETE /sessions/{id}` | Fence new gateway actions and stop runtime; owner only |
| `GET /sessions/{id}/events` | SSE snapshot followed by runtime events; optional `Last-Event-ID` |

Errors are `{"error": <message>, "code": <stable code>}`. Codes distinguish
responses sharing a status: 409 is `conflict` or `not_ready`; 413
`too_large`; 429 `limit_exceeded`; 503 `storage_unavailable` or
`not_delivered` (certainly not applied, safe to retry); 502
`runtime_unavailable` (outcome unknown, do not blindly retry).

SSE provides streaming while writes use ordinary HTTP. Use streaming `fetch` for
bearer headers; browser `EventSource` cannot set them. Cookie-based host auth
also requires host CSRF protection. A JavaScript submission:

```javascript
const response = await fetch(`${base}/sessions/${sessionId}/prompts`, {
  method: "POST",
  headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
  body: JSON.stringify({ text: "Investigate this bug" })
});
if (!response.ok) throw new Error(`Admission failed: ${response.status}`);
const { request_id } = await response.json(); // admitted, not completed
```

SSE uses `event: runtime` with a versioned envelope
`{"v":1,"kind":"snapshot"|"event","cursor":{"generation","sequence"}|null,"data":…}`.
The first frame is a `snapshot` whose `data` is the daemon attach result; later
`event` frames carry native session frames in `data`. Frames with a cursor have
SSE `id: <generation>:<sequence>`. On reconnect, send `Last-Event-ID`: the stream
starts with a fresh snapshot (apply it as the new state), then skips events at or
before that cursor in the same generation. There is no gateway replay journal:
reconnection converges state rather than replaying every frame. `event: error`
carries `{error, code}` and terminates the stream.

Every session member, viewers included, receives every frame, including tool
output. Restrict who joins a session if that is not acceptable.

## Collaboration and trust boundary

A session starts with one immutable owner. They invite existing workspace members
as contributors or viewers. Contributors submit prompts; viewers read/subscribe.
Only the owner shares, revokes, cancels or closes. Every action also checks the
host's current workspace policy.

The daemon adapter stores prompts as ordinary JSON user content with
`request_id`, `author: {tenant_id, user_id}`, and `text`, so authors survive in
durable conversation history and agent context. Attribution does not authorize
use of the participant's private credentials. Simultaneous messages are ordered
by daemon admission; HTTP arrival does not imply a particular order.

The host isolates workspace runtimes, files, secrets and networking. Duplicate
configured socket paths are rejected, but aliases and sandbox provisioning are
host responsibilities. Code running in one workspace shares that execution trust
boundary: session ACLs alone do not protect files from other agents in that
workspace. Use separate execution environments when stronger isolation is needed.

Open subscriptions are not re-authorized per event. A membership or status change
made through this gateway process stops affected streams before their next
event. Workspace policy changes and changes made by other gateway instances
apply within 5 seconds. HTTP credentials are re-verified before an SSE event at
most every `Authenticator::stream_revalidation()` (default 5 seconds; return
zero to check every event). Revocation stops subsequent delivery; previously
authorized work/data cannot be recalled. Dropping a subscription never cancels
the agent.

## Persistence and failure handling

Implement `SessionStore` over your database, keying by tenant and session.
Persist workspace, members, status, binding and revision. `replace` atomically
compares revisions and rejects stale writes. `MemoryStore` loses metadata on exit;
it is not durable SaaS storage. History remains the runtime's responsibility.

Creation records `provisioning` before contacting the runtime, then `ready` or
`failed`. Failed binding persistence triggers an attempt to close the unbound
runtime. Crashes or ambiguous transport outcomes require host reconciliation;
the daemon session name carries the gateway ID. Close fences metadata first;
failed runtime shutdown also needs reconciliation. Two independent stores are
not treated as a single transaction.

Mutations continue after HTTP disconnection while the Tokio runtime is alive.
Admission receipts do not mean model completion. Timeouts/lost responses may
hide successful admission. There are no automatic mutation retries or
exactly-once guarantees; reconcile history before resubmitting. The daemon
adapter reuses up to 4 idle command connections per workspace (discarded after
30 seconds idle) and retries a command on a fresh connection only when a pooled
one failed before delivery. Each subscription holds one connection, bounded by
`max_subscriptions`. Runtime recovery and queue durability are
execution-adapter responsibilities.

`Gateway::metrics()` returns process-local counters (sessions created/failed,
prompts admitted/failed, runtime errors, active subscriptions, access
rechecks, revoked streams) for the host's metrics exporter.

The host supplies TLS, CORS/CSRF policy, rate/concurrency limits, retention and
credential rotation. `WorkspacePolicy` provides admission quota checks. Hard
token budgets, subagent accounting and stopping work need runtime-side budget
enforcement, not a one-time HTTP balance check. Text is capped at 64 KiB, HTTP
bodies at 128 KiB, daemon frames and pre-response event backlogs at 8 MiB.

## Telemetry

Opt in with a host-owned `TelemetryClient` through `with_telemetry`. Creation,
sharing and prompt admission emit `agent feature outcome` with `gateway_session`,
`gateway_share` and `gateway_prompt`, a random feature ID and `completed` outcome.
No participant, tenant, session, content, path or credential enters these events.

## Verification and parity

Run `make gateway-check`, `make check` and `make deny`. Focused tests cover access,
roles, revocation, atomic metadata updates, attribution, HTTP impersonation,
stream revocation and recheck cost, resume, telemetry privacy, and native
daemon commands/handshake, connection reuse, dynamic routes and subscription
limits.

This opt-in surface leaves existing CLI, TUI, ACP and daemon schemas unchanged.
Native identifiers such as `prime-agent.daemon` and command names are preserved.
Socket contract tests are not a TS-binary comparison: merging still requires
recorded comparison against the TS daemon for create/prompt/attach/abort/kill.
An unavailable TS binary remains an outstanding merge gate.

Local verification on 2026-10-02: the 12 focused gateway tests and Clippy passed,
as did the 14 CI-selection tests. A live Rust supervisor/worker with a scripted
engine also passed creation, cross-tenant denial, invitation, an attributed
prompt delivered to two simultaneous subscribers, history recovery on reconnect,
owner-only cancellation, revocation and closure. This is runtime integration
evidence, not a comparison with the TS binary or real-provider behavior.
`make check` stopped at the pre-existing macOS `must_use_candidate` lint in
`pa-core/src/platform/process.rs:327`; the remaining workspace gates did not run.
`make deny` could not run because `cargo-deny` is not installed.
