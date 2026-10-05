# pa-gateway

Embed collaborative agent sessions in your own application. Use the Rust service
directly, or mount its optional HTTP router for JavaScript, Python and other
clients. Your application keeps its authentication, database and infrastructure.

## Scope

Tenant-scoped session access, many-to-many membership, owner/contributor/viewer
permissions, attributed prompt admission, cancellation, closure and authorized
event subscriptions. The native daemon adapter uses its existing JSONL protocol
and follow-up queue. ACP is a separate stdio interface.
Optional process-local diagnostics cover HTTP exchanges and native agent rosters.

## Non-goals

No agent loop, tools, model providers, daemon supervision, account system,
billing, database migrations or sandbox provisioner. Session roles control
gateway access; they do not sandbox code executed by an agent.

## Public API and dependencies

- `Gateway<S, P, R>`: create/list/read/share/transfer/revoke/prompt/cancel/close/
  subscribe, workspace administration listing and `metrics()` counters.
- `SessionStore`: host persistence with atomic revision checks, keyset-paginated
  listing and durable prompt idempotency keys.
- `WorkspacePolicy`: current workspace authorization, administration and admission checks.
- `Runtime`: host execution boundary, with `DaemonRuntime` supplied (workspace
  routes can be registered and unregistered while running).
- `DaemonEndpoint`: trusted socket, create configuration and subscription limit per workspace.
- `MemoryStore`: ephemeral reference adapter for local development and tests.
- `EventStream`, `Error`, `Result`: streams and service outcomes.
- Optional `http::{Authenticator, router}`: credential validation and Axum routes.
- Optional `debug::{Inspector, DebugWatch, TraceSource, TraceWatch, router}`: bounded operational snapshots,
  native roster observation and an embeddable operator dashboard.

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
    {"token": "replace-with-alice-secret", "principal": {"tenant_id": "acme", "user_id": "alice"}, "workspaces": ["engineering"], "administers": ["engineering"]},
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

## Private sessions with a shared application kernel

```sh
cargo run -p pa-gateway --features debug --example multi_user -- \
  /absolute/host-config.json 127.0.0.1:3032 /absolute/kernel-venv/bin/python
```

The Python interpreter must already contain `prime-agent-runtime`. Open
`http://127.0.0.1:3032/`. Solo mode opens Alice's private session; multi-session
mode opens separate sessions for Alice, Bob and Camille. Their prompts,
histories and agent kernels remain session-scoped. They are **not** added as
members of each other's conversations.

One application-owned `python -m rlm.repl` process, using the same Prime Agent
runtime, holds the shared Python namespace. The executable `app-kernel` skill
connects each private agent to this process. Alice can create an object, Bob can
modify it, and the frontend renders `app` after each execution. The frontend can
also execute a cell directly through the authenticated application API. Closing
one chat does not dispose the shared kernel; restarting the demo resets it.

The host builds a live function catalog after each cell. Public top-level functions
defined in the application namespace appear with their signatures, docstrings and
sync/async kind. Redefinitions replace entries and deletions remove them, including
after a cell partially fails. Imported functions, private names, classes and methods
are excluded. Defaults are masked as `Ellipsis` and annotations omitted so rendering
metadata cannot invoke their `repr` or copy application values into another context.

The frontend sends messages through the example host's
`POST /app/sessions/{id}/prompts` route. The server adds a current, bounded catalog
recap (20 entries / 8 KiB metadata) and the caller's connection context, then calls
`Gateway::prompt`. This host route retains gateway authorization and idempotency.
The generic `/agents` API remains unchanged; embedding hosts explicitly adopt the
context enrichment in [`application.rs`](examples/multi_user/application.rs).
Catalog text is marked as application data, never instructions. Prompts/history,
function bodies, defaults and application values are not included in the recap.

Agents can call `await client.catalog()` (authenticated
`GET /app/kernel/catalog?session_id=...`) for the latest snapshot; each execution
also returns one. The skill requires checking it before defining/replacing a
function and encourages reuse and docstrings. The frontend displays the catalog
beside application state. The complete snapshot is capped at 128 entries / 48 KiB
entry metadata and reports truncation/errors explicitly. A snapshot includes the
kernel ID and revision; it is not a lock or a guarantee against duplicate logic.
Background mutations appear at the next completed cell. Metadata is shared
application content: do not put private prompts or credentials in docstrings.

The host serializes cells in a bounded queue and never retries code. Agent
prompts and history are not implicitly copied to the shared namespace. The
application kernel is separate from the private agent kernels and has no agent
host (`rlm.spawn`, conversation operations). This separation keeps application
state lifetime independent from conversations. Shared Python access is a trusted
workspace execution capability, not an OS sandbox between mutually hostile users.

Source: [`examples/multi_user.rs`](examples/multi_user.rs), its
[kernel host](examples/multi_user/kernel.rs), [Python skill](examples/multi_user/app-kernel/SKILL.md)
and dependency-free [frontend](examples/multi_user/index.html). The gateway
library still has no dependency on the engine. Applications can replace the
example's state projection, authentication and storage while using the same
HTTP/session and diagnostic APIs.

The demo mints local credentials and private connection files; configured
application credentials are never exposed. It binds loopback, validates
Host/Origin/fetch-site and refuses kernel execution against another user's
private session. `/demo/users` is intentionally a local developer bootstrap,
not production login. `/inspect/` is an operator view of all traces, including
private prompts when capture is enabled. Normal users must not receive it.

## Gateway Inspector

Enable the `debug` feature and opt in when starting the local example:

```sh
cargo run -p pa-gateway --features debug --example embedded -- \
  /absolute/host-config.json 127.0.0.1:3030 --debug 127.0.0.1:3031
```

Open `http://127.0.0.1:3031/`. The dashboard refreshes every second and shows:

- HTTP requests, verified user/tenant, session, response status, time to headers
  and full response duration. Open SSE bodies remain visible as connections;
  disconnecting a client does not imply the agent stopped.
- Main agents and subagents, parent runtime IDs, model/provider, current activity
  and status from the daemon's native roster. This includes existing runtime
  sessions even when no gateway chat stream is open.
- Workspace observer connectivity, a transition timeline, filtering, individual
  request/agent details, pause/resume and JSON export of the current snapshot.

The inspector makes no execution mutations. In its default metadata mode,
only allowlisted roster metadata and HTTP timings are retained; path-bearing
roster keys become opaque IDs. Connections mean open HTTP event streams, not
browser presence or TCP connection counts.

For prompts, daemon/worker frames, model payloads and Python execution, enable
[execution capture and its sequence graph](src/debug/README.md). This explicit
option records content in private local files and adds authenticated trace APIs.
It is independent of adoption telemetry and requires rebuilding/restarting the
Rust daemon and workers with the recorder enabled.

Capture is bounded to 1,000 HTTP exchanges, 500 transitions, 64 workspaces and
2,000 agents per workspace. Counters still include evicted HTTP entries; agent
truncation is explicit. Completed requests are evicted before active ones. If
more than 1,000 requests remain open, some connection details are omitted while
the global counters remain accurate. Metadata resets on process restart; explicit execution capture can reload its retained files.
Observer failures retain last-known agent data **marked stale**, then reconnect
with capped backoff; they never retry prompts or affect running agents.

For an embedding host:

```rust,ignore
let inspector = pa_gateway::debug::Inspector::default();
let observation = inspector.watch_daemon(&runtime)?; // Arc<DaemonRuntime>
let gateway = Gateway::new(store, policy, runtime)
    .with_telemetry(host_telemetry)
    .with_inspector(inspector.clone());
let app = axum::Router::new()
    .nest("/agents", pa_gateway::http::router(gateway, user_auth))
    .nest("/inspect", pa_gateway::debug::router(inspector, operator_auth));
// Serve app, keeping observation alive. Open /inspect/ (trailing slash).
```

`Inspector::snapshot()` exposes the same typed `pa-types::gateway::debug` data
for a host's own UI. HTTP recording works with every `Runtime`; the supplied
agent observer requires `DaemonRuntime`. Dropping `DebugWatch` stops observation
and marks its workspaces stopped. The host must provide separate **operator**
authorization: the inspector intentionally sees all configured tenants and must
never use ordinary participant credentials as admin access. `/snapshot`, `/trace/events` and `/trace/events/{id}` check
that authorization on every request; the static page has no embedded data, accepts
an optional bearer credential kept only in page memory, and does not load CDNs.

The example's separate debug listener refuses non-loopback addresses. Its local
operator adapter checks Host, Origin, fetch-site and a non-simple request header,
and supplies no CORS allowance, preventing a foreign web page from fetching local
diagnostics. Any local process can read this intentionally local developer
endpoint. Replace that adapter with real operator authentication before embedding
the dashboard on a shared host; do not reverse-proxy the local example publicly.
Without `--debug`, the example opens no debug listener or roster subscription.

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
| `GET /sessions?after=&limit=` | `{"sessions":[…],"next":…}`: the caller's accessible sessions by ID; `limit` 1–200, default 50 |
| `GET /workspaces/{workspace_id}/sessions?after=&limit=` | Every session in the workspace, any status; administrators only |
| `GET /sessions/{id}` | Metadata without the internal runtime binding |
| `PUT /sessions/{id}/members/{user_id}` | `{"role":"contributor"}` or `viewer`; owner or administrator |
| `DELETE /sessions/{id}/members/{user_id}` | Revoke member; owner or administrator |
| `PUT /sessions/{id}/owner` | `{"user_id":"bob"}`: transfer to an existing member; owner or administrator |
| `POST /sessions/{id}/prompts` | `{"text":"Investigate this bug"}` → 202 admission receipt; optional `Idempotency-Key` header |
| `POST /sessions/{id}/cancel` | Stop current run; owner or administrator; queued inputs follow daemon semantics |
| `DELETE /sessions/{id}` | Fence new gateway actions and stop runtime, in any status; repeat to retry a failed shutdown; owner or administrator |
| `GET /sessions/{id}/events` | SSE snapshot followed by runtime events; optional `Last-Event-ID` |

Errors are `{"error": <message>, "code": <stable code>}`. Codes distinguish
responses sharing a status: 409 is `conflict`, `not_ready` or
`idempotency_unresolved`; 413 `too_large`; 429 `limit_exceeded`; 503
`storage_unavailable` or `not_delivered` (certainly not applied, safe to retry);
502 `runtime_unavailable` (outcome unknown, do not blindly retry).

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

Prompts with an `Idempotency-Key` (1–256 bytes, scoped to author and session)
are recorded before delivery. Retrying an admitted key returns the original
receipt without resubmitting. A retry whose earlier attempt has an unknown
outcome returns 409 `idempotency_unresolved`: reconcile history (prompts carry
their `request_id`) before using a new key. A certain failure (`not_delivered`)
frees the key.

## Collaboration and trust boundary

A session starts with one owner. They invite existing workspace members as
contributors or viewers. Contributors submit prompts; viewers read/subscribe.
Only the owner shares, transfers ownership, revokes, cancels or closes. Every
action also checks the host's current workspace policy.

Workspace administrators, those granted `GatewayAction::Administer` by the
policy, may perform the owner's actions on any session of their workspace and
list all its sessions, including `provisioning` and `failed` ones. They can
recover a session whose owner left by transferring or closing it. Administration
never grants reading, subscribing to or prompting a session the administrator is
not a member of.

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
compares revisions and rejects stale writes. Index membership and workspace so
`list_member` and `list_workspace` read one page by session ID instead of the
whole tenant. Store prompt keys with a unique constraint and expire settled keys
on your retention schedule. `MemoryStore` loses metadata and keys on exit; it is
not durable SaaS storage. History remains the runtime's responsibility.

Creation records `provisioning` before contacting the runtime, then `ready` or
`failed`. Failed binding persistence triggers an attempt to close the unbound
runtime. Crashes or ambiguous transport outcomes require host reconciliation;
the daemon session name carries the gateway ID. Administrators find stuck
sessions with `list_workspace` and close them. Close fences metadata first;
closing again retries a failed runtime shutdown. Two independent stores are not
treated as a single transaction.

Mutations continue after HTTP disconnection while the Tokio runtime is alive.
Admission receipts do not mean model completion. Timeouts/lost responses may
hide successful admission; idempotency keys make such retries safe. The daemon
adapter reuses up to 4 idle command connections per workspace (discarded after
30 seconds idle) and retries a command on a fresh connection only when a pooled
one failed before delivery. Each subscription holds one connection, bounded by
`max_subscriptions`. Runtime recovery and queue durability are
execution-adapter responsibilities.

`Gateway::metrics()` returns process-local counters (sessions created/failed,
prompts admitted/replayed/failed, runtime errors, active subscriptions, access
rechecks, revoked streams) for the host's metrics exporter.

The host supplies TLS, CORS/CSRF policy, rate/concurrency limits, retention and
credential rotation. `WorkspacePolicy` provides admission quota checks. Hard
token budgets, subagent accounting and stopping work need runtime-side budget
enforcement, not a one-time HTTP balance check. Text is capped at 64 KiB, HTTP
bodies at 128 KiB, daemon frames and pre-response event backlogs at 8 MiB.

## Telemetry

Opt in with a host-owned `TelemetryClient` through `with_telemetry`. Creation,
sharing, ownership transfer and prompt admission emit `agent feature outcome`
with `gateway_session`, `gateway_share`, `gateway_transfer` and `gateway_prompt`,
a random feature ID and `completed` outcome. Idempotent replays emit nothing.
Enabling `with_inspector` after `with_telemetry` emits `gateway_inspector` (and
`gateway_execution_trace` when content collection is enabled) through
the same versioned adoption schema. Debug snapshots are not sent to telemetry.
No participant, tenant, session, content, path or credential enters these events.

## Verification and parity

Run `make gateway-check`, `make check` and `make deny`. Focused tests cover access,
roles, revocation, atomic metadata updates, attribution, HTTP impersonation,
stream revocation and recheck cost, resume, idempotency, pagination,
administration and close retry, telemetry privacy, and native daemon
commands/handshake, connection reuse, dynamic routes and subscription limits.

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

Inspector validation on 2026-10-02: all 19 gateway tests and all-feature Clippy
passed, including streaming-body lifetime, operator authorization, credential
exclusion, bounded history, native roster subscription and observer replacement.
The default-feature build also passed. A real local daemon/provider run confirmed
authenticated connection tracking, Python tool execution, runtime transitions,
HTTP error recording and disconnect cleanup; the dashboard, filters and request
details were checked in Chrome. Six live local-access checks covered foreign
Host/Origin/fetch-site rejection and allowed loopback access. `make check` still
stops at the same unrelated `pa-core` lint above. This is a new opt-in operator
surface with new public inspector types/methods; it adds no dependency package,
no model tool and no daemon wire identifier. It is not TS parity-diff evidence.

Execution capture and shared-kernel validation on 2026-10-03: gateway all-feature
and default-feature suites passed; the additional same-timestamp ordering
regression passed. All-feature gateway Clippy and workspace fmt passed. Recorder
and request-timing tests passed, including a real-kernel regression for queued
execute requests and their correlated stdout. A live two-agent run verified the
same application Python process/object across private sessions, serialized
concurrent mutations, one agent modifying the other's application object,
separate model contexts, denied cross-session access, and application state
surviving chat closure. Native model prompts and Python cells were inspected in
the graph in Chrome. Chrome blocked automatic navigation to the new demo port
3032, so frontend interaction verification remains limited; its live HTTP/API
flow was exercised directly. `make check` still stops at the pre-existing macOS
lint described above; no TS visual parity claim is made for these new examples.

Function-catalog validation on 2026-10-05: all-feature gateway tests and Clippy
passed, as did four Python projection regressions and the real-kernel example
test (run with `PA_APP_KERNEL_PYTHON=/absolute/kernel-python cargo test -p
pa-gateway --example multi_user --features debug --locked -- --ignored`). The
regressions cover definitions, aliases, redefinition/deletion, partially failed
cells, broken app projection, metadata bounds and avoiding default-value `repr`.
A live provider run confirmed that the very first request includes the host recap,
the second user's agent calls `client.catalog()` and reuses the first user's
function with unchanged Python object identity. Authorization, idempotent prompt
admission and closed-session denial passed. No library API, daemon protocol or
dependency changed; host-example routes and the telemetry feature vocabulary grew.
The local demo with this feature runs on port 3033 because an open session on 3032
prevented a safe restart. Chrome also blocked automatic navigation to 3033, so
frontend visual verification remains unavailable. `make check` still stops at
the pre-existing `pa-core/src/platform/process.rs:327` lint above.
