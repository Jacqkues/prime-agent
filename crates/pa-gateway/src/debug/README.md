# Execution capture

The execution graph follows observed exchanges through Application → Gateway →
Daemon → Worker → Model / Agent kernel, plus the host application kernel. Clicking an arrow opens its captured payload,
with readable model messages, full JSON and download. Filter by workspace,
active session or boundary. The HTTP and agent overview remains available.

## Enable in an embedding host

1. Create a private Unix directory (`mkdir -m 700 /absolute/capture`).
2. Start the **Rust daemon and new workers** with
   `PRIME_AGENT_DEBUG_TRACE_DIR=/absolute/capture`. Setting it only on the gateway
   cannot instrument an already-running daemon. Omit it to disable recording.
3. Enable the gateway's `debug` Cargo feature and collect that directory:

```rust,ignore
let capture = inspector.watch_traces(vec![pa_gateway::debug::TraceSource {
    workspace,
    directory: "/absolute/capture".into(),
}])?;
// Keep capture alive. Attach inspector to Gateway and mount debug::router
// with a separate operator authenticator as in the crate README.
```

For the `embedded` example, add a `traces` array to its host configuration and
pass `--debug 127.0.0.1:3031`:

```json
{"traces":[{"workspace":{"tenant_id":"team","workspace_id":"app"},"directory":"/absolute/capture"}]}
```

Use one directory per isolated daemon/workspace. No directory can be selected by
an HTTP caller. This is a reusable library capability; neither Pandor nor a
particular application's token/storage scheme is required.

## What is captured

| Boundary | Content |
| --- | --- |
| HTTP | Authenticated request body/path/principal; status, non-SSE response body and duration |
| Daemon | Received and successfully written native JSON frames, including prompt envelopes |
| Worker | Received command/header/payload and written response/event payloads |
| Model | Final provider request body after payload hooks, including system/user messages and tools; normalized streamed response events and final message |
| Kernel | JSON sent to and received from the persistent Python kernel: code, host requests, output, results and protocol errors |

This is boundary instrumentation, not a network packet sniffer. Provider response
events are normalized by the engine, not raw provider HTTP bytes. Auxiliary
one-off model calls outside the agent stream (for example compaction), arbitrary
HTTP inside Python skills, process stderr and binary framing headers on worker
responses are outside this capture. Native daemon events cover streamed client
content; SSE bodies are not duplicated into HTTP response records.

The sequence graph orders observation timestamps; arrows indicate the boundary
actually observed. Concurrent sessions can interleave. Active session IDs and
correlation IDs are retained where the protocol provides them; timing alone
does not establish parent/child causality or a distributed trace span.

## Content access and bounds

`Inspector::trace_index()` / `GET /trace/events` expose a payload-free index.
`Inspector::trace(id)` / `GET /trace/events/{id}` retrieve one retained record.
Both routes use the same **operator-only** authentication as `/snapshot`, on
every request. Ordinary SaaS users must never receive this all-workspaces view.
The UI renders payloads as text, uses no CDN and stores no bearer token on disk.

Structured credential keys and JSON-encoded credential objects are redacted
before disk persistence and again on ingestion. Ordinary prompt/code/output text
is deliberately preserved and can itself contain secrets. This is not general
secret detection. No trace content is sent to adoption telemetry.

The producer uses a 16-record queue, a 4 MiB payload ceiling and two rotating
32 MiB segments per process instance (0600 files in a 0700 directory). Oversized
payloads become explicit omission markers; full queues increment a drop counter
reported in subsequent records. Recording is best-effort: abrupt process exit
can lose queued records. Writer errors are reported to process stderr.

The collector retains at most 2,000 payloads / 64 MiB in memory, reads complete
JSONL records on a 250 ms interval and exposes errors instead of silently hiding
capture failures. HTTP bodies are limited to 128 KiB; truncated responses are
marked. UI counters show retained/evicted/dropped records. Polling refreshes the
graph each second and pages it in batches of 100 exchanges.

Files from previous process instances remain available after restart. The host
owns their archive/deletion policy; collection refuses directories containing
more than 128 segments until older captures are archived. Stop collection by
dropping `TraceWatch`; disable future production by removing the environment
variable and restarting the relevant runtime processes.

## Ownership and verification

Shared capture schema/redaction lives in `pa-types`, producer instrumentation in
`pa-core` and `pa-daemon`, collection/operator UI in `pa-gateway`. No dependency
from gateway to the engine/daemon and no new dependency package is introduced.
This adds public diagnostic APIs; existing daemon/kernel wire bytes are unchanged.
The graph is an explicitly requested Rust extension with no TS dashboard
equivalent. Native protocol parity and existing request-timing tests must remain
green; runtime verification should inspect a real model request and Python result.

Embedding hosts can call `Inspector::observe(workspace, ExecutionTrace)` for their
own actual runtime boundaries. `watch_traces(vec![])` explicitly enables bounded
in-memory host capture with no file sources. The multi-user example uses this
seam for its application kernel; those events are visible in its `/inspect/`
and remain separate from the private agent kernel's file-backed traces.
