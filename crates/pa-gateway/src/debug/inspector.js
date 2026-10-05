"use strict";
const $ = (id) => document.getElementById(id);
let snapshot = null,
  paused = false,
  token = "",
  selectedView = "overview",
  pending = false;
const time = (value) =>
  new Date(value).toLocaleTimeString([], { hour12: false });
const duration = (value) =>
  value < 1000
    ? `${value} ms`
    : value < 60000
      ? `${(value / 1000).toFixed(1)} s`
      : `${Math.floor(value / 60000)}m ${Math.floor(value / 1000) % 60}s`;
const short = (value) => (value ? value.slice(0, 8) : "—");
const scope = (value) => `${value.tenant_id} / ${value.workspace_id}`;
const node = (tag, text, className) => {
  const el = document.createElement(tag);
  el.textContent = text;
  if (className) el.className = className;
  return el;
};
const matches = (value) =>
  JSON.stringify(value).toLowerCase().includes($("search").value.toLowerCase());
function details(title, value) {
  $("detail-title").textContent = title;
  $("detail-json").textContent = JSON.stringify(value, null, 2);
  $("detail").showModal();
}
function row(cells, title, value) {
  const tr = document.createElement("tr");
  tr.tabIndex = 0;
  tr.setAttribute("aria-label", title);
  for (const content of cells) {
    const td = document.createElement("td");
    td.append(
      typeof content === "string" ? document.createTextNode(content) : content,
    );
    tr.append(td);
  }
  tr.addEventListener("click", () => details(title, value));
  tr.addEventListener("keydown", (e) => {
    if (e.key === "Enter") details(title, value);
  });
  return tr;
}
function labeled(primary, secondary) {
  const cell = node("span", primary, "mono");
  cell.append(node("span", secondary, "secondary"));
  return cell;
}
function badge(text, kind) {
  return node("span", text, `status ${kind}`);
}
function render() {
  if (!snapshot) return;
  const selected = $("workspace").value;
  const workspaces = snapshot.workspaces.filter(
    (w) => !selected || scope(w.workspace) === selected,
  );
  const agents = workspaces.flatMap((w) =>
    w.agents.map((a) => ({
      ...a,
      workspace: w.workspace,
      connection: w.connection,
    })),
  );
  const sessionIds = new Set(
    agents
      .flatMap((a) => [a.gateway_session_id, a.active_session_id, a.session_id])
      .filter(Boolean),
  );
  const requests = snapshot.requests.filter(
    (r) => (!selected || sessionIds.has(r.session_id)) && matches(r),
  );
  const shownAgents = agents.filter(matches);
  $("total").textContent = snapshot.total_requests.toLocaleString();
  $("streams").textContent = snapshot.open_streams.toLocaleString();
  $("running").textContent = snapshot.workspaces
    .filter((w) => w.connection === "connected")
    .reduce(
      (n, w) => n + w.agents.filter((a) => a.status === "running").length,
      0,
    );
  $("errors").textContent = snapshot.requests.filter(
    (r) => r.status >= 400,
  ).length;
  $("updated").textContent = `Updated ${time(snapshot.captured_at_ms)}`;
  $("retention").textContent = snapshot.evicted_requests
    ? `${snapshot.evicted_requests.toLocaleString()} older entries discarded`
    : "Latest 1,000 requests";
  $("request-count").textContent = requests.length;
  $("agent-count").textContent = shownAgents.length;
  $("agents-body").replaceChildren(
    ...shownAgents.map((a) =>
      row(
        [
          labeled(
            `${a.kind === "subagent" ? "↳ " : ""}${short(a.active_session_id || a.session_id || a.id)}`,
            a.kind,
          ),
          scope(a.workspace),
          labeled(a.model || "—", a.provider || ""),
          badge(
            a.connection === "connected" ? a.status : "stale",
            a.connection === "connected" ? a.status : "stale",
          ),
          a.activity,
        ],
        `Agent ${a.active_session_id || a.id}`,
        a,
      ),
    ),
  );
  $("requests-body").replaceChildren(
    ...requests.map((r) => {
      const method = node("span", r.method, "method");
      const route = node("span", "", "mono");
      route.append(method, document.createTextNode(r.route));
      const status =
        r.status === null
          ? badge(r.ended_at_ms ? "interrupted" : "pending", "stale")
          : badge(
              String(r.status),
              r.status >= 400
                ? "error"
                : r.streaming && !r.ended_at_ms
                  ? "streaming"
                  : "ok",
            );
      const age = r.duration_ms ?? snapshot.captured_at_ms - r.started_at_ms;
      return row(
        [
          time(r.started_at_ms),
          route,
          r.principal
            ? labeled(r.principal.user_id, r.principal.tenant_id)
            : "Unauthenticated",
          status,
          `${duration(age)}${r.ended_at_ms ? "" : " · open"}`,
        ],
        `${r.method} ${r.route}`,
        r,
      );
    }),
  );
  const connections = requests.filter((r) => r.streaming && !r.ended_at_ms);
  $("connections-body").replaceChildren(
    ...connections.map((r) =>
      row(
        [
          r.principal?.user_id || "—",
          r.principal?.tenant_id || "—",
          short(r.session_id),
          duration(snapshot.captured_at_ms - r.started_at_ms),
          short(r.id),
        ],
        `Connection ${r.id}`,
        r,
      ),
    ),
  );
  $("agents-empty").hidden = shownAgents.length > 0;
  $("requests-empty").hidden = requests.length > 0;
  $("connections-empty").hidden = connections.length > 0;
  const events = snapshot.events.filter(
    (e) => (!selected || scope(e.workspace) === selected) && matches(e),
  );
  $("events").replaceChildren(
    ...events.slice(0, 80).map((e) => {
      const li = document.createElement("li");
      const top = node("div", "", "event-top");
      top.append(
        node("span", e.activity),
        node("span", time(e.at_ms), "event-time"),
      );
      li.append(
        top,
        node(
          "span",
          `${e.workspace.workspace_id}${e.agent_id ? ` · ${e.agent_id.slice(-8)}` : ""}`,
          "event-agent",
        ),
      );
      return li;
    }),
  );
  $("events-empty").hidden = events.length > 0;
  $("workspace-health").replaceChildren(
    ...workspaces.map((w) => {
      const item = node("div", "", "workspace-state");
      item.append(
        node("span", w.workspace.workspace_id),
        badge(w.connection, w.connection === "connected" ? "ok" : "stale"),
      );
      return item;
    }),
  );
  const stale = snapshot.workspaces.filter((w) => w.connection !== "connected");
  const omitted = snapshot.workspaces.some((w) => w.truncated);
  if (stale.length || omitted) {
    $("notice").textContent =
      `${stale.length ? "Some runtimes are disconnected. Their last known agent state is stale. " : ""}${omitted ? "The agent observation limit was reached; this view is incomplete." : ""}`;
    $("notice").hidden = false;
  } else $("notice").hidden = true;
  $("agents-panel").hidden = !["overview", "agents"].includes(selectedView);
  $("requests-panel").hidden = !["overview", "requests"].includes(selectedView);
  $("connections-panel").hidden = selectedView !== "connections";
  $("export").disabled = false;
  if (typeof renderTraces === "function") renderTraces();
}
async function refresh() {
  if (paused || pending) return;
  pending = true;
  try {
    const headers = { "X-Prime-Agent-Debug": "1" };
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(new URL("snapshot", location.href), {
      headers,
      cache: "no-store",
      signal: AbortSignal.timeout(5000),
    });
    if (!response.ok) {
      $("login").hidden = ![401, 403].includes(response.status);
      throw new Error(
        [401, 403].includes(response.status)
          ? "Operator access required"
          : `Inspector unavailable (${response.status})`,
      );
    }
    snapshot = await response.json();
    $("login").hidden = true;
    $("health").textContent = "Live · 1s";
    $("health").className = "health live";
    const selected = $("workspace").value;
    const options = [node("option", "All workspaces")];
    options[0].value = "";
    for (const workspace of snapshot.workspaces) {
      const label = scope(workspace.workspace);
      const option = node("option", label);
      option.value = label;
      options.push(option);
    }
    $("workspace").replaceChildren(...options);
    $("workspace").value = selected;
    await refreshTraces();
    render();
  } catch (error) {
    $("health").textContent = "Disconnected";
    $("health").className = "health error";
    $("notice").textContent =
      `${error.message}. ${snapshot ? "Showing the last snapshot." : "No snapshot received."}`;
    $("notice").hidden = false;
  } finally {
    pending = false;
  }
}
$("pause").addEventListener("click", () => {
  paused = !paused;
  $("pause").textContent = paused ? "Resume" : "Pause";
  $("health").textContent = paused ? "Paused" : "Connecting…";
  $("health").className = "health";
  if (!paused) refresh();
});
$("login").addEventListener("submit", (e) => {
  e.preventDefault();
  token = $("token").value.trim();
  $("token").value = "";
  refresh();
});
$("close-detail").addEventListener("click", () => $("detail").close());
$("search").addEventListener("input", render);
$("workspace").addEventListener("change", render);
document.querySelectorAll("[data-view]").forEach((button) =>
  button.addEventListener("click", () => {
    selectedView = button.dataset.view;
    document
      .querySelectorAll("[data-view]")
      .forEach((other) => other.classList.toggle("active", other === button));
    render();
  }),
);
$("export").addEventListener("click", () => {
  if (!snapshot) return;
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(snapshot, null, 2)], { type: "application/json" }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = "prime-agent-gateway-debug.json";
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
refresh();
setInterval(refresh, 1000);
