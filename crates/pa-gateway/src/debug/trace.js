"use strict";
let traceIndex = null,
  selectedTrace = null,
  tracePage = 0,
  traceFollow = true,
  traceSerial = 0;
const traceLanes = [
  "Application",
  "Gateway / client",
  "Daemon",
  "Worker",
  "Model",
  "Agent kernel",
  "Application kernel",
];
const traceBoundary = {
  http_request: [0, 1],
  http_response: [1, 0],
  daemon_receive: [1, 2],
  daemon_send: [2, 1],
  worker_receive: [2, 3],
  worker_send: [3, 2],
  model_request: [3, 4],
  model_response: [4, 3],
  kernel_send: [3, 5],
  kernel_receive: [5, 3],
  application_kernel_send: [0, 6],
  application_kernel_receive: [6, 0],
};
const traceColors = {
  model_request: "#6b58a5",
  model_response: "#6b58a5",
  kernel_send: "#af783c",
  kernel_receive: "#af783c",
  application_kernel_send: "#347ca1",
  application_kernel_receive: "#347ca1",
};
const traceSvg = (tag, attrs, text) => {
  const element = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [key, value] of Object.entries(attrs))
    element.setAttribute(key, String(value));
  if (text !== undefined) element.textContent = text;
  return element;
};
async function debugFetch(path) {
  const headers = { "X-Prime-Agent-Debug": "1" };
  if (token) headers.Authorization = `Bearer ${token}`;
  const response = await fetch(new URL(path, location.href), {
    headers,
    cache: "no-store",
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok)
    throw new Error(`Trace request failed (${response.status})`);
  return response.json();
}
async function refreshTraces() {
  try {
    traceIndex = await debugFetch("trace/events");
    $("capture-label").textContent = traceIndex.enabled
      ? "CONTENT CAPTURE ON"
      : "METADATA MODE";
    $("capture-description").textContent = traceIndex.enabled
      ? "Prompts, messages and tool data. Credentials redacted."
      : "Execution capture is not configured.";
    if (traceIndex.enabled && !window.traceViewInitialized) {
      window.traceViewInitialized = true;
      selectedView = "trace";
      document
        .querySelectorAll("[data-view]")
        .forEach((button) =>
          button.classList.toggle("active", button.dataset.view === "trace"),
        );
    }
    renderTraces();
  } catch (error) {
    $("trace-status").textContent =
      `${error.message}. Last trace snapshot retained.`;
  }
}
function renderTraces() {
  $("trace-panel").hidden = selectedView !== "trace";
  $("ordinary-layout").hidden = selectedView === "trace";
  if (!traceIndex) return;
  const selected = $("trace-session").value;
  const sessions = [
    ...new Set(
      traceIndex.entries
        .map((entry) => entry.active_session_id)
        .filter(Boolean),
    ),
  ];
  const options = [node("option", "All sessions")];
  options[0].value = "";
  for (const id of sessions) {
    const option = node("option", id);
    option.value = id;
    options.push(option);
  }
  $("trace-session").replaceChildren(...options);
  $("trace-session").value = sessions.includes(selected) ? selected : "";
  const session = $("trace-session").value,
    point = $("trace-kind").value,
    workspace = $("workspace").value;
  const entries = traceIndex.entries.filter(
    (entry) =>
      (!session || entry.active_session_id === session) &&
      (!point || entry.point.startsWith(point)) &&
      (!workspace ||
        (entry.workspace && scope(entry.workspace) === workspace)) &&
      matches(entry),
  );
  const pageCount = Math.max(1, Math.ceil(entries.length / 100));
  tracePage = traceFollow ? pageCount - 1 : Math.min(tracePage, pageCount - 1);
  const visible = entries.slice(tracePage * 100, (tracePage + 1) * 100);
  $("trace-page").textContent =
    `${entries.length ? tracePage * 100 + 1 : 0}–${Math.min((tracePage + 1) * 100, entries.length)} of ${entries.length} exchanges`;
  $("trace-prev").disabled = tracePage === 0;
  $("trace-next").disabled = tracePage >= pageCount - 1;
  $("trace-empty").hidden = entries.length > 0;
  $("trace-empty").textContent = traceIndex.enabled
    ? "Waiting for matching exchanges. Send a message in your application to capture the real execution."
    : "Enable runtime capture with PRIME_AGENT_DEBUG_TRACE_DIR and configure a trace source in your embedding host.";
  const problems = traceIndex.sources
    .filter((source) => source.error)
    .map((source) => `${source.workspace.workspace_id}: ${source.error}`);
  $("trace-status").textContent = problems.length
    ? problems.join(" · ")
    : `${traceIndex.entries.length} retained · ${(traceIndex.retained_bytes / 1048576).toFixed(1)} MiB · ${traceIndex.evicted} evicted · ${traceIndex.dropped} dropped at capture`;
  $("trace-status").classList.toggle(
    "capture-error",
    problems.length > 0 || traceIndex.dropped > 0,
  );
  const counts = Object.keys(traceBoundary).reduce((map, point) => {
    map[point] = entries.filter((entry) => entry.point === point).length;
    return map;
  }, {});
  $("flow-map").replaceChildren(
    ...traceLanes.map((name, index) => {
      const item = node("button", "", "flow-node");
      item.type = "button";
      const related = Object.keys(traceBoundary).filter((point) =>
        traceBoundary[point].includes(index),
      );
      const total = related.reduce((n, point) => n + counts[point], 0);
      item.append(
        node("span", String(index + 1).padStart(2, "0"), "flow-number"),
        node("strong", name),
        node("small", `${total} exchanges`),
      );
      item.addEventListener("click", () => {
        $("trace-kind").value = [
          "http",
          "http",
          "daemon",
          "worker",
          "model",
          "kernel",
          "application_kernel",
        ][index];
        traceFollow = true;
        renderTraces();
      });
      return item;
    }),
  );
  const width = 1170,
    height = 74 + visible.length * 68;
  const svg = traceSvg("svg", {
    viewBox: `0 0 ${width} ${Math.max(height, 180)}`,
    width,
    "aria-label": "Execution sequence graph",
  });
  const x = (index) => 130 + index * 153;
  traceLanes.forEach((name, index) => {
    svg.append(
      traceSvg("line", {
        x1: x(index),
        x2: x(index),
        y1: 46,
        y2: height,
        stroke: "#dce5df",
        "stroke-dasharray": "3 6",
      }),
    );
    svg.append(
      traceSvg(
        "text",
        { x: x(index), y: 26, "text-anchor": "middle", class: "lane-title" },
        name,
      ),
    );
  });
  visible.forEach((entry, index) => {
    const y = 76 + index * 68,
      [from, to] = traceBoundary[entry.point],
      color = traceColors[entry.point] || "#377653",
      start = x(from),
      end = x(to),
      direction = end > start ? 1 : -1;
    const group = traceSvg("g", {
      role: "button",
      tabindex: "0",
      "aria-label": `${entry.point} ${entry.label} ${entry.id}`,
      class: `trace-exchange${selectedTrace?.id === entry.id ? " selected" : ""}`,
    });
    group.append(
      traceSvg("rect", {
        x: 0,
        y: y - 28,
        width,
        height: 62,
        rx: 6,
        class: "trace-row-background",
      }),
    );
    group.append(
      traceSvg(
        "text",
        { x: 10, y: y - 7, class: "trace-time" },
        time(entry.at_ms),
      ),
    );
    group.append(
      traceSvg(
        "text",
        { x: 10, y: y + 9, class: "trace-relative" },
        `+${duration(entry.at_ms - (entries[0]?.at_ms || entry.at_ms))}`,
      ),
    );
    group.append(
      traceSvg("line", {
        x1: start,
        x2: end - direction * 9,
        y1: y + 8,
        y2: y + 8,
        stroke: color,
        "stroke-width": 2,
      }),
    );
    group.append(
      traceSvg("circle", { cx: start, cy: y + 8, r: 4, fill: color }),
    );
    group.append(
      traceSvg("path", {
        d: `M${end},${y + 8} L${end - direction * 9},${y + 3} L${end - direction * 9},${y + 13} Z`,
        fill: color,
      }),
    );
    group.append(
      traceSvg(
        "text",
        {
          x: Math.min(start, end) + 8,
          y: y - 3,
          class: "trace-event-label",
          fill: color,
        },
        `${entry.point.replaceAll("_", " ")} · ${entry.label}`,
      ),
    );
    group.append(
      traceSvg(
        "text",
        { x: Math.min(start, end) + 8, y: y + 27, class: "trace-relative" },
        `${short(entry.active_session_id)} · ${(entry.bytes / 1024).toFixed(1)} KiB${entry.truncated ? " · TRUNCATED" : ""}`,
      ),
    );
    group.addEventListener("click", () => selectTrace(entry));
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        selectTrace(entry);
      }
    });
    svg.append(group);
  });
  const graph = $("trace-graph"),
    scroll = graph.scrollTop;
  graph.replaceChildren(svg);
  graph.scrollTop = scroll;
}
async function selectTrace(entry) {
  const serial = ++traceSerial;
  selectedTrace = null;
  $("payload-download").disabled = true;
  $("payload-title").textContent = entry.point.replaceAll("_", " ");
  $("payload-meta").textContent =
    `${time(entry.at_ms)} · PID ${entry.pid} · ${entry.active_session_id || "no session bound"}`;
  $("payload-json").textContent = "Loading captured payload…";
  $("payload-readable").replaceChildren();
  try {
    const record = await debugFetch(
      `trace/events/${encodeURIComponent(entry.id)}`,
    );
    if (serial !== traceSerial) return;
    selectedTrace = record;
    $("payload-json").textContent = JSON.stringify(record, null, 2);
    $("payload-download").disabled = false;
    const data = record.payload,
      body = data.body || data;
    const messages = body.messages || data.data?.messages;
    const readable = $("payload-readable");
    if (Array.isArray(messages)) {
      for (const message of messages) {
        const card = node("article", "", "prompt-message");
        card.append(
          node("h3", message.role || "message"),
          node(
            "pre",
            typeof message.content === "string"
              ? message.content
              : JSON.stringify(message.content ?? message, null, 2),
          ),
        );
        readable.append(card);
      }
    } else {
      const code =
        body.code ||
        data.code ||
        data.event?.args?.code ||
        data.event?.result ||
        body.message ||
        body.delta ||
        body.text;
      readable.append(
        node(
          "pre",
          typeof code === "string"
            ? code
            : JSON.stringify(code ?? data, null, 2),
        ),
      );
    }
    renderTraces();
  } catch (error) {
    if (serial === traceSerial)
      $("payload-json").textContent =
        `${error.message}. The capture may have been evicted.`;
  }
}
$("trace-session").addEventListener("change", () => {
  traceFollow = true;
  renderTraces();
});
$("trace-kind").addEventListener("change", () => {
  traceFollow = true;
  renderTraces();
});
$("trace-prev").addEventListener("click", () => {
  traceFollow = false;
  $("trace-follow").checked = false;
  tracePage--;
  renderTraces();
});
$("trace-next").addEventListener("click", () => {
  traceFollow = false;
  $("trace-follow").checked = false;
  tracePage++;
  renderTraces();
});
$("trace-follow").addEventListener("change", () => {
  traceFollow = $("trace-follow").checked;
  renderTraces();
});
document.querySelectorAll("[data-payload-view]").forEach((button) =>
  button.addEventListener("click", () => {
    const raw = button.dataset.payloadView === "json";
    $("payload-json").hidden = !raw;
    $("payload-readable").hidden = raw;
    document
      .querySelectorAll("[data-payload-view]")
      .forEach((item) => item.classList.toggle("active", item === button));
  }),
);
$("payload-download").addEventListener("click", () => {
  if (!selectedTrace) return;
  const url = URL.createObjectURL(
    new Blob([JSON.stringify(selectedTrace, null, 2)], {
      type: "application/json",
    }),
  );
  const link = document.createElement("a");
  link.href = url;
  link.download = `prime-agent-trace-${selectedTrace.id.replaceAll(":", "-")}.json`;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
