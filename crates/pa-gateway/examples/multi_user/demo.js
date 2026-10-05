"use strict";
const $ = (id) => document.getElementById(id);
let config,
  users = [],
  mode = "solo",
  busy = false,
  polling = false,
  revision = -1,
  catalogError = null;
const samples = [
  "Dans le kernel partagé de l’application, ajoute à app['tasks'] une tâche intitulée Préparer la démo avec done=False et un id unique. Préserve les autres tâches. Puis confirme brièvement ici.",
  "Dans le même kernel d’application, inspecte app['tasks'] puis passe la première tâche à done=True si elle existe. Confirme ce que tu as modifié, sans inventer de tâche.",
  "Lis app dans le kernel partagé et résume son état. Ne modifie rien.",
];
function node(tag, text, className) {
  const element = document.createElement(tag);
  element.textContent = text;
  if (className) element.className = className;
  return element;
}
function notice(text, error = false) {
  $("notice").textContent = text;
  $("notice").classList.toggle("error", error);
}
function journal(text) {
  $("journal").querySelector(".quiet")?.remove();
  const item = node("li", "");
  item.append(
    node("time", new Date().toLocaleTimeString("fr-FR")),
    document.createTextNode(text),
  );
  $("journal").prepend(item);
  while ($("journal").children.length > 100) $("journal").lastChild.remove();
}
async function request(user, method, path, body) {
  const response = await fetch(path, {
    method,
    headers: {
      Authorization: `Bearer ${user.token}`,
      "Content-Type": "application/json",
      "Idempotency-Key": crypto.randomUUID(),
    },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(70000),
  });
  const data = response.status === 204 ? null : await response.json();
  if (!response.ok)
    throw new Error(`${response.status} · ${data?.error || "Requête refusée"}`);
  return data;
}
function update() {
  $("users").classList.toggle("solo", mode === "solo");
  $("solo").classList.toggle("selected", mode === "solo");
  $("shared").classList.toggle("selected", mode === "shared");
  const count = users.filter((user) => user.session).length;
  $("start").disabled = busy || count > 0;
  $("close").disabled = busy || count === 0;
  $("solo").disabled = busy;
  $("shared").disabled = busy;
  $("session-label").textContent = `${count} session(s) privée(s)`;
  $("connection-count").textContent =
    `${users.filter((user) => user.connected).length} connexion(s)`;
  $("increment").disabled = busy || !users[0]?.session;
  $("execute-code").disabled = busy || !users[0]?.session;
  users.forEach((user, index) => {
    user.card.hidden = mode === "solo" && index > 0;
    user.card.querySelector(".role").textContent = "Session privée";
    user.card.querySelector(".user-id").textContent = user.session
      ? `${user.principal.user_id} / ${user.session.id.slice(0, 8)}`
      : user.principal.user_id;
    user.card
      .querySelector(".connection")
      .classList.toggle("connected", user.connected);
    user.card.querySelector(".connection-label").textContent = user.connected
      ? "Conversation privée connectée"
      : user.controller
        ? "Connexion…"
        : "Déconnecté";
    user.card.querySelector(".reconnect").textContent = user.controller
      ? "Déconnecter"
      : "Reconnecter";
    user.card.querySelector(".reconnect").disabled = busy || !user.session;
    for (const element of user.card.querySelectorAll("textarea,.send,.sample"))
      element.disabled = busy || !user.connected || user.sending;
  });
}
function bubble(user, kind, author, text) {
  user.messages.querySelector(".empty")?.remove();
  const element = node("div", "", `bubble ${kind}`);
  element.append(node("strong", author), node("span", text));
  user.messages.append(element);
  while (user.messages.children.length > 150) user.messages.firstChild.remove();
  user.messages.scrollTop = user.messages.scrollHeight;
  return element.querySelector("span");
}
function content(message) {
  return typeof message?.content === "string"
    ? message.content
    : (message?.content || [])
        .filter((part) => part.type === "text")
        .map((part) => part.text || "")
        .join("");
}
function userMessage(user, message) {
  let text = content(message),
    id;
  try {
    const envelope = JSON.parse(text);
    if (envelope.text && envelope.author) {
      text = envelope.text;
      id = envelope.request_id;
    }
  } catch {
    /* Native plain text remains valid. */
  }
  if (id && user.seen.has(id)) return;
  if (id) user.seen.add(id);
  const marker = "\n\nMessage utilisateur:\n";
  if (text.includes(marker))
    text = text.slice(text.indexOf(marker) + marker.length);
  bubble(user, "user", user.name, text);
}
function frame(user, envelope) {
  if (envelope.error) throw new Error(envelope.error);
  const data = envelope.v ? envelope.data : envelope;
  if (envelope.kind === "snapshot" || data.type === "snapshot") {
    user.messages.replaceChildren();
    user.seen.clear();
    user.answer = null;
    const state = data.snapshot || data.state || data;
    for (const message of state.messages || state.session?.messages || []) {
      if (message.role === "user") userMessage(user, message);
      else if (message.role === "assistant" && content(message))
        bubble(user, "assistant", "Prime Agent", content(message));
    }
    if (!user.messages.children.length)
      user.messages.append(
        node(
          "p",
          "Votre conversation est privée.\nLe kernel applicatif est partagé.",
          "empty",
        ),
      );
    user.connected = true;
    update();
    user.ready?.();
    user.ready = null;
    return;
  }
  if (data.type === "session_closed") {
    user.controller?.abort();
    return;
  }
  const event = data.event || data;
  if (event.type === "agent_start")
    user.card.querySelector(".thinking").hidden = false;
  if (event.type === "message_start" && event.message?.role === "user")
    userMessage(user, event.message);
  if (event.type === "message_start" && event.message?.role === "assistant")
    user.answer = null;
  if (
    event.type === "message_update" &&
    event.assistantMessageEvent?.type === "text_delta"
  ) {
    user.answer ||= bubble(user, "assistant", "Prime Agent", "");
    user.answer.textContent += event.assistantMessageEvent.delta || "";
    user.messages.scrollTop = user.messages.scrollHeight;
  }
  if (event.type === "message_end" && event.message?.role === "assistant") {
    const text = content(event.message);
    if (text) {
      user.answer ||= bubble(user, "assistant", "Prime Agent", "");
      user.answer.textContent = text;
    }
    if (event.message.errorMessage)
      bubble(user, "system", "Erreur", event.message.errorMessage);
  }
  if (event.type === "tool_execution_start")
    bubble(
      user,
      "system",
      `Outil · ${event.toolName}`,
      event.args?.code || "Exécution en cours…",
    );
  if (event.type === "tool_execution_end")
    bubble(
      user,
      "system",
      event.isError ? "Erreur outil" : "Résultat outil",
      content(event.result).slice(0, 6000),
    );
  if (event.type === "agent_end") {
    user.card.querySelector(".thinking").hidden = true;
    user.answer = null;
    journal(`${user.name} · son agent a terminé dans sa session privée.`);
    void refreshState();
  }
}
async function connect(user) {
  user.controller?.abort();
  const controller = new AbortController();
  user.controller = controller;
  user.connected = false;
  update();
  const ready = new Promise((resolve, reject) => {
    user.ready = resolve;
    user.rejectReady = reject;
  });
  const sid = user.session.id;
  void (async () => {
    try {
      const response = await fetch(`/agents/sessions/${sid}/events`, {
        headers: { Authorization: `Bearer ${user.token}` },
        signal: controller.signal,
      });
      if (!response.ok)
        throw new Error(`Connexion refusée (${response.status})`);
      const reader = response.body.getReader(),
        decoder = new TextDecoder();
      let buffer = "";
      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        buffer = buffer.replaceAll("\r\n", "\n");
        let boundary;
        while ((boundary = buffer.indexOf("\n\n")) >= 0) {
          const raw = buffer.slice(0, boundary);
          buffer = buffer.slice(boundary + 2);
          const data = raw
            .split("\n")
            .filter((line) => line.startsWith("data:"))
            .map((line) => line.slice(5).trimStart())
            .join("\n");
          if (data) frame(user, JSON.parse(data));
        }
      }
      if (!controller.signal.aborted) journal(`${user.name} · flux fermé.`);
    } catch (error) {
      if (!controller.signal.aborted) {
        journal(`${user.name} · ${error.message}`);
        notice(error.message, true);
      }
    } finally {
      if (user.controller === controller) {
        user.connected = false;
        user.controller = null;
        user.rejectReady?.(new Error(`${user.name} : connexion non établie`));
        user.rejectReady = null;
        user.ready = null;
        update();
      }
    }
  })();
  await ready;
  user.rejectReady = null;
  journal(`${user.name} connecté à sa propre session ${sid.slice(0, 8)}.`);
}
async function close() {
  for (const user of users) {
    if (user.session) {
      await request(user, "DELETE", `/agents/sessions/${user.session.id}`);
      user.controller?.abort();
      user.controller = null;
      user.connected = false;
      user.session = null;
      user.card.querySelector(".thinking").hidden = true;
    }
  }
  journal(
    "Sessions privées fermées. Le kernel et ses objets continuent d’exister.",
  );
  update();
}
async function task(action) {
  if (busy) return;
  busy = true;
  update();
  try {
    await action();
  } catch (error) {
    notice(error.message, true);
  } finally {
    busy = false;
    update();
  }
}
async function refreshState() {
  if (!users.length || polling) return;
  polling = true;
  try {
    const view = await request(users[0], "GET", "/app/state");
    $("kernel-label").textContent =
      `${view.kernel_id.slice(-8)} · révision ${view.revision}`;
    if (view.error) notice(view.error, true);
    if (view.revision !== revision || view.catalog?.error !== catalogError) {
      if (revision >= 0)
        journal(
          `Kernel applicatif · état mis à jour (révision ${view.revision}).`,
        );
      revision = view.revision;
      catalogError = view.catalog?.error;
      $("counter-value").textContent = String(view.state?.counter ?? "—");
      $("app-json").textContent = JSON.stringify(view.state, null, 2);
      const catalog = view.catalog;
      $("function-count").textContent = catalog?.error
        ? "Catalogue indisponible"
        : `${catalog?.total || 0} fonction(s) · révision ${view.revision}${catalog?.truncated ? " · liste partielle" : ""}`;
      $("function-list").replaceChildren(
        ...(catalog?.entries?.length
          ? catalog.entries.map((entry) => {
              const item = node("li", "");
              item.append(
                node("code", `${entry.kind === "async_function" ? "async " : ""}${entry.name}${entry.signature}`),
                node("p", entry.description || "Aucune description. Ajoutez une docstring pour faciliter la réutilisation."),
              );
              return item;
            })
          : [node("li", catalog?.error || "Aucune fonction publique définie pour le moment.")]),
      );
      const tasks = Array.isArray(view.state?.tasks) ? view.state.tasks : [];
      $("task-board").replaceChildren(
        ...(tasks.length
          ? tasks.map((item) =>
              node(
                "li",
                `${item.done ? "✓" : "○"} ${item.title || JSON.stringify(item)}`,
                item.done ? "done" : "",
              ),
            )
          : [node("li", "Aucune tâche pour le moment.")]),
      );
    }
  } catch (error) {
    $("kernel-label").textContent = error.message;
  } finally {
    polling = false;
  }
}
async function executeApplication(code) {
  const result = await request(users[0], "POST", "/app/kernel", {
    session_id: users[0].session.id,
    code,
  });
  $("app-output").textContent = JSON.stringify(result, null, 2);
  journal(`Frontend → même kernel Python · ${result.status}.`);
  await refreshState();
}
async function setup() {
  const response = await fetch("/demo/users", {
    headers: { "X-Prime-Agent-Demo": "1" },
    cache: "no-store",
  });
  if (!response.ok)
    throw new Error("Le serveur de démonstration n’est pas disponible.");
  config = await response.json();
  users = config.users.map((identity, index) => {
    const card = $("user-template").content.firstElementChild.cloneNode(true);
    card.querySelector("h2").textContent = identity.name;
    card.querySelector(".avatar").textContent = identity.name[0];
    const textarea = card.querySelector("textarea");
    textarea.id = `message-${index}`;
    card.querySelector("label").htmlFor = textarea.id;
    card.querySelector("label").textContent =
      `Message privé de ${identity.name}`;
    const user = {
      ...identity,
      card,
      messages: card.querySelector(".messages"),
      seen: new Set(),
      connected: false,
      sending: false,
      session: null,
    };
    card.querySelector(".sample").textContent = [
      "Créer une tâche",
      "Modifier la tâche",
      "Lire l’état partagé",
    ][index];
    card.querySelector(".sample").addEventListener("click", () => {
      textarea.value = samples[index];
      textarea.focus();
    });
    card
      .querySelector(".composer")
      .addEventListener("submit", async (event) => {
        event.preventDefault();
        const message = textarea.value.trim();
        if (!message || !user.session || user.sending) return;
        user.sending = true;
        update();
        try {
          await request(
            user,
            "POST",
            `/app/sessions/${user.session.id}/prompts`,
            { text: message },
          );
          textarea.value = "";
          journal(`${user.name} · message admis dans sa session privée.`);
          notice(
            "Le message reste dans cette conversation. Les appels Python peuvent modifier l’état partagé.",
          );
        } catch (error) {
          notice(error.message, true);
        } finally {
          user.sending = false;
          update();
        }
      });
    card.querySelector(".reconnect").addEventListener("click", () =>
      task(async () => {
        if (user.controller) {
          user.controller.abort();
          user.controller = null;
          user.connected = false;
          journal(`${user.name} déconnecté · l’exécution continue.`);
        } else await connect(user);
      }),
    );
    $("users").append(card);
    return user;
  });
  for (const next of ["solo", "shared"])
    $(next).addEventListener("click", () =>
      task(async () => {
        if (mode === next) return;
        await close();
        mode = next;
        for (const user of users)
          user.messages.replaceChildren(
            node("p", "Démarrez les sessions pour commencer.", "empty"),
          );
        notice(
          "Les sessions ont leur propre contexte. Le kernel applicatif conserve les objets communs.",
        );
      }),
    );
  $("start").addEventListener("click", () =>
    task(async () => {
      for (const user of users.slice(0, mode === "solo" ? 1 : 3)) {
        user.session = await request(user, "POST", "/agents/sessions", {
          workspace_id: config.workspace_id,
        });
        await connect(user);
      }
      notice(
        "Sessions privées prêtes. Créez un objet avec Alice, puis modifiez-le avec Bob.",
      );
    }),
  );
  $("close").addEventListener("click", () => task(close));
  $("increment").addEventListener("click", () =>
    task(() =>
      executeApplication("app['counter'] += 1\nprint(app['counter'])"),
    ),
  );
  $("execute-code").addEventListener("click", () =>
    task(() => executeApplication($("app-code").value)),
  );
  update();
  await refreshState();
  setInterval(() => void refreshState(), 1000);
}
setup().catch((error) => {
  notice(error.message, true);
  $("start").disabled = true;
});
