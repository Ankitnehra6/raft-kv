/* Raft visualiser.
 *
 * This file draws; it does not decide anything. Every role, term, log entry and message on
 * screen comes from a real RaftNode on the server. A visualisation that re-implemented the
 * algorithm in JavaScript would be a drawing of Raft — this is Raft, rendered.
 */

const SVG_NS = "http://www.w3.org/2000/svg";
const el = (id) => document.getElementById(id);

const view = {
  state: null,
  running: false,
  timer: null,
  partitionSelection: new Set(),
};

/* --- server ------------------------------------------------------------------ */

async function call(path, params = {}) {
  const query = new URLSearchParams(params).toString();
  const response = await fetch(path + (query ? "?" + query : ""), { cache: "no-store" });
  if (!response.ok) throw new Error(path + " returned " + response.status);
  view.state = await response.json();
  render();
}

/* --- geometry ---------------------------------------------------------------- */

const WIDTH = 640;
const HEIGHT = 360;
const CENTRE = { x: WIDTH / 2, y: HEIGHT / 2 };
const ORBIT = 128;
const NODE_RADIUS = 31;

/** Nodes are laid out on a circle, first one at the top. */
function positionOf(index, count) {
  const angle = (index / count) * Math.PI * 2 - Math.PI / 2;
  return {
    x: CENTRE.x + ORBIT * Math.cos(angle),
    y: CENTRE.y + ORBIT * Math.sin(angle),
  };
}

function positions(nodes) {
  const map = new Map();
  nodes.forEach((node, i) => map.set(node.id, positionOf(i, nodes.length)));
  return map;
}

function colourFor(node) {
  if (node.crashed) return "var(--crashed)";
  return { LEADER: "var(--leader)", CANDIDATE: "var(--candidate)" }[node.role] ?? "var(--follower)";
}

/* --- rendering ---------------------------------------------------------------- */

function render() {
  const state = view.state;
  if (!state) return;

  el("tick").textContent = state.tick;
  el("seed").textContent = state.seed;
  el("inflight").textContent = state.messages.length;

  const leaders = state.nodes.filter((n) => n.role === "LEADER" && !n.crashed);
  // Several leaders means a split brain, which is worth showing rather than hiding
  // behind whichever one happens to sort first.
  el("leader").textContent =
    leaders.length === 1 ? leaders[0].id : leaders.length === 0 ? "none" : leaders.map((n) => n.id).join(" + ");
  el("term").textContent = Math.max(0, ...state.nodes.map((n) => n.term));

  drawCluster(state);
  drawLogs(state);
  drawEvents(state);
  drawPartitionChips(state);
}

function drawCluster(state) {
  const svg = el("cluster");
  svg.replaceChildren();

  const where = positions(state.nodes);
  const byId = new Map(state.nodes.map((n) => [n.id, n]));

  // Links first, so nodes and messages draw over them.
  for (let i = 0; i < state.nodes.length; i++) {
    for (let j = i + 1; j < state.nodes.length; j++) {
      const a = state.nodes[i];
      const b = state.nodes[j];
      const cut = a.crashed || b.crashed || a.partitionGroup !== b.partitionGroup;

      svg.appendChild(
        line(where.get(a.id), where.get(b.id), cut ? "link link-cut" : "link")
      );
    }
  }

  // Messages, positioned by how far along their delay they are.
  for (const message of state.messages) {
    const from = where.get(message.from);
    const to = where.get(message.to);
    if (!from || !to) continue;

    const t = message.progress;
    const dot = document.createElementNS(SVG_NS, "circle");
    dot.setAttribute("cx", from.x + (to.x - from.x) * t);
    dot.setAttribute("cy", from.y + (to.y - from.y) * t);
    dot.setAttribute("class", "msg msg-" + message.kind);
    dot.appendChild(title(`${message.from} → ${message.to}: ${message.kind} (term ${message.term})`));
    svg.appendChild(dot);
  }

  // Nodes.
  for (const node of state.nodes) {
    const at = where.get(node.id);
    const group = document.createElementNS(SVG_NS, "g");

    const circle = document.createElementNS(SVG_NS, "circle");
    circle.setAttribute("cx", at.x);
    circle.setAttribute("cy", at.y);
    circle.setAttribute("r", NODE_RADIUS);
    circle.setAttribute("class", "node-circle");
    circle.setAttribute("fill", node.crashed ? "transparent" : colourFor(node));
    circle.setAttribute("stroke", colourFor(node));
    circle.setAttribute("stroke-width", node.role === "LEADER" ? 3 : 1.5);
    if (node.crashed) circle.setAttribute("stroke-dasharray", "5 4");

    circle.addEventListener("click", () =>
      call(node.crashed ? "/api/restart" : "/api/crash", { node: node.id })
    );
    circle.appendChild(
      title(
        `${node.id} — ${node.crashed ? "DOWN" : node.role}\n` +
          `term ${node.term}, commit ${node.commitIndex}, log ${node.lastIndex}\n` +
          `${node.crashed ? "click to restart" : "click to crash"}`
      )
    );
    group.appendChild(circle);

    group.appendChild(
      text(at.x, at.y - 2, node.id, "node-label", node.crashed ? colourFor(node) : "#08131a")
    );
    group.appendChild(
      text(
        at.x,
        at.y + 13,
        `t${node.term}`,
        "node-sub",
        node.crashed ? "var(--text-faint)" : "#08131a"
      )
    );

    svg.appendChild(group);
  }
}

function line(from, to, className) {
  const element = document.createElementNS(SVG_NS, "line");
  element.setAttribute("x1", from.x);
  element.setAttribute("y1", from.y);
  element.setAttribute("x2", to.x);
  element.setAttribute("y2", to.y);
  element.setAttribute("class", className);
  return element;
}

function text(x, y, content, className, fill) {
  const element = document.createElementNS(SVG_NS, "text");
  element.setAttribute("x", x);
  element.setAttribute("y", y);
  element.setAttribute("text-anchor", "middle");
  element.setAttribute("class", className);
  element.setAttribute("fill", fill);
  element.textContent = content;
  return element;
}

function title(content) {
  const element = document.createElementNS(SVG_NS, "title");
  element.textContent = content;
  return element;
}

function drawLogs(state) {
  const container = el("logs");
  container.replaceChildren();

  // Every track is the same width, so lagging replicas are visible as gaps rather than
  // as shorter rows that look deliberate.
  const width = Math.max(1, state.maxLogIndex);

  for (const node of state.nodes) {
    const row = document.createElement("div");
    row.className = "log-row";

    const name = document.createElement("div");
    name.className = "log-name";
    name.innerHTML = `<span class="dot" style="background:${colourFor(node)}"></span>${node.id}`;
    row.appendChild(name);

    const track = document.createElement("div");
    track.className = "log-track";

    const entries = new Map(node.log.map((e) => [e.index, e]));

    for (let index = 1; index <= width; index++) {
      const cell = document.createElement("div");
      cell.className = "log-cell";

      if (index <= node.snapshotIndex) {
        // Compacted away: the entry existed, its contents are in a snapshot.
        cell.classList.add("snapshot");
        cell.title = `index ${index} — folded into a snapshot`;
      } else if (entries.has(index)) {
        const entry = entries.get(index);
        cell.textContent = entry.term;
        if (entry.committed) cell.classList.add("committed");
        if (entry.type === "CONFIGURATION") cell.classList.add("config");
        cell.title = `index ${index}, term ${entry.term}, ${entry.type}${entry.committed ? " — committed" : ""}`;
      } else {
        cell.style.opacity = 0.25;
        cell.title = `index ${index} — not replicated here yet`;
      }
      track.appendChild(cell);
    }

    row.appendChild(track);
    container.appendChild(row);
  }
}

function drawEvents(state) {
  const container = el("events");
  if (!state.events.length) {
    container.innerHTML = '<p class="empty">Press Run.</p>';
    return;
  }
  container.replaceChildren();
  for (const event of state.events) {
    const line = document.createElement("div");
    line.className = "event-line";
    line.textContent = event;
    container.appendChild(line);
  }
}

function drawPartitionChips(state) {
  const container = el("partition-chips");
  container.replaceChildren();

  // Reflect the server's actual partition rather than only what was clicked here: the
  // cluster can be split by any client, and chips showing "nothing selected" over a live
  // partition would be the UI lying about the state it exists to display.
  const groups = new Set(state.nodes.map((n) => n.partitionGroup));
  if (groups.size > 1) {
    view.partitionSelection = new Set(
      state.nodes.filter((n) => n.partitionGroup === 1).map((n) => n.id)
    );
  } else if (groups.size === 1) {
    view.partitionSelection.clear();
  }

  for (const node of state.nodes) {
    const chip = document.createElement("span");
    chip.className = "chip" + (view.partitionSelection.has(node.id) ? " selected" : "");
    chip.textContent = node.id;
    chip.addEventListener("click", () => {
      if (view.partitionSelection.has(node.id)) view.partitionSelection.delete(node.id);
      else view.partitionSelection.add(node.id);
      applyPartition();
    });
    container.appendChild(chip);
  }
}

function applyPartition() {
  const group = [...view.partitionSelection];
  // An empty or total selection is not a partition; heal instead of sending a no-op.
  if (group.length === 0 || group.length === view.state.nodes.length) {
    call("/api/heal");
  } else {
    call("/api/partition", { group: group.join(",") });
  }
}

/* --- running ------------------------------------------------------------------- */

function setRunning(running) {
  view.running = running;
  const button = el("play");
  button.textContent = running ? "⏸ Pause" : "▶ Run";
  button.classList.toggle("running", running);

  clearInterval(view.timer);
  if (running) {
    const ticksPerSecond = Number(el("speed").value);
    view.timer = setInterval(() => call("/api/step", { ticks: 1 }), 1000 / ticksPerSecond);
  }
}

/* --- wiring --------------------------------------------------------------------- */

el("play").addEventListener("click", () => setRunning(!view.running));
el("step").addEventListener("click", () => call("/api/step", { ticks: 1 }));
el("step10").addEventListener("click", () => call("/api/step", { ticks: 10 }));

el("speed").addEventListener("input", (e) => {
  el("speed-out").textContent = e.target.value + " ticks/s";
  if (view.running) setRunning(true); // restart the timer at the new rate
});

el("propose").addEventListener("click", () =>
  call("/api/propose", { key: "k" + Math.floor(Math.random() * 3), value: "v" + Date.now() % 1000 })
);

el("heal").addEventListener("click", () => {
  view.partitionSelection.clear();
  call("/api/heal");
});

el("drop").addEventListener("input", (e) => {
  el("drop-out").textContent = e.target.value + "%";
  call("/api/network", { drop: Number(e.target.value) / 100 });
});

el("delay").addEventListener("input", (e) => {
  const value = Number(e.target.value);
  el("delay-out").textContent = value + (value === 1 ? " tick" : " ticks");
  call("/api/network", { maxDelay: value });
});

el("reset").addEventListener("click", () => {
  view.partitionSelection.clear();
  setRunning(false);
  call("/api/reset", { size: el("size").value, seed: Date.now() });
});

call("/api/state").then(() => setRunning(true));
