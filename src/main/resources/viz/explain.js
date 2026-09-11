/* explain.js — a plain-English layer over this visualiser.
   Everything below the EXPLAIN block is the shared engine used by every project
   in the portfolio; only this block is specific to the Raft store. */

window.EXPLAIN = {
  id: "raft-kv",
  title: "Consensus, explained",
  subtitle: "What this builds, and what every word on screen means",

  what:
    "This is a database spread across several machines that keeps working when some of them die. " +
    "Every node on screen is real — you can crash one, cut the network in half, and watch the " +
    "survivors carry on agreeing with each other.",

  why:
    "Getting several machines to agree on the order of events, when any of them can crash or be " +
    "cut off at any moment, is one of the genuinely hard problems in computing. The naive " +
    "approaches all fail the same way: two halves of a split network each decide they are in " +
    "charge, both accept writes, and the data silently diverges.",

  uses: [
    "Click a node to crash it, then restart it and watch it catch up on what it missed.",
    "Isolate some nodes from the rest and see which side can still accept writes.",
    "Propose a write and follow it replicating across the cluster, entry by entry.",
  ],

  honest:
    "The whole cluster runs on an injectable clock and a virtual network, so the tests are exact " +
    "and contain no sleeps. Client histories recorded under partitions, crashes and packet loss " +
    "were machine-checked for correctness across multiple random seeds.",

  steps: [
    {
      title: "One node is elected leader",
      body:
        "Nodes wait a random amount of time; the first to get bored asks the others to vote for " +
        "it. Winning needs a majority, which is what stops two leaders being elected at once.",
    },
    {
      title: "All writes go through the leader",
      body:
        "The leader appends the write to its log and sends it to everyone else. Followers do not " +
        "accept writes directly — a single ordering point is what makes the result consistent.",
    },
    {
      title: "A majority makes it official",
      body:
        "Once more than half the nodes have stored the entry, it is committed and can be shown to " +
        "clients. A minority can never commit anything, which is exactly why a split network " +
        "cannot produce two conflicting truths.",
    },
    {
      title: "Failures heal themselves",
      body:
        "A crashed node restarts and is sent whatever it missed. A leader that gets cut off is " +
        "replaced by the side that still has a majority, and steps down when it rejoins.",
    },
  ],

  glossary: {
    "Raft": {
      short: "The agreement protocol implemented here, designed to be understandable where earlier ones were not.",
      why: "It is what runs underneath etcd and Consul — the machinery behind Kubernetes' own state store.",
    },
    "consensus": {
      short: "Several machines agreeing on the same sequence of events despite failures.",
    },
    "node": {
      short: "One server in the cluster.",
    },
    "cluster": {
      short: "The whole group of nodes working together as one logical database.",
    },
    "leader": {
      short: "The single node currently allowed to accept writes.",
      why: "Funnelling every write through one node is what gives the cluster a single agreed order of events.",
    },
    "follower": {
      short: "A node that accepts entries from the leader and stores them.",
    },
    "candidate": {
      short: "A node that has given up waiting and is asking the others to elect it.",
    },
    "term": {
      short: "A numbered period with at most one leader. Every election starts a new one.",
      why: "It is how nodes tell a current leader from a stale one that has been away and does not know it was replaced.",
    },
    "election": {
      short: "The vote that picks a new leader after the old one stops responding.",
    },
    "majority": {
      short: "More than half the nodes. Three of five, two of three.",
      why: "Two different majorities must overlap in at least one node, which is the whole reason two conflicting decisions cannot both stand.",
    },
    "quorum": {
      short: "Another word for the majority needed before a decision counts.",
    },
    "log": {
      short: "The ordered list of writes each node stores. Every node's log should end up identical.",
    },
    "entry": {
      short: "One write in the log.",
    },
    "committed": {
      short: "Stored by a majority, and therefore permanent and safe to show a client.",
      why: "Before that point a write can still be lost. After it, it survives any minority of failures.",
    },
    "replication": {
      short: "Copying the leader's log to the followers.",
    },
    "partition": {
      short: "A network fault that splits the cluster into groups that cannot talk to each other.",
      why: "The classic hard case: both halves are alive and each may believe the other has died.",
    },
    "packet loss": {
      short: "Messages disappearing in transit — normal on real networks.",
    },
    "snapshot": {
      short: "A compacted copy of the state, so the log can be truncated instead of growing forever.",
    },
    "tick": {
      short: "One step of the simulated clock driving the cluster.",
      why: "Time is injected rather than real, so tests are exact and instant instead of full of sleeps.",
    },
    "linearizable": {
      short: "Behaves exactly as if there were one single machine handling requests one at a time.",
      why: "The strongest correctness guarantee available, and the one actually checked here against recorded histories.",
    },
    "state machine": {
      short: "The key-value store itself — it applies committed entries in order to produce the current data.",
    },
  },
};
/* ------------------------------------------------------------------------
   The engine. Identical in every project in the portfolio; only the EXPLAIN
   block above it changes.

   Three jobs:
     1. A "What is this?" button that opens a plain-English panel.
     2. Underlining every glossary term where it already appears on the page,
        so a definition is one click away from the word itself.
     3. Keeping all of that out of the way of the dashboard it is explaining.

   No dependencies, no build step, no network. It injects its own styles so a
   project only has to add one <script defer> tag.
   ------------------------------------------------------------------------ */
(function () {
  "use strict";

  if (!window.EXPLAIN) return;
  const DATA = window.EXPLAIN;
  const KEY = "explain-seen:" + (DATA.id || location.pathname);

  const esc = s => String(s).replace(/[&<>"]/g,
    c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

  /* ---------------- styles ---------------- */
  const style = document.createElement("style");
  style.textContent = `
  .xp-fab {
    position: fixed; right: 20px; bottom: 20px; z-index: 9998;
    display: inline-flex; align-items: center; gap: 8px;
    padding: 10px 16px; border-radius: 999px; cursor: pointer;
    background: #1d4ed8; color: #fff; border: 1px solid #3b82f6;
    font: 600 13px/1 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    box-shadow: 0 6px 22px rgba(0,0,0,.45); transition: transform .12s, background .12s;
  }
  .xp-fab:hover { background: #2563eb; transform: translateY(-1px); }
  .xp-fab .xp-dot { width: 7px; height: 7px; border-radius: 50%; background: #93c5fd; }

  .xp-scrim {
    position: fixed; inset: 0; z-index: 9998; background: rgba(0,0,0,.5);
    opacity: 0; pointer-events: none; transition: opacity .18s;
  }
  .xp-scrim.on { opacity: 1; pointer-events: auto; }

  .xp-panel {
    position: fixed; top: 0; right: 0; bottom: 0; width: min(520px, 100%);
    z-index: 9999; background: #0f141b; border-left: 1px solid #223041;
    transform: translateX(100%); transition: transform .22s ease;
    display: flex; flex-direction: column;
    font: 14px/1.6 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    color: #e6edf3;
  }
  .xp-panel.on { transform: translateX(0); }

  /* Each project styles its own headings, and those rules would otherwise reach inside this
     panel -- one dashboard uppercases every h2, another spaces its letters out. Reset the
     properties that travel, so the explainer looks the same in all seven repos. */
  .xp-panel h2, .xp-panel h3, .xp-panel p, .xp-panel div, .xp-panel input {
    text-transform: none; letter-spacing: normal; font-family: inherit;
  }
  .xp-panel h2, .xp-panel h3 { font-weight: 600; }
  .xp-panel * { box-sizing: border-box; }

  .xp-head {
    padding: 18px 22px 14px; border-bottom: 1px solid #223041;
    display: flex; align-items: flex-start; gap: 12px;
  }
  .xp-head h2 { margin: 0 0 3px; font-size: 16px; }
  .xp-head p { margin: 0; color: #8b9bb0; font-size: 12.5px; }
  .xp-close {
    margin-left: auto; background: none; border: 1px solid #223041; color: #8b9bb0;
    width: 28px; height: 28px; border-radius: 7px; cursor: pointer; font-size: 15px; line-height: 1;
  }
  .xp-close:hover { color: #e6edf3; border-color: #3b4c63; }

  .xp-tabs { display: flex; gap: 6px; padding: 12px 22px 0; }
  .xp-tab {
    padding: 6px 12px; border-radius: 7px 7px 0 0; cursor: pointer; font-size: 12.5px;
    color: #8b9bb0; border: 1px solid transparent; border-bottom: none;
  }
  .xp-tab:hover { color: #e6edf3; }
  .xp-tab.on { color: #93c5fd; background: #131b24; border-color: #223041; }

  .xp-body { padding: 18px 22px 28px; overflow-y: auto; flex: 1; }
  .xp-body h3 {
    font-size: 11px; text-transform: uppercase; letter-spacing: .09em;
    color: #64748b; margin: 22px 0 8px; font-weight: 600;
  }
  .xp-body h3:first-child { margin-top: 0; }
  .xp-lede { font-size: 14.5px; color: #dbe6f0; margin: 0 0 4px; }
  .xp-note {
    margin-top: 14px; padding: 11px 13px; border-left: 3px solid #d29922;
    background: #d2992212; border-radius: 0 7px 7px 0; color: #e8d8b0; font-size: 13px;
  }

  .xp-step { display: flex; gap: 12px; margin-bottom: 14px; }
  .xp-step .n {
    flex: 0 0 24px; height: 24px; border-radius: 50%; background: #1d4ed8; color: #fff;
    display: grid; place-items: center; font-size: 12px; font-weight: 700;
  }
  .xp-step .t { font-weight: 600; margin-bottom: 1px; }
  .xp-step .b { color: #9fb0c4; font-size: 13px; }

  .xp-search {
    width: 100%; padding: 8px 11px; border-radius: 8px; margin-bottom: 14px;
    background: #0b1119; border: 1px solid #223041; color: #e6edf3; font-size: 13px;
  }
  .xp-term { padding: 11px 0; border-bottom: 1px solid #1a2330; }
  .xp-term:last-child { border-bottom: none; }
  .xp-term .k {
    font: 600 13px ui-monospace, SFMono-Regular, Menlo, monospace; color: #93c5fd;
  }
  .xp-term .s { color: #dbe6f0; font-size: 13.5px; margin-top: 2px; }
  .xp-term .w { color: #8b9bb0; font-size: 12.5px; margin-top: 4px; }
  .xp-term .w b { color: #b8c7d9; font-weight: 600; }
  .xp-empty { color: #64748b; font-style: italic; font-size: 13px; }

  /* the inline term markers */
  .xp-mark {
    border-bottom: 1px dashed #4b6b93; cursor: help;
    text-decoration: none; color: inherit;
  }
  .xp-mark:hover { border-bottom-color: #93c5fd; background: #1d4ed822; }

  .xp-pop {
    position: absolute; z-index: 10000; max-width: 320px; padding: 11px 13px;
    background: #111923; border: 1px solid #2c3e54; border-radius: 9px;
    box-shadow: 0 10px 30px rgba(0,0,0,.5); color: #e6edf3;
    font: 13px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  .xp-pop .k {
    font: 600 12px ui-monospace, SFMono-Regular, Menlo, monospace;
    color: #93c5fd; display: block; margin-bottom: 3px;
  }
  .xp-pop .w { color: #8b9bb0; font-size: 12.5px; margin-top: 5px; }

  @media print { .xp-fab, .xp-panel, .xp-scrim, .xp-pop { display: none !important; } }
  `;
  document.head.appendChild(style);

  /* ---------------- panel ---------------- */
  const scrim = document.createElement("div");
  scrim.className = "xp-scrim";

  const panel = document.createElement("aside");
  panel.className = "xp-panel";
  panel.innerHTML = `
    <div class="xp-head">
      <div>
        <h2>${esc(DATA.title || "What is this?")}</h2>
        <p>${esc(DATA.subtitle || "In plain English")}</p>
      </div>
      <button class="xp-close" aria-label="Close">&times;</button>
    </div>
    <div class="xp-tabs">
      <div class="xp-tab on" data-tab="what">What &amp; why</div>
      <div class="xp-tab" data-tab="how">How it works</div>
      <div class="xp-tab" data-tab="words">Every word</div>
    </div>
    <div class="xp-body"></div>`;

  const body = panel.querySelector(".xp-body");

  function renderWhat() {
    const uses = (DATA.uses || []).map(u => `<div class="xp-step">
        <div class="n">✓</div><div><div class="b">${esc(u)}</div></div></div>`).join("");
    body.innerHTML = `
      <h3>What this is</h3>
      <p class="xp-lede">${esc(DATA.what || "")}</p>
      ${DATA.why ? `<h3>Why it is hard</h3><p class="xp-lede">${esc(DATA.why)}</p>` : ""}
      ${uses ? `<h3>What you can do here</h3>${uses}` : ""}
      ${DATA.honest ? `<div class="xp-note">${esc(DATA.honest)}</div>` : ""}`;
  }

  function renderHow() {
    body.innerHTML = "<h3>Step by step</h3>" + (DATA.steps || []).map((s, i) => `
      <div class="xp-step">
        <div class="n">${i + 1}</div>
        <div><div class="t">${esc(s.title)}</div><div class="b">${esc(s.body)}</div></div>
      </div>`).join("");
  }

  function renderWords(filter) {
    const entries = Object.entries(DATA.glossary || {});
    const q = (filter || "").trim().toLowerCase();
    const hits = entries.filter(([k, v]) =>
      !q || k.toLowerCase().includes(q) || (v.short + " " + (v.why || "")).toLowerCase().includes(q));

    body.innerHTML = `
      <input class="xp-search" placeholder="Filter ${entries.length} terms…" value="${esc(filter || "")}">
      ${hits.length ? hits.map(([k, v]) => `
        <div class="xp-term">
          <div class="k">${esc(k)}</div>
          <div class="s">${esc(v.short)}</div>
          ${v.why ? `<div class="w"><b>Why it matters:</b> ${esc(v.why)}</div>` : ""}
        </div>`).join("")
      : `<p class="xp-empty">Nothing matches “${esc(filter)}”.</p>`}`;

    const input = body.querySelector(".xp-search");
    input.addEventListener("input", e => {
      const pos = e.target.selectionStart;
      renderWords(e.target.value);
      const next = body.querySelector(".xp-search");
      next.focus();
      next.setSelectionRange(pos, pos);
    });
  }

  const views = { what: renderWhat, how: renderHow, words: () => renderWords("") };

  panel.querySelectorAll(".xp-tab").forEach(tab =>
    tab.addEventListener("click", () => {
      panel.querySelectorAll(".xp-tab").forEach(t => t.classList.toggle("on", t === tab));
      views[tab.dataset.tab]();
    }));

  function open(tab) {
    if (tab) {
      panel.querySelectorAll(".xp-tab").forEach(t => t.classList.toggle("on", t.dataset.tab === tab));
      views[tab]();
    }
    panel.classList.add("on");
    scrim.classList.add("on");
    try { localStorage.setItem(KEY, "1"); } catch (e) { /* private mode */ }
  }
  function close() {
    panel.classList.remove("on");
    scrim.classList.remove("on");
  }

  panel.querySelector(".xp-close").addEventListener("click", close);
  scrim.addEventListener("click", close);
  document.addEventListener("keydown", e => { if (e.key === "Escape") { close(); hidePop(); } });

  const fab = document.createElement("button");
  fab.className = "xp-fab";
  fab.innerHTML = `<span class="xp-dot"></span>What is this?`;
  fab.addEventListener("click", () => open("what"));

  /* ---------------- inline term markers ---------------- */
  let pop = null;
  function hidePop() { if (pop) { pop.remove(); pop = null; } }

  function showPop(target, term, entry) {
    hidePop();
    pop = document.createElement("div");
    pop.className = "xp-pop";
    pop.innerHTML = `<span class="k">${esc(term)}</span>${esc(entry.short)}
      ${entry.why ? `<div class="w"><b>Why it matters:</b> ${esc(entry.why)}</div>` : ""}`;
    document.body.appendChild(pop);

    const r = target.getBoundingClientRect();
    const top = r.bottom + window.scrollY + 8;
    let left = r.left + window.scrollX;
    // Keep it on screen when the term sits near the right edge.
    left = Math.min(left, window.scrollX + document.documentElement.clientWidth - pop.offsetWidth - 12);
    pop.style.top = top + "px";
    pop.style.left = Math.max(8, left) + "px";
  }

  document.addEventListener("click", e => {
    const mark = e.target.closest?.(".xp-mark");
    if (!mark) { hidePop(); return; }
    e.preventDefault();
    const term = mark.dataset.term;
    showPop(mark, term, DATA.glossary[term]);
  });

  /* Wrap the first occurrence of each glossary term in ordinary page text.
     Skips anything inside SVG, form controls, scripts and this panel -- rewriting
     those would break the charts and inputs the dashboard depends on. */
  function markTerms() {
    const terms = Object.keys(DATA.glossary || {}).sort((a, b) => b.length - a.length);
    if (!terms.length) return;
    const done = new Set();
    const SKIP = new Set(["SCRIPT", "STYLE", "TEXTAREA", "INPUT", "SELECT", "OPTION", "BUTTON"]);

    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        if (!node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        let el = node.parentElement;
        while (el) {
          if (SKIP.has(el.tagName)) return NodeFilter.FILTER_REJECT;
          if (el.namespaceURI === "http://www.w3.org/2000/svg") return NodeFilter.FILTER_REJECT;
          if (el.classList && (el.classList.contains("xp-panel") ||
              el.classList.contains("xp-pop") || el.classList.contains("xp-fab") ||
              el.classList.contains("xp-mark"))) return NodeFilter.FILTER_REJECT;
          el = el.parentElement;
        }
        return NodeFilter.FILTER_ACCEPT;
      }
    });

    const targets = [];
    let n;
    while ((n = walker.nextNode())) targets.push(n);

    for (const node of targets) {
      for (const term of terms) {
        if (done.has(term)) continue;
        const re = new RegExp(`\\b${term.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i");
        const m = re.exec(node.nodeValue);
        if (!m) continue;

        const after = node.splitText(m.index);
        after.nodeValue = after.nodeValue.slice(m[0].length);
        const mark = document.createElement("span");
        mark.className = "xp-mark";
        mark.dataset.term = term;
        mark.textContent = m[0];
        after.parentNode.insertBefore(mark, after);
        done.add(term);
        break; // this text node is now split; move on rather than rescanning it
      }
    }
  }

  function boot() {
    document.body.appendChild(scrim);
    document.body.appendChild(panel);
    document.body.appendChild(fab);
    renderWhat();
    try { markTerms(); } catch (e) { /* never break the dashboard over a tooltip */ }

    // #explain, #explain=how, #explain=words -- a shareable link straight to the explanation,
    // which is also what makes the panel screenshottable for the README.
    const hash = /^#explain(?:=(\w+))?$/.exec(location.hash);
    if (hash) {
      open(views[hash[1]] ? hash[1] : "what");
      return;
    }

    let seen = null;
    try { seen = localStorage.getItem(KEY); } catch (e) { /* private mode */ }
    if (!seen) setTimeout(() => open("what"), 600);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
