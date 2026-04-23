# Reusable chatbot UI — served at GET /embed and GET /embed/chat (same origin as the API).
# Load ?session_id=<uuid> after schema activation in the main app.
#
# Default API base for the embed form comes from the same env as the Streamlit app
# (``NL_SQL_API_URL`` / ``API_URL``); use :func:`get_embed_html` so it is not hardcoded.

from __future__ import annotations

import json

EMBED_PAGE_TITLE = "NL → SQL — embed"

EMBED_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>NL → SQL (embed)</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, -apple-system, sans-serif; margin: 0; padding: 0; background: #0b1220; color: #e2e8f0; min-height: 100vh; display: flex; flex-direction: column; }
    .wrap { max-width: 720px; margin: 0 auto; padding: 12px; flex: 1; display: flex; flex-direction: column; min-height: 0; }
    h1 { font-size: 1.05rem; margin: 0 0 4px 0; font-weight: 600; }
    .sub { font-size: 0.8rem; color: #94a3b8; margin: 0 0 8px 0; }
    details { font-size: 0.8rem; color: #94a3b8; margin-bottom: 8px; }
    details summary { cursor: pointer; color: #cbd5e1; }
    label { display: block; font-size: 0.7rem; color: #94a3b8; margin: 6px 0 2px; }
    input, textarea, button, select { width: 100%; padding: 8px; border-radius: 8px; border: 1px solid #334155; background: #0f172a; color: #e2e8f0; font-size: 0.9rem; }
    textarea#input { min-height: 48px; resize: vertical; }
    .row2 { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
    button#send { margin-top: 8px; cursor: pointer; background: #1d4ed8; border-color: #2563eb; font-weight: 600; }
    button#send:disabled, button#clear:disabled { opacity: 0.5; cursor: not-allowed; }
    button#clear { margin-top: 8px; background: #1e293b; border-color: #334155; font-weight: 500; }
    #thread { flex: 1; overflow-y: auto; min-height: 200px; max-height: min(60vh, 480px); margin: 8px 0; padding: 8px; background: #0f172a; border-radius: 10px; border: 1px solid #1e293b; }
    .msg { margin-bottom: 12px; }
    .msg.user .bubble { background: #1e3a5f; border: 1px solid #2563eb; margin-left: 24px; border-radius: 10px 10px 2px 10px; padding: 8px 10px; }
    .msg.asst .bubble { background: #111827; border: 1px solid #334155; margin-right: 16px; border-radius: 10px 10px 10px 2px; padding: 8px 10px; }
    .msg .role { font-size: 0.65rem; color: #64748b; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.04em; }
    .msg pre.sql { background: #020617; padding: 8px; border-radius: 6px; overflow: auto; max-height: 180px; font-size: 0.75rem; border: 1px solid #1e293b; margin: 8px 0 0 0; white-space: pre-wrap; word-break: break-word; }
    .msg table { width: 100%; border-collapse: collapse; font-size: 0.72rem; margin-top: 8px; }
    .msg th, .msg td { border: 1px solid #334155; padding: 4px 6px; text-align: left; }
    .msg th { background: #1e293b; }
    .msg .meta { font-size: 0.7rem; color: #94a3b8; margin-top: 6px; }
    .err { color: #fca5a5; font-size: 0.85rem; margin: 4px 0; }
    .foot { font-size: 0.7rem; color: #64748b; margin-top: 6px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>NL → SQL</h1>
    <p class="sub">Reusable component — works in an iframe or via <code>POST /generate-sql</code>. Set <code>session_id</code> for a project that has completed <strong>Module 1: Configuration</strong> (schema activated).</p>
    <details>
      <summary>Settings (API base, session, limits)</summary>
      <label>API base</label>
      <input id="api" type="text" value="" placeholder="https://api.example.com" />
      <label>session_id</label>
      <input id="sid" type="text" value="" placeholder="project NL session UUID" />
      <div class="row2">
        <div>
          <label>top_k (1–10)</label>
          <input id="topk" type="number" min="1" max="10" value="3" />
        </div>
        <div>
          <label>row_limit (1–1000)</label>
          <input id="rowl" type="number" min="1" max="1000" value="20" />
        </div>
      </div>
    </details>
    <div id="err" class="err" aria-live="polite"></div>
    <div id="thread" role="log" aria-relevant="additions"></div>
    <label for="input">Your question</label>
    <textarea id="input" placeholder="e.g. What was total revenue last month?" rows="2"></textarea>
    <button type="button" id="send">Send — generate &amp; run SQL</button>
    <button type="button" id="clear">Clear thread</button>
    <p class="foot">Same-origin: no CORS. Other hosts: configure <code>CORS_ALLOWED_ORIGINS</code> and call the API from your app server when possible.</p>
  </div>
  <script>
  (function () {
    function esc(s) {
      if (s == null) return "";
      return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    }
    function buildTable(cols, rows) {
      if (!cols || !cols.length || !rows || !rows.length) return "<p class=\"meta\">No rows.</p>";
      var h = "<table><thead><tr>";
      for (var c = 0; c < cols.length; c++) h += "<th>" + esc(cols[c]) + "</th>";
      h += "</tr></thead><tbody>";
      var lim = Math.min(rows.length, 100);
      for (var r = 0; r < lim; r++) {
        h += "<tr>";
        for (var c = 0; c < cols.length; c++) {
          var v = rows[r][cols[c]];
          h += "<td>" + esc(v === undefined ? "" : (typeof v === "object" ? JSON.stringify(v) : v)) + "</td>";
        }
        h += "</tr>";
      }
      h += "</tbody></table>";
      if (rows.length > lim) h += "<p class=\"meta\">Showing " + lim + " of " + rows.length + " row(s) in this view.</p>";
      return h;
    }
    var api = document.getElementById("api");
    var sid = document.getElementById("sid");
    var topk = document.getElementById("topk");
    var rowl = document.getElementById("rowl");
    var input = document.getElementById("input");
    var thread = document.getElementById("thread");
    var err = document.getElementById("err");
    var send = document.getElementById("send");
    var clear = document.getElementById("clear");
    // Same protocol + host as this page when served over http(s); else use server-provided default (matches .env)
    (function setDefaultApi() {
      var p = (window.location && window.location.protocol) || "";
      if (p === "http:" || p === "https:") {
        api.value = window.location.origin;
      } else {
        api.value = __NL_SQL_DEFAULT_API_INJECT__;
      }
    })();
    var usp = new URLSearchParams(window.location.search);
    if (usp.get("session_id")) sid.value = usp.get("session_id");
    clear.onclick = function () {
      thread.innerHTML = "";
      err.textContent = "";
    };
    function appendUser(text) {
      var d = document.createElement("div");
      d.className = "msg user";
      d.innerHTML = "<div class=\"role\">You</div><div class=\"bubble\">" + esc(text) + "</div>";
      thread.appendChild(d);
      thread.scrollTop = thread.scrollHeight;
    }
    function appendAsst(j, ok) {
      var d = document.createElement("div");
      d.className = "msg asst";
      var inner = "<div class=\"role\">Assistant</div><div class=\"bubble\">";
      if (!ok) {
        inner += "<p class=\"err\">" + esc(j && (j.detail || j.message) ? (j.detail || j.message) : JSON.stringify(j)) + "</p>";
      } else {
        inner += "<p>" + esc(j.explanation || "") + "</p>";
        inner += "<pre class=\"sql\">" + esc(j.sql || "") + "</pre>";
        inner += buildTable(j.columns, j.rows);
        var ms = j.execution_ms != null ? j.execution_ms : "—";
        var rc = j.row_count != null ? j.row_count : "—";
        var tc = j.total_count != null ? j.total_count : "—";
        inner += "<p class=\"meta\">" + esc(ms) + " ms · rows returned: " + esc(rc) + " · total: " + esc(tc);
        if (j.tables_used && j.tables_used.length) inner += " · tables: " + esc(j.tables_used.join(", "));
        inner += "</p>";
      }
      inner += "</div>";
      d.innerHTML = inner;
      thread.appendChild(d);
      thread.scrollTop = thread.scrollHeight;
    }
    function doSend() {
      err.textContent = "";
      var base = (api.value || "").replace(/\\/$/, "");
      var s = (sid.value || "").trim();
      var prompt = (input.value || "").trim();
      var tk = parseInt(String(topk.value || "3"), 10) || 3;
      var rl = parseInt(String(rowl.value || "20"), 10) || 20;
      if (tk < 1) tk = 1;
      if (tk > 10) tk = 10;
      if (rl < 1) rl = 1;
      if (rl > 1000) rl = 1000;
      if (!base || !s || prompt.length < 3) {
        err.textContent = "Set API base, session_id, and a question (3+ characters).";
        return;
      }
      appendUser(prompt);
      input.value = "";
      send.disabled = true;
      clear.disabled = true;
      fetch(base + "/generate-sql", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ prompt: prompt, session_id: s, top_k: tk, row_limit: rl, offset: 0 })
      })
      .then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j, status: r.status }; }); })
      .then(function (x) {
        send.disabled = false;
        clear.disabled = false;
        appendAsst(x.j, x.ok);
      })
      .catch(function (e) {
        send.disabled = false;
        clear.disabled = false;
        err.textContent = String(e);
      });
    }
    send.onclick = doSend;
    input.addEventListener("keydown", function (e) {
      if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        doSend();
      }
    });
  })();
  </script>
</body>
</html>
"""


def get_embed_html() -> str:
    """Return the embed page HTML with a non-hardcoded default API (from env via ``utils.config``)."""
    from utils.config import nl_sql_api_url

    injected = json.dumps(nl_sql_api_url())
    return EMBED_HTML_TEMPLATE.replace("__NL_SQL_DEFAULT_API_INJECT__", injected)
