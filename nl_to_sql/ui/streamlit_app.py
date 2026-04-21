"""
ui/streamlit_app.py — Streamlit frontend for NL → SQL

Run (from `nl_to_sql/`):  streamlit run ui/streamlit_app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# `nl_to_sql/` must be on sys.path so `utils` and sibling imports resolve.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import uuid
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st

from utils.config import (
    db_sync_schema_default,
    default_sync_row_limit,
    nl_sql_api_url,
    streamlit_row_limit_options,
)
from utils.env import load_app_env
from utils.http import safe_response_payload

load_app_env()

API_URL = nl_sql_api_url()
_API_PORT = urlparse(API_URL).port or (443 if urlparse(API_URL).scheme == "https" else 80)


st.set_page_config(
    page_title="NL → SQL Explorer",
    page_icon="🔍",
    layout="wide",
)

if "auth_user" not in st.session_state:
    st.session_state.auth_user = None

if not st.session_state.auth_user:
    st.switch_page("pages/signin.py")
    st.stop()

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id"  not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "suggested_prompts" not in st.session_state:
    st.session_state.suggested_prompts = []   # cached prompt suggestions
if "prompts_last_query" not in st.session_state:
    st.session_state.prompts_last_query = ""  # query that drove current suggestions

# Sync-manager session state
_SYNC_DEFAULTS = {
    "sj_all_tables": [],    # all tables parsed from uploaded JSON
    "sj_selected":   [],    # tables user chose to sync
    "sj_queue":      [],    # tables still waiting to be processed
    "sj_results":    {},    # {table: status_dict} for finished tables
    "sj_active":     False, # True while sync loop is running
    "sj_row_limit":  default_sync_row_limit(),
    "sj_pass_next":  False, # True → skip the next table in queue
    "sj_file_name":  None,  # track which file is loaded
    "sj_last_table_count": None,   # last /sync-tables table_count (API-visible tables)
    "sj_last_sync_hint":    "",    # last hint / repair message from API
}
for _k, _v in _SYNC_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("⚙️ Settings")
    _auth = st.session_state.auth_user or {}
    st.caption(f"Signed in as `{_auth.get('username', 'user')}`")
    st.page_link("pages/dashboard.py", label="Tenant Dashboard", icon="🏠")
    if st.button("🚪 Sign Out", use_container_width=True):
        st.session_state.auth_user = None
        st.switch_page("pages/signin.py")
        st.stop()
    st.divider()
    top_k     = st.slider("Tables to retrieve (top-K)", 1, 10, 3)
    row_limit = st.select_slider(
        "Rows per page  (used only when your question has no explicit number)",
        options=streamlit_row_limit_options(),
        value=20,
        help="If your question already says a number (e.g. 'top 5', 'show 20'), that number is always used instead of this slider. Every query uses LIMIT + OFFSET for pagination.",
    )

    col1, col2 = st.columns(2)
    if col1.button("🗑️ Clear Chat"):
        requests.delete(f"{API_URL}/session/{st.session_state.session_id}")
        st.session_state.chat_history = []
        st.session_state.session_id   = str(uuid.uuid4())
        st.rerun()

    if col2.button("🔄 Reload DB"):
        with st.spinner("Reloading schema…"):
            try:
                r = requests.post(f"{API_URL}/reload-schema", timeout=60)
                if r.ok:
                    info, jerr = safe_response_payload(r)
                    if jerr or not isinstance(info, dict):
                        st.error(jerr or "Invalid JSON from reload-schema")
                    else:
                        n = info.get("table_count", 0)
                        st.success(f"✅ {n} table(s) visible to the API / embeddings")
                        if info.get("reader_grants_repaired"):
                            st.info(
                                "Access fix: SELECT was granted to your DB_USER on tables "
                                "in the configured sync schema (DB_SYNC_SCHEMA). "
                                "(reader role could not see admin-created tables before)."
                            )
                        if info.get("hint"):
                            st.warning(info["hint"])
                        if info.get("repair_error") and not info.get("hint"):
                            st.warning(info["repair_error"])
                else:
                    _b, jerr = safe_response_payload(r)
                    st.error(jerr or (_b.get("detail") if isinstance(_b, dict) else "Reload failed"))
            except Exception as ex:
                st.error(str(ex))
        st.rerun()

    # ── Table count badge ─────────────────────────────────────────────────────
    st.divider()
    try:
        _h = requests.get(f"{API_URL}/health", timeout=3)
        if _h.ok:
            _hd = _h.json()
            _cnt = _hd.get("table_count", 0)
            if _cnt > 0:
                st.success(f"📊 {_cnt} table(s) loaded in DB")
            else:
                st.warning("⚠️ No tables loaded")
    except Exception:
        st.caption("⚠️ API not reachable")

    # ══════════════════════════════════════════════════════════════════════
    # 📁  Sync from Schema File  —  full state machine
    # ══════════════════════════════════════════════════════════════════════
    st.divider()
    st.subheader("📁 Sync from Schema File")

    import json as _json

    # ── helper to render one result row ───────────────────────────────────
    def _render_result(tbl, s):
        status = s.get("status", "error")
        if status == "ok":
            act  = s.get("action", "?")
            rows = s.get("rows_upserted", 0)
            bcnt = s.get("local_count_before", 0)
            acnt = s.get("local_count_after",  rows)
            nc   = s.get("columns_added", [])
            col_note = f" · +{len(nc)} col(s)" if nc else ""
            st.success(
                f"✅ `{tbl}` — **{act}** · {rows:,} rows"
                f"{col_note} · count {bcnt:,}→{acnt:,}"
            )
        elif status == "skipped":
            st.warning(f"⏭ `{tbl}` — passed/skipped")
        else:
            st.error(f"❌ `{tbl}` — {s.get('reason','error')}")

    # ─────────────────────────────────────────────────────────────────────
    # STATE 0 : no file loaded yet
    # ─────────────────────────────────────────────────────────────────────
    if not st.session_state.sj_all_tables:
        st.caption(
            "Upload a JSON file listing table names.\n\n"
            "Formats: `[\"t1\",\"t2\"]`  or  `{\"tables\":[…]}`  or  `{\"t1\":{},…}`"
        )
        uploaded_file = st.file_uploader(
            "Upload schema JSON",
            type=["json"],
            key="schema_json_upload",
        )
        if uploaded_file:
            try:
                raw = _json.loads(uploaded_file.read())
                if isinstance(raw, list):
                    names = [x for x in raw if isinstance(x, str) and x.strip()]
                elif isinstance(raw, dict):
                    t = raw.get("tables", raw)
                    if isinstance(t, list):
                        names = [x for x in t if isinstance(x, str)]
                    elif isinstance(t, dict):
                        names = list(t.keys())
                    else:
                        names = list(raw.keys())
                else:
                    names = []

                if not names:
                    st.error("No table names found in the file.")
                else:
                    st.session_state.sj_all_tables = sorted(names)
                    st.session_state.sj_selected   = []   # start empty — user picks what they need
                    st.session_state.sj_file_name  = uploaded_file.name
                    st.rerun()
            except Exception as ex:
                st.error(f"Cannot parse JSON: {ex}")

    # ─────────────────────────────────────────────────────────────────────
    # STATE 1 : file loaded — checkbox list with search + select-all
    # ─────────────────────────────────────────────────────────────────────
    elif not st.session_state.sj_active and not st.session_state.sj_queue:

        all_t   = st.session_state.sj_all_tables
        done    = st.session_state.sj_results
        sel_set = set(st.session_state.sj_selected)

        # ── File header ────────────────────────────────────────────────────
        fh1, fh2 = st.columns([4, 1])
        fh1.caption(f"📄 `{st.session_state.sj_file_name}` — **{len(all_t)}** tables")
        if fh2.button("✕", key="sel_clear", help="Remove file & reset"):
            import copy
            for k, v in _SYNC_DEFAULTS.items():
                st.session_state[k] = copy.deepcopy(v)
            # clear all checkbox keys
            for t in all_t:
                st.session_state.pop(f"chk_{t}", None)
            st.rerun()

        # ── Search box ─────────────────────────────────────────────────────
        sj_search = st.text_input(
            "search",
            placeholder="🔍 Search tables…  e.g. orders, line_items, sku",
            key="sj_search_box",
            label_visibility="collapsed",
        )
        sq       = sj_search.strip().lower()
        visible  = [t for t in all_t if sq in t.lower()] if sq else all_t

        # ── Select-all / Deselect-all (acts on visible list) ──────────────
        sa1, sa2, sa3 = st.columns([1, 1, 1])

        if sa1.button("☑ All", key="chk_sel_all", use_container_width=True,
                      help="Select all visible tables"):
            for t in visible:
                st.session_state[f"chk_{t}"] = True
            sel_set.update(visible)
            st.session_state.sj_selected = sorted(sel_set)
            st.rerun()

        if sa2.button("☐ None", key="chk_sel_none", use_container_width=True,
                      help="Deselect all visible tables"):
            for t in visible:
                st.session_state[f"chk_{t}"] = False
            sel_set -= set(visible)
            st.session_state.sj_selected = sorted(sel_set)
            st.rerun()

        if sa3.button("🗑 Clear", key="chk_sel_clear_all", use_container_width=True,
                      help="Deselect everything"):
            for t in all_t:
                st.session_state.pop(f"chk_{t}", None)
            st.session_state.sj_selected = []
            sel_set.clear()
            st.rerun()

        # ── Status line ────────────────────────────────────────────────────
        if sq:
            st.caption(
                f"🔎 **{len(visible)}** match(es)  ·  "
                f"**{len(sel_set)}** selected total"
            )
        else:
            st.caption(f"**{len(sel_set)} / {len(all_t)}** tables selected")

        # ── Checkbox list (max 50 visible at once) ─────────────────────────
        MAX_SHOW = 50
        show_t   = visible[:MAX_SHOW]

        for t in show_t:
            # seed checkbox state from sel_set on first render
            if f"chk_{t}" not in st.session_state:
                st.session_state[f"chk_{t}"] = (t in sel_set)

            checked = st.checkbox(t, key=f"chk_{t}")
            if checked:
                sel_set.add(t)
            else:
                sel_set.discard(t)

        if len(visible) > MAX_SHOW:
            st.caption(
                f"_Showing {MAX_SHOW} of {len(visible)} — "
                f"refine your search to see more._"
            )

        # persist checkbox results back to session state
        st.session_state.sj_selected = sorted(sel_set)
        chosen = sorted(sel_set)

        # ── Selected summary (collapsible) ─────────────────────────────────
        if chosen:
            with st.expander(f"✅ Selected tables ({len(chosen)})", expanded=False):
                st.markdown(
                    "\n".join(f"• `{t}`" for t in chosen)
                )

        # ── Rows per table + sync button ───────────────────────────────────
        st.divider()
        st.session_state.sj_row_limit = st.number_input(
            "Rows to fetch per table  (0 = all rows — no limit)",
            min_value=0, max_value=500_000,
            value=st.session_state.sj_row_limit,
            step=100, key="sj_rl_input",
            help=(
                "Set to 0 to fetch every row from the remote API. "
                "Recommended when tables are related (e.g. orders + order_items) "
                "so JOIN queries return real values instead of NULLs. "
                "Large tables may take a while."
            ),
        )
        if st.session_state.sj_row_limit == 0:
            st.info(
                "⚠️ **No-limit mode:** all rows will be fetched. "
                "For large tables (100k+ rows) this may take several minutes."
            )

        # Show previous results if any
        if done:
            with st.expander(f"Previous results ({len(done)} table(s))", expanded=False):
                for tbl, s in done.items():
                    _render_result(tbl, s)

        if st.button(
            f"▶ Start Sync  ({len(chosen)} table(s))",
            use_container_width=True,
            disabled=(len(chosen) == 0),
            type="primary",
        ):
            st.session_state.sj_queue     = list(chosen)
            st.session_state.sj_results   = {}
            st.session_state.sj_active    = True
            st.session_state.sj_pass_next = False
            st.rerun()

    # ─────────────────────────────────────────────────────────────────────
    # STATE 2 : sync in progress  — one table per rerun
    # ─────────────────────────────────────────────────────────────────────
    elif st.session_state.sj_active:

        queue   = st.session_state.sj_queue
        results = st.session_state.sj_results
        total   = len(st.session_state.sj_selected)
        done_n  = total - len(queue)

        st.caption(f"**Syncing…  {done_n}/{total} done**")
        st.progress(done_n / max(total, 1))

        # Control buttons
        b1, b2, b3 = st.columns(3)
        if b1.button("⏭ Pass",   key="btn_pass",
                     help="Skip this table, continue with rest"):
            st.session_state.sj_pass_next = True
            st.rerun()

        if b2.button("⏹ Stop",   key="btn_stop",
                     help="Finish current table then stop"):
            st.session_state.sj_active = False
            st.session_state.sj_queue  = []   # abandon remaining
            st.rerun()

        if b3.button("✕ Cancel", key="btn_cancel",
                     help="Stop immediately and discard all results"):
            for k, v in _SYNC_DEFAULTS.items():
                import copy
                st.session_state[k] = copy.deepcopy(v)
            st.rerun()

        # Show completed results so far
        if results:
            with st.expander("Results so far", expanded=True):
                for tbl, s in results.items():
                    _render_result(tbl, s)

        # ── Process next table ────────────────────────────────────────────
        if queue:
            current = queue[0]
            st.info(f"⏳ Processing `{current}` …")

            if st.session_state.sj_pass_next:
                # User pressed Pass — skip this table
                results[current] = {"status": "skipped"}
                st.session_state.sj_results   = results
                st.session_state.sj_queue      = queue[1:]
                st.session_state.sj_pass_next  = False
            else:
                # Sync the table
                try:
                    r = requests.post(
                        f"{API_URL}/sync-tables",
                        json={
                            "tables":    [current],
                            "row_limit": st.session_state.sj_row_limit,
                        },
                        timeout=120,
                    )
                    body, jerr = safe_response_payload(r)
                    if jerr or not isinstance(body, dict):
                        results[current] = {"status": "error", "reason": jerr or "Bad API response"}
                    elif r.ok:
                        tbl_status = body.get("results", {}).get(current, {})
                        if "action" in tbl_status:
                            tbl_status["status"] = "ok"
                        results[current] = tbl_status
                        st.session_state.sj_last_table_count = body.get("table_count")
                        hint = body.get("hint") or ""
                        if body.get("repair_error") and not hint:
                            hint = str(body.get("repair_error"))
                        st.session_state.sj_last_sync_hint = hint
                        if body.get("reader_grants_repaired"):
                            st.session_state.sj_last_sync_hint = (
                                (hint + " — ") if hint else ""
                            ) + "Reader access (GRANT SELECT) was repaired for DB_USER."
                    else:
                        results[current] = {
                            "status": "error",
                            "reason": body.get("detail", jerr or "API error")
                            if isinstance(body, dict) else (jerr or "API error"),
                        }
                except Exception as ex:
                    results[current] = {"status": "error", "reason": str(ex)}

                st.session_state.sj_results = results
                st.session_state.sj_queue   = queue[1:]

            # If queue is now empty → sync complete
            if not st.session_state.sj_queue:
                st.session_state.sj_active = False

            st.rerun()   # move to next table

    # ─────────────────────────────────────────────────────────────────────
    # STATE 3 : sync complete  — show final results
    # ─────────────────────────────────────────────────────────────────────
    else:
        results = st.session_state.sj_results
        ok_n    = sum(1 for s in results.values() if s.get("status") == "ok")
        sk_n    = sum(1 for s in results.values() if s.get("status") == "skipped")
        err_n   = sum(1 for s in results.values() if s.get("status") == "error")

        st.success(
            f"✅ Sync complete!  "
            f"**{ok_n}** synced · **{sk_n}** passed · **{err_n}** errors"
        )
        if st.session_state.get("sj_last_sync_hint"):
            st.warning(st.session_state.sj_last_sync_hint)
        if st.session_state.get("sj_last_table_count") is not None:
            st.caption(
                f"API reports **{st.session_state.sj_last_table_count}** table(s) after last sync "
                "(embeddings refreshed). Use **Reload DB** if this looks wrong."
            )

        for tbl, s in results.items():
            _render_result(tbl, s)

        st.divider()
        ra, rb = st.columns(2)
        if ra.button("🔄 Sync Again", use_container_width=True,
                     help="Re-select and sync again"):
            st.session_state.sj_queue   = []
            st.session_state.sj_active  = False
            st.session_state.sj_results = {}
            st.rerun()

        if rb.button("📂 New File", use_container_width=True,
                     help="Upload a different JSON file"):
            for k, v in _SYNC_DEFAULTS.items():
                import copy
                st.session_state[k] = copy.deepcopy(v)
            st.rerun()



# ── Main area ─────────────────────────────────────────────────────────────────
st.title("🔍 Automation SQL Generator")

# ── No-tables guard: check API health and show a clear message if DB is empty ─
try:
    _health = requests.get(f"{API_URL}/health", timeout=5)
    if _health.ok:
        _hdata = _health.json()
        if not _hdata.get("has_tables", True):
            db   = _hdata.get("db_name", "?")
            scan = _hdata.get("db_schema_scan") or _hdata.get("db_schemas", "(see .env)")
            sync = _hdata.get("db_sync_schema") or db_sync_schema_default()
            st.warning(
                f"### 📭 Database `{db}` — no tables visible to the API yet\n\n"
                f"**Metadata scan (DB_SCHEMAS):** `{scan}`  \n"
                f"**Importer target (DB_SYNC_SCHEMA):** `{sync}`\n\n"
                "**👈 Use the sidebar to load your tables:**\n\n"
                "1. Scroll to **📁 Sync from Schema File** in the left sidebar.\n"
                "2. Upload your schema JSON file (list of table names).\n"
                "3. Select the tables you want and click **▶ Start Sync**.\n"
                "4. After sync completes, click **🔄 Reload DB** — the chat will appear here automatically.\n\n"
                "_The chat input is hidden until at least one table is loaded._"
            )
            st.stop()          # Sidebar still renders — only main chat is blocked
    else:
        st.warning(
            f"⚠️ Cannot reach the API at `{API_URL}`. Start the FastAPI server "
            f"(same host/port as NL_SQL_API_URL / API_URL in .env)."
        )
        st.stop()
except requests.exceptions.ConnectionError:
    st.warning(
        f"⚠️ Cannot connect to the API at `{API_URL}`. From folder `nl_to_sql` run: "
        f"`python -m uvicorn main:app --reload --port {_API_PORT}` "
        "(or match your configured NL_SQL_API_URL)."
    )
    st.stop()

# ── Dynamic example prompts ───────────────────────────────────────────────────
def _last_user_query() -> str:
    """Return the most recent user message from chat history, or empty string."""
    for turn in reversed(st.session_state.chat_history):
        if turn["role"] == "user":
            return turn["content"]
    return ""

def _fetch_suggested_prompts(last_query: str = "") -> list[str]:
    """Call the API and return 6 suggested prompts."""
    try:
        params = {"last_query": last_query} if last_query else {}
        r = requests.get(f"{API_URL}/suggest-prompts", params=params, timeout=15)
        if r.ok:
            return r.json().get("prompts", [])
    except Exception:
        pass
    return []

# Determine when to (re)fetch suggestions:
#   • First load (no prompts cached yet)
#   • After a new chat answer (last query changed)
_current_last = _last_user_query()
_need_refresh  = (
    not st.session_state.suggested_prompts
    or st.session_state.prompts_last_query != _current_last
)
if _need_refresh:
    _new = _fetch_suggested_prompts(_current_last)
    if _new:
        st.session_state.suggested_prompts  = _new
        st.session_state.prompts_last_query = _current_last

with st.expander("💡 Suggested prompts", expanded=True):
    _hdr, _btn = st.columns([5, 1])
    _hdr.caption(
        "Follow-up suggestions" if _current_last
        else "Tap any prompt to use it · refreshes after each answer"
    )
    if _btn.button("🔄", key="refresh_prompts", help="Get new suggestions"):
        fresh = _fetch_suggested_prompts(_current_last)
        if fresh:
            st.session_state.suggested_prompts  = fresh
            st.session_state.prompts_last_query = _current_last
        st.rerun()

    _prompts = st.session_state.suggested_prompts
    if _prompts:
        _cols = st.columns(2)
        for _i, _ex in enumerate(_prompts):
            if _cols[_i % 2].button(_ex, key=f"ex_{_i}", use_container_width=True):
                st.session_state["pending_prompt"] = _ex
    else:
        st.caption("_Waiting for API…_")

# ── Chat history ──────────────────────────────────────────────────────────────
for idx, turn in enumerate(st.session_state.chat_history):
    if turn["role"] == "user":
        with st.chat_message("user"):
            st.write(turn["content"])
    else:
        with st.chat_message("assistant"):
            data = turn.get("data", {})

            if "error" in data:
                st.error(data["error"])
            else:
                st.markdown(f"**Explanation:** {data.get('explanation', '')}")

                with st.expander("🧾 Generated SQL"):
                    st.code(data.get("sql", ""), language="sql")

                sql         = data.get("sql", "")
                columns     = data.get("columns", [])
                chart       = data.get("chart_suggestion", "table")
                viz_cfg     = data.get("viz_config") or {}
                total_count = data.get("total_count", 0)

                # ── Per-message pagination state ──────────────────────────
                pg_key    = f"pg_{idx}"        # current page number
                rows_key  = f"rows_{idx}"      # rows for current page (changes per page)
                chart_key = f"chart_{idx}"     # ALL rows for chart (set once, never changes)

                # Seed initial state on first render
                if pg_key not in st.session_state:
                    st.session_state[pg_key]   = 1
                    st.session_state[rows_key] = data.get("rows", [])

                # Fetch ALL rows for chart rendering (once, on first render).
                # Chart always shows the complete result — not just the current page.
                if chart_key not in st.session_state:
                    initial_rows = data.get("rows", [])
                    if total_count > len(initial_rows) and total_count <= 5_000 and sql:
                        # Fetch all rows in one shot for the chart
                        try:
                            _cr = requests.post(
                                f"{API_URL}/sql/page",
                                json={"sql": sql, "page": 1,
                                      "page_size": total_count},
                                timeout=60,
                            )
                            st.session_state[chart_key] = (
                                _cr.json()["rows"] if _cr.ok else initial_rows
                            )
                        except Exception:
                            st.session_state[chart_key] = initial_rows
                    else:
                        # total_count <= current fetch, or too large — use what we have
                        st.session_state[chart_key] = initial_rows

                cur_page  = st.session_state[pg_key]
                cur_rows  = st.session_state[rows_key]
                chart_rows = st.session_state[chart_key]   # full dataset for charts
                ps        = max(len(data.get("rows", [])), 1)  # original page size
                total_pages = max(1, -(-total_count // ps))    # ceiling div

                # ── Metrics row ───────────────────────────────────────────
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("Showing",    f"{len(cur_rows):,}")
                m2.metric("Total rows", f"{total_count:,}")
                m3.metric("Page",       f"{cur_page} / {total_pages}")
                m4.metric("Time (ms)",  data.get("execution_ms", 0))

                # Tables used by the agent
                tables_used = data.get("tables_used", [])
                if tables_used:
                    st.caption(f"🗂 Tables used: {' · '.join(f'`{t}`' for t in tables_used)}")

                # ── Large-dataset banner ──────────────────────────────────
                if total_count > ps:
                    st.info(
                        f"Showing **{len(cur_rows):,}** of **{total_count:,}** rows "
                        f"(page {cur_page}/{total_pages}). "
                        f"Use **◀ Prev / Next ▶** to browse all pages."
                    )

                # ── Dataframe (current page only) ─────────────────────────
                if cur_rows:
                    df = pd.DataFrame(cur_rows, columns=columns)
                    bool_cols = df.select_dtypes(include="bool").columns
                    if len(bool_cols):
                        df[bool_cols] = df[bool_cols].astype(str)

                    # Always show ALL columns (including NULL ones) so user can see what's missing.
                    # Detect empty columns (actual NULL, string "None", or empty string "").
                    def _is_empty_col(series):
                        cleaned = series.astype(str).str.strip()
                        return cleaned.isin({"None", "nan", "", "null", "NULL"}).all()

                    null_cols = [c for c in df.columns if _is_empty_col(df[c])]

                    st.dataframe(df, use_container_width=True, hide_index=True)

                    if null_cols:
                        st.warning(
                            f"⚠️ **{len(null_cols)} column(s) returned no data:** "
                            f"{', '.join(f'`{c}`' for c in null_cols)}  \n"
                            "The JOIN found no matching rows for these fields.  \n"
                            "**Fix:** Rephrase to use a stronger filter, e.g.  \n"
                            "*'List orders with non-null ship dates'* → "
                            "the system can use INNER JOIN + IS NOT NULL automatically."
                        )

                # ── Charts (always use full dataset, not current page) ─────
                if chart_rows:
                    import plotly.express as px

                    # Build chart_df from ALL rows (never just the current page)
                    chart_df    = pd.DataFrame(chart_rows, columns=columns)
                    bool_cols_c = chart_df.select_dtypes(include="bool").columns
                    if len(bool_cols_c):
                        chart_df[bool_cols_c] = chart_df[bool_cols_c].astype(str)

                    # For charts: drop all-NULL columns (they can't be plotted anyway)
                    chart_non_null = [c for c in chart_df.columns if chart_df[c].notna().any()
                                      and not (chart_df[c].astype(str).str.strip().eq("None").all())]
                    chart_df = chart_df[chart_non_null] if chart_non_null else chart_df
                    # (chart uses filtered df; table above still shows all columns with warning)

                    numeric_cols = chart_df.select_dtypes("number").columns.tolist()
                    text_cols    = chart_df.select_dtypes("object").columns.tolist()

                    # Resolve LLM-provided axis names; fall back to auto-detect
                    def _col(key: str, pool: list[str]) -> str | None:
                        hint = viz_cfg.get(key)
                        if hint and hint in chart_df.columns:
                            return hint
                        return pool[0] if pool else None

                    x_col   = _col("x", text_cols or numeric_cols)
                    y_col   = _col("y", numeric_cols or text_cols)
                    clr_col = viz_cfg.get("color") if viz_cfg.get("color") in chart_df.columns else None
                    title   = viz_cfg.get("title") or ""

                    if chart == "bar" and x_col and y_col:
                        fig = px.bar(chart_df, x=x_col, y=y_col, color=clr_col,
                                     title=title, text_auto=True)
                        fig.update_layout(xaxis_tickangle=-35)
                        st.plotly_chart(fig, use_container_width=True)

                    elif chart == "line" and x_col and y_col:
                        fig = px.line(chart_df, x=x_col, y=y_col, color=clr_col,
                                      title=title, markers=True)
                        st.plotly_chart(fig, use_container_width=True)

                    elif chart == "pie" and len(chart_df) <= 50 and x_col and y_col:
                        fig = px.pie(chart_df, names=x_col, values=y_col, title=title,
                                     hole=0.3)
                        st.plotly_chart(fig, use_container_width=True)

                    elif chart == "scatter" and len(numeric_cols) >= 2:
                        sc_x = _col("x", numeric_cols)
                        sc_y = _col("y", [c for c in numeric_cols if c != sc_x] or numeric_cols)
                        fig  = px.scatter(
                            chart_df, x=sc_x, y=sc_y,
                            color=clr_col,
                            hover_data=chart_df.columns.tolist(),
                            title=title or f"{sc_y} vs {sc_x}",
                        )
                        fig.update_traces(marker=dict(size=7, opacity=0.7))
                        st.plotly_chart(fig, use_container_width=True)

                    elif chart == "heatmap" and len(text_cols) >= 2 and numeric_cols:
                        heat_x   = _col("x", text_cols)
                        heat_y   = _col("y", [c for c in text_cols if c != heat_x] or text_cols)
                        heat_val = numeric_cols[0]
                        pivot = (
                            chart_df.groupby([heat_y, heat_x])[heat_val]
                              .sum()
                              .reset_index()
                              .pivot(index=heat_y, columns=heat_x, values=heat_val)
                              .fillna(0)
                        )
                        fig = px.imshow(
                            pivot,
                            text_auto=True,
                            aspect="auto",
                            title=title or f"{heat_val} by {heat_y} × {heat_x}",
                            color_continuous_scale="Blues",
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    elif chart == "kpi" and numeric_cols:
                        kpi_cols = st.columns(min(len(numeric_cols), 4))
                        for i, col in enumerate(numeric_cols[:4]):
                            kpi_cols[i].metric(col, f"{chart_df[col].iloc[0]:,}")

                    # ── Pagination controls ───────────────────────────────
                    if total_pages > 1 and sql:
                        nav1, nav2, _ = st.columns([1, 1, 4])

                        if nav1.button("◀ Prev", key=f"prev_{idx}",
                                       disabled=(cur_page <= 1)):
                            with st.spinner("Loading page …"):
                                pr = requests.post(
                                    f"{API_URL}/sql/page",
                                    json={"sql": sql, "page": cur_page - 1,
                                          "page_size": ps},
                                    timeout=60,
                                )
                            if pr.ok:
                                st.session_state[pg_key]   = cur_page - 1
                                st.session_state[rows_key] = pr.json()["rows"]
                                st.rerun()

                        if nav2.button("Next ▶", key=f"next_{idx}",
                                       disabled=(cur_page >= total_pages)):
                            with st.spinner("Loading page …"):
                                pr = requests.post(
                                    f"{API_URL}/sql/page",
                                    json={"sql": sql, "page": cur_page + 1,
                                          "page_size": ps},
                                    timeout=60,
                                )
                            if pr.ok:
                                st.session_state[pg_key]   = cur_page + 1
                                st.session_state[rows_key] = pr.json()["rows"]
                                st.rerun()
                else:
                    st.info("Query returned no rows.")

# ── Input ─────────────────────────────────────────────────────────────────────
# Pick up any prompt injected by sidebar buttons (Schema Browser)
_injected = st.session_state.pop("_inject_prompt", None)
pending   = st.session_state.pop("pending_prompt", None)
prompt    = st.chat_input("Ask a question about your data …") or _injected or pending

if prompt:
    st.session_state.chat_history.append({"role": "user", "content": prompt})

    # Step-by-step progress so the user sees activity while waiting
    _prog   = st.empty()
    _steps  = [
        "🔍 Finding relevant tables …",
        "🤖 Generating SQL …",
        "⚡ Executing query …",
        "✅ Done!",
    ]
    import time as _time

    def _show(step: int, msg: str = ""):
        _prog.info(_steps[step] + (f" {msg}" if msg else ""))

    _show(0)
    try:
        _t0   = _time.monotonic()
        _show(1)
        resp  = requests.post(
            f"{API_URL}/generate-sql",
            json={
                "prompt":     prompt,
                "session_id": st.session_state.session_id,
                "top_k":      top_k,
                "row_limit":  row_limit,
                "offset":     0,
            },
            timeout=60,
        )
        _show(2)
        body, parse_err = safe_response_payload(resp)
        if parse_err:
            data = {"error": parse_err}
        elif resp.ok:
            data = body if isinstance(body, dict) else {"error": "Unexpected API response shape"}
        else:
            detail = "Unknown error"
            if isinstance(body, dict):
                detail = body.get("detail", body.get("message", str(body)))
            data = {"error": detail}
        _elapsed = _time.monotonic() - _t0
        _show(3, f"({_elapsed:.1f}s)")
        _time.sleep(0.4)          # brief flash so user sees the ✅
    except Exception as e:
        data = {"error": str(e)}
    finally:
        _prog.empty()             # clear the progress bar

    st.session_state.chat_history.append({"role": "assistant", "content": "", "data": data})
    st.rerun()