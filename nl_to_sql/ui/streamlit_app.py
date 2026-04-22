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

import hashlib
import math
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st

from utils.config import (
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
st.markdown(
    """
<style>
  .stApp { background: linear-gradient(180deg, #0b1220 0%, #0e1628 40%, #0a0f18 100%); color: #e2e8f0; }
  [data-testid="stSidebar"] { background: #0f172a; border-right: 1px solid #1e293b; }
  [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] label { color: #cbd5e1 !important; }
  .nl-banner { background: linear-gradient(90deg,#134e5e,#0f2942); padding:1rem 1.25rem; border-radius:10px;
    border:1px solid #1e3a4a; margin-bottom:1rem; }
  .nl-banner h3 { color:#e0f7fa; margin:0 0 0.35rem 0; font-size:1.05rem; }
  .nl-banner p { color:#94a3b8; margin:0; font-size:0.95rem; }
</style>
""",
    unsafe_allow_html=True,
)

# ── Session state ─────────────────────────────────────────────────────────────
if "session_id"  not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []
if "suggested_prompts" not in st.session_state:
    st.session_state.suggested_prompts = []   # cached prompt suggestions
if "prompts_last_query" not in st.session_state:
    st.session_state.prompts_last_query = ""  # query that drove current suggestions

# Dynamic DB connection (API session mirrors chat session_id)
_DB_DEFAULTS = {
    "conn_host":       "",
    "conn_port":       "5432",
    "conn_user":       "",
    "conn_pass":       "",
    "catalog_db":      "",
    "db_list":         [],
    "pick_database":   "",
    "schema_list":     [],
    "pick_schemas":    [],
    "table_flat":      [],
    "pick_tables":     [],
    "sel_table_labels": [],
    "table_catalog_fp": "",
    "nl_ready":        False,
    "conn_source":     "live",
    "file_db_name":    "",
    "pg_session_connected": False,
}
for _k, _v in _DB_DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v
if "schema_activation_job_id" not in st.session_state:
    st.session_state.schema_activation_job_id = None
if "schema_job_result" not in st.session_state:
    st.session_state.schema_job_result = None
if "schema_job_error" not in st.session_state:
    st.session_state.schema_job_error = None
if "ui_module" not in st.session_state:
    st.session_state.ui_module = "configuration"
if "cfg_dialog_open" not in st.session_state:
    st.session_state.cfg_dialog_open = False
if "top_k" not in st.session_state:
    st.session_state.top_k = 3
if "row_limit" not in st.session_state:
    st.session_state.row_limit = 20
if "schema_job_paused" not in st.session_state:
    st.session_state.schema_job_paused = False

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

def _nl_session_ready() -> bool:
    """True when the API reports an activated schema with at least one table."""
    try:
        r = requests.get(
            f"{API_URL}/health",
            params={"session_id": st.session_state.session_id},
            timeout=5,
        )
        if not r.ok:
            return False
        d = r.json()
        return bool(d.get("activated") and d.get("has_tables"))
    except Exception:
        return False


def _sync_schema_job_paused_from_api() -> None:
    """
    Align ``schema_job_paused`` with the FastAPI job status. The @st.fragment poll
    may run after the main script in a given cycle, so the main Configuration page
    would otherwise miss Pause and keep hiding **Go to Chat**.
    """
    jid = st.session_state.get("schema_activation_job_id")
    if not jid:
        st.session_state.schema_job_paused = False
        return
    try:
        r = requests.get(
            f"{API_URL}/schema/from-file/job/{jid}",
            params={"session_id": st.session_state.session_id},
            timeout=15,
        )
        if r.ok:
            info = r.json()
            if isinstance(info, dict):
                st.session_state.schema_job_paused = bool(info.get("paused"))
    except Exception:
        pass


def _schema_activation_running_without_pause() -> bool:
    """
    True while an async schema upload/sync job is in progress and not paused.
    In that state we hide **Go to Chat** / block switching to Chat until the job
    completes, is cancelled, errors out, or the user pauses.
    """
    if not st.session_state.get("schema_activation_job_id"):
        return False
    _sync_schema_job_paused_from_api()
    return not bool(st.session_state.get("schema_job_paused"))


def _post_schema_job_control(job_id: str, session_id: str, action: str) -> bool:
    try:
        r = requests.post(
            f"{API_URL}/schema/from-file/job/{job_id}/control",
            json={"session_id": session_id, "action": action},
            timeout=45,
        )
        return bool(r.ok)
    except Exception:
        return False


@st.fragment(run_every=timedelta(seconds=2))
def _poll_schema_upload_job_fragment() -> None:
    """Background schema activation: real table progress + Pause / Resume / Cancel."""
    jid = st.session_state.get("schema_activation_job_id")
    if not jid:
        st.session_state.schema_job_paused = False
        return
    sid = st.session_state.session_id
    try:
        r = requests.get(
            f"{API_URL}/schema/from-file/job/{jid}",
            params={"session_id": sid},
            timeout=45,
        )
        info, jerr = safe_response_payload(r)
        if jerr or not r.ok:
            st.warning(f"Job status: {jerr or r.text[:300]}")
            return
        if not isinstance(info, dict):
            return

        st.session_state.schema_job_paused = bool(info.get("paused"))

        st.markdown("**Schema activation (running in background)**")
        ph = info.get("phase") or ""
        msg = info.get("message") or ""
        cur = int(info.get("sync_current") or 0)
        tot = int(info.get("sync_total") or 0)
        if tot > 0 and ph == "remote_sync":
            st.progress(min(1.0, cur / float(tot)))
            st.caption(
                f"Remote sync: table **{cur}** / **{tot}**  ·  `{info.get('current_table') or '…'}`"
            )
        else:
            st.progress(0.08 if ph in ("provision", "extract_schema", "running") else 0.02)
            st.caption(msg or ph or "Working…")

        if info.get("paused"):
            st.caption("**Paused** — click Resume to continue after the current step.")

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Pause", key=f"sch_job_pause_{jid}", help="After the current table finishes"):
                _post_schema_job_control(jid, sid, "pause")
        with c2:
            if st.button("Resume", key=f"sch_job_resume_{jid}"):
                _post_schema_job_control(jid, sid, "resume")
        with c3:
            if st.button("Cancel", key=f"sch_job_cancel_{jid}", type="secondary"):
                _post_schema_job_control(jid, sid, "cancel")

        stt = info.get("status")
        if stt == "done" and info.get("result"):
            st.session_state.nl_ready = True
            st.session_state.cfg_dialog_open = False
            st.session_state.schema_job_paused = False
            st.session_state.schema_job_result = info["result"]
            st.session_state.schema_activation_job_id = None
            st.rerun()
        elif stt == "error":
            st.session_state.schema_job_error = info.get("error", "Activation job failed")
            st.session_state.schema_job_paused = False
            st.session_state.schema_activation_job_id = None
            st.rerun()
    except Exception as ex:
        st.caption(f"Could not poll job status: {ex}")


_sid = st.session_state.session_id

# ── Sidebar (module nav — always) ───────────────────────────────────────────
with st.sidebar:
    st.markdown("### NL → SQL")
    st.caption("Natural language to SQL")
    _nav_c1, _nav_c2 = st.columns(2)
    with _nav_c1:
        _sty_cfg = "primary" if st.session_state.ui_module == "configuration" else "secondary"
        if st.button("⚙️ Configuration", use_container_width=True, type=_sty_cfg):
            st.session_state.ui_module = "configuration"
            st.session_state.cfg_dialog_open = False
            st.rerun()
    with _nav_c2:
        _sty_chat = "primary" if st.session_state.ui_module == "chat" else "secondary"
        if st.button("💬 Chat", use_container_width=True, type=_sty_chat):
            if _nl_session_ready() and _schema_activation_running_without_pause():
                st.session_state.schema_chat_nav_blocked = True
            else:
                st.session_state.ui_module = "chat"
                st.session_state.cfg_dialog_open = not _nl_session_ready()
            st.rerun()
    st.divider()

# ── Sidebar (database & schema — Configuration module only) ─────────────────
if st.session_state.ui_module == "configuration":
    with st.sidebar:
        st.header("🗄️ Database & schema")

        _src = st.radio(
            "Schema source", ["Live PostgreSQL", "Upload schema JSON"], horizontal=True
        )
        st.session_state.conn_source = "file" if _src.startswith("Upload") else "live"

        if st.session_state.conn_source == "file":
            _jr = st.session_state.pop("schema_job_result", None)
            if _jr and isinstance(_jr, dict):
                st.session_state.nl_ready = True
                st.session_state.cfg_dialog_open = False
                st.success(
                    f"✅ {_jr.get('table_count', 0)} table(s) — "
                    f"execution: {'on' if _jr.get('execution_enabled') else 'off'}"
                )
                if _jr.get("hint"):
                    st.info(_jr["hint"])
                ds = _jr.get("data_sync")
                if isinstance(ds, dict) and ds.get("error"):
                    st.warning(f"Remote data sync: {ds['error']}")
                elif isinstance(ds, dict) and ds.get("per_table"):
                    err_list = ds.get("errors") or []
                    with st.expander("Remote data load (per table)", expanded=bool(err_list)):
                        st.json(ds)
            _je = st.session_state.pop("schema_job_error", None)
            if _je:
                st.error(_je)

            _poll_schema_upload_job_fragment()

            _upload_mode = st.radio(
                "Schema JSON",
                (
                    "File only — schema label + upload (no database login)",
                    "New connection — PostgreSQL host/user, connect, then upload JSON",
                ),
                key="schema_json_upload_mode",
                help=(
                    "File only: quickest path; NL→SQL uses the JSON in memory. "
                    "New connection: sign in first so your session keeps the DB pool when the file is applied."
                ),
            )
            _use_new_pg = _upload_mode.startswith("New")

            if _use_new_pg:
                st.markdown("##### 1 — Connect to PostgreSQL")
                st.caption("Use the same fields as **Live PostgreSQL** — connect and open the target database, then upload the file below.")
                st.session_state.conn_host = st.text_input("Host", value=st.session_state.conn_host, key="in_host")
                st.session_state.conn_port = st.text_input("Port", value=st.session_state.conn_port or "5432", key="in_port")
                st.session_state.conn_user = st.text_input("Username", value=st.session_state.conn_user, key="in_user")
                st.session_state.conn_pass = st.text_input("Password", type="password", value=st.session_state.conn_pass, key="in_pass")
                st.caption("Initial DB for listing: leave blank to use your **username** as the database name.")
                st.session_state.catalog_db = st.text_input(
                    "Initial database (optional)",
                    value=st.session_state.catalog_db,
                    key="in_catdb",
                )
                if st.button("Connect", key="btn_connect_file_upload"):
                    try:
                        port = int((st.session_state.conn_port or "5432").strip() or "5432")
                    except ValueError:
                        st.error("Invalid port")
                        port = 5432
                    body = {
                        "session_id": _sid,
                        "host": (st.session_state.conn_host or "").strip(),
                        "port": port,
                        "username": (st.session_state.conn_user or "").strip(),
                        "password": st.session_state.conn_pass or "",
                    }
                    if (st.session_state.catalog_db or "").strip():
                        body["catalog_database"] = st.session_state.catalog_db.strip()
                    try:
                        r = requests.post(f"{API_URL}/db/connect", json=body, timeout=45)
                        info, jerr = safe_response_payload(r)
                        if jerr:
                            st.error(jerr)
                        elif r.ok and isinstance(info, dict):
                            st.session_state.db_list = info.get("databases") or []
                            st.session_state.pg_session_connected = True
                            st.success(f"Connected — {len(st.session_state.db_list)} database(s) listed.")
                        else:
                            st.error((info or {}).get("detail", "Connect failed") if isinstance(info, dict) else "Connect failed")
                    except Exception as ex:
                        st.error(str(ex))

                if st.session_state.db_list:
                    st.selectbox(
                        "Choose database",
                        options=st.session_state.db_list,
                        key="sb_database",
                    )
                    if st.button("Open this database", key="btn_open_db_file_upload"):
                        try:
                            r = requests.post(
                                f"{API_URL}/db/use-database",
                                json={"session_id": _sid, "database": st.session_state.sb_database},
                                timeout=45,
                            )
                            info, jerr = safe_response_payload(r)
                            if jerr:
                                st.error(jerr)
                            elif r.ok:
                                st.success("Database opened — you can upload the schema JSON below.")
                            else:
                                st.error((info or {}).get("detail", "Failed") if isinstance(info, dict) else "Failed")
                        except Exception as ex:
                            st.error(str(ex))

                st.divider()
                st.markdown("##### 2 — Upload schema JSON")
                if not st.session_state.db_list:
                    st.info("Connect above first so the session keeps your database login with the uploaded file.")
            else:
                st.caption(
                    "No host or password — the JSON is loaded in-memory for NL→SQL. "
                    "Switch to **Live PostgreSQL** later if you want to run queries against a real database."
                )

            st.session_state.file_db_name = st.text_input(
                "Target database name (required)",
                value=st.session_state.file_db_name,
                key="sf_dbname",
                help="Used as the PostgreSQL database name when you create tables on the server; otherwise a logical label for NL→SQL.",
            )
            _provision = st.checkbox(
                "Create database & tables on PostgreSQL from this JSON (server DDL)",
                key="provision_pg_ddl",
                help=(
                    "Connect first (use **Live PostgreSQL → Connect**, or **New connection** above). "
                    "Runs CREATE DATABASE if missing, then CREATE SCHEMA/TABLE. "
                    "The name above becomes the database name."
                ),
            )
            _remote_url = ""
            _remote_limit = "5000"
            if _provision:
                _remote_url = st.text_input(
                    "Remote SQL API URL (optional — loads row data after DDL)",
                    key="sf_remote_api_url",
                    help=(
                        "POST JSON body `{\"query\": \"SELECT ...\"}` (same as your SQL passthrough). "
                        "After tables are created, the app runs SELECT * per table (with a row limit) "
                        "and upserts into PostgreSQL so columns and data match the remote DB."
                    ),
                )
                _remote_limit = st.text_input(
                    "Max rows per table from API",
                    value="5000",
                    key="sf_remote_row_limit",
                    help=(
                        "Per table, per run: max rows to pull from the remote API (1–100000). "
                        "When your local table already has data and a numeric `id` column, the next run "
                        "requests **new** rows with `id` greater than local MAX(id) (incremental append), "
                        "not the first page again."
                    ),
                )
            _sf = st.file_uploader("Schema JSON", type=["json"], key="sf_upload")
            if st.button("Activate uploaded schema", type="primary"):
                if not (st.session_state.file_db_name or "").strip():
                    st.error("Enter a target database name.")
                elif not _sf:
                    st.error("Choose a JSON file.")
                elif _provision and not (_use_new_pg or st.session_state.get("pg_session_connected")):
                    st.error(
                        "Connect to PostgreSQL first: open **Live PostgreSQL** and click **Connect**, "
                        "or choose **New connection** here and connect — then activate again."
                    )
                else:
                    try:
                        _keep = (
                            "true"
                            if (_use_new_pg or st.session_state.get("pg_session_connected"))
                            else "false"
                        )
                        _mat = "true" if (_provision and _keep == "true") else "false"
                        _to = 900 if _mat == "true" else 120
                        _file_name = _sf.name
                        _file_body = _sf.getvalue()
                        _post_data = {
                            "session_id": _sid,
                            "database_name": st.session_state.file_db_name.strip(),
                            "keep_connection": _keep,
                            "materialize": _mat,
                            "target_database": st.session_state.file_db_name.strip(),
                            "remote_data_url": (_remote_url or "").strip(),
                            "remote_row_limit": (_remote_limit or "5000").strip(),
                        }
                        _post_files = {
                            "file": (_file_name, _file_body, "application/json"),
                        }

                        _ru = (_remote_url or "").strip()
                        _use_async = _provision and _keep == "true" and (
                            _mat == "true" or bool(_ru)
                        )

                        if _use_async:
                            r = requests.post(
                                f"{API_URL}/schema/from-file/async",
                                data=_post_data,
                                files=_post_files,
                                timeout=90,
                            )
                            info, jerr = safe_response_payload(r)
                            if jerr:
                                st.error(jerr)
                            elif r.ok and isinstance(info, dict) and info.get("job_id"):
                                st.session_state.schema_activation_job_id = info["job_id"]
                                st.info(
                                    "Background job started — **Pause / Resume / Cancel** appear below. "
                                    "Table progress updates every few seconds."
                                )
                                st.rerun()
                            else:
                                st.error(
                                    (info or {}).get("detail", "Failed to start background job")
                                    if isinstance(info, dict)
                                    else "Failed to start background job"
                                )
                        else:
                            _prog = st.progress(0)
                            _cap = st.empty()
                            _start = time.monotonic()
                            _tips = [
                                "Uploading schema and waiting for the API…",
                                "Provisioning database and DDL (if enabled)…",
                                "Loading remote table data can take several minutes…",
                            ]
                            _tip_i = 0
                            with ThreadPoolExecutor(max_workers=1) as _pool:
                                _future = _pool.submit(
                                    requests.post,
                                    f"{API_URL}/schema/from-file",
                                    data=_post_data,
                                    files=_post_files,
                                    timeout=_to,
                                )
                                while not _future.done():
                                    _elapsed = time.monotonic() - _start
                                    _pct = min(0.92, 1.0 - math.exp(-_elapsed / 42.0))
                                    _prog.progress(_pct)
                                    _cap.caption(
                                        f"{_tips[_tip_i % len(_tips)]} "
                                        f"**{int(_elapsed)}s** elapsed — still working…"
                                    )
                                    _tip_i += 1
                                    time.sleep(0.25)
                                r = _future.result()
                            _prog.progress(1.0)
                            _cap.caption("Response received — updating UI…")
                            info, jerr = safe_response_payload(r)
                            if jerr:
                                st.error(jerr)
                            elif r.ok and isinstance(info, dict):
                                st.session_state.nl_ready = True
                                st.session_state.cfg_dialog_open = False
                                st.success(
                                    f"✅ {info.get('table_count', 0)} table(s) — "
                                    f"execution: {'on' if info.get('execution_enabled') else 'off'}"
                                )
                                if info.get("hint"):
                                    st.info(info["hint"])
                                ds = info.get("data_sync")
                                if isinstance(ds, dict) and ds.get("error"):
                                    st.warning(f"Remote data sync: {ds['error']}")
                                elif isinstance(ds, dict) and ds.get("per_table"):
                                    err_list = ds.get("errors") or []
                                    with st.expander("Remote data load (per table)", expanded=bool(err_list)):
                                        st.json(ds)
                            else:
                                st.error(
                                    info.get("detail", "Activation failed")
                                    if isinstance(info, dict)
                                    else "Activation failed"
                                )
                    except Exception as ex:
                        st.error(str(ex))
        else:
            st.session_state.conn_host = st.text_input("Host", value=st.session_state.conn_host, key="in_host")
            st.session_state.conn_port = st.text_input("Port", value=st.session_state.conn_port or "5432", key="in_port")
            st.session_state.conn_user = st.text_input("Username", value=st.session_state.conn_user, key="in_user")
            st.session_state.conn_pass = st.text_input("Password", type="password", value=st.session_state.conn_pass, key="in_pass")
            st.caption("Initial DB for listing: leave blank to use your **username** as the database name.")
            st.session_state.catalog_db = st.text_input(
                "Initial database (optional)",
                value=st.session_state.catalog_db,
                key="in_catdb",
            )
            if st.button("Connect"):
                try:
                    port = int((st.session_state.conn_port or "5432").strip() or "5432")
                except ValueError:
                    st.error("Invalid port")
                    port = 5432
                body = {
                    "session_id": _sid,
                    "host": (st.session_state.conn_host or "").strip(),
                    "port": port,
                    "username": (st.session_state.conn_user or "").strip(),
                    "password": st.session_state.conn_pass or "",
                }
                if (st.session_state.catalog_db or "").strip():
                    body["catalog_database"] = st.session_state.catalog_db.strip()
                try:
                    r = requests.post(f"{API_URL}/db/connect", json=body, timeout=45)
                    info, jerr = safe_response_payload(r)
                    if jerr:
                        st.error(jerr)
                    elif r.ok and isinstance(info, dict):
                        st.session_state.db_list = info.get("databases") or []
                        st.session_state.pg_session_connected = True
                        st.success(f"Connected — {len(st.session_state.db_list)} database(s) listed.")
                    else:
                        st.error((info or {}).get("detail", "Connect failed") if isinstance(info, dict) else "Connect failed")
                except Exception as ex:
                    st.error(str(ex))

            if st.session_state.db_list:
                st.selectbox(
                    "Choose database",
                    options=st.session_state.db_list,
                    key="sb_database",
                )
                if st.button("Open this database"):
                    try:
                        r = requests.post(
                            f"{API_URL}/db/use-database",
                            json={"session_id": _sid, "database": st.session_state.sb_database},
                            timeout=45,
                        )
                        info, jerr = safe_response_payload(r)
                        if jerr:
                            st.error(jerr)
                        elif r.ok:
                            rs = requests.get(f"{API_URL}/db/schemas", params={"session_id": _sid}, timeout=45)
                            if rs.ok:
                                st.session_state.schema_list = rs.json().get("schemas") or []
                                st.success("Database opened — pick schema(s) and load tables.")
                            else:
                                st.error("Could not list schemas.")
                        else:
                            st.error((info or {}).get("detail", "Failed") if isinstance(info, dict) else "Failed")
                    except Exception as ex:
                        st.error(str(ex))

            if st.session_state.schema_list:
                st.session_state.pick_schemas = st.multiselect(
                    "Schemas",
                    options=st.session_state.schema_list,
                    default=st.session_state.pick_schemas
                    if st.session_state.pick_schemas
                    else st.session_state.schema_list[: min(5, len(st.session_state.schema_list))],
                    key="ms_schemas",
                )
                if st.button("Load tables in selected schemas"):
                    sch_param = ",".join(st.session_state.pick_schemas or [])
                    try:
                        rt = requests.get(
                            f"{API_URL}/db/tables",
                            params={"session_id": _sid, "schemas": sch_param},
                            timeout=120,
                        )
                        if rt.ok:
                            st.session_state.table_flat = rt.json().get("flat") or []
                            st.caption(f"Found **{len(st.session_state.table_flat)}** table(s).")
                        else:
                            st.error(rt.text)
                    except Exception as ex:
                        st.error(str(ex))

            if st.session_state.table_flat:
                _labels = [f'{t["schema"]}.{t["name"]}' for t in st.session_state.table_flat]
                _fp = hashlib.md5("\n".join(sorted(_labels)).encode("utf-8")).hexdigest()
                if st.session_state.get("table_catalog_fp") != _fp:
                    st.session_state.table_catalog_fp = _fp
                    st.session_state.sel_table_labels = []

                st.markdown("**Tables for NL→SQL**")
                st.caption(
                    f"**{len(_labels):,}** table(s) in this catalog. "
                    "**Select matching** adds only names that match the search. "
                    "**Select all tables** adds every loaded name. "
                    "Scroll the list below to review the full catalog."
                )

                _browse_cap = 5000
                with st.expander(
                    f"Browse all table names ({len(_labels):,}) — scroll to review",
                    expanded=len(_labels) <= 50,
                ):
                    st.dataframe(
                        pd.DataFrame({"schema.table": _labels[:_browse_cap]}),
                        use_container_width=True,
                        height=320,
                        hide_index=True,
                    )
                    if len(_labels) > _browse_cap:
                        st.caption(
                            f"Showing the first **{_browse_cap:,}** names. "
                            "Use **Search** + **Select matching** to add tables beyond this list."
                        )

                st.markdown("**Add to selection**")
                _ts = st.text_input(
                    "Search tables",
                    key="table_search_nl",
                    placeholder="Type letters — matching tables appear below instantly (e.g. cho, sales, store)",
                )
                _qq = (_ts or "").strip().lower()
                _visible = [L for L in _labels if _qq in L.lower()] if _qq else []
                st.caption("_Live filter — results update on every keystroke._")

                if _qq:
                    if _visible:
                        _live_cap = 5000
                        _show = _visible[:_live_cap]
                        st.markdown(f"**Matching tables ({len(_visible):,})**")
                        st.dataframe(
                            pd.DataFrame({"schema.table": _show}),
                            use_container_width=True,
                            height=min(340, 100 + min(len(_show), 12) * 22),
                            hide_index=True,
                        )
                        if len(_visible) > _live_cap:
                            st.caption(f"Showing first **{_live_cap:,}** matches — refine the search to narrow further.")
                    else:
                        st.warning("No table names contain that text — try another substring.")
                else:
                    st.info("Start typing above to filter the catalog; matching names will show here.")

                _m_col, _all_col, _clr_col = st.columns(3)
                if _m_col.button(
                    "Select matching",
                    use_container_width=True,
                    type="primary",
                    help="Adds every table whose name contains the search text (union with current selection).",
                ):
                    if not _qq:
                        st.warning("Enter a search term first, or use **Select all tables**.")
                    elif _visible:
                        _cur = set(st.session_state.sel_table_labels or [])
                        _cur.update(_visible)
                        st.session_state.sel_table_labels = sorted(_cur)
                        st.rerun()
                if _all_col.button(
                    "Select all tables",
                    use_container_width=True,
                    help="Add every table in the loaded catalog to the selection.",
                ):
                    st.session_state.sel_table_labels = list(_labels)
                    st.rerun()
                if _clr_col.button(
                    "Clear",
                    use_container_width=True,
                    help="Remove all tables from the selection.",
                ):
                    st.session_state.sel_table_labels = []
                    for _k in list(st.session_state.keys()):
                        if isinstance(_k, str) and _k.startswith("tcb_"):
                            del st.session_state[_k]
                    st.rerun()

                _nsel = len(st.session_state.sel_table_labels or [])
                _match = len(_visible) if _qq else 0
                st.caption(
                    f"**{_nsel:,}** in selection · **{_match:,}** match search · **{len(_labels):,}** in catalog"
                )

                _picked = st.session_state.sel_table_labels or []
                if _picked:
                    _preview_n = min(80, len(_picked))
                    with st.expander(f"Preview selected names ({len(_picked):,} total)", expanded=False):
                        st.text("\n".join(_picked[:_preview_n]) + (f"\n… +{len(_picked) - _preview_n} more" if len(_picked) > _preview_n else ""))

                if st.button("Activate selection", type="primary"):
                    parts = []
                    for L in st.session_state.sel_table_labels or []:
                        if "." in L:
                            s, n = L.split(".", 1)
                            parts.append({"schema": s.strip(), "name": n.strip()})
                    if not parts:
                        st.error("Select at least one table.")
                    else:
                        if len(parts) > 400:
                            st.caption("_Large activation — this may take several minutes._")
                        try:
                            _act_to = max(180, min(1200, 60 + len(parts) * 3))
                            act = requests.post(
                                f"{API_URL}/db/activate",
                                json={
                                    "session_id": _sid,
                                    "database": st.session_state.sb_database,
                                    "tables": parts,
                                },
                                timeout=_act_to,
                            )
                            ai, err = safe_response_payload(act)
                            if err:
                                st.error(err)
                            elif act.ok and isinstance(ai, dict):
                                st.session_state.nl_ready = True
                                st.session_state.cfg_dialog_open = False
                                st.success(f"✅ Active — **{ai.get('table_count', 0)}** table(s) for NL→SQL")
                            else:
                                st.error((ai or {}).get("detail", "Activate failed") if isinstance(ai, dict) else "Activate failed")
                        except Exception as ex:
                            st.error(str(ex))

        st.divider()
        st.subheader("⚙️ Chat settings")
        _rl_opts = streamlit_row_limit_options()
        _rl_val = (
            st.session_state.row_limit
            if st.session_state.row_limit in _rl_opts
            else _rl_opts[0]
        )
        st.session_state.top_k = st.slider(
            "Tables to retrieve (top-K)",
            1,
            10,
            int(st.session_state.top_k),
            key="slider_top_k",
        )
        st.session_state.row_limit = st.select_slider(
            "Rows per page  (used only when your question has no explicit number)",
            options=_rl_opts,
            value=_rl_val,
            key="slider_row_limit",
            help="If your question already says a number (e.g. 'top 5', 'show 20'), that number is always used instead of this slider. Every query uses LIMIT + OFFSET for pagination.",
        )

        col1, col2 = st.columns(2)
        if col1.button("🗑️ Clear Chat"):
            requests.delete(f"{API_URL}/session/{st.session_state.session_id}")
            st.session_state.chat_history = []
            st.session_state.session_id = str(uuid.uuid4())
            st.rerun()

        if col2.button("🔄 Reload schema"):
            with st.spinner("Reloading…"):
                try:
                    r = requests.post(
                        f"{API_URL}/reload-schema",
                        json={"session_id": st.session_state.session_id},
                        timeout=120,
                    )
                    if r.ok:
                        info, jerr = safe_response_payload(r)
                        if jerr or not isinstance(info, dict):
                            st.error(jerr or "Invalid JSON")
                        else:
                            st.success(f"✅ {info.get('table_count', 0)} table(s) reloaded")
                    else:
                        _b, jerr = safe_response_payload(r)
                        st.error(jerr or (_b.get("detail") if isinstance(_b, dict) else "Reload failed"))
                except Exception as ex:
                    st.error(str(ex))
            st.rerun()

        st.divider()
        try:
            _h = requests.get(
                f"{API_URL}/health",
                params={"session_id": st.session_state.session_id},
                timeout=5,
            )
            if _h.ok:
                _hd = _h.json()
                _cnt = _hd.get("table_count", 0)
                if _hd.get("activated") and _cnt > 0:
                    st.success(f"📊 NL→SQL: **{_cnt}** table(s)")
                elif _hd.get("activated"):
                    st.warning("Schema active but no tables")
                else:
                    st.info("Connect and activate above to enable chat.")
        except Exception:
            st.caption("⚠️ API not reachable")


else:
    with st.sidebar:
        st.info(
            "**Chat** needs an activated schema. Use **Configuration** above, "
            "then return here."
        )
        if st.button("← Open Configuration", use_container_width=True, key="sb_open_cfg"):
            st.session_state.ui_module = "configuration"
            st.session_state.cfg_dialog_open = False
            st.rerun()
        try:
            _h_chat = requests.get(
                f"{API_URL}/health",
                params={"session_id": st.session_state.session_id},
                timeout=5,
            )
            if _h_chat.ok:
                _hd_c = _h_chat.json()
                if _hd_c.get("activated") and _hd_c.get("has_tables"):
                    st.success(f"📊 **{_hd_c.get('table_count', 0)}** table(s) ready")
                else:
                    st.warning("Connect and activate under **Configuration**.")
            else:
                st.caption("API not reachable")
        except Exception:
            st.caption("API not reachable")

# ── Main area ─────────────────────────────────────────────────────────────────
st.title("NL → SQL")
st.caption("Natural language to SQL")

try:
    _health_ping = requests.get(
        f"{API_URL}/health",
        params={"session_id": st.session_state.session_id},
        timeout=5,
    )
except requests.exceptions.ConnectionError:
    st.warning(
        f"⚠️ Cannot connect to the API at `{API_URL}`. From folder `nl_to_sql` run: "
        f"`python -m uvicorn main:app --reload --port {_API_PORT}` "
        "(or match your configured NL_SQL_API_URL)."
    )
    st.stop()
if not _health_ping.ok:
    st.warning(
        f"⚠️ Cannot reach the API at `{API_URL}`. Start the FastAPI server "
        f"(same host/port as NL_SQL_API_URL / API_URL in .env)."
    )
    st.stop()

if st.session_state.ui_module == "configuration":
    st.subheader("Configuration")
    st.markdown(
        """
<div class="nl-banner">
  <h3>Workspace</h3>
  <p>Use the <b>Configuration</b> sidebar: connect (or upload schema), pick database → schemas → tables,
  then <b>Activate</b>. When ready, use <b>Chat</b> (top of the sidebar or the button below).</p>
</div>
""",
        unsafe_allow_html=True,
    )
    if st.session_state.pop("schema_chat_nav_blocked", False):
        st.warning(
            "**Chat** is unavailable while the background schema job is **running**. "
            "Wait until it **finishes**, or use **Pause** or **Cancel** in the sidebar — "
            "after **Pause**, you can open Chat with the schema loaded so far."
        )
    _sync_blocking_chat = _schema_activation_running_without_pause()
    if _nl_session_ready() and not _sync_blocking_chat:
        st.success(
            "✅ **Schema is active.** You can open **Chat** now — use the **💬 Chat** tab "
            "at the **top of the left sidebar** (scroll up if you only see table lists), "
            "or click **Go to Chat** here."
        )
        if st.button("Go to Chat →", type="primary", key="main_go_chat_btn"):
            st.session_state.ui_module = "chat"
            st.session_state.cfg_dialog_open = False
            st.rerun()
    elif _nl_session_ready() and _sync_blocking_chat:
        st.info(
            "⏳ **Schema activation is still running** in the sidebar (remote sync / provisioning). "
            "**Go to Chat** stays off until the job **completes** or you **Pause** or **Cancel** it. "
            "If you **Pause**, Chat opens for the tables loaded up to that point."
        )
    st.info(
        "NL→SQL is limited to the tables you activate for this session "
        "(not every table in the server unless you select them)."
    )
    st.stop()

# ── Chat module (main) ───────────────────────────────────────────────────────
st.subheader("Chat")

if _nl_session_ready():
    st.session_state.cfg_dialog_open = False

if not _nl_session_ready():
    if st.session_state.cfg_dialog_open:

        @st.dialog("Connect configuration")
        def _cfg_required_dialog():
            st.markdown(
                "No tables are visible to the app yet. Complete **Configuration** "
                "in the sidebar: connect or upload a schema, select tables, then **Activate**."
            )
            if st.button("Open Configuration", type="primary", use_container_width=True):
                st.session_state.ui_module = "configuration"
                st.session_state.cfg_dialog_open = False
                st.rerun()

        _cfg_required_dialog()
    st.warning(
        "### No active schema for NL→SQL\n\n"
        "**Configuration** (sidebar): connect to PostgreSQL or upload a schema JSON, "
        "pick tables, then **Activate**. Then return to **Chat**."
    )
    st.caption("_Load tables under Configuration to enable chat._")
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
        params = {
            "session_id": st.session_state.session_id,
            "last_query": last_query,
        }
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
                                json={
                                    "sql": sql,
                                    "session_id": st.session_state.session_id,
                                    "page": 1,
                                    "page_size": total_count,
                                },
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
                                    json={
                                        "sql": sql,
                                        "session_id": st.session_state.session_id,
                                        "page": cur_page - 1,
                                        "page_size": ps,
                                    },
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
                                    json={
                                        "sql": sql,
                                        "session_id": st.session_state.session_id,
                                        "page": cur_page + 1,
                                        "page_size": ps,
                                    },
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
                "top_k":      int(st.session_state.top_k),
                "row_limit":  int(st.session_state.row_limit),
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