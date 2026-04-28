"""
NL → SQL workbench — shared UI for the Configuration and Chat pages.
"""
from __future__ import annotations

import sys
from pathlib import Path

# `nl_to_sql/` must be on sys.path so `utils` and sibling imports resolve.
_D = Path(__file__).resolve().parent
if str(_D) not in sys.path:
    sys.path.insert(0, str(_D))
from ensure_path import install

install()

import hashlib
import html
import json
import math
import re
import time
import uuid
from difflib import get_close_matches
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests
import streamlit as st

from utils.config import (
    nl_sql_api_url,
    remote_sync_default_row_limit,
    sql_max_page_size,
    streamlit_row_limit_options,
    ui_schema_table_browse_limit,
)

try:
    from utils.config import allow_data_ingestion_to_connected_db
except ImportError:  # older utils/config.py or stale copy — keep default-off behaviour
    import os

    def allow_data_ingestion_to_connected_db() -> bool:
        v = (os.getenv("SMART_QUERY_ALLOW_DATA_INGESTION", "") or "").strip().lower()
        return v in ("1", "true", "yes", "on")
from utils.env import load_app_env
from utils.http import safe_response_payload
from schema.file_schema import schema_from_uploaded_json

load_app_env()


API_URL = nl_sql_api_url()
_API_PORT = urlparse(API_URL).port or (443 if urlparse(API_URL).scheme == "https" else 80)




from ui.auth.session import clear_auth_session
from ui.sidebar_icons import (
    SIDEBAR_CHAT,
    SIDEBAR_COMPANIES,
    SIDEBAR_CONFIGURATION,
    SIDEBAR_OPEN_PROJECT,
    SIDEBAR_PROJECTS,
)
from ui.tenant.project_context import apply_project_workspace, get_active_project_id
from ui.tenant.state import (
    ensure_tenant_state,
    find_project_by_id,
    get_tenant_by_id,
    update_project_nl_session_id,
)

_NL_WB_PAGE = "configuration"

def set_workbench_page(name: str) -> None:
    global _NL_WB_PAGE
    if name in ("configuration", "chat"):
        _NL_WB_PAGE = name


def workbench_page() -> str:
    return _NL_WB_PAGE


def _db_connect_request_body() -> dict:
    """Build JSON for ``POST /db/connect`` (direct or via optional SSH tunnel)."""
    _sid = st.session_state.session_id
    try:
        port = int((st.session_state.conn_port or "5432").strip() or "5432")
    except ValueError:
        port = 5432
    body: dict = {
        "session_id": _sid,
        "host": (st.session_state.conn_host or "").strip(),
        "port": port,
        "username": (st.session_state.conn_user or "").strip(),
        "password": st.session_state.conn_pass or "",
    }
    if (st.session_state.catalog_db or "").strip():
        body["catalog_database"] = st.session_state.catalog_db.strip()
    if st.session_state.get("conn_use_ssh") and (st.session_state.get("conn_ssh_key") or "").strip():
        body["ssh_host"] = (st.session_state.get("conn_ssh_host") or "").strip()
        try:
            body["ssh_port"] = int((st.session_state.get("conn_ssh_port") or "22").strip() or 22)
        except ValueError:
            body["ssh_port"] = 22
        _su = (st.session_state.get("conn_ssh_user") or "").strip()
        body["ssh_username"] = _su or (st.session_state.conn_user or "").strip()
        body["ssh_private_key"] = (st.session_state.get("conn_ssh_key") or "").strip()
        if (st.session_state.get("conn_ssh_key_pass") or "").strip():
            body["ssh_private_key_passphrase"] = (st.session_state.get("conn_ssh_key_pass") or "").strip()
    return body


def _apply_uploaded_ssh_key_to_session(uploaded: Any) -> None:
    """Load an uploaded private-key file into ``conn_ssh_key`` (UTF-8, lenient on errors)."""
    if uploaded is None:
        return
    try:
        raw: bytes = uploaded.getvalue()
    except Exception:
        return
    if not raw:
        return
    st.session_state["conn_ssh_key"] = raw.decode("utf-8", errors="replace")


def _run_configuration_db_connect() -> None:
    """POST ``/db/connect`` from the Configuration panel (Local or Server)."""
    # Backend default is: initial_db = (catalog_database or username).
    # In the UI we collect "Database name" as ``file_db_name``; use it as the catalog DB
    # unless the user explicitly provided a different catalog DB.
    if not (st.session_state.get("catalog_db") or "").strip():
        _dbn = (st.session_state.get("file_db_name") or "").strip()
        if _dbn:
            st.session_state["catalog_db"] = _dbn
    body = _db_connect_request_body()
    try:
        r = requests.post(f"{API_URL}/db/connect", json=body, timeout=90)
        info, jerr = safe_response_payload(r)
        if jerr:
            _cfg_notify_error_with_help(str(jerr), "Connection failed")
        elif r.ok and isinstance(info, dict):
            st.session_state.db_list = info.get("databases") or []
            st.session_state.pg_session_connected = True
            _sfx = " (via SSH tunnel)" if info.get("via_ssh") else ""
            _cfg_notify(
                "success",
                f"Connected — {len(st.session_state.db_list)} database(s) listed.{_sfx}",
            )
        else:
            _cfg_notify_error_with_help(
                (info or {}).get("detail", "Connect failed") if isinstance(info, dict) else "Connect failed",
                "Connection failed",
            )
    except Exception as ex:
        _cfg_notify_error_with_help(str(ex), "Connection failed")


def _persist_activated_schema_to_app() -> None:
    """Cache activated table/column metadata in the app database (``app_project_schema_cache``)."""
    auth = st.session_state.get("auth_user") or {}
    uid = auth.get("user_id")
    pid = get_active_project_id()
    if uid is None or not pid:
        return
    try:
        r = requests.get(
            f"{API_URL}/schema",
            params={"session_id": st.session_state.session_id},
            timeout=90,
        )
        if not r.ok:
            return
        data = r.json()
        if not data:
            return
        from workspace_store import save_project_schema_cache

        save_project_schema_cache(int(uid), str(pid), data)
    except Exception:
        pass


def _render_workbench_sidebar_brand_and_links() -> None:
    """Shared sidebar chrome (Smart Query, workspace nav). Call inside ``with st.sidebar:`` — same as Tenant Dashboard / Projects page."""
    _auth = st.session_state.auth_user or {}
    _uname = str(_auth.get("username", "user") or "user")
    _init = (html.escape(_uname[:1] or "?")).upper()
    _display = html.escape(_uname)
    st.markdown(
        f"""
        <div class="sqg-sb-top" style="display:flex;align-items:center;gap:0.5rem">
          <span style="display:flex;width:30px;height:30px;border-radius:8px;background:#5b21b6;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.12)">
            <span style="display:flex;flex-direction:column;gap:2px;align-items:flex-start;justify-content:center">
              <span style="height:2px;width:12px;background:#fff;border-radius:1px"></span>
              <span style="height:2px;width:8px;background:#fff;border-radius:1px;opacity:0.95"></span>
              <span style="height:2px;width:10px;background:#fff;border-radius:1px;opacity:0.9"></span>
            </span>
          </span>
          <span class="sqg-sb-brand" style="margin:0">Smart Query</span>
        </div>
        <div class="sqg-sb-user">
          <div class="sqg-sb-av">{_init}</div>
          <div>
            <div class="sqg-sb-name">{_display}</div>
            <div class="sqg-sb-role">Member</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("WORKSPACE")
    st.page_link("pages/dashboard.py", label="Projects", icon=SIDEBAR_PROJECTS)
    st.page_link("pages/tenants.py", label="Companies", icon=SIDEBAR_COMPANIES)
    st.page_link("pages/project_open.py", label="Open project", icon=SIDEBAR_OPEN_PROJECT)
    st.page_link("pages/project_chat.py", label="Chat", icon=SIDEBAR_CHAT)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon=SIDEBAR_CONFIGURATION)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)


def _render_workbench_sidebar_shell(signout_key: str) -> None:
    with st.sidebar:
        _render_workbench_sidebar_brand_and_links()
        if st.button("Sign out", use_container_width=True, type="secondary", key=signout_key):
            clear_auth_session()
            st.switch_page("pages/signin.py")
            st.stop()

def _schema_active(h: dict) -> bool:
    return bool((h or {}).get("activated") and (h or {}).get("has_tables"))


def _nl_session_ready() -> bool:
    """True when the API reports an activated schema with at least one table (see ``_workbench_health`` in ``run``)."""
    d = st.session_state.get("_workbench_health")
    if not d:
        return False
    return _schema_active(d)


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


def _quote_ident(name: str) -> str:
    """
    Quote PostgreSQL identifier safely (supports dashed names like `pdf-content-control`).
    """
    n = (name or "").strip()
    if not n:
        raise ValueError("Identifier is empty.")
    return '"' + n.replace('"', '""') + '"'


def _parse_uploaded_tables_for_preview(file_obj: Any) -> list[dict[str, Any]]:
    """Extract table list from uploaded schema JSON for remote count preview."""
    if not file_obj:
        return []
    raw = file_obj.getvalue()
    data = json.loads(raw.decode("utf-8"))
    schema, _sk = schema_from_uploaded_json(data)
    out: list[dict[str, Any]] = []
    for key, meta in (schema.get("tables") or {}).items():
        sch = (meta.get("schema_name") or "").strip() or (str(key).split(".", 1)[0] if "." in str(key) else "public")
        tbl = (meta.get("table_name") or "").strip() or (str(key).split(".", 1)[-1])
        cols = meta.get("columns") or []
        out.append(
            {
                "key": f"{sch}.{tbl}",
                "schema": sch,
                "table": tbl,
                "columns": len(cols) if isinstance(cols, list) else 0,
            }
        )
    return out


def _fetch_remote_table_count(api_url: str, schema_name: str, table_name: str) -> int | None:
    """Run SELECT COUNT(*) against remote SQL API."""
    q = f"SELECT COUNT(*) AS record_count FROM {_quote_ident(schema_name)}.{_quote_ident(table_name)}"
    r = requests.post(
        (api_url or "").strip(),
        json={"query": q},
        headers={"Content-Type": "application/json"},
        timeout=45,
    )
    r.raise_for_status()
    body = r.json()
    rows = body if isinstance(body, list) else (
        body.get("rows")
        or body.get("data")
        or body.get("result")
        or body.get("records")
        or body.get("entity")
        or []
    )
    if not isinstance(rows, list) or not rows:
        return 0
    row0 = rows[0] if isinstance(rows[0], dict) else {}
    if not isinstance(row0, dict):
        return 0
    for k in ("record_count", "count", "COUNT", "count(*)", "COUNT(*)"):
        if k in row0:
            try:
                return int(row0[k] or 0)
            except Exception:
                return 0
    for v in row0.values():
        try:
            return int(v or 0)
        except Exception:
            continue
    return 0


def _fetch_remote_rows(api_url: str, query: str, timeout: int = 60) -> list[dict[str, Any]]:
    """Run SQL against remote passthrough API and normalize row payload."""
    r = requests.post(
        (api_url or "").strip(),
        json={"query": query},
        headers={"Content-Type": "application/json"},
        timeout=timeout,
    )
    r.raise_for_status()
    body = r.json()
    rows = body if isinstance(body, list) else (
        body.get("rows")
        or body.get("data")
        or body.get("result")
        or body.get("records")
        or body.get("entity")
        or []
    )
    return rows if isinstance(rows, list) else []


def _reset_connection_runtime_state() -> None:
    """Clear connection/schema runtime state; keep user preferences."""
    for k, v in {
        "db_list": [],
        "pick_database": "",
        "schema_list": [],
        "pick_schemas": [],
        "table_flat": [],
        "pick_tables": [],
        "sel_table_labels": [],
        "table_catalog_fp": "",
        "nl_ready": False,
        "file_db_name": "",
        "pg_session_connected": False,
        "schema_activation_job_id": None,
        "schema_job_result": None,
        "schema_job_error": None,
        "schema_job_paused": False,
        "schema_upload_need_connection_help": False,
        "remote_conn_url": "",
        "remote_conn_db_list": [],
        "remote_conn_schema_list": [],
        "remote_conn_table_list": [],
        "remote_conn_selected_db": "",
        "remote_conn_selected_schema": "",
        "remote_conn_selected_tables": [],
        "remote_conn_load_mode": "Load limited data",
        "remote_conn_row_limit": int(remote_sync_default_row_limit()),
        "_workbench_health": {},
    }.items():
        st.session_state[k] = v
    st.session_state.pop("sb_database", None)
    st.session_state.pop("pending_new_db_name_upload", None)
    st.session_state.pop("remote_preview_summary", None)
    # Clear per-result paging/chart caches.
    for _k in list(st.session_state.keys()):
        if _k.startswith("pg_") or _k.startswith("rows_") or _k.startswith("chart_") or _k.startswith("page_sz_"):
            st.session_state.pop(_k, None)
    st.session_state.chat_history = []
    st.session_state.suggested_prompts = []
    st.session_state.prompts_last_query = ""


def _delete_api_session_if_exists(session_id: str) -> None:
    sid = (session_id or "").strip()
    if not sid:
        return
    try:
        requests.delete(f"{API_URL}/session/{sid}", timeout=15)
    except Exception:
        pass


def _start_new_connection_session() -> None:
    """Rotate to a brand-new API session for the active project."""
    old_sid = (st.session_state.get("session_id") or "").strip()
    _delete_api_session_if_exists(old_sid)
    new_sid = str(uuid.uuid4())
    st.session_state["session_id"] = new_sid
    pid = get_active_project_id() or ""
    if pid:
        update_project_nl_session_id(pid, new_sid)
    _reset_connection_runtime_state()


def _disconnect_current_connection_session() -> None:
    """Disconnect current API session and clear connection/schema state."""
    old_sid = (st.session_state.get("session_id") or "").strip()
    _delete_api_session_if_exists(old_sid)
    _reset_connection_runtime_state()


def _apply_chat_schema_change(database: str, schemas: list[str]) -> tuple[bool, str]:
    """Switch active DB/schema selection and activate matching tables for chat."""
    sid = (st.session_state.get("session_id") or "").strip()
    db = (database or "").strip()
    sch = [str(s).strip() for s in (schemas or []) if str(s).strip()]
    if not sid:
        return False, "No active session."
    if not db:
        return False, "Choose a database."
    if not sch:
        return False, "Choose at least one schema."
    try:
        r_use = requests.post(
            f"{API_URL}/db/use-database",
            json={"session_id": sid, "database": db},
            timeout=45,
        )
        use_body, use_err = safe_response_payload(r_use)
        if use_err:
            return False, use_err
        if not r_use.ok:
            return False, (use_body or {}).get("detail", "Could not switch database.") if isinstance(use_body, dict) else "Could not switch database."

        r_tbl = requests.get(
            f"{API_URL}/db/tables",
            params={"session_id": sid, "schemas": ",".join(sch)},
            timeout=90,
        )
        tbl_body, tbl_err = safe_response_payload(r_tbl)
        if tbl_err:
            return False, tbl_err
        if not r_tbl.ok:
            return False, (tbl_body or {}).get("detail", "Could not load tables.") if isinstance(tbl_body, dict) else "Could not load tables."
        flat = (tbl_body or {}).get("flat") if isinstance(tbl_body, dict) else []
        if not flat:
            return False, "No tables found for the selected schema(s)."

        parts = [
            {"schema": str(t.get("schema") or "").strip(), "name": str(t.get("name") or "").strip()}
            for t in flat
            if str(t.get("schema") or "").strip() and str(t.get("name") or "").strip()
        ]
        if not parts:
            return False, "No valid tables found for activation."

        r_act = requests.post(
            f"{API_URL}/db/activate",
            json={"session_id": sid, "database": db, "tables": parts},
            timeout=max(180, min(1200, 60 + len(parts) * 3)),
        )
        act_body, act_err = safe_response_payload(r_act)
        if act_err:
            return False, act_err
        if not r_act.ok:
            return False, (act_body or {}).get("detail", "Could not activate selected schema.") if isinstance(act_body, dict) else "Could not activate selected schema."

        st.session_state.sb_database = db
        st.session_state.file_db_name = db
        st.session_state.schema_list = sch
        st.session_state.pick_schemas = sch
        st.session_state.table_flat = flat
        st.session_state.sel_table_labels = [f'{t["schema"]}.{t["name"]}' for t in flat if t.get("schema") and t.get("name")]
        st.session_state.pg_session_connected = True
        st.session_state.nl_ready = True
        # Schema switched: start a fresh chat thread for the new context.
        st.session_state.chat_history = []
        st.session_state.suggested_prompts = []
        st.session_state.prompts_last_query = ""
        for _k in list(st.session_state.keys()):
            if _k.startswith("pg_") or _k.startswith("rows_") or _k.startswith("chart_") or _k.startswith("page_sz_"):
                st.session_state.pop(_k, None)
        _persist_activated_schema_to_app()
        return True, f"Schema switched: {db} ({len(parts)} table(s) active)."
    except Exception as ex:
        return False, str(ex)


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


def _on_configuration_source_changed() -> None:
    """Connection type changed: switch main panel to setup (hide Getting started)."""
    st.session_state["cfg_show_connection_main"] = True
    # Fresh form/session view when user switches connection mode.
    # Keeps chat/session history untouched; clears only connection wizard state.
    for _k, _v in {
        "pg_session_connected": False,
        "db_list": [],
        "schema_list": [],
        "table_flat": [],
        "pick_schemas": [],
        "pick_tables": [],
        "sel_table_labels": [],
        "table_catalog_fp": "",
        "sb_database": "",
        "file_db_name": "",
        "catalog_db": "",
        "schema_activation_job_id": None,
        "schema_job_result": None,
        "schema_job_error": None,
        "schema_upload_need_connection_help": False,
        "conn_host": "",
        "conn_port": "5432",
        "conn_user": "",
        "conn_pass": "",
        "conn_use_ssh": False,
        "conn_ssh_host": "",
        "conn_ssh_port": "22",
        "conn_ssh_user": "",
        "conn_ssh_key": "",
        "conn_ssh_key_pass": "",
        "remote_conn_url": "",
        "remote_conn_db_list": [],
        "remote_conn_schema_list": [],
        "remote_conn_table_list": [],
        "remote_conn_selected_db": "",
        "remote_conn_selected_schema": "",
        "remote_conn_selected_tables": [],
        "remote_conn_load_mode": "Load limited data",
        "remote_conn_row_limit": int(remote_sync_default_row_limit()),
        "remote_conn_counts_summary": {},
    }.items():
        st.session_state[_k] = _v


def _cfg_notify(level: str, message: str) -> None:
    """Queue configuration notifications (auto-hide after 2 minutes)."""
    lvl = (level or "info").strip().lower()
    if lvl not in {"success", "error", "warning", "info"}:
        lvl = "info"
    q = st.session_state.setdefault("_cfg_notifications", [])
    q.append(
        {
            "id": uuid.uuid4().hex[:10],
            "level": lvl,
            "message": str(message or ""),
            "created_at": time.time(),
        }
    )
    st.session_state["_cfg_notifications"] = q[-8:]


def _cfg_notify_once(key: str, level: str, message: str) -> None:
    """Queue once per key until key is cleared/replaced."""
    skey = "_cfg_notifications_seen_keys"
    seen = st.session_state.setdefault(skey, set())
    if key in seen:
        return
    _cfg_notify(level, message)
    seen.add(key)
    st.session_state[skey] = seen


def _cfg_troubleshoot_hint(err: str) -> str:
    """Map common upload/connection errors to actionable hints."""
    e = str(err or "").lower()
    if "no pg_hba.conf entry" in e:
        return "PostgreSQL server rejected your client IP. Add a `pg_hba.conf` rule for this host/user/database and reload PostgreSQL."
    if "password authentication failed" in e:
        return "Username/password is wrong for this database. Verify credentials and DB name."
    if "connection refused" in e or "could not connect to server" in e:
        return "Host/port is unreachable. Check server status, network route, firewall, and port."
    if "ssl" in e and ("required" in e or "encryption" in e):
        return "SSL settings mismatch between client and server. Enable required SSL mode/certs on server/client."
    if "invalid identifier" in e:
        return "Identifier contains special characters. Keep quoted identifier handling enabled and retry metadata fetch."
    if "timed out" in e or "timeout" in e:
        return "Request timed out. Try again, reduce scope (schema/table count), or increase server timeout."
    if "json" in e and ("invalid" in e or "decode" in e):
        return "Remote API response format is unexpected. Ensure endpoint returns JSON rows for `{\"query\": \"...\"}` payload."
    if "403" in e or "forbidden" in e:
        return "Access denied by remote service. Check token/permissions/IP allow-list."
    if "404" in e:
        return "Endpoint path is wrong. Recheck the remote URL route."
    if "500" in e:
        return "Remote server internal error. Retry later or inspect upstream service logs."
    return "Check URL, credentials, DB access policy, and remote API response shape. If it persists, share the exact error text."


def _cfg_notify_error_with_help(err: str, prefix: str = "Operation failed") -> None:
    err_text = _detail_text(err, "Unknown error")
    msg = f"{prefix}: {err_text}"
    hint = _cfg_troubleshoot_hint(err_text)
    _cfg_notify("error", msg)
    _cfg_notify("warning", f"Troubleshooter: {hint}")


def _detail_text(payload: Any, default: str) -> str:
    """Extract readable error detail from API payload shapes (dict/list/str)."""
    if isinstance(payload, dict):
        return str(payload.get("detail") or payload.get("message") or default)
    if isinstance(payload, list):
        parts: list[str] = []
        for item in payload[:6]:
            if isinstance(item, dict):
                loc = item.get("loc")
                msg = item.get("msg") or item.get("detail") or item
                if isinstance(loc, list):
                    parts.append(f"{'.'.join(str(x) for x in loc)}: {msg}")
                else:
                    parts.append(str(msg))
            else:
                parts.append(str(item))
        return "; ".join(parts) if parts else default
    if isinstance(payload, str):
        return payload
    return default


def _render_cfg_notifications() -> None:
    """Render floating notifications; each auto-hides after 120 seconds."""
    q = list(st.session_state.get("_cfg_notifications") or [])
    if not q:
        return
    now = time.time()
    keep = [n for n in q if now - float(n.get("created_at") or 0) < 120]
    st.session_state["_cfg_notifications"] = keep
    if not keep:
        return
    st.markdown(
        """
        <style>
        .cfg-toast-wrap { position: fixed; top: 0.95rem; right: 1rem; z-index: 9999; width: min(420px, 92vw); }
        .cfg-toast { border-radius: 10px; padding: 0.62rem 0.78rem; margin-bottom: 0.5rem; border: 1px solid transparent;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.16); font-size: 0.9rem; line-height: 1.35; }
        .cfg-toast--success { background: #ecfdf5; border-color: #10b981; color: #065f46; }
        .cfg-toast--warning, .cfg-toast--error { background: #fef2f2; border-color: #ef4444; color: #991b1b; }
        .cfg-toast--info { background: #eff6ff; border-color: #60a5fa; color: #1e3a8a; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    html_parts = ['<div class="cfg-toast-wrap">']
    for n in keep:
        nid = html.escape(str(n.get("id") or ""))
        lvl = html.escape(str(n.get("level") or "info"))
        msg = html.escape(str(n.get("message") or ""))
        html_parts.append(
            f'<div id="cfg-toast-{nid}" class="cfg-toast cfg-toast--{lvl}">{msg}</div>'
            f"<script>setTimeout(function(){{var e=document.getElementById('cfg-toast-{nid}'); if(e) e.remove();}}, 120000);</script>"
        )
    html_parts.append("</div>")
    st.markdown("".join(html_parts), unsafe_allow_html=True)


def _cfg_source_options() -> tuple[str, str, str]:
    return (
        "Local DB connection",
        "Server connection",
        "Remote URL connection",
    )


def _apply_cfg_source_selection(source_label: str) -> None:
    # Current redesigned Configuration keeps a single credentials workflow.
    st.session_state.conn_source = "live"


def _render_cfg_source_selector() -> str:
    """Render a redesigned connection-type selector card."""
    _cfg_opts = _cfg_source_options()
    if st.session_state.get("cfg_schema_source") not in _cfg_opts:
        st.session_state["cfg_schema_source"] = _cfg_opts[0]
    _current = st.session_state.get("cfg_schema_source") or _cfg_opts[0]
    _active_idx = _cfg_opts.index(_current) if _current in _cfg_opts else 0
    _style_block = """
        <style>
        section.main .cfg-source-card {
            border: 1px solid #e5e7eb !important;
            border-radius: 12px !important;
            background: #ffffff !important;
            padding: 0.85rem !important;
            margin: 0.2rem 0 0.6rem 0 !important;
        }
        section.main .cfg-source-card-title {
            margin: 0 0 0.5rem 0 !important;
            font-size: 0.72rem !important;
            font-weight: 700 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
            color: #6b7280 !important;
        }
        section.main .st-key-cfg_source_btn_0,
        section.main .st-key-cfg_source_btn_1,
        section.main .st-key-cfg_source_btn_2 {
            margin: 0 0 0.35rem 0 !important;
        }
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"],
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"],
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"] {
            min-height: 2.25rem !important;
            justify-content: flex-start !important;
            padding-left: 0.7rem !important;
            border-radius: 8px !important;
            background: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            box-shadow: none !important;
        }
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"] p,
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"] p,
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"] p {
            font-size: 0.88rem !important;
            font-weight: 500 !important;
            margin: 0 !important;
            color: #374151 !important;
            text-align: left !important;
            width: 100% !important;
        }
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"]:hover,
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"]:hover,
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"]:hover {
            border-color: #d1d5db !important;
            background: #fafafa !important;
        }
        section.main .st-key-cfg_source_btn___ACTIVE__ [data-baseweb="button"] {
            border: 1.5px solid #7c3aed !important;
            background: #f5f3ff !important;
        }
        section.main .st-key-cfg_source_btn___ACTIVE__ [data-baseweb="button"] p {
            color: #1f2937 !important;
            font-weight: 600 !important;
        }
        </style>
        """
    st.markdown(_style_block.replace("__ACTIVE__", str(_active_idx)), unsafe_allow_html=True)
    st.markdown('<div class="cfg-source-card"><p class="cfg-source-card-title">Connection Type</p>', unsafe_allow_html=True)
    for _ix, _opt in enumerate(_cfg_opts):
        _is_active = _current == _opt
        _prefix = "◉" if _is_active else "○"
        _label = f"{_prefix}  {_opt}"
        if st.button(
            _label,
            key=f"cfg_source_btn_{_ix}",
            use_container_width=True,
            type="secondary",
        ):
            st.session_state["cfg_schema_source"] = _opt
            _on_configuration_source_changed()
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)
    return _current


def _render_configuration_connection_main(sid: str) -> None:
    """Connect / upload / activate UI (main column)."""
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

        _um_cur = (st.session_state.get("schema_json_upload_mode") or "") or ""
        if "File only" in _um_cur:
            st.session_state.pop("schema_json_upload_mode", None)
        _opt_connect = "Connect here — host / user / Connect, then database + JSON"
        _opt_paired = "Already connected — I used **Connect database (PostgreSQL)** at the top"
        if st.session_state.pop("_pending_schema_upload_mode_connect_here", False):
            st.session_state["schema_json_upload_mode"] = _opt_connect
        _upload_mode = st.radio(
            "How do you want to use PostgreSQL?",
            (_opt_connect, _opt_paired),
            key="schema_json_upload_mode",
            help=(
                "Activation always requires a working PostgreSQL **session** (host validated). "
                "Connect here, or use **Source → Connect database** first, then return to this flow."
            ),
        )
        _embed_connect = "Connect here" in _upload_mode

        if _embed_connect:
            st.markdown("##### 1 — Connect to PostgreSQL")
            st.caption("Enter PostgreSQL credentials and validate the connection first.")
            st.session_state.conn_host = st.text_input("Host", value=st.session_state.conn_host, key="in_host")
            st.session_state.conn_port = st.text_input("Port", value=st.session_state.conn_port or "5432", key="in_port")
            st.session_state.conn_user = st.text_input("Username", value=st.session_state.conn_user, key="in_user")
            st.session_state.conn_pass = st.text_input("Password", type="password", value=st.session_state.conn_pass, key="in_pass")
            with st.expander("SSH tunnel (optional — same as **Connect database** path)"):
                st.checkbox("Connect through SSH (private key)", key="conn_use_ssh")
                st.text_input("Bastion / SSH host", key="conn_ssh_host")
                st.text_input("SSH port", key="conn_ssh_port")
                st.text_input("SSH username (default: same as database user, if left empty here)", key="conn_ssh_user")
                _f_embed = st.file_uploader(
                    "Upload private key",
                    type=None,
                    key="file_embed_ssh_key_upload",
                    help="PEM or OpenSSH private key. Fills the box below; you can still paste instead.",
                )
                _apply_uploaded_ssh_key_to_session(_f_embed)
                st.text_area("SSH private key (PEM) — or paste", height=100, key="conn_ssh_key")
                st.text_input("Key passphrase (if any)", type="password", key="conn_ssh_key_pass")
            if st.button("Connect", key="btn_connect_file_upload", type="primary"):
                body = _db_connect_request_body()
                try:
                    r = requests.post(f"{API_URL}/db/connect", json=body, timeout=90)
                    info, jerr = safe_response_payload(r)
                    if jerr:
                        st.error(jerr)
                    elif r.ok and isinstance(info, dict):
                        st.session_state.db_list = info.get("databases") or []
                        st.session_state.pg_session_connected = True
                        st.session_state.schema_list = []
                        st.session_state.schema_upload_need_connection_help = False
                        _sfx = " (via SSH tunnel)" if info.get("via_ssh") else ""
                        st.success(
                            f"Connection valid — {len(st.session_state.db_list)} database(s) found.{_sfx}"
                        )
                    else:
                        st.error((info or {}).get("detail", "Connect failed") if isinstance(info, dict) else "Connect failed")
                except Exception as ex:
                    st.error(str(ex))

            if st.session_state.get("pg_session_connected"):
                st.markdown("##### 2 — Choose target database")
                st.caption("After connection is valid, select an existing database or provide a new one to create during activation.")
                # Apply a new name before st.selectbox is created. Streamlit forbids assigning
                # st.session_state.sb_database after the selectbox (key=sb_database) is built.
                _db_opts: list = list(st.session_state.get("db_list") or [])
                _pnd = st.session_state.pop("pending_new_db_name_upload", None)
                if _pnd and str(_pnd).strip():
                    _pn = str(_pnd).strip()
                    if _pn not in _db_opts:
                        _db_opts = [_pn] + _db_opts
                    st.session_state.sb_database = _pn
                if _db_opts and st.session_state.get("sb_database") not in _db_opts:
                    st.session_state.sb_database = _db_opts[0]
                if _db_opts:
                    st.selectbox(
                        "Choose database",
                        options=_db_opts,
                        key="sb_database",
                    )
                    if st.button("Open this database", key="btn_open_db_file_upload"):
                        try:
                            r = requests.post(
                                f"{API_URL}/db/use-database",
                                json={"session_id": sid, "database": st.session_state.sb_database},
                                timeout=45,
                            )
                            info, jerr = safe_response_payload(r)
                            if jerr:
                                st.error(jerr)
                            elif r.ok:
                                try:
                                    rs = requests.get(
                                        f"{API_URL}/db/schemas",
                                        params={"session_id": sid},
                                        timeout=45,
                                    )
                                    if rs.ok:
                                        st.session_state.schema_list = rs.json().get("schemas") or []
                                    else:
                                        st.session_state.schema_list = []
                                except Exception:
                                    st.session_state.schema_list = []
                                st.success("Database opened — schema list loaded.")
                            else:
                                st.error((info or {}).get("detail", "Failed") if isinstance(info, dict) else "Failed")
                        except Exception as ex:
                            st.error(str(ex))
                _use_new_db = st.checkbox(
                    "Use a new database name instead of the selected database",
                    key="use_new_db_upload",
                    help="Off: target database is taken from 'Choose database'. On: enter a new DB name below.",
                )
                if not _use_new_db and (st.session_state.get("sb_database") or "").strip():
                    st.session_state.file_db_name = (st.session_state.get("sb_database") or "").strip()
                    st.caption(f"Target database is set to selected DB: `{st.session_state.file_db_name}`")
                if _use_new_db:
                    _new_db = st.text_input(
                        "New DB name (optional)",
                        key="new_db_name_upload",
                        placeholder="e.g. analytics_db",
                    )
                    if _new_db.strip():
                        if st.button("Use new database name", key="btn_use_new_db_upload"):
                            st.session_state["pending_new_db_name_upload"] = _new_db.strip()
                            st.session_state.file_db_name = _new_db.strip()
                            st.session_state.schema_list = []
                            st.rerun()

            if st.session_state.get("pg_session_connected") and st.session_state.get("schema_list"):
                st.multiselect(
                    "Schemas (detected)",
                    options=st.session_state.schema_list,
                    default=st.session_state.schema_list[: min(6, len(st.session_state.schema_list))],
                    disabled=True,
                    key="ms_schemas_upload_preview",
                )
        else:
            st.markdown("##### Using a connection you opened elsewhere")
            st.caption(
                "Use this if you already clicked **Connect & activate** under **Source → Connect database (PostgreSQL)**. "
                "The same host/user session is reused; you do not enter credentials again here."
            )
            if st.session_state.get("pg_session_connected"):
                st.success("**Connected** — you can set the target database name and upload the JSON file below.")
            else:
                st.warning(
                    "Not connected yet. Open **Source** at the top, choose **Connect database (PostgreSQL)**, "
                    "enter host/user, click **Connect & activate**, then return. "
                    "Or select **Connect here** in the options above and complete **Connect** first."
                )
                _m1, _m2 = st.columns(2)
                if _m1.button("Connect here now", key="upload_mode_connect_here_now", use_container_width=True):
                    st.session_state["_pending_schema_upload_mode_connect_here"] = True
                    st.rerun()
                if _m2.button("Open Connect database flow", key="upload_mode_open_live_flow", use_container_width=True):
                    st.session_state["_pending_cfg_source_live"] = True
                    st.session_state.conn_source = "live"
                    st.rerun()
                # In "Already connected" mode, hide step 3 until a live PG session exists.
                return

        st.divider()
        st.markdown("##### 3 — Target database + schema JSON")
        if not st.session_state.get("pg_session_connected"):
            st.info(
                "**Connect to PostgreSQL first** (Connect here, or **Connect database** at the top). "
                "Target name and file upload are enabled after a successful connection."
            )
        _pg_ok = bool(st.session_state.get("pg_session_connected"))
        _selected_db_name = (st.session_state.get("sb_database") or "").strip()
        _use_new_db = bool(st.session_state.get("use_new_db_upload"))
        _auto_target_from_selected = bool(_pg_ok and _selected_db_name and not _use_new_db)
        if _auto_target_from_selected:
            st.session_state.file_db_name = _selected_db_name
            st.info(f"Target database: `{_selected_db_name}` (from **Choose database** above).")
        else:
            st.session_state.file_db_name = st.text_input(
                "Target database name (required)",
                value=st.session_state.file_db_name,
                key="sf_dbname",
                disabled=not _pg_ok,
                help="PostgreSQL database name (created or used when the schema is applied to the server).",
            )
        _target_db_name = (st.session_state.file_db_name or "").strip()
        _existing_db_names = {
            str(x).strip().lower()
            for x in (st.session_state.get("db_list") or [])
            if str(x).strip()
        }
        _target_exists_in_connected_db = (
            bool(st.session_state.get("pg_session_connected"))
            and bool(_target_db_name)
            and _target_db_name.lower() in _existing_db_names
        )
        if _target_exists_in_connected_db:
            st.warning(
                f"Database `{_target_db_name}` already exists in the connected PostgreSQL instance. "
                "Choose a different target database name before activation."
            )
        _rd = remote_sync_default_row_limit()
        _ingestion_ok = allow_data_ingestion_to_connected_db()
        st.caption(
            "**Optional — materialize on PostgreSQL + load rows from a remote API** "
            "(default off; for metadata-only upload you can leave this unchanged.)"
        )
        if not _ingestion_ok:
            st.info(
                "The controls below are **read-only** until you set `SMART_QUERY_ALLOW_DATA_INGESTION=1` in the project **`.env`** "
                "and **restart** this Streamlit app **and** the API. "
                "Use **Source → Connect database (PostgreSQL)** to query a live client DB without this option."
            )
        _remote_url = ""
        _remote_limit = str(_rd)
        _load_all_rows = False
        _provision_cb = st.checkbox(
            "Create database & tables on PostgreSQL from this JSON (server DDL)",
            key="provision_pg_ddl",
            disabled=not _ingestion_ok,
            help=(
                "Connect first (use **Connect database → Connect**, or **Connect here** above). "
                "Runs CREATE DATABASE if missing, then CREATE SCHEMA/TABLE. "
                "The name above becomes the database name."
            ),
        )
        _provision = bool(_provision_cb) and _ingestion_ok
        if _ingestion_ok and _provision:
            _remote_url = st.text_input(
                "Remote SQL API URL (required — loads row data after DDL)",
                key="sf_remote_api_url",
                help=(
                    "POST JSON body `{\"query\": \"SELECT ...\"}` (same as your SQL passthrough). "
                    "After tables are created, the app runs SELECT * per table (with a row limit) "
                    "and upserts into PostgreSQL so columns and data match the remote DB."
                ),
            )
            _load_all_rows = st.checkbox(
                "Load all rows (count first)",
                key="sf_remote_load_all_rows",
                help="When enabled, row limit is ignored and the app requests full table data. Use the preview below to estimate workload first.",
            )
            _remote_limit = st.text_input(
                "Max rows per table from API",
                value=str(_rd),
                key="sf_remote_row_limit",
                disabled=_load_all_rows,
                help=(
                    "Per table, per run: max rows to pull from the remote API (1–100000). "
                    "When your local table already has data and a numeric `id` column, the next run "
                    "requests **new** rows with `id` greater than local MAX(id) (incremental append), "
                    "not the first page again."
                ),
            )
            if _load_all_rows:
                st.info("Load all rows is ON — row limit will be sent as `0` (no LIMIT in remote SQL).")
        elif not _ingestion_ok:
            st.text_input(
                "Remote SQL API URL (used after materialize — remote row load)",
                value="",
                key="sf_remote_api_url_inert",
                disabled=True,
                placeholder="Set SMART_QUERY_ALLOW_DATA_INGESTION=1, restart, then check the box above",
                help=(
                    "When ingestion is allowed and you enable **Create database & tables**, "
                    "you will enter your HTTP SQL endpoint here."
                ),
            )
            st.text_input(
                "Max rows per table from API",
                value=str(_rd),
                key="sf_remote_row_limit_inert",
                disabled=True,
                help="Unlocked with materialize when `SMART_QUERY_ALLOW_DATA_INGESTION=1` is set.",
            )
        else:
            st.caption("Enable **Create database & tables** above to set Remote API URL and row limit.")
            _remote_limit = str(_rd)
        _remote_limit_effective = "0" if (_ingestion_ok and _provision and _load_all_rows) else _remote_limit
        _sf = st.file_uploader(
            "Schema JSON",
            type=["json"],
            key="sf_upload",
            disabled=not _pg_ok,
        )
        _has_file = bool(_sf)
        _has_label = bool((st.session_state.file_db_name or "").strip())
        _has_remote_url = bool((_remote_url or "").strip())
        _has_remote_limit = bool((_remote_limit_effective or "").strip())
        if _ingestion_ok and _provision and _load_all_rows and _has_file and _has_remote_url:
            st.markdown("##### Remote data scope preview")
            st.caption(
                "Count rows in the remote source before activation. This helps when clients ask for full-load."
            )
            if st.button(
                "Preview remote table counts",
                key="btn_preview_remote_counts",
                use_container_width=True,
                type="secondary",
            ):
                try:
                    with st.spinner("Fetching table list and remote row counts…"):
                        _tables = _parse_uploaded_tables_for_preview(_sf)
                        _schema_names = sorted({t["schema"] for t in _tables})
                        _rows_total = 0
                        _rows_detail: list[dict[str, Any]] = []
                        for t in _tables:
                            _cnt = _fetch_remote_table_count(
                                (_remote_url or "").strip(),
                                str(t["schema"]),
                                str(t["table"]),
                            )
                            _cnt_i = int(_cnt or 0)
                            _rows_total += _cnt_i
                            _rows_detail.append(
                                {
                                    "table": f'{t["schema"]}.{t["table"]}',
                                    "columns": int(t["columns"]),
                                    "records": _cnt_i,
                                }
                            )
                        st.session_state["remote_preview_summary"] = {
                            "database_count": len(st.session_state.get("db_list") or []),
                            "schema_count": len(_schema_names),
                            "table_count": len(_tables),
                            "rows_total": _rows_total,
                            "rows_detail": sorted(_rows_detail, key=lambda x: int(x["records"]), reverse=True),
                        }
                except Exception as ex:
                    st.error(f"Could not preview remote counts: {ex}")
            _rp = st.session_state.get("remote_preview_summary")
            if isinstance(_rp, dict) and _rp.get("rows_detail"):
                st.info(
                    f"database: {_rp.get('database_count', 0)}  ·  schema: {_rp.get('schema_count', 0)}  ·  "
                    f"table: {_rp.get('table_count', 0)}  ·  total records: {_rp.get('rows_total', 0):,}"
                )
                _top = (_rp.get("rows_detail") or [])[:200]
                st.dataframe(
                    pd.DataFrame(_top, columns=["table", "columns", "records"]),
                    use_container_width=True,
                    hide_index=True,
                    height=min(420, 80 + min(len(_top), 14) * 26),
                )
        elif not (_ingestion_ok and _provision and _load_all_rows):
            st.session_state.pop("remote_preview_summary", None)
        # Activate only after a real PostgreSQL session (Connect here, or top "Connect database").
        if _ingestion_ok and _provision:
            _enable_activate_upload = bool(
                _pg_ok
                and _has_remote_url
                and _has_remote_limit
                and _has_file
                and _has_label
                and (not _target_exists_in_connected_db)
            )
        else:
            _enable_activate_upload = bool(
                _pg_ok
                and _has_file
                and _has_label
                and (not _target_exists_in_connected_db)
            )
        if not _provision:
            st.session_state.schema_upload_need_connection_help = False
        if not _enable_activate_upload and not _target_exists_in_connected_db:
            if not _pg_ok:
                st.caption(
                    "Connect to PostgreSQL first — then set **Target database name**, choose your **.json** file, and **Activate**."
                )
            elif _ingestion_ok and _provision and (not _has_remote_url or not _has_remote_limit):
                st.caption("Fill **Remote SQL API URL** and **Max rows per table** when using **Create database & tables**.")
            elif not _has_label or not _has_file:
                st.caption("Enter a **Target database name** and upload a **.json** file, then **Activate**.")
        _disable_activate_btn = bool(not _enable_activate_upload or _target_exists_in_connected_db)
        if st.button("Activate uploaded schema", type="primary", disabled=_disable_activate_btn):
            if not (st.session_state.file_db_name or "").strip():
                st.error("Enter a target database name.")
            elif not _sf:
                st.error("Choose a JSON file.")
            elif _target_exists_in_connected_db:
                st.error(
                    f"Target database `{_target_db_name}` already exists. "
                    "Please provide a new database name."
                )
            elif _provision and not st.session_state.get("pg_session_connected"):
                st.session_state.schema_upload_need_connection_help = True
            else:
                try:
                    # Materialize flow needs the live PG session; keep it and run async so
                    # progress + Pause/Resume/Stop controls are always visible.
                    _keep = "true" if (st.session_state.get("pg_session_connected") or _provision) else "false"
                    _mat = "true" if (_provision and _keep == "true") else "false"
                    _to = 900 if _mat == "true" else 120
                    _file_name = _sf.name
                    _file_body = _sf.getvalue()
                    _post_data = {
                        "session_id": sid,
                        "database_name": st.session_state.file_db_name.strip(),
                        "keep_connection": _keep,
                        "materialize": _mat,
                        "target_database": st.session_state.file_db_name.strip(),
                        "remote_data_url": (_remote_url or "").strip(),
                        "remote_row_limit": (_remote_limit_effective or str(_rd)).strip(),
                    }
                    _post_files = {
                        "file": (_file_name, _file_body, "application/json"),
                    }

                    _ru = (_remote_url or "").strip()
                    _use_async = bool(_provision and _keep == "true")

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
                                "Background job started — **Pause / Resume / Stop** appear below. "
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
                        st.session_state[_PENDING_SYNC_SCHEMA] = {
                            "post_data": _post_data,
                            "file_name": _file_name,
                            "file_body": _file_body,
                            "timeout": _to,
                        }
                        st.rerun()
                except Exception as ex:
                    st.error(str(ex))

        if st.session_state.get("schema_upload_need_connection_help"):
            st.warning(
                "Connection required for server DDL. "
                "Choose one of the options below to continue."
            )
            _h1, _h2 = st.columns(2)
            if _h1.button("Use New connection", key="goto_new_conn_help", use_container_width=True):
                st.session_state["_pending_upload_mode_new_connection"] = True
                st.session_state.schema_upload_need_connection_help = False
                st.rerun()
            if _h2.button("Go to Connect database", key="goto_live_conn_help", use_container_width=True):
                st.session_state["_pending_cfg_source_live"] = True
                st.session_state.conn_source = "live"
                st.session_state.schema_upload_need_connection_help = False
                st.rerun()
    else:
        # Layout tightening: the configuration page already adds header + status + divider.
        # Pull this section up slightly to avoid the large "blank" band above Connection Type.
        st.markdown(
            """
            <style>
              /* Only affects this second-page block (scoped by anchor class). */
              .cfg-conn-main-anchor { margin-top: -1.05rem; }
              .cfg-conn-main-anchor + div p.cfg-source-card-title { margin-top: 0.0rem !important; margin-bottom: 0.35rem !important; }
            </style>
            <div class="cfg-conn-main-anchor"></div>
            """,
            unsafe_allow_html=True,
        )

        # ── Frontend validation errors (dismissible) ────────────────────────────
        st.session_state.setdefault("cfg_form_error", "")
        _err_slot = st.empty()

        def _render_cfg_form_error() -> None:
            msg = (st.session_state.get("cfg_form_error") or "").strip()
            if not msg:
                _err_slot.empty()
                return
            with _err_slot.container():
                st.markdown(
                    """
                    <style>
                      .cfg-alert-wrap { position: relative; }
                      .cfg-form-alert {
                        background: #fef2f2;
                        border: 1px solid #fecaca;
                        border-left: 4px solid #ef4444;
                        border-radius: 10px;
                        padding: 0.6rem 2.65rem 0.6rem 0.75rem; /* reserve space for close */
                        color: #991b1b;
                        font-weight: 600;
                        line-height: 1.35;
                        min-height: 2.35rem;
                        display: flex;
                        align-items: center;
                      }
                      /* Make the close button appear inside the alert */
                      .st-key-dismiss_cfg_form_error {
                        position: absolute !important;
                        top: 0.38rem;
                        right: 0.5rem;
                        margin: 0 !important;
                        padding: 0 !important;
                        z-index: 10;
                      }
                      .st-key-dismiss_cfg_form_error [data-baseweb="button"] {
                        height: 1.55rem !important;
                        min-height: 1.55rem !important;
                        width: 1.55rem !important;
                        border-radius: 999px !important;
                        border: 1px solid #ef4444 !important;
                        background: #ef4444 !important;
                        padding: 0 !important;
                        margin-top: 0.4rem !important; /* vertically center within alert */
                      }
                      .st-key-dismiss_cfg_form_error [data-baseweb="button"] * {
                        color: #ffffff !important;
                        fill: #ffffff !important;
                        font-weight: 800 !important;
                      }
                      .st-key-dismiss_cfg_form_error [data-baseweb="button"]:hover {
                        background: #dc2626 !important;
                        border-color: #dc2626 !important;
                      }
                    </style>
                    """,
                    unsafe_allow_html=True,
                )
                st.markdown(
                    f"<div class='cfg-alert-wrap'><div class='cfg-form-alert'>{html.escape(msg)}</div></div>",
                    unsafe_allow_html=True,
                )
                # Render the close button after the alert; CSS positions it inside `.cfg-alert-wrap`.
                if st.button("✕", key="dismiss_cfg_form_error", help="Dismiss"):
                    st.session_state["cfg_form_error"] = ""
                    st.rerun()

        def _fail(msg: str) -> None:
            # Do not rerun for validation errors.
            st.session_state["cfg_form_error"] = msg

        # Connection Type (top) + full-width form below.
        # Keep the same `cfg_schema_source` key and change handler.
        _cfg_opts = _cfg_source_options()
        if st.session_state.get("cfg_schema_source") not in _cfg_opts:
            st.session_state["cfg_schema_source"] = _cfg_opts[0]
        st.markdown(
            "<p class='cfg-source-card-title' style='margin-top:0.2rem !important'>Connection Type</p>",
            unsafe_allow_html=True,
        )
        # Horizontal segmented selector (matches instruction-page style; preserves cfg_schema_source key).
        _current = st.session_state.get("cfg_schema_source") or _cfg_opts[0]
        _active_idx = _cfg_opts.index(_current) if _current in _cfg_opts else 0
        _style_block = """
        <style>
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"],
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"],
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"] {
            width: 100% !important;
            border: 1.5px solid #e5e7eb !important;
            background: #ffffff !important;
            border-radius: 999px !important;
            box-shadow: 0 1px 8px rgba(0,0,0,0.04) !important;
        }
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"] p,
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"] p,
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"] p {
            font-size: 0.88rem !important;
            font-weight: 600 !important;
            margin: 0 !important;
            color: #374151 !important;
            text-align: center !important;
            width: 100% !important;
        }
        section.main .st-key-cfg_source_btn_0 [data-baseweb="button"]:hover,
        section.main .st-key-cfg_source_btn_1 [data-baseweb="button"]:hover,
        section.main .st-key-cfg_source_btn_2 [data-baseweb="button"]:hover {
            border-color: #d1d5db !important;
            background: #fafafa !important;
        }
        section.main .st-key-cfg_source_btn___ACTIVE__ [data-baseweb="button"] {
            border: 1.5px solid #7c3aed !important;
            background: #f5f3ff !important;
        }
        section.main .st-key-cfg_source_btn___ACTIVE__ [data-baseweb="button"] p {
            color: #1f2937 !important;
            font-weight: 700 !important;
        }
        </style>
        """
        st.markdown(_style_block.replace("__ACTIVE__", str(_active_idx)), unsafe_allow_html=True)
        _c1, _c2, _c3 = st.columns(3, gap="small")
        for _col, _ix, _opt in ((_c1, 0, _cfg_opts[0]), (_c2, 1, _cfg_opts[1]), (_c3, 2, _cfg_opts[2])):
            with _col:
                if st.button(_opt, key=f"cfg_source_btn_{_ix}", use_container_width=True, type="secondary"):
                    if st.session_state.get("cfg_schema_source") != _opt:
                        st.session_state["cfg_schema_source"] = _opt
                        _on_configuration_source_changed()
                    st.rerun()
        _src = st.session_state.get("cfg_schema_source") or _cfg_opts[0]
        _apply_cfg_source_selection(_src)

        if _src == "Remote URL connection":
            st.markdown(
                "<p class='cfg-source-card-title' style='margin-top:0.2rem !important'>Remote URL Connection</p>",
                unsafe_allow_html=True,
            )
            st.caption("Paste your remote SQL API endpoint. The app will fetch database, schema, table list and counts.")
            with st.form("remote_url_form", clear_on_submit=False):
                remote_url = st.text_input(
                    "Remote SQL API URL",
                    key="remote_conn_url",
                    placeholder="https://api.example.com",
                )
                submitted_remote = st.form_submit_button(
                    "Fetch remote metadata",
                    type="primary",
                    use_container_width=True,
                )
            if submitted_remote:
                st.session_state["cfg_form_error"] = ""
                _ru = (remote_url or "").strip()
                if not _ru:
                    _fail("Please enter Remote SQL API URL before fetching metadata.")
                else:
                    try:
                        _db_rows = _fetch_remote_rows(_ru, "SELECT current_database() AS database_name")
                        _db_name = "remote_database"
                        if _db_rows and isinstance(_db_rows[0], dict):
                            _db_name = str(
                                _db_rows[0].get("database_name")
                                or _db_rows[0].get("current_database")
                                or list(_db_rows[0].values())[0]
                                or "remote_database"
                            )
                        _sch_rows = _fetch_remote_rows(
                            _ru,
                            "SELECT schema_name FROM information_schema.schemata "
                            "WHERE schema_name NOT IN ('information_schema') "
                            "AND schema_name NOT LIKE 'pg_%' ORDER BY schema_name",
                        )
                        _schemas = [
                            str(r.get("schema_name") or r.get("schema") or "").strip()
                            for r in _sch_rows
                            if isinstance(r, dict)
                        ]
                        _schemas = [s for s in _schemas if s]
                        st.session_state.remote_conn_db_list = [_db_name]
                        st.session_state.remote_conn_schema_list = _schemas
                        st.session_state.remote_conn_selected_db = _db_name
                        st.session_state.remote_conn_selected_schema = _schemas[0] if _schemas else ""
                        st.session_state.remote_conn_table_list = []
                        st.session_state.remote_conn_selected_tables = []
                        _cfg_notify("success", f"Remote metadata fetched: {len(_schemas)} schema(s).")
                        # Render toast immediately (avoid "second click" feel).
                        _render_cfg_notifications()
                    except Exception as ex:
                        # Keep it clean: show only the real error, no troubleshooter for this step.
                        _fail(f"Remote metadata fetch failed: {ex}")
        elif _src == "Server connection":
            with st.form("server_connection_form", clear_on_submit=False):
                with st.container(border=True):
                    st.caption("**PostgreSQL** — where the server listens *after* you SSH in (often `127.0.0.1`).")
                    st.caption("Use the **database account password** here. SSH is configured below.")
                    _hcol, _pcol = st.columns([4, 1], gap="small")
                    with _hcol:
                        host = st.text_input(
                            "Host",
                            value=st.session_state.get("conn_host") or "",
                            key="in_host",
                            placeholder="127.0.0.1",
                        )
                    with _pcol:
                        port = st.text_input(
                            "Port",
                            value=st.session_state.get("conn_port") or "5432",
                            key="in_port",
                            placeholder="5432",
                        )
                    username = st.text_input(
                        "Username",
                        value=st.session_state.get("conn_user") or "",
                        key="in_user",
                        placeholder="database user (e.g. cocreaduser)",
                    )
                    password = st.text_input(
                        "Password",
                        type="password",
                        value=st.session_state.get("conn_pass") or "",
                        key="in_pass",
                        placeholder="database password",
                    )
                    dbname = st.text_input(
                        "Database name",
                        value=st.session_state.get("file_db_name") or "",
                        key="cfg_dbname_main",
                        placeholder="e.g. medicine",
                    )
                with st.container(border=True):
                    st.caption("**SSH jump host** — optional; use when you reach Postgres only through a bastion.")
                    st.caption(
                        "Auth is with a **private key** (PEM), not an SSH password. "
                        "Host/port here are the **bastion** (e.g. `10.100.0.2:22` as `ubuntu`)."
                    )
                    use_ssh = st.checkbox("Connect through SSH tunnel", key="conn_use_ssh")
                    ssh_host = ssh_port = ssh_user = ssh_key = ""
                    if use_ssh:
                        _s1, _s2 = st.columns(2, gap="small")
                        with _s1:
                            ssh_host = st.text_input(
                                "Bastion / server (IP or hostname)",
                                key="conn_ssh_host",
                                placeholder="e.g. 10.100.0.2",
                            )
                        with _s2:
                            ssh_port = st.text_input("SSH port", key="conn_ssh_port", placeholder="22")
                        ssh_user = st.text_input("SSH username", key="conn_ssh_user", placeholder="e.g. ubuntu")
                        st.caption("Upload a key file **or** paste in the box — an upload replaces the box.")
                        _f_srv = st.file_uploader(
                            "Private key file",
                            type=None,
                            key="cfg_server_ssh_key_upload",
                            help="e.g. id_rsa, id_ed25519, or a .pem. Must be UTF-8 text (standard PEM/OpenSSH).",
                        )
                        _apply_uploaded_ssh_key_to_session(_f_srv)
                        ssh_key = st.text_area(
                            "SSH private key (PEM) — or paste",
                            height=120,
                            key="conn_ssh_key",
                            placeholder="-----BEGIN … PRIVATE KEY----- (paste full key)",
                        )
                        st.text_input(
                            "Key passphrase (if your key is encrypted)",
                            type="password",
                            key="conn_ssh_key_pass",
                        )
                submitted_server = st.form_submit_button(
                    "Connect to server",
                    type="primary",
                    use_container_width=True,
                )
            if submitted_server:
                st.session_state["cfg_form_error"] = ""
                missing: list[str] = []
                if not (host or "").strip():
                    missing.append("Host")
                if not (port or "").strip():
                    missing.append("Port")
                if not (username or "").strip():
                    missing.append("Username")
                if not (password or "").strip():
                    missing.append("Password")
                if not (dbname or "").strip():
                    missing.append("Database name")
                # Server connection requires SSH tunnel.
                if not st.session_state.get("conn_use_ssh"):
                    missing.append("SSH tunnel (enable 'Connect through SSH tunnel')")
                else:
                    if not (ssh_host or "").strip():
                        missing.append("SSH host")
                    if not (ssh_port or "").strip():
                        missing.append("SSH port")
                    if not (ssh_user or "").strip():
                        missing.append("SSH username")
                    if not (ssh_key or "").strip():
                        missing.append("SSH private key")
                if missing:
                    _fail("Please fill " + ", ".join(missing) + " before connecting.")
                else:
                    # Persist to session_state keys expected by _run_configuration_db_connect().
                    st.session_state.conn_host = (host or "").strip()
                    st.session_state.conn_port = (port or "").strip()
                    st.session_state.conn_user = (username or "").strip()
                    st.session_state.conn_pass = password or ""
                    st.session_state.file_db_name = (dbname or "").strip()
                    _run_configuration_db_connect()
                    # Suppress "Troubleshooter:" toast for normal connection attempts.
                    q = list(st.session_state.get("_cfg_notifications") or [])
                    st.session_state["_cfg_notifications"] = [
                        n
                        for n in q
                        if not (
                            str(n.get("level") or "") == "warning"
                            and str(n.get("message") or "").startswith("Troubleshooter:")
                        )
                    ]
                    # Render toast immediately (avoid "second click" feel).
                    _render_cfg_notifications()
        elif _src == "Local DB connection":
            st.session_state.conn_use_ssh = False
            st.markdown(
                "<p class='cfg-source-card-title' style='margin-top:0.2rem !important'>Database credentials</p>",
                unsafe_allow_html=True,
            )
            st.caption("Local DB: enter host, username, and password, then connect.")
            with st.form("local_db_connection_form", clear_on_submit=False):
                _hcol, _pcol = st.columns([4, 1], gap="small")
                with _hcol:
                    host = st.text_input(
                        "Host",
                        value=st.session_state.get("conn_host") or "",
                        key="in_host",
                        placeholder="localhost or IP",
                    )
                with _pcol:
                    port = st.text_input(
                        "Port",
                        value=st.session_state.get("conn_port") or "5432",
                        key="in_port",
                        placeholder="5432",
                    )
                username = st.text_input(
                    "Username",
                    value=st.session_state.get("conn_user") or "",
                    key="in_user",
                    placeholder="postgres",
                )
                password = st.text_input(
                    "Password",
                    type="password",
                    value=st.session_state.get("conn_pass") or "",
                    key="in_pass",
                )
                submitted_local = st.form_submit_button(
                    "Connection",
                    type="primary",
                    use_container_width=True,
                )
            if submitted_local:
                st.session_state["cfg_form_error"] = ""
                if not ((host or "").strip() and (port or "").strip() and (username or "").strip() and (password or "").strip()):
                    _fail("Please fill Host, Port, Username and Password before connecting.")
                else:
                    st.session_state.conn_host = (host or "").strip()
                    st.session_state.conn_port = (port or "").strip()
                    st.session_state.conn_user = (username or "").strip()
                    st.session_state.conn_pass = password or ""
                    _run_configuration_db_connect()
                    # Suppress "Troubleshooter:" toast for normal connection attempts.
                    q = list(st.session_state.get("_cfg_notifications") or [])
                    st.session_state["_cfg_notifications"] = [
                        n
                        for n in q
                        if not (
                            str(n.get("level") or "") == "warning"
                            and str(n.get("message") or "").startswith("Troubleshooter:")
                        )
                    ]
                    # Render toast immediately (avoid "second click" feel).
                    _render_cfg_notifications()

        # Render the error banner once per run (avoids DuplicateWidgetID).
        _render_cfg_form_error()

        if _src == "Remote URL connection":
            _rdb = list(st.session_state.get("remote_conn_db_list") or [])
            _rsc = list(st.session_state.get("remote_conn_schema_list") or [])
            if _rdb:
                st.selectbox(
                    "Selected DB",
                    options=_rdb,
                    key="remote_conn_selected_db",
                )
            if _rsc:
                st.selectbox(
                    "Selected schema",
                    options=_rsc,
                    key="remote_conn_selected_schema",
                )
                if st.button("Fetch table list and counts", key="remote_fetch_tables_btn"):
                    _ru = (st.session_state.get("remote_conn_url") or "").strip()
                    _sch = (st.session_state.get("remote_conn_selected_schema") or "").strip()
                    try:
                        with st.spinner("Fetching table list and row counts from remote URL..."):
                            _sch_lit = _sch.replace("'", "''")
                            _tbl_rows = _fetch_remote_rows(
                                _ru,
                                "SELECT table_name FROM information_schema.tables "
                                f"WHERE table_schema = '{_sch_lit}' AND table_type='BASE TABLE' ORDER BY table_name",
                            )
                            _tables = [
                                str(r.get("table_name") or r.get("table") or "").strip()
                                for r in _tbl_rows
                                if isinstance(r, dict)
                            ]
                            _tables = [t for t in _tables if t]
                            _with_counts: list[dict[str, Any]] = []
                            for _t in _tables:
                                _cnt = _fetch_remote_table_count(_ru, _sch, _t)
                                _with_counts.append({"schema": _sch, "table": _t, "count": int(_cnt or 0)})
                        st.session_state.remote_conn_table_list = _with_counts
                        st.session_state.remote_conn_selected_tables = [f"{x['schema']}.{x['table']}" for x in _with_counts]
                        st.session_state.remote_conn_counts_summary = {
                            "db_count": len(_rdb),
                            "schema_count": len(_rsc),
                            "table_count": len(_with_counts),
                            "row_count_total": int(sum(int(x.get("count") or 0) for x in _with_counts)),
                        }
                        _cfg_notify("success", f"Fetched {len(_with_counts)} table(s) with counts.")
                    except Exception as ex:
                        _cfg_notify_error_with_help(str(ex), "Could not fetch table list")
            _sum = st.session_state.get("remote_conn_counts_summary") or {}
            if _sum:
                _m1, _m2, _m3, _m4 = st.columns(4)
                _m1.metric("DB count", int(_sum.get("db_count") or 0))
                _m2.metric("Schema count", int(_sum.get("schema_count") or 0))
                _m3.metric("Table count", int(_sum.get("table_count") or 0))
                _m4.metric("Data row count", int(_sum.get("row_count_total") or 0))
            _rt = list(st.session_state.get("remote_conn_table_list") or [])
            if _rt:
                _opts = [f"{x['schema']}.{x['table']} ({x['count']})" for x in _rt]
                _pre = st.session_state.get("remote_conn_selected_tables") or []
                _label_to_key = {f"{x['schema']}.{x['table']} ({x['count']})": f"{x['schema']}.{x['table']}" for x in _rt}
                _pre_labels = [l for l, k in _label_to_key.items() if k in _pre]
                _picked_labels = st.multiselect(
                    "Selected table",
                    options=_opts,
                    default=_pre_labels,
                    key="remote_conn_selected_tables_labels",
                )
                st.session_state.remote_conn_selected_tables = [_label_to_key[l] for l in _picked_labels if l in _label_to_key]
                st.radio(
                    "Load mode",
                    options=["Load limited data", "Load all data"],
                    key="remote_conn_load_mode",
                    horizontal=True,
                )
                if st.session_state.get("remote_conn_load_mode") == "Load limited data":
                    st.number_input(
                        "Row limit",
                        min_value=1,
                        max_value=100000,
                        value=int(st.session_state.get("remote_conn_row_limit") or remote_sync_default_row_limit()),
                        key="remote_conn_row_limit",
                    )
                _a1, _a2 = st.columns(2)
                with _a1:
                    _act_alert = st.empty()
                    if st.button("Activate selection", type="primary", use_container_width=True, key="remote_activate_btn"):
                        _picked = list(st.session_state.get("remote_conn_selected_tables") or [])
                        if not _picked:
                            _msg = "Select at least one table to activate."
                            _act_alert.warning(_msg)
                            _cfg_notify("warning", _msg)
                        else:
                            _db_raw = st.session_state.get("remote_conn_selected_db")
                            if isinstance(_db_raw, list):
                                _db = str(_db_raw[0]).strip() if _db_raw else ""
                            else:
                                _db = str(_db_raw or "").strip()
                            _table_refs: list[dict[str, str]] = []
                            for p in _picked:
                                if isinstance(p, list):
                                    p = ".".join(str(x) for x in p if str(x).strip())
                                p = str(p or "").strip()
                                if not p:
                                    continue
                                if "." in p:
                                    _sch, _tbl = p.split(".", 1)
                                else:
                                    _sch, _tbl = (st.session_state.get("remote_conn_selected_schema") or "public"), p
                                _sch = str(_sch or "").strip()
                                _tbl = str(_tbl or "").strip()
                                if not _sch or not _tbl:
                                    continue
                                _table_refs.append({"schema": _sch, "name": _tbl})
                            if not _table_refs:
                                _cfg_notify("warning", "No valid table identifiers found in selection.")
                                return
                            try:
                                _status = st.empty()
                                _bar = st.progress(0)
                                # Remote URL mode should always use remote-schema activation path,
                                # independent of any previous local/server connection state.
                                _is_live_pool = False
                                if _is_live_pool:
                                    _status.info("Switching active database...")
                                    _bar.progress(15)
                                    r_use = requests.post(
                                        f"{API_URL}/db/use-database",
                                        json={"session_id": sid, "database": _db},
                                        timeout=45,
                                    )
                                    use_body, use_err = safe_response_payload(r_use)
                                    if use_err:
                                        _act_alert.error(f"Activation failed: {use_err}")
                                        _cfg_notify_error_with_help(str(use_err), "Activation failed")
                                        _bar.empty()
                                        _status.empty()
                                        return
                                    if not r_use.ok:
                                        _e = _detail_text(use_body, "Could not switch database.")
                                        _act_alert.error(f"Activation failed: {_e}")
                                        _cfg_notify_error_with_help(
                                            _e,
                                            "Activation failed",
                                        )
                                        _bar.empty()
                                        _status.empty()
                                        return
                                    _status.info("Preparing activation payload...")
                                    _bar.progress(35)
                                    r_act = requests.post(
                                        f"{API_URL}/db/activate",
                                        json={
                                            "session_id": sid,
                                            "database": _db,
                                            "tables": _table_refs,
                                        },
                                        timeout=120,
                                    )
                                else:
                                    # Pure Remote URL mode: build schema JSON from remote metadata and activate via file path.
                                    _status.info("Building schema from remote metadata...")
                                    _bar.progress(20)
                                    _ru = (st.session_state.get("remote_conn_url") or "").strip()
                                    _schema_payload = {"database_name": _db, "tables": {}}
                                    for tr in _table_refs:
                                        _sch = tr.get("schema", "")
                                        _tbl = tr.get("name", "")
                                        _sch_lit = str(_sch).replace("'", "''")
                                        _tbl_lit = str(_tbl).replace("'", "''")
                                        _col_rows = _fetch_remote_rows(
                                            _ru,
                                            "SELECT column_name, data_type FROM information_schema.columns "
                                            f"WHERE table_schema = '{_sch_lit}' AND table_name = '{_tbl_lit}' "
                                            "ORDER BY ordinal_position",
                                            timeout=60,
                                        )
                                        _cols = []
                                        for c in _col_rows:
                                            if not isinstance(c, dict):
                                                continue
                                            _cn = str(c.get("column_name") or "").strip()
                                            if not _cn:
                                                continue
                                            _cols.append(
                                                {
                                                    "column_name": _cn,
                                                    "data_type": str(c.get("data_type") or "text"),
                                                }
                                            )
                                        _schema_payload["tables"][f"{_sch}.{_tbl}"] = {
                                            "schema_name": _sch,
                                            "table_name": _tbl,
                                            "columns": _cols,
                                        }
                                    _status.info("Activating remote schema...")
                                    _bar.progress(65)
                                    _bytes = json.dumps(_schema_payload).encode("utf-8")
                                    _row_mode = st.session_state.get("remote_conn_load_mode") or "Load limited data"
                                    _row_limit = (
                                        "0"
                                        if _row_mode == "Load all data"
                                        else str(int(st.session_state.get("remote_conn_row_limit") or remote_sync_default_row_limit()))
                                    )
                                    r_act = requests.post(
                                        f"{API_URL}/schema/from-file",
                                        data={
                                            "session_id": sid,
                                            "database_name": _db or "remote_database",
                                            "keep_connection": "false",
                                            "materialize": "false",
                                            "remote_data_url": _ru,
                                            "remote_row_limit": _row_limit,
                                        },
                                        files={"file": ("remote_schema.json", _bytes, "application/json")},
                                        timeout=180,
                                    )
                                _status.info("Validating activation response...")
                                _bar.progress(80)
                                info, jerr = safe_response_payload(r_act)
                                if jerr:
                                    _act_alert.error(f"Activation failed: {jerr}")
                                    _cfg_notify_error_with_help(str(jerr), "Activation failed")
                                elif r_act.status_code == 422 and _is_live_pool:
                                    # Fallback: rebuild table refs from backend metadata and retry once.
                                    _status.info("Normalizing selected tables and retrying activation...")
                                    _bar.progress(88)
                                    _schemas = sorted({x["schema"] for x in _table_refs})
                                    r_tbl = requests.get(
                                        f"{API_URL}/db/tables",
                                        params={"session_id": sid, "schemas": ",".join(_schemas)},
                                        timeout=90,
                                    )
                                    tbl_body, tbl_err = safe_response_payload(r_tbl)
                                    if tbl_err or not r_tbl.ok:
                                        _e = str(tbl_err or _detail_text(tbl_body, "Could not load tables for retry."))
                                        _act_alert.error(f"Activation failed: {_e}")
                                        _cfg_notify_error_with_help(
                                            _e,
                                            "Activation failed",
                                        )
                                    else:
                                        flat = (tbl_body or {}).get("flat") if isinstance(tbl_body, dict) else []
                                        desired = {f"{x['schema']}.{x['name']}" for x in _table_refs}
                                        parts_retry = [
                                            {"schema": str(t.get("schema") or "").strip(), "name": str(t.get("name") or "").strip()}
                                            for t in (flat or [])
                                            if f"{str(t.get('schema') or '').strip()}.{str(t.get('name') or '').strip()}" in desired
                                        ]
                                        if not parts_retry:
                                            parts_retry = [
                                                {"schema": str(t.get("schema") or "").strip(), "name": str(t.get("name") or "").strip()}
                                                for t in (flat or [])
                                                if str(t.get("schema") or "").strip() and str(t.get("name") or "").strip()
                                            ]
                                        r_act2 = requests.post(
                                            f"{API_URL}/db/activate",
                                            json={"session_id": sid, "database": _db, "tables": parts_retry},
                                            timeout=max(180, min(1200, 60 + len(parts_retry) * 3)),
                                        )
                                        info2, jerr2 = safe_response_payload(r_act2)
                                        if jerr2:
                                            _act_alert.error(f"Activation failed: {jerr2}")
                                            _cfg_notify_error_with_help(str(jerr2), "Activation failed")
                                        elif r_act2.ok:
                                            st.session_state.nl_ready = True
                                            st.session_state.pg_session_connected = True
                                            st.session_state.file_db_name = _db
                                            st.session_state.show_chat_invite = True
                                            _bar.progress(100)
                                            _status.success("Activation complete.")
                                            _cfg_notify("success", f"Activated {_db} with {len(parts_retry)} table(s).")
                                            st.rerun()
                                        else:
                                            _e2 = _detail_text(info2, "Activation failed")
                                            _act_alert.error(f"Activation failed: {_e2}")
                                            _cfg_notify_error_with_help(
                                                _e2,
                                                "Activation failed",
                                            )
                                elif r_act.ok:
                                    st.session_state.nl_ready = True
                                    st.session_state.pg_session_connected = True
                                    st.session_state.file_db_name = _db
                                    st.session_state.show_chat_invite = True
                                    _bar.progress(100)
                                    _status.success("Activation complete.")
                                    _cfg_notify("success", f"Activated {_db} with {len(_picked)} table(s).")
                                    st.rerun()
                                else:
                                    _e = _detail_text(info, "Activation failed")
                                    _act_alert.error(f"Activation failed: {_e}")
                                    _cfg_notify_error_with_help(
                                        _e,
                                        "Activation failed",
                                    )
                                _bar.empty()
                                _status.empty()
                            except Exception as ex:
                                _act_alert.error(f"Activate failed: {ex}")
                                _cfg_notify_error_with_help(str(ex), "Activate failed")
                with _a2:
                    _can_chat = bool(st.session_state.get("nl_ready"))
                    if st.button(
                        "Go to Chat →",
                        use_container_width=True,
                        key="remote_go_chat_btn",
                        disabled=not _can_chat,
                    ):
                        st.switch_page("pages/project_chat.py")
            return

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
                        json={"session_id": sid, "database": st.session_state.sb_database},
                        timeout=45,
                    )
                    info, jerr = safe_response_payload(r)
                    if jerr:
                        _cfg_notify("error", jerr)
                    elif r.ok:
                        rs = requests.get(f"{API_URL}/db/schemas", params={"session_id": sid}, timeout=45)
                        if rs.ok:
                            st.session_state.schema_list = rs.json().get("schemas") or []
                            _cfg_notify("success", "Database opened — pick schema(s) and load tables.")
                        else:
                            _cfg_notify("error", "Could not list schemas.")
                    else:
                        _cfg_notify("error", (info or {}).get("detail", "Failed") if isinstance(info, dict) else "Failed")
                except Exception as ex:
                    _cfg_notify("error", str(ex))

        if st.session_state.schema_list:
            _sch_opts = st.session_state.schema_list
            _prev_pick = st.session_state.get("pick_schemas") or []
            _valid_pick = [s for s in _prev_pick if s in _sch_opts]
            _schema_default = (
                _valid_pick
                if _valid_pick
                else _sch_opts[: min(5, len(_sch_opts))]
            )
            st.session_state.pick_schemas = st.multiselect(
                "Schemas",
                options=_sch_opts,
                default=_schema_default,
                key="ms_schemas",
            )
            if st.button("Load tables in selected schemas"):
                sch_param = ",".join(st.session_state.pick_schemas or [])
                try:
                    rt = requests.get(
                        f"{API_URL}/db/tables",
                        params={"session_id": sid, "schemas": sch_param},
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
            _ts = st.text_input(
                "Search tables",
                key="table_search_nl",
                placeholder="Type letters — matching tables appear below instantly (e.g. cho, sales, store)",
            )
            st.caption(
                f"**{len(_labels):,}** table(s) in this catalog. "
                "**Select matching** adds only names that match the search. "
                "**Select all tables** adds every loaded name. "
                "Scroll the list below to review the full catalog."
            )

            _browse_cap = ui_schema_table_browse_limit()
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
            _qq = (_ts or "").strip().lower()
            _qq_norm = re.sub(r"[^a-z0-9]+", "", _qq)

            def _table_search_suggestions(query: str, labels: list[str]) -> list[str]:
                q = (query or "").strip().lower()
                q_norm = re.sub(r"[^a-z0-9]+", "", q)
                if len(q_norm) < 2:
                    return []
                token_pool: set[str] = set()
                label_pool: set[str] = set()
                for lbl in labels:
                    ll = lbl.lower()
                    label_pool.add(ll)
                    _parts = re.split(r"[^a-zA-Z0-9]+", lbl.lower())
                    for p in _parts:
                        if len(p) >= 2:
                            token_pool.add(p)
                starts_token = sorted([t for t in token_pool if t.startswith(q_norm)])[:6]
                starts_label = sorted([l for l in label_pool if q in l or q_norm in re.sub(r"[^a-z0-9]+", "", l)])[:6]
                close = get_close_matches(q_norm, list(token_pool), n=6, cutoff=0.5)
                merged: list[str] = []
                for s in starts_token + starts_label + close:
                    if s not in merged:
                        merged.append(s)
                return merged[:10]

            _visible = (
                [
                    L
                    for L in _labels
                    if (
                        (_qq and _qq in L.lower())
                        or (_qq_norm and _qq_norm in re.sub(r"[^a-z0-9]+", "", L.lower()))
                    )
                ]
                if (_qq or _qq_norm)
                else []
            )
            st.caption("Results update as you type.")

            if _qq:
                if _visible:
                    _live_cap = ui_schema_table_browse_limit()
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
                                "session_id": sid,
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
                            st.session_state.show_chat_invite = True
                            _persist_activated_schema_to_app()
                            st.success(
                                f"✅ Active — **{ai.get('table_count', 0)}** table(s) for NL→SQL. "
                                "A schema metadata snapshot was saved in the app database (catalog only; not your row data)."
                            )
                        else:
                            st.error((ai or {}).get("detail", "Activate failed") if isinstance(ai, dict) else "Activate failed")
                    except Exception as ex:
                        st.error(str(ex))

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

    if st.session_state.get("pg_session_connected"):
        st.divider()
        st.subheader("Schema refresh")
        st.caption("Re-fetches the latest table/column metadata from the **connected** database (not available for *upload-only* / file JSON without a live pool).")
        if st.button("🔄 Update schema from database", type="secondary", key="btn_upd_sch_cfg"):
            with st.spinner("Re-fetching schema…"):
                try:
                    r = requests.post(
                        f"{API_URL}/reload-schema",
                        json={"session_id": sid},
                        timeout=120,
                    )
                    info, jerr = safe_response_payload(r)
                    if jerr:
                        st.error(jerr)
                    elif r.ok and isinstance(info, dict):
                        _persist_activated_schema_to_app()
                        st.success(
                            f"✅ **{info.get('table_count', 0)}** table(s) — metadata reloaded. "
                            f"SQL cache cleared: **{info.get('sql_cache_cleared', 0)}** — "
                            "app DB schema snapshot updated (catalog only)."
                        )
                    else:
                        st.error(
                            (info or {}).get("detail", "Update failed")
                            if isinstance(info, dict)
                            else "Update failed"
                        )
                except Exception as ex:
                    st.error(str(ex))
            st.rerun()

def _render_configuration_getting_started() -> None:
    """Configuration hero + onboarding cards (presentation only, no behavior changes)."""
    st.markdown(
        """
    <style>
      /* Light cards + purple accent — match Tenant / Projects (sqg-dash-proj) */
      .cfg-shell { margin-top: 0.15rem; }
      .cfg-kicker {
        color: #9ca3af;
        font-size: 0.66rem;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        font-weight: 700;
        margin: 0.1rem 0 0.5rem 0;
      }
      .cfg-card {
        background: #ffffff;
        border: 1px solid #e5e7eb;
        border-left: 4px solid #7c3aed;
        border-radius: 12px;
        padding: 0.9rem 1rem;
        margin-bottom: 0.55rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        transition: box-shadow 0.18s ease;
      }
      .cfg-card:hover { box-shadow: 0 8px 24px rgba(124, 58, 237, 0.1); }
      .cfg-card--info {
        background: linear-gradient(135deg, #ede9fe, #f5f3ff);
        border: 1px solid #c4b5fd;
        border-left: 4px solid #7c3aed;
        box-shadow: 0 2px 8px rgba(124, 58, 237, 0.08);
      }
      .cfg-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.6rem;
      }
      .cfg-row-main {
        display: flex;
        align-items: flex-start;
        gap: 0.62rem;
        min-width: 0;
      }
      .cfg-dot {
        width: 1.3rem;
        height: 1.3rem;
        min-width: 1.3rem;
        border-radius: 999px;
        border: 1px solid #ddd6fe;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        color: #6d28d9;
        background: #ede9fe;
        font-size: 0.7rem;
        font-weight: 800;
        line-height: 1;
        margin-top: 0.1rem;
      }
      .cfg-step-num {
        width: 1.15rem;
        height: 1.15rem;
        min-width: 1.15rem;
        border-radius: 999px;
        border: 1px solid #c4b5fd;
        color: #5b21b6;
        background: #f5f3ff;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-size: 0.64rem;
        font-weight: 800;
        margin-top: 0.08rem;
      }
      .cfg-title {
        margin: 0;
        color: #1e1b2e;
        font-weight: 700;
        font-size: 0.9rem;
        line-height: 1.35;
      }
      .cfg-desc {
        margin: 0.15rem 0 0 0;
        color: #4b5563;
        font-size: 0.8rem;
        line-height: 1.5;
      }
      .cfg-desc strong { color: #1e1b2e; }
      .cfg-chip {
        background: #faf7ff;
        border: 1px solid #ddd6fe;
        color: #5b21b6;
        border-radius: 6px;
        padding: 0.05rem 0.35rem;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 0.68rem;
        font-weight: 500;
      }
      .cfg-note {
        color: #6b7280;
        font-size: 0.8rem;
        margin-top: 0.5rem;
        text-align: center;
        line-height: 1.45;
      }
      .cfg-chevron {
        color: #9ca3af;
        font-size: 0.95rem;
        line-height: 1;
        margin-left: 0.4rem;
        flex-shrink: 0;
      }
    </style>
    <div class="cfg-shell">
      <p class="cfg-kicker">Getting started</p>
      <div class="cfg-card cfg-card--info">
        <div class="cfg-row">
          <div class="cfg-row-main">
            <span class="cfg-dot">i</span>
            <div>
              <p class="cfg-title">What to do</p>
              <p class="cfg-desc">
                Select <span class="cfg-chip">Source</span> in the left sidebar, then in the
                <strong>main panel</strong> connect to PostgreSQL or upload a schema file. Go to
                <span class="cfg-chip">database &gt; activities &gt; actions</span> and click
                <span class="cfg-chip">Activate</span>. When you're ready, open
                <span class="cfg-chip">Chat</span> from the sidebar.
              </p>
            </div>
          </div>
          <span class="cfg-chevron">›</span>
        </div>
      </div>

      <div class="cfg-card">
        <div class="cfg-row">
          <div class="cfg-row-main">
            <span class="cfg-step-num">1</span>
            <div>
              <p class="cfg-title">Connect your database</p>
              <p class="cfg-desc">After <span class="cfg-chip">Continue to connection setup</span>, use the <strong>main panel</strong> to enter <span class="cfg-chip">Host</span> and <span class="cfg-chip">Port</span> — or upload a schema <span class="cfg-chip">.json</span> file if you prefer.</p>
            </div>
          </div>
          <span class="cfg-chevron">›</span>
        </div>
      </div>

      <div class="cfg-card">
        <div class="cfg-row">
          <div class="cfg-row-main">
            <span class="cfg-step-num">2</span>
            <div>
              <p class="cfg-title">Activate in activities</p>
              <p class="cfg-desc">Go to <span class="cfg-chip">database &gt; activities &gt; actions</span> and click <span class="cfg-chip">Activate</span> to enable the assistant.</p>
            </div>
          </div>
          <span class="cfg-chevron">›</span>
        </div>
      </div>

      <div class="cfg-card">
        <div class="cfg-row">
          <div class="cfg-row-main">
            <span class="cfg-step-num">3</span>
            <div>
              <p class="cfg-title">Open Chat and start querying</p>
              <p class="cfg-desc">Once activated, open <span class="cfg-chip">Chat</span> from the sidebar to query your data in plain English.</p>
            </div>
          </div>
          <span class="cfg-chevron">›</span>
        </div>
      </div>

      <p class="cfg-note">Any PostgreSQL instance can be connected · no special configuration required · only activated tables are used.</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


def _apply_configuration_ui_redesign_styles() -> None:
    """Configuration-only tweaks on top of :func:`apply_dashboard_theme` + :func:`apply_tenant_page_shell` (no nav overrides)."""
    st.markdown(
        """
        <style>
          :root { --sqg-accent: #7c3aed; --sqg-ink: #1e1b2e; }
          /* Readable defaults on dark sidebar (same as apply_dashboard_theme nav) */
          [data-testid="stSidebar"] [data-testid="stVerticalBlock"] p,
          [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
          [data-testid="stSidebar"] [data-testid="stMarkdown"] p { color: #e2e8f0 !important; -webkit-text-fill-color: #e2e8f0 !important; }
          [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p,
          [data-testid="stSidebar"] [data-testid="stCaption"] p,
          [data-testid="stSidebar"] [data-testid="stCaption"] { color: #9ca3af !important; -webkit-text-fill-color: #9ca3af !important; }
          [data-testid="stSidebar"] [data-baseweb="slider"] + div [data-testid="stWidgetLabel"] p,
          [data-testid="stSidebar"] [data-baseweb="select"] + div p { color: #d1d5db !important; }
          /* Context chips in dark sidebar (Tenant Dashboard family) */
          .cfg-sb-project-row { display: flex; gap: 0.35rem; flex-wrap: wrap; margin: 0.2rem 0 0.6rem 0; }
          .cfg-sb-chip {
            background: rgba(255, 255, 255, 0.1) !important;
            color: #f1f5f9 !important;
            border: 1px solid rgba(255, 255, 255, 0.16) !important;
            border-radius: 8px;
            padding: 0.14rem 0.45rem;
            font-size: 0.72rem;
            line-height: 1.3;
            font-weight: 600;
          }
          .cfg-sb-sec {
            margin: 0.75rem 0 0.4rem 0;
            color: #c4b5fd !important;
            -webkit-text-fill-color: #c4b5fd !important;
            font-size: 0.6rem !important;
            letter-spacing: 0.1em !important;
            text-transform: uppercase !important;
            font-weight: 700 !important;
          }
          [data-testid="stSidebar"] [data-testid="stWidgetLabel"] p,
          [data-testid="stSidebar"] [data-testid="stWidgetLabel"] label,
          [data-testid="stSidebar"] [data-baseweb="radio"] label p {
            color: #e2e8f0 !important;
            -webkit-text-fill-color: #e2e8f0 !important;
            font-size: 0.84rem !important;
            font-weight: 500 !important;
          }
          [data-testid="stSidebar"] [data-baseweb="input"],
          [data-testid="stSidebar"] [data-baseweb="select"] {
            background: rgba(255, 255, 255, 0.05) !important;
            border: 1px solid rgba(255, 255, 255, 0.12) !important;
            border-radius: 8px !important;
          }
          [data-testid="stSidebar"] [data-baseweb="input"] input,
          [data-testid="stSidebar"] [data-baseweb="select"] input,
          [data-testid="stSidebar"] [data-baseweb="base-input"] {
            color: #f1f5f9 !important;
            -webkit-text-fill-color: #f1f5f9 !important;
          }
          [data-testid="stSidebar"] [data-baseweb="slider"] p,
          [data-testid="stSidebar"] [data-baseweb="switch"] p,
          [data-testid="stSidebar"] [data-baseweb="switch"] label,
          [data-testid="stSidebar"] [data-baseweb="switch"] span { color: #e2e8f0 !important; }
          [data-testid="stSidebar"] [data-baseweb="radio"] label,
          [data-testid="stSidebar"] [data-baseweb="radio"] label span { color: #e2e8f0 !important; }
          [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
            background: rgba(255, 255, 255, 0.04) !important;
            border: 1px dashed rgba(255, 255, 255, 0.22) !important;
            border-radius: 10px !important;
          }
          [data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * { color: #d1d5db !important; }
          [data-testid="stSidebar"] [data-testid="stFileUploaderFileName"] { color: #e2e8f0 !important; }
          /* Main: user row chip — white card, purple accent (matches project cards) */
          .cfg-user-chip-main {
            display: inline-flex; align-items: center; justify-content: flex-start; gap: 0.4rem;
            padding: 0 0.65rem; height: 2.3rem; width: 100%; box-sizing: border-box;
            border-radius: 10px; border: 1px solid #e5e7eb;
            border-left: 3px solid var(--sqg-accent) !important;
            background: #ffffff !important;
            color: var(--sqg-ink) !important;
            font-size: 0.8rem; font-weight: 600; margin: 0;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
          }
          .cfg-user-chip-ico {
            width: 1.4rem; height: 1.4rem; border-radius: 999px; flex-shrink: 0;
            display: inline-flex; align-items: center; justify-content: center;
            background: #ede9fe !important; color: #6d28d9 !important;
            font-size: 0.72rem; font-weight: 800; line-height: 1; border: 1px solid #ddd6fe;
          }
          .cfg-user-chip-name { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
          .cfg-user-actions { max-width: 220px; margin-left: auto; }
          .cfg-user-actions [data-testid="stButton"] { margin: 0 !important; }
          .cfg-user-actions [data-testid="stButton"] button {
            height: 2.3rem !important; min-height: 2.3rem !important; box-sizing: border-box !important;
            border-radius: 8px !important;
          }
          section.main [data-testid="stProgressBar"] > div > div { background: var(--sqg-accent) !important; }
          section.main [data-testid="stProgressBar"] { background: #e5e7eb !important; border-radius: 6px !important; }
          section.main [data-testid="stExpander"] details > summary,
          section.main [data-testid="stExpander"] summary,
          section.main [data-testid="stExpander"] summary p {
            color: var(--sqg-ink) !important; font-weight: 600 !important;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_no_active_project_state(page_name: str = "chat") -> None:
    """Styled empty state when no project is selected (Chat / Configuration)."""
    page = (page_name or "chat").strip().lower()
    _render_workbench_sidebar_shell("signout_wb_no_project")

    page_title = "Chat" if page == "chat" else "Configuration"
    followup_text = (
        "Open a project from the dashboard to start asking questions in natural language."
        if page == "chat"
        else "Open a project from the dashboard to configure your data source and activate schema."
    )

    st.markdown(f'<div class="sqg-dash-title"><h1>{page_title}</h1></div>', unsafe_allow_html=True)
    st.markdown(
        '<p class="sqg-dash-sub">Project context is required before continuing.</p>',
        unsafe_allow_html=True,
    )
    st.markdown(
        (
            '<div class="sqg-dash-info">'
            '<b>No active project selected.</b> '
            f"{followup_text}"
            "</div>"
        ),
        unsafe_allow_html=True,
    )

    _l, _c, _r = st.columns([1.8, 1.4, 1.8])
    with _c:
        if st.button("Go to dashboard", key="np_go_dashboard", type="primary", use_container_width=True):
            st.switch_page("pages/dashboard.py")


def _render_chat_locked_ui() -> None:
    """Styled locked-chat screen (UI only) until schema activation is complete."""
    _auth = st.session_state.auth_user or {}
    _user = (_auth.get("username") or "user").strip()
    _user_initial = (_user[:1] or "U").upper()
    _p = find_project_by_id(get_active_project_id() or "")
    _pcc = (str(((_p or {}).get("client_code") or ""))).strip()
    _pname = (_p or {}).get("name") or "—"
    _comp = get_tenant_by_id((_p or {}).get("tenant_id") or "")
    _cn = (_comp or {}).get("name") or ""

    st.markdown(
        """
        <style>
          .chat-lock-shell {
            min-height: 72vh;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
            padding: 1.2rem 0.8rem 1.6rem;
          }
          .chat-lock-card { max-width: 560px; width: 100%; }
          .chat-lock-ico {
            width: 72px;
            height: 72px;
            margin: 0 auto 1rem;
            border-radius: 18px;
            border: 1px solid #2a3658;
            background: rgba(30, 41, 82, 0.55);
            display: flex;
            align-items: center;
            justify-content: center;
            color: #a5b4fc;
            font-size: 1.65rem;
            box-shadow: 0 10px 28px rgba(0,0,0,0.35);
          }
          .chat-lock-title {
            margin: 0;
            color: #eef2ff;
            font-size: 1.9rem;
            font-weight: 800;
            letter-spacing: -0.02em;
          }
          .chat-lock-sub {
            margin: 0.55rem auto 1.15rem;
            color: #9ca3af;
            max-width: 480px;
            line-height: 1.55;
            font-size: 0.95rem;
          }
          .chat-lock-steps {
            margin: 1.25rem auto 0;
            display: grid;
            grid-template-columns: repeat(3, minmax(0, 1fr));
            gap: 0.65rem;
            max-width: 520px;
            border-top: 1px solid #1f2937;
            padding-top: 0.9rem;
          }
          .chat-lock-step {
            color: #6b7280;
            font-size: 0.75rem;
            line-height: 1.35;
          }
          .chat-lock-step b {
            display: inline-flex;
            width: 1.3rem;
            height: 1.3rem;
            border-radius: 999px;
            align-items: center;
            justify-content: center;
            border: 1px solid #374151;
            color: #9ca3af;
            margin-bottom: 0.2rem;
            font-size: 0.7rem;
          }
          .chat-sb-brand {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            margin-bottom: 0.7rem;
          }
          .chat-sb-brand-ico {
            width: 1.45rem;
            height: 1.45rem;
            border-radius: 6px;
            background: linear-gradient(135deg, #8b5cf6, #3b82f6);
            display: inline-flex;
            align-items: center;
            justify-content: center;
            color: #fff;
            font-size: 0.75rem;
            font-weight: 700;
          }
          .chat-sb-brand-name {
            color: #f8fafc;
            font-size: 0.9rem;
            font-weight: 700;
          }
          .chat-sb-user {
            background: rgba(17, 24, 39, 0.72);
            border: 1px solid #1f2937;
            border-radius: 10px;
            padding: 0.58rem 0.62rem;
            margin: 0.55rem 0 0.9rem;
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.6rem;
          }
          .chat-sb-user-left {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            min-width: 0;
          }
          .chat-sb-user-ico {
            width: 1.6rem;
            height: 1.6rem;
            border-radius: 999px;
            background: linear-gradient(135deg, #f59e0b, #f97316);
            color: #111827;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            font-size: 0.74rem;
            font-weight: 800;
            flex-shrink: 0;
          }
          .chat-sb-user-name {
            color: #e5e7eb;
            font-size: 0.82rem;
            font-weight: 700;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
          }
          .chat-sb-user-sub {
            color: #9ca3af;
            font-size: 0.73rem;
          }
          .chat-sb-sec {
            color: #6b7280;
            font-size: 0.66rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            font-weight: 700;
            margin: 0.85rem 0 0.35rem;
          }
          .chat-sb-chip-wrap {
            display: flex;
            flex-wrap: wrap;
            gap: 0.34rem;
            margin-bottom: 0.55rem;
          }
          .chat-sb-chip {
            background: #181d2b;
            border: 1px solid #312e81;
            color: #c4b5fd;
            border-radius: 999px;
            padding: 0.08rem 0.38rem;
            font-size: 0.68rem;
          }
          .chat-sb-muted {
            color: #9ca3af;
            font-size: 0.78rem;
            line-height: 1.4;
            margin-bottom: 0.75rem;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.sidebar:
        st.markdown(
            (
                '<div class="chat-sb-brand">'
                '<span class="chat-sb-brand-ico">Q</span>'
                '<span class="chat-sb-brand-name">NL → SQL</span>'
                "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.markdown(
            (
                '<div class="chat-sb-user">'
                '<div class="chat-sb-user-left">'
                f'<span class="chat-sb-user-ico">{_user_initial}</span>'
                '<div>'
                f'<div class="chat-sb-user-name">{_user}</div>'
                '<div class="chat-sb-user-sub">Signed in</div>'
                "</div></div></div>"
            ),
            unsafe_allow_html=True,
        )
        if st.button("Sign out", use_container_width=True, key="signout_gated"):
            st.session_state.auth_user = None
            st.switch_page("pages/signin.py")
            st.stop()

        st.markdown('<p class="chat-sb-sec">Project</p>', unsafe_allow_html=True)
        st.markdown(
            (
                '<div class="chat-sb-chip-wrap">'
                + (f'<span class="chat-sb-chip">{_pcc}</span>' if _pcc else "")
                + (f'<span class="chat-sb-chip">{_pname}</span>' if _pname else "")
                + (f'<span class="chat-sb-chip">{_cn}</span>' if _cn else "")
                + "</div>"
            ),
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="chat-sb-muted">Natural language to SQL conversion for your database schema.</div>',
            unsafe_allow_html=True,
        )
        st.markdown('<p class="chat-sb-sec">Navigation</p>', unsafe_allow_html=True)
        st.page_link("pages/project_configuration.py", label="Configuration", icon="⚙️")
        st.page_link("pages/project_chat.py", label="Chat", icon="💬")

    st.markdown(
        """
        <div class="chat-lock-shell">
          <div class="chat-lock-card">
            <div class="chat-lock-ico">🔒</div>
            <h2 class="chat-lock-title">Chat is locked</h2>
            <p class="chat-lock-sub">
              Activate a schema in <b>Configuration</b> to unlock the chat interface
              and start querying.
            </p>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    _c1, _c2, _c3 = st.columns([2.6, 1.8, 2.6])
    with _c2:
        if st.button("Open Configuration ↗", type="primary", key="main_gate_m1", use_container_width=True):
            st.switch_page("pages/project_configuration.py")

    st.markdown(
        """
        <div class="chat-lock-steps">
          <div class="chat-lock-step"><b>1</b><br/>Open<br/>Configuration</div>
          <div class="chat-lock-step"><b>2</b><br/>Activate a<br/>schema</div>
          <div class="chat-lock-step"><b>3</b><br/>Chat<br/>unlocks</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
            st.warning("Could not read the schema job status. Check that the data service is running, then try again.")
            with st.expander("Technical details"):
                st.caption((jerr or (r.text[:500] if r.text else "No response") or ""))
            return
        if not isinstance(info, dict):
            return

        st.session_state.schema_job_paused = bool(info.get("paused"))

        st.markdown("**Preparing your schema (background job)**")
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
            if st.button("Stop", key=f"sch_job_cancel_{jid}", type="secondary"):
                _post_schema_job_control(jid, sid, "cancel")

        stt = info.get("status")
        if stt == "done":
            st.session_state.nl_ready = True
            st.session_state.cfg_dialog_open = False
            st.session_state.schema_job_paused = False
            st.session_state.schema_job_result = info.get("result")
            st.session_state.schema_activation_job_id = None
            st.session_state.show_chat_invite = True
            st.rerun()
        elif stt == "error":
            st.session_state.schema_job_error = info.get("error", "Activation job failed")
            st.session_state.schema_job_paused = False
            st.session_state.schema_activation_job_id = None
            st.rerun()
    except Exception as ex:
        st.caption(f"Could not poll job status: {ex}")


# Deferred from the sidebar "Activate" button so progress renders in the main area (right),
# same as the async /schema/from-file job poller.
_PENDING_SYNC_SCHEMA = "_pending_schema_from_file_sync"


def _run_pending_sync_schema_from_file() -> None:
    pending = st.session_state.get(_PENDING_SYNC_SCHEMA)
    if not isinstance(pending, dict):
        return
    st.session_state.pop(_PENDING_SYNC_SCHEMA, None)
    post_data = pending.get("post_data") or {}
    file_name = pending.get("file_name") or "schema.json"
    file_body = pending.get("file_body") or b""
    timeout = int(pending.get("timeout") or 120)
    post_files = {"file": (file_name, file_body, "application/json")}

    st.markdown("**Preparing your schema (uploading JSON to the API…)**")
    try:
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
                data=post_data,
                files=post_files,
                timeout=timeout,
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
            st.session_state.schema_upload_need_connection_help = False
            st.session_state.nl_ready = True
            st.session_state.cfg_dialog_open = False
            st.session_state.show_chat_invite = True
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


def _chat_context_chips(pname: str, pcc: str, cname: str) -> list[str]:
    out: list[str] = []
    for raw in (pcc, (pname or "").split("_")[0].lower() if pname else "", (cname or "").split()[0].lower() if cname else ""):
        s = (raw or "").strip().lower()[:12]
        if s and s not in out:
            out.append(s)
    return out[:3]


def _chat_recent_user_prompts() -> list[str]:
    out: list[str] = []
    for turn in reversed(st.session_state.get("chat_history") or []):
        if turn.get("role") != "user":
            continue
        t = (turn.get("content") or "").strip()
        if t and t not in out:
            out.append(t)
        if len(out) >= 5:
            break
    return out


def _chat_recent_user_prompt_groups(max_items: int = 200) -> list[tuple[str, list[str]]]:
    """
    Build sidebar history groups like ChatGPT (Today, Yesterday, dated buckets),
    deduping repeated prompts while keeping the newest occurrence.
    """
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)

    def _turn_date(turn: dict) -> datetime.date | None:
        raw = (
            turn.get("created_at")
            or turn.get("timestamp")
            or turn.get("time")
            or ""
        )
        s = str(raw or "").strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
        except Exception:
            return None

    grouped: dict[str, list[str]] = {}
    seen: set[str] = set()
    taken = 0
    for turn in reversed(st.session_state.get("chat_history") or []):
        if turn.get("role") != "user":
            continue
        txt = (turn.get("content") or "").strip()
        if not txt:
            continue
        key = txt.lower()
        if key in seen:
            continue
        seen.add(key)
        d = _turn_date(turn)
        if d == today:
            bucket = f"Today · {today.strftime('%d %b %Y')}"
        elif d == yesterday:
            bucket = f"Yesterday · {yesterday.strftime('%d %b %Y')}"
        elif d:
            bucket = d.strftime("%d %b %Y")
        else:
            bucket = "Earlier"
        grouped.setdefault(bucket, []).append(txt)
        taken += 1
        if taken >= max_items:
            break
    return list(grouped.items())


def run() -> None:
    if "auth_user" not in st.session_state:
        st.session_state.auth_user = None
    
    if not st.session_state.auth_user:
        st.switch_page("pages/signin.py")
        st.stop()
    # Chat: `apply_dashboard_theme` + `apply_tenant_page_shell` + `apply_chat_page_theme` in `project_chat.py`
    # Configuration: `apply_dashboard_theme` + `apply_tenant_page_shell` in `project_configuration.py`

    # ── Session (per project / FastAPI session_id) ─────────────────────────────
    apply_project_workspace(ensure_tenant_state)
    if not get_active_project_id() or not find_project_by_id(get_active_project_id() or ""):
        if workbench_page() in ("chat", "configuration"):
            _render_no_active_project_state(workbench_page())
        else:
            st.error("No active project. Open a project from the dashboard first.")
            st.page_link("pages/dashboard.py", label="Back to dashboard", icon="🏠")
        st.stop()
    # Dynamic DB connection (API session mirrors per-project session_id)
    _DB_DEFAULTS = {
        "conn_host": "",
        "conn_port": "5432",
        "conn_user": "",
        "conn_pass": "",
        "catalog_db": "",
        "db_list": [],
        "pick_database": "",
        "schema_list": [],
        "pick_schemas": [],
        "table_flat": [],
        "pick_tables": [],
        "sel_table_labels": [],
        "table_catalog_fp": "",
        "nl_ready": False,
        "conn_source": "live",
        "file_db_name": "",
        "pg_session_connected": False,
        "conn_use_ssh": False,
        "conn_ssh_host": "",
        "conn_ssh_port": "22",
        "conn_ssh_user": "",
        "conn_ssh_key": "",
        "conn_ssh_key_pass": "",
    }
    for _k, _v in _DB_DEFAULTS.items():
        if _k not in st.session_state:
            st.session_state[_k] = _v
    _legacy_cfg_source_map = {
        "Live PostgreSQL": "Local DB connection",
        "Connect database (PostgreSQL)": "Local DB connection",
        "Upload schema JSON": "Remote URL connection",
    }
    _cfg_src = st.session_state.get("cfg_schema_source")
    if _cfg_src in _legacy_cfg_source_map:
        st.session_state["cfg_schema_source"] = _legacy_cfg_source_map[_cfg_src]
    st.session_state.setdefault("chat_history", [])
    st.session_state.setdefault("suggested_prompts", [])
    st.session_state.setdefault("prompts_last_query", "")
    st.session_state.setdefault("schema_activation_job_id", None)
    st.session_state.setdefault("schema_job_result", None)
    st.session_state.setdefault("schema_job_error", None)
    st.session_state.setdefault("cfg_dialog_open", False)
    st.session_state.setdefault("top_k", 3)
    st.session_state.setdefault("row_limit", 20)
    st.session_state.setdefault("schema_job_paused", False)
    st.session_state.setdefault("show_chat_invite", False)
    st.session_state.setdefault("schema_upload_need_connection_help", False)
    st.session_state.setdefault("_pending_cfg_source_live", False)
    st.session_state.setdefault("_pending_upload_mode_new_connection", False)
    st.session_state.setdefault("cfg_show_connection_main", False)
    st.session_state.setdefault("chat_change_schema_open", False)
    st.session_state.setdefault("chat_change_schema_db", "")
    st.session_state.setdefault("chat_change_schema_schemas", [])

    # API calls use the per-project session id (mirrors st.session_state.session_id)
    _sid = st.session_state.session_id

    def _ensure_workbench_health(sid: str) -> None:
        """Load ``/health`` into ``_workbench_health`` or stop if the API is down."""
        try:
            r = requests.get(
                f"{API_URL}/health",
                params={"session_id": sid},
                timeout=5,
            )
        except requests.exceptions.ConnectionError:
            st.error("**Can’t connect to the data service.** It may be stopped, or the address in your environment may be wrong.")
            with st.expander("For administrators (how to run the API locally)"):
                st.code(
                    f"python -m uvicorn main:app --reload --port {_API_PORT}\n# Expected URL: {API_URL}",
                    language="bash",
                )
            st.stop()
        if not r.ok:
            st.error("**The data service did not respond.** Check that the API is running and matches your environment settings.")
            with st.expander("Technical details"):
                st.caption(f"URL: `{API_URL}` · HTTP {r.status_code}")
            st.stop()
        try:
            d = r.json() if r.content else {}
        except Exception:
            d = {}
        st.session_state["_workbench_health"] = d

    _ensure_workbench_health(_sid)

    # ── Block Chat until Configuration has an activated schema ───────────────────
    if workbench_page() == "chat" and not _nl_session_ready():
        _render_chat_locked_ui()
        st.stop()

    # ── Sidebar: Chat only — navigation + account + clear (no connection / schema / query defaults) ─
    if workbench_page() == "chat":
        with st.sidebar:
            _auth = st.session_state.auth_user or {}
            _uname = str(_auth.get("username", "user") or "user")
            _init = (html.escape(_uname[:1] or "?")).upper()
            _display = html.escape(_uname)
            st.markdown(
                f"""
                <div class="sqg-sb-top" style="display:flex;align-items:center;gap:0.5rem">
                  <span style="display:flex;width:30px;height:30px;border-radius:8px;background:#5b21b6;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.12)">
                    <span style="display:flex;flex-direction:column;gap:2px;align-items:flex-start;justify-content:center">
                      <span style="height:2px;width:12px;background:#fff;border-radius:1px"></span>
                      <span style="height:2px;width:8px;background:#fff;border-radius:1px;opacity:0.95"></span>
                      <span style="height:2px;width:10px;background:#fff;border-radius:1px;opacity:0.9"></span>
                    </span>
                  </span>
                  <span class="sqg-sb-brand" style="margin:0">Smart Query</span>
                </div>
                <div class="sqg-sb-user">
                  <div class="sqg-sb-av">{_init}</div>
                  <div>
                    <div class="sqg-sb-name">{_display}</div>
                    <div class="sqg-sb-role">Member</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Go to dashboard", use_container_width=True, key="chat_go_dashboard_btn"):
                st.switch_page("pages/dashboard.py")
                st.stop()
            _cc1, _cc2 = st.columns(2)
            with _cc1:
                if st.button("Clear conversation", key="chat_clear_conversation_btn", use_container_width=True, type="secondary"):
                    st.session_state.chat_history = []
                    st.session_state.suggested_prompts = []
                    st.session_state.prompts_last_query = ""
                    for _k in list(st.session_state.keys()):
                        if _k.startswith("pg_") or _k.startswith("rows_") or _k.startswith("chart_") or _k.startswith("page_sz_"):
                            st.session_state.pop(_k, None)
                    st.success("Conversation cleared.")
                    st.rerun()
            with _cc2:
                if st.button("Change schema", key="chat_change_schema_btn", use_container_width=True):
                    st.session_state.chat_change_schema_open = True
                    st.rerun()
            st.markdown("<p class='sqg-chat-sec'>Recent questions</p>", unsafe_allow_html=True)
            _rc_groups = _chat_recent_user_prompt_groups(max_items=250)
            _last_u = ""
            for t in reversed(st.session_state.get("chat_history") or []):
                if t.get("role") == "user":
                    _last_u = (t.get("content") or "").strip()
                    break
            # Always keep recent questions inside a fixed-height scroll area so
            # footer controls are not pushed down as history grows.
            try:
                _recent_parent = st.container(height=360, border=False)
            except TypeError:
                _recent_parent = st.container()
            with _recent_parent:
                _btn_i = 0
                for _bucket, _items in _rc_groups:
                    st.caption(_bucket)
                    for _txt in _items:
                        _short = _txt if len(_txt) <= 42 else _txt[:40] + "…"
                        if st.button(
                            _short,
                            key=f"ch_sb_recent_{_btn_i}",
                            use_container_width=True,
                            type="secondary" if _txt != _last_u else "primary",
                            help=_txt,
                        ):
                            st.session_state["pending_prompt"] = _txt
                            st.rerun()
                        _btn_i += 1
            if not _rc_groups:
                st.caption("Your questions will show here after you ask something.")
            st.markdown(
                """
                <style>
                .st-key-signout_chat {
                  position: sticky;
                  bottom: 0;
                  padding-top: 0.45rem;
                  margin-top: 0.45rem;
                  background: linear-gradient(180deg, rgba(10,13,33,0.0), rgba(10,13,33,0.9) 40%);
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Logout", use_container_width=True, key="signout_chat", type="secondary"):
                clear_auth_session()
                st.switch_page("pages/signin.py")
                st.stop()

        if st.session_state.get("chat_change_schema_open"):
            @st.dialog("Change schema")
            def _chat_change_schema_dialog():
                sid = (st.session_state.get("session_id") or "").strip()
                if not st.session_state.get("pg_session_connected"):
                    st.warning(
                        "You can continue chatting with the currently active schema. "
                        "To change schema, first connect a live database in Configuration."
                    )
                    c_nc1, c_nc2 = st.columns(2)
                    with c_nc1:
                        if st.button("Open Configuration", type="primary", use_container_width=True, key="chat_change_schema_open_cfg"):
                            st.session_state.chat_change_schema_open = False
                            st.switch_page("pages/project_configuration.py")
                    with c_nc2:
                        if st.button("Cancel", use_container_width=True, key="chat_change_schema_cancel_disconnected"):
                            st.session_state.chat_change_schema_open = False
                            st.rerun()
                    return
                _db_opts = list(st.session_state.get("db_list") or [])
                if not _db_opts:
                    try:
                        _rc = requests.post(
                            f"{API_URL}/db/connect",
                            json=_db_connect_request_body(),
                            timeout=45,
                        )
                        _bi, _be = safe_response_payload(_rc)
                        if not _be and _rc.ok and isinstance(_bi, dict):
                            _db_opts = _bi.get("databases") or []
                            st.session_state.db_list = _db_opts
                    except Exception:
                        _db_opts = list(st.session_state.get("db_list") or [])
                if not _db_opts:
                    st.error("Could not load database list.")
                    if st.button("Cancel", use_container_width=True, key="chat_change_schema_cancel_nodb"):
                        st.session_state.chat_change_schema_open = False
                        st.rerun()
                    return

                _default_db = (st.session_state.get("chat_change_schema_db") or st.session_state.get("sb_database") or _db_opts[0])
                if _default_db not in _db_opts:
                    _default_db = _db_opts[0]
                _db_sel = st.selectbox("Database", options=_db_opts, index=_db_opts.index(_default_db), key="chat_change_schema_db")

                _schema_opts: list[str] = []
                try:
                    _ru = requests.post(
                        f"{API_URL}/db/use-database",
                        json={"session_id": sid, "database": _db_sel},
                        timeout=45,
                    )
                    if _ru.ok:
                        _rs = requests.get(f"{API_URL}/db/schemas", params={"session_id": sid}, timeout=45)
                        if _rs.ok:
                            _schema_opts = _rs.json().get("schemas") or []
                except Exception:
                    _schema_opts = []
                if not _schema_opts:
                    _schema_opts = list(st.session_state.get("schema_list") or [])
                _default_sch = [s for s in (st.session_state.get("chat_change_schema_schemas") or st.session_state.get("pick_schemas") or []) if s in _schema_opts]
                if not _default_sch and _schema_opts:
                    _default_sch = _schema_opts[:1]
                _sch_sel = st.multiselect(
                    "Schemas",
                    options=_schema_opts,
                    default=_default_sch,
                    key="chat_change_schema_schemas",
                    help="Select one or more schemas to activate for chat.",
                )
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Add", type="primary", use_container_width=True, key="chat_change_schema_add"):
                        ok, msg = _apply_chat_schema_change(_db_sel, _sch_sel)
                        if ok:
                            st.session_state.chat_change_schema_open = False
                            st.success(msg)
                            st.rerun()
                        st.error(msg)
                with c2:
                    if st.button("Cancel", use_container_width=True, key="chat_change_schema_cancel"):
                        st.session_state.chat_change_schema_open = False
                        st.rerun()

            _chat_change_schema_dialog()
    
    # ── Sidebar: database & schema (Configuration page only) ────────────────────
    if workbench_page() == "configuration":
        _apply_configuration_ui_redesign_styles()
        with st.sidebar:
            _auth = st.session_state.auth_user or {}
            _uname = str(_auth.get("username", "user") or "user")
            _init = (html.escape(_uname[:1] or "?")).upper()
            _display = html.escape(_uname)
            st.markdown(
                f"""
                <div class="sqg-sb-top" style="display:flex;align-items:center;gap:0.5rem">
                  <span style="display:flex;width:30px;height:30px;border-radius:8px;background:#5b21b6;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.12)">
                    <span style="display:flex;flex-direction:column;gap:2px;align-items:flex-start;justify-content:center">
                      <span style="height:2px;width:12px;background:#fff;border-radius:1px"></span>
                      <span style="height:2px;width:8px;background:#fff;border-radius:1px;opacity:0.95"></span>
                      <span style="height:2px;width:10px;background:#fff;border-radius:1px;opacity:0.9"></span>
                    </span>
                  </span>
                  <span class="sqg-sb-brand" style="margin:0">Smart Query</span>
                </div>
                <div class="sqg-sb-user">
                  <div class="sqg-sb-av">{_init}</div>
                  <div>
                    <div class="sqg-sb-name">{_display}</div>
                    <div class="sqg-sb-role">Member</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            if st.button("Go to dashboard", use_container_width=True, key="cfg_go_dashboard_btn"):
                st.switch_page("pages/dashboard.py")
                st.stop()
            _p = find_project_by_id(get_active_project_id() or "")
            _pname = (_p or {}).get("name") or "—"
            _pcc = (str(((_p or {}).get("client_code") or ""))).strip()
            _comp = get_tenant_by_id((_p or {}).get("tenant_id") or "")
            _cn = (_comp or {}).get("name") or ""

            st.caption("Project context")
            st.markdown(
                (
                    '<div class="cfg-sb-project-row">'
                    + f'<span class="cfg-sb-chip">{_pname}</span>'
                    + (f'<span class="cfg-sb-chip">{_cn}</span>' if _cn else "")
                    + (f'<span class="cfg-sb-chip">{_pcc}</span>' if _pcc else "")
                    + "</div>"
                ),
                unsafe_allow_html=True,
            )

            st.divider()
            st.markdown('<p class="cfg-sb-sec">Settings</p>', unsafe_allow_html=True)
            st.toggle("Natural language to SQL", value=True, key="cfg_nl_sql_toggle")
            st.markdown('<p class="cfg-sb-sec">Query defaults (Chat)</p>', unsafe_allow_html=True)
            st.caption(
                "Used on the Chat page: how many tables to consider (top-K) and the default row cap when "
                "your question does not name a number."
            )
            st.session_state.top_k = st.slider(
                "Tables to retrieve (top-K)",
                1,
                10,
                int(st.session_state.top_k or 3),
                key="cfg_topk",
            )
            _rlo_cfg = streamlit_row_limit_options()
            _rdef_cfg = (
                st.session_state.row_limit
                if st.session_state.row_limit in _rlo_cfg
                else _rlo_cfg[0]
            )
            st.session_state.row_limit = st.select_slider(
                "Default rows per page",
                options=_rlo_cfg,
                value=_rdef_cfg,
                key="cfg_rowlim",
                help=(
                    "If your question already says a number (e.g. 'top 5', 'show 20'), that number is used. "
                    "Queries use LIMIT + OFFSET for pagination."
                ),
            )
            st.divider()
            if st.button("Sign out", use_container_width=True, type="secondary", key="signout_cfg_sb"):
                clear_auth_session()
                st.switch_page("pages/signin.py")
                st.stop()

    # ── Main area ─────────────────────────────────────────────────────────────────
    if workbench_page() == "configuration":
        _render_cfg_notifications()
        _cfg_l, _cfg_r = st.columns([7.0, 2.0], gap="small")
        with _cfg_l:
            st.markdown('<div class="sqg-dash-title"><h1>Configuration</h1></div>', unsafe_allow_html=True)
            st.markdown(
                '<p class="sqg-dash-sub">Choose your connection type below, then complete setup in this panel.</p>',
                unsafe_allow_html=True,
            )
            # Apply pending source/mode switches before rendering the selector.
            if st.session_state.pop("_pending_cfg_source_live", False):
                st.session_state["cfg_schema_source"] = "Local DB connection"
            if st.session_state.pop("_pending_upload_mode_new_connection", False):
                st.session_state["cfg_schema_source"] = "Remote URL connection"
                st.session_state["schema_json_upload_mode"] = (
                    "New connection — PostgreSQL host/user, connect, then upload JSON"
                )
            if st.session_state.get("cfg_schema_source") not in _cfg_source_options():
                st.session_state["cfg_schema_source"] = _cfg_source_options()[0]
            _apply_cfg_source_selection(st.session_state.get("cfg_schema_source") or _cfg_source_options()[0])
            _is_conn_cfg = bool(st.session_state.get("pg_session_connected"))
            st.caption("Connection status: Connected" if _is_conn_cfg else "Connection status: Not connected")
        with _cfg_r:
            _top_chat_ready = _nl_session_ready()
            _top_chat_blocked = _schema_activation_running_without_pause() if _top_chat_ready else False
            if _top_chat_ready:
                st.markdown("<div style='height:2.1rem'></div>", unsafe_allow_html=True)
                if st.button(
                    "Go to Chat →",
                    key="main_go_chat_top_btn",
                    use_container_width=True,
                    type="primary",
                    disabled=_top_chat_blocked,
                    help="Available when schema activation is finished or paused.",
                ):
                    st.switch_page("pages/project_chat.py")

        # Synchronous /schema/from-file: deferred from the sidebar so the bar appears in the main pane.
        _run_pending_sync_schema_from_file()

        # Connection / upload UI must run *before* the chat-invite dialog so
        # ``show_chat_invite`` is set in the same run as "Activate selection" / upload success.
        if st.session_state.get("cfg_show_connection_main", False):
            st.divider()
            _render_configuration_connection_main(_sid)
        else:
            _render_configuration_getting_started()
            if st.button("Continue to connection setup", type="primary", key="cfg_continue_conn_main"):
                st.session_state["cfg_show_connection_main"] = True
                st.rerun()
        # Show long-running schema activation progress directly after setup section
        # so users don't need to scroll back up to find the live bar/controls.
        if st.session_state.get("schema_activation_job_id"):
            st.divider()
            _poll_schema_upload_job_fragment()
        if st.session_state.get("show_chat_invite"):

            @st.dialog("Schema is active")
            def _invite_chat_dialog():
                st.success("**Configuration** is complete — your schema is **active**.")
                st.markdown(
                    "Use **Go to Chat** to ask questions in plain language, or stay on Configuration to change connection or tables."
                )
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Go to Chat", type="primary", use_container_width=True, key="dlg_invite_m2"):
                        st.session_state.show_chat_invite = False
                        st.switch_page("pages/project_chat.py")
                with c2:
                    if st.button("Stay on this page", use_container_width=True, key="dlg_invite_stay"):
                        st.session_state.show_chat_invite = False
                        st.rerun()

            _invite_chat_dialog()
        if st.session_state.pop("schema_chat_nav_blocked", False):
            _cfg_notify("warning", "Chat is unavailable while the schema job is still running.")
        _sync_blocking_chat = _schema_activation_running_without_pause()
        if _nl_session_ready():
            if not _sync_blocking_chat:
                _cfg_notify_once("schema_active_ready", "success", "Schema is active. Open the Chat page from the sidebar (or below).")
            else:
                _cfg_notify_once("schema_active_running", "warning", "Schema activation is still running (use Pause to open Chat on partial data).")
            if st.button(
                "Go to Chat →",
                type="primary",
                key="main_go_chat_btn",
                disabled=_sync_blocking_chat,
                help="Available when schema activation is finished or paused.",
            ):
                st.switch_page("pages/project_chat.py")
        _cfg_notify_once("only_activated_tables", "success", "Only the tables you activate are used for this session.")
        st.stop()
    else:
        # Chat: single-column header (no top prompt dropdown)
        st.session_state.pop("header_try_prompt", None)
        _h_hdr = st.session_state.get("_workbench_health") or {}
        _hdr_tc = int(_h_hdr.get("table_count") or 0)
        _hdr_db = (st.session_state.get("file_db_name") or _h_hdr.get("database") or "schema")
        st.markdown(
            (
                f'<div class="sqg-chat-hero-brief">'
                f'<div class="sqg-dash-title"><h1>Chat</h1></div>'
                f'<p class="sqg-dash-sub">{html.escape(_hdr_db)} · {_hdr_tc} tables active</p>'
                f"</div>"
            ),
            unsafe_allow_html=True,
        )

    # ── Chat main (only when schema is active; gated above) ───────────────────────
    if not _nl_session_ready():
        st.error("Unexpected state: Chat should be locked until the schema is active.")
        st.stop()
    st.session_state.cfg_dialog_open = False

    _h_view = st.session_state.get("_workbench_health") or {}
    _hero_db = (st.session_state.get("file_db_name") or _h_view.get("database") or "schema")

    if not (st.session_state.get("chat_history") or []):
        def _chat_last_user_for_suggest() -> str:
            for t in reversed(st.session_state.get("chat_history") or []):
                if t.get("role") == "user":
                    return str(t.get("content") or "")
            return ""

        def _chat_fetch_suggest_prompts(last_q: str) -> list[str]:
            try:
                r = requests.get(
                    f"{API_URL}/suggest-prompts",
                    params={"session_id": st.session_state.session_id, "last_query": last_q},
                    timeout=15,
                )
                if r.ok:
                    return r.json().get("prompts", []) or []
            except Exception:
                pass
            return []

        _lqs = _chat_last_user_for_suggest()
        if (
            not st.session_state.get("suggested_prompts")
            or st.session_state.get("prompts_last_query") != _lqs
        ):
            _np = _chat_fetch_suggest_prompts(_lqs)
            if _np:
                st.session_state.suggested_prompts = _np
                st.session_state.prompts_last_query = _lqs
        _stext = [str(x).strip() for x in (st.session_state.suggested_prompts or []) if str(x).strip()][:6]
        if _stext:
            st.caption("Try asking")
            sc1, sc2, sc3 = st.columns(3)
            for _ix, _ex in enumerate(_stext):
                with [sc1, sc2, sc3][_ix % 3]:
                    _bl = _ex if len(_ex) <= 44 else _ex[:42] + "…"
                    if st.button(
                        _bl,
                        key=f"ch_starter_{_ix}",
                        use_container_width=True,
                        type="secondary",
                        help=_ex,
                    ):
                        st.session_state["pending_prompt"] = _ex
                        st.rerun()
        else:
            st.caption("Ask a question in the box below — your thread will show here.")

    # ── Chat history (primary — like ChatGPT: scrollable thread, oldest first) ────
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
                    _execution_skipped = bool(data.get("execution_skipped", False))
                    _exp_text = str(data.get("explanation", "") or "").lower()
                    if not _execution_skipped:
                        if "executed on remote sql api" in _exp_text:
                            st.caption("Execution: Remote API")
                        else:
                            st.caption("Execution: PostgreSQL")
    
                    with st.expander("Generated SQL", expanded=False):
                        st.code(data.get("sql", ""), language="sql")
    
                    sql         = data.get("sql", "")
                    columns     = data.get("columns", [])
                    chart       = data.get("chart_suggestion", "table")
                    viz_cfg     = data.get("viz_config") or {}
                    total_count = data.get("total_count", 0)
    
                    # ── Per-message pagination state ──────────────────────────
                    pg_key      = f"pg_{idx}"         # current page number
                    rows_key    = f"rows_{idx}"       # rows for current page (changes per page)
                    chart_key   = f"chart_{idx}"      # ALL rows for chart (set once, never changes)
                    page_sz_key = f"page_sz_{idx}"    # page size used for this message

                    try:
                        ps = max(1, min(int(st.session_state.get("row_limit", 20)), sql_max_page_size()))
                    except Exception:
                        ps = 20
    
                    # Seed initial state on first render
                    if pg_key not in st.session_state:
                        st.session_state[pg_key] = 1
                    if page_sz_key not in st.session_state:
                        st.session_state[page_sz_key] = ps
                    if rows_key not in st.session_state:
                        st.session_state[rows_key] = data.get("rows", [])[:ps]

                    # If user changes "Default rows per page" in sidebar, reset this result to page 1.
                    if st.session_state.get(page_sz_key) != ps:
                        st.session_state[pg_key] = 1
                        st.session_state[rows_key] = data.get("rows", [])[:ps]
                        st.session_state[page_sz_key] = ps
    
                    # Fetch ALL rows for chart rendering (once, on first render).
                    # Chart always shows the complete result — not just the current page.
                    if chart_key not in st.session_state:
                        initial_rows = data.get("rows", [])
                        if total_count > len(initial_rows) and total_count <= sql_max_page_size() and sql:
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
    
                    cur_page   = st.session_state[pg_key]
                    cur_rows   = st.session_state[rows_key]
                    chart_rows = st.session_state[chart_key]   # full dataset for charts
                    total_pages = max(1, -(-total_count // ps))    # ceiling div

                    # Ensure current rows reflect selected page size and current page.
                    _need_refresh = (
                        bool(sql)
                        and total_count > 0
                        and (
                            len(cur_rows) > ps
                            or (total_count > ps and len(cur_rows) != min(ps, max(total_count - ((cur_page - 1) * ps), 0)))
                        )
                    )
                    if _need_refresh:
                        try:
                            _pr = requests.post(
                                f"{API_URL}/sql/page",
                                json={
                                    "sql": sql,
                                    "session_id": st.session_state.session_id,
                                    "page": cur_page,
                                    "page_size": ps,
                                },
                                timeout=60,
                            )
                            if _pr.ok:
                                cur_rows = _pr.json().get("rows", []) or []
                                st.session_state[rows_key] = cur_rows
                        except Exception:
                            pass
    
                    # ── Metrics row ───────────────────────────────────────────
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("Showing",    f"{len(cur_rows):,}")
                    m2.metric("Total rows", f"{total_count:,}")
                    m3.metric("Page",       f"{cur_page} / {total_pages}")
                    m4.metric("Time (ms)",  data.get("execution_ms", 0))
    
                    # Tables used by the agent
                    tables_used = data.get("tables_used", [])
                    if tables_used:
                        _chips = "".join(
                            f'<span class="sqg-chat-used-chip">{html.escape(str(t))}</span>'
                            for t in tables_used
                        )
                        st.markdown(
                            f'<div class="sqg-chat-used"><span class="sqg-chat-used-label">Tables used</span>{_chips}</div>',
                            unsafe_allow_html=True,
                        )
    
                    # ── Large-dataset banner ──────────────────────────────────
                    if total_count > ps:
                        st.info(
                            f"Rows {len(cur_rows):,} of {total_count:,} (page {cur_page}/{total_pages})."
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
                                f"No data in {len(null_cols)} column(s): {', '.join(f'`{c}`' for c in null_cols[:20])}."
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
    st.caption(
        f"Enter to send · Shift+Enter for a new line · Active schema: {html.escape(_hero_db)}"
    )
    
    if prompt:
        _ts_now = datetime.now().isoformat(timespec="seconds")
        st.session_state.chat_history.append({"role": "user", "content": prompt, "created_at": _ts_now})
    
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
    
        st.session_state.chat_history.append({"role": "assistant", "content": "", "data": data, "created_at": datetime.now().isoformat(timespec="seconds")})
        st.rerun()

if __name__ == '__main__':
    st.set_page_config(page_title='NL → SQL', page_icon='🔍', layout='wide')
    set_workbench_page('configuration')
    run()
