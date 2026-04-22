"""
Per-project Streamlit state isolation for the NL→SQL workbench.

Each project has a stable API ``session_id`` (``nl_session_id``) stored on the
project record. All other UI state is snapshotted when switching projects so
COC / BMS / other workspaces do not clobber one another in the browser.
"""

from __future__ import annotations

import uuid
from typing import Any, Callable

import streamlit as st

from ui.tenant.state import ensure_tenant_state, find_project_by_id

NL_ACTIVE_PROJECT = "nl_active_project_id"
NL_SNAPSHOTS = "nl_project_snapshots"
NL_WORKING_ID = "_nl_working_project_id"

# Keys we never put in project snapshots (global auth / project list / our bookkeeping).
_GLOBAL_SKIP: frozenset[str] = frozenset(
    {
        "auth_user",
        "tenant_projects",
        "tenant_recent_activity",
        "tenant_selected_project_id",
        NL_ACTIVE_PROJECT,
        NL_SNAPSHOTS,
        NL_WORKING_ID,
    }
)

# Exact keys saved per project (plus dynamic pg_/rows_/chart_/tcb_ prefixes)
_SNAPSHOT_STABLE: tuple[str, ...] = (
    "session_id",
    "chat_history",
    "suggested_prompts",
    "prompts_last_query",
    "conn_host",
    "conn_port",
    "conn_user",
    "conn_pass",
    "catalog_db",
    "db_list",
    "pick_database",
    "schema_list",
    "pick_schemas",
    "table_flat",
    "pick_tables",
    "sel_table_labels",
    "table_catalog_fp",
    "nl_ready",
    "conn_source",
    "file_db_name",
    "pg_session_connected",
    "schema_activation_job_id",
    "schema_job_result",
    "schema_job_error",
    "cfg_dialog_open",
    "top_k",
    "row_limit",
    "schema_job_paused",
    "schema_chat_nav_blocked",
    "ms_schemas",  # multiselect key for schemas
    "table_search_nl",
    "schema_json_upload_mode",
)

_DEFAULT_ROW_LIMIT = 20
_DEFAULTS_FOR_NEW_PROJECT: dict[str, Any] = {
    "session_id": "",  # replaced by project nl_session_id
    "chat_history": [],
    "suggested_prompts": [],
    "prompts_last_query": "",
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
    "schema_activation_job_id": None,
    "schema_job_result": None,
    "schema_job_error": None,
    "cfg_dialog_open": False,
    "top_k": 3,
    "row_limit": _DEFAULT_ROW_LIMIT,
    "schema_job_paused": False,
}


def _should_snapshot_key(k: str) -> bool:
    if k in _GLOBAL_SKIP:
        return False
    if k in _SNAPSHOT_STABLE:
        return True
    if k.startswith("pg_") or k.startswith("rows_") or k.startswith("chart_") or k.startswith("tcb_"):
        return True
    if k in {"sb_database", "sf_dbname", "in_host", "in_port", "in_user", "in_pass", "in_catdb"}:
        return True
    if k in {"sf_upload", "sf_remote_api_url", "sf_remote_row_limit", "schema_json_upload_mode", "ms_schemas", "ms_schemas"}:
        return True
    return False


def _export_snapshot() -> dict[str, Any]:
    return {
        k: st.session_state[k]
        for k in st.session_state.keys()
        if _should_snapshot_key(k) and k in st.session_state
    }


def _apply_snapshot(merged: dict[str, Any]) -> None:
    for k, v in merged.items():
        st.session_state[k] = v


def set_active_project_id(project_id: str | None) -> None:
    st.session_state[NL_ACTIVE_PROJECT] = (project_id or "").strip() or None


def get_active_project_id() -> str | None:
    v = st.session_state.get(NL_ACTIVE_PROJECT)
    if not v or not str(v).strip():
        return None
    return str(v).strip()


def apply_project_workspace(ensure: Callable[[], None] | None = None) -> bool:
    """
    Call at the start of the NL workbench, after ``ensure_tenant_state``.

    When the active project changes, the previous project's UI state is written to
    ``nl_project_snapshots``; the new project's snapshot (if any) is merged on top
    of defaults. The API ``session_id`` in session state is always the project's
    ``nl_session_id``.
    """
    if ensure is not None:
        ensure()
    else:
        ensure_tenant_state()

    snaps: dict[str, Any] = st.session_state.setdefault(NL_SNAPSHOTS, {})  # type: ignore[assignment]
    work = st.session_state.get(NL_WORKING_ID)
    pid = get_active_project_id()

    if work and isinstance(work, str):
        snap = _export_snapshot()
        if snap:
            snaps[work] = snap

    if not pid:
        st.session_state.pop(NL_WORKING_ID, None)
        return False

    rec = find_project_by_id(pid)
    if not rec:
        st.session_state.pop(NL_WORKING_ID, None)
        return False

    if "nl_session_id" not in rec or not (rec.get("nl_session_id") or "").strip():
        rec["nl_session_id"] = str(uuid.uuid4())
        for p in st.session_state.get("tenant_projects", []):
            if p.get("id") == rec.get("id"):
                p["nl_session_id"] = rec["nl_session_id"]  # type: ignore[union-attr]
                break

    if work != pid:
        base = {**_DEFAULTS_FOR_NEW_PROJECT, **(snaps.get(pid) or {})}
        base["session_id"] = str(rec.get("nl_session_id") or "")
        if not st.session_state.get("row_limit"):
            base["row_limit"] = _DEFAULT_ROW_LIMIT
        _apply_snapshot(base)
        st.session_state[NL_WORKING_ID] = pid
    else:
        st.session_state["session_id"] = str(rec.get("nl_session_id") or st.session_state.get("session_id", ""))
    return True


def require_active_project(redirect: str) -> bool:
    """If no active project, switch_page to *redirect* and return False."""
    if not get_active_project_id() or not find_project_by_id(get_active_project_id() or ""):
        st.switch_page(redirect)  # type: ignore[arg-type]
        st.stop()
        return False
    return True
