"""State: User (auth) → Tenant (company) → Project — persisted in PostgreSQL when signed in."""

from __future__ import annotations

import logging
from datetime import datetime
import uuid

import streamlit as st

PROJECTS_KEY = "tenant_projects"
TENANTS_KEY = "nl_tenants"
ACTIVITY_KEY = "tenant_recent_activity"
SELECTED_PROJECT_ID_KEY = "tenant_selected_project_id"
DEFAULT_TENANT_ID = "ten-default"

log = logging.getLogger(__name__)


def _default_nl_session() -> str:
    return str(uuid.uuid4())


def _migrate_project_dict(p: dict) -> dict:
    p.setdefault("client_code", "")
    p.setdefault("nl_session_id", _default_nl_session())
    p.setdefault("tenant_id", DEFAULT_TENANT_ID)
    return p


def _auth_uid() -> int | None:
    u = (st.session_state.get("auth_user") or {}).get("user_id")
    if u is None:
        return None
    try:
        return int(u)
    except (TypeError, ValueError):
        return None


def _persist_enabled() -> bool:
    return _auth_uid() is not None


def ensure_tenant_state() -> None:
    uid = _auth_uid()
    if uid is not None:
        if ACTIVITY_KEY not in st.session_state:
            st.session_state[ACTIVITY_KEY] = []
        if SELECTED_PROJECT_ID_KEY not in st.session_state:
            st.session_state[SELECTED_PROJECT_ID_KEY] = None
        hkey = f"_workspace_hydrated_v1_{uid}"
        if st.session_state.get(hkey):
            if TENANTS_KEY not in st.session_state:
                st.session_state[TENANTS_KEY] = []
            if PROJECTS_KEY not in st.session_state:
                st.session_state[PROJECTS_KEY] = []
            return
        try:
            from workspace_store import load_workspace, ensure_backend

            ensure_backend()
            tlist, plist = load_workspace(uid)
            st.session_state[TENANTS_KEY] = tlist
            st.session_state[PROJECTS_KEY] = [_migrate_project_dict(p) for p in plist]
            st.session_state[hkey] = True
            st.session_state.pop("workspace_db_error", None)
        except Exception as e:  # noqa: BLE001
            log.exception("Workspace load from DB failed: %s", e)
            st.session_state["workspace_db_error"] = str(e)
            st.session_state[TENANTS_KEY] = st.session_state.get(TENANTS_KEY) or []
            st.session_state[PROJECTS_KEY] = st.session_state.get(PROJECTS_KEY) or []
    else:
        if TENANTS_KEY not in st.session_state:
            st.session_state[TENANTS_KEY] = []
        if PROJECTS_KEY not in st.session_state:
            st.session_state[PROJECTS_KEY] = []
        if ACTIVITY_KEY not in st.session_state:
            st.session_state[ACTIVITY_KEY] = []
        if SELECTED_PROJECT_ID_KEY not in st.session_state:
            st.session_state[SELECTED_PROJECT_ID_KEY] = None


# ── Tenants (companies) ───────────────────────────────────────────────────────


def tenants() -> list[dict]:
    ensure_tenant_state()
    return st.session_state[TENANTS_KEY]


def get_tenant_by_id(tenant_id: str) -> dict | None:
    ensure_tenant_state()
    for t in st.session_state[TENANTS_KEY]:
        if isinstance(t, dict) and t.get("id") == tenant_id:
            return t
    return None


def create_tenant(name: str, code: str) -> dict:
    ensure_tenant_state()
    t: dict = {
        "id": f"ten-{uuid.uuid4().hex[:6]}",
        "name": (name or "").strip() or "Unnamed company",
        "code": (code or "").strip() or "—",
        "updated_at": "just now",
    }
    uid = _auth_uid()
    if _persist_enabled():
        try:
            from workspace_store import db_upsert_tenant

            db_upsert_tenant(uid, t)  # type: ignore[arg-type]
        except Exception as e:  # noqa: BLE001
            log.exception("db_upsert_tenant: %s", e)
            st.session_state["workspace_db_error"] = str(e)
            return None
    st.session_state[TENANTS_KEY].insert(0, t)
    add_activity(f"Created company (tenant) `{t['name']}`.")
    return t


def delete_tenant(tenant_id: str) -> bool:
    if tenant_id == DEFAULT_TENANT_ID:
        return False
    ensure_tenant_state()
    for p in st.session_state[PROJECTS_KEY]:
        if isinstance(p, dict) and p.get("tenant_id") == tenant_id:
            return False
    uid = _auth_uid()
    if _persist_enabled():
        try:
            from workspace_store import db_delete_tenant

            if not db_delete_tenant(uid, tenant_id):  # type: ignore[arg-type]
                return False
        except Exception as e:  # noqa: BLE001
            log.exception("db_delete_tenant: %s", e)
            st.session_state["workspace_db_error"] = str(e)
            return False
    for i, t in enumerate(st.session_state[TENANTS_KEY]):
        if isinstance(t, dict) and t.get("id") == tenant_id:
            st.session_state[TENANTS_KEY].pop(i)
            add_activity(f"Removed company (tenant) `{t.get('name', tenant_id)}`.")
            return True
    return False


def add_activity(text: str) -> None:
    ensure_tenant_state()
    ts = datetime.now().strftime("%H:%M")
    st.session_state[ACTIVITY_KEY].insert(0, f"[{ts}] {text}")
    st.session_state[ACTIVITY_KEY] = st.session_state[ACTIVITY_KEY][:15]


def projects() -> list[dict]:
    ensure_tenant_state()
    return st.session_state[PROJECTS_KEY]


def projects_for_tenant(tenant_id: str | None) -> list[dict]:
    all_p = projects()
    if not tenant_id or tenant_id == "__all__":
        return all_p
    return [p for p in all_p if isinstance(p, dict) and p.get("tenant_id") == tenant_id]


def set_selected_project(project_id: str | None) -> None:
    ensure_tenant_state()
    st.session_state[SELECTED_PROJECT_ID_KEY] = project_id


def selected_project() -> dict | None:
    ensure_tenant_state()
    pid = st.session_state.get(SELECTED_PROJECT_ID_KEY)
    if not pid:
        return None
    for p in st.session_state[PROJECTS_KEY]:
        if p["id"] == pid:
            return _migrate_project_dict(p)
    return None


def find_project_by_id(project_id: str) -> dict | None:
    ensure_tenant_state()
    for p in st.session_state[PROJECTS_KEY]:
        if isinstance(p, dict) and p.get("id") == project_id:
            return _migrate_project_dict(p)
    return None


def update_project_nl_session_id(project_id: str, new_session_id: str) -> bool:
    ensure_tenant_state()
    nid = (new_session_id or "").strip()
    if not nid:
        return False
    uid = _auth_uid()
    for p in st.session_state[PROJECTS_KEY]:
        if isinstance(p, dict) and p.get("id") == project_id:
            p["nl_session_id"] = nid
            if _persist_enabled():
                try:
                    from workspace_store import db_update_project_nl_session

                    db_update_project_nl_session(uid, project_id, nid)  # type: ignore[arg-type]
                except Exception as e:  # noqa: BLE001
                    log.exception("db_update_project_nl_session: %s", e)
            return True
    return False


def create_project(
    name: str,
    description: str,
    *,
    tenant_id: str = DEFAULT_TENANT_ID,
    client_code: str = "",
) -> dict:
    ensure_tenant_state()
    tid = (tenant_id or "").strip() or DEFAULT_TENANT_ID
    if not get_tenant_by_id(tid):
        tid = DEFAULT_TENANT_ID
    project: dict = {
        "id": f"proj-{uuid.uuid4().hex[:6]}",
        "tenant_id": tid,
        "name": name.strip(),
        "description": description.strip() or "No description.",
        "status": "Draft",
        "updated_at": "just now",
        "client_code": (client_code or "").strip(),
        "nl_session_id": _default_nl_session(),
    }
    uid = _auth_uid()
    if _persist_enabled():
        try:
            from workspace_store import db_upsert_project

            db_upsert_project(uid, project)  # type: ignore[arg-type]
        except Exception as e:  # noqa: BLE001
            log.exception("db_upsert_project: %s", e)
            st.session_state["workspace_db_error"] = str(e)
            return None
    st.session_state[PROJECTS_KEY].insert(0, project)
    set_selected_project(project["id"])
    add_activity(f"Created project `{project['name']}`.")
    return project


def update_project(
    project_id: str,
    *,
    name: str,
    description: str,
    status: str,
    client_code: str = "",
    tenant_id: str = "",
) -> bool:
    ensure_tenant_state()
    tid = (tenant_id or "").strip() or DEFAULT_TENANT_ID
    if not get_tenant_by_id(tid):
        tid = DEFAULT_TENANT_ID
    uid = _auth_uid()
    for project in st.session_state[PROJECTS_KEY]:
        if project["id"] == project_id:
            project["name"] = name.strip()
            project["description"] = description.strip() or "No description."
            project["status"] = status
            project["client_code"] = (client_code or "").strip()
            project["tenant_id"] = tid
            project["updated_at"] = "just now"
            if not project.get("nl_session_id"):
                project["nl_session_id"] = _default_nl_session()
            if _persist_enabled():
                try:
                    from workspace_store import db_upsert_project

                    db_upsert_project(uid, _migrate_project_dict(project.copy()))  # type: ignore[arg-type]
                except Exception as e:  # noqa: BLE001
                    log.exception("db_upsert_project: %s", e)
                    st.session_state["workspace_db_error"] = str(e)
                    return False
            add_activity(f"Updated project `{project['name']}`.")
            return True
    return False


def delete_project(project_id: str) -> bool:
    ensure_tenant_state()
    uid = _auth_uid()
    if _persist_enabled():
        try:
            from workspace_store import db_delete_project

            db_delete_project(uid, project_id)  # type: ignore[arg-type]
        except Exception as e:  # noqa: BLE001
            log.exception("db_delete_project: %s", e)
            st.session_state["workspace_db_error"] = str(e)
            return False
    for idx, project in enumerate(st.session_state[PROJECTS_KEY]):
        if project["id"] == project_id:
            removed = st.session_state[PROJECTS_KEY].pop(idx)
            add_activity(f"Deleted project `{removed['name']}`.")
            if st.session_state.get(SELECTED_PROJECT_ID_KEY) == project_id:
                st.session_state[SELECTED_PROJECT_ID_KEY] = None
            return True
    return False
