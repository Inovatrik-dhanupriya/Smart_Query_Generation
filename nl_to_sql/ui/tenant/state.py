"""State helpers for tenant dashboard/project pages (frontend-only)."""

from __future__ import annotations

from datetime import datetime
import uuid

import streamlit as st

PROJECTS_KEY = "tenant_projects"
ACTIVITY_KEY = "tenant_recent_activity"
SELECTED_PROJECT_ID_KEY = "tenant_selected_project_id"


def ensure_tenant_state() -> None:
    if PROJECTS_KEY not in st.session_state:
        st.session_state[PROJECTS_KEY] = [
            {
                "id": "proj-001",
                "name": "Sales Insights",
                "description": "Analyze sales trends and top-performing products.",
                "status": "Active",
                "updated_at": "2 hours ago",
            },
            {
                "id": "proj-002",
                "name": "Clinic Operations",
                "description": "Daily clinic metrics and operational summaries.",
                "status": "Draft",
                "updated_at": "Yesterday",
            },
        ]
    if ACTIVITY_KEY not in st.session_state:
        st.session_state[ACTIVITY_KEY] = [
            "Signed in to tenant dashboard.",
            "Viewed project summaries.",
            "Opened project list.",
        ]
    if SELECTED_PROJECT_ID_KEY not in st.session_state:
        st.session_state[SELECTED_PROJECT_ID_KEY] = None


def add_activity(text: str) -> None:
    ensure_tenant_state()
    ts = datetime.now().strftime("%H:%M")
    st.session_state[ACTIVITY_KEY].insert(0, f"[{ts}] {text}")
    st.session_state[ACTIVITY_KEY] = st.session_state[ACTIVITY_KEY][:15]


def projects() -> list[dict]:
    ensure_tenant_state()
    return st.session_state[PROJECTS_KEY]


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
            return p
    return None


def create_project(name: str, description: str) -> dict:
    ensure_tenant_state()
    project = {
        "id": f"proj-{uuid.uuid4().hex[:6]}",
        "name": name.strip(),
        "description": description.strip() or "No description.",
        "status": "Draft",
        "updated_at": "just now",
    }
    st.session_state[PROJECTS_KEY].insert(0, project)
    set_selected_project(project["id"])
    add_activity(f"Created project `{project['name']}`.")
    return project


def update_project(project_id: str, *, name: str, description: str, status: str) -> bool:
    ensure_tenant_state()
    for project in st.session_state[PROJECTS_KEY]:
        if project["id"] == project_id:
            project["name"] = name.strip()
            project["description"] = description.strip() or "No description."
            project["status"] = status
            project["updated_at"] = "just now"
            add_activity(f"Updated project `{project['name']}`.")
            return True
    return False


def delete_project(project_id: str) -> bool:
    ensure_tenant_state()
    for idx, project in enumerate(st.session_state[PROJECTS_KEY]):
        if project["id"] == project_id:
            removed = st.session_state[PROJECTS_KEY].pop(idx)
            add_activity(f"Deleted project `{removed['name']}`.")
            if st.session_state.get(SELECTED_PROJECT_ID_KEY) == project_id:
                st.session_state[SELECTED_PROJECT_ID_KEY] = None
            return True
    return False
