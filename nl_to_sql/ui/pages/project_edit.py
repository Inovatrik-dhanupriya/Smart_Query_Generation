"""Edit Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import (
    ensure_tenant_state,
    get_tenant_by_id,
    selected_project,
    tenants,
    update_project,
)

st.set_page_config(page_title="Edit Project", page_icon="✏️", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")

project = selected_project()
if not project:
    render_page_header("Edit Project", "No project selected.")
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

render_page_header("Edit Project", f"Update project `{project['name']}`.")

_tn_list = [t for t in tenants() if isinstance(t, dict)]
_tn_labels = [f"{t.get('name', '?')} ({t.get('code', '—')})" for t in _tn_list]
_tn_by_label = {f"{t.get('name', '?')} ({t.get('code', '—')})": t.get("id") for t in _tn_list}
_cur_tid = project.get("tenant_id") or "ten-default"
_def_label = None
for _t in _tn_list:
    if _t.get("id") == _cur_tid:
        _def_label = f"{_t.get('name', '?')} ({_t.get('code', '—')})"
        break
if not _def_label and _tn_labels:
    _def_label = _tn_labels[0]

with st.form("edit_project_form", clear_on_submit=False):
    _sel_t = st.selectbox(
        "Company (tenant)",
        options=_tn_labels,
        index=_tn_labels.index(_def_label) if _def_label in _tn_labels else 0,
    )
    name = st.text_input("Project Name", value=project["name"])
    client_code = st.text_input(
        "Client / org code (optional)",
        value=str(project.get("client_code") or ""),
    )
    description = st.text_area("Description", value=project["description"], height=120)
    status = st.selectbox("Status", options=["Draft", "Active"], index=0 if project["status"] == "Draft" else 1)
    submitted = st.form_submit_button("Save Changes", use_container_width=True)

if submitted:
    if not name.strip():
        st.error("Project name is required.")
    else:
        _tid = _tn_by_label.get(_sel_t) or (get_tenant_by_id("ten-default") or {}).get("id", "ten-default")
        update_project(
            project["id"],
            name=name,
            description=description,
            status=status,
            client_code=client_code,
            tenant_id=_tid,
        )
        st.success("Project updated successfully.")
        st.switch_page("pages/dashboard.py")
