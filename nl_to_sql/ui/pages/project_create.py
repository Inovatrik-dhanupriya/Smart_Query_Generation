"""Create Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import create_project, ensure_tenant_state, get_tenant_by_id, tenants

st.set_page_config(page_title="Create Project", page_icon="➕", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")

render_page_header("Create Project", "Create a new workspace project.")

_tn_list = [t for t in tenants() if isinstance(t, dict)]
_tn_labels = [f"{t.get('name', '?')} ({t.get('code', '—')})" for t in _tn_list]
_tn_by_label = {f"{t.get('name', '?')} ({t.get('code', '—')})": t.get("id") for t in _tn_list}

with st.form("create_project_form", clear_on_submit=False):
    _sel_t = st.selectbox(
        "Company (tenant)",
        options=_tn_labels,
        help="User → company → project. Add companies on the **Companies (tenants)** page.",
    )
    name = st.text_input("Project Name", placeholder="e.g. Revenue Insights")
    client_code = st.text_input(
        "Client / org code (optional)",
        placeholder="e.g. COC, BMS — label for this workspace",
    )
    description = st.text_area(
        "Description",
        placeholder="Describe the goal and expected analysis outcomes.",
        height=120,
    )
    submitted = st.form_submit_button("Create", use_container_width=True)

if submitted:
    if not name.strip():
        st.error("Project name is required.")
    else:
        _tid = _tn_by_label.get(_sel_t) or (get_tenant_by_id("ten-default") or {}).get("id", "ten-default")
        _p = create_project(
            name=name,
            description=description,
            tenant_id=_tid,
            client_code=client_code,
        )
        if _p is None:
            st.error(st.session_state.get("workspace_db_error") or "Could not save project to the database.")
        else:
            st.success("Project created successfully.")
            st.switch_page("pages/dashboard.py")
