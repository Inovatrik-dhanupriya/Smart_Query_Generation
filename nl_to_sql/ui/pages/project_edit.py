"""Edit Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import ensure_tenant_state, selected_project, update_project

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

with st.form("edit_project_form", clear_on_submit=False):
    name = st.text_input("Project Name", value=project["name"])
    description = st.text_area("Description", value=project["description"], height=120)
    status = st.selectbox("Status", options=["Draft", "Active"], index=0 if project["status"] == "Draft" else 1)
    submitted = st.form_submit_button("Save Changes", use_container_width=True)

if submitted:
    if not name.strip():
        st.error("Project name is required.")
    else:
        update_project(project["id"], name=name, description=description, status=status)
        st.success("Project updated successfully.")
        st.switch_page("pages/dashboard.py")
