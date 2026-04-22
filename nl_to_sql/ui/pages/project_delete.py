"""Delete Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import delete_project, ensure_tenant_state, selected_project

st.set_page_config(page_title="Delete Project", page_icon="🗑️", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")

project = selected_project()
if not project:
    render_page_header("Delete Project", "No project selected.")
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

render_page_header("Delete Project", f"Remove `{project['name']}` from your projects.")
st.error("This is a frontend-only delete placeholder. Backend soft-delete can be added later.")
confirm = st.checkbox("Yes, I want to delete this project.")

left, right = st.columns(2)
if left.button("Delete Project", use_container_width=True, disabled=not confirm):
    delete_project(project["id"])
    st.success("Project deleted.")
    st.switch_page("pages/dashboard.py")
if right.button("Cancel", use_container_width=True):
    st.switch_page("pages/dashboard.py")
