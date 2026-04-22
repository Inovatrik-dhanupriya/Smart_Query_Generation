"""Create Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import create_project, ensure_tenant_state

st.set_page_config(page_title="Create Project", page_icon="➕", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")

render_page_header("Create Project", "Create a new workspace project.")

with st.form("create_project_form", clear_on_submit=False):
    name = st.text_input("Project Name", placeholder="e.g. Revenue Insights")
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
        create_project(name=name, description=description)
        st.success("Project created successfully.")
        st.switch_page("pages/dashboard.py")
