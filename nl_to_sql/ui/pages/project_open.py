"""Open Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import ensure_tenant_state, selected_project

st.set_page_config(page_title="Open Project", page_icon="📂", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")
    st.page_link("streamlit_app.py", label="Go to SQL Explorer", icon="🔍")

project = selected_project()
if not project:
    render_page_header("Open Project", "No project selected.")
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

render_page_header("Open Project", f"Project: {project['name']}")
st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
st.write(f"**Description:** {project['description']}")
st.write(f"**Status:** {project['status']}")
st.write(f"**Updated:** {project['updated_at']}")
st.markdown("</div>", unsafe_allow_html=True)

if st.button("Open in SQL Explorer", use_container_width=True):
    st.switch_page("streamlit_app.py")
