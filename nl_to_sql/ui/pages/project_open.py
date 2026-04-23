"""Open Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
import requests

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.project_context import set_active_project_id
from ui.tenant.state import ensure_tenant_state, get_tenant_by_id, selected_project
from utils.config import nl_sql_api_url

st.set_page_config(page_title="Open Project", page_icon="📂", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Back to Dashboard", icon="🏠")

project = selected_project()
if not project:
    render_page_header("Open Project", "No project selected.")
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

set_active_project_id(project["id"])

render_page_header("Open Project", f"Project: {project['name']}")
st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
st.write(f"**Description:** {project['description']}")
_ct = get_tenant_by_id(project.get("tenant_id") or "")
if _ct and _ct.get("name"):
    st.write(f"**Company (tenant):** {_ct.get('name')} (`{_ct.get('code', '—')}`)")
if (project.get("client_code") or "").strip():
    st.write(f"**Label:** {project.get('client_code', '').strip()}")
st.write(f"**Status:** {project['status']}")
st.write(f"**Updated:** {project['updated_at']}")
st.markdown("</div>", unsafe_allow_html=True)
st.caption("This project has its own saved connection and chat history — other projects stay separate.")

_api_base = (nl_sql_api_url() or "").rstrip("/")
_ns = (project.get("nl_session_id") or "").strip()
_chat_unlocked: bool | None = None
if _api_base and _ns:
    try:
        _hr = requests.get(f"{_api_base}/health", params={"session_id": _ns}, timeout=4)
        if _hr.ok:
            _hd = _hr.json()
            _chat_unlocked = bool(_hd.get("activated") and _hd.get("has_tables"))
    except Exception:
        _chat_unlocked = None

_sid = _ns

a, b = st.columns(2)
if a.button("Configuration (connect data)", use_container_width=True, type="primary"):
    st.switch_page("pages/project_configuration.py")
if b.button(
    "Chat",
    use_container_width=True,
    disabled=_chat_unlocked is False,
    help="Connect and activate at least one table in Configuration first (or try again if the system could not be reached)."
    if _chat_unlocked is False
    else None,
):
    st.switch_page("pages/project_chat.py")
if _chat_unlocked is False:
    st.caption("**Chat** unlocks after you connect and activate a schema in **Configuration**.")

with st.expander("Embed in your site or app (for developers)"):
    st.caption(
        "After the schema is active, you can show the same chat in an iframe or call the service from your backend."
    )
    st.markdown("**Session ID**")
    st.code(_sid if _sid else "(open Configuration for this project first)", language=None)
    st.markdown("**Embed** — adjust the height; the API must be reachable from the page that hosts the iframe.")
    if _api_base and _sid:
        st.code(
            f'<iframe src="{_api_base}/embed?session_id={_sid}" title="Chat" width="100%" height="640" style="border:1px solid #334155;border-radius:8px"></iframe>',
            language="html",
        )
    else:
        st.code("<!-- Set your API URL in the environment, then open Configuration. -->", language="html")
    st.caption("Your integration team can use the generate-sql endpoint and set cross-origin (CORS) rules as needed.")
