"""Open Project page (frontend-only)."""

from __future__ import annotations

import html
import sys
from pathlib import Path

import streamlit as st
import requests

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.auth.session import clear_auth_session, restore_auth_session
from ui.sidebar_icons import (
    SIDEBAR_CHAT,
    SIDEBAR_COMPANIES,
    SIDEBAR_CONFIGURATION,
    SIDEBAR_OPEN_PROJECT,
    SIDEBAR_PROJECTS,
)
from ui.theme import apply_dashboard_theme, apply_tenant_page_shell
from ui.tenant.project_context import set_active_project_id
from ui.tenant.state import ensure_tenant_state, get_tenant_by_id, selected_project
from utils.config import nl_sql_api_url

st.set_page_config(page_title="Open Project", page_icon="📂", layout="wide")
apply_dashboard_theme()
apply_tenant_page_shell()
ensure_tenant_state()

if not restore_auth_session():
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    _auth = st.session_state.get("auth_user") or {}
    _uname = str(_auth.get("username", "user") or "user")
    _init = (html.escape(_uname[:1] or "?")).upper()
    _display = html.escape(_uname)
    _role = "Member"
    st.markdown(
        f"""
        <div class="sqg-sb-top" style="display:flex;align-items:center;gap:0.5rem">
          <span style="display:flex;width:30px;height:30px;border-radius:8px;background:#5b21b6;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.12)">
            <span style="display:flex;flex-direction:column;gap:2px;align-items:flex-start;justify-content:center">
              <span style="height:2px;width:12px;background:#fff;border-radius:1px"></span>
              <span style="height:2px;width:8px;background:#fff;border-radius:1px;opacity:0.95"></span>
              <span style="height:2px;width:10px;background:#fff;border-radius:1px;opacity:0.9"></span>
            </span>
          </span>
          <span class="sqg-sb-brand" style="margin:0">Smart Query</span>
        </div>
        <div class="sqg-sb-user">
          <div class="sqg-sb-av">{_init}</div>
          <div>
            <div class="sqg-sb-name">{_display}</div>
            <div class="sqg-sb-role">{_role}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("WORKSPACE")
    st.page_link("pages/dashboard.py", label="Projects", icon=SIDEBAR_PROJECTS, help=None)
    st.page_link("pages/tenants.py", label="Companies", icon=SIDEBAR_COMPANIES, help=None)
    st.page_link("pages/project_open.py", label="Open project", icon=SIDEBAR_OPEN_PROJECT, help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon=SIDEBAR_CHAT, help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon=SIDEBAR_CONFIGURATION, help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="open_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

project = selected_project()
if not project:
    st.markdown('<div class="sqg-dash-title"><h1>Open Project</h1></div>', unsafe_allow_html=True)
    st.markdown('<p class="sqg-dash-sub">No project selected.</p>', unsafe_allow_html=True)
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

set_active_project_id(project["id"])

st.markdown('<div class="sqg-dash-title"><h1>Open Project</h1></div>', unsafe_allow_html=True)
st.markdown(
    "<p class='sqg-dash-sub'>Project name, company, and description.</p>",
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div class="sqg-dash-info" role="note">
      <div class="sqg-dash-info-ico" aria-hidden="true">i</div>
      <div>Use Configuration to connect and activate data. Chat is enabled once schema activation is complete.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
_ct = get_tenant_by_id(project.get("tenant_id") or "")
_pname = html.escape(str(project.get("name") or "—"))
_desc = html.escape((str(project.get("description") or "")).strip() or "—")
if _ct and (str(_ct.get("name") or "")).strip():
    _cname = html.escape((str(_ct.get("name") or "")).strip())
else:
    _cname = "—"
st.markdown(
    f"""
    <div class="sqg-dash-proj" role="region" aria-label="Project details">
      <dl class="sqg-kv">
        <dt>Project name</dt>
        <dd>{_pname}</dd>
        <dt>Company</dt>
        <dd>{_cname}</dd>
        <dt>Description</dt>
        <dd>{_desc}</dd>
      </dl>
    </div>
    """,
    unsafe_allow_html=True,
)
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
