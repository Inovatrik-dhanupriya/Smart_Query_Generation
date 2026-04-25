"""Companies (tenants) — User → Tenant (company) → Project hierarchy."""

from __future__ import annotations

import html
import sys
from pathlib import Path

import streamlit as st

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
from ui.tenant.state import (
    create_tenant,
    ensure_tenant_state,
)

st.set_page_config(page_title="Companies", page_icon="🏬", layout="wide")
apply_dashboard_theme()
apply_tenant_page_shell()
ensure_tenant_state()

if not restore_auth_session():
    st.switch_page("pages/signin.py")
    st.stop()

_auth = st.session_state.get("auth_user") or {}
_uname = str(_auth.get("username", "user") or "user")
_init = (html.escape(_uname[:1] or "?")).upper()
_display = html.escape(_uname)
_role = "Member"

with st.sidebar:
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
    if st.button("Sign out", use_container_width=True, type="secondary", key="tenants_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

st.markdown('<div class="sqg-dash-title"><h1>Companies</h1></div>', unsafe_allow_html=True)
st.markdown(
    '<p class="sqg-dash-sub">Manage organizations for your projects. Each project stays scoped to one company.</p>',
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div class="sqg-dash-info" role="note">
      <div class="sqg-dash-info-ico" aria-hidden="true">i</div>
      <div>Use companies to keep project organization clean. On the Projects page, filter workspaces by company.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.form("new_tenant"):
    t_name = st.text_input("Company name", placeholder="e.g. Acme Corp")
    add = st.form_submit_button("Add company", use_container_width=True, type="primary")

if add:
    if not (t_name or "").strip():
        st.error("Company name is required.")
    else:
        t = create_tenant(name=t_name)
        if t is None:
            st.error(st.session_state.get("workspace_db_error") or "Could not save to the database.")
        else:
            st.success("Company added.")
            st.switch_page("pages/dashboard.py")
