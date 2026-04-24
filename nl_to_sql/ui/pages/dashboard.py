"""
Tenant dashboard page (authenticated).
Run app from `ui/streamlit_app.py` and open `/dashboard`.
"""

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
from ui.tenant.dashboard import render_tenant_dashboard
from ui.theme import apply_dashboard_theme

st.set_page_config(page_title="Projects", page_icon="📁", layout="wide")
apply_dashboard_theme()

if not restore_auth_session():
    st.switch_page("pages/signin.py")
    st.stop()

_auth = st.session_state.auth_user or {}
_uname = str(_auth.get("username", "user") or "user")
_init = (html.escape(_uname[:1] or "?")).upper()
_display = html.escape(_uname)
_role = "Member"

with st.sidebar:
    st.markdown(
        f"""
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.35rem">
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
    st.page_link("pages/dashboard.py", label="Projects", icon="🗃️", help=None)
    st.page_link("pages/tenants.py", label="Companies", icon="🏬", help=None)
    st.page_link("pages/project_create.py", label="Databases", icon="🗄️", help=None)
    st.page_link("pages/project_open.py", label="Open project", icon="📂", help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon="💬", help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon="🔧", help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="dash_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

render_tenant_dashboard()
