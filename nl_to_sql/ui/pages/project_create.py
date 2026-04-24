"""Create Project page (frontend-only)."""

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
from ui.theme import apply_dashboard_theme
from ui.tenant.state import create_project, ensure_tenant_state, get_tenant_by_id, tenants

st.set_page_config(page_title="Create Project", page_icon="➕", layout="wide")
apply_dashboard_theme()
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
        <div class="sqg-sb-head">
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
    st.page_link("pages/project_open.py", label="Open project", icon="📂", help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon="💬", help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon="🔧", help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="create_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

st.markdown('<div class="sqg-dash-title"><h1>Create Project</h1></div>', unsafe_allow_html=True)
st.markdown(
    '<p class="sqg-dash-sub">Create a new workspace project under a company and keep chat/configuration scoped to it.</p>',
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div class="sqg-dash-info" role="note">
      <div class="sqg-dash-info-ico">ℹ️</div>
      <div>Select the target company first. Each project gets its own isolated workspace context.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

_tn_list = [t for t in tenants() if isinstance(t, dict)]
_tn_labels = [str(t.get("name", "?")) for t in _tn_list]
_tn_by_label = {str(t.get("name", "?")): t.get("id") for t in _tn_list}

st.markdown("<div class='sqg-dash-proj'>", unsafe_allow_html=True)
with st.form("create_project_form", clear_on_submit=False):
    _sel_t = st.selectbox(
        "Company (tenant)",
        options=_tn_labels,
        help="User → company → project. Add companies on the Companies page.",
    )
    name = st.text_input("Project Name", placeholder="e.g. Revenue Insights")
    description = st.text_area(
        "Description",
        placeholder="Describe the goal and expected analysis outcomes.",
        height=120,
    )
    submitted = st.form_submit_button("Create", use_container_width=True, type="primary")
st.markdown("</div>", unsafe_allow_html=True)

if submitted:
    if not name.strip():
        st.error("Project name is required.")
    else:
        _tid = _tn_by_label.get(_sel_t) or (get_tenant_by_id("ten-default") or {}).get("id", "ten-default")
        _p = create_project(
            name=name,
            description=description,
            tenant_id=_tid,
        )
        if _p is None:
            st.error(st.session_state.get("workspace_db_error") or "Could not save project to the database.")
        else:
            st.success("Project created successfully.")
            st.switch_page("pages/dashboard.py")
