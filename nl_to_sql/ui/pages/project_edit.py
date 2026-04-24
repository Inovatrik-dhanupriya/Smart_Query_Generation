"""Edit Project page (frontend-only)."""

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
from ui.tenant.state import (
    PROJECT_STATUS_OPTIONS,
    ensure_tenant_state,
    get_tenant_by_id,
    project_status_select_index,
    selected_project,
    tenants,
    update_project,
)

st.set_page_config(page_title="Edit Project", page_icon="✏️", layout="wide")
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
    st.page_link("pages/project_create.py", label="Databases", icon="🗄️", help=None)
    st.page_link("pages/project_open.py", label="Open project", icon="📂", help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon="💬", help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon="🔧", help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="edit_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

project = selected_project()
if not project:
    st.markdown('<div class="sqg-dash-title"><h1>Edit Project</h1></div>', unsafe_allow_html=True)
    st.markdown('<p class="sqg-dash-sub">No project selected.</p>', unsafe_allow_html=True)
    st.warning("Select a project from the dashboard first.")
    if st.button("Go to Dashboard", use_container_width=True):
        st.switch_page("pages/dashboard.py")
    st.stop()

st.markdown('<div class="sqg-dash-title"><h1>Edit Project</h1></div>', unsafe_allow_html=True)
st.markdown(
    f"<p class='sqg-dash-sub'>Update project <strong>{html.escape(project['name'])}</strong>.</p>",
    unsafe_allow_html=True,
)
st.markdown(
    """
    <div class="sqg-dash-info" role="note">
      <div class="sqg-dash-info-ico">ℹ️</div>
      <div>Update project details, company, and status (Draft → Active → Archived). Changes apply to this project only.</div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    """
    <style>
    section.main form [data-baseweb="button"][kind="primary"] p,
    section.main form [data-baseweb="button"][kind="primary"] span,
    section.main form [data-baseweb="button"][kind="primary"] div,
    section.main form [data-baseweb="button"][kind="primary"] label {
        color: #ffffff !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

_tn_list = [t for t in tenants() if isinstance(t, dict)]
_tn_labels = [str(t.get("name", "?")) for t in _tn_list]
_tn_by_label = {str(t.get("name", "?")): t.get("id") for t in _tn_list}
_cur_tid = project.get("tenant_id") or "ten-default"
_def_label = None
for _t in _tn_list:
    if _t.get("id") == _cur_tid:
        _def_label = str(_t.get("name", "?"))
        break
if not _def_label and _tn_labels:
    _def_label = _tn_labels[0]

st.markdown("<div class='sqg-dash-proj'>", unsafe_allow_html=True)
with st.form("edit_project_form", clear_on_submit=False):
    _sel_t = st.selectbox(
        "Company (tenant)",
        options=_tn_labels,
        index=_tn_labels.index(_def_label) if _def_label in _tn_labels else 0,
    )
    name = st.text_input("Project Name", value=project["name"])
    description = st.text_area("Description", value=project["description"], height=120)
    status = st.selectbox(
        "Status",
        options=list(PROJECT_STATUS_OPTIONS),
        index=project_status_select_index(project.get("status")),
        help="Draft: not live yet. Active: configure DB and use Chat. Archived: done / retired (kept, not deleted).",
    )
    submitted = st.form_submit_button("Save Changes", use_container_width=True, type="primary")
st.markdown("</div>", unsafe_allow_html=True)

if submitted:
    if not name.strip():
        st.error("Project name is required.")
    else:
        _tid = _tn_by_label.get(_sel_t) or (get_tenant_by_id("ten-default") or {}).get("id", "ten-default")
        update_project(
            project["id"],
            name=name,
            description=description,
            status=status,
            client_code="",
            tenant_id=_tid,
        )
        st.success("Project updated successfully.")
        st.switch_page("pages/dashboard.py")
