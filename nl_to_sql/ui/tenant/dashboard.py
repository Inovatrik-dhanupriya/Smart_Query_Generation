"""Frontend-only tenant dashboard components."""

from __future__ import annotations

from datetime import datetime

import streamlit as st

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import (
    ACTIVITY_KEY,
    ensure_tenant_state,
    projects,
    set_selected_project,
)


def _card(title: str, value: str, help_text: str = "") -> None:
    st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
    st.caption(title)
    st.subheader(value)
    if help_text:
        st.caption(help_text)
    st.markdown("</div>", unsafe_allow_html=True)


def _render_project_row(project: dict, idx: int) -> None:
    st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
    st.markdown(f"**{project['name']}**")
    st.caption(project["description"])
    meta_a, meta_b = st.columns(2)
    meta_a.caption(f"Status: `{project['status']}`")
    meta_b.caption(f"Updated: `{project['updated_at']}`")

    a, b, c = st.columns(3)
    if a.button("Open Project", key=f"open_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_open.py")
    if b.button("Edit", key=f"edit_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_edit.py")
    if c.button("Delete", key=f"delete_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_delete.py")
    st.markdown("</div>", unsafe_allow_html=True)


def render_tenant_dashboard() -> None:
    apply_shared_theme()
    ensure_tenant_state()

    auth = st.session_state.get("auth_user") or {}
    username = auth.get("username", "user")
    my_projects = projects()
    activity = st.session_state[ACTIVITY_KEY]

    render_page_header(
        "Tenant Dashboard",
        f"Welcome, {username}. Manage projects and continue your workspace flow.",
    )
    st.markdown(
        """
<div class="nl-banner">
  <h3>Workspace Overview</h3>
  <p>Use this dashboard to create, open, edit, and delete projects before entering NL-to-SQL explorer.</p>
</div>
""",
        unsafe_allow_html=True,
    )

    st.divider()
    st.markdown("### Welcome")
    st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
    st.write(
        "This is your tenant home. Use quick actions below to create or open projects, "
        "then jump into NL-to-SQL exploration."
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown("### Project Summary")
    c1, c2, c3 = st.columns(3)
    c1_metric = str(len(my_projects))
    c2_metric = str(sum(1 for p in my_projects if p["status"].lower() == "active"))
    c3_metric = datetime.now().strftime("%d %b %Y")
    with c1:
        _card("Total Projects", c1_metric, "All projects in your tenant.")
    with c2:
        _card("Active Projects", c2_metric, "Currently active workspaces.")
    with c3:
        _card("Today", c3_metric, "Dashboard snapshot date.")

    st.markdown("### My Projects")
    top_left, _ = st.columns([1, 4])
    if top_left.button("Create Project", use_container_width=True):
        st.switch_page("pages/project_create.py")
    if not my_projects:
        st.info("No projects yet. Click Create Project to get started.")
    for idx, project in enumerate(my_projects):
        _render_project_row(project, idx)

    st.markdown("### Recent Activity")
    st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
    for item in activity:
        st.write(f"- {item}")
    st.caption("Frontend-only placeholder. Connect backend activity feed later.")
    st.markdown("</div>", unsafe_allow_html=True)
