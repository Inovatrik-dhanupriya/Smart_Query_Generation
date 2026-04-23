"""Frontend-only tenant dashboard components."""

from __future__ import annotations

import html
from datetime import datetime

import streamlit as st

from ui.tenant.state import (
    ensure_tenant_state,
    get_tenant_by_id,
    projects_for_tenant,
    set_selected_project,
    tenants,
)


def _filter_projects(my_projects: list[dict], mode: str) -> list[dict]:
    m = (mode or "all").strip().lower()
    if m == "all":
        return my_projects
    if m == "active":
        return [p for p in my_projects if (p.get("status") or "").lower() == "active"]
    if m == "archived":
        return [
            p
            for p in my_projects
            if (p.get("status") or "").lower() in ("archived", "closed", "inactive")
        ]
    return my_projects


def _metric_block(title: str, value: str, hint: str, icon: str, icon_class: str) -> None:
    st.markdown(
        f"""
<div class="sqg-dash-metric">
  <div class="sqg-dash-metric-body">
    <p class="sqg-dmi-title">{html.escape(title)}</p>
    <p class="sqg-dmi-val">{html.escape(value)}</p>
    <p class="sqg-dmi-hint">{html.escape(hint)}</p>
  </div>
  <div class="sqg-dash-metric-ico {icon_class}" aria-hidden="true">{icon}</div>
</div>
""",
        unsafe_allow_html=True,
    )


def _render_project_row(project: dict, idx: int) -> None:
    st.markdown("<div class='sqg-dash-proj'>", unsafe_allow_html=True)
    st.markdown(f"**{project['name']}**")
    _t = get_tenant_by_id(project.get("tenant_id") or "") or {}
    if _t.get("name"):
        st.caption(f"Company: **`{_t.get('name')}`**")
    if (project.get("client_code") or "").strip():
        st.caption(f"Label: `{project['client_code'].strip()}`")
    st.caption(project.get("description") or "—")
    meta_a, meta_b = st.columns(2)
    meta_a.caption(f"Status: `{project.get('status', '—')}`")
    meta_b.caption(f"Updated: `{project.get('updated_at', '—')}`")

    a, b, c = st.columns(3)
    if a.button("Open", key=f"open_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_open.py")
    if b.button("Edit", key=f"edit_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_edit.py")
    if c.button("Delete", key=f"delete_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_delete.py")
    st.markdown("</div>", unsafe_allow_html=True)


def _empty_state() -> None:
    st.markdown(
        """
<div class="sqg-dash-empty">
  <div class="sqg-dash-empty-icowrap"><span class="sqg-dash-empty-ico" aria-hidden="true">📁➕</span></div>
  <h3>No projects yet</h3>
  <p>Create a project, connect your database, then use <b>Chat</b> to turn plain English
  into SQL and results.</p>
</div>
""",
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="sqg-dash-empty-cta-gutter" aria-hidden="true"></div>',
        unsafe_allow_html=True,
    )
    if st.button(
        "➕ Create your first project",
        use_container_width=True,
        type="primary",
        key="dash_empty_cta",
    ):
        st.switch_page("pages/project_create.py")


def render_tenant_dashboard() -> None:
    ensure_tenant_state()

    dbe = st.session_state.pop("workspace_db_error", None)
    if dbe:
        st.error(f"Could not load or save projects: {dbe}")

    auth = st.session_state.get("auth_user") or {}
    username = str(auth.get("username", "user") or "user")
    tlist = [t for t in tenants() if isinstance(t, dict)]
    t_labels = ["All companies"] + [str(t.get("name", "?")) for t in tlist]
    t_map: dict[str, str] = {"All companies": "__all__"}
    for t in tlist:
        t_map[str(t.get("name", "?"))] = t.get("id") or ""

    c_title, c_filter, c_companies, c_new = st.columns(
        [0.75, 0.9, 0.38, 0.52],
        gap="large",
        vertical_alignment="center",
    )
    with c_title:
        st.markdown(
            '<div class="sqg-dash-title"><h1>Projects</h1></div>',
            unsafe_allow_html=True,
        )
    with c_filter:
        selected_label = st.selectbox(
            "show_projects_for",
            options=t_labels,
            key="dashboard_tenant_filter_widget",
            help="Filter projects by company, or all companies in your account.",
            label_visibility="collapsed",
        )
    with c_companies:
        st.page_link("pages/tenants.py", label="Companies", icon="🏬")
    with c_new:
        if st.button(
            "➕ New project",
            type="primary",
            use_container_width=False,
            key="dash_new_proj",
        ):
            st.switch_page("pages/project_create.py")

    st.markdown(
        f"""
<p class="sqg-dash-sub">Hi {html.escape(username)} — open a project to connect data and ask questions.</p>
""",
        unsafe_allow_html=True,
    )

    ftid = t_map.get(selected_label, "__all__")
    my_projects = projects_for_tenant(ftid)

    st.markdown(
        """
<div class="sqg-dash-info" role="note">
  <div class="sqg-dash-info-ico">ℹ️</div>
  <div><strong>How it works</strong> — choose a <span class="sqg-info-kw">company</span> (if you use more
  than one), then open a <span class="sqg-info-kw">project</span>. Each project has its own
  <span class="sqg-info-kw">data setup</span> and <span class="sqg-info-kw">chat</span>
  so answers stay in context.</div>
</div>
""",
        unsafe_allow_html=True,
    )

    c1, c2, c3 = st.columns(3)
    c1_metric = str(len(my_projects))
    c2_metric = str(sum(1 for p in my_projects if (p.get("status") or "").lower() == "active"))
    c3_metric = datetime.now().strftime("%d %b %Y")
    with c1:
        _metric_block(
            "Total projects",
            c1_metric,
            "All projects in your tenant",
            "▣",
            "sqg-dmi-grid",
        )
    with c2:
        _metric_block("Active workspaces", c2_metric, "Currently active", "▶", "sqg-dmi-pulse")
    with c3:
        _metric_block("Snapshot date", c3_metric, "Dashboard snapshot", "▦", "sqg-dmi-cal")

    st.markdown("---")
    if "dashboard_project_view_tab" in st.session_state:
        _r = st.session_state.pop("dashboard_project_view_tab", "All")
        st.session_state["dashboard_proj_mode"] = {
            "All": "all",
            "Active": "active",
            "Archived": "archived",
        }.get(_r if isinstance(_r, str) else str(_r), "all")
    st.session_state.setdefault("dashboard_proj_mode", "all")

    pm = st.session_state.dashboard_proj_mode
    h_left, b_all, b_act, b_arch = st.columns(
        [1.25, 0.16, 0.2, 0.24],
        gap="small",
        vertical_alignment="center",
    )
    with h_left:
        st.markdown(
            '<p class="sqg-dash-sec sqg-dash-sec--row">My projects</p>',
            unsafe_allow_html=True,
        )
    with b_all:
        if st.button(
            "All",
            type="primary" if pm == "all" else "secondary",
            use_container_width=True,
            key="dpm_all",
        ):
            st.session_state.dashboard_proj_mode = "all"
    with b_act:
        if st.button(
            "Active",
            type="primary" if pm == "active" else "secondary",
            use_container_width=True,
            key="dpm_active",
        ):
            st.session_state.dashboard_proj_mode = "active"
    with b_arch:
        if st.button(
            "Archived",
            type="primary" if pm == "archived" else "secondary",
            use_container_width=True,
            key="dpm_arch",
        ):
            st.session_state.dashboard_proj_mode = "archived"

    mode = st.session_state.dashboard_proj_mode
    filtered = _filter_projects(my_projects, mode)

    if not filtered:
        if not my_projects:
            _empty_state()
        else:
            st.info("No projects match this filter. Try a different view or company selection.")
    else:
        for idx, project in enumerate(filtered):
            _render_project_row(project, idx)
