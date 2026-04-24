"""Frontend-only tenant dashboard components."""

from __future__ import annotations

import html
from datetime import datetime

import streamlit as st

from ui.tenant.state import (
    delete_project,
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
        _end = frozenset({"archived", "completed", "closed", "inactive"})
        return [p for p in my_projects if (p.get("status") or "").lower() in _end]
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
    st.markdown(
        f'<p class="sqg-dash-proj-title" style="font-size:1.58rem;font-weight:900;line-height:1.2;letter-spacing:-0.01em;margin:0 0 0.55rem 0;color:#1e1b2e;">{html.escape(project["name"])}</p>',
        unsafe_allow_html=True,
    )
    _t = get_tenant_by_id(project.get("tenant_id") or "") or {}
    if _t.get("name"):
        _cn = html.escape(str(_t.get("name") or ""))
        st.markdown(
            f'<p class="sqg-dash-proj-line"><span class="sqg-dash-proj-label" style="font-size:0.78rem;color:#9ca3af;font-weight:600;text-transform:uppercase;letter-spacing:0.02em;">Company:</span> <span class="sqg-dash-proj-value" style="font-size:0.96rem;color:#312e81;font-weight:700;">{_cn}</span></p>',
            unsafe_allow_html=True,
        )
    st.markdown(
        f'<p class="sqg-dash-proj-line sqg-dash-proj-desc" style="font-weight:700;">{html.escape(str(project.get("description") or "—"))}</p>',
        unsafe_allow_html=True,
    )
    meta_a, meta_b = st.columns(2)
    with meta_a:
        _status = str(project.get("status", "—"))
        _status_display = _status.strip().capitalize() if _status.strip() else "—"
        st.markdown(
            f'<p class="sqg-dash-proj-line"><span class="sqg-dash-proj-label sqg-dash-proj-label--subtle">Status</span> <span class="sqg-dash-status-badge" style="margin-left:0.6rem;font-weight:700;">{html.escape(_status_display)}</span></p>',
            unsafe_allow_html=True,
        )
    with meta_b:
        st.markdown(
            f'<p class="sqg-dash-proj-line sqg-dash-proj-line--right"><span class="sqg-dash-proj-label">Updated:</span> <span class="sqg-dash-proj-meta">{html.escape(str(project.get("updated_at", "—")))}</span></p>',
            unsafe_allow_html=True,
        )

    a, b, c = st.columns(3)
    if a.button("Open", key=f"open_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_open.py")
    if b.button("Edit", key=f"edit_{idx}", use_container_width=True):
        set_selected_project(project["id"])
        st.switch_page("pages/project_edit.py")
    if c.button("Delete", key=f"delete_{idx}", use_container_width=True):
        st.session_state["dashboard_delete_project_id"] = project["id"]
        st.session_state["dashboard_delete_project_name"] = project.get("name") or "this project"
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
    st.session_state.setdefault("dashboard_delete_project_id", None)
    st.session_state.setdefault("dashboard_delete_project_name", "")

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

    delete_id = st.session_state.get("dashboard_delete_project_id")
    if delete_id:
        delete_name = str(st.session_state.get("dashboard_delete_project_name") or "this project")

        @st.dialog("Confirm delete")
        def _confirm_delete_dialog() -> None:
            st.warning(
                f"Delete **{html.escape(delete_name)}**? This action cannot be undone."
            )
            left, right = st.columns(2)
            with left:
                if st.button("Cancel", use_container_width=True, key="dash_delete_cancel"):
                    st.session_state["dashboard_delete_project_id"] = None
                    st.session_state["dashboard_delete_project_name"] = ""
            with right:
                if st.button("Delete project", type="primary", use_container_width=True, key="dash_delete_confirm"):
                    ok = delete_project(str(delete_id))
                    st.session_state["dashboard_delete_project_id"] = None
                    st.session_state["dashboard_delete_project_name"] = ""
                    if ok:
                        st.success("Project deleted.")
                    else:
                        st.error("Could not delete project. Please try again.")

        _confirm_delete_dialog()
