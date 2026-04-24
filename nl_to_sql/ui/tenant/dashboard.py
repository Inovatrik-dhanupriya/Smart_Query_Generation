"""Frontend-only tenant dashboard components."""

from __future__ import annotations

import html
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as st_components

from ui.sidebar_icons import SIDEBAR_COMPANIES
from ui.tenant.state import (
    delete_project,
    ensure_tenant_state,
    get_tenant_by_id,
    projects_for_tenant,
    set_selected_project,
    tenants,
)


def _dashboard_set_dpm(mode: str) -> None:
    """on_click: updates filter mode before the fragment re-runs (no st.rerun)."""
    m = (mode or "").strip().lower()
    st.session_state["dashboard_proj_mode"] = m if m in ("all", "active", "archived") else "all"


def _filter_main_buttons_type_button() -> None:
    """Set type=button on any submit-styled main-area buttons (Streamlit/embedded forms; no navigation)."""
    st_components.html(
        """
        <script>
        (function () {
          const d = (window.parent && window.parent.document) || document;
          d.querySelectorAll('section.main button[type="submit"]').forEach(
            (b) => b.setAttribute('type', 'button')
          );
        })();
        </script>
        """,
        height=0,
        width=0,
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


# 6-col grid: wider Project + Actions, slightly narrower Description; same for header and rows
_PROJ_TBL_COLS = (2.6, 1.45, 2.5, 1.0, 1.35, 2.1)
_TBL_GAP = "small"  # must match in header + rows for column alignment


def _desc_preview(desc: str, max_len: int = 120) -> str:
    s = (desc or "—").strip() or "—"
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def _status_badge_html(status: str) -> str:
    raw = str(status or "—")
    display = raw.strip().capitalize() if raw.strip() else "—"
    s = raw.strip().lower()
    if s == "active":
        stl = "color:#14532d !important;background:#d1fae5 !important;border:1px solid #6ee7b7 !important;"
    elif s == "draft":
        stl = (
            "color:#4c1d95 !important;background:linear-gradient(180deg,#fffbeb,#fef3c7) !important;"
            "box-shadow:0 0 0 1px rgba(124,58,237,0.28) !important;border:1px solid rgba(253,230,138,0.7) !important;"
        )
    elif s in frozenset({"archived", "completed", "closed", "inactive"}):
        stl = "color:#44403c !important;background:#e7e5e4 !important;border:1px solid #c4bfbc !important;"
    else:
        stl = "color:#581c87 !important;background:#ede9fe !important;border:1px solid #c4b5fd !important;"
    return (
        f'<span class="sqg-dash-status-badge sqg-proj-tbl-badge" style="margin:0 !important;{stl}">'
        f"{html.escape(display)}</span>"
    )


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


def _render_projects_table_header() -> None:
    c0, c1, c2, c3, c4, c5 = st.columns(
        _PROJ_TBL_COLS, gap=_TBL_GAP, vertical_alignment="center"
    )
    with c0:
        st.markdown(
            '<p class="sqg-proj-th">Project</p>',
            unsafe_allow_html=True,
        )
    with c1:
        st.markdown(
            '<p class="sqg-proj-th">Company</p>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            '<p class="sqg-proj-th">Description</p>',
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            '<p class="sqg-proj-th">Status</p>',
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            '<p class="sqg-proj-th">Updated</p>',
            unsafe_allow_html=True,
        )
    with c5:
        st.markdown(
            '<div class="sqg-proj-th-action-wrap"><p class="sqg-proj-th sqg-proj-th--action">Action</p></div>',
            unsafe_allow_html=True,
        )


def _render_project_table_row(project: dict, idx: int) -> None:
    _t = get_tenant_by_id(project.get("tenant_id") or "") or {}
    _cn = str(_t.get("name") or "—")
    _status = str(project.get("status", "—"))
    desc = str(project.get("description") or "—")
    desc_show = _desc_preview(desc, 120)
    row_class = "sqg-proj-td-name--sep" if idx > 0 else ""

    c0, c1, c2, c3, c4, c5 = st.columns(
        _PROJ_TBL_COLS, gap=_TBL_GAP, vertical_alignment="center"
    )
    with c0:
        st.markdown(
            f'<p class="sqg-proj-td-name {row_class}">{html.escape(project["name"])}</p>',
            unsafe_allow_html=True,
        )
    with c1:
        st.markdown(
            f'<p class="sqg-proj-td sqg-proj-td--company">{html.escape(_cn)}</p>',
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f'<p class="sqg-proj-td sqg-proj-td--desc" title="{html.escape(desc)}">'
            f"{html.escape(desc_show)}</p>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f'<p class="sqg-proj-td sqg-proj-td--status"><span class="sqg-proj-td-statuscell">'
            f"{_status_badge_html(_status)}</span></p>",
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            f'<p class="sqg-proj-td sqg-proj-td-meta">'
            f"{html.escape(str(project.get('updated_at', '—')))}</p>",
            unsafe_allow_html=True,
        )
    with c5:
        b_open, b_edit, b_delete = st.columns(3, gap="medium", vertical_alignment="center")
        with b_open:
            if st.button("Open", key=f"open_{idx}", use_container_width=True):
                set_selected_project(project["id"])
                st.switch_page("pages/project_open.py")
        with b_edit:
            if st.button("Edit", key=f"edit_{idx}", use_container_width=True):
                set_selected_project(project["id"])
                st.switch_page("pages/project_edit.py")
        with b_delete:
            if st.button("Delete", key=f"delete_{idx}", use_container_width=True):
                st.session_state["dashboard_delete_project_id"] = project["id"]
                st.session_state["dashboard_delete_project_name"] = project.get("name") or "this project"
                st.rerun()  # full run so the dialog in render_tenant_dashboard (outside the fragment) opens


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


@st.fragment
def _render_dashboard_projects_block(my_projects: list[dict]) -> None:
    """Filter row + table + delete dialog. Fragment re-runs on these interactions only (not full page)."""
    st.markdown("---")
    if "dashboard_project_view_tab" in st.session_state:
        _r = st.session_state.pop("dashboard_project_view_tab", "All")
        st.session_state["dashboard_proj_mode"] = {
            "All": "all",
            "Active": "active",
            "Archived": "archived",
        }.get(_r if isinstance(_r, str) else str(_r), "all")
    st.session_state.setdefault("dashboard_proj_mode", "all")

    _filter_main_buttons_type_button()

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
        st.button(
            "All",
            type="primary" if pm == "all" else "secondary",
            use_container_width=True,
            key="dpm_all",
            on_click=_dashboard_set_dpm,
            args=("all",),
        )
    with b_act:
        st.button(
            "Active",
            type="primary" if pm == "active" else "secondary",
            use_container_width=True,
            key="dpm_active",
            on_click=_dashboard_set_dpm,
            args=("active",),
        )
    with b_arch:
        st.button(
            "Archived",
            type="primary" if pm == "archived" else "secondary",
            use_container_width=True,
            key="dpm_arch",
            on_click=_dashboard_set_dpm,
            args=("archived",),
        )

    mode = st.session_state.dashboard_proj_mode
    filtered = _filter_projects(my_projects, mode)

    if not filtered:
        if not my_projects:
            _empty_state()
        else:
            st.info("No projects match this filter. Try a different view or company selection.")
    else:
        _render_projects_table_header()
        for idx, project in enumerate(filtered):
            _render_project_table_row(project, idx)


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
        st.page_link("pages/tenants.py", label="Companies", icon=SIDEBAR_COMPANIES)
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

    _render_dashboard_projects_block(my_projects)

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
                if st.button("Cancel", use_container_width=True, key="dash_delete_cancel", type="secondary"):
                    st.session_state["dashboard_delete_project_id"] = None
                    st.session_state["dashboard_delete_project_name"] = ""
                    st.rerun()
            with right:
                if st.button("Delete project", type="primary", use_container_width=True, key="dash_delete_confirm"):
                    ok = delete_project(str(delete_id))
                    st.session_state["dashboard_delete_project_id"] = None
                    st.session_state["dashboard_delete_project_name"] = ""
                    if ok:
                        st.toast("Project deleted.")
                    else:
                        st.toast("Could not delete project. Please try again.")
                    st.rerun()

        _confirm_delete_dialog()
