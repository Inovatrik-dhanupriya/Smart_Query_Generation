"""Open Project page (frontend-only)."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st
import requests

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

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
st.caption("This project has a **dedicated** NL→SQL session (schema + chat) — other projects stay separate.")

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
if a.button("Module 1 — Configuration", use_container_width=True, type="primary"):
    st.switch_page("pages/1_Project_Configuration.py")
if b.button(
    "Module 2 — Chat",
    use_container_width=True,
    disabled=_chat_unlocked is False,
    help="Complete **Module 1** and **Activate** at least one table first (or wait for the API if it could not be checked)."
    if _chat_unlocked is False
    else None,
):
    st.switch_page("pages/2_Project_Chat.py")
if _chat_unlocked is False:
    st.caption("🔒 **Module 2** stays disabled until the schema is activated in Module 1.")

with st.expander("Reusable component — iframe & API (same session as this project)"):
    st.caption(
        "After you complete **Module 1** and activate a schema, you can embed the chat or call the API. "
        "Use the **session_id** below for `GET /embed`, `GET /embed/chat`, and `POST /generate-sql`."
    )
    st.markdown("**session_id**")
    st.code(_sid if _sid else "(open Module 1 once to provision)", language=None)
    st.markdown("**iframe** (set height as needed; API must be reachable from the browser hosting the page)")
    if _api_base and _sid:
        st.code(
            f'<iframe src="{_api_base}/embed?session_id={_sid}" title="NL to SQL" width="100%" height="640" style="border:1px solid #334155;border-radius:8px"></iframe>',
            language="html",
        )
    else:
        st.code("<!-- set NL_SQL_API_URL and open Module 1 for this project -->", language="html")
    st.markdown("**API** — `POST /generate-sql` (JSON: `prompt`, `session_id`, optional `top_k`, `row_limit`, `offset`)")
    st.caption(
        "If the parent page is on a **different origin** than the API, set `CORS_ALLOWED_ORIGINS` in `.env` "
        "or call the API from your **backend** to avoid browser CORS."
    )
