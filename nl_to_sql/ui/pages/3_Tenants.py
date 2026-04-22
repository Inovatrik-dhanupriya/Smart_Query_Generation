"""Companies (tenants) — User → Tenant (company) → Project hierarchy."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import (
    DEFAULT_TENANT_ID,
    create_tenant,
    delete_tenant,
    ensure_tenant_state,
    get_tenant_by_id,
    tenants,
)

st.set_page_config(page_title="Companies (tenants)", page_icon="🏬", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Dashboard", icon="🏠")

render_page_header(
    "Companies (tenants)",
    "Each company is a separate tenant. Projects belong to one company; DB connection and schema are configured per project.",
)

st.info(
    "**Structure:** User (sign-in) → **Company** (this page) → **Project** (dashboard) → **Configuration** "
    "(database + schema) → **Chat**. Separate companies help separate client environments in the UI."
)

with st.form("new_tenant"):
    c1, c2 = st.columns(2)
    with c1:
        t_name = st.text_input("Company name", placeholder="e.g. Acme Corp")
    with c2:
        t_code = st.text_input("Short code", placeholder="e.g. ACME")
    add = st.form_submit_button("Add company", use_container_width=True)

if add:
    if not (t_name or "").strip():
        st.error("Company name is required.")
    else:
        t = create_tenant(name=t_name, code=t_code)
        if t is None:
            st.error(st.session_state.get("workspace_db_error") or "Could not save to the database.")
        else:
            st.success("Company added.")
            st.rerun()

st.divider()
st.subheader("Existing companies")

for t in tenants():
    if not isinstance(t, dict):
        continue
    with st.container():
        st.markdown(f"**{t.get('name', '—')}** · code `{t.get('code', '—')}` · `{t.get('id', '')}`")
        if t.get("id") != DEFAULT_TENANT_ID:
            if st.button("Delete", key=f"del_{t.get('id')}", type="secondary"):
                if delete_tenant(t.get("id") or ""):
                    st.rerun()
                else:
                    st.warning("Cannot delete: remove or move projects that use this company first, or it is the default company.")
        st.divider()
