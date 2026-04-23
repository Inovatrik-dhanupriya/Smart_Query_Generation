"""Companies (tenants) — User → Tenant (company) → Project hierarchy."""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.theme import apply_shared_theme, render_page_header
from ui.tenant.state import (
    DEFAULT_TENANT_ID,
    create_tenant,
    delete_tenant,
    ensure_tenant_state,
    get_tenant_by_id,
    tenants,
)

st.set_page_config(page_title="Companies", page_icon="🏬", layout="wide")
apply_shared_theme()
ensure_tenant_state()

if not st.session_state.get("auth_user"):
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    st.page_link("pages/dashboard.py", label="Dashboard", icon="🏠")

render_page_header(
    "Companies",
    "Add organizations you work with. Each project belongs to one company so data and access stay separate.",
)

st.info(
    "On the main screen, filter projects by company. You’ll still sign in with **your** one account."
)

with st.form("new_tenant"):
    t_name = st.text_input("Company name", placeholder="e.g. Acme Corp")
    add = st.form_submit_button("Add company", use_container_width=True)

if add:
    if not (t_name or "").strip():
        st.error("Company name is required.")
    else:
        t = create_tenant(name=t_name)
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
        st.markdown(f"**{t.get('name', '—')}** · `{t.get('id', '')}`")
        if t.get("id") != DEFAULT_TENANT_ID:
            if st.button("Delete", key=f"del_{t.get('id')}", type="secondary"):
                if delete_tenant(t.get("id") or ""):
                    st.rerun()
                else:
                    st.warning("Cannot delete: remove or move projects that use this company first, or it is the default company.")
        st.divider()
