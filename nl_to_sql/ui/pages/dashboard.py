"""
Tenant dashboard page (authenticated).
Run app from `ui/streamlit_app.py` and open `/dashboard`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.tenant.dashboard import render_tenant_dashboard
from ui.theme import apply_shared_theme

st.set_page_config(page_title="Dashboard", page_icon="🏠", layout="wide")
apply_shared_theme()

if "auth_user" not in st.session_state:
    st.session_state.auth_user = None

if not st.session_state.auth_user:
    st.switch_page("pages/signin.py")
    st.stop()

with st.sidebar:
    _auth = st.session_state.auth_user or {}
    st.caption(f"Signed in as `{_auth.get('username', 'user')}`")
    st.page_link("pages/3_Tenants.py", label="Companies (tenants)", icon="🏬")
    st.caption("Open a project below, then use **Configuration** or **Chat** for that project.")
    if st.button("🚪 Sign Out", use_container_width=True):
        st.session_state.auth_user = None
        st.switch_page("pages/signin.py")
        st.stop()

render_tenant_dashboard()
