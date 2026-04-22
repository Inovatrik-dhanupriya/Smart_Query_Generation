"""
Home entry for the Streamlit multipage app.

Run (from ``nl_to_sql/``):  ``streamlit run ui/streamlit_app.py``

Authenticated users are sent to the tenant dashboard to pick a project; each
project has its own Configuration and Chat pages with isolated API sessions.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from utils.env import load_app_env

load_app_env()

st.set_page_config(page_title="NL → SQL", page_icon="🔍", layout="wide")

if "auth_user" not in st.session_state:
    st.session_state.auth_user = None

if not st.session_state.auth_user:
    st.switch_page("pages/signin.py")
    st.stop()

st.switch_page("pages/dashboard.py")
st.stop()
