"""
Sign In page for Streamlit multipage routing.

Run app from `ui/streamlit_app.py` and open `/signin`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.auth.pages import render_sign_in_page
from ui.theme import apply_shared_theme

st.set_page_config(page_title="Sign In", page_icon="🔐", layout="wide")
apply_shared_theme()

if st.session_state.get("auth_user"):
    st.switch_page("pages/dashboard.py")

with st.sidebar:
    st.page_link("pages/signup.py", label="Go to Sign Up", icon="📝")


def _to_sign_up() -> None:
    st.switch_page("pages/signup.py")


def _to_main() -> None:
    st.switch_page("pages/dashboard.py")


render_sign_in_page(go_to_sign_up=_to_sign_up, go_to_main=_to_main)
