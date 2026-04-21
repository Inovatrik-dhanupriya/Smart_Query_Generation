"""
Sign Up page for Streamlit multipage routing.

Run app from `ui/streamlit_app.py` and open `/signup`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.auth.pages import render_sign_up_page
from ui.theme import apply_shared_theme

st.set_page_config(page_title="Sign Up", page_icon="📝", layout="wide")
apply_shared_theme()

if st.session_state.get("auth_user"):
    st.switch_page("streamlit_app.py")

with st.sidebar:
    st.page_link("pages/signin.py", label="Go to Sign In", icon="🔐")


def _to_sign_in() -> None:
    st.switch_page("pages/signin.py")


render_sign_up_page(go_to_sign_in=_to_sign_in)
