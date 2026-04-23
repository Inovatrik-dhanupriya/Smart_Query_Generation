"""
Sign Up page for Streamlit multipage routing.

Run app from `ui/streamlit_app.py` and open `/signup`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.auth.pages import render_sign_up_page

st.set_page_config(
    page_title="Create account",
    page_icon="📝",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(
    """
    <style>
      section[data-testid="stSidebar"] { display: none !important; }
      [data-testid="collapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

if st.session_state.get("auth_user"):
    st.switch_page("pages/dashboard.py")


def _to_sign_in() -> None:
    st.switch_page("pages/signin.py")


render_sign_up_page(go_to_sign_in=_to_sign_in)
