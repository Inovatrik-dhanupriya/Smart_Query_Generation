"""
Sign In page for Streamlit multipage routing.

Run app from `ui/streamlit_app.py` and open `/signin`.
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

from ui.auth.pages import render_sign_in_page
from ui.auth.session import restore_auth_session

st.set_page_config(
    page_title="Sign in",
    page_icon="🔐",
    layout="wide",
    initial_sidebar_state="collapsed",
)
# Landing page: no duplicate theme here — layout applies it. Hide default sidebar for a clean hero.
st.markdown(
    """
    <style>
      section[data-testid="stSidebar"] { display: none !important; }
      [data-testid="collapsedControl"] { display: none !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

if restore_auth_session():
    st.switch_page("pages/dashboard.py")


def _to_sign_up() -> None:
    st.switch_page("pages/signup.py")


def _to_main() -> None:
    st.switch_page("pages/dashboard.py")


render_sign_in_page(go_to_sign_up=_to_sign_up, go_to_main=_to_main)
