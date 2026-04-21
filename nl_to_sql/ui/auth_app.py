"""
ui/auth_app.py — Streamlit auth entrypoint.

Run (from `nl_to_sql/`):
    streamlit run ui/auth_app.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# `nl_to_sql/` must be on sys.path so sibling imports resolve.
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ui.auth.pages import render_sign_in_page, render_sign_up_page

st.set_page_config(page_title="Smart Query Auth", page_icon="🔐", layout="wide")

_SIGN_IN = "signin"
_SIGN_UP = "signup"


def _get_route() -> str:
    raw = st.query_params.get("page", _SIGN_IN)
    if isinstance(raw, list):
        raw = raw[0] if raw else _SIGN_IN
    route = str(raw).strip().lower()
    return route if route in {_SIGN_IN, _SIGN_UP} else _SIGN_IN


def _set_route(route: str) -> None:
    st.query_params.clear()
    st.query_params["page"] = route
    st.rerun()


def _go_sign_in() -> None:
    _set_route(_SIGN_IN)


def _go_sign_up() -> None:
    _set_route(_SIGN_UP)


def _go_main() -> None:
    st.switch_page("pages/dashboard.py")


st.title("Smart Query Generation")
route = _get_route()

if route == _SIGN_UP:
    render_sign_up_page(go_to_sign_in=_go_sign_in)
else:
    render_sign_in_page(go_to_sign_up=_go_sign_up, go_to_main=_go_main)
