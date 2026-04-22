"""Reusable auth UI components."""

from __future__ import annotations

from typing import Callable

import streamlit as st
from ui.theme import apply_shared_theme, render_page_header


def render_auth_styles() -> None:
    st.markdown(
        """
        <style>
            .auth-title-wrap { text-align: center; margin-bottom: 1rem; }
            .auth-subtitle { text-align: center; color: rgba(255,255,255,0.72); margin-bottom: 1.1rem; }
            .auth-footer { text-align: center; margin-top: 0.75rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_auth_layout(title: str, subtitle: str, form_renderer: Callable[[], None]) -> None:
    apply_shared_theme()
    render_auth_styles()
    left, center, right = st.columns([1, 1.5, 1])
    with center:
        st.markdown("<div class='auth-title-wrap'>", unsafe_allow_html=True)
        render_page_header(title, subtitle)
        st.markdown("</div>", unsafe_allow_html=True)
        st.markdown("<div class='sqg-card'>", unsafe_allow_html=True)
        with st.container():
            form_renderer()
        st.markdown("</div>", unsafe_allow_html=True)


def render_nav_link(text: str, button_text: str, on_click: Callable[[], None]) -> None:
    st.markdown(f"<p class='auth-footer'>{text}</p>", unsafe_allow_html=True)
    if st.button(button_text, use_container_width=True):
        on_click()
