"""Shared Streamlit visual styles for the UI package."""

from __future__ import annotations

import streamlit as st


def apply_shared_theme() -> None:
    """Apply consistent spacing/colors/gradient text style across pages."""
    st.markdown(
        """
        <style>
            .block-container {
                padding-top: 1.4rem;
                padding-bottom: 1.5rem;
            }
            .sqg-page-title {
                margin: 0;
                line-height: 1.1;
                font-weight: 800;
                letter-spacing: -0.01em;
                background: linear-gradient(90deg, #60a5fa 0%, #a78bfa 45%, #34d399 100%);
                -webkit-background-clip: text;
                background-clip: text;
                color: transparent;
            }
            .sqg-page-subtitle {
                margin-top: 0.3rem;
                color: rgba(255, 255, 255, 0.70);
            }
            .sqg-card {
                background: rgba(255, 255, 255, 0.03);
                border: 1px solid rgba(148, 163, 184, 0.25);
                border-radius: 14px;
                padding: 1rem 1rem 0.5rem 1rem;
            }
            @media (max-width: 768px) {
                .block-container { padding-top: 1rem; }
                .sqg-card { border-radius: 10px; padding: 0.85rem 0.85rem 0.4rem 0.85rem; }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_page_header(title: str, subtitle: str = "") -> None:
    st.markdown(f"<h1 class='sqg-page-title'>{title}</h1>", unsafe_allow_html=True)
    if subtitle:
        st.markdown(f"<p class='sqg-page-subtitle'>{subtitle}</p>", unsafe_allow_html=True)
