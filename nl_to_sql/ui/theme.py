"""Shared Streamlit visual styles for the UI package."""

from __future__ import annotations

import streamlit as st


def apply_shared_theme() -> None:
    """Apply consistent spacing/colors/gradient text style across pages."""
    st.markdown(
        """
        <style>
            .stApp {
                background: linear-gradient(180deg, #0b1220 0%, #0e1628 40%, #0a0f18 100%);
                color: #e2e8f0;
            }
            [data-testid="stSidebar"] {
                background: #0f172a;
                border-right: 1px solid #1e293b;
            }
            [data-testid="stSidebar"] .stMarkdown,
            [data-testid="stSidebar"] label {
                color: #cbd5e1 !important;
            }
            /* Hide Streamlit's auto-generated multipage list. */
            [data-testid="stSidebarNav"] {
                display: none;
            }
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
            .nl-banner {
                background: linear-gradient(90deg, #134e5e, #0f2942);
                padding: 1rem 1.25rem;
                border-radius: 10px;
                border: 1px solid #1e3a4a;
                margin-bottom: 1rem;
            }
            .nl-banner h3 {
                color: #e0f7fa;
                margin: 0 0 0.35rem 0;
                font-size: 1.05rem;
            }
            .nl-banner p {
                color: #94a3b8;
                margin: 0;
                font-size: 0.95rem;
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
    # Match the heading style used in streamlit_app.py (`st.title` + `st.caption`).
    st.title(title)
    if subtitle:
        st.caption(subtitle)
