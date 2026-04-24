"""
Tenant dashboard page (authenticated).
Run app from `ui/streamlit_app.py` and open `/dashboard`.
"""

from __future__ import annotations

import html
import sys
from pathlib import Path

import streamlit as st

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

from ui.auth.session import clear_auth_session, restore_auth_session
from ui.sidebar_icons import (
    SIDEBAR_CHAT,
    SIDEBAR_COMPANIES,
    SIDEBAR_CONFIGURATION,
    SIDEBAR_OPEN_PROJECT,
    SIDEBAR_PROJECTS,
)
from ui.tenant.dashboard import render_tenant_dashboard
from ui.theme import apply_dashboard_theme

st.set_page_config(page_title="Projects", page_icon="📁", layout="wide")
apply_dashboard_theme()
st.markdown(
    """
    <style>
      :root { --sqg-accent: #7c3aed; --sqg-accent-2: #6d28d9; --sqg-ink: #1e1b2e; --sqg-body: #4b5563; --sqg-muted: #9ca3af; }
      [data-testid="stAppViewContainer"] .stApp { background: #f8f7ff !important; color: var(--sqg-ink) !important; }
      [data-testid="stAppViewBlockContainer"] { background: #f8f7ff !important; }
      section.main { background: #f8f7ff !important; }
      section.main > div.block-container {
        background: transparent !important;
        border: none !important;
        border-top: 3px solid var(--sqg-accent) !important;
        border-radius: 0 !important;
        box-shadow: none !important;
      }

      section.main .sqg-dash-title h1 { color: var(--sqg-ink) !important; font-size: 2.7rem !important; font-weight: 900 !important; line-height: 1.05 !important; letter-spacing: -0.02em !important; }
      section.main .sqg-dash-sub { color: #6b7280 !important; font-size: 1.02rem !important; font-weight: 400 !important; line-height: 1.55 !important; margin: 0.12rem 0 1.05rem !important; }
      section.main [data-baseweb="select"] { background: #ffffff !important; border: 1.5px solid #e5e7eb !important; border-radius: 8px !important; }
      section.main [data-baseweb="select"] [data-baseweb="base-input"] { color: var(--sqg-ink) !important; }
      section.main [data-baseweb="select"]:focus-within { border-color: var(--sqg-accent) !important; }
      section.main [data-testid="stPageLink"] a { color: var(--sqg-accent) !important; font-weight: 500 !important; }
      section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type [data-baseweb="button"][kind="primary"] {
        background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important;
        color: #ffffff !important; font-weight: 600 !important; border-radius: 8px !important; border: none !important;
        box-shadow: 0 4px 14px rgba(124,58,237,0.4) !important;
      }
      section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type [data-baseweb="button"][kind="primary"] * { color: #ffffff !important; }

      .sqg-dash-info {
        background: linear-gradient(135deg, #ede9fe, #f5f3ff) !important; border: 1px solid #c4b5fd !important; border-left: 4px solid var(--sqg-accent) !important;
        border-radius: 10px !important; color: #4b5563 !important;
      }
      .sqg-info-kw { color: var(--sqg-accent) !important; font-weight: 600 !important; }

      .sqg-dash-metric {
        background: #ffffff !important; border: 1px solid #ede9fe !important; border-top: 3px solid var(--sqg-accent) !important;
        border-radius: 14px !important; box-shadow: 0 2px 12px rgba(124,58,237,0.08) !important;
      }
      .sqg-dmi-title { font-size: 0.72rem !important; color: var(--sqg-muted) !important; letter-spacing: 0.1em !important; font-weight: 600 !important; }
      .sqg-dmi-val {
        font-size: 1.05rem !important;
        font-weight: 100 !important;
        color: var(--sqg-ink) !important;
        line-height: 1.0 !important;
        letter-spacing: -0.02em !important;
      }
      .sqg-dmi-hint { color: #6b7280 !important; font-size: 0.86rem !important; font-weight: 400 !important; }
      .sqg-dash-metric-ico { background: linear-gradient(135deg, #ede9fe, #ddd6fe) !important; color: var(--sqg-accent) !important; }
      section.main [data-testid="stMarkdownContainer"] p.sqg-dmi-title { font-size: 1rem !important; font-weight: 600 !important; }
      section.main [data-testid="stMarkdownContainer"] p.sqg-dmi-val {
        font-size: 2.65rem !important;
        font-weight: 900 !important;
        line-height: 1.05 !important;
        letter-spacing: -0.02em !important;
        color: var(--sqg-ink) !important;
      }

      section.main .sqg-dash-sec { font-size: 1.45rem !important; font-weight: 650 !important; color: var(--sqg-ink) !important; letter-spacing: -0.01em !important; }
      .sqg-dash-proj {
        background: #ffffff !important; border: 1px solid #e5e7eb !important; border-left: 4px solid var(--sqg-accent) !important;
        border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
        transition: box-shadow .18s ease, transform .18s ease !important;
        margin-bottom: 1rem !important;
      }
      .sqg-dash-proj:hover { box-shadow: 0 8px 24px rgba(124,58,237,0.12) !important; transform: translateY(-2px) !important; }
      section.main .sqg-dash-proj .sqg-dash-proj-title {
        color: var(--sqg-ink) !important; font-size: 3.55rem !important; font-weight: 900 !important; line-height: 1.2 !important;
        margin: 0 0 0.55rem 0 !important;
        letter-spacing: -0.01em !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] p.sqg-dash-proj-title {
        color: var(--sqg-ink) !important; font-size: 1.55rem !important; font-weight: 900 !important; line-height: 1.2 !important;
        margin: 0 0 0.55rem 0 !important;
        letter-spacing: -0.01em !important;
      }
      section.main .sqg-dash-proj p.sqg-dash-proj-line {
        color: #4b5563 !important; font-size: 0.92rem !important; font-weight: 400 !important; line-height: 1.62 !important; margin: 0.16rem 0 0.52rem 0 !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-label {
        font-size: 0.76rem !important; color: var(--sqg-muted) !important; font-weight: 600 !important; letter-spacing: 0.02em !important; text-transform: uppercase !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] .sqg-dash-proj-label {
        font-size: 0.76rem !important; color: var(--sqg-muted) !important; font-weight: 600 !important; letter-spacing: 0.02em !important; text-transform: uppercase !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-label--subtle {
        opacity: 0.78 !important; margin-right: 0.3rem !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-value {
        font-size: 0.95rem !important; color: #312e81 !important; font-weight: 700 !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] .sqg-dash-proj-value {
        font-size: 0.95rem !important; color: #312e81 !important; font-weight: 700 !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-desc {
        font-size: 0.92rem !important; color: var(--sqg-body) !important; font-weight: 400 !important; line-height: 1.62 !important;
        margin: 0.3rem 0 0.72rem 0 !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] p.sqg-dash-proj-desc {
        font-size: 0.92rem !important; color: var(--sqg-body) !important; font-weight: 400 !important; line-height: 1.62 !important;
        margin: 0.3rem 0 0.72rem 0 !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-meta {
        font-size: 0.76rem !important; color: var(--sqg-muted) !important; font-weight: 500 !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] .sqg-dash-proj-meta {
        font-size: 0.76rem !important; color: var(--sqg-muted) !important; font-weight: 500 !important;
      }
      section.main .sqg-dash-proj .sqg-dash-proj-line--right {
        text-align: right !important;
      }
      section.main .sqg-dash-proj .sqg-dash-status-badge {
        display: inline-flex !important; align-items: center !important;
        border-radius: 999px !important; padding: 2px 10px !important;
        font-size: 0.75rem !important; font-weight: 600 !important;
        color: #16a34a !important; background: #dcfce7 !important; margin-left: 0.42rem !important;
      }
      section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] .sqg-dash-status-badge {
        display: inline-flex !important; align-items: center !important;
        border-radius: 999px !important; padding: 2px 10px !important;
        font-size: 0.75rem !important; font-weight: 600 !important;
        color: #16a34a !important; background: #dcfce7 !important; margin-left: 0.42rem !important;
      }
      section.main .sqg-dash-proj .sqg-dash-status-badge:empty { display: none !important; }

      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-baseweb="button"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.92), rgba(109,40,217,0.88)) !important;
        color: #ffffff !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important; border: none !important;
        box-shadow: 0 2px 8px rgba(124,58,237,0.3) !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-baseweb="button"] * { color: #ffffff !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-testid="stButton"] > button {
        background: linear-gradient(135deg, rgba(124,58,237,0.92), rgba(109,40,217,0.88)) !important;
        color: #ffffff !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important; border: none !important;
        box-shadow: 0 2px 8px rgba(124,58,237,0.3) !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-testid="stButton"] > button * { color: #ffffff !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-testid^="stBaseButton"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.92), rgba(109,40,217,0.88)) !important;
        color: #ffffff !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important; border: none !important;
        box-shadow: 0 2px 8px rgba(124,58,237,0.3) !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-testid^="stBaseButton"] * { color: #ffffff !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(1) [data-baseweb="button"]:hover { filter: brightness(1.1) !important; transform: translateY(-1px) !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.12), rgba(124,58,237,0.06)) !important;
        border: 1.5px solid rgba(124,58,237,0.24) !important; color: #4c1d95 !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button {
        background: linear-gradient(135deg, rgba(124,58,237,0.12), rgba(124,58,237,0.06)) !important;
        border: 1.5px solid rgba(124,58,237,0.24) !important; color: #4c1d95 !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button * { color: #4c1d95 !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid^="stBaseButton"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.12), rgba(124,58,237,0.06)) !important;
        border: 1.5px solid rgba(124,58,237,0.24) !important; color: #4c1d95 !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"]:hover { border-color: var(--sqg-accent) !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"]:hover * { color: var(--sqg-accent) !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"]:hover,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"]:active,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-baseweb="button"]:focus-visible,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:hover,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:active,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:focus-visible {
        background: linear-gradient(135deg, rgba(124,58,237,0.18), rgba(124,58,237,0.10)) !important;
        border-color: rgba(124,58,237,0.52) !important;
        color: #5b21b6 !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:hover *,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:active *,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(2) [data-testid="stButton"] > button:focus-visible * {
        color: #5b21b6 !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-baseweb="button"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.1), rgba(124,58,237,0.04)) !important;
        border: 1.5px solid rgba(124,58,237,0.18) !important; color: #7f1d1d !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button {
        background: linear-gradient(135deg, rgba(124,58,237,0.1), rgba(124,58,237,0.04)) !important;
        border: 1.5px solid rgba(124,58,237,0.18) !important; color: #7f1d1d !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button * { color: #7f1d1d !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid^="stBaseButton"] {
        background: linear-gradient(135deg, rgba(124,58,237,0.1), rgba(124,58,237,0.04)) !important;
        border: 1.5px solid rgba(124,58,237,0.18) !important; color: #7f1d1d !important; font-weight: 600 !important; font-size: 0.9rem !important; border-radius: 8px !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-baseweb="button"]:hover { background: rgba(124,58,237,0.16) !important; }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-baseweb="button"]:hover,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-baseweb="button"]:active,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-baseweb="button"]:focus-visible,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:hover,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:active,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:focus-visible {
        background: linear-gradient(135deg, rgba(124,58,237,0.18), rgba(124,58,237,0.09)) !important;
        border-color: rgba(124,58,237,0.45) !important;
        color: #6d28d9 !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:hover *,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:active *,
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type > [data-testid="column"]:nth-child(3) [data-testid="stButton"] > button:focus-visible * {
        color: #6d28d9 !important;
      }
      section.main .sqg-dash-proj [data-testid="stHorizontalBlock"]:last-of-type [data-baseweb="button"] { transition: all .16s ease !important; }

      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"] {
        background: #f3f4f6 !important; color: #6b7280 !important; border: none !important; border-radius: 20px !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"] {
        background: #f3f4f6 !important; color: #6b7280 !important; border: none !important; border-radius: 20px !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] {
        background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important; color: #ffffff !important; border: none !important; border-radius: 20px !important; font-weight: 600 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="primary"] {
        background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important; color: #ffffff !important; border: none !important; border-radius: 20px !important; font-weight: 600 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] *,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="primary"] * {
        color: #ffffff !important;
        fill: #ffffff !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"]:hover {
        background: #ede9fe !important; color: var(--sqg-accent) !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"]:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"]:active,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"]:focus-visible,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:active,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:focus-visible {
        background: linear-gradient(135deg, rgba(124,58,237,0.18), rgba(124,58,237,0.10)) !important;
        border: 1px solid rgba(124,58,237,0.48) !important;
        color: #5b21b6 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:hover *,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:active *,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="secondary"]:focus-visible * {
        color: #5b21b6 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"]:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"]:active,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"]:focus-visible,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="primary"]:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="primary"]:active,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stButton"] > button[kind="primary"]:focus-visible {
        background: linear-gradient(135deg, rgba(124,58,237,0.92), rgba(109,40,217,0.88)) !important;
        border: 1px solid rgba(124,58,237,0.62) !important;
        color: #ffffff !important;
      }

      /* "My projects" title + All / Active / Archived: one baseline, even spacing */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) {
        align-items: center !important; width: 100% !important; box-sizing: border-box !important;
        margin: 0.2rem 0 0.9rem 0 !important; row-gap: 0.4rem !important; column-gap: 0.5rem !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) p.sqg-dash-sec--row { margin: 0 !important; padding: 0 !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="column"]:first-child {
        display: flex !important; flex-direction: column !important; justify-content: center !important; min-height: 2.4rem !important;
      }

      /* —— My projects table: 6-col grid, header+rows share _PROJ_TBL_COLS in Python —— */
      /* Match header & body: same per-column padding so th sits over the same box as td */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"],
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"] {
        box-sizing: border-box !important; padding: 0.12rem 0.32rem !important; min-width: 0 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="stMarkdownContainer"],
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"] [data-testid="stMarkdownContainer"] {
        margin: 0 !important; width: 100% !important; max-width: 100% !important;
      }
      section.main p.sqg-proj-th {
        font-size: 0.8rem !important; font-weight: 700 !important; letter-spacing: 0.1em !important;
        text-transform: uppercase !important; color: #4b5563 !important; margin: 0 !important; line-height: 1.3 !important;
        padding: 0.24rem 0.06rem !important; width: 100% !important; box-sizing: border-box !important;
      }
      /* Description / Status / Updated column headers: stronger contrast */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:is(:nth-child(3),:nth-child(4),:nth-child(5)) p.sqg-proj-th {
        color: #0f172a !important; font-weight: 800 !important; letter-spacing: 0.09em !important;
      }
      /* Header: single band; th vertically centered in each track */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) {
        background: #faf7ff !important; border: 1px solid #e5e7eb !important; border-bottom: 1px solid #e9e1ff !important;
        border-radius: 10px 10px 0 0 !important; padding: 0.9rem 1.35rem !important; margin: 0 0 0 !important;
        box-shadow: 0 1px 0 rgba(124,58,237,0.05) !important;
        display: flex !important; flex-direction: row !important; flex-wrap: nowrap !important; align-items: stretch !important;
        width: 100% !important; box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3),:nth-child(4),:nth-child(5)) {
        display: flex !important; flex-direction: column !important; justify-content: center !important; align-items: stretch !important;
        min-height: 0 !important; align-self: stretch !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3),:nth-child(4),:nth-child(5)) [data-testid="stVerticalBlock"] {
        display: flex !important; flex-direction: column !important; justify-content: center !important; align-items: stretch !important;
        width: 100% !important; margin: 0 !important; flex: 0 0 auto !important; min-height: 0 !important; align-self: stretch !important;
      }
      /* th: same horizontal axis as td — left / center / right per column */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) p.sqg-proj-th { text-align: left !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:is(:nth-child(4),:nth-child(5)) p.sqg-proj-th { text-align: center !important; }
      section.main .sqg-proj-th-action-wrap {
        width: 100% !important; max-width: 20rem !important; margin: 0 auto !important; padding: 0.24rem 0.06rem !important; box-sizing: border-box !important; text-align: center !important;
      }
      section.main p.sqg-proj-th--action { text-align: center !important; width: 100% !important; display: block !important; margin: 0 !important; padding: 0 !important; line-height: 1.3 !important; }
      /* Header: Action column — label centered over the 3-button group below */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:nth-child(6) {
        display: flex !important; flex-direction: row !important; align-items: center !important; justify-content: center !important;
        align-self: stretch !important; min-width: 0 !important; box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-th) [data-testid="column"]:nth-child(6) [data-testid="stVerticalBlock"] {
        display: flex !important; flex-direction: row !important; align-items: center !important; justify-content: center !important; width: 100% !important; height: 100% !important; margin: 0 !important;
      }
      /* Data row: cell alignment 1–3 left, 4–5 center */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(1) p { text-align: left !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(2) p { text-align: left !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(3) p { text-align: left !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(4) p,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(5) p { text-align: center !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(4) p.sqg-proj-td--status {
        display: flex !important; justify-content: center !important; align-items: center !important; width: 100% !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(4) .sqg-proj-td-statuscell { justify-content: center !important; }
      /* Data cols 1–5: same vertical centering in cell as header (stacks with row align-items: center) */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3),:nth-child(4),:nth-child(5)) {
        display: flex !important; flex-direction: column !important; justify-content: center !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3),:nth-child(4),:nth-child(5)) [data-testid="stVerticalBlock"] {
        display: flex !important; flex-direction: column !important; justify-content: center !important; width: 100% !important; min-height: 0 !important; margin: 0 !important;
      }
      /* Data row: action col — center Open / Edit / Delete as a group */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) {
        display: flex !important; flex-direction: row !important; align-items: center !important; justify-content: center !important;
        min-width: 0 !important; box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stVerticalBlock"] {
        display: flex !important; flex-direction: row !important; flex-wrap: nowrap !important; align-items: center !important; justify-content: center !important;
        width: 100% !important; max-width: 100% !important; min-width: 0 !important; margin: 0 !important; padding: 0 !important; gap: 0 !important;
        box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] {
        display: flex !important; flex: 0 1 auto !important; width: 100% !important; max-width: 20rem !important;
        flex-wrap: nowrap !important; align-items: center !important; justify-content: center !important;
        min-height: 0 !important; gap: 0.55rem 0.65rem !important; margin: 0 auto !important; box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="column"] {
        flex: 1 1 0 !important; min-width: 0 !important; max-width: 33% !important;
        display: flex !important; flex-direction: column !important; align-items: center !important; justify-content: center !important; padding: 0 0.2rem !important; box-sizing: border-box !important;
        align-self: stretch !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="column"] [data-testid="stVerticalBlock"] {
        display: flex !important; flex-direction: column !important; align-items: center !important; justify-content: center !important; width: 100% !important; margin: 0 !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="column"] [data-testid="element-container"] {
        display: flex !important; flex-direction: column !important; align-items: center !important; width: 100% !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="column"] [data-testid="stButton"],
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="column"] [data-baseweb="button"] {
        width: 100% !important; max-width: 100% !important; box-sizing: border-box !important;
      }
      /* Row typography hierarchy */
      section.main p.sqg-proj-td-name {
        color: var(--sqg-ink) !important; font-size: 1.22rem !important; font-weight: 800 !important; line-height: 1.3 !important;
        margin: 0 !important; letter-spacing: -0.02em !important;
      }
      section.main p.sqg-proj-td { margin: 0 !important; min-height: 0 !important; }
      section.main p.sqg-proj-td--company {
        color: #3730a3 !important; font-size: 0.98rem !important; font-weight: 600 !important; line-height: 1.45 !important;
      }
      section.main p.sqg-proj-td--desc {
        color: #52525b !important; font-size: 0.88rem !important; font-weight: 500 !important; line-height: 1.5 !important;
        display: -webkit-box !important; -webkit-line-clamp: 2 !important; -webkit-box-orient: vertical !important; overflow: hidden !important;
      }
      section.main p.sqg-proj-td-meta {
        color: #57534e !important; font-size: 0.8rem !important; font-weight: 500 !important; line-height: 1.4 !important; white-space: nowrap !important;
        letter-spacing: 0.01em !important;
      }
      section.main p.sqg-proj-td--status { margin: 0 !important; line-height: 1 !important; }
      section.main .sqg-proj-td-statuscell { display: inline-flex !important; align-items: center !important; }
      /* Status pill badges (Active / Draft / Archived) */
      section.main .sqg-proj-tbl-badge,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) p.sqg-dash-status-badge {
        display: inline-flex !important; align-items: center !important; justify-content: center !important; border-radius: 9999px !important;
        padding: 4px 14px !important; font-size: 0.72rem !important; font-weight: 700 !important; line-height: 1.3 !important;
        white-space: nowrap !important; margin: 0 !important; box-sizing: border-box !important;
      }
      /* Data row: separation + hover (structure unchanged) */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) {
        background: #ffffff !important; border: 1px solid #e5e0ed !important; border-left: 3px solid var(--sqg-accent) !important;
        border-radius: 8px !important; padding: 0.9rem 1.35rem !important; margin: 0 0 0.5rem 0 !important;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.045) !important; transition: box-shadow 0.18s ease, background 0.18s ease, border-color 0.18s ease, transform 0.18s ease !important;
        align-items: center !important; width: 100% !important; box-sizing: border-box !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name):hover {
        box-shadow: 0 4px 16px rgba(124, 58, 237, 0.11) !important; border-color: #ddd4f5 !important; background: #fcfbff !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name--sep) { margin-top: 0.6rem !important; }

      /* Open / Edit / Delete — same outline style as former Edit; aligned in centered sub-cols */
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-baseweb="button"],
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid="stButton"] > button,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid^="stBaseButton"] {
        background: #ffffff !important; border: 1.5px solid #d1d5db !important; color: #4b5563 !important; font-weight: 600 !important; font-size: 0.86rem !important;
        border-radius: 8px !important; min-height: 2.4rem !important; padding: 0.3rem 0.55rem !important; white-space: nowrap !important; width: 100% !important; max-width: 100% !important;
        box-shadow: 0 1px 2px rgba(15,23,42,0.05) !important; transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease, filter 0.18s ease !important;
        box-sizing: border-box !important; flex-shrink: 0 !important; align-self: center !important; filter: none !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-baseweb="button"] *,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid="stButton"] > button * { color: #4b5563 !important; }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-baseweb="button"]:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid="stButton"] > button:hover,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid^="stBaseButton"]:hover {
        background: #f5f3ff !important; border-color: var(--sqg-accent) !important; color: #5b21b6 !important; box-shadow: 0 2px 8px rgba(124,58,237,0.12) !important; transform: translateY(-1px) !important; filter: none !important;
      }
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-baseweb="button"]:hover *,
      section.main [data-testid="stHorizontalBlock"]:has(p.sqg-proj-td-name) [data-testid="column"]:nth-child(6) [data-testid="stHorizontalBlock"] [data-testid="column"]:is(:nth-child(1),:nth-child(2),:nth-child(3)) [data-testid="stButton"] > button:hover * { color: #5b21b6 !important; }

      section.main p { font-size: 0.95rem !important; line-height: 1.62 !important; color: #4b5563 !important; }
      section.main [data-testid="stCaption"], section.main [data-testid="stCaption"] p { font-size: 0.78rem !important; color: var(--sqg-muted) !important; font-weight: 500 !important; }
    </style>
    """,
    unsafe_allow_html=True,
)

if not restore_auth_session():
    st.switch_page("pages/signin.py")
    st.stop()

_auth = st.session_state.auth_user or {}
_uname = str(_auth.get("username", "user") or "user")
_init = (html.escape(_uname[:1] or "?")).upper()
_display = html.escape(_uname)
_role = "Member"

with st.sidebar:
    st.markdown(
        f"""
        <div class="sqg-sb-top" style="display:flex;align-items:center;gap:0.5rem">
          <span style="display:flex;width:30px;height:30px;border-radius:8px;background:#5b21b6;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.12)">
            <span style="display:flex;flex-direction:column;gap:2px;align-items:flex-start;justify-content:center">
              <span style="height:2px;width:12px;background:#fff;border-radius:1px"></span>
              <span style="height:2px;width:8px;background:#fff;border-radius:1px;opacity:0.95"></span>
              <span style="height:2px;width:10px;background:#fff;border-radius:1px;opacity:0.9"></span>
            </span>
          </span>
          <span class="sqg-sb-brand" style="margin:0">Smart Query</span>
        </div>
        <div class="sqg-sb-user">
          <div class="sqg-sb-av">{_init}</div>
          <div>
            <div class="sqg-sb-name">{_display}</div>
            <div class="sqg-sb-role">{_role}</div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption("WORKSPACE")
    st.page_link("pages/dashboard.py", label="Projects", icon=SIDEBAR_PROJECTS, help=None)
    st.page_link("pages/tenants.py", label="Companies", icon=SIDEBAR_COMPANIES, help=None)
    st.page_link("pages/project_open.py", label="Open project", icon=SIDEBAR_OPEN_PROJECT, help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon=SIDEBAR_CHAT, help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon=SIDEBAR_CONFIGURATION, help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="dash_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

render_tenant_dashboard()
