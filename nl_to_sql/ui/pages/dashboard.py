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

      [data-testid="stSidebar"] { background: #1e1b2e !important; border-right: 1px solid rgba(255,255,255,0.08) !important; }
      .sqg-sb-brand { color: #ffffff !important; font-weight: 700 !important; }
      .sqg-sb-name { color: #ffffff !important; font-weight: 600 !important; }
      .sqg-sb-role { color: #a78bfa !important; font-size: 0.75rem !important; }
      [data-testid="stSidebar"] [data-testid="stCaption"] p {
        color: #6b7280 !important; font-size: 0.65rem !important; letter-spacing: 0.12em !important; font-weight: 700 !important;
      }
      [data-testid="stSidebar"] [data-testid="stPageLink"] a {
        background: transparent !important; color: #c4b5fd !important; border-radius: 8px !important; border-left: 3px solid transparent !important;
      }
      [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover {
        background: rgba(124,58,237,0.15) !important; color: #ffffff !important;
      }
      [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] {
        background: rgba(124,58,237,0.2) !important; color: #ffffff !important; border-left-color: var(--sqg-accent) !important; font-weight: 600 !important;
      }
      [data-testid="stSidebar"] [data-baseweb="button"] {
        background: transparent !important; border: 1px solid rgba(255,255,255,0.2) !important; color: #c4b5fd !important;
      }
      [data-testid="stSidebar"] [data-baseweb="button"] * { color: #c4b5fd !important; fill: #c4b5fd !important; }
      [data-testid="stSidebar"] [data-baseweb="button"]:hover { border-color: var(--sqg-accent) !important; }
      [data-testid="stSidebar"] [data-baseweb="button"]:hover * { color: #ffffff !important; fill: #ffffff !important; }

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
      section.main [data-testid="stMarkdownContainer"] p.sqg-dmi-title { font-size: 0.72rem !important; font-weight: 600 !important; }
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
        <div style="display:flex;align-items:center;gap:0.5rem;margin-bottom:0.35rem">
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
    st.page_link("pages/dashboard.py", label="Projects", icon="🗃️", help=None)
    st.page_link("pages/tenants.py", label="Companies", icon="🏬", help=None)
    st.page_link("pages/project_open.py", label="Open project", icon="📂", help=None)
    st.page_link("pages/project_chat.py", label="Chat", icon="💬", help=None)
    st.caption("SETTINGS")
    st.page_link("pages/project_configuration.py", label="Configuration", icon="🔧", help=None)
    st.divider()
    st.markdown('<div class="sqg-sb-gutter" aria-hidden="true"></div>', unsafe_allow_html=True)
    if st.button("Sign out", use_container_width=True, type="secondary", key="dash_sign_out"):
        clear_auth_session()
        st.switch_page("pages/signin.py")
        st.stop()

render_tenant_dashboard()
