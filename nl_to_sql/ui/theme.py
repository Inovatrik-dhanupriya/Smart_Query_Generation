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


def apply_dashboard_theme() -> None:
    """
    Light, card-based tenant dashboard (QueryBase-style).

    Styles are NOT scoped to body classes: Streamlit may not run ``<script>`` from st.markdown
    in some environments, so we inject strong rules only from ``pages/dashboard.py`` (per-run).
    """
    st.markdown(
        """
        <style>
        /* QueryBase/Smart Query: off-white app shell, main reading area white */
        :root { --sqg-purple: #5b21b6; --sqg-purple-hover: #6d28d9; }
        [data-testid="stAppViewContainer"] .stApp { color: #0f172a !important; background: #f3f4f6 !important; }
        [data-testid="stAppViewBlockContainer"] { background: #f3f4f6 !important; padding: 0.5rem 1.5rem 1.5rem !important; box-sizing: border-box !important; }
        [data-testid="stHeader"] { background: #ffffff !important; }
        /* Vertically center the card when the page is short; long pages grow normally (margin auto) */
        section.main {
            background: #f3f4f6 !important;
            min-height: calc(100vh - 4.25rem) !important;
            display: flex !important;
            flex-direction: column !important;
            box-sizing: border-box !important;
            padding: 0.75rem 0 1rem 0 !important;
        }
        section.main > div.block-container {
            max-width: 1120px !important; margin-left: auto !important; margin-right: auto !important; width: 100% !important;
            margin-top: auto !important; margin-bottom: auto !important; flex: 0 0 auto !important;
            background: #ffffff !important; border-radius: 12px !important; border: 1px solid #e5e7eb !important; padding: 1.25rem 1.35rem 1.5rem !important; box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
        }
        [data-testid="stSidebar"] { background: #f8fafc !important; border-right: 1px solid #e2e8f0 !important; }
        [data-testid="stSidebar"] a, [data-testid="stSidebar"] [data-testid="stPageLink-Nav"] a { color: #1e293b !important; }
        [data-testid="stSidebar"] a[aria-current="page"] {
            background: #eef2ff !important; border-radius: 8px; font-weight: 600; color: var(--sqg-purple) !important;
        }
        [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #64748b !important; }
        [data-testid="stSidebar"] [data-testid="stCaption"] {
            text-transform: uppercase !important; letter-spacing: 0.07em; font-size: 0.65rem !important; font-weight: 700; color: #94a3b8 !important;
        }
        /* All / Active / Archived — force single horizontal row + align right in wide column */
        section.main [data-baseweb="radio"] { width: 100% !important; }
        section.main [data-baseweb="radio"] [role="radiogroup"] {
            display: flex !important; flex-direction: row !important; flex-wrap: nowrap !important;
            justify-content: flex-end !important; align-items: center !important; gap: 0.5rem 1.25rem !important;
        }
        section.main [data-baseweb="radio"] label { display: inline-flex !important; align-items: center !important; margin: 0 !important; }
        /* Extra fallback in case [client] showSidebarNavigation in config is not picked up */
        [data-testid="stSidebarNav"] { display: none !important; }
        /* Main only — avoids clobbering other pages' sidebar if styles linger */
        section.main [data-baseweb="input"] { background: #fff !important; }
        section.main [data-baseweb="textarea"] { background: #fff !important; }
        /* Company filter: dark control + light text (reference) */
        section.main [data-baseweb="select"] { background: #1e293b !important; border: 1px solid #334155 !important; border-radius: 8px; }
        section.main [data-baseweb="select"] [data-baseweb="base-input"] { color: #f8fafc !important; }
        section.main [data-baseweb="select"] [data-baseweb="base-input"]::placeholder { color: #94a3b8 !important; }
        section.main label { color: #64748b !important; }
        section.main [data-baseweb="tab"] { color: #334155; }
        section.main [data-baseweb="tab"] [aria-selected="true"] { color: #5b21b6 !important; }
        section.main [data-baseweb="button"] { border-radius: 8px; }
        section.main button[kind="primary"] {
            background: var(--sqg-purple) !important; border-color: #4c1d95 !important; color: #ffffff !important;
        }
        section.main button[kind="primary"] p, section.main button[kind="primary"] span { color: #ffffff !important; }
        section.main button[kind="secondary"] p,
        section.main button[kind="secondary"] span,
        section.main button[kind="secondary"] div,
        section.main button[kind="secondary"] label {
            color: #0f172a !important;
        }
        section.main [data-baseweb="button"][kind="primary"] p,
        section.main [data-baseweb="button"][kind="primary"] span,
        section.main [data-baseweb="button"][kind="primary"] div,
        section.main [data-baseweb="button"][kind="primary"] label,
        section.main button[kind="primary"] p,
        section.main button[kind="primary"] span,
        section.main button[kind="primary"] div,
        section.main button[kind="primary"] label {
            color: #ffffff !important;
        }
        /* Toolbar + metric rows: one baseline / vertical center in each cell */
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] { width: 100% !important; }
        section.main [data-testid="stHorizontalBlock"] { align-items: center !important; }
        section.main [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            min-width: 0 !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
        }
        /* First row: company filter, Companies, New project — single-line CTA, right-aligned, aligned with select */
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type > [data-testid="column"]:last-child {
            align-items: flex-end !important;
            min-width: 10.5rem !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type [data-baseweb="button"][data-testid="stBaseButton-primary"],
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type button[kind="primary"] {
            white-space: nowrap !important;
            width: max-content !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type [data-baseweb="button"][data-testid="stBaseButton-primary"] p,
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type [data-baseweb="button"][data-testid="stBaseButton-primary"] span,
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type button[kind="primary"] p,
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"]:first-of-type button[kind="primary"] span { white-space: nowrap !important; }
        /* Page link and primary button share same vertical rhythm as the select */
        section.main [data-testid="stHorizontalBlock"] [data-testid="stElementContainer"] { margin-bottom: 0 !important; }
        section.main [data-testid="stHorizontalBlock"] [data-baseweb="select"] { margin-top: 0 !important; margin-bottom: 0 !important; }
        /* Sign out: dark bar + light label (wins over sidebar `span` / `p` muted color) */
        [data-testid="stSidebar"] [data-baseweb="button"] {
            background: #1e293b !important;
            color: #f8fafc !important;
            border: 1px solid #334155 !important;
            border-radius: 8px !important;
        }
        [data-testid="stSidebar"] [data-baseweb="button"] p,
        [data-testid="stSidebar"] [data-baseweb="button"] span,
        [data-testid="stSidebar"] [data-baseweb="button"] div,
        [data-testid="stSidebar"] [data-baseweb="button"] label { color: #f8fafc !important; }
        [data-testid="stSidebar"] [data-baseweb="button"] svg,
        [data-testid="stSidebar"] [data-baseweb="button"] path { color: #f8fafc !important; fill: #f8fafc !important; }
        /* My projects row: link-style All / Active / Archived (QueryBase) */
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) {
            align-items: center !important;
        }
        /* Inactive: light pill, dark label (overrides Streamlit dark secondary + bad contrast) */
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"],
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-secondary"] {
            background: #f1f5f9 !important;
            color: #334155 !important;
            border: 1px solid #e2e8f0 !important;
            font-weight: 600 !important;
            font-size: 0.9rem !important;
            min-height: 2.15rem !important;
            box-shadow: none !important;
            border-radius: 9999px !important;
        }
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"] p,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"] span,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"] div,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="secondary"] label,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-secondary"] p,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-secondary"] span,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-secondary"] div,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-secondary"] label {
            color: #334155 !important;
        }
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"],
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-primary"] {
            color: #5b21b6 !important;
            background: #f5f3ff !important;
            border: 1px solid #ddd6fe !important;
            border-radius: 9999px !important;
        }
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] p,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] span,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] div,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-baseweb="button"][kind="primary"] label,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-primary"] p,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-primary"] span,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-primary"] div,
        section.main [data-testid="stHorizontalBlock"]:has(p.sqg-dash-sec--row) [data-testid="stBaseButton-primary"] label { color: #5b21b6 !important; }
        /* Streamlit markdown: force readable copy on the light main area */
        section.main [data-testid="stMarkdownContainer"] { color: #334155 !important; }
        section.main [data-testid="stMarkdownContainer"] p { color: #334155 !important; }
        section.main [data-testid="stMarkdownContainer"] h1,
        section.main [data-testid="stMarkdownContainer"] h2,
        section.main [data-testid="stMarkdownContainer"] h3 { color: #0f172a !important; }
        section.main [data-testid="stCaption"] { color: #64748b !important; }

        .sqg-sb-brand { font-weight: 800; font-size: 1.1rem; color: #1e1b4b; letter-spacing: -0.02em; margin: 0.25rem 0 0.2rem; }
        .sqg-sb-mute { color: #64748b; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.06em; margin: 0.75rem 0 0.4rem; }
        .sqg-sb-user {
            display: flex; align-items: center; gap: 0.75rem; padding: 0.6rem 0.5rem; margin: 0.3rem 0 0.6rem;
            background: #f8fafc; border-radius: 10px; border: 1px solid #e2e8f0;
        }
        .sqg-sb-av {
            display: flex; align-items: center; justify-content: center;
            width: 2.2rem; height: 2.2rem; border-radius: 999px; background: #7c3aed; color: #fff; font-weight: 700; font-size: 0.95rem;
        }
        .sqg-sb-name { font-weight: 600; color: #0f172a; font-size: 0.9rem; }
        .sqg-sb-role { font-size: 0.8rem; color: #64748b; }
        .sqg-sb-gutter { min-height: 4rem; flex: 0 0 auto; }
        @media (min-height: 800px) { .sqg-sb-gutter { min-height: 7rem; } }
        .sqg-dash-topbar { display: flex; align-items: center; flex-wrap: wrap; gap: 0.5rem 1rem; width: 100%; margin: 0 0 0.2rem; }
        .sqg-dash-title h1 { margin: 0; font-size: 1.6rem; font-weight: 800; color: #0f172a !important; letter-spacing: -0.02em; }
        .sqg-dash-title { min-width: 0; }
        .sqg-dash-sub { margin: 0 0 0.9rem; color: #64748b !important; font-size: 0.92rem; line-height: 1.4; }
        .sqg-dash-hero p { margin: 0.2rem 0 0; color: #475569 !important; font-size: 0.95rem; }
        .sqg-kw { color: #5b21b6 !important; font-weight: 700; }
        .sqg-info-kw { color: #5b21b6 !important; font-weight: 600; }
        .sqg-dash-top-sep { border: none; border-top: 1px solid #e2e8f0; margin: 1.25rem 0; }
        .sqg-dash-info {
            display: flex; align-items: flex-start; gap: 0.65rem;
            background: #f5f3ff; border: 1px solid #e9d5ff; border-radius: 10px; padding: 0.9rem 1.1rem; margin: 0 0 1.1rem 0; color: #4c1d95; font-size: 0.92rem; line-height: 1.45;
        }
        .sqg-dash-info-ico { flex-shrink: 0; width: 1.25rem; text-align: center; font-size: 1.1rem; }
        .sqg-dash-metric {
            display: flex; align-items: flex-start; justify-content: space-between; gap: 0.6rem; flex-direction: row;
            background: #fafafa; border: 1px solid #e5e7eb; border-radius: 12px; padding: 0.95rem 0.9rem; box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        .sqg-dash-metric-body { flex: 1; min-width: 0; }
        .sqg-dash-metric-ico { width: 2.4rem; height: 2.4rem; border-radius: 8px; display: flex; align-items: center; justify-content: center; font-size: 1.1rem; flex-shrink: 0; }
        .sqg-dmi-grid { background: #ede9fe; }
        .sqg-dmi-pulse { background: #dcfce7; }
        .sqg-dmi-cal { background: #ffedd5; }
        .sqg-dmi-title { color: #64748b; font-size: 0.78rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; margin: 0 0 0.2rem; }
        .sqg-dmi-val { color: #0f172a; font-size: 1.45rem; font-weight: 800; margin: 0; line-height: 1.2; }
        .sqg-dmi-hint { color: #94a3b8; font-size: 0.8rem; margin: 0.25rem 0 0; }
        .sqg-dash-sec { font-size: 1.1rem; font-weight: 700; color: #0f172a; margin: 0.5rem 0 0.75rem; }
        p.sqg-dash-sec--row { margin: 0 !important; line-height: 1.2 !important; display: block !important; }
        .sqg-dash-myprow { display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 0.5rem; margin: 0.4rem 0 0.6rem; }
        .sqg-dash-tabpanel { min-height: 2rem; }
        .sqg-dash-empty {
            text-align: center; width: 100%; max-width: 28rem; margin: 0 auto; padding: 2rem 1.5rem 1.5rem; background: #fafafa; border: 1px solid #e5e7eb; border-radius: 12px; margin-top: 0.25rem;
            min-height: 240px; display: flex; flex-direction: column; align-items: center; justify-content: center; box-sizing: border-box;
        }
        .sqg-dash-empty-icowrap {
            display: inline-flex; width: 3.4rem; height: 3.4rem; border-radius: 10px; background: #ede9fe; align-items: center; justify-content: center; margin: 0 auto 0.75rem;
            border: 1px solid #ddd6fe;
        }
        .sqg-dash-empty-ico { font-size: 1.75rem; line-height: 1; }
        .sqg-dash-empty h3 { margin: 0.35rem 0 0.4rem; font-size: 1.15rem; color: #0f172a !important; font-weight: 800 !important; }
        .sqg-dash-empty p { margin: 0; color: #334155 !important; font-size: 0.95rem !important; line-height: 1.55 !important; max-width: 24rem; margin-left: auto; margin-right: auto; }
        .sqg-dash-empty p b, .sqg-dash-empty p strong { color: #1e1b4b !important; font-weight: 600; }
        .sqg-dash-empty-cta-gutter { min-height: 1.5rem; height: 1.5rem; box-sizing: border-box; }
        .sqg-dash-proj {
            background: #ffffff; border: 1px solid #e2e8f0; border-radius: 12px; padding: 0.9rem 1rem 0.5rem; margin: 0 0 0.6rem 0; box-shadow: 0 1px 2px rgba(0,0,0,0.04);
        }
        /* Project card action buttons: keep text visible always */
        section.main .sqg-dash-proj [data-baseweb="button"],
        section.main .sqg-dash-proj [data-testid^="stBaseButton"] {
            background: #f8fafc !important;
            border: 1px solid #cbd5e1 !important;
            color: #0f172a !important;
        }
        section.main .sqg-dash-proj [data-baseweb="button"]:hover,
        section.main .sqg-dash-proj [data-baseweb="button"]:focus,
        section.main .sqg-dash-proj [data-baseweb="button"]:active,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"]:hover,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"]:focus,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"]:active {
            background: #eef2f7 !important;
            color: #0f172a !important;
            border-color: #94a3b8 !important;
        }
        section.main .sqg-dash-proj [data-baseweb="button"] p,
        section.main .sqg-dash-proj [data-baseweb="button"] span,
        section.main .sqg-dash-proj [data-baseweb="button"] div,
        section.main .sqg-dash-proj [data-baseweb="button"] label,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"] p,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"] span,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"] div,
        section.main .sqg-dash-proj [data-testid^="stBaseButton"] label {
            color: #0f172a !important;
            opacity: 1 !important;
        }
        @media (max-width: 1024px) {
            [data-testid="stAppViewBlockContainer"] {
                padding: 0.35rem 0.85rem 1rem !important;
            }
            section.main [data-testid="stHorizontalBlock"] {
                flex-wrap: wrap !important;
                gap: 0.55rem !important;
            }
            section.main [data-testid="stHorizontalBlock"] > [data-testid="column"] {
                min-width: 220px !important;
                flex: 1 1 220px !important;
            }
            section.main [data-testid="stHorizontalBlock"] [data-baseweb="select"],
            section.main [data-testid="stHorizontalBlock"] [data-baseweb="button"],
            section.main [data-testid="stHorizontalBlock"] [data-testid="stPageLink-Nav"] a {
                width: 100% !important;
            }
        }
        @media (max-width: 768px) {
            [data-testid="stAppViewBlockContainer"] {
                padding: 0.2rem 0.65rem 0.85rem !important;
            }
            section.main {
                min-height: calc(100vh - 3.5rem) !important;
                padding: 0.45rem 0 0.7rem 0 !important;
            }
            section.main > div.block-container {
                padding: 0.85rem 0.8rem 1rem !important;
                border-radius: 10px !important;
            }
            section.main [data-testid="stHorizontalBlock"] > [data-testid="column"] {
                min-width: 100% !important;
                flex: 1 1 100% !important;
            }
            .sqg-dash-title h1 { font-size: 1.35rem !important; }
            .sqg-dash-sub { font-size: 0.86rem !important; }
            .sqg-dmi-val { font-size: 1.2rem !important; }
            .sqg-dash-proj { padding: 0.75rem 0.8rem 0.45rem !important; }
            .sqg-dash-empty { min-height: 180px; padding: 1.2rem 1rem 1rem; }
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
