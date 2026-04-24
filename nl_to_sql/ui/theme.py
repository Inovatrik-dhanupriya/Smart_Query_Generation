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
            /* Inputs: keep white field + dark readable text */
            [data-baseweb="input"] input,
            [data-baseweb="textarea"] textarea,
            [data-baseweb="select"] input {
                color: #0f172a !important;
                -webkit-text-fill-color: #0f172a !important;
            }
            [data-baseweb="input"] input::placeholder,
            [data-baseweb="textarea"] textarea::placeholder,
            [data-baseweb="select"] input::placeholder {
                color: #64748b !important;
                opacity: 1 !important;
            }
            [data-baseweb="input"],
            [data-baseweb="textarea"],
            [data-baseweb="select"] {
                background: #dbeafe !important;
                border-color: #334155 !important;
            }
            /* Sidebar buttons: readable contrast on dark panel */
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"],
            [data-testid="stSidebar"] [data-baseweb="button"][kind="primary"] {
                background: #2563eb !important;
                border: 1px solid #1d4ed8 !important;
            }
            [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"],
            [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] {
                background: #f8fafc !important;
                border: 1px solid #cbd5e1 !important;
            }
            [data-testid="stSidebar"] [data-baseweb="button"] *,
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"] *,
            [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] * {
                color: #0f172a !important;
                fill: #0f172a !important;
            }
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"] * {
                color: #dbeafe !important;
                fill: #dbeafe !important;
            }
            /* Hide Streamlit's auto-generated multipage list. */
            [data-testid="stSidebarNav"] {
                display: none;
            }
            .block-container {
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
        :root { --sqg-purple: #5b21b6; --sqg-purple-hover: #6d28d9; --sb-bg: #16151f; --sb-accent: #a78bfa; --sb-muted: #94a3b8; }
        [data-testid="stAppViewContainer"] .stApp { color: #0f172a !important; background: #f3f4f6 !important; }
        [data-testid="stAppViewBlockContainer"] { background: #f3f4f6 !important; padding: 0.5rem 1.5rem 1.5rem !important; box-sizing: border-box !important; }
        /* Top bar: match sidebar shell */
        [data-testid="stHeader"],
        [data-testid="stToolbar"],
        [data-testid="stDecoration"] {
            background: #f8fafc !important;
            border-bottom: 1px solid #e2e8f0 !important;
        }
        [data-testid="stHeader"] [data-baseweb="button"] { color: #1e293b !important; }
        /* Vertically center the card when the page is short; long pages grow normally (margin auto) */
        section.main {
            background: #f3f4f6 !important;
            min-height: calc(100vh - 4.25rem) !important;
            display: flex !important;
            flex-direction: column !important;
            box-sizing: border-box !important;
            padding: 0.5rem 0 1rem 0 !important;
        }
        section.main > div.block-container {
            max-width: 1120px !important; margin-left: auto !important; margin-right: auto !important; width: 100% !important;
            margin-top: 0 !important; margin-bottom: 0.75rem !important; flex: 0 0 auto !important;
            background: #dbeafe !important; border-radius: 12px !important; border: 1px solid #e5e7eb !important; padding: 1.75rem 1.35rem 1.5rem !important; box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
        }
        /* —— Dark SaaS sidebar: mono Material icons, purple active, readable hierarchy —— */
        [data-testid="stSidebar"] {
            background: var(--sb-bg) !important;
            border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
        }
        [data-testid="stSidebar"] [data-testid="stCaptionContainer"] p,
        [data-testid="stSidebar"] [data-testid="stCaption"] p,
        [data-testid="stSidebar"] [data-testid="stCaption"] {
            text-transform: uppercase !important;
            letter-spacing: 0.1em !important;
            font-size: 0.6rem !important;
            font-weight: 700 !important;
            color: #9ca3af !important;
            margin-top: 0.65rem !important;
            margin-bottom: 0.35rem !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink-Nav"] a {
            color: #d1d5db !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a {
            display: flex !important;
            align-items: center !important;
            gap: 0.7rem !important;
            min-height: 2.5rem !important;
            box-sizing: border-box !important;
            padding: 0.4rem 0.7rem 0.4rem 0.6rem !important;
            margin: 0.08rem 0 !important;
            border-radius: 8px !important;
            border-left: 3px solid transparent !important;
            background: transparent !important;
            text-decoration: none !important;
            transition: background 0.16s ease, color 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:not([aria-current="page"]) {
            color: #d1d5db !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a p {
            color: #d1d5db !important;
            font-size: 0.9rem !important;
            font-weight: 500 !important;
            margin: 0 !important;
            line-height: 1.3 !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:not([aria-current="page"]) svg {
            color: #b4bcc6 !important;
            fill: #b4bcc6 !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover {
            background: rgba(255, 255, 255, 0.06) !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover p,
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover span {
            color: #e2e8f0 !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a:hover svg {
            color: #cbd5e1 !important;
            fill: #cbd5e1 !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] {
            background: rgba(124, 58, 237, 0.22) !important;
            border-left-color: var(--sb-accent) !important;
            font-weight: 600 !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] p {
            color: #ffffff !important;
        }
        [data-testid="stSidebar"] [data-testid="stPageLink"] a[aria-current="page"] svg {
            color: #c4b5fd !important;
            fill: #c4b5fd !important;
        }
        [data-testid="stSidebar"] hr {
            border: none !important;
            border-top: 1px solid rgba(255, 255, 255, 0.1) !important;
            margin: 0.6rem 0 !important;
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
            background: var(--sqg-purple) !important; border-color: #4c1d95 !important; color: #dbeafe !important;
        }
        section.main button[kind="primary"] p, section.main button[kind="primary"] span { color: #dbeafe !important; }
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
        /* Sign out (secondary / outline): match dark panel — not plain white */
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"],
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] {
            background: rgba(255, 255, 255, 0.05) !important;
            color: #e2e8f0 !important;
            border: 1px solid rgba(255, 255, 255, 0.12) !important;
            border-radius: 8px !important;
            min-height: 2.4rem !important;
            font-weight: 500 !important;
            transition: background 0.16s ease, border-color 0.16s ease, color 0.16s ease !important;
        }
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] p,
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] span,
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] div,
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] label,
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] p,
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] span { color: #e2e8f0 !important; }
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] svg,
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"] path,
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"] svg { color: #94a3b8 !important; fill: #94a3b8 !important; }
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"]:hover,
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"]:hover {
            background: rgba(124, 58, 237, 0.15) !important;
            border-color: rgba(167, 139, 250, 0.45) !important;
        }
        [data-testid="stSidebar"] [data-baseweb="button"][kind="secondary"]:hover *,
        [data-testid="stSidebar"] [data-testid="stBaseButton-secondary"]:hover * { color: #f1f5f9 !important; fill: #e2e8f0 !important; }
        /* Other sidebar controls (e.g. file upload primary): keep legible on dark */
        [data-testid="stSidebar"] [data-baseweb="button"][kind="primary"] { border-radius: 8px !important; }
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
        /* Dashboard: darker captions (project cards use light blue bg; default grey was too faint) */
        section.main [data-testid="stCaption"] { color: #1e293b !important; }
        section.main [data-testid="stCaption"] code { color: #0f172a !important; background: #f1f5f9 !important; }

        [data-testid="stSidebar"] .sqg-sb-top,
        [data-testid="stSidebar"] .sqg-sb-head {
            display: flex; align-items: center; gap: 0.5rem;
            margin-bottom: 1.5rem !important;
        }
        [data-testid="stSidebar"] .sqg-sb-brand {
            font-weight: 800; font-size: 1.02rem; color: #f8fafc !important; letter-spacing: -0.02em; margin: 0 !important;
        }
        [data-testid="stSidebar"] .sqg-sb-mute { color: #6b7280; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.08em; }
        [data-testid="stSidebar"] .sqg-sb-user {
            display: flex; align-items: center; gap: 0.75rem;
            padding: 0.75rem 0.7rem; margin: 0 0 0.85rem;
            background: rgba(255, 255, 255, 0.06) !important;
            border-radius: 10px; border: 1px solid rgba(255, 255, 255, 0.1);
        }
        [data-testid="stSidebar"] .sqg-sb-av {
            display: flex; align-items: center; justify-content: center; flex-shrink: 0;
            width: 2.4rem; height: 2.4rem; border-radius: 999px;
            background: linear-gradient(135deg, #6d28d9, #5b21b6); color: #fff;
            font-weight: 700; font-size: 0.95rem; letter-spacing: 0;
        }
        [data-testid="stSidebar"] .sqg-sb-name {
            font-weight: 600; color: #f4f4f5 !important; font-size: 0.95rem !important; line-height: 1.25;
        }
        [data-testid="stSidebar"] .sqg-sb-role {
            font-size: 0.8rem; color: #a1a1aa !important; margin-top: 0.12rem; line-height: 1.2;
        }
        .sqg-sb-gutter { min-height: 4rem; flex: 0 0 auto; }
        @media (min-height: 800px) { .sqg-sb-gutter { min-height: 7rem; } }
        .sqg-dash-topbar { display: flex; align-items: center; flex-wrap: wrap; gap: 0.5rem 1rem; width: 100%; margin: 0 0 0.2rem; }
        .sqg-dash-title h1 { margin: 0; font-size: 1.6rem; font-weight: 800; color: #0f172a !important; letter-spacing: -0.02em; }
        /* Standard in-card header band: title + toolbar never flush to the card top */
        .sqg-dash-title { min-width: 0; margin: 0 0 0.15rem; padding-top: 0.15rem; }
        .sqg-dash-sub { margin: 0 0 0.9rem; color: #334155 !important; font-size: 0.92rem; line-height: 1.4; }
        .sqg-dash-hero p { margin: 0.2rem 0 0; color: #475569 !important; font-size: 0.95rem; }
        .sqg-kw { color: #5b21b6 !important; font-weight: 700; }
        .sqg-info-kw { color: #5b21b6 !important; font-weight: 600; }
        .sqg-dash-top-sep { border: none; border-top: 1px solid #e2e8f0; margin: 1.25rem 0; }
        .sqg-dash-info {
            display: flex; align-items: flex-start; gap: 0.65rem;
            background: #f5f3ff; border: 1px solid #e9d5ff; border-radius: 10px; padding: 0.9rem 1.1rem; margin: 0 0 1.1rem 0; color: #1e1b4b; font-size: 0.92rem; line-height: 1.45;
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
        .sqg-dmi-title { color: #334155; font-size: 0.78rem; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; margin: 0 0 0.2rem; }
        .sqg-dmi-val { color: #0f172a; font-size: 1.45rem; font-weight: 800; margin: 0; line-height: 1.2; }
        .sqg-dmi-hint { color: #475569; font-size: 0.8rem; margin: 0.25rem 0 0; }
        .sqg-dash-sec { font-size: 1.1rem; font-weight: 700; color: #0f172a; margin: 0.5rem 0 0.75rem; }
        p.sqg-dash-sec--row { margin: 0 !important; line-height: 1.2 !important; display: block !important; }
        /* My projects — card body lines (Company, description, Status, Updated): dark labels */
        p.sqg-dash-proj-line,
        section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] p.sqg-dash-proj-line {
            color: #0f172a !important;
            -webkit-text-fill-color: #0f172a !important;
            font-size: 0.9rem !important;
            line-height: 1.45 !important;
            margin: 0.1rem 0 0.4rem 0 !important;
        }
        p.sqg-dash-proj-line code {
            color: #0f172a !important;
            -webkit-text-fill-color: #0f172a !important;
            background: rgba(15, 23, 42, 0.07) !important;
            padding: 0.12rem 0.4rem !important;
            border-radius: 4px !important;
            font-size: 0.88em !important;
        }
        p.sqg-dash-proj-line strong { color: #0f172a !important; }
        /* Legacy: captions if any remain inside project card */
        section.main .sqg-dash-proj [data-testid="stCaption"],
        section.main .sqg-dash-proj [data-testid="stCaption"] p,
        section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] p:not(.sqg-dash-proj-line),
        section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] span { color: #0f172a !important; }
        section.main .sqg-dash-proj [data-testid="stMarkdownContainer"] strong { color: #0f172a !important; }
        section.main [data-baseweb="notification"] [data-testid="stMarkdownContainer"] p,
        section.main [data-testid="stAlert"] [data-testid="stMarkdownContainer"] p { color: #0f172a !important; }
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
            background: #dbeafe; border: 1px solid #e2e8f0; border-radius: 12px; padding: 0.9rem 1rem 0.5rem; margin: 0 0 0.6rem 0; box-shadow: 0 1px 2px rgba(0,0,0,0.04);
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


def apply_tenant_page_shell() -> None:
    """
    Main-area shell to match the Projects (Tenant Dashboard) page: off-white #f8f7ff
    content band, purple top edge, display typography, callout cards, and form cards
    (same tokens as pages/dashboard.py). Call after :func:`apply_dashboard_theme`.
    """
    st.markdown(
        """
        <style>
        :root { --sqg-accent: #7c3aed; --sqg-accent-2: #6d28d9; --sqg-ink: #1e1b2e; --sqg-body: #4b5563; --sqg-muted: #9ca3af; }
        /* Do not set color on .stApp — it inherits into the dark sidebar and hides labels/captions. */
        [data-testid="stAppViewContainer"] .stApp { background: #f8f7ff !important; }
        [data-testid="stAppViewBlockContainer"] { background: #f8f7ff !important; }
        section.main { background: #f8f7ff !important; color: var(--sqg-ink) !important; }
        section.main > div.block-container {
            background: transparent !important;
            border: none !important;
            border-top: 3px solid var(--sqg-accent) !important;
            border-radius: 0 !important;
            box-shadow: none !important;
        }
        [data-testid="stHeader"],
        [data-testid="stToolbar"] {
            background: #f8fafc !important;
            border-bottom: 1px solid #e2e8f0 !important;
        }
        section.main .sqg-dash-title h1 {
            color: var(--sqg-ink) !important;
            font-size: 2.7rem !important;
            font-weight: 900 !important;
            line-height: 1.05 !important;
            letter-spacing: -0.02em !important;
        }
        section.main .sqg-dash-sub {
            color: #6b7280 !important;
            font-size: 1.02rem !important;
            font-weight: 400 !important;
            line-height: 1.55 !important;
            margin: 0.12rem 0 1.05rem !important;
        }
        .sqg-dash-info {
            display: flex !important;
            align-items: flex-start !important;
            gap: 0.65rem !important;
            background: linear-gradient(135deg, #ede9fe, #f5f3ff) !important;
            border: 1px solid #c4b5fd !important;
            border-left: 4px solid var(--sqg-accent) !important;
            border-radius: 10px !important;
            color: #4b5563 !important;
            font-size: 0.92rem !important;
            line-height: 1.45 !important;
            padding: 0.9rem 1.1rem !important;
            margin: 0 0 1.1rem 0 !important;
            box-sizing: border-box !important;
        }
        .sqg-dash-info-ico {
            flex-shrink: 0;
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
            width: 1.4rem !important;
            height: 1.4rem !important;
            border-radius: 999px !important;
            background: #ede9fe !important;
            color: var(--sqg-accent) !important;
            font-size: 0.72rem !important;
            font-weight: 800 !important;
            font-style: normal !important;
            line-height: 1 !important;
            margin-top: 0.08rem !important;
        }
        /* Card surface — match Projects / dashboard overrides (not theme default blue) */
        section.main .sqg-dash-proj {
            background: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-left: 4px solid var(--sqg-accent) !important;
            border-radius: 12px !important;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
            padding: 1.1rem 1.15rem 1.05rem !important;
            margin: 0 0 1rem 0 !important;
            box-sizing: border-box !important;
            transition: box-shadow 0.18s ease, transform 0.18s ease !important;
        }
        section.main .sqg-dash-proj:hover {
            box-shadow: 0 8px 24px rgba(124, 58, 237, 0.12) !important;
        }
        /* Streamlit form = same card (Companies add form, etc.) */
        section.main [data-testid="stForm"] {
            background: #ffffff !important;
            border: 1px solid #e5e7eb !important;
            border-left: 4px solid var(--sqg-accent) !important;
            border-radius: 12px !important;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
            padding: 1.1rem 1.15rem 1.1rem !important;
            margin: 0 0 1rem 0 !important;
            box-sizing: border-box !important;
        }
        section.main [data-testid="stForm"] [data-testid="stWidgetLabel"] p,
        section.main [data-testid="stForm"] [data-testid="stWidgetLabel"] label {
            color: #374151 !important;
            font-weight: 500 !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="input"],
        section.main [data-testid="stForm"] [data-baseweb="textarea"] {
            background: #ffffff !important;
            border: 1.5px solid #e5e7eb !important;
            border-radius: 8px !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="input"] input,
        section.main [data-testid="stForm"] [data-baseweb="textarea"] textarea {
            color: var(--sqg-ink) !important;
            -webkit-text-fill-color: var(--sqg-ink) !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="input"]:focus-within,
        section.main [data-testid="stForm"] [data-baseweb="textarea"]:focus-within { border-color: var(--sqg-accent) !important; }
        section.main [data-testid="stForm"] [data-baseweb="select"] {
            background: #ffffff !important;
            border: 1.5px solid #e5e7eb !important;
            border-radius: 8px !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="select"] [data-baseweb="base-input"],
        section.main [data-testid="stForm"] [data-baseweb="select"] input {
            color: var(--sqg-ink) !important;
            -webkit-text-fill-color: var(--sqg-ink) !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="select"]:focus-within { border-color: var(--sqg-accent) !important; }
        section.main [data-testid="stForm"] [data-baseweb="button"][kind="primary"],
        section.main [data-testid="stForm"] [data-testid="stFormSubmitButton"] button,
        section.main [data-testid="stForm"] [data-testid^="stBaseButton-primary"] {
            background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important;
            color: #ffffff !important;
            font-weight: 600 !important;
            border-radius: 8px !important;
            border: none !important;
            box-shadow: 0 4px 14px rgba(124, 58, 237, 0.35) !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="button"][kind="primary"] *,
        section.main [data-testid="stForm"] [data-testid="stFormSubmitButton"] button *,
        section.main [data-testid="stForm"] [data-testid^="stBaseButton-primary"] * {
            color: #ffffff !important;
        }
        section.main [data-testid="stForm"] [data-baseweb="button"][kind="primary"]:hover {
            filter: brightness(1.04) !important;
            box-shadow: 0 6px 18px rgba(124, 58, 237, 0.42) !important;
        }
        section.main p, section.main [data-testid="stMarkdownContainer"] p { color: #4b5563 !important; }
        section.main [data-testid="stCaption"] p, section.main [data-testid="stCaption"] { color: var(--sqg-muted) !important; }
        section.main [data-baseweb="notification"] [data-testid="stMarkdownContainer"] p,
        section.main [data-testid="stAlert"] [data-testid="stMarkdownContainer"] p { color: #0f172a !important; }
        /* Read-only key/value in white project cards (Open Project, etc.) */
        section.main .sqg-dash-proj dl.sqg-kv { margin: 0 !important; }
        section.main .sqg-dash-proj .sqg-kv dt {
            font-size: 0.7rem !important; text-transform: uppercase !important; letter-spacing: 0.08em !important;
            font-weight: 600 !important; color: #6b7280 !important; margin: 0.85rem 0 0.25rem !important; line-height: 1.2 !important;
        }
        section.main .sqg-dash-proj .sqg-kv dt:first-of-type { margin-top: 0 !important; }
        section.main .sqg-dash-proj .sqg-kv dd {
            margin: 0 !important; color: #1e1b2e !important; font-size: 0.95rem !important; font-weight: 500 !important;
            line-height: 1.45 !important; word-wrap: break-word !important;
        }
        /* CTA row: primary = gradient, secondary = outline (matches Projects toolbar) */
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="primary"],
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-testid^="stBaseButton-primary"] {
            background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important;
            color: #ffffff !important; font-weight: 600 !important; border-radius: 8px !important; border: none !important;
            box-shadow: 0 4px 14px rgba(124, 58, 237, 0.35) !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="primary"] *,
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-testid^="stBaseButton-primary"] * { color: #ffffff !important; }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="primary"]:hover {
            filter: brightness(1.04) !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="secondary"] {
            background: #ffffff !important; border: 1.5px solid #d1d5db !important; color: #4b5563 !important; font-weight: 600 !important;
            border-radius: 8px !important; box-shadow: 0 1px 2px rgba(15, 23, 42, 0.05) !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="secondary"] * { color: #4b5563 !important; }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="secondary"]:hover {
            background: #f5f3ff !important; border-color: var(--sqg-accent) !important; color: #5b21b6 !important;
        }
        section.main [data-testid="stVerticalBlock"] > [data-testid="stHorizontalBlock"] [data-baseweb="button"][kind="secondary"]:hover * { color: #5b21b6 !important; }
        section.main [data-testid="stExpander"] {
            background: #ffffff !important; border: 1px solid #e5e7eb !important; border-left: 4px solid var(--sqg-accent) !important;
            border-radius: 12px !important; box-shadow: 0 2px 8px rgba(0,0,0,0.04) !important; margin-top: 0.5rem !important;
        }
        section.main [data-testid="stExpander"] details { background: #faf7ff !important; border: none !important; }
        @media (max-width: 768px) {
            section.main .sqg-dash-title h1 { font-size: 1.75rem !important; }
            section.main .sqg-dash-sub { font-size: 0.92rem !important; }
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


def apply_chat_page_theme() -> None:
    """
    Chat-specific UI on top of the Tenant shell (:func:`apply_dashboard_theme` + :func:`apply_tenant_page_shell`).
    Does not override app / sidebar shell — only messages, input, cards, and data grid on the main pane.
    """
    st.markdown(
        """
        <style>
            :root {
                --sqg-accent: #7c3aed;
                --sqg-accent-2: #6d28d9;
                --sqg-ink: #1e1b2e;
            }
            /* Main: comfortable reading width (shell already sets #f8f7ff) */
            section.main .block-container { max-width: 1200px !important; }
            .sqg-chat-hero-brief {
                margin: 0 0 0.85rem 0; padding: 0 0 0.65rem 0; max-width: 960px;
                border-bottom: 1px solid #e5e7eb;
            }

            /* Chat sidebar: section labels (match Configuration cfg-sb-sec) */
            [data-testid="stSidebar"] .sqg-chat-sec {
                color: #c4b5fd !important; -webkit-text-fill-color: #c4b5fd !important;
                text-transform: uppercase; font-size: 0.6rem !important; letter-spacing: 0.1em; font-weight: 700;
                margin: 0.65rem 0 0.35rem 0;
            }
            [data-testid="stSidebar"] [data-testid="stSubheader"] { color: #e2e8f0 !important; }
            .sqg-chat-ctx { display: flex; flex-wrap: wrap; gap: 0.4rem; margin: 0.2rem 0 0.6rem; }
            .sqg-chat-chip {
                display: inline-block; font-size: 0.75rem; font-weight: 600;
                color: #f1f5f9 !important;
                background: rgba(255, 255, 255, 0.1) !important;
                border: 1px solid rgba(255, 255, 255, 0.16) !important;
                border-radius: 9999px; padding: 0.16rem 0.5rem; line-height: 1.2;
            }
            .sqg-chat-chip--on {
                border-color: #a78bfa !important; color: #f5f3ff !important;
                box-shadow: 0 0 0 1px rgba(124, 58, 237, 0.35);
            }
            [data-testid="stChatMessage"] { background: transparent !important; }
            [data-testid="stChatMessage"] [data-testid="stVerticalBlock"] { border-radius: 12px; }
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p,
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] li {
                color: #1e1b2e !important; -webkit-text-fill-color: #1e1b2e !important;
            }
            [data-testid="stChatInputContainer"] {
                background: transparent !important;
                border-top: 1px solid #e5e7eb !important;
                padding-top: 0.5rem !important;
            }
            [data-testid="stChatInput"] {
                background: #ffffff !important;
                border: 1.5px solid #e5e7eb !important;
                border-radius: 12px !important;
                box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            }
            [data-testid="stChatInput"]:focus-within { border-color: var(--sqg-accent) !important; }
            [data-testid="stChatInput"] textarea {
                color: #1e1b2e !important; -webkit-text-fill-color: #1e1b2e !important;
            }
            /* Starter suggestion buttons: tenant secondary on light */
            section.main [data-baseweb="button"][kind="secondary"] {
                background: #ffffff !important; border: 1.5px solid #e5e7eb !important; color: #4b5563 !important;
                border-radius: 8px !important; font-weight: 500 !important;
            }
            section.main [data-baseweb="button"][kind="secondary"] p,
            section.main [data-baseweb="button"][kind="secondary"] span { color: #4b5563 !important; }
            section.main [data-baseweb="button"][kind="secondary"]:hover {
                background: #f5f3ff !important; border-color: var(--sqg-accent) !important; color: #5b21b6 !important;
            }
            section.main [data-baseweb="button"][kind="primary"] {
                background: linear-gradient(135deg, var(--sqg-accent), var(--sqg-accent-2)) !important;
                border: none !important; border-radius: 8px !important;
                box-shadow: 0 4px 14px rgba(124, 58, 237, 0.35) !important;
            }
            section.main [data-baseweb="button"][kind="primary"] p,
            section.main [data-baseweb="button"][kind="primary"] span { color: #ffffff !important; }
            .sqg-chat-used { display: flex; flex-wrap: wrap; align-items: center; gap: 0.35rem; margin: 0.4rem 0 0.25rem; }
            .sqg-chat-used-label { color: #6d28d9 !important; font-size: 0.78rem; font-weight: 700; margin-right: 0.2rem; }
            .sqg-chat-used-chip {
                display: inline-block; padding: 0.1rem 0.45rem; border-radius: 999px;
                border: 1px solid #ddd6fe; background: #faf7ff; color: #5b21b6; font-size: 0.75rem; line-height: 1.25;
            }
            section.main [data-testid="stExpander"] {
                background: #ffffff !important; border: 1px solid #e5e7eb !important;
                border-left: 4px solid var(--sqg-accent) !important; border-radius: 12px !important;
            }
            section.main [data-testid="stExpander"] details { background: #faf7ff !important; }
            /* Result table: light grid (Tenant / dashboard tables) */
            section.main [data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
                border: 1px solid #e5e7eb !important; border-radius: 10px !important; background: #ffffff !important;
            }
            section.main .stDataFrameGlideDataEditor {
                --gdg-text-dark: #1e1b2e !important;
                --gdg-text-medium: #4b5563 !important;
                --gdg-text-light: #6b7280 !important;
                --gdg-text-bubble: #4b5563 !important;
                --gdg-bg-icon-header: #e5e7eb !important;
                --gdg-fg-icon-header: #1e1b2e !important;
                --gdg-text-header: #374151 !important;
                --gdg-text-group-header: #4b5563 !important;
                --gdg-text-header-selected: #1e1b2e !important;
                --gdg-bg-cell: #ffffff !important;
                --gdg-bg-cell-medium: #f9fafb !important;
                --gdg-bg-header: #f3f4f6 !important;
                --gdg-bg-header-has-focus: #e5e7eb !important;
                --gdg-bg-header-hovered: #e5e7eb !important;
                --gdg-bg-bubble: #f3f4f6 !important;
                --gdg-bg-bubble-selected: #ede9fe !important;
                --gdg-bg-search-result: rgba(124, 58, 237, 0.12) !important;
                --gdg-border-color: #e5e7eb !important;
                --gdg-horizontal-border-color: #e5e7eb !important;
                --gdg-drilldown-border: #d1d5db !important;
                --gdg-link-color: #7c3aed !important;
            }
            section.main [data-testid="stDataFrame"] canvas { background: #ffffff !important; }
            /* Sticky chat footer in dark sidebar: blend with #16151f */
            [data-testid="stSidebar"] .sqg-chat-footer-fixed {
                position: sticky; bottom: 0; z-index: 3; padding-top: 0.45rem; margin-top: 0.35rem;
                background: linear-gradient(
                    to top,
                    #16151f 62%,
                    rgba(22, 21, 31, 0.92) 85%,
                    rgba(22, 21, 31, 0) 100%
                );
            }
            [data-testid="stSidebar"] .sqg-sb-foot {
                border-top: 1px solid rgba(255, 255, 255, 0.1); padding: 0.75rem 0.15rem 0.35rem; margin-top: 0.25rem;
            }
            [data-testid="stSidebar"] .sqg-sb-foot-row { display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.5rem; }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-av {
                display: inline-flex; width: 2.1rem; height: 2.1rem; border-radius: 999px; align-items: center; justify-content: center;
                font-size: 0.9rem; font-weight: 700; color: #fff; flex-shrink: 0; background: #5b21b6 !important;
            }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-name { color: #f3f4f6 !important; font-size: 0.9rem; font-weight: 600; }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-role { color: #9ca3af !important; font-size: 0.75rem; margin-top: 0.1rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
