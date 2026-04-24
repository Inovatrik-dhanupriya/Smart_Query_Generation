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
        :root { --sqg-purple: #5b21b6; --sqg-purple-hover: #6d28d9; }
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
        /* Dashboard: darker captions (project cards use light blue bg; default grey was too faint) */
        section.main [data-testid="stCaption"] { color: #1e293b !important; }
        section.main [data-testid="stCaption"] code { color: #0f172a !important; background: #f1f5f9 !important; }

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


def render_page_header(title: str, subtitle: str = "") -> None:
    # Match the heading style used in streamlit_app.py (`st.title` + `st.caption`).
    st.title(title)
    if subtitle:
        st.caption(subtitle)


def apply_chat_page_theme() -> None:
    """
    Dark Chat layout (product mock): charcoal shell, blue accents, card-style suggested prompts.
    Call only on the project Chat page (after :func:`apply_shared_theme`) so other pages are unchanged.
    """
    st.markdown(
        """
        <style>
            :root {
                --sqg-chat-bg: #0d0e12;
                --sqg-chat-elev: #16171d;
                --sqg-chat-border: #2a2d36;
                --sqg-chat-muted: #949494;
                --sqg-chat-text: #f4f4f5;
                --sqg-chat-blue: #4d77ff;
            }
            [data-testid="stAppViewContainer"] .stApp,
            .stApp { background: var(--sqg-chat-bg) !important; color: var(--sqg-chat-text) !important; }
            section.main { background: var(--sqg-chat-bg) !important; }
            section.main .block-container {
                max-width: 1200px !important;
                padding: 0.5rem 1.25rem 2rem 1.25rem !important;
            }
            [data-testid="stHeader"] { background: var(--sqg-chat-bg) !important; }
            [data-testid="stSidebar"] {
                background: var(--sqg-chat-elev) !important;
                border-right: 1px solid var(--sqg-chat-border) !important;
            }
            [data-testid="stSidebar"] .stMarkdown, [data-testid="stSidebar"] label, [data-testid="stSidebar"] p, [data-testid="stSidebar"] span { color: #c4c4c8 !important; }
            [data-testid="stSidebar"] [data-testid="stCaption"] { color: var(--sqg-chat-muted) !important; text-transform: uppercase; letter-spacing: 0.08em; font-size: 0.65rem !important; }
            [data-testid="stSidebar"] a[aria-current="page"] {
                background: rgba(77, 119, 255, 0.2) !important; border-radius: 10px; font-weight: 600; color: var(--sqg-chat-blue) !important;
            }
            [data-testid="stSidebar"] [data-testid="stPageLink-Nav"] a, [data-testid="stSidebar"] a[href] {
                color: #e4e4e7 !important; text-decoration: none; border-radius: 10px; padding: 0.4rem 0.5rem;
            }
            [data-testid="stSidebar"] [data-baseweb="button"] { border-radius: 10px !important; }
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"] { background: var(--sqg-chat-blue) !important; border: none !important; }
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"] p,
            [data-testid="stSidebar"] [data-testid="stBaseButton-primary"] span { color: #fff !important; }
            .sqg-chat-brand-line {
                font-size: 1.05rem; font-weight: 800; margin: 0.15rem 0 0.1rem;
                background: linear-gradient(90deg, #4d77ff, #8b5cf6);
                -webkit-background-clip: text; background-clip: text; color: transparent;
            }
            .sqg-chat-sub { color: #a1a1a6; font-size: 0.72rem; text-transform: lowercase; }
            .sqg-chat-ctx {
                display: flex; flex-wrap: wrap; gap: 0.4rem; margin: 0.2rem 0 0.6rem;
            }
            .sqg-chat-chip {
                display: inline-block; font-size: 0.8rem; font-weight: 500;
                color: #e4e4e7; border: 1px solid #3f424d; background: #1e1f24;
                border-radius: 9999px; padding: 0.2rem 0.6rem; line-height: 1.2;
            }
            .sqg-chat-chip--on {
                border-color: #22c55e; color: #bbf7d0; box-shadow: 0 0 0 1px rgba(34, 197, 94, 0.35);
            }
            .sqg-chat-sec { color: #8b8b90 !important; text-transform: uppercase; font-size: 0.6rem; letter-spacing: 0.1em; font-weight: 700; margin: 0.6rem 0 0.3rem; }
            .sqg-chat-recent {
                list-style: none; padding: 0; margin: 0.2rem 0 0.5rem; font-size: 0.78rem; font-family: ui-monospace, Consolas, monospace; color: #a1a1a6; line-height: 1.35;
            }
            .sqg-chat-recent li { margin: 0.25rem 0; padding: 0.2rem 0.35rem; border-radius: 6px; word-break: break-all; }
            .sqg-chat-recent .active { color: #e4e4e7; background: #262830; border-left: 2px solid var(--sqg-chat-blue); }
            .sqg-chat-hero {
                text-align: center; padding: 1.2rem 0.5rem 0.2rem; margin-bottom: 0.25rem;
            }
            .sqg-chat-hero-ico { font-size: 1.75rem; margin-bottom: 0.35rem; opacity: 0.9; }
            .sqg-chat-hero h2 { font-size: 1.4rem; font-weight: 700; color: #fafafa; margin: 0 0 0.35rem; }
            .sqg-chat-hero h2 .sqg-kw { color: var(--sqg-chat-blue) !important; }
            .sqg-chat-hero p { color: #949494; font-size: 0.9rem; margin: 0; }
            .sqg-chat-head { margin: 0 0 0.1rem; font-size: 1.5rem; font-weight: 800; color: #fafafa; letter-spacing: -0.02em; }
            .sqg-chat-headline { color: #a1a1a6; font-size: 0.95rem; margin: 0 0 0.2rem; }
            .sqg-chat-hero-brief {
                margin: 0 0 0.5rem; padding: 0 0 0.75rem; border-bottom: 1px solid #2a2d36; max-width: 920px;
            }
            section.main [data-testid="stCaptionContainer"] p { color: #aeb8cc !important; }
            .sqg-chat-toolbar { display: flex; align-items: center; justify-content: flex-end; gap: 0.35rem; }
            .sqg-chat-sugbar {
                text-align: center; color: #5c5c62; font-size: 0.6rem; letter-spacing: 0.16em; font-weight: 600;
                margin: 1.25rem 0 0.6rem; text-transform: uppercase;
            }
            .sqg-chat-sugbar::before, .sqg-chat-sugbar::after {
                content: ""; display: inline-block; width: 32%; max-width: 10rem; height: 1px; background: #2a2d36; vertical-align: middle; margin: 0 0.5rem;
            }
            .sqg-sg-card {
                background: var(--sqg-chat-elev); border: 1px solid var(--sqg-chat-border);
                border-radius: 10px; padding: 0.65rem 0.7rem; margin: 0; min-height: 4.5rem; text-align: left;
            }
            .sqg-sg-ico { font-size: 1.1rem; margin-bottom: 0.2rem; }
            .sqg-sg-ttl { color: #fafafa; font-size: 0.88rem; font-weight: 600; margin: 0 0 0.2rem; }
            .sqg-sg-txt { color: #9ca3af; font-size: 0.78rem; line-height: 1.3; }
            .sqg-chat-used {
                display: flex;
                flex-wrap: wrap;
                align-items: center;
                gap: 0.35rem;
                margin: 0.35rem 0 0.2rem;
            }
            .sqg-chat-used-label {
                color: #93c5fd;
                font-size: 0.78rem;
                font-weight: 700;
                letter-spacing: 0.01em;
                margin-right: 0.2rem;
            }
            .sqg-chat-used-chip {
                display: inline-block;
                padding: 0.1rem 0.45rem;
                border-radius: 999px;
                border: 1px solid #334155;
                background: #111827;
                color: #dbeafe;
                font-size: 0.75rem;
                line-height: 1.25;
            }
            [data-testid="stChatMessage"] { background: transparent !important; }
            [data-testid="stChatMessage"] [data-testid="stVerticalBlock"] { border-radius: 10px; }
            [data-testid="stChatMessage"] [data-testid="stMarkdownContainer"] p { color: #e4e4e7 !important; }
            /* Chat input: remove white footer strip + use white input with dark text */
            [data-testid="stChatInputContainer"] {
                background: transparent !important;
                border-top: 1px solid #2a2d36 !important;
            }
            [data-testid="stChatInput"] {
                background: #ffffff !important;
                border: 1px solid #cbd5e1 !important;
                border-radius: 12px !important;
            }
            [data-testid="stChatInput"] [data-baseweb="textarea"] {
                background: #ffffff !important;
                border: none !important;
            }
            [data-testid="stChatInput"] textarea {
                background: #ffffff !important;
                color: #0f172a !important;
                -webkit-text-fill-color: #0f172a !important;
                caret-color: #0f172a !important;
            }
            [data-testid="stChatInput"] textarea::placeholder { color: #64748b !important; }
            section.main [data-testid="stBaseButton-secondary"] {
                background: #161a24 !important;
                border: 1px solid #334155 !important;
                border-radius: 8px !important;
            }
            section.main [data-testid="stBaseButton-secondary"] p,
            section.main [data-testid="stBaseButton-secondary"] span,
            section.main [data-testid="stBaseButton-secondary"] div,
            section.main [data-testid="stBaseButton-secondary"] label {
                color: #dbe7ff !important;
            }
            [data-testid="stBaseButton-primary"] { background: var(--sqg-chat-blue) !important; border: none !important; border-radius: 8px !important; }
            .sqg-chat-foot { color: #5c5c62; font-size: 0.75rem; margin: 0.5rem 0 0.25rem; }
            [data-testid="stExpander"] { background: #16171d !important; border: 1px solid #2a2d36; border-radius: 10px; }
            section.main [data-testid="stMarkdownContainer"] a { color: #93b4ff; }
            /* Chat dataframe: dark grid (Streamlit Glide Data Editor) */
            section.main [data-testid="stDataFrame"] [data-testid="stDataFrameResizable"] {
                border: 1px solid #2a2d36 !important;
                background: #111827 !important;
            }
            section.main .stDataFrameGlideDataEditor {
                --gdg-text-dark: #e5e7eb !important;
                --gdg-text-medium: #cbd5e1 !important;
                --gdg-text-light: #94a3b8 !important;
                --gdg-text-bubble: #cbd5e1 !important;
                --gdg-bg-icon-header: #475569 !important;
                --gdg-fg-icon-header: #e5e7eb !important;
                --gdg-text-header: #cbd5e1 !important;
                --gdg-text-group-header: #cbd5e1 !important;
                --gdg-text-header-selected: #ffffff !important;
                --gdg-bg-cell: #111827 !important;
                --gdg-bg-cell-medium: #0f172a !important;
                --gdg-bg-header: #1f2937 !important;
                --gdg-bg-header-has-focus: #334155 !important;
                --gdg-bg-header-hovered: #334155 !important;
                --gdg-bg-bubble: #1e293b !important;
                --gdg-bg-bubble-selected: #334155 !important;
                --gdg-bg-search-result: rgba(77, 119, 255, 0.22) !important;
                --gdg-border-color: #334155 !important;
                --gdg-horizontal-border-color: #334155 !important;
                --gdg-drilldown-border: #475569 !important;
                --gdg-link-color: #93c5fd !important;
            }
            section.main [data-testid="stDataFrame"] canvas {
                background: #111827 !important;
            }
            /* Chat sidebar footer: keep admin/actions pinned while recent list scrolls */
            [data-testid="stSidebar"] .sqg-chat-footer-fixed {
                position: sticky;
                bottom: 0;
                z-index: 3;
                padding-top: 0.45rem;
                margin-top: 0.35rem;
                background: linear-gradient(
                    to top,
                    rgba(11, 12, 16, 0.98) 70%,
                    rgba(11, 12, 16, 0.86) 88%,
                    rgba(11, 12, 16, 0)
                );
            }
            [data-testid="stSidebar"] .sqg-sb-foot {
                border-top: 1px solid #2a2d36; padding: 0.75rem 0.25rem 0.4rem; margin-top: 0.25rem;
            }
            [data-testid="stSidebar"] .sqg-sb-foot-row {
                display: flex; align-items: center; gap: 0.6rem; margin-bottom: 0.65rem;
            }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-av {
                display: inline-flex; width: 2.1rem; height: 2.1rem; border-radius: 999px;
                align-items: center; justify-content: center; font-size: 0.9rem; font-weight: 700; color: #fff; flex-shrink: 0;
            }
            [data-testid="stSidebar"] .sqg-sb-foot-text { display: flex; flex-direction: column; min-width: 0; line-height: 1.2; }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-name { display: block; color: #f4f4f5; font-size: 0.92rem; font-weight: 600; }
            [data-testid="stSidebar"] .sqg-sb-foot .sqg-sb-role { display: block; color: #8b8b90; font-size: 0.75rem; margin-top: 0.1rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )
