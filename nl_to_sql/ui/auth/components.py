"""Auth screens — QueryBase-style split: dark form / purple marketing, optional reversed for sign-up."""

from __future__ import annotations

import html
from typing import Callable, Literal

import streamlit as st

from ui.theme import apply_shared_theme

AuthVariant = Literal["signin", "signup"]


def _body_class_inject(variant: AuthVariant) -> None:
    cls = "auth-sqg-landing auth-sqg-" + variant
    st.markdown(
        f"""
        <script>
        (function() {{
          document.body.classList.add("auth-sqg-landing", "auth-sqg-{variant}");
        }})();
        </script>
        """,
        unsafe_allow_html=True,
    )


def render_auth_styles() -> None:
    st.markdown(
        """
        <style>
          /* Full-page: kill default Streamlit page gradient on auth */
          body.auth-sqg-landing .stApp {
            background: #050507 !important;
            color: #e2e8f0;
          }
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"] {
            padding: 0 0 0.5rem 0 !important;
            min-height: calc(100vh - 3rem) !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            box-sizing: border-box;
            background: #050507 !important;
          }
          body.auth-sqg-landing section.main > div.block-container {
            max-width: 100% !important;
            width: 100% !important;
            padding: 0 0.5rem 1rem 0.5rem !important;
            margin: 0 !important;
            flex: 0 0 auto;
          }
          body.auth-sqg-landing [data-testid="collapsedControl"] { display: none !important; }

          body.auth-sqg-landing div[data-testid="stHorizontalBlock"] {
            align-items: stretch !important;
            width: 100% !important;
            max-width: 1200px;
            margin: 0 auto !important;
            gap: 0 !important;
            min-height: min(90vh, 900px) !important;
            border-radius: 0;
            overflow: hidden;
            box-shadow: 0 25px 80px rgba(0, 0, 0, 0.5);
            border: 1px solid rgba(255, 255, 255, 0.06);
            border-radius: 16px;
          }
          @media (max-width: 900px) {
            body.auth-sqg-landing [data-testid="stAppViewBlockContainer"] {
              min-height: auto !important;
              justify-content: flex-start !important;
            }
            body.auth-sqg-landing div[data-testid="stHorizontalBlock"] { min-height: auto !important; }
          }
          /* Column shells */
          body.auth-sqg-landing [data-testid="stHorizontalBlock"] > [data-testid="column"] {
            padding: 0 !important;
            min-height: min(90vh, 900px) !important;
          }
          body.auth-sqg-signin [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
            background: #0a0a0b !important;
            border-right: 1px solid rgba(255, 255, 255, 0.08);
            display: flex; flex-direction: column; align-items: stretch; justify-content: center;
            padding: 1.5rem 1.75rem 2rem 1.75rem !important;
          }
          body.auth-sqg-signup [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child {
            background: #0a0a0b !important;
            border-left: 1px solid rgba(255, 255, 255, 0.08);
            display: flex; flex-direction: column; align-items: stretch; justify-content: center;
            padding: 1.5rem 1.75rem 2rem 1.75rem !important;
          }
          body.auth-sqg-signin [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child,
          body.auth-sqg-signup [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
            background: linear-gradient(165deg, #4c1d95 0%, #5b21b6 40%, #3b0764 100%) !important;
            display: flex; flex-direction: column; align-items: stretch; justify-content: center;
            padding: 1.5rem 1.25rem 1.5rem 1.25rem !important;
            position: relative; overflow: hidden;
          }
          /* Logo + hero (form side) */
          .auth-logo-row { display: flex; align-items: center; gap: 0.5rem; margin: 0 0 1.5rem; max-width: 420px; }
          .auth-logo-ico {
            display: flex; width: 32px; height: 32px; border-radius: 8px; background: #6d28d9;
            align-items: center; justify-content: center; flex-shrink: 0; box-shadow: 0 2px 8px rgba(0,0,0,0.3);
          }
          .auth-logo-ico b {
            display: grid; width: 14px; height: 14px; grid-template: 1fr 1fr / 1fr 1fr; gap: 2px; opacity: 0.95;
          }
          .auth-logo-ico b i { display: block; background: #fff; border-radius: 1px; }
          .auth-logo-text { font-weight: 800; font-size: 1.1rem; color: #fafafa; letter-spacing: -0.02em; }
          .auth-hero { margin: 0 0 1.25rem; max-width: 420px; text-align: left; }
          .auth-hero h1.auth-title { margin: 0 0 0.4rem; font-size: 1.65rem; font-weight: 800; color: #fff; }
          .auth-hero p.auth-sub { margin: 0; color: #94a3b8; font-size: 0.92rem; line-height: 1.4; }
          .auth-progress {
            max-width: 420px; margin: 0 0 1rem;
            display: flex; gap: 4px; height: 3px; border-radius: 2px; overflow: hidden; background: #27272a;
          }
          .auth-progress span { flex: 1; background: #3f3f46; }
          .auth-progress span.on { background: #a78bfa; }

          /* Form: left-aligned, high-contrast dark inputs (professional + readable) */
          body.auth-sqg-landing [data-testid="stForm"] {
            max-width: 420px; width: 100%; text-align: left; margin: 0 !important; padding: 0 !important;
            background: transparent !important; border: none !important; box-shadow: none !important;
            border-radius: 0 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] label {
            text-transform: uppercase; letter-spacing: 0.06em; font-size: 0.65rem !important; color: #cbd5e1 !important;
            font-weight: 600 !important; justify-content: flex-start !important; text-align: left;
          }
          /* Fallback: ensure field labels stay visible even if body class injection is unavailable */
          [data-testid="stForm"] [data-testid="stWidgetLabel"] p,
          [data-testid="stForm"] [data-testid="stWidgetLabel"] label,
          [data-testid="stForm"] label {
            color: #dbeafe !important;
            opacity: 1 !important;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-size: 0.68rem !important;
            font-weight: 700 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="form-control-container"] {
            align-items: flex-start; text-align: left;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] {
            max-width: 100% !important; align-self: stretch;
            background: #0f172a !important;
            border: 1px solid #334155 !important;
            border-radius: 8px !important;
            color: #f8fafc !important;
            min-height: 42px;
            transition: border-color .15s ease, box-shadow .15s ease;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input {
            color: #f8fafc !important;
            caret-color: #f8fafc !important;
            -webkit-text-fill-color: #f8fafc !important;
            font-weight: 500 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input::placeholder {
            color: #94a3b8 !important;
            opacity: 1 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"]:focus-within {
            border-color: #7c3aed !important;
            box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.18) !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] button,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] svg,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] path {
            color: #cbd5e1 !important;
            fill: #cbd5e1 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stFormSubmitButton"] {
            width: 100%; border-radius: 8px; margin-top: 0.25rem;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stFormSubmitButton"] button {
            width: 100% !important;
            background: #6d28d9 !important; color: #fff !important; border: 1px solid #5b21b6 !important; font-weight: 600;
            border-radius: 8px; min-height: 44px;
          }
          /* Fallback submit button colors (in case body class selectors do not apply) */
          [data-testid="stFormSubmitButton"] button {
            background: #6d28d9 !important;
            border: 1px solid #5b21b6 !important;
            color: #dbeafe !important;
            font-weight: 700 !important;
          }
          [data-testid="stFormSubmitButton"] button *,
          [data-testid="stFormSubmitButton"] button p,
          [data-testid="stFormSubmitButton"] button span,
          [data-testid="stFormSubmitButton"] button div,
          [data-testid="stFormSubmitButton"] button label {
            color: #dbeafe !important;
            fill: #dbeafe !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stVerticalBlockBorderWrapper"] {
            background: transparent !important; border: none !important; padding: 0 !important; box-shadow: none !important;
          }
          /* Form column inner alignment */
          body.auth-sqg-landing [data-testid="stHorizontalBlock"] > [data-testid="column"] [data-testid="stVerticalBlock"] {
            max-width: 420px; width: 100%; margin: 0 auto; align-items: stretch !important;
          }
          body.auth-sqg-landing [data-testid="stHorizontalBlock"] > [data-testid="column"] [data-testid="element-container"] {
            align-items: flex-start; text-align: left; width: 100%;
            max-width: 420px; margin-left: auto; margin-right: auto;
          }

          .auth-nav-block { max-width: 420px; width: 100%; margin: 0.5rem auto 0; text-align: center; }
          .auth-nav-wrap { color: #94a3b8; font-size: 0.88rem; margin: 0 0 0.4rem; }
          body.auth-sqg-landing [data-testid="column"] [data-testid="stButton"] { max-width: 420px; }
          body.auth-sqg-landing [data-testid="column"] [data-testid="stButton"] button {
            background: #18181b !important; color: #e4e4e7 !important; border: 1px solid #3f3f46 !important;
            border-radius: 8px !important;
          }
          /* Fallback secondary/nav button colors (Create account / Sign in link button) */
          [data-testid="stButton"] button {
            background: #e2e8f0 !important;
            color: #0f172a !important;
            border: 1px solid #cbd5e1 !important;
            border-radius: 8px !important;
            font-weight: 600 !important;
          }
          [data-testid="stButton"] button *,
          [data-testid="stButton"] button p,
          [data-testid="stButton"] button span,
          [data-testid="stButton"] button div,
          [data-testid="stButton"] button label {
            color: #0f172a !important;
            fill: #0f172a !important;
            opacity: 1 !important;
          }

          /* Purple panels — sign-in right */
          .auth-sr-wrap { position: relative; z-index: 1; min-height: 100%; display: flex; flex-direction: column; align-items: center; justify-content: space-between; padding: 0.5rem 0.25rem; }
          .auth-sr-orb1, .auth-sr-orb2 { position: absolute; border-radius: 50%; filter: blur(50px); opacity: 0.4; pointer-events: none; }
          .auth-sr-orb1 { width: 200px; height: 200px; background: #7c3aed; top: -40px; right: -20px; }
          .auth-sr-orb2 { width: 180px; height: 180px; background: #4c1d95; bottom: 10%; left: -40px; }
          .auth-sr-mock { width: 100%; max-width: 300px; background: #0f172a; border-radius: 12px; padding: 0.6rem; box-shadow: 0 12px 40px rgba(0,0,0,0.4); border: 1px solid rgba(255,255,255,0.1); }
          .auth-sr-mock-top { display: flex; align-items: center; gap: 0.4rem; margin-bottom: 0.5rem; }
          .auth-sr-dot { width: 8px; height: 8px; border-radius: 50%; }
          .auth-sr-bars { display: grid; grid-template: repeat(2,1fr) / 1.2fr 0.8fr; gap: 0.4rem; min-height: 80px; }
          .auth-sr-b { background: rgba(59, 130, 246, 0.4); border-radius: 4px; }
          .auth-sr-b.n2 { background: rgba(99, 102, 241, 0.35); }
          .auth-sr-b.n3 { background: rgba(34, 197, 94, 0.3); }
          .auth-sr-b.n4 { background: rgba(250, 204, 21, 0.2); }
          .auth-sr-dots { display: flex; gap: 5px; justify-content: center; margin: 0.9rem 0; }
          .auth-sr-dots span { width: 6px; height: 6px; border-radius: 50%; background: rgba(255,255,255,0.3); }
          .auth-sr-dots span.on { background: #fff; }
          .auth-sr-t { text-align: center; color: #faf5ff; font-weight: 800; font-size: 1.15rem; margin: 0 0 0.4rem; }
          .auth-sr-s { text-align: center; color: rgba(255,255,255,0.8); font-size: 0.86rem; line-height: 1.5; max-width: 20rem; margin: 0 auto; }
          .auth-sr-cards { display: flex; flex-direction: column; gap: 0.5rem; width: 100%; max-width: 20rem; margin-top: 1rem; }
          .auth-sr-card { background: rgba(15, 23, 42, 0.45); backdrop-filter: blur(8px); border: 1px solid rgba(255,255,255,0.12); border-radius: 10px; padding: 0.65rem 0.8rem; text-align: left; }
          .auth-sr-card b { color: #fff; font-size: 0.86rem; display: block; margin-bottom: 0.2rem; }
          .auth-sr-card p { color: #c4b5fd; font-size: 0.75rem; margin: 0; line-height: 1.4; }
          .auth-sr-ico { font-size: 0.95rem; margin-bottom: 0.15rem; }

          /* Sign-up left — marketing */
          .auth-su-lpad { max-width: 24rem; margin: 0 auto; }
          .auth-su-title { color: #fff; font-size: 1.45rem; font-weight: 800; line-height: 1.2; margin: 0.75rem 0 0.5rem; letter-spacing: -0.02em; }
          .auth-su-sub { color: #c4b5fd; font-size: 0.88rem; line-height: 1.5; margin: 0 0 1.2rem; }
          .auth-su-steps { position: relative; padding-left: 0.25rem; }
          .auth-su-line { position: absolute; left: 14px; top: 6px; bottom: 6px; width: 1px; background: rgba(255,255,255,0.2); }
          .auth-su-step { position: relative; display: flex; gap: 0.75rem; margin-bottom: 0.9rem; }
          .auth-su-num { flex-shrink: 0; width: 28px; height: 28px; border-radius: 50%; background: rgba(255,255,255,0.12); color: #fff; font-size: 0.8rem; font-weight: 700; display: flex; align-items: center; justify-content: center; }
          .auth-su-shead { color: #fff; font-weight: 600; font-size: 0.9rem; margin: 0; }
          .auth-su-sdesc { color: #c4b5fd; font-size: 0.78rem; margin: 0.15rem 0 0; line-height: 1.4; }
          .auth-su-trust { display: flex; align-items: center; gap: 0.4rem; color: #a78bfa; font-size: 0.7rem; margin-top: 1.25rem; padding-top: 0.6rem; border-top: 1px solid rgba(255,255,255,0.1); }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _signin_right_panel() -> str:
    return """
<div class="auth-sr-wrap">
  <div class="auth-sr-orb1" aria-hidden="true"></div>
  <div class="auth-sr-orb2" aria-hidden="true"></div>
  <div class="auth-sr-mock" aria-hidden="true">
    <div class="auth-sr-mock-top">
      <span class="auth-sr-dot" style="background:#22c55e"></span>
      <span class="auth-sr-dot" style="background:#eab308"></span>
      <span class="auth-sr-dot" style="background:#64748b; margin-left: auto"></span>
    </div>
    <div class="auth-sr-bars">
      <div class="auth-sr-b"></div><div class="auth-sr-b n2"></div>
      <div class="auth-sr-b n3"></div><div class="auth-sr-b n4"></div>
    </div>
  </div>
  <div>
    <div class="auth-sr-dots" aria-hidden="true"><span class="on"></span><span></span><span></span></div>
    <p class="auth-sr-t">Smart Query</p>
    <p class="auth-sr-s">Plain-language questions → SQL and answers, with your data and your rules.</p>
  </div>
  <div class="auth-sr-cards">
    <div class="auth-sr-card">
      <div class="auth-sr-ico">〰</div>
      <b>Natural language to SQL</b>
      <p>Ask questions, get instant, explainable results.</p>
    </div>
    <div class="auth-sr-card">
      <div class="auth-sr-ico">▦</div>
      <b>Multi-tenant workspaces</b>
      <p>Isolated projects per company and team.</p>
    </div>
  </div>
</div>
"""


def _sign_up_left_panel() -> str:
    return """
<div class="auth-sr-wrap" style="justify-content: flex-start">
  <div class="auth-sr-orb1" aria-hidden="true"></div>
  <div class="auth-sr-orb2" aria-hidden="true"></div>
  <div class="auth-su-lpad">
    <div class="auth-logo-row" style="margin-top:0">
      <div class="auth-logo-ico"><b><i></i><i></i><i></i><i></i></b></div>
      <span class="auth-logo-text">Smart Query</span>
    </div>
    <h2 class="auth-su-title">From plain English to SQL in seconds</h2>
    <p class="auth-su-sub">Connect your database and start asking questions naturally — no SQL expertise needed.</p>
    <div class="auth-su-steps">
      <div class="auth-su-line" aria-hidden="true"></div>
      <div class="auth-su-step">
        <div class="auth-su-num">1</div>
        <div>
          <p class="auth-su-shead">Create your account</p>
          <p class="auth-su-sdesc">Set up your workspace in under 60 seconds.</p>
        </div>
      </div>
      <div class="auth-su-step">
        <div class="auth-su-num">2</div>
        <div>
          <p class="auth-su-shead">Connect a database</p>
          <p class="auth-su-sdesc">Postgres, MySQL, SQLite, and more.</p>
        </div>
      </div>
      <div class="auth-su-step" style="margin-bottom:0">
        <div class="auth-su-num">3</div>
        <div>
          <p class="auth-su-shead">Ask anything</p>
          <p class="auth-su-sdesc">Get SQL plus results in plain English.</p>
        </div>
      </div>
    </div>
    <div class="auth-su-trust" role="note">
      <span>🛡</span> Encrypted in transit — you control your data.
    </div>
  </div>
</div>
"""


def _form_hero(
    title: str, subtitle: str, *, show_logo: bool, show_signup_progress: bool
) -> str:
    logo = ""
    if show_logo:
        logo = (
            "<div class=\"auth-logo-row\">"
            "<div class=\"auth-logo-ico\"><b><i></i><i></i><i></i><i></i></b></div>"
            f'<span class="auth-logo-text">Smart Query</span>'
            "</div>"
        )
    prog = ""
    if show_signup_progress:
        prog = (
            "<div class=\"auth-progress\" aria-hidden=\"true\">"
            + "".join(
                f'<span class="{"on" if i == 0 else ""}"></span>' for i in range(4)
            )
            + "</div>"
        )
    return f"""
{logo}
{prog}
<div class="auth-hero">
  <h1 class="auth-title">{html.escape(title)}</h1>
  <p class="auth-sub">{html.escape(subtitle)}</p>
</div>
"""


def render_auth_layout(
    title: str,
    subtitle: str,
    form_renderer: Callable[[], None],
    *,
    variant: AuthVariant = "signin",
) -> None:
    apply_shared_theme()
    _body_class_inject(variant)
    render_auth_styles()
    c1, c2 = st.columns(2, gap="small")

    if variant == "signin":
        with c1:
            st.markdown(
                _form_hero(
                    title,
                    subtitle,
                    show_logo=True,
                    show_signup_progress=False,
                ),
                unsafe_allow_html=True,
            )
            form_renderer()
        with c2:
            st.markdown(_signin_right_panel(), unsafe_allow_html=True)
    else:
        with c1:
            st.markdown(_sign_up_left_panel(), unsafe_allow_html=True)
        with c2:
            st.markdown(
                _form_hero(
                    title,
                    subtitle,
                    show_logo=False,
                    show_signup_progress=True,
                ),
                unsafe_allow_html=True,
            )
            form_renderer()


def render_nav_link(
    text: str,
    button_text: str,
    on_click: Callable[[], None],
    *,
    button_key: str = "auth_nav_cta",
) -> None:
    st.markdown(
        f'<div class="auth-nav-block">'
        f'<p class="auth-nav-wrap">{html.escape(text)}</p></div>',
        unsafe_allow_html=True,
    )
    if st.button(
        button_text, use_container_width=True, type="secondary", key=button_key
    ):
        on_click()
