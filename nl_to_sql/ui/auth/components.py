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
          /* Robust fallback when body class injection is unavailable */
          section.main:has(.auth-su-lpad) > div.block-container > div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"]:first-of-type {
            align-items: center !important;
            min-height: calc(100vh - 5rem) !important;
            margin-top: 0 !important;
            transform: none !important;
          }
          section.main:has(.auth-su-lpad) > div.block-container > div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"] > [data-testid="column"] {
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
            min-height: calc(100vh - 5rem) !important;
          }
          section.main:has(.auth-su-lpad) .auth-sr-wrap {
            justify-content: center !important;
            min-height: 100% !important;
          }
          section.main:has(.auth-progress) .auth-progress {
            max-width: 460px;
            margin: 0 0 1.15rem;
            display: flex;
            gap: 0.45rem;
            height: 4px;
            background: transparent;
            border-radius: 999px;
          }
          section.main:has(.auth-progress) .auth-progress span {
            flex: 1;
            background: rgba(148,163,184,0.28);
            border-radius: 999px;
          }
          section.main:has(.auth-progress) .auth-progress span.on {
            background: linear-gradient(90deg, #7c3aed, #8b5cf6);
            box-shadow: 0 0 0 1px rgba(124,58,237,0.22), 0 0 10px rgba(124,58,237,0.35);
          }

          /* Fallback scope: if body class script is blocked, still style auth page via :has() */
          section.main:has(.auth-hero) [data-testid="stForm"] [data-testid="stWidgetLabel"] p,
          section.main:has(.auth-hero) [data-testid="stForm"] label {
            color: #e2e8f0 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
            font-size: 0.75rem !important;
            font-weight: 700 !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] {
            background: transparent !important;
            border: 1.5px solid rgba(255,255,255,0.25) !important;
            border-radius: 8px !important;
            min-height: 46px !important;
            padding: 0 !important;
            overflow: hidden !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] > div,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="base-input"],
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] [data-baseweb="base-input"] {
            background: transparent !important;
            background-color: transparent !important;
            box-shadow: none !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input {
            color: #ffffff !important;
            -webkit-text-fill-color: #ffffff !important;
            caret-color: #ffffff !important;
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 12px 16px !important;
            line-height: 1.35 !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:hover,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:focus,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:active {
            background: transparent !important;
            box-shadow: none !important;
            outline: none !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill:hover,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill:focus {
            -webkit-text-fill-color: #ffffff !important;
            box-shadow: 0 0 0 1000px transparent inset !important;
            transition: background-color 9999s ease-in-out 0s !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] input::placeholder {
            color: rgba(255,255,255,0.38) !important;
            opacity: 1 !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"]:focus-within {
            border-color: #7c3aed !important;
            box-shadow: 0 0 0 3px rgba(124,58,237,0.2) !important;
            outline: none !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] button,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] svg,
          section.main:has(.auth-hero) [data-testid="stForm"] [data-baseweb="input"] path {
            color: rgba(255,255,255,0.5) !important;
            fill: rgba(255,255,255,0.5) !important;
          }
          section.main:has(.auth-hero) [data-testid="stForm"] [data-testid="stFormSubmitButton"] button {
            background: #7c3aed !important;
            border: 1px solid #6d28d9 !important;
            color: #ffffff !important;
            font-weight: 600 !important;
          }
          section.main:has(.auth-nav-block) [data-testid="stButton"] button {
            background: transparent !important;
            border: 1.5px solid rgba(255,255,255,0.25) !important;
            color: #ffffff !important;
            border-radius: 8px !important;
          }
          section.main:has(.auth-nav-block) [data-testid="stButton"] button:hover {
            border-color: #7c3aed !important;
            color: #a78bfa !important;
            background: rgba(124,58,237,0.1) !important;
          }

          body.auth-sqg-landing .stApp {
            color: #e2e8f0 !important;
            background:
              radial-gradient(ellipse at 70% 50%, rgba(124,58,237,0.15) 0%, transparent 60%),
              radial-gradient(ellipse at 15% 85%, rgba(99,102,241,0.08) 0%, transparent 55%),
              #0a0a14 !important;
          }
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"] {
            position: relative;
            padding: 0 !important;
            min-height: 100vh !important;
            background: transparent !important;
          }
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"]::before,
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"]::after {
            content: "";
            position: fixed;
            border-radius: 999px;
            filter: blur(80px);
            opacity: 0.15;
            pointer-events: none;
            z-index: 0;
          }
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"]::before {
            width: 280px; height: 280px; background: #7c3aed; left: 8%; top: 16%;
          }
          body.auth-sqg-landing [data-testid="stAppViewBlockContainer"]::after {
            width: 260px; height: 260px; background: #6366f1; right: 6%; bottom: 10%;
          }
          body.auth-sqg-landing section.main > div.block-container {
            position: relative;
            z-index: 1;
            max-width: 1280px !important;
            width: 100% !important;
            margin: 0 auto !important;
            padding: 0 1.5rem 0.6rem 1.5rem !important;
            overflow: hidden !important;
            min-height: 100vh !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
          }
          body.auth-sqg-landing [data-testid="collapsedControl"],
          body.auth-sqg-landing [data-testid="stSidebar"] { display: none !important; }

          body.auth-sqg-landing section.main > div.block-container > div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"]:first-of-type {
            align-items: center !important;
            width: 100% !important;
            max-width: 1280px;
            margin: 0 auto !important;
            min-height: calc(100vh - 5rem) !important;
            transform: none !important;
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 18px;
            overflow: hidden;
            box-shadow: 0 30px 80px rgba(0,0,0,0.45);
            background: rgba(10,10,20,0.6);
          }
          body.auth-sqg-landing section.main > div.block-container > div[data-testid="stVerticalBlock"] > div[data-testid="stHorizontalBlock"]:first-of-type > [data-testid="column"] {
            min-height: calc(100vh - 5rem) !important;
            padding: 1.25rem !important;
            display: flex !important;
            flex-direction: column !important;
            justify-content: center !important;
          }
          body.auth-sqg-signin [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child,
          body.auth-sqg-signup [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child {
            background: #0d0d1a !important;
            display: flex; flex-direction: column; justify-content: center;
          }
          body.auth-sqg-signin [data-testid="stHorizontalBlock"] > [data-testid="column"]:last-child,
          body.auth-sqg-signup [data-testid="stHorizontalBlock"] > [data-testid="column"]:first-child {
            background: #12121f !important;
            border-left: 1px solid rgba(255,255,255,0.08);
            position: relative;
          }

          .auth-logo-row { display:flex; align-items:center; gap:0.55rem; margin:0 0 1.5rem; max-width:460px; }
          .auth-logo-ico { display:flex; width:32px; height:32px; border-radius:8px; background:#7c3aed; align-items:center; justify-content:center; }
          .auth-logo-ico b { display:flex; flex-direction:column; gap:2px; }
          .auth-logo-ico b i { display:block; width:12px; height:2px; border-radius:2px; background:#ffffff; }
          .auth-logo-text { color:#ffffff; font-weight:800; font-size:1.08rem; }
          .auth-hero { margin:0 0 1.3rem; max-width:460px; text-align:left; }
          .auth-hero h1.auth-title { margin:0 0 0.4rem; font-size:2.5rem; font-weight:800; color:#ffffff; line-height:1.1; letter-spacing:-0.03em; }
          .auth-hero p.auth-sub { margin:0; color:#94a3b8; font-size:0.98rem; line-height:1.5; }
          .auth-progress {
            max-width: 460px;
            margin: 0 0 1.15rem;
            display: flex;
            gap: 0.45rem;
            height: 4px;
            background: transparent;
            border-radius: 999px;
          }
          .auth-progress span {
            flex: 1;
            background: rgba(148, 163, 184, 0.28);
            border-radius: 999px;
          }
          .auth-progress span.on {
            background: linear-gradient(90deg, #7c3aed, #8b5cf6);
            box-shadow: 0 0 0 1px rgba(124,58,237,0.22), 0 0 10px rgba(124,58,237,0.35);
          }

          body.auth-sqg-landing [data-testid="stForm"] {
            max-width:460px; width:100%; margin:0 !important; padding:0 !important;
            background:transparent !important; border:none !important; box-shadow:none !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stWidgetLabel"] p,
          body.auth-sqg-landing [data-testid="stForm"] label {
            color: #e2e8f0 !important;
            text-transform: uppercase !important;
            letter-spacing: 0.08em !important;
            font-size: 0.75rem !important;
            font-weight: 700 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] {
            background: transparent !important;
            border: 1.5px solid rgba(255,255,255,0.25) !important;
            border-radius:8px !important;
            min-height:46px;
            padding: 0 !important;
            overflow: hidden !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] > div,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="base-input"],
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] [data-baseweb="base-input"] {
            background: transparent !important;
            background-color: transparent !important;
            box-shadow: none !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input {
            color:#ffffff !important;
            -webkit-text-fill-color:#ffffff !important;
            caret-color:#ffffff !important;
            background: transparent !important;
            border: none !important;
            box-shadow: none !important;
            padding: 12px 16px !important;
            line-height: 1.35 !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:hover,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:focus,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:active {
            background: transparent !important;
            box-shadow: none !important;
            outline: none !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill:hover,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input:-webkit-autofill:focus {
            -webkit-text-fill-color: #ffffff !important;
            box-shadow: 0 0 0 1000px transparent inset !important;
            transition: background-color 9999s ease-in-out 0s !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"]:hover,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] > div:hover {
            background: transparent !important;
            background-color: transparent !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] input::placeholder { color: rgba(255,255,255,0.38) !important; opacity:1 !important; }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"]:focus-within {
            border-color:#7c3aed !important;
            box-shadow:0 0 0 3px rgba(124,58,237,0.2) !important;
            outline: none !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] button,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] svg,
          body.auth-sqg-landing [data-testid="stForm"] [data-baseweb="input"] path {
            color: rgba(255,255,255,0.5) !important;
            fill: rgba(255,255,255,0.5) !important;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stFormSubmitButton"] button {
            width:100% !important; min-height:46px; border-radius:10px;
            background: #7c3aed !important;
            border:1px solid #6d28d9 !important;
            color:#ffffff !important; font-weight:600 !important;
            transition: transform .15s ease, filter .15s ease;
          }
          body.auth-sqg-landing [data-testid="stForm"] [data-testid="stFormSubmitButton"] button:hover {
            transform: scale(1.02);
            filter: brightness(1.1);
          }
          .auth-forgot-wrap { max-width: 460px; text-align: right; margin: 0.15rem 0 0.45rem; }
          .auth-forgot-wrap a { color:#a78bfa !important; text-decoration:none; font-size:0.85rem; }
          .auth-forgot-wrap a:hover { color:#c4b5fd !important; text-decoration:underline; }
          .auth-nav-block { max-width:460px; width:100%; margin:0.28rem auto 0; text-align:center; }
          .auth-nav-wrap { color:#94a3b8; font-size:0.9rem; margin:0 0 0.15rem; text-align:center; }
          body.auth-sqg-landing [data-testid="stButton"] {
            max-width: 460px !important;
            width: 100% !important;
            margin: 0 auto !important;
          }
          body.auth-sqg-landing [data-testid="stButton"] button {
            width:100% !important; min-height:44px;
            background: transparent !important;
            border: 1.5px solid rgba(255,255,255,0.25) !important;
            color: #ffffff !important;
            border-radius:10px !important;
          }
          body.auth-sqg-landing [data-testid="stButton"] button:hover {
            border-color: #7c3aed !important;
            color: #a78bfa !important;
            background: rgba(124,58,237,0.1) !important;
          }

          .auth-sr-wrap { height:100%; display:flex; flex-direction:column; justify-content:center; gap:1rem; color:#e2e8f0; position:relative; z-index:1; }
          .auth-sr-sql {
            background:#1a1a2e; border:1px solid rgba(255,255,255,0.08); border-radius:14px;
            padding:1rem; box-shadow:0 14px 40px rgba(0,0,0,0.35);
          }
          .auth-sr-terminal { background:#10121f; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:0.85rem; }
          .auth-sr-term-top { display:flex; gap:5px; margin-bottom:0.65rem; }
          .auth-sr-term-top span { width:8px; height:8px; border-radius:999px; background:#334155; }
          .auth-sr-term-top span:nth-child(1) { background:#ef4444; }
          .auth-sr-term-top span:nth-child(2) { background:#f59e0b; }
          .auth-sr-term-top span:nth-child(3) { background:#22c55e; }
          .auth-sr-line { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; font-size:0.81rem; color:#cbd5e1; line-height:1.55; white-space:nowrap; overflow:hidden; }
          .auth-sr-line.typing { width:0; border-right:2px solid #7c3aed; animation: authTyping 4.8s steps(56, end) infinite, authCaret .8s step-end infinite; }
          .sql-k { color:#7c3aed; font-weight:700; }
          .sql-s { color:#34d399; }
          .sql-n { color:#f59e0b; }
          @keyframes authTyping { 0%,10% { width:0 } 45%,100% { width:100% } }
          @keyframes authCaret { 0%,100% { border-color:transparent } 50% { border-color:#7c3aed } }

          .auth-sr-cards { display:grid; grid-template-columns:1fr; gap:0.7rem; }
          .auth-sr-card {
            position:relative;
            background: rgba(255,255,255,0.04);
            border:1px solid rgba(255,255,255,0.1);
            border-radius:12px;
            padding:0.72rem 0.8rem;
          }
          .auth-sr-ico { color:#7c3aed; font-size:1rem; margin-bottom:0.2rem; }
          .auth-sr-card b { color:#ffffff; display:block; margin-bottom:0.18rem; }
          .auth-sr-card p { color:#cbd5e1; margin:0; font-size:0.82rem; line-height:1.45; }
          .auth-sr-arrows {
            position:absolute; right:0.7rem; top:0.65rem; opacity:0;
            display:flex; gap:0.45rem; transition: opacity .15s ease;
          }
          .auth-sr-wrap:hover .auth-sr-arrows { opacity:1; }
          .auth-sr-arrow { color:#ffffff; font-size:0.88rem; line-height:1; }

          /* Sign-up left — premium SaaS timeline */
          .auth-su-lpad {
            max-width: 30rem;
            margin: 0 auto;
            margin-top: 1.1rem;
            padding: 0.6rem 0.35rem 0.5rem;
            background: linear-gradient(160deg, rgba(124,58,237,0.12), rgba(99,102,241,0.05));
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 14px;
            box-shadow: 0 14px 36px rgba(2,6,23,0.35);
          }
          body.auth-sqg-signup .auth-sr-wrap {
            justify-content: center !important;
            min-height: 100%;
          }
          .auth-su-badge {
            display: inline-flex;
            align-items: center;
            gap: 0.35rem;
            color: #ddd6fe;
            background: rgba(124,58,237,0.2);
            border: 1px solid rgba(167,139,250,0.35);
            border-radius: 999px;
            padding: 0.28rem 0.65rem;
            font-size: 0.72rem;
            font-weight: 700;
            letter-spacing: 0.04em;
            margin: 0.1rem 0 0.7rem;
          }
          .auth-su-title {
            color: #ffffff;
            font-size: 1.75rem;
            font-weight: 800;
            line-height: 1.16;
            margin: 0.2rem 0 0.62rem;
            letter-spacing: -0.02em;
          }
          .auth-su-sub {
            color: #cbd5e1;
            font-size: 0.92rem;
            line-height: 1.58;
            margin: 0 0 1.35rem;
            padding-bottom: 0.85rem;
            border-bottom: 1px solid rgba(255,255,255,0.12);
          }
          .auth-su-steps { position: relative; padding-left: 0.1rem; }
          .auth-su-line {
            position: absolute;
            left: 16px;
            top: 10px;
            bottom: 14px;
            width: 1.5px;
            background: linear-gradient(180deg, rgba(167,139,250,0.7), rgba(167,139,250,0.08));
          }
          .auth-su-step {
            position: relative;
            display: flex;
            gap: 0.85rem;
            margin-bottom: 1rem;
            padding: 0.7rem 0.75rem 0.75rem 0.58rem;
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 11px;
          }
          .auth-su-step:last-child { margin-bottom: 0.2rem; }
          .auth-su-num {
            flex-shrink: 0;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            background: radial-gradient(circle at 30% 30%, #8b5cf6, #6d28d9);
            color: #ffffff;
            font-size: 0.82rem;
            font-weight: 800;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 0 0 3px rgba(124,58,237,0.18);
          }
          .auth-su-shead { color: #ffffff; font-weight: 700; font-size: 0.95rem; margin: 0; }
          .auth-su-sdesc { color: #cbd5e1; font-size: 0.82rem; margin: 0.16rem 0 0; line-height: 1.45; }
          .auth-su-trust {
            display: flex;
            align-items: center;
            gap: 0.45rem;
            color: #d8b4fe;
            font-size: 0.74rem;
            margin-top: 1rem;
            padding: 0.65rem 0.7rem;
            border-top: 1px solid rgba(255,255,255,0.12);
            background: rgba(255,255,255,0.03);
            border-radius: 9px;
          }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _signin_right_panel() -> str:
    return """
<div class="auth-sr-wrap">
  <div class="auth-sr-sql">
    <div class="auth-sr-terminal" aria-hidden="true">
      <div class="auth-sr-term-top">
        <span></span><span></span><span></span>
      </div>
      <div class="auth-sr-line typing">Q: Show top <span class="sql-n">10</span> customers by revenue this month</div>
      <div class="auth-sr-line"><span class="sql-k">SELECT</span> customer_name, <span class="sql-k">SUM</span>(revenue) <span class="sql-k">AS</span> total_revenue</div>
      <div class="auth-sr-line"><span class="sql-k">FROM</span> sales <span class="sql-k">WHERE</span> sale_date <span class="sql-k">>=</span> <span class="sql-s">'2026-04-01'</span></div>
      <div class="auth-sr-line"><span class="sql-k">GROUP BY</span> customer_name <span class="sql-k">ORDER BY</span> total_revenue <span class="sql-k">DESC LIMIT</span> <span class="sql-n">10</span>;</div>
    </div>
  </div>
  <div class="auth-sr-cards">
    <div class="auth-sr-card">
      <div class="auth-sr-arrows"><span class="auth-sr-arrow">&lt;</span><span class="auth-sr-arrow">&gt;</span></div>
      <div class="auth-sr-ico">⚡</div>
      <b>Natural language to SQL</b>
      <p>Ask questions, get instant, explainable results.</p>
    </div>
    <div class="auth-sr-card">
      <div class="auth-sr-ico">▦</div>
      <b>Multi-tenant workspaces</b>
      <p>Isolated projects per company and team.</p>
    </div>
    <div class="auth-sr-card">
      <div class="auth-sr-ico">🕒</div>
      <b>Query History</b>
      <p>Review recent prompts and SQL runs for faster iteration.</p>
    </div>
  </div>
</div>
"""


def _sign_up_left_panel() -> str:
    return """
<div class="auth-sr-wrap">
  <div class="auth-su-lpad">
    <div class="auth-logo-row" style="margin-top:0">
      <div class="auth-logo-ico"><b><i></i><i></i><i></i><i></i></b></div>
      <span class="auth-logo-text">Smart Query</span>
    </div>
    <div class="auth-su-badge"><span>✦</span><span>Fast onboarding</span></div>
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


def _auth_navbar() -> str:
    return """
<nav class="auth-nav">
  <div class="auth-nav-left">
    <a class="auth-nav-logo" href="#" aria-label="Smart Query home">
      <span class="auth-nav-logo-icon"><b><i></i><i></i><i></i></b></span>
      <span class="auth-nav-logo-text">Smart Query</span>
    </a>
  </div>
  <div class="auth-nav-right">
    <a class="auth-nav-link" href="#">Docs</a>
    <a class="auth-nav-link" href="#">Pricing</a>
    <a class="auth-nav-link" href="#">Login</a>
    <a class="auth-nav-link auth-nav-cta" href="#">Get Started</a>
  </div>
</nav>
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
                    show_signup_progress=False,
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
        f'<div class="auth-nav-block" style="display:flex;justify-content:center;align-items:center;width:100%;">'
        f'<p class="auth-nav-wrap">{html.escape(text)}</p></div>',
        unsafe_allow_html=True,
    )
    if st.button(
        button_text, use_container_width=True, type="secondary", key=button_key
    ):
        on_click()
