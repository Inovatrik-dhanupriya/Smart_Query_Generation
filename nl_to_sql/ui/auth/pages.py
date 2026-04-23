"""Auth pages (Sign In / Sign Up) built from reusable components."""

from __future__ import annotations

from typing import Callable
import time
import uuid

import streamlit as st

from ui.auth.components import render_auth_layout, render_nav_link
from ui.auth.service import sign_in, sign_up
from ui.auth.validators import validate_sign_in, validate_sign_up

_AUTH_ALERTS_KEY = "auth_alerts"
_AUTH_USER_KEY = "auth_user"
_AUTH_VIEW_KEY = "_auth_view"


def _set_alerts(kind: str, messages: list[str]) -> None:
    st.session_state[_AUTH_ALERTS_KEY] = [
        {"id": str(uuid.uuid4()), "kind": kind, "text": msg}
        for msg in messages
    ]


def _dismiss_alert(alert_id: str) -> None:
    alerts = st.session_state.get(_AUTH_ALERTS_KEY, [])
    st.session_state[_AUTH_ALERTS_KEY] = [
        a for a in alerts if a.get("id") != alert_id
    ]


def _render_alerts() -> None:
    alerts = st.session_state.get(_AUTH_ALERTS_KEY, [])
    if not alerts:
        return

    for alert in alerts:
        left, right = st.columns([12, 1])
        with left:
            if alert["kind"] == "success":
                st.success(alert["text"])
            else:
                st.error(alert["text"])
        with right:
            st.button(
                "✕",
                key=f"close_{alert['id']}",
                on_click=_dismiss_alert,
                args=(alert["id"],),
            )


def render_sign_in_page(
    go_to_sign_up: Callable[[], None],
    go_to_main: Callable[[], None] | None = None,
) -> None:
    if st.session_state.get(_AUTH_VIEW_KEY) != "signin":
        st.session_state[_AUTH_VIEW_KEY] = "signin"
        st.session_state[_AUTH_ALERTS_KEY] = []

    def _form() -> None:
        with st.form("sign_in_form", clear_on_submit=False):
            username = st.text_input("Username", placeholder="Enter your username")
            password = st.text_input(
                "Password",
                placeholder="Enter your password",
                type="password",
            )
            submitted = st.form_submit_button("Sign In", use_container_width=True)

        if submitted:
            errors = validate_sign_in(username, password)
            if errors:
                _set_alerts("error", errors)
            else:
                result = sign_in(username=username, password=password)
                if result.get("ok"):
                    data = result.get("data") if isinstance(result.get("data"), dict) else {}
                    st.session_state[_AUTH_USER_KEY] = {
                        "user_id": data.get("user_id"),
                        "username": data.get("username", username),
                        "email": data.get("email"),
                        "company_name": data.get("company_name"),
                    }
                    if go_to_main:
                        go_to_main()
                        return
                    _set_alerts("success", [result.get("message", "Signed in successfully.")])
                else:
                    _set_alerts("error", [result.get("message", "Sign in failed.")])

        _render_alerts()
        render_nav_link(
            text="Don't have an account?",
            button_text="Create an account",
            on_click=go_to_sign_up,
            button_key="auth_nav_to_signup",
        )

    render_auth_layout(
        title="Welcome back",
        subtitle="Please enter your details to sign in.",
        form_renderer=_form,
        variant="signin",
    )


def render_sign_up_page(go_to_sign_in: Callable[[], None]) -> None:
    if st.session_state.get(_AUTH_VIEW_KEY) != "signup":
        st.session_state[_AUTH_VIEW_KEY] = "signup"
        st.session_state[_AUTH_ALERTS_KEY] = []

    def _form() -> None:
        with st.form("sign_up_form", clear_on_submit=False):
            email = st.text_input("Email", placeholder="Enter your email")
            company_name = st.text_input("Company Name", placeholder="Enter your company name")
            username = st.text_input("Username", placeholder="Choose a username")
            password = st.text_input(
                "Password",
                placeholder="Create a password",
                type="password",
            )
            confirm_password = st.text_input(
                "Confirm Password",
                placeholder="Re-enter your password",
                type="password",
            )
            submitted = st.form_submit_button("Sign Up", use_container_width=True)

        if submitted:
            email = (email or "").strip()
            company_name = (company_name or "").strip()
            username = (username or "").strip()
            password = password or ""
            confirm_password = confirm_password or ""
            errors = validate_sign_up(email, company_name, username, password, confirm_password)
            if errors:
                _set_alerts("error", errors)
            else:
                result = sign_up(
                    email=email,
                    company_name=company_name,
                    username=username,
                    password=password,
                    confirm_password=confirm_password,
                )
                if result.get("ok"):
                    st.session_state[_AUTH_ALERTS_KEY] = []
                    success_msg = result.get(
                        "message", "Account created successfully."
                    )
                    st.success(f"{success_msg}")
                    time.sleep(1)
                    go_to_sign_in()
                    return
                else:
                    _set_alerts("error", [result.get("message", "Sign up failed.")])

        _render_alerts()
        render_nav_link(
            text="Already have an account?",
            button_text="Sign in",
            on_click=go_to_sign_in,
            button_key="auth_nav_to_signin",
        )

    render_auth_layout(
        title="Create your account",
        subtitle="Get started free — no credit card required.",
        form_renderer=_form,
        variant="signup",
    )
