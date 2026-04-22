"""Validation helpers for auth forms."""

from __future__ import annotations

import re

_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]{3,32}$")


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match((email or "").strip()))


def is_valid_username(username: str) -> bool:
    return bool(_USERNAME_RE.match((username or "").strip()))


def validate_sign_in(username: str, password: str) -> list[str]:
    errors: list[str] = []
    if not username.strip():
        errors.append("Username is required.")
    if not password:
        errors.append("Password is required.")
    return errors


def validate_sign_up(
    email: str,
    company_name: str,
    username: str,
    password: str,
    confirm_password: str,
) -> list[str]:
    errors: list[str] = []
    if not email.strip():
        errors.append("Email is required.")
    elif not is_valid_email(email):
        errors.append("Please enter a valid email address.")

    if not username.strip():
        errors.append("Username is required.")
    elif not is_valid_username(username):
        errors.append(
            "Username must be 3-32 characters and contain only letters, numbers, or underscores."
        )

    if len(password) < 8:
        errors.append("Password must be at least 8 characters.")
    if not company_name.strip():
        errors.append("Company name is required.")
    if password != confirm_password:
        errors.append("Confirm password does not match.")
    return errors
