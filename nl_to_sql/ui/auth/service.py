"""Backend-facing auth service stubs.

These functions are intentionally simple so backend integration can be plugged in
without changing UI components.
"""

from __future__ import annotations

from typing import Any

import requests

from utils.config import nl_sql_api_url
from utils.constants import UI_AUTH_HTTP_TIMEOUT_SEC
from utils.http import safe_response_payload

API_URL = nl_sql_api_url()


def sign_in(username: str, password: str) -> dict[str, Any]:
    try:
        resp = requests.post(
            f"{API_URL}/api/platform/auth/signin",
            json={
                "username": username,
                "password": password,
            },
            timeout=UI_AUTH_HTTP_TIMEOUT_SEC,
        )
        body, jerr = safe_response_payload(resp)
        if jerr:
            return {"ok": False, "message": jerr}
        if resp.ok:
            msg = body.get("message", "Signed in successfully.") if isinstance(body, dict) else "Signed in successfully."
            return {"ok": True, "message": msg, "data": body}
        detail = body.get("detail") if isinstance(body, dict) else None
        return {"ok": False, "message": detail or f"Signin failed (HTTP {resp.status_code})."}
    except Exception as ex:
        return {"ok": False, "message": f"Could not reach API: {ex}"}


def sign_up(
    email: str,
    company_name: str,
    username: str,
    password: str,
    confirm_password: str,
) -> dict[str, Any]:
    try:
        resp = requests.post(
            f"{API_URL}/api/platform/auth/signup",
            json={
                "email": email,
                "company_name": company_name,
                "username": username,
                "password": password,
                "confirm_password": confirm_password,
            },
            timeout=UI_AUTH_HTTP_TIMEOUT_SEC,
        )
        body, jerr = safe_response_payload(resp)
        if jerr:
            return {"ok": False, "message": jerr}
        if resp.ok:
            msg = body.get("message", "Account created successfully.") if isinstance(body, dict) else "Account created successfully."
            return {"ok": True, "message": msg, "data": body}
        detail = body.get("detail") if isinstance(body, dict) else None
        return {"ok": False, "message": detail or f"Signup failed (HTTP {resp.status_code})."}
    except Exception as ex:
        return {"ok": False, "message": f"Could not reach API: {ex}"}
