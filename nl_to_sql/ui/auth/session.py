"""Client-side auth session persistence for Streamlit pages."""

from __future__ import annotations

import base64
import json
import time
from pathlib import Path
from typing import Any

import streamlit as st

from utils.constants import (
    AUTH_LOCAL_SESSION_FILENAME,
    AUTH_QUERY_PARAM_EXP,
    AUTH_QUERY_PARAM_USER,
    AUTH_SESSION_TTL_MINUTES_DEFAULT,
)

AUTH_USER_KEY = "auth_user"
AUTH_EXP_KEY = "_auth_expires_at"

_QP_USER_KEY = AUTH_QUERY_PARAM_USER
_QP_EXP_KEY = AUTH_QUERY_PARAM_EXP
_SESSION_FILE = Path(__file__).resolve().parent / AUTH_LOCAL_SESSION_FILENAME


def _b64_encode_json(payload: dict[str, Any]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64_decode_json(encoded: str) -> dict[str, Any] | None:
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def _qp_get(name: str) -> str | None:
    val = st.query_params.get(name)
    if val is None:
        return None
    if isinstance(val, list):
        return str(val[0]) if val else None
    return str(val)


def _qp_set(name: str, value: str) -> None:
    st.query_params[name] = value


def _qp_del(name: str) -> None:
    if name in st.query_params:
        del st.query_params[name]


def _write_local_session(user: dict[str, Any], exp: float) -> None:
    try:
        _SESSION_FILE.write_text(
            json.dumps({"user": user, "exp": float(exp)}, separators=(",", ":")),
            encoding="utf-8",
        )
    except Exception:
        # Non-fatal: query params + session_state can still work.
        pass


def _read_local_session() -> tuple[dict[str, Any], float] | None:
    try:
        if not _SESSION_FILE.exists():
            return None
        payload = json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
        user = payload.get("user")
        exp = payload.get("exp")
        if not isinstance(user, dict) or not isinstance(exp, (int, float)):
            return None
        return user, float(exp)
    except Exception:
        return None


def _clear_local_session() -> None:
    try:
        if _SESSION_FILE.exists():
            _SESSION_FILE.unlink()
    except Exception:
        pass


def set_auth_session(user: dict[str, Any], ttl_minutes: int = AUTH_SESSION_TTL_MINUTES_DEFAULT) -> None:
    """Persist auth in session_state + query params for browser refresh recovery."""
    now = time.time()
    exp = now + max(1, int(ttl_minutes)) * 60
    st.session_state[AUTH_USER_KEY] = user
    st.session_state[AUTH_EXP_KEY] = exp
    _qp_set(_QP_USER_KEY, _b64_encode_json(user))
    _qp_set(_QP_EXP_KEY, str(int(exp)))
    _write_local_session(user, exp)


def clear_auth_session() -> None:
    st.session_state.pop(AUTH_USER_KEY, None)
    st.session_state.pop(AUTH_EXP_KEY, None)
    _qp_del(_QP_USER_KEY)
    _qp_del(_QP_EXP_KEY)
    _clear_local_session()


def restore_auth_session() -> bool:
    """Restore login after refresh if not expired (30-minute default)."""
    now = time.time()

    user = st.session_state.get(AUTH_USER_KEY)
    exp = st.session_state.get(AUTH_EXP_KEY)
    if isinstance(user, dict) and user and isinstance(exp, (int, float)) and float(exp) > now:
        return True

    # Primary cross-refresh source when query params are stripped between pages.
    local = _read_local_session()
    if local:
        local_user, local_exp = local
        if local_exp > now:
            st.session_state[AUTH_USER_KEY] = local_user
            st.session_state[AUTH_EXP_KEY] = local_exp
            _qp_set(_QP_USER_KEY, _b64_encode_json(local_user))
            _qp_set(_QP_EXP_KEY, str(int(local_exp)))
            return True

    enc_user = _qp_get(_QP_USER_KEY)
    exp_raw = _qp_get(_QP_EXP_KEY)
    if not enc_user or not exp_raw:
        clear_auth_session()
        return False

    try:
        exp_qp = float(exp_raw)
    except Exception:
        clear_auth_session()
        return False

    if exp_qp <= now:
        clear_auth_session()
        return False

    decoded_user = _b64_decode_json(enc_user)
    if not decoded_user:
        clear_auth_session()
        return False

    st.session_state[AUTH_USER_KEY] = decoded_user
    st.session_state[AUTH_EXP_KEY] = exp_qp
    _write_local_session(decoded_user, exp_qp)
    return True
