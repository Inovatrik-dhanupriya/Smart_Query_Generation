"""
Central configuration from environment variables.
Prefer setting values in ``Table_automation/.env`` instead of editing code.
"""
from __future__ import annotations

import os


def _int(name: str, default: int, *, minimum: int | None = None, maximum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or not str(raw).strip():
        v = default
    else:
        try:
            v = int(raw.strip())
        except ValueError:
            v = default
    if minimum is not None:
        v = max(minimum, v)
    if maximum is not None:
        v = min(maximum, v)
    return v


# ── SQL engine ───────────────────────────────────────────────────────────────
def sql_max_query_length() -> int:
    return _int("SQL_MAX_QUERY_LENGTH", 5000, minimum=500, maximum=50_000)


def sql_preview_limit() -> int:
    return _int("SQL_PREVIEW_LIMIT", 1000, minimum=1, maximum=50_000)


def sql_max_page_size() -> int:
    return _int("SQL_MAX_PAGE_SIZE", 5000, minimum=1, maximum=50_000)


def sql_statement_timeout_sec() -> int:
    return _int("SQL_STATEMENT_TIMEOUT_SEC", 60, minimum=5, maximum=600)


def sql_cache_ttl_seconds() -> int:
    return _int("SQL_CACHE_TTL", 60, minimum=0, maximum=86400)


# ── Remote sync API ───────────────────────────────────────────────────────────
def remote_api_timeout_sec() -> int:
    return _int("REMOTE_API_TIMEOUT_SEC", 60, minimum=5, maximum=600)


# ── FastAPI app / sessions ───────────────────────────────────────────────────
def session_max_turns() -> int:
    return _int("SESSION_MAX_TURNS", 10, minimum=1, maximum=100)


def session_max_age_hours() -> int:
    return _int("SESSION_MAX_AGE_HOURS", 24, minimum=1, maximum=168)


def agent_table_threshold() -> int:
    """When table count ≤ this, skip the LLM table-selection agent (FAISS only)."""
    return _int("AGENT_TABLE_THRESHOLD", 10, minimum=1, maximum=500)


def llm_cache_max_entries() -> int:
    return _int("LLM_CACHE_MAX", 200, minimum=10, maximum=10_000)


# ── Streamlit UI ─────────────────────────────────────────────────────────────
def nl_sql_api_url() -> str:
    """Backend base URL for the Streamlit app (no trailing slash)."""
    u = (
        os.getenv("NL_SQL_API_URL", "").strip()
        or os.getenv("API_URL", "").strip()
        or "http://127.0.0.1:8000"
    )
    return u.rstrip("/")


def streamlit_row_limit_options() -> list[int]:
    """Comma-separated ints, e.g. ``UI_ROW_LIMIT_OPTIONS=10,25,50,100``."""
    raw = os.getenv("UI_ROW_LIMIT_OPTIONS", "").strip()
    if not raw:
        return [10, 20, 25, 50, 100, 250, 500, 1000]
    out: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            continue
    return sorted(set(out)) if out else [10, 20, 25, 50, 100, 250, 500, 1000]


def default_sync_row_limit() -> int:
    return _int("UI_DEFAULT_SYNC_ROW_LIMIT", 1000, minimum=0, maximum=100_000)


def remote_sync_default_row_limit() -> int:
    """Default max rows per table when loading from a remote SQL API (schema JSON flow)."""
    return _int("REMOTE_SYNC_DEFAULT_ROW_LIMIT", 5000, minimum=1, maximum=100_000)


def ui_schema_table_browse_limit() -> int:
    """Max table names to render in Configuration browse / search (large catalogs)."""
    return _int("UI_SCHEMA_TABLE_BROWSE_LIMIT", 5000, minimum=100, maximum=200_000)


def db_sync_schema_default() -> str:
    """Legacy hook — sync target schema comes from the Streamlit UI, not from here."""
    return (os.getenv("DB_SYNC_SCHEMA") or "").strip()


def cors_origins() -> list[str]:
    """
    Comma-separated origins for FastAPI CORS. If unset, allows common local
    Streamlit origins for development; set ``CORS_ORIGINS`` explicitly in production.
    """
    raw = os.getenv("CORS_ORIGINS", "").strip()
    if not raw:
        return [
            "http://localhost:8501",
            "http://127.0.0.1:8501",
        ]
    return [o.strip() for o in raw.split(",") if o.strip()]
