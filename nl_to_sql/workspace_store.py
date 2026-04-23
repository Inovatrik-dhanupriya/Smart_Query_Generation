"""
Persist workspace tenants and projects in USER_DETAILS database (per auth user_id).

Streamlit must run with the same .env as the API so get_app_db_cursor() works.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from utils.env import load_app_env

load_app_env()

log = logging.getLogger(__name__)

DEFAULT_TENANT_ID = "ten-default"

# Defer import so unqualified scripts don't need Postgres at import time
def _db():
    from db import get_app_db_cursor, prepare_app_auth_backend

    return get_app_db_cursor, prepare_app_auth_backend


def ensure_backend() -> None:
    _, prep = _db()
    prep()


def _row_first(row: Any) -> Any:
    """Return first column value for dict/tuple DB rows."""
    if row is None:
        return None
    if isinstance(row, dict):
        if row:
            return next(iter(row.values()))
        return None
    if isinstance(row, (list, tuple)):
        return row[0] if row else None
    return None


def _preferred_default_tenant_name(user_id: int) -> str:
    """Best-effort company name from auth profile for default tenant label."""
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            "SELECT company_name FROM public.auth_users WHERE id = %s LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
    name = str(_row_first(row) or "").strip()
    if name:
        return name
    return "Default company"


def _fmt(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M")
    s = str(v)
    return s[:19] if len(s) > 10 else s


def ensure_default_tenant_row(user_id: int) -> None:
    """Insert default company row for this user if none exist (matches UI default tenant id)."""
    get_app_db_cursor, _ = _db()
    preferred_name = _preferred_default_tenant_name(user_id)
    with get_app_db_cursor() as cur:
        cur.execute(
            "SELECT name FROM public.app_workspace_tenants WHERE user_id = %s AND id = %s",
            (user_id, DEFAULT_TENANT_ID),
        )
        row = cur.fetchone()
        if row is not None:
            existing_name = str(_row_first(row) or "").strip().lower()
            # Backfill old placeholder labels for accounts that already had a default tenant.
            if existing_name in ("", "default company") and preferred_name.lower() != "default company":
                cur.execute(
                    """
                    UPDATE public.app_workspace_tenants
                    SET name = %s, updated_at = NOW()
                    WHERE user_id = %s AND id = %s
                    """,
                    (preferred_name, user_id, DEFAULT_TENANT_ID),
                )
            return
        cur.execute(
            """
            INSERT INTO public.app_workspace_tenants (user_id, id, name, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            """,
            (user_id, DEFAULT_TENANT_ID, preferred_name),
        )


def load_tenants(user_id: int) -> list[dict]:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, name, created_at, updated_at
            FROM public.app_workspace_tenants
            WHERE user_id = %s
            ORDER BY name ASC, id ASC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        out.append(
            {
                "id": r["id"],
                "name": r["name"],
                "updated_at": _fmt(r.get("updated_at")),
            }
        )
    return out


def load_projects(user_id: int) -> list[dict]:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            """
            SELECT id, tenant_id, name, description, status, client_code, nl_session_id, updated_at
            FROM public.app_workspace_projects
            WHERE user_id = %s
            ORDER BY updated_at DESC, name ASC
            """,
            (user_id,),
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        out.append(
            {
                "id": r["id"],
                "tenant_id": r["tenant_id"],
                "name": r["name"],
                "description": r.get("description") or "",
                "status": r.get("status") or "Draft",
                "client_code": r.get("client_code") or "",
                "nl_session_id": r.get("nl_session_id") or "",
                "updated_at": _fmt(r.get("updated_at")),
            }
        )
    return out


def load_workspace(user_id: int) -> tuple[list[dict], list[dict]]:
    ensure_default_tenant_row(user_id)
    return load_tenants(user_id), load_projects(user_id)


def db_upsert_tenant(user_id: int, t: dict) -> None:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.app_workspace_tenants (user_id, id, name, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id, id) DO UPDATE
            SET name = EXCLUDED.name, updated_at = NOW()
            """,
            (user_id, t["id"], t["name"]),
        )


def db_delete_tenant(user_id: int, tenant_id: str) -> bool:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            "SELECT 1 FROM public.app_workspace_projects WHERE user_id = %s AND tenant_id = %s LIMIT 1",
            (user_id, tenant_id),
        )
        if cur.fetchone() is not None:
            return False
        cur.execute(
            "DELETE FROM public.app_workspace_tenants WHERE user_id = %s AND id = %s",
            (user_id, tenant_id),
        )
    return True


def db_upsert_project(user_id: int, p: dict) -> None:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.app_workspace_projects
                (user_id, id, tenant_id, name, description, status, client_code, nl_session_id, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (user_id, id) DO UPDATE
            SET tenant_id = EXCLUDED.tenant_id,
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                status = EXCLUDED.status,
                client_code = EXCLUDED.client_code,
                nl_session_id = EXCLUDED.nl_session_id,
                updated_at = NOW()
            """,
            (
                user_id,
                p["id"],
                p.get("tenant_id") or DEFAULT_TENANT_ID,
                p["name"],
                p.get("description") or "",
                p.get("status") or "Draft",
                p.get("client_code") or "",
                p.get("nl_session_id") or "",
            ),
        )


def db_delete_project(user_id: int, project_id: str) -> None:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            "DELETE FROM public.app_workspace_projects WHERE user_id = %s AND id = %s",
            (user_id, project_id),
        )


def db_update_project_nl_session(user_id: int, project_id: str, new_session_id: str) -> None:
    get_app_db_cursor, _ = _db()
    with get_app_db_cursor() as cur:
        cur.execute(
            """
            UPDATE public.app_workspace_projects
            SET nl_session_id = %s, updated_at = NOW()
            WHERE user_id = %s AND id = %s
            """,
            (new_session_id, user_id, project_id),
        )
