"""List databases and schemas using a one-off PostgreSQL connection."""
from __future__ import annotations

from db import PgCredentials, one_shot_connect


def list_database_names(creds: PgCredentials) -> list[str]:
    conn = one_shot_connect(creds, read_only=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT datname::text
            FROM pg_database
            WHERE datistemplate = false AND datallowconn
            ORDER BY datname
            """
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def list_schema_names(creds: PgCredentials) -> list[str]:
    conn = one_shot_connect(creds, read_only=True)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT schema_name::text
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
            ORDER BY schema_name
            """
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
