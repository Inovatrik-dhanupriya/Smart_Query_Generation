"""
schema/importer.py
Fetches data from a remote SQL-passthrough API and syncs it into the
local PostgreSQL database using an admin (write-capable) connection.

Smart-sync logic per table:
  • Table doesn't exist → CREATE TABLE + INSERT all rows
  • Table already exists →
      – Any column in the API data that's missing locally → ALTER TABLE ADD COLUMN
      – Upsert all rows (ON CONFLICT id DO UPDATE)
      – Reports rows added / columns added
"""
from __future__ import annotations

import json as _json
import logging
import os
import re
from typing import Any

import psycopg2
from psycopg2 import sql as pg_sql
import requests

from schema.extractor import get_sync_target_schema
from utils.config import remote_api_timeout_sec

log = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


# ── Type helpers ──────────────────────────────────────────────────────────────

def _pg_type(col: str, val: Any, *, for_alter: bool = False) -> str:
    """
    Infer a PostgreSQL column type from a Python value.
    for_alter=True → skip PRIMARY KEY clause (can't use it in ALTER TABLE).
    dict/list values are serialised to JSON and stored as TEXT.
    """
    if col == "id" and not for_alter:
        return "BIGINT PRIMARY KEY"
    if isinstance(val, bool):           # bool before int — bool IS int in Python
        return "BOOLEAN"
    if isinstance(val, int):
        return "BIGINT"
    if isinstance(val, float):
        return "DOUBLE PRECISION"
    if isinstance(val, (dict, list)):   # nested JSON → store as TEXT
        return "TEXT"
    return "TEXT"


def _safe_val(val: Any) -> Any:
    """
    Convert values that psycopg2 cannot adapt natively.
    dict / list  → JSON string
    Everything else is returned unchanged.
    """
    if isinstance(val, (dict, list)):
        return _json.dumps(val, ensure_ascii=False, default=str)
    return val


def _admin_conn() -> psycopg2.extensions.connection:
    """Open a write-capable (admin) PostgreSQL connection."""
    dbname = os.getenv("DB_NAME", "").strip()
    if not dbname:
        raise RuntimeError("DB_NAME must be set in the environment for table sync.")
    conn = psycopg2.connect(
        host     = os.getenv("DB_HOST",           "localhost"),
        port     = int(os.getenv("DB_PORT",       "5432")),
        dbname   = dbname,
        user     = os.getenv("DB_ADMIN_USER",     "postgres"),
        password = os.getenv("DB_ADMIN_PASSWORD", ""),
    )
    conn.autocommit = False
    return conn


def count_public_physical_tables_admin() -> int:
    """
    Count ordinary tables in the sync target schema (pg_catalog, admin connection).
    Used to detect 'tables exist but the API read user cannot see them' issues.
    """
    sync_ns = get_sync_target_schema()
    conn = _admin_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM pg_catalog.pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r'
            """,
            (sync_ns,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def repair_reader_grants_public() -> tuple[bool, str | None]:
    """
    Grant USAGE on the sync schema and SELECT on all its tables to DB_USER,
    plus default privileges for future tables created by DB_ADMIN_USER.

    PostgreSQL's information_schema only lists tables the current role may access.
    Sync uses the admin role, while NL→SQL reads with DB_USER — without GRANT,
    the UI/embeddings see 0 tables even though pgAdmin (superuser) shows data.
    """
    reader = os.getenv("DB_USER", "").strip()
    if not reader:
        return False, "DB_USER is not set"
    admin = os.getenv("DB_ADMIN_USER", "postgres").strip()
    if reader == admin:
        return False, None  # same role already sees its objects

    sync_ns = get_sync_target_schema()
    conn = _admin_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = off;")
        cur.execute(
            pg_sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                pg_sql.Identifier(sync_ns), pg_sql.Identifier(reader)
            )
        )
        cur.execute(
            pg_sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                pg_sql.Identifier(sync_ns),
                pg_sql.Identifier(reader),
            )
        )
        try:
            cur.execute(
                pg_sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} "
                    "GRANT SELECT ON TABLES TO {}"
                ).format(
                    pg_sql.Identifier(admin),
                    pg_sql.Identifier(sync_ns),
                    pg_sql.Identifier(reader),
                )
            )
        except Exception as e:
            log.debug("ALTER DEFAULT PRIVILEGES skipped: %s", e)
        conn.commit()
        return True, None
    except Exception as e:
        conn.rollback()
        log.warning("repair_reader_grants_public failed: %s", e)
        return False, str(e)
    finally:
        conn.close()


def _grant_reader_select_on_table(cur, table_name: str) -> None:
    """Let DB_USER read a table the admin just created/updated (same transaction)."""
    reader = os.getenv("DB_USER", "").strip()
    if not reader:
        return
    admin = os.getenv("DB_ADMIN_USER", "postgres").strip()
    if reader == admin:
        return
    sync_ns = get_sync_target_schema()
    try:
        cur.execute(
            pg_sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                pg_sql.Identifier(sync_ns), pg_sql.Identifier(reader)
            )
        )
        cur.execute(
            pg_sql.SQL("GRANT SELECT ON TABLE {}.{} TO {}").format(
                pg_sql.Identifier(sync_ns),
                pg_sql.Identifier(table_name),
                pg_sql.Identifier(reader),
            )
        )
    except Exception as e:
        log.warning(
            "GRANT SELECT on %s.%s to %s failed (non-fatal): %s",
            sync_ns,
            table_name,
            reader,
            e,
        )


# ── Remote API ────────────────────────────────────────────────────────────────

def fetch_from_remote_api(query: str, api_url: str | None = None) -> list[dict]:
    """
    POST the query to the remote SQL-passthrough API.
    Supports response shapes:
      {"entity": [...]}  |  {"data": [...]}  |  {"rows": [...]}  |  [...]

    ``api_url`` overrides ``REMOTE_API_URL`` from the environment; at least one
    must be set (no vendor URL is hardcoded in code).
    """
    url = (api_url or os.getenv("REMOTE_API_URL", "")).strip()
    if not url:
        raise ValueError(
            "Set REMOTE_API_URL in .env to your SQL passthrough API base URL, "
            "or pass api_url when calling import/sync."
        )
    log.info(f"Remote API  POST {url}  query={query[:80]!r}…")

    resp = requests.post(
        url,
        json    = {"query": query},
        headers = {"Content-Type": "application/json"},
        timeout = remote_api_timeout_sec(),
    )
    resp.raise_for_status()

    body = resp.json()
    if isinstance(body, list):
        return body
    for key in ("entity", "data", "rows", "result", "records"):
        if key in body and isinstance(body[key], list):
            return body[key]

    raise ValueError(f"Remote API returned unexpected shape: {list(body.keys())}")


# ── Schema JSON parser ────────────────────────────────────────────────────────

def parse_schema_json(data: Any) -> list[str]:
    """
    Extract table names from a schema JSON in any of these common formats:

    Format A – plain list of strings
        ["invoices", "line_items", "customers"]

    Format B – object with "tables" as a list
        {"tables": ["invoices", "line_items"]}

    Format C – object with "tables" as a dict  (pgAdmin-style)
        {"tables": {"invoices": {…}, "line_items": {…}}}

    Format D – flat object whose keys are table names
        {"invoices": {…}, "line_items": {…}}
    """
    if isinstance(data, list):
        # Format A
        return [str(x) for x in data if isinstance(x, str) and x.strip()]

    if isinstance(data, dict):
        if "tables" in data:
            t = data["tables"]
            if isinstance(t, list):           # Format B
                return [str(x) for x in t if isinstance(x, str) and x.strip()]
            if isinstance(t, dict):           # Format C
                return list(t.keys())

        # Format D — every top-level key is a table name
        return list(data.keys())

    return []


# ── Smart sync ────────────────────────────────────────────────────────────────

def sync_table(
    table_name: str,
    records:    list[dict],
) -> dict:
    """
    Syncs one table from fetched records.

    Returns a status dict:
    {
      "action":             "created" | "synced",
      "rows_upserted":      int,
      "columns_added":      [str, …],
      "local_count_before": int,
      "local_count_after":  int,
    }
    """
    if not _SAFE_NAME_RE.match(table_name):
        raise ValueError(
            f"Invalid table name {table_name!r}. "
            "Only letters, digits and underscores are allowed."
        )
    if not records:
        raise ValueError("No records returned from the remote API.")

    first  = records[0]
    has_id = "id" in first
    sync_schema = get_sync_target_schema()
    tbl_ref = pg_sql.SQL("{}.{}").format(
        pg_sql.Identifier(sync_schema), pg_sql.Identifier(table_name)
    )

    conn = _admin_conn()
    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = off;")

        # ── Does the table already exist? ─────────────────────────────────
        cur.execute(
            """SELECT EXISTS (
                   SELECT 1 FROM information_schema.tables
                   WHERE table_schema = %s AND table_name = %s
               )""",
            (sync_schema, table_name),
        )
        table_exists: bool = cur.fetchone()[0]

        columns_added:      list[str] = []
        local_count_before: int       = 0

        if not table_exists:
            # ── CREATE TABLE ──────────────────────────────────────────────
            col_chunks = [
                pg_sql.SQL("{} {}").format(pg_sql.Identifier(c), pg_sql.SQL(_pg_type(c, v)))
                for c, v in first.items()
            ]
            cur.execute(
                pg_sql.SQL("CREATE TABLE {} ({})").format(
                    tbl_ref,
                    pg_sql.SQL(", ").join(col_chunks),
                )
            )
            log.info("Created table %s.%s", sync_schema, table_name)
            action = "created"

        else:
            # ── Table exists — find missing columns ───────────────────────
            cur.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s""",
                (sync_schema, table_name),
            )
            existing_cols = {row[0] for row in cur.fetchall()}

            for col, val in first.items():
                if col not in existing_cols:
                    pg_t = _pg_type(col, val, for_alter=True)
                    cur.execute(
                        pg_sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}").format(
                            tbl_ref,
                            pg_sql.Identifier(col),
                            pg_sql.SQL(pg_t),
                        )
                    )
                    columns_added.append(col)
                    log.info("Added column '%s' (%s) to %s.%s", col, pg_t, sync_schema, table_name)

            # Row count before sync
            cur.execute(pg_sql.SQL("SELECT COUNT(*) FROM {}").format(tbl_ref))
            local_count_before = cur.fetchone()[0]
            action = "synced"

        # ── UPSERT all records ─────────────────────────────────────────────
        rows_upserted = 0
        for row in records:
            cols_joined = pg_sql.SQL(", ").join(pg_sql.Identifier(c) for c in row)
            phs = pg_sql.SQL(", ").join([pg_sql.Placeholder() for _ in row])
            vals = [_safe_val(v) for v in row.values()]

            if has_id:
                update_cols = [c for c in row if c != "id"]
                if update_cols:
                    set_parts = pg_sql.SQL(", ").join(
                        pg_sql.SQL("{} = EXCLUDED.{}").format(
                            pg_sql.Identifier(c), pg_sql.Identifier(c)
                        )
                        for c in update_cols
                    )
                else:
                    set_parts = pg_sql.SQL('"id" = EXCLUDED."id"')
                stmt = pg_sql.SQL(
                    "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}"
                ).format(tbl_ref, cols_joined, phs, set_parts)
            else:
                stmt = pg_sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    tbl_ref, cols_joined, phs
                )

            cur.execute(stmt, vals)
            rows_upserted += 1

        # Row count after sync
        cur.execute(pg_sql.SQL("SELECT COUNT(*) FROM {}").format(tbl_ref))
        local_count_after: int = cur.fetchone()[0]

        # NL→SQL uses DB_USER (read pool); sync uses admin. Without SELECT grant,
        # information_schema is empty for the reader → 0 tables in UI / FAISS.
        _grant_reader_select_on_table(cur, table_name)

        conn.commit()
        log.info(
            f"Sync '{table_name}': action={action}, "
            f"rows_upserted={rows_upserted}, "
            f"columns_added={columns_added}, "
            f"count {local_count_before}→{local_count_after}"
        )
        return {
            "action":             action,
            "rows_upserted":      rows_upserted,
            "columns_added":      columns_added,
            "local_count_before": local_count_before,
            "local_count_after":  local_count_after,
        }

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Legacy helper (used by /import-table endpoint) ────────────────────────────

def import_table(table_name: str, records: list[dict]) -> int:
    """Thin wrapper around sync_table for backward compatibility."""
    result = sync_table(table_name, records)
    return result["rows_upserted"]
