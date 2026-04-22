"""
schema/importer.py
Fetch data from a remote SQL-passthrough API and sync into PostgreSQL using
credentials supplied by the caller (UI session) — not from environment variables.
"""
from __future__ import annotations

import json as _json
import logging
import os
import re
import threading
import time
from collections.abc import Callable
from typing import Any

import psycopg2
from psycopg2 import sql as pg_sql
import requests

from db import PgCredentials, open_write_connection
from schema.extractor import validate_pg_identifier
from utils.config import remote_api_timeout_sec

log = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _has_single_column_unique_on_id(
    cur: Any, sync_ns: str, table_name: str
) -> bool:
    """True if ``id`` alone is covered by a PRIMARY KEY or UNIQUE constraint."""
    cur.execute(
        """
        SELECT 1
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_schema = kcu.constraint_schema
         AND tc.constraint_name = kcu.constraint_name
         AND tc.table_name = kcu.table_name
        WHERE tc.table_schema = %s AND tc.table_name = %s
          AND tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE')
          AND kcu.column_name = 'id'
        GROUP BY tc.constraint_schema, tc.table_name, tc.constraint_name
        HAVING COUNT(*) = 1
        LIMIT 1
        """,
        (sync_ns, table_name),
    )
    return cur.fetchone() is not None


def _pg_type(col: str, val: Any, *, for_alter: bool = False) -> str:
    if col == "id" and not for_alter:
        return "BIGINT PRIMARY KEY"
    if isinstance(val, bool):
        return "BOOLEAN"
    if isinstance(val, int):
        return "BIGINT"
    if isinstance(val, float):
        return "DOUBLE PRECISION"
    if isinstance(val, (dict, list)):
        return "TEXT"
    return "TEXT"


def _safe_val(val: Any) -> Any:
    if isinstance(val, (dict, list)):
        return _json.dumps(val, ensure_ascii=False, default=str)
    return val


def count_physical_tables(creds: PgCredentials, sync_schema: str) -> int:
    """Count ordinary tables in the given schema (write connection)."""
    ss = validate_pg_identifier(sync_schema)
    conn = open_write_connection(creds)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*)::int
            FROM pg_catalog.pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relkind = 'r'
            """,
            (ss,),
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0
    finally:
        conn.close()


def fetch_from_remote_api(query: str, api_url: str | None = None) -> list[dict]:
    url = (api_url or os.getenv("REMOTE_API_URL", "")).strip()
    if not url:
        raise ValueError(
            "Set REMOTE_API_URL in .env to your SQL passthrough API base URL, "
            "or pass api_url when calling import/sync."
        )
    log.info("Remote API  POST %s  query=%r …", url, query[:80])

    resp = requests.post(
        url,
        json={"query": query},
        headers={"Content-Type": "application/json"},
        timeout=remote_api_timeout_sec(),
    )
    resp.raise_for_status()

    body = resp.json()
    if isinstance(body, list):
        return body
    for key in ("entity", "data", "rows", "result", "records"):
        if key in body and isinstance(body[key], list):
            return body[key]

    raise ValueError(f"Remote API returned unexpected shape: {list(body.keys())}")


def parse_schema_json(data: Any) -> list[str]:
    if isinstance(data, list):
        return [str(x) for x in data if isinstance(x, str) and x.strip()]

    if isinstance(data, dict):
        if "tables" in data:
            t = data["tables"]
            if isinstance(t, list):
                return [str(x) for x in t if isinstance(x, str) and x.strip()]
            if isinstance(t, dict):
                return list(t.keys())
        return list(data.keys())

    return []


def sync_table(
    table_name: str,
    records: list[dict],
    *,
    creds: PgCredentials,
    sync_schema: str,
) -> dict:
    if not _SAFE_NAME_RE.match(table_name):
        raise ValueError(
            f"Invalid table name {table_name!r}. "
            "Only letters, digits and underscores are allowed."
        )
    if not records:
        raise ValueError("No records returned from the remote API.")

    first = records[0]
    has_id = "id" in first
    sync_ns = validate_pg_identifier(sync_schema)
    tbl_ref = pg_sql.SQL("{}.{}").format(
        pg_sql.Identifier(sync_ns), pg_sql.Identifier(table_name)
    )

    conn = open_write_connection(creds)
    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = off;")

        cur.execute(
            """SELECT EXISTS (
                   SELECT 1 FROM information_schema.tables
                   WHERE table_schema = %s AND table_name = %s
               )""",
            (sync_ns, table_name),
        )
        table_exists: bool = cur.fetchone()[0]

        columns_added: list[str] = []
        local_count_before: int = 0

        if not table_exists:
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
            log.info("Created table %s.%s", sync_ns, table_name)
            action = "created"

        else:
            cur.execute(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema = %s AND table_name = %s""",
                (sync_ns, table_name),
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
                    log.info("Added column '%s' (%s) to %s.%s", col, pg_t, sync_ns, table_name)

            cur.execute(pg_sql.SQL("SELECT COUNT(*) FROM {}").format(tbl_ref))
            local_count_before = cur.fetchone()[0]
            action = "synced"

        upsert_on_id = False
        if has_id:
            upsert_on_id = _has_single_column_unique_on_id(cur, sync_ns, table_name)
            if not upsert_on_id:
                try:
                    cur.execute(
                        pg_sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (id)").format(
                            tbl_ref
                        )
                    )
                    upsert_on_id = True
                    log.info(
                        "Added PRIMARY KEY (id) on %s.%s for upsert sync",
                        sync_ns,
                        table_name,
                    )
                except Exception as ex:
                    log.warning(
                        "Cannot ADD PRIMARY KEY (id) on %s.%s (%s); "
                        "using delete+insert per row",
                        sync_ns,
                        table_name,
                        ex,
                    )

        if has_id:
            by_id: dict[Any, dict] = {}
            for r in records:
                by_id[r["id"]] = r
            records = list(by_id.values())

        rows_upserted = 0
        for row in records:
            cols_joined = pg_sql.SQL(", ").join(pg_sql.Identifier(c) for c in row)
            phs = pg_sql.SQL(", ").join([pg_sql.Placeholder() for _ in row])
            vals = [_safe_val(v) for v in row.values()]

            if has_id and upsert_on_id:
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
            elif has_id:
                cur.execute(
                    pg_sql.SQL("DELETE FROM {} WHERE {} = %s").format(
                        tbl_ref, pg_sql.Identifier("id")
                    ),
                    (row["id"],),
                )
                stmt = pg_sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    tbl_ref, cols_joined, phs
                )
            else:
                stmt = pg_sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                    tbl_ref, cols_joined, phs
                )

            cur.execute(stmt, vals)
            rows_upserted += 1

        cur.execute(pg_sql.SQL("SELECT COUNT(*) FROM {}").format(tbl_ref))
        local_count_after: int = cur.fetchone()[0]

        conn.commit()
        log.info(
            "Sync '%s': action=%s, rows_upserted=%s, columns_added=%s, count %s→%s",
            table_name,
            action,
            rows_upserted,
            columns_added,
            local_count_before,
            local_count_after,
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


def import_table(
    table_name: str,
    records: list[dict],
    *,
    creds: PgCredentials,
    sync_schema: str,
) -> int:
    result = sync_table(table_name, records, creds=creds, sync_schema=sync_schema)
    return result["rows_upserted"]


def _select_star_sql(schema_name: str, table_name: str, *, limit: int | None) -> str:
    """Build ``SELECT * FROM schema.table`` for the remote SQL passthrough API."""
    ss = validate_pg_identifier(schema_name)
    tn = validate_pg_identifier(table_name)
    q = f'SELECT * FROM "{ss}"."{tn}"'
    if limit is not None and limit > 0:
        q += f" LIMIT {int(limit)}"
    return q


def _incremental_id_cursor(
    creds: PgCredentials, sync_schema: str, table_name: str
) -> tuple[str, int | None]:
    """
    Decide how to page remote ``SELECT`` when the local table has a numeric ``id``.

    Returns ``(kind, max_id)``:
    - ``("none", None)`` — no usable ``id`` column; caller uses plain ``SELECT … LIMIT``.
    - ``("empty", None)`` — local table has ``id`` but 0 rows (first batch).
    - ``("gt", m)`` — local rows exist; fetch remote rows with ``id > m``.
    """
    if not _SAFE_NAME_RE.match(table_name):
        return ("none", None)
    ss = validate_pg_identifier(sync_schema)
    tn = validate_pg_identifier(table_name)
    conn = open_write_connection(creds)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'id'
            """,
            (ss, tn),
        )
        row = cur.fetchone()
        if not row:
            return ("none", None)
        dt = (row[0] or "").lower()
        if "serial" in dt or dt in ("bigint", "integer", "smallint", "bigserial"):
            pass
        elif "int" not in dt:
            return ("none", None)
        cur.execute(
            pg_sql.SQL("SELECT COUNT(*)::bigint, MAX({}) FROM {}.{}").format(
                pg_sql.Identifier("id"),
                pg_sql.Identifier(ss),
                pg_sql.Identifier(tn),
            )
        )
        cnt_raw, mx = cur.fetchone()
        cnt = int(cnt_raw or 0)
        if cnt == 0:
            return ("empty", None)
        if mx is None:
            return ("none", None)
        return ("gt", int(mx))
    except Exception as ex:
        log.debug("incremental_id_cursor %s.%s: %s", sync_schema, table_name, ex)
        return ("none", None)
    finally:
        conn.close()


def _remote_fetch_sql(
    schema_name: str,
    table_name: str,
    *,
    limit: int | None,
    inc_kind: str,
    inc_max_id: int | None,
) -> str:
    """
    SQL sent to the remote passthrough. When ``inc_kind == "gt"``, only rows with
    ``id`` greater than the local maximum are fetched (append / incremental load).
    """
    ss = validate_pg_identifier(schema_name)
    tn = validate_pg_identifier(table_name)
    base = f'SELECT * FROM "{ss}"."{tn}"'
    if inc_kind == "gt" and inc_max_id is not None:
        m = int(inc_max_id)
        q = f'{base} WHERE "id" > {m} ORDER BY "id" ASC'
    elif inc_kind == "empty":
        q = f'{base} ORDER BY "id" ASC'
    else:
        q = base
    if limit is not None and limit > 0:
        q += f" LIMIT {int(limit)}"
    return q


def bulk_sync_tables_from_remote(
    creds: PgCredentials,
    tables: list[tuple[str, str]],
    api_url: str,
    *,
    row_limit: int = 5_000,
    incremental: bool = True,
    cancel_event: threading.Event | None = None,
    pause_event: threading.Event | None = None,
    on_progress: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    """
    For each ``(schema, table)``, run a remote ``SELECT`` via ``fetch_from_remote_api``,
    then ``sync_table`` into the local database (adds missing columns and upserts rows).

    When ``incremental`` is True (default), if the local table has a numeric ``id`` column,
    the remote query fetches only rows **after** the local ``MAX(id)`` (``WHERE id > …``),
    so repeated runs with a small ``row_limit`` **append** new rows instead of re-fetching
    the first page and upserting the same keys.

    ``pause_event``: when set, blocks between tables until cleared (resume).
    ``cancel_event``: when set, stops before the next table and returns ``canceled: True``.
    """
    url = (api_url or "").strip()
    if not url:
        raise ValueError("remote API URL is empty")

    per_table: dict[str, Any] = {}
    errors: list[str] = []
    total_rows = 0
    n = len(tables)

    for i, (sch, tbl) in enumerate(tables):
        if cancel_event is not None and cancel_event.is_set():
            return {
                "tables_processed": n,
                "rows_upserted_total": total_rows,
                "per_table": per_table,
                "errors": errors,
                "canceled": True,
                "stopped_at_index": i,
            }
        while pause_event is not None and pause_event.is_set():
            time.sleep(0.25)
            if cancel_event is not None and cancel_event.is_set():
                return {
                    "tables_processed": n,
                    "rows_upserted_total": total_rows,
                    "per_table": per_table,
                    "errors": errors,
                    "canceled": True,
                    "stopped_at_index": i,
                }
        key = f"{sch}.{tbl}"
        if on_progress is not None:
            on_progress(i + 1, n, key)
        try:
            lim = row_limit if row_limit > 0 else None
            if incremental:
                ik, imx = _incremental_id_cursor(creds, sch, tbl)
                q = _remote_fetch_sql(
                    sch, tbl, limit=lim, inc_kind=ik, inc_max_id=imx
                )
                fetch_mode = f"incremental:{ik}"
            else:
                q = _select_star_sql(sch, tbl, limit=lim)
                fetch_mode = "replace_window"
            log.info("bulk_sync %s remote query: %s", key, q[:500])
            rows = fetch_from_remote_api(q, url)
            if not rows:
                per_table[key] = {
                    "status": "empty",
                    "fetch_mode": fetch_mode,
                    "rows_upserted": 0,
                }
                continue
            st = sync_table(tbl, rows, creds=creds, sync_schema=sch)
            per_table[key] = {"status": "ok", "fetch_mode": fetch_mode, **st}
            total_rows += int(st.get("rows_upserted") or 0)
        except Exception as e:
            log.warning("bulk_sync %s: %s", key, e)
            errors.append(f"{key}: {e}")
            per_table[key] = {"status": "error", "reason": str(e)}

    return {
        "tables_processed": n,
        "rows_upserted_total": total_rows,
        "per_table": per_table,
        "errors": errors,
    }
