"""
sql_engine.py  —  Validation + Execution + Pagination

SQL Result Cache
  - Identical SELECT queries (same SQL text) return cached results for
    SQL_CACHE_TTL seconds (see ``utils.config.sql_cache_ttl_seconds``).
  - Cache is keyed on the normalised SQL string (lowercased, whitespace collapsed).
  - Pagination queries (/sql/page) are NOT cached — they carry OFFSET so each
    page is already a distinct query.
  - Cache is in-process memory (dict). It clears on server restart.
"""
from __future__ import annotations

import hashlib
import re
import time

from db import get_cursor
from utils.config import (
    sql_cache_ttl_seconds,
    sql_max_page_size,
    sql_max_query_length,
    sql_preview_limit,
    sql_statement_timeout_sec,
)

# ── SQL result cache ──────────────────────────────────────────────────────────

# { sql_hash: {"result": dict, "expires_at": float} }
_sql_cache: dict[str, dict] = {}


def _cache_key(sql: str) -> str:
    """Normalise SQL → stable MD5 cache key."""
    normalised = re.sub(r"\s+", " ", sql.strip().lower())
    return hashlib.md5(normalised.encode()).hexdigest()


def _cache_get(sql: str) -> dict | None:
    """Return cached result if still fresh, else None."""
    if sql_cache_ttl_seconds() <= 0:
        return None
    entry = _sql_cache.get(_cache_key(sql))
    if entry and time.time() < entry["expires_at"]:
        return entry["result"]
    return None


def _cache_set(sql: str, result: dict) -> None:
    """Store result in cache with TTL."""
    ttl = sql_cache_ttl_seconds()
    if ttl <= 0:
        return
    _sql_cache[_cache_key(sql)] = {
        "result":     result,
        "expires_at": time.time() + ttl,
    }


def cache_clear() -> int:
    """Clear all cached results. Returns number of entries removed."""
    count = len(_sql_cache)
    _sql_cache.clear()
    return count


def cache_stats() -> dict:
    """Return basic cache statistics."""
    now   = time.time()
    alive = sum(1 for e in _sql_cache.values() if now < e["expires_at"])
    return {"total_entries": len(_sql_cache), "live_entries": alive,
            "ttl_seconds": sql_cache_ttl_seconds()}


# ── Validation ────────────────────────────────────────────────────────────────

BLOCKED_KEYWORDS = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|EXEC|EXECUTE|CALL"
    r"|COPY|VACUUM|ANALYZE|EXPLAIN|SHOW|SET|RESET|NOTIFY|LISTEN|UNLISTEN|CHECKPOINT)\b",
    re.IGNORECASE,
)

SQL_INJECTION_PATTERNS = re.compile(
    r"(;|\-\-|\/\*|\*\/|xp_|SLEEP\s*\(|BENCHMARK\s*\(|LOAD_FILE|INTO\s+OUTFILE"
    r"|pg_read_file|pg_ls_dir|pg_stat_file|COPY\s+.*\s+TO|dblink|lo_import|lo_export"
    r"|UNION\s+(ALL\s+)?SELECT)",
    re.IGNORECASE,
)

class SQLValidationError(Exception):
    pass


def validate_sql(sql: str) -> str:
    """
    Raises SQLValidationError if query is unsafe.
    Returns cleaned (no trailing semicolon) SQL on success.
    """
    sql = sql.strip().rstrip(";")

    max_len = sql_max_query_length()
    if len(sql) > max_len:
        raise SQLValidationError(
            f"Query exceeds maximum allowed length of {max_len} characters "
            f"(set SQL_MAX_QUERY_LENGTH in .env)."
        )

    if not sql.upper().startswith("SELECT"):
        raise SQLValidationError("Only SELECT queries are allowed.")

    if BLOCKED_KEYWORDS.search(sql):
        raise SQLValidationError(
            "Query contains a blocked keyword. Only read-only SELECTs are permitted."
        )

    if SQL_INJECTION_PATTERNS.search(sql):
        raise SQLValidationError("Query contains suspicious patterns.")

    return sql


# ── LIMIT / OFFSET helpers ────────────────────────────────────────────────────

_LIMIT_RE  = re.compile(r"\bLIMIT\s+(\d+)\b",  re.IGNORECASE)
_OFFSET_RE = re.compile(r"\bOFFSET\s+\d+\b",   re.IGNORECASE)


def _extract_limit(sql: str) -> int | None:
    """Return the integer in LIMIT N, or None if not present."""
    m = _LIMIT_RE.search(sql)
    return int(m.group(1)) if m else None


def _strip_limit_offset(sql: str) -> str:
    sql = _LIMIT_RE.sub("", sql)
    sql = _OFFSET_RE.sub("", sql)
    return sql.strip()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_query(sql: str) -> tuple[list[str], list[dict], int]:
    """Execute sql, return (columns, rows, elapsed_ms)."""
    start = time.perf_counter()
    to_sec = sql_statement_timeout_sec()
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(f"SET statement_timeout = '{to_sec}s';")
        cur.execute(sql)
        rows = cur.fetchall()
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    if not rows:
        return [], [], elapsed_ms

    columns   = list(rows[0].keys())
    safe_rows = [
        {k: (str(v) if not isinstance(v, (int, float, type(None))) else v)
         for k, v in row.items()}
        for row in rows
    ]
    return columns, safe_rows, elapsed_ms


def _count_query(base_sql: str) -> int:
    """Wrap base_sql in COUNT(*) to get total rows without fetching data."""
    count_sql = f"SELECT COUNT(*) AS total FROM ({base_sql}) AS _count_wrap"
    to_sec = sql_statement_timeout_sec()
    with get_cursor(dict_cursor=True) as cur:
        cur.execute(f"SET statement_timeout = '{to_sec}s';")
        cur.execute(count_sql)
        row = cur.fetchone()
    return int(row["total"]) if row else 0


# ── Public API ────────────────────────────────────────────────────────────────

def execute_sql(sql: str) -> dict:
    """
    Chat-UI execution with result caching.

    - Identical queries (same SQL text) return cached results (SQL_CACHE_TTL).
    - If the SQL already contains LIMIT (… OFFSET …), it is executed as-is (after
      validation) so the UI does not treat it like an unbounded scan. total_count
      is then the size of that result (len(rows)), not COUNT(*) of the whole table.
    - If there is no LIMIT, caps at SQL_PREVIEW_LIMIT and total_count = full row count
      of the inner query (for pagination / charts).
    """
    sql = validate_sql(sql)

    # Return cached result if available
    cached = _cache_get(sql)
    if cached is not None:
        cached["cached"] = True
        return cached

    original_limit = _extract_limit(sql)

    # Bounded NL→SQL answer: run the statement the model wrote (keeps LIMIT/OFFSET).
    preview_cap = sql_preview_limit()
    if original_limit is not None:
        if original_limit > preview_cap:
            raise SQLValidationError(
                f"LIMIT {original_limit} exceeds preview cap {preview_cap} "
                f"(raise SQL_PREVIEW_LIMIT in .env if needed)."
            )
        columns, rows, elapsed_ms = _run_query(sql)
        n = len(rows)
        result = {
            "columns":      columns,
            "rows":         rows,
            "row_count":    n,
            "total_count":  n,
            "has_more":     False,
            "execution_ms": elapsed_ms,
            "cached":       False,
        }
        _cache_set(sql, result)
        return result

    # No LIMIT — preview cap + full cardinality for pagination / chart expansion
    base_sql = _strip_limit_offset(sql)
    total = _count_query(base_sql)
    cap = sql_preview_limit()
    fetch_limit = min(cap, total) if total else cap
    paged_sql = f"{base_sql} LIMIT {fetch_limit} OFFSET 0"
    columns, rows, elapsed_ms = _run_query(paged_sql)

    result = {
        "columns":      columns,
        "rows":         rows,
        "row_count":    len(rows),
        "total_count":  total,
        "has_more":     total > fetch_limit,
        "execution_ms": elapsed_ms,
        "cached":       False,
    }
    _cache_set(sql, result)
    return result


def execute_sql_page(sql: str, page: int = 1, page_size: int = 500) -> dict:
    """
    Paginated execution for large datasets.

    Args:
        sql:       Raw SELECT query (LIMIT/OFFSET stripped and rewritten).
        page:      1-based page number.
        page_size: Rows per page (capped at SQL_MAX_PAGE_SIZE from .env).

    Returns:
      { columns, rows, row_count, total_count, page, page_size,
        total_pages, has_next, has_prev, execution_ms }
    """
    sql       = validate_sql(sql)
    page      = max(1, page)
    page_size = min(max(1, page_size), sql_max_page_size())
    offset    = (page - 1) * page_size

    base_sql  = _strip_limit_offset(sql)
    total     = _count_query(base_sql)

    paged_sql = f"{base_sql} LIMIT {page_size} OFFSET {offset}"
    columns, rows, elapsed_ms = _run_query(paged_sql)

    total_pages = max(1, -(-total // page_size))   # ceiling division

    return {
        "columns":      columns,
        "rows":         rows,
        "row_count":    len(rows),
        "total_count":  total,
        "page":         page,
        "page_size":    page_size,
        "total_pages":  total_pages,
        "has_next":     page < total_pages,
        "has_prev":     page > 1,
        "execution_ms": elapsed_ms,
    }
