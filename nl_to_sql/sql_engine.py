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

import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Parenthesis
from sqlparse.tokens import Keyword

from db import get_cursor
from utils.config import (
    sql_cache_ttl_seconds,
    sql_max_page_size,
    sql_max_query_length,
    sql_preview_limit,
    sql_statement_timeout_sec,
)

# ── SQL result cache ──────────────────────────────────────────────────────────

# { cache_key: {"result": dict, "expires_at": float} }
_sql_cache: dict[str, dict] = {}


def _cache_key(session_id: str, sql: str) -> str:
    """Session + normalised SQL → stable cache key (no cross-session leakage)."""
    normalised = re.sub(r"\s+", " ", sql.strip().lower())
    raw = f"{session_id}|{normalised}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(session_id: str, sql: str) -> dict | None:
    """Return cached result if still fresh, else None."""
    if sql_cache_ttl_seconds() <= 0:
        return None
    entry = _sql_cache.get(_cache_key(session_id, sql))
    if entry and time.time() < entry["expires_at"]:
        return entry["result"]
    return None


def _cache_set(session_id: str, sql: str, result: dict) -> None:
    """Store result in cache with TTL."""
    ttl = sql_cache_ttl_seconds()
    if ttl <= 0:
        return
    _sql_cache[_cache_key(session_id, sql)] = {
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


def _pg_quote_ident(ident: str) -> str:
    return '"' + ident.replace('"', '""') + '"'


def fix_postgresql_mixed_case_identifiers(sql: str, schema: dict) -> str:
    """
    PostgreSQL folds unquoted identifiers to lowercase, so ``Chocolate_Sales_schema.store``
    becomes ``chocolate_sales_schema.store`` and misses the real relation.

    Rewrite unqualified ``schema.table`` / folded spellings to quoted
    ``\"Schema\".\"table\"`` using names from ``schema`` metadata (information_schema).
    """
    replacements: list[tuple[str, str]] = []
    for _key, meta in schema.get("tables", {}).items():
        sch = (meta.get("schema_name") or "public").strip()
        tbl = (meta.get("table_name") or "").strip()
        if not tbl:
            continue
        if sch != "public":
            good = f"{_pg_quote_ident(sch)}.{_pg_quote_ident(tbl)}"
            for bad in (f"{sch}.{tbl}", f"{sch.lower()}.{tbl.lower()}"):
                if bad != good:
                    replacements.append((bad, good))
        # public.* single-segment names: skip blind replace (would corrupt ORDER BY, etc.)

    replacements.sort(key=lambda x: len(x[0]), reverse=True)
    out = sql
    for bad, good in replacements:
        if bad not in out:
            continue
        if good in out and bad == good:
            continue
        out = out.replace(bad, good)
    return out


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


def _is_from_or_join_keyword(token) -> bool:
    if token.ttype is not Keyword:
        return False
    v = token.value.upper()
    if v == "FROM":
        return True
    return "JOIN" in v


def _relation_tuples_from_identifier(
    ident: Identifier, _depth: int
) -> set[tuple[str | None, str]]:
    """Physical (schema, table) pairs from a sqlparse Identifier in FROM/JOIN."""
    out: set[tuple[str | None, str]] = set()
    inner = [t for t in ident.tokens if not t.is_whitespace]
    if not inner:
        return out
    if isinstance(inner[0], Parenthesis):
        body = str(inner[0]).strip()
        if len(body) >= 2 and body[0] == "(" and body[-1] == ")":
            inner_sql = body[1:-1].strip()
            if inner_sql.upper().startswith("SELECT") and _depth < 14:
                out.update(extract_from_join_relations(inner_sql, _depth + 1))
        return out
    parent = ident.get_parent_name()
    real = ident.get_real_name()
    if not real:
        return out
    out.add((parent, real))
    return out


def extract_from_join_relations(sql: str, _depth: int = 0) -> set[tuple[str | None, str]]:
    """
    Best-effort physical relations referenced after FROM / JOIN (including nested
    SELECTs in subqueries). Each tuple is (schema_or_none, table_name) as written.
    """
    out: set[tuple[str | None, str]] = set()
    if _depth > 14 or not (sql or "").strip():
        return out
    parsed = sqlparse.parse(sql)
    if not parsed:
        return out
    stmt = parsed[0]
    tokens = list(stmt.tokens)
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t.is_whitespace:
            i += 1
            continue
        if _is_from_or_join_keyword(t):
            i += 1
            while i < len(tokens) and tokens[i].is_whitespace:
                i += 1
            if i >= len(tokens):
                break
            nt = tokens[i]
            if isinstance(nt, IdentifierList):
                for ident in nt.get_identifiers():
                    if isinstance(ident, Identifier):
                        out.update(_relation_tuples_from_identifier(ident, _depth))
            elif isinstance(nt, Identifier):
                out.update(_relation_tuples_from_identifier(nt, _depth))
            elif isinstance(nt, Parenthesis):
                body = str(nt).strip()
                if len(body) >= 2 and body[0] == "(" and body[-1] == ")":
                    inner_sql = body[1:-1].strip()
                    if inner_sql.upper().startswith("SELECT") and _depth < 14:
                        out.update(extract_from_join_relations(inner_sql, _depth + 1))
            i += 1
            continue
        i += 1
    return out


def resolve_table_ref_to_key(
    schema_part: str | None,
    table_part: str,
    schema: dict,
) -> str | None:
    """Map a SQL relation reference to ``schema['tables']`` key, or None."""
    tbl = (table_part or "").strip().strip('"')
    if not tbl:
        return None
    sch_raw = (schema_part or "").strip().strip('"')
    tbl_l = tbl.lower()
    sch_l = sch_raw.lower() if sch_raw else ""
    for key, meta in (schema.get("tables") or {}).items():
        ms = (meta.get("schema_name") or "public").lower()
        mt = (meta.get("table_name") or "").lower()
        if mt != tbl_l:
            continue
        if not sch_raw or sch_l == "public":
            if ms == "public":
                return key
        elif ms == sch_l:
            return key
    return None


def unknown_tables_in_sql(sql: str, schema: dict) -> list[str]:
    """Human-readable relation names that are not in ``schema['tables']``."""
    unknown: list[str] = []
    seen: set[str] = set()
    for sch, tbl in sorted(extract_from_join_relations(sql), key=lambda x: (x[0] or "", x[1])):
        if resolve_table_ref_to_key(sch, tbl, schema) is not None:
            continue
        label = f"{sch}.{tbl}" if sch else tbl
        if label not in seen:
            seen.add(label)
            unknown.append(label)
    return unknown


def validate_sql_tables_against_schema(sql: str, schema: dict) -> None:
    """
    Raises SQLValidationError if FROM/JOIN references a relation that does not
    exist in the activated schema (catches common LLM table hallucinations).
    """
    bad = unknown_tables_in_sql(sql, schema)
    if bad:
        preview = ", ".join(f"`{b}`" for b in bad[:6])
        more = f" (+{len(bad) - 6} more)" if len(bad) > 6 else ""
        raise SQLValidationError(
            "SQL references table(s) that are not in the activated schema: "
            f"{preview}{more}. Use only tables you see in the schema context (T: lines)."
        )


def canonical_tables_referenced_in_sql(sql: str, schema: dict) -> list[str]:
    """Stable list of schema dict keys for relations present in FROM/JOIN."""
    keys: list[str] = []
    seen: set[str] = set()
    for sch, tbl in sorted(extract_from_join_relations(sql), key=lambda x: (x[0] or "", x[1])):
        k = resolve_table_ref_to_key(sch, tbl, schema)
        if k and k not in seen:
            seen.add(k)
            keys.append(k)
    return keys


# ── LIMIT / OFFSET helpers ────────────────────────────────────────────────────

_LIMIT_RE  = re.compile(r"\bLIMIT\s+(\d+)\b",  re.IGNORECASE)
_OFFSET_RE = re.compile(r"\bOFFSET\s+(\d+)\b", re.IGNORECASE)


def _extract_limit(sql: str) -> int | None:
    """Return the integer in LIMIT N, or None if not present."""
    m = _LIMIT_RE.search(sql)
    return int(m.group(1)) if m else None


def _extract_offset(sql: str) -> int:
    """Return integer in OFFSET N, or 0 when missing."""
    m = _OFFSET_RE.search(sql)
    return int(m.group(1)) if m else 0


def _strip_limit_offset(sql: str) -> str:
    sql = _LIMIT_RE.sub("", sql)
    sql = _OFFSET_RE.sub("", sql)
    return sql.strip()


# ── Helpers ───────────────────────────────────────────────────────────────────

def _run_query(session_id: str, sql: str) -> tuple[list[str], list[dict], int]:
    """Execute sql, return (columns, rows, elapsed_ms)."""
    start = time.perf_counter()
    to_sec = sql_statement_timeout_sec()
    with get_cursor(session_id, dict_cursor=True) as cur:
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


def _count_query(session_id: str, base_sql: str) -> int:
    """Wrap base_sql in COUNT(*) to get total rows without fetching data."""
    count_sql = f"SELECT COUNT(*) AS total FROM ({base_sql}) AS _count_wrap"
    to_sec = sql_statement_timeout_sec()
    with get_cursor(session_id, dict_cursor=True) as cur:
        cur.execute(f"SET statement_timeout = '{to_sec}s';")
        cur.execute(count_sql)
        row = cur.fetchone()
    return int(row["total"]) if row else 0


# ── Public API ────────────────────────────────────────────────────────────────

def execute_sql(sql: str, session_id: str) -> dict:
    """
    Chat-UI execution with result caching.

    - Identical queries (same SQL text) return cached results (SQL_CACHE_TTL).
    - If the SQL already contains LIMIT (… OFFSET …), execute that SQL as-is for the
      current page, but still compute ``total_count`` on the base query (without
      LIMIT/OFFSET) so the UI can paginate consistently.
    - If there is no LIMIT, caps at SQL_PREVIEW_LIMIT and total_count = full row count
      of the inner query (for pagination / charts).
    """
    sql = validate_sql(sql)

    # Return cached result if available
    cached = _cache_get(session_id, sql)
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
        columns, rows, elapsed_ms = _run_query(session_id, sql)
        n = len(rows)
        base_sql = _strip_limit_offset(sql)
        total = _count_query(session_id, base_sql)
        off = _extract_offset(sql)
        result = {
            "columns":      columns,
            "rows":         rows,
            "row_count":    n,
            "total_count":  total,
            "has_more":     (off + n) < total,
            "execution_ms": elapsed_ms,
            "cached":       False,
        }
        _cache_set(session_id, sql, result)
        return result

    # No LIMIT — preview cap + full cardinality for pagination / chart expansion
    base_sql = _strip_limit_offset(sql)
    total = _count_query(session_id, base_sql)
    cap = sql_preview_limit()
    fetch_limit = min(cap, total) if total else cap
    paged_sql = f"{base_sql} LIMIT {fetch_limit} OFFSET 0"
    columns, rows, elapsed_ms = _run_query(session_id, paged_sql)

    result = {
        "columns":      columns,
        "rows":         rows,
        "row_count":    len(rows),
        "total_count":  total,
        "has_more":     total > fetch_limit,
        "execution_ms": elapsed_ms,
        "cached":       False,
    }
    _cache_set(session_id, sql, result)
    return result


def execute_sql_page(sql: str, session_id: str, page: int = 1, page_size: int = 500) -> dict:
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
    total     = _count_query(session_id, base_sql)

    paged_sql = f"{base_sql} LIMIT {page_size} OFFSET {offset}"
    columns, rows, elapsed_ms = _run_query(session_id, paged_sql)

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
