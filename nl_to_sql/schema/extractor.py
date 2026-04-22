"""
schema/extractor.py — PostgreSQL schema metadata extraction (session-scoped).
Credentials and schema/table scope come from the UI — not from environment variables.
"""
from __future__ import annotations

import json
import re
from typing import Iterable

from db import get_cursor

_SYNC_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")

_SYSTEM_SCHEMAS = frozenset({"pg_catalog", "information_schema", "pg_toast"})


def validate_pg_identifier(name: str) -> str:
    """Validate a PostgreSQL identifier (schema / table name). Raises ValueError if invalid."""
    s = (name or "").strip()
    if not _SYNC_SCHEMA_RE.match(s):
        raise ValueError(f"Invalid PostgreSQL identifier: {name!r}")
    return s


def schema_scan_description(allowed_schemas: list[str] | None) -> str:
    """Human-readable description of which schemas are scanned."""
    if allowed_schemas:
        return ", ".join(allowed_schemas)
    return "(all non-system schemas)"


def _schema_filter_clause(allowed_schemas: list[str] | None) -> tuple[str, tuple]:
    """
    Returns (WHERE fragment for information_schema.tables, params).
    ``allowed_schemas`` None or empty → all schemas except system catalogs.
    """
    if allowed_schemas:
        cleaned = [validate_pg_identifier(s) for s in allowed_schemas]
        placeholders = ",".join(["%s"] * len(cleaned))
        return f"table_schema IN ({placeholders})", tuple(cleaned)
    excl = tuple(_SYSTEM_SCHEMAS)
    placeholders = ",".join(["%s"] * len(excl))
    return f"table_schema NOT IN ({placeholders})", excl


def _table_key(schema: str, table: str) -> str:
    """Returns 'table' for public schema, 'schema.table' otherwise."""
    return table if schema == "public" else f"{schema}.{table}"


# ── Tables ────────────────────────────────────────────────────────────────────

def get_tables(
    session_id: str,
    allowed_schemas: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Returns list of (schema_name, table_name) tuples."""
    where, params = _schema_filter_clause(allowed_schemas)
    with get_cursor(session_id) as cur:
        cur.execute(
            f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE {where}
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
            """,
            params,
        )
        return [(row["table_schema"], row["table_name"]) for row in cur.fetchall()]


def list_tables_for_pairs(
    session_id: str,
    pairs: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Return (schema, table) tuples that exist in the DB from the given allowlist.
    """
    if not pairs:
        return []
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for sch, tbl in pairs:
        sk = validate_pg_identifier(sch)
        tk = validate_pg_identifier(tbl)
        key = (sk, tk)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    with get_cursor(session_id) as cur:
        ok: list[tuple[str, str]] = []
        for sk, tk in out:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s AND table_type = 'BASE TABLE'
                """,
                (sk, tk),
            )
            if cur.fetchone():
                ok.append((sk, tk))
        return ok


# ── Columns (PK / nullable / default) ────────────────────────────────────────

def get_columns(session_id: str, schema_name: str, table: str) -> list[dict]:
    with get_cursor(session_id) as cur:
        cur.execute(
            """
            SELECT
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.column_default,
                CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END AS is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT ku.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage ku
                  ON tc.constraint_name = ku.constraint_name
                 AND tc.table_name      = ku.table_name
                 AND tc.table_schema    = ku.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND tc.table_schema    = %s
                  AND tc.table_name      = %s
            ) pk ON c.column_name = pk.column_name
            WHERE c.table_schema = %s
              AND c.table_name   = %s
            ORDER BY c.ordinal_position;
            """,
            (schema_name, table, schema_name, table),
        )
        return [dict(r) for r in cur.fetchall()]


def get_foreign_keys(session_id: str, schema_name: str, table: str) -> list[dict]:
    with get_cursor(session_id) as cur:
        cur.execute(
            """
            SELECT
                l_a.attname::text AS column_name,
                fn.nspname::text AS foreign_schema,
                fr.relname::text AS foreign_table,
                r_a.attname::text AS foreign_column
            FROM pg_constraint c
            JOIN pg_class l_t ON c.conrelid = l_t.oid
            JOIN pg_namespace ln ON l_t.relnamespace = ln.oid
            JOIN LATERAL unnest(c.conkey, c.confkey) AS u(loc_attnum, ref_attnum) ON true
            JOIN pg_attribute l_a ON l_a.attrelid = l_t.oid AND l_a.attnum = u.loc_attnum
            JOIN pg_class fr ON c.confrelid = fr.oid
            JOIN pg_namespace fn ON fr.relnamespace = fn.oid
            JOIN pg_attribute r_a ON r_a.attrelid = fr.oid AND r_a.attnum = u.ref_attnum
            WHERE c.contype = 'f'
              AND ln.nspname = %s
              AND l_t.relname = %s
              AND NOT l_a.attisdropped
              AND NOT r_a.attisdropped;
            """,
            (schema_name, table),
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for r in rows:
        fk_key = _table_key(r["foreign_schema"], r["foreign_table"])
        out.append({
            "column_name":    r["column_name"],
            "foreign_table":  fk_key,
            "foreign_column": r["foreign_column"],
        })
    return out


def get_unique_constraints(session_id: str, schema_name: str, table: str) -> list[dict]:
    with get_cursor(session_id) as cur:
        cur.execute(
            """
            SELECT
                tc.constraint_name,
                array_agg(kcu.column_name ORDER BY kcu.ordinal_position) AS columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'UNIQUE'
              AND tc.table_schema    = %s
              AND tc.table_name      = %s
            GROUP BY tc.constraint_name;
            """,
            (schema_name, table),
        )
        rows = cur.fetchall()
        return [{"constraint_name": r["constraint_name"], "columns": list(r["columns"])}
                for r in rows]


def get_check_constraints(session_id: str, schema_name: str, table: str) -> list[dict]:
    with get_cursor(session_id) as cur:
        cur.execute(
            """
            SELECT
                tc.constraint_name,
                cc.check_clause
            FROM information_schema.table_constraints tc
            JOIN information_schema.check_constraints cc
              ON tc.constraint_name   = cc.constraint_name
             AND tc.constraint_schema = cc.constraint_schema
            WHERE tc.constraint_type = 'CHECK'
              AND tc.table_schema    = %s
              AND tc.table_name      = %s
              AND tc.constraint_name NOT LIKE '%%_not_null';
            """,
            (schema_name, table),
        )
        return [dict(r) for r in cur.fetchall()]


def get_indexes(session_id: str, schema_name: str, table: str) -> list[dict]:
    with get_cursor(session_id) as cur:
        cur.execute(
            """
            SELECT
                i.relname                          AS index_name,
                ix.indisunique                     AS is_unique,
                ix.indisprimary                    AS is_primary,
                array_agg(a.attname ORDER BY k.n)  AS columns,
                pg_get_indexdef(ix.indexrelid)     AS index_def
            FROM pg_class t
            JOIN pg_index ix      ON t.oid          = ix.indrelid
            JOIN pg_class i       ON ix.indexrelid  = i.oid
            JOIN pg_namespace ns  ON t.relnamespace = ns.oid
            JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
              ON true
            JOIN pg_attribute a   ON a.attrelid     = t.oid
                                 AND a.attnum       = k.attnum
            WHERE ns.nspname = %s
              AND t.relname  = %s
              AND t.relkind  = 'r'
            GROUP BY i.relname, ix.indisunique, ix.indisprimary, ix.indexrelid
            ORDER BY i.relname;
            """,
            (schema_name, table),
        )
        return [dict(r) for r in cur.fetchall()]


def get_enum_types(
    session_id: str,
    allowed_schemas: list[str] | None = None,
) -> dict[str, list[str]]:
    """ENUM types, optionally limited to namespaces in ``allowed_schemas``."""
    with get_cursor(session_id) as cur:
        if allowed_schemas:
            sch = [validate_pg_identifier(s) for s in allowed_schemas]
            ph = ",".join(["%s"] * len(sch))
            cur.execute(
                f"""
                SELECT
                    t.typname        AS enum_name,
                    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
                FROM pg_type t
                JOIN pg_enum e        ON t.oid          = e.enumtypid
                JOIN pg_namespace ns  ON t.typnamespace = ns.oid
                WHERE ns.nspname IN ({ph})
                GROUP BY t.typname
                ORDER BY t.typname;
                """,
                tuple(sch),
            )
        else:
            cur.execute(
                """
                SELECT
                    t.typname        AS enum_name,
                    array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
                FROM pg_type t
                JOIN pg_enum e        ON t.oid          = e.enumtypid
                JOIN pg_namespace ns  ON t.typnamespace = ns.oid
                WHERE ns.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                GROUP BY t.typname
                ORDER BY t.typname;
                """
            )
        return {r["enum_name"]: list(r["enum_values"]) for r in cur.fetchall()}


def get_domain_types(
    session_id: str,
    allowed_schemas: list[str] | None = None,
) -> list[dict]:
    with get_cursor(session_id) as cur:
        if allowed_schemas:
            sch = [validate_pg_identifier(s) for s in allowed_schemas]
            ph = ",".join(["%s"] * len(sch))
            cur.execute(
                f"""
                SELECT
                    d.domain_name,
                    d.data_type         AS base_type,
                    d.character_maximum_length,
                    dc.check_clause
                FROM information_schema.domains d
                LEFT JOIN information_schema.domain_constraints dcon
                  ON d.domain_name   = dcon.domain_name
                 AND d.domain_schema = dcon.constraint_schema
                LEFT JOIN information_schema.check_constraints dc
                  ON dcon.constraint_name = dc.constraint_name
                WHERE d.domain_schema IN ({ph})
                ORDER BY d.domain_name;
                """,
                tuple(sch),
            )
        else:
            cur.execute(
                """
                SELECT
                    d.domain_name,
                    d.data_type         AS base_type,
                    d.character_maximum_length,
                    dc.check_clause
                FROM information_schema.domains d
                LEFT JOIN information_schema.domain_constraints dcon
                  ON d.domain_name   = dcon.domain_name
                 AND d.domain_schema = dcon.constraint_schema
                LEFT JOIN information_schema.check_constraints dc
                  ON dcon.constraint_name = dc.constraint_name
                WHERE d.domain_schema NOT IN ('pg_catalog','information_schema','pg_toast')
                ORDER BY d.domain_name;
                """
            )
        return [dict(r) for r in cur.fetchall()]


def get_sample_rows(session_id: str, schema_name: str, table: str, n: int = 3) -> list[dict]:
    ss = validate_pg_identifier(schema_name)
    tt = validate_pg_identifier(table)
    with get_cursor(session_id) as cur:
        cur.execute(f'SELECT * FROM "{ss}"."{tt}" LIMIT %s;', (n,))
        return [dict(r) for r in cur.fetchall()]


# ── Main extractor ────────────────────────────────────────────────────────────

def _infer_logical_fks(schema: dict) -> None:
    """
    Infer logical foreign keys from ``_id`` column naming when DB has no FK constraints.
    """
    all_table_keys = set(schema["tables"].keys())
    lower_to_key = {k.lower(): k for k in all_table_keys}

    def _find_table(name: str) -> str | None:
        for candidate in (name, name + "s", name.rstrip("s")):
            key = candidate.lower()
            if key in lower_to_key:
                return lower_to_key[key]
        return None

    for table_key, meta in schema["tables"].items():
        real_fks_cols = {fk["column_name"] for fk in meta.get("foreign_keys", [])}
        logical_fks = []

        for col in meta.get("columns", []):
            col_name = col["column_name"]
            if not col_name.endswith("_id") or col_name in real_fks_cols:
                continue

            candidate = col_name[:-3]
            ref_key = _find_table(candidate)
            if ref_key and ref_key != table_key:
                logical_fks.append({
                    "column_name":    col_name,
                    "foreign_table":  ref_key,
                    "foreign_column": "id",
                    "inferred":       True,
                })

        if logical_fks:
            meta["foreign_keys"] = meta.get("foreign_keys", []) + logical_fks


def extract_full_schema(
    session_id: str,
    *,
    allowed_schemas: list[str] | None = None,
    only_tables: Iterable[tuple[str, str]] | None = None,
) -> dict:
    """
    Extract metadata for tables in scope.

    ``only_tables``: optional iterable of (schema_name, table_name). When set and
    non-empty, only those tables are loaded (must exist). When None or empty,
    all base tables matching ``allowed_schemas`` are loaded.
    """
    scope_pairs: list[tuple[str, str]]
    if only_tables is not None:
        raw = list(only_tables)
        if len(raw) == 0:
            scope_pairs = get_tables(session_id, allowed_schemas)
        else:
            scope_pairs = list_tables_for_pairs(session_id, raw)
            if not scope_pairs:
                raise ValueError(
                    "None of the selected tables exist or are visible with the current connection."
                )
    else:
        scope_pairs = get_tables(session_id, allowed_schemas)

    enum_schema_scope = (
        list({p[0] for p in scope_pairs}) if scope_pairs else allowed_schemas
    )

    schema = {
        "enums":   get_enum_types(session_id, enum_schema_scope or allowed_schemas),
        "domains": get_domain_types(session_id, allowed_schemas),
        "tables":  {},
    }

    for schema_name, table in scope_pairs:
        key = _table_key(schema_name, table)
        schema["tables"][key] = {
            "schema_name":        schema_name,
            "table_name":         table,
            "columns":            get_columns(session_id, schema_name, table),
            "foreign_keys":       get_foreign_keys(session_id, schema_name, table),
            "unique_constraints": get_unique_constraints(session_id, schema_name, table),
            "check_constraints":  get_check_constraints(session_id, schema_name, table),
            "indexes":            get_indexes(session_id, schema_name, table),
            "sample_rows":        get_sample_rows(session_id, schema_name, table),
        }

    _infer_logical_fks(schema)

    return schema


def build_table_catalog(schema: dict) -> str:
    """Compact catalog string for the LLM table-selection agent."""
    import json as _json

    tables = schema.get("tables", {})
    lines = [f"=== DATABASE TABLE CATALOG ({len(tables)} table(s)) ===\n"]

    for table, meta in tables.items():
        cols = meta.get("columns", [])
        fks = meta.get("foreign_keys", [])
        sample = meta.get("sample_rows", [])

        pk_cols = [c["column_name"] for c in cols if c.get("is_primary_key")]
        other_cols = [c["column_name"] for c in cols if not c.get("is_primary_key")]
        key_cols = (pk_cols + other_cols)[:12]
        extra = len(cols) - 12

        lines.append(f"• {table}  ({len(cols)} columns)")
        lines.append(
            f"  Columns : {', '.join(key_cols)}" + (f"  … +{extra} more" if extra > 0 else "")
        )

        if fks:
            fk_str = ", ".join(
                f"{fk['column_name']} → {fk['foreign_table']}.{fk['foreign_column']}"
                for fk in fks
            )
            lines.append(f"  FK links: {fk_str}")

        if sample:
            non_null = {k: v for k, v in sample[0].items() if v is not None}
            preview = dict(list(non_null.items())[:8])
            lines.append(f"  Sample  : {_json.dumps(preview, default=str)}")

        lines.append("")

    return "\n".join(lines)


def schema_to_text(schema: dict) -> dict[str, str]:
    """Rich per-table text for FAISS + prompt injection."""
    enums = schema.get("enums", {})
    domains = schema.get("domains", [])

    descriptions: dict[str, str] = {}

    for table, meta in schema["tables"].items():
        lines: list[str] = [f"Table: {table}"]

        lines.append("Columns:")
        for c in meta["columns"]:
            tags = []
            if c["is_primary_key"]:
                tags.append("PK")
            if c["is_nullable"] == "NO":
                tags.append("NOT NULL")
            if c["column_default"] is not None:
                tags.append(f"DEFAULT {c['column_default']}")

            udt = c.get("udt_name", "")
            if udt in enums:
                tags.append(f"ENUM({', '.join(enums[udt])})")

            tag_str = f"  [{', '.join(tags)}]" if tags else ""
            lines.append(f"  - {c['column_name']} ({c['data_type']}){tag_str}")

        if meta["foreign_keys"]:
            lines.append("Foreign Keys:")
            for fk in meta["foreign_keys"]:
                lines.append(
                    f"  - {fk['column_name']} -> {fk['foreign_table']}.{fk['foreign_column']}"
                )

        if meta["unique_constraints"]:
            lines.append("Unique Constraints:")
            for uc in meta["unique_constraints"]:
                lines.append(f"  - UNIQUE ({', '.join(uc['columns'])})")

        if meta["check_constraints"]:
            lines.append("Check Constraints:")
            for cc in meta["check_constraints"]:
                lines.append(f"  - CHECK {cc['check_clause']}")

        non_pk_indexes = [ix for ix in meta["indexes"] if not ix["is_primary"]]
        if non_pk_indexes:
            lines.append("Indexes:")
            for ix in non_pk_indexes:
                unique_tag = "UNIQUE " if ix["is_unique"] else ""
                cols = ", ".join(ix["columns"]) if ix["columns"] else ""
                lines.append(f"  - {unique_tag}INDEX {ix['index_name']} ON ({cols})")

        sample = meta.get("sample_rows", [])
        if sample:
            lines.append("Sample rows (non-null values, up to 15 fields):")
            for row in sample[:2]:
                non_null = {k: v for k, v in row.items() if v is not None}
                preview = dict(list(non_null.items())[:15])
                lines.append(f"  {preview}")

        descriptions[table] = "\n".join(lines)

    if enums:
        for table in descriptions:
            descriptions[table] += (
                "\n\nDatabase ENUMs:\n"
                + "\n".join(f"  - {k}: {', '.join(v)}" for k, v in enums.items())
            )

    if domains:
        for table in descriptions:
            domain_lines = []
            for d in domains:
                chk = f" CHECK {d['check_clause']}" if d.get("check_clause") else ""
                domain_lines.append(f"  - {d['domain_name']} ({d['base_type']}){chk}")
            descriptions[table] += "\n\nDatabase DOMAINs:\n" + "\n".join(domain_lines)

    return descriptions


if __name__ == "__main__":
    print("Use API /db/activate after connecting — no standalone extract without session.")
