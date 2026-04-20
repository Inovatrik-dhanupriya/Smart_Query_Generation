"""
schema/extractor.py — PostgreSQL schema metadata extraction.
Extracts full PostgreSQL schema metadata:
  - Columns  (PK / nullable / default)
  - Foreign Keys
  - UNIQUE constraints
  - CHECK constraints
  - Indexes
  - ENUM / DOMAIN types
  - Sample rows

Multi-schema support:
  Tables are identified as "schema.table_name" when they live outside the
  default public schema.  Set DB_SCHEMAS in .env to restrict which schemas
  are scanned (comma-separated, e.g. "public,sales_schema").
  By default all non-system schemas are included.
"""
import json
import os
import re

from db import get_cursor
from utils.env import load_app_env

load_app_env()

_SYNC_SCHEMA_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")

# Schemas to include — read from env, default = all non-system schemas
_SYSTEM_SCHEMAS = {"pg_catalog", "information_schema", "pg_toast"}
_ENV_SCHEMAS    = os.getenv("DB_SCHEMAS", "")   # e.g. "public,Chocolate_Sales_schema"


def get_sync_target_schema() -> str:
    """
    Schema where table_importer creates/syncs tables (CREATE, GRANT).
    Independent of DB_SCHEMAS (which only filters NL→SQL metadata scanning).
    """
    s = (os.getenv("DB_SYNC_SCHEMA", "public") or "public").strip()
    if not _SYNC_SCHEMA_RE.match(s):
        raise ValueError(
            f"DB_SYNC_SCHEMA must be a valid PostgreSQL identifier (got {s!r})."
        )
    return s


def schema_scan_description() -> str:
    """Human-readable description of which schemas extract_full_schema() scans."""
    raw = _ENV_SCHEMAS.strip()
    return raw if raw else "(all non-system schemas)"


def _schema_filter() -> tuple[str, tuple]:
    """
    Returns (WHERE clause snippet, params) for filtering by allowed schemas.
    """
    if _ENV_SCHEMAS.strip():
        allowed = tuple(s.strip() for s in _ENV_SCHEMAS.split(",") if s.strip())
        placeholders = ",".join(["%s"] * len(allowed))
        return f"table_schema IN ({placeholders})", allowed
    else:
        # Exclude only system schemas — include everything else
        excl = tuple(_SYSTEM_SCHEMAS)
        placeholders = ",".join(["%s"] * len(excl))
        return f"table_schema NOT IN ({placeholders})", excl


def _table_key(schema: str, table: str) -> str:
    """Returns 'table' for public schema, 'schema.table' otherwise."""
    return table if schema == "public" else f"{schema}.{table}"


# ── Tables ────────────────────────────────────────────────────────────────────

def get_tables() -> list[tuple[str, str]]:
    """Returns list of (schema_name, table_name) tuples."""
    where, params = _schema_filter()
    with get_cursor() as cur:
        cur.execute(f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE {where}
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """, params)
        return [(row["table_schema"], row["table_name"]) for row in cur.fetchall()]


# ── Columns (PK / nullable / default) ────────────────────────────────────────

def get_columns(schema_name: str, table: str) -> list[dict]:
    with get_cursor() as cur:
        cur.execute("""
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
        """, (schema_name, table, schema_name, table))
        return [dict(r) for r in cur.fetchall()]


# ── Foreign Keys ──────────────────────────────────────────────────────────────

def get_foreign_keys(schema_name: str, table: str) -> list[dict]:
    with get_cursor() as cur:
        cur.execute("""
            SELECT
                kcu.column_name,
                ccu.table_name  AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema    = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema    = %s
              AND tc.table_name      = %s;
        """, (schema_name, table))
        return [dict(r) for r in cur.fetchall()]


# ── UNIQUE constraints ────────────────────────────────────────────────────────

def get_unique_constraints(schema_name: str, table: str) -> list[dict]:
    with get_cursor() as cur:
        cur.execute("""
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
        """, (schema_name, table))
        rows = cur.fetchall()
        return [{"constraint_name": r["constraint_name"], "columns": list(r["columns"])}
                for r in rows]


# ── CHECK constraints ─────────────────────────────────────────────────────────

def get_check_constraints(schema_name: str, table: str) -> list[dict]:
    with get_cursor() as cur:
        cur.execute("""
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
        """, (schema_name, table))
        return [dict(r) for r in cur.fetchall()]


# ── Indexes ───────────────────────────────────────────────────────────────────

def get_indexes(schema_name: str, table: str) -> list[dict]:
    with get_cursor() as cur:
        cur.execute("""
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
        """, (schema_name, table))
        return [dict(r) for r in cur.fetchall()]


# ── ENUM / DOMAIN types ───────────────────────────────────────────────────────

def get_enum_types() -> dict[str, list[str]]:
    """Returns all ENUM types across all allowed schemas."""
    where, params = _schema_filter()
    with get_cursor() as cur:
        cur.execute(f"""
            SELECT
                t.typname        AS enum_name,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS enum_values
            FROM pg_type t
            JOIN pg_enum e        ON t.oid          = e.enumtypid
            JOIN pg_namespace ns  ON t.typnamespace = ns.oid
            WHERE ns.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
            GROUP BY t.typname
            ORDER BY t.typname;
        """)
        return {r["enum_name"]: list(r["enum_values"]) for r in cur.fetchall()}


def get_domain_types() -> list[dict]:
    """Returns all DOMAIN types across all allowed schemas."""
    with get_cursor() as cur:
        cur.execute("""
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
        """)
        return [dict(r) for r in cur.fetchall()]


# ── Sample rows ───────────────────────────────────────────────────────────────

def get_sample_rows(schema_name: str, table: str, n: int = 3) -> list[dict]:
    with get_cursor() as cur:
        cur.execute(f'SELECT * FROM "{schema_name}"."{table}" LIMIT %s;', (n,))
        return [dict(r) for r in cur.fetchall()]


# ── Main extractor ────────────────────────────────────────────────────────────

def _infer_logical_fks(schema: dict) -> None:
    """
    When tables are created by the sync tool they have no DB-level FK constraints.
    This function infers logical foreign keys from column naming patterns and
    injects them into each table's 'foreign_keys' list so FK expansion works.

    Pattern rule:
      • Column name ends with ``_id`` → strip the suffix, resolve the referenced
        table by exact name, then plural (+ ``s``), then singular (strip trailing ``s``),
        case-insensitive against keys in the schema.
    Only adds a logical FK if:
      • The column is NOT already covered by a real FK constraint.
      • The referenced table actually exists in the schema.
    """
    all_table_keys  = set(schema["tables"].keys())
    # Build a lowercase name → real key mapping for fast lookup
    lower_to_key    = {k.lower(): k for k in all_table_keys}

    def _find_table(name: str) -> str | None:
        """Return the real table key for a candidate name, or None."""
        for candidate in (name, name + "s", name.rstrip("s")):
            key = candidate.lower()
            if key in lower_to_key:
                return lower_to_key[key]
        return None

    for table_key, meta in schema["tables"].items():
        real_fks_cols = {fk["column_name"] for fk in meta.get("foreign_keys", [])}
        logical_fks   = []

        for col in meta.get("columns", []):
            col_name = col["column_name"]
            if not col_name.endswith("_id") or col_name in real_fks_cols:
                continue

            # Derive candidate table name from column name
            candidate = col_name[:-3]          # strip "_id"
            ref_key   = _find_table(candidate)
            if ref_key and ref_key != table_key:
                logical_fks.append({
                    "column_name":    col_name,
                    "foreign_table":  ref_key,
                    "foreign_column": "id",
                    "inferred":       True,    # flag so we can distinguish from real FKs
                })

        if logical_fks:
            meta["foreign_keys"] = meta.get("foreign_keys", []) + logical_fks


def extract_full_schema() -> dict:
    """
    Returns:
    {
      "enums":   {...},
      "domains": [...],
      "tables": {
        "sales":                    {...},   # public schema → key = table name
        "Chocolate_Sales_schema.customers": {...},  # other schemas → key = schema.table
        ...
      }
    }
    After building from DB metadata, logical FKs are inferred from _id column
    naming patterns so that FK expansion works even without DB-level constraints.
    """
    schema = {
        "enums":   get_enum_types(),
        "domains": get_domain_types(),
        "tables":  {},
    }

    for (schema_name, table) in get_tables():
        key = _table_key(schema_name, table)
        schema["tables"][key] = {
            "schema_name":        schema_name,
            "table_name":         table,
            "columns":            get_columns(schema_name, table),
            "foreign_keys":       get_foreign_keys(schema_name, table),
            "unique_constraints": get_unique_constraints(schema_name, table),
            "check_constraints":  get_check_constraints(schema_name, table),
            "indexes":            get_indexes(schema_name, table),
            "sample_rows":        get_sample_rows(schema_name, table),
        }

    # Infer logical FK links from _id column naming (works even without DB constraints)
    _infer_logical_fks(schema)

    return schema


# ── Table catalog (compact summary for agent-mode table selection) ────────────

def build_table_catalog(schema: dict) -> str:
    """
    Builds a compact but information-rich summary of EVERY table in the DB.
    Used by the agent LLM to select which tables are relevant for a question
    without the user ever needing to mention table names.

    Format per table:
      • table_name  (N columns)
        Key columns: col1, col2, ...
        FK links:    col → other_table.col
        Sample:      {col: val, ...}
    """
    import json as _json
    tables = schema.get("tables", {})
    lines  = [f"=== DATABASE TABLE CATALOG ({len(tables)} table(s)) ===\n"]

    for table, meta in tables.items():
        cols   = meta.get("columns", [])
        fks    = meta.get("foreign_keys", [])
        sample = meta.get("sample_rows", [])

        # Primary key columns first, then others, max 12 shown
        pk_cols    = [c["column_name"] for c in cols if c.get("is_primary_key")]
        other_cols = [c["column_name"] for c in cols if not c.get("is_primary_key")]
        key_cols   = (pk_cols + other_cols)[:12]
        extra      = len(cols) - 12

        lines.append(f"• {table}  ({len(cols)} columns)")
        lines.append(f"  Columns : {', '.join(key_cols)}"
                     + (f"  … +{extra} more" if extra > 0 else ""))

        if fks:
            fk_str = ", ".join(
                f"{fk['column_name']} → {fk['foreign_table']}.{fk['foreign_column']}"
                for fk in fks
            )
            lines.append(f"  FK links: {fk_str}")

        if sample:
            # Show first sample row, non-null values only, max 8 fields
            non_null = {k: v for k, v in sample[0].items() if v is not None}
            preview  = dict(list(non_null.items())[:8])
            lines.append(f"  Sample  : {_json.dumps(preview, default=str)}")

        lines.append("")

    return "\n".join(lines)


# ── Schema → plain text (for Gemini embedding + prompt injection) ─────────────

def schema_to_text(schema: dict) -> dict[str, str]:
    """
    Converts each table's full metadata to a rich plain-text description
    used for FAISS embedding and LLM prompt injection.
    """
    enums   = schema.get("enums", {})
    domains = schema.get("domains", [])

    descriptions: dict[str, str] = {}

    for table, meta in schema["tables"].items():
        lines: list[str] = [f"Table: {table}"]

        # ── Columns ──────────────────────────────────────────────────────────
        lines.append("Columns:")
        for c in meta["columns"]:
            tags = []
            if c["is_primary_key"]:          tags.append("PK")
            if c["is_nullable"] == "NO":     tags.append("NOT NULL")
            if c["column_default"] is not None:
                tags.append(f"DEFAULT {c['column_default']}")

            # resolve ENUM labels inline
            udt = c.get("udt_name", "")
            if udt in enums:
                tags.append(f"ENUM({', '.join(enums[udt])})")

            tag_str = f"  [{', '.join(tags)}]" if tags else ""
            lines.append(f"  - {c['column_name']} ({c['data_type']}){tag_str}")

        # ── Foreign Keys ──────────────────────────────────────────────────────
        if meta["foreign_keys"]:
            lines.append("Foreign Keys:")
            for fk in meta["foreign_keys"]:
                lines.append(
                    f"  - {fk['column_name']} -> {fk['foreign_table']}.{fk['foreign_column']}"
                )

        # ── UNIQUE constraints ────────────────────────────────────────────────
        if meta["unique_constraints"]:
            lines.append("Unique Constraints:")
            for uc in meta["unique_constraints"]:
                lines.append(f"  - UNIQUE ({', '.join(uc['columns'])})")

        # ── CHECK constraints ─────────────────────────────────────────────────
        if meta["check_constraints"]:
            lines.append("Check Constraints:")
            for cc in meta["check_constraints"]:
                lines.append(f"  - CHECK {cc['check_clause']}")

        # ── Indexes ───────────────────────────────────────────────────────────
        non_pk_indexes = [ix for ix in meta["indexes"] if not ix["is_primary"]]
        if non_pk_indexes:
            lines.append("Indexes:")
            for ix in non_pk_indexes:
                unique_tag = "UNIQUE " if ix["is_unique"] else ""
                cols = ", ".join(ix["columns"]) if ix["columns"] else ""
                lines.append(f"  - {unique_tag}INDEX {ix['index_name']} ON ({cols})")

        # ── Sample rows hint (with non-null values) ───────────────────────────
        sample = meta.get("sample_rows", [])
        if sample:
            lines.append("Sample rows (non-null values, up to 15 fields):")
            for row in sample[:2]:
                non_null = {k: v for k, v in row.items() if v is not None}
                preview = dict(list(non_null.items())[:15])
                lines.append(f"  {preview}")

        descriptions[table] = "\n".join(lines)

    # ── Append global ENUM / DOMAIN info ─────────────────────────────────────
    if enums:
        for table in descriptions:
            descriptions[table] += (
                "\n\nDatabase ENUMs:\n" +
                "\n".join(f"  - {k}: {', '.join(v)}" for k, v in enums.items())
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
    schema = extract_full_schema()
    print(json.dumps(schema, indent=2, default=str))
