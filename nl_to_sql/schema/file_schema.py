"""
Build an in-memory schema dict from user-uploaded JSON (no live DB required).

Accepts many shapes: ``tables`` map, root ``columns`` map, legacy top-level keys,
arrays of table names, nested ``schema.tables``, etc.

When a table has no column definitions, a synthetic ``id`` column is added by default
so NL→SQL always loads. Set env ``NL_SCHEMA_STRICT_COLUMNS=1`` to skip tables instead.
"""
from __future__ import annotations

import os
from typing import Any

from schema.extractor import _infer_logical_fks, _table_key

# Metadata / non-table keys at JSON root (legacy top-level discovery)
_META_SKIP = frozenset({
    "database_name",
    "enums",
    "domains",
    "columns_by_table",
    "column_map",
    "table_columns",
    "columns_by_name",
    "fields_by_table",
    "version",
    "meta",
    "metadata",
    "info",
    "config",
})

_STRICT_COLUMNS = os.getenv("NL_SCHEMA_STRICT_COLUMNS", "").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def _norm_column(entry: dict[str, Any]) -> dict[str, Any]:
    name = str(entry.get("column_name") or entry.get("name") or "").strip()
    if not name:
        raise ValueError("Each column needs column_name (or name).")
    dt = str(entry.get("data_type") or entry.get("type") or "text")
    udt = str(entry.get("udt_name") or entry.get("udt") or "")
    return {
        "column_name":     name,
        "data_type":       dt,
        "udt_name":        udt,
        "is_nullable":     str(entry.get("is_nullable", "YES")),
        "column_default":  entry.get("column_default"),
        "is_primary_key":  bool(entry.get("is_primary_key", False)),
    }


def _placeholder_columns() -> list[dict[str, Any]]:
    return [_norm_column({"column_name": "id", "data_type": "bigint"})]


def _parse_columns_flexible(cols_raw: Any) -> list[dict[str, Any]]:
    if cols_raw is None:
        return []
    if not isinstance(cols_raw, list) or not cols_raw:
        return []
    el0 = cols_raw[0]
    if isinstance(el0, str):
        out: list[dict[str, Any]] = []
        for c in cols_raw:
            s = str(c).strip()
            if s:
                out.append(_norm_column({"column_name": s, "data_type": "text"}))
        return out
    if isinstance(el0, dict):
        return [_norm_column(c) for c in cols_raw if isinstance(c, dict)]
    return []


def _coerce_meta(meta: Any) -> dict[str, Any]:
    if isinstance(meta, dict):
        return meta
    if isinstance(meta, list):
        return {"columns": meta}
    return {}


def _normalize_tables_blob(blob: dict[Any, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for k, v in blob.items():
        key = str(k).strip()
        if not key:
            continue
        if isinstance(v, dict):
            out[key] = v
        elif isinstance(v, list):
            out[key] = {"columns": v}
        else:
            out[key] = {}
    return out


def _wrap_list_root(rows: list[Any]) -> dict[str, Any]:
    tables: dict[str, dict[str, Any]] = {}
    for item in rows:
        if isinstance(item, str) and item.strip():
            tables[item.strip()] = {}
        elif isinstance(item, dict):
            nm = item.get("name") or item.get("table") or item.get("table_name")
            if nm and str(nm).strip():
                tables[str(nm).strip()] = item
    return {"tables": tables}


def _discover_raw_tables(data: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Best-effort: find table name → metadata dict."""

    t = data.get("tables")
    if isinstance(t, dict) and t:
        return _normalize_tables_blob(t)

    sch = data.get("schema")
    if isinstance(sch, dict):
        t = sch.get("tables")
        if isinstance(t, dict) and t:
            return _normalize_tables_blob(t)

    cr = data.get("columns")
    if isinstance(cr, dict) and cr:
        sample = next(iter(cr.values()), None)
        if isinstance(sample, list):
            return {str(k): {"columns": v} for k, v in cr.items() if isinstance(v, list)}

    for arr_key in ("table_list", "tableNames", "table_names", "names", "entities"):
        arr = data.get(arr_key)
        if isinstance(arr, list) and arr:
            return {str(x).strip(): {} for x in arr if isinstance(x, (str, int)) and str(x).strip()}

    raw = {
        k: v
        for k, v in data.items()
        if k not in _META_SKIP and not (k == "columns" and isinstance(v, dict))
    }
    only_tables: dict[str, dict[str, Any]] = {}
    for k, v in raw.items():
        if isinstance(v, dict):
            only_tables[str(k)] = v
    if only_tables:
        return only_tables

    return {}


def _collect_global_column_maps(data: dict[str, Any]) -> dict[str, list[Any]]:
    merged: dict[str, list[Any]] = {}

    def _ingest_block(blk: Any) -> None:
        if not isinstance(blk, dict):
            return
        for k, v in blk.items():
            if not isinstance(v, list) or not v:
                continue
            if not (isinstance(v[0], (dict, str)) if v else True):
                continue
            merged[str(k)] = v

    for key in (
        "columns_by_table",
        "column_map",
        "table_columns",
        "columns_by_name",
        "fields_by_table",
    ):
        _ingest_block(data.get(key))

    colroot = data.get("columns")
    if isinstance(colroot, dict) and colroot:
        first = next(iter(colroot.values()), None)
        if isinstance(first, list):
            _ingest_block(colroot)

    return merged


def _lookup_global_columns(
    global_cols: dict[str, list[Any]],
    tkey: str,
    schema_name: str,
    table_name: str,
) -> list[Any] | None:
    fq = f"{schema_name}.{table_name}" if schema_name and table_name else None
    for key in (tkey, fq, table_name, tkey.split(".")[-1] if "." in tkey else None):
        if key and key in global_cols:
            return global_cols[key]
    return None


def schema_from_uploaded_json(data: Any) -> tuple[dict[str, Any], list[str]]:
    """
    Build schema dict from arbitrary client JSON.

    - Root **object** with ``tables``, ``schema.tables``, ``columns`` map, legacy keys, etc.
    - Root **array** of table name strings (or small objects with ``name`` / ``table``).

    By default, tables with no column definitions get a synthetic ``id`` column so the
    file always loads. Set ``NL_SCHEMA_STRICT_COLUMNS=1`` to skip those tables instead.
    """
    skipped: list[str] = []
    placeholder_count = 0

    if isinstance(data, list):
        data = _wrap_list_root(data)
    if not isinstance(data, dict):
        raise ValueError("Schema JSON must be an object or an array of table names.")

    raw_tables = _discover_raw_tables(data)
    if not raw_tables:
        raise ValueError(
            "Could not find any tables in the JSON. "
            'Expected a "tables" object, a "columns" map of lists, '
            "top-level table keys, or a JSON array of table name strings."
        )

    global_cols = _collect_global_column_maps(data)
    tables_out: dict[str, Any] = {}

    for tkey, meta in raw_tables.items():
        meta = _coerce_meta(meta)
        if not isinstance(meta, dict):
            skipped.append(f"{tkey!r} (could not read table entry)")
            continue

        schema_name = (meta.get("schema_name") or "").strip()
        table_name = (meta.get("table_name") or "").strip()
        if not schema_name or not table_name:
            if "." in str(tkey):
                parts = str(tkey).split(".", 1)
                schema_name = schema_name or parts[0].strip()
                table_name = table_name or parts[1].strip()
            else:
                schema_name = schema_name or "public"
                table_name = table_name or str(tkey).strip()

        cols_raw: Any = meta.get("columns") or meta.get("cols") or meta.get("fields")
        if not isinstance(cols_raw, list) or not cols_raw:
            gl = _lookup_global_columns(global_cols, str(tkey), schema_name, table_name)
            if gl is not None:
                cols_raw = gl

        columns = _parse_columns_flexible(cols_raw)
        if not columns:
            if _STRICT_COLUMNS:
                skipped.append(f"{tkey!r} (empty or missing columns)")
                continue
            columns = _placeholder_columns()
            placeholder_count += 1

        key = _table_key(schema_name, table_name)

        fks = []
        for fk in meta.get("foreign_keys") or []:
            if not isinstance(fk, dict):
                continue
            try:
                fks.append({
                    "column_name":    fk["column_name"],
                    "foreign_table":  fk["foreign_table"],
                    "foreign_column": fk.get("foreign_column", "id"),
                })
            except (KeyError, TypeError):
                continue

        tables_out[key] = {
            "schema_name":        schema_name,
            "table_name":         table_name,
            "columns":            columns,
            "foreign_keys":       fks,
            "unique_constraints": meta.get("unique_constraints") or [],
            "check_constraints":  meta.get("check_constraints") or [],
            "indexes":            meta.get("indexes") or [],
            "sample_rows":        meta.get("sample_rows") or [],
        }

    if not tables_out:
        detail = "No usable tables found in schema JSON."
        if skipped:
            tail = "; ".join(skipped[:25])
            if len(skipped) > 25:
                tail += f" … (+{len(skipped) - 25} more)"
            detail += f" Skipped: {tail}"
        raise ValueError(detail)

    if placeholder_count and not _STRICT_COLUMNS:
        skipped.insert(
            0,
            f"(info) {placeholder_count} table(s) had no column list — added synthetic `id` for NL→SQL.",
        )

    schema = {
        "enums":   data.get("enums") or {},
        "domains": data.get("domains") or [],
        "tables":  tables_out,
    }
    _infer_logical_fks(schema)
    return schema, skipped
