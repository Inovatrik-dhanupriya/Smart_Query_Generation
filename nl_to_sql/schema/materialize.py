"""
Create a PostgreSQL database (if needed) and tables from an in-memory schema dict
(produced by ``file_schema.schema_from_uploaded_json``).
"""
from __future__ import annotations

import logging
import re
from typing import Any

from psycopg2 import sql

from db import PgCredentials, open_write_connection
from schema.extractor import validate_pg_identifier

log = logging.getLogger(__name__)

_DB_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def validate_database_name(name: str) -> str:
    s = (name or "").strip()
    if not _DB_NAME_RE.match(s):
        raise ValueError(
            f"Invalid database name {name!r}. Use letters, digits, underscore; max 63 chars."
        )
    return s


def _map_pg_type(data_type: str) -> str:
    dt = (data_type or "text").lower().strip()
    if "(" in dt and dt.split("(")[0].strip() in (
        "character varying",
        "varchar",
        "char",
        "numeric",
        "decimal",
    ):
        return dt.upper()
    mapping = {
        "text": "TEXT",
        "varchar": "TEXT",
        "string": "TEXT",
        "int": "INTEGER",
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "float": "DOUBLE PRECISION",
        "double": "DOUBLE PRECISION",
        "real": "REAL",
        "numeric": "NUMERIC",
        "decimal": "NUMERIC",
        "bool": "BOOLEAN",
        "boolean": "BOOLEAN",
        "date": "DATE",
        "timestamp": "TIMESTAMP",
        "timestamptz": "TIMESTAMPTZ",
        "time": "TIME",
        "uuid": "UUID",
        "json": "JSONB",
        "jsonb": "JSONB",
        "bytea": "BYTEA",
        "serial": "SERIAL",
        "bigserial": "BIGSERIAL",
    }
    if dt in mapping:
        return mapping[dt]
    return "TEXT"


def _is_integer_like_pg_type(pg_t: str) -> bool:
    base = pg_t.split("(")[0].strip().upper()
    return base in (
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "SERIAL",
        "BIGSERIAL",
    )


def ensure_database_exists(creds: PgCredentials, new_db: str) -> bool:
    """
    Connect to maintenance DB, CREATE DATABASE if missing.
    Returns True if database was created, False if it already existed.
    """
    validate_database_name(new_db)
    created = False
    last_err: Exception | None = None
    for maint in ("postgres", "template1"):
        mc = PgCredentials(
            host=creds.host,
            port=creds.port,
            user=creds.user,
            password=creds.password,
            database=maint,
        )
        try:
            conn = open_write_connection(mc)
        except Exception as e:
            last_err = e
            continue
        try:
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (new_db,),
            )
            if cur.fetchone():
                return False
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(new_db)))
            created = True
            log.info("Created database %s", new_db)
            return created
        finally:
            conn.close()
    raise RuntimeError(
        f"Could not connect to postgres/template1 to create database {new_db!r}: {last_err}"
    ) from last_err


def create_tables_from_schema(creds: PgCredentials, schema: dict[str, Any]) -> int:
    """
    Run CREATE SCHEMA / CREATE TABLE for every table in ``schema["tables"]``.
    Returns number of tables created (attempted).
    """
    conn = open_write_connection(creds)
    n = 0
    try:
        cur = conn.cursor()
        for _key, meta in schema.get("tables", {}).items():
            sch_raw = (meta.get("schema_name") or "public").strip()
            tbl_raw = (meta.get("table_name") or "").strip()
            if not tbl_raw:
                continue
            sch = validate_pg_identifier(sch_raw)
            tbl = validate_pg_identifier(tbl_raw)

            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(sch))
            )
            coldefs: list[sql.Composable] = []
            for c in meta.get("columns") or []:
                if not isinstance(c, dict):
                    continue
                cn = (c.get("column_name") or c.get("name") or "").strip()
                if not cn:
                    continue
                pg_t = _map_pg_type(str(c.get("data_type") or "text"))
                if cn == "id" and _is_integer_like_pg_type(pg_t):
                    coldefs.append(
                        sql.SQL("{} {} PRIMARY KEY").format(
                            sql.Identifier(cn), sql.SQL(pg_t)
                        )
                    )
                else:
                    coldefs.append(
                        sql.SQL("{} {}").format(sql.Identifier(cn), sql.SQL(pg_t)),
                    )
            if not coldefs:
                coldefs.append(sql.SQL("id BIGINT PRIMARY KEY"))

            stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(sch),
                sql.Identifier(tbl),
                sql.SQL(", ").join(coldefs),
            )
            cur.execute(stmt)
            n += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return n


def provision_schema_to_database(
    creds: PgCredentials,
    target_database: str,
    schema: dict[str, Any],
) -> dict[str, Any]:
    """
    Create ``target_database`` if it does not exist, then create all tables from ``schema``.
    """
    target_database = validate_database_name(target_database)
    created_db = ensure_database_exists(creds, target_database)
    target_creds = PgCredentials(
        host=creds.host,
        port=creds.port,
        user=creds.user,
        password=creds.password,
        database=target_database,
    )
    n_tables = create_tables_from_schema(target_creds, schema)
    notes: list[str] = []
    if not created_db:
        notes.append(
            "Database already existed on the server — it was reused (not an error)."
        )
    else:
        notes.append("A new database was created.")
    notes.append(
        "Tables use CREATE TABLE IF NOT EXISTS: existing tables are left unchanged; "
        "only missing tables are created."
    )
    return {
        "target_database": target_database,
        "created_database": created_db,
        "tables_created": n_tables,
        "notes": notes,
    }
