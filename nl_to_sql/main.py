"""
main.py — NL → SQL FastAPI

Database host, database name, schema, and table names are supplied by the client
(UI); nothing is read from environment variables for connection targets.

Run (from ``nl_to_sql/``):
    uvicorn main:app --reload --port 8000
"""
# Note: avoid ``from __future__ import annotations`` here — Pydantic v2 + FastAPI
# need real class objects in route parameter annotations (e.g. ``DbConnectBody``).

import hashlib
import json as _json
import logging
import os
import re
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import bcrypt
from fastapi import FastAPI, HTTPException, Request
from fastapi import FastAPI, File, Form, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from google.genai import errors as genai_errors

from utils.config import (
    agent_table_threshold,
    cors_origins,
    db_sync_schema_default,
    llm_cache_max_entries,
    session_max_age_hours,
    session_max_turns,
)

try:
    from utils.config import allow_data_ingestion_to_connected_db
except ImportError:
    def allow_data_ingestion_to_connected_db() -> bool:
        v = (os.getenv("SMART_QUERY_ALLOW_DATA_INGESTION", "") or "").strip().lower()
        return v in ("1", "true", "yes", "on")
from embed_page import get_embed_html
from utils.env import load_app_env, package_root

load_app_env()

from db import (
    PgCredentials,
    close_pool,
    close_pool_only,
    has_pool,
    register_pool,
    start_ssh_pg_tunnel,
)
from llm import (
    expand_selected_tables_for_nl_query,
    generate_sql,
    inferred_top_k_for_query,
    select_tables_agent,
    suggest_prompts,
)
from schema.discovery import list_database_names, list_schema_names
from schema.extractor import (
    build_table_catalog,
    extract_full_schema,
    get_tables,
    schema_scan_description,
    schema_to_text,
    validate_pg_identifier,
)
from schema.file_schema import schema_from_uploaded_json
from schema.materialize import provision_schema_to_database
from schema.importer import (
    bulk_sync_tables_from_remote,
    fetch_from_remote_api,
    import_table as import_table_to_db,
    sync_table,
)
from schema.retriever import SchemaRetriever, fk_expand_seed_tables
from sql_engine import (
    SQLValidationError,
    cache_clear,
    cache_stats,
    canonical_tables_referenced_in_sql,
    execute_sql,
    execute_sql_page,
    fix_postgresql_mixed_case_identifiers,
    unknown_tables_in_sql,
    validate_sql,
    validate_sql_tables_against_schema,
)
from db import get_app_db_cursor, prepare_app_auth_backend

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

_REQUIRED_ENV = ["GEMINI_API_KEY"]


def _check_env() -> None:
    missing = [k for k in _REQUIRED_ENV if not os.getenv(k)]
    if missing:
        log.critical("Missing required environment variables: %s", missing)
        sys.exit(1)


_check_env()

# ── App-level FastAPI state (used by startup/lifespan) ───────────────────────
app_state: dict = {}

# ── Per-session NL→SQL state (key = client session_id from UI) ───────────────
nl_sessions: dict[str, dict[str, Any]] = {}

# ── Background schema upload jobs (for cancel / pause / resume during remote sync) ─
_schema_upload_jobs: dict[str, dict[str, Any]] = {}
_schema_upload_jobs_lock = threading.Lock()


def _schema_job_progress_cb(job_id: str):
    def _cb(cur: int, total: int, table_key: str) -> None:
        with _schema_upload_jobs_lock:
            j = _schema_upload_jobs.get(job_id)
            if j:
                j["sync_current"] = cur
                j["sync_total"] = total
                j["current_table"] = table_key
                j["phase"] = "remote_sync"

    return _cb


def _schema_upload_core(
    job_id: str | None,
    sid: str,
    database_name: str,
    schema: dict[str, Any],
    skipped_tables: list,
    _kc: bool,
    _mat: bool,
    _target: str,
    remote_data_url: str,
    remote_row_limit: str,
) -> dict[str, Any]:
    """
    Shared implementation for POST /schema/from-file and background jobs.
    """
    cancel_ev = pause_ev = None
    on_prog = None
    if job_id:
        with _schema_upload_jobs_lock:
            job = _schema_upload_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Unknown job_id")
        cancel_ev = job["cancel"]
        pause_ev = job["pause"]
        on_prog = _schema_job_progress_cb(job_id)

    if _mat:
        if not _kc:
            raise HTTPException(
                status_code=400,
                detail="PostgreSQL session must be connected before provisioning.",
            )
        sess_pre = _get_nl(sid)
        creds = sess_pre.get("credentials") if sess_pre else None
        if not creds:
            raise HTTPException(
                status_code=400,
                detail="No database credentials in session. POST /db/connect before materializing.",
            )
        if job_id:
            with _schema_upload_jobs_lock:
                j = _schema_upload_jobs.get(job_id)
                if j:
                    j["phase"] = "provision"
                    j["message"] = "Creating database and DDL…"
        try:
            prov = provision_schema_to_database(creds, _target, schema)
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        close_pool_only(sid)
        new_creds = PgCredentials(
            host=creds.host,
            port=creds.port,
            user=creds.user,
            password=creds.password,
            database=_target,
        )
        register_pool(sid, new_creds, read_only=True)
        sess = _ensure_nl(sid)
        sess["credentials"] = new_creds
        sess["database"] = _target

        pairs = [
            (m["schema_name"], m["table_name"])
            for m in schema.get("tables", {}).values()
        ]
        sch_set = sorted({p[0] for p in pairs})
        if job_id:
            with _schema_upload_jobs_lock:
                j = _schema_upload_jobs.get(job_id)
                if j:
                    j["phase"] = "extract_schema"
                    j["message"] = "Reading table metadata…"
        try:
            schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
        except Exception as ex:
            log.warning("Post-DDL extract_full_schema failed; using file schema: %s", ex)

        data_sync: dict[str, Any] | None = None
        _ru = (remote_data_url or "").strip()
        if _ru:
            if not allow_data_ingestion_to_connected_db():
                raise HTTPException(
                    status_code=403,
                    detail=(
                        "Remote row load into PostgreSQL is disabled. "
                        "The product stores only schema metadata in the app database and runs "
                        "queries on your connected client DB. To enable the optional row-sync "
                        "feature (not recommended for strict 'no data copy' policies), set "
                        "SMART_QUERY_ALLOW_DATA_INGESTION=1 in the API environment."
                    ),
                )
            try:
                rl = int((remote_row_limit or "5000").strip() or "5000")
            except ValueError:
                rl = 5000
            rl = max(1, min(rl, 100_000))
            if job_id:
                with _schema_upload_jobs_lock:
                    j = _schema_upload_jobs.get(job_id)
                    if j:
                        j["sync_total"] = len(pairs)
                        j["sync_current"] = 0
                        j["phase"] = "remote_sync"
                        j["message"] = "Loading rows from remote API…"
            try:
                data_sync = bulk_sync_tables_from_remote(
                    new_creds,
                    pairs,
                    _ru,
                    row_limit=rl,
                    cancel_event=cancel_ev,
                    pause_event=pause_ev,
                    on_progress=on_prog,
                )
            except Exception as e:
                log.exception("Remote data sync failed")
                data_sync = {"error": str(e)}
            try:
                schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
            except Exception as ex:
                log.warning("Post-sync extract_full_schema failed: %s", ex)

        _rebuild_nl_state(sid, schema)
        sess["execution_enabled"] = True
        sess["source"] = "live"
        sess["logical_database_name"] = _target
        sess["selected_schemas"] = sch_set
        sess["selected_pairs"] = pairs
        if _ru:
            sess["remote_data_url"] = _ru
        cache_clear()

        hint = (
            f"Created database {prov['target_database']!r} "
            f"({'new' if prov['created_database'] else 'already existed'}). "
            f"Applied DDL for {prov['tables_created']} table(s). SQL execution is enabled."
        )
        for note in prov.get("notes") or []:
            hint += " " + note
        if skipped_tables:
            preview = "; ".join(skipped_tables[:15])
            if len(skipped_tables) > 15:
                preview += " …"
            hint += f" Notes: {preview}"
        if data_sync and data_sync.get("error"):
            hint += f" Remote data sync error: {data_sync['error']}"
        elif data_sync and data_sync.get("canceled"):
            hint += " Remote data sync was stopped before all tables finished (partial load)."
        elif data_sync and "rows_upserted_total" in data_sync:
            hint += (
                f" Remote API: upserted ~{data_sync['rows_upserted_total']} row(s) "
                f"across {data_sync.get('tables_processed', 0)} table(s)."
            )
            if _ru:
                hint += (
                    " Incremental mode: for tables with a numeric `id`, each run requests rows "
                    "with `id` greater than your local `MAX(id)` (up to `row_limit` per table), "
                    "so smaller limits append new data instead of re-fetching the first page."
                )
            err_n = len(data_sync.get("errors") or [])
            if err_n:
                hint += f" ({err_n} table(s) reported fetch/sync errors — see data_sync in response.)"
        return {
            "status": "active",
            "database_name": _target,
            "table_count": len(schema.get("tables", {})),
            "tables": list(schema.get("tables", {}).keys()),
            "skipped_tables": skipped_tables,
            "execution_enabled": True,
            "materialized": prov,
            "data_sync": data_sync,
            "hint": hint,
        }

    _rebuild_nl_state(sid, schema)
    sess = _ensure_nl(sid)
    sess["execution_enabled"] = False
    sess["source"] = "file"
    sess["logical_database_name"] = database_name.strip()
    sess["selected_schemas"] = []
    sess["selected_pairs"] = list(schema.get("tables", {}).keys())
    if not _kc:
        sess["credentials"] = None
        sess["database"] = None
        close_pool(sid)
    hint = (
        "Connect to a PostgreSQL instance with the same tables, then POST /db/activate "
        "to enable query execution."
    )
    if skipped_tables:
        preview = "; ".join(skipped_tables[:20])
        if len(skipped_tables) > 20:
            preview += f" … (+{len(skipped_tables) - 20} more)"
        hint = (
            f"Skipped {len(skipped_tables)} table(s) with no column definitions: {preview}. "
            + hint
        )
    return {
        "status": "active",
        "database_name": database_name.strip(),
        "table_count": len(schema.get("tables", {})),
        "tables": list(schema.get("tables", {}).keys()),
        "skipped_tables": skipped_tables,
        "execution_enabled": False,
        "hint": hint,
    }


def _faiss_dir_for(session_id: str) -> Path:
    h = hashlib.sha256(session_id.encode("utf-8")).hexdigest()[:28]
    return package_root() / ".faiss_cache" / f"s_{h}"


def _get_nl(session_id: str) -> dict[str, Any] | None:
    sid = (session_id or "").strip()
    if not sid:
        return None
    return nl_sessions.get(sid)


def _ensure_nl(session_id: str) -> dict[str, Any]:
    sid = (session_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required.")
    if sid not in nl_sessions:
        nl_sessions[sid] = {
            "schema":         {"tables": {}, "enums": {}, "domains": []},
            "descriptions":   {},
            "table_catalog":  "",
            "retriever":      None,
            "execution_enabled": False,
            "credentials":    None,
            "database":       None,
            "selected_schemas": [],
            "selected_pairs": [],
            "source":         None,
            "logical_database_name": None,
        }
    return nl_sessions[sid]


def _gemini_embed_error_to_http(e: genai_errors.ClientError) -> HTTPException:
    """Turn Gemini embedding/text API failures into JSON-friendly HTTP errors for the UI."""
    sc = getattr(e, "status_code", None)
    text = str(e).lower()
    if sc == 429 or "resource_exhausted" in text:
        return HTTPException(
            status_code=503,
            detail=f"Gemini rate limit (429) during embeddings: {e}",
        )
    if sc in (400, 401, 403) and (
        "api key" in text
        or "api_key" in text
        or "invalid_argument" in text
        or "permission" in text
    ):
        return HTTPException(
            status_code=502,
            detail=f"Gemini request rejected (check GEMINI_API_KEY and API access): {e}",
        )
    return HTTPException(
        status_code=502,
        detail=f"Gemini API error during embedding: {e}",
    )


def _rebuild_nl_state(sid: str, schema: dict) -> None:
    sess = _ensure_nl(sid)
    descriptions = schema_to_text(schema)
    table_catalog = build_table_catalog(schema)
    cache_dir = _faiss_dir_for(sid)
    try:
        retriever = SchemaRetriever(descriptions, cache_dir=cache_dir)
    except genai_errors.ClientError as e:
        log.warning("SchemaRetriever embedding failed: %s", e)
        raise _gemini_embed_error_to_http(e) from e
    sess["schema"] = schema
    sess["descriptions"] = descriptions
    sess["table_catalog"] = table_catalog
    sess["retriever"] = retriever


def get_sync_target_schema() -> str:
    """Compatibility helper used by startup hints."""
    return db_sync_schema_default() or "public"


def _extract_schema_with_reader_repair() -> tuple[dict, dict]:
    """
    Startup schema extraction helper.

    Restored compatibility function: keeps the previous call-site contract
    (returns ``(schema, meta)``) and runs before app lifespan startup logic.
    """
    meta: dict = {"reader_grants_repaired": False, "admin_public_table_count": None}
    try:
        # New extractor expects a session id; older variants did not.
        try:
            schema = extract_full_schema("startup")
        except TypeError:
            schema = extract_full_schema()  # pragma: no cover (legacy fallback)
    except Exception as e:
        log.warning("Startup schema extract failed: %s", e)
        schema = {"tables": {}, "enums": {}, "domains": []}

    if not isinstance(schema, dict):
        schema = {"tables": {}, "enums": {}, "domains": []}
    schema.setdefault("tables", {})
    schema.setdefault("enums", {})
    schema.setdefault("domains", [])

    if not schema.get("tables"):
        sync_s = get_sync_target_schema()
        meta["hint"] = f"No tables found at startup (sync schema: {sync_s!r})."
    return schema, meta


@asynccontextmanager
async def lifespan(app: FastAPI):
    # App auth DB (Userdetails + public.auth_users). Uses DB_ADMIN_* or DB_USER/DB_PASSWORD.
    try:
        prepare_app_auth_backend()
        log.info("Auth schema is ready in app-level database.")
    except Exception as e:
        log.warning("App database/auth setup failed: %s. Continuing.", e)
    log.info("Extracting schema from database …")
    schema, _repair = _extract_schema_with_reader_repair()
    descriptions  = schema_to_text(schema)
    table_catalog = build_table_catalog(schema)
    retriever     = SchemaRetriever(descriptions)

    app_state["schema"]         = schema
    app_state["descriptions"]   = descriptions
    app_state["table_catalog"]  = table_catalog
    app_state["retriever"]      = retriever

    tables = list(schema["tables"].keys())
    if not tables:
        log.warning("No tables visible in information_schema for the configured user at API startup.")
    else:
        log.info(f"Schema loaded — {len(tables)} table(s): {tables}")
    log.info("NL→SQL API ready — connect and activate a session from the UI.")
    yield
    log.info("Shutting down.")


limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="NL → SQL API",
    description="Natural language to SQL — database context is provided per session from the UI.",
    version="3.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins(),
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type"],
)


def _embed_html_response() -> HTMLResponse:
    # connect-src: allow other http(s) API bases in the settings field (e.g. dev vs prod)
    csp = (
        "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline' 'self'; "
        "connect-src 'self' http: https: ws: wss:"
    )
    return HTMLResponse(content=get_embed_html(), headers={"Content-Security-Policy": csp})


@app.get("/embed", response_class=HTMLResponse, tags=["embed"])
def embed_widget():
    """
    Reusable chatbot page: question → ``POST /generate-sql`` → SQL + result table.
    Open ``/embed?session_id=<uuid>`` after **Module 1: Configuration** (schema activated).
    Parent apps can iframe this URL (avoid ``X-Frame-Options: DENY`` on the API).
    """
    return _embed_html_response()


@app.get("/embed/chat", response_class=HTMLResponse, tags=["embed"])
def embed_chat_widget():
    """Alias of ``GET /embed`` for integrations that name the route ``/embed/chat``."""
    return _embed_html_response()

# ── Chat session persistence (same as before) ──────────────────────────────────
_SESSION_FILE = Path(os.getenv(
    "SESSION_STORE_FILE",
    Path(__file__).resolve().parent.parent / ".session_store.json",
))
_SESSION_MAX_TURNS = session_max_turns()
_SESSION_MAX_AGE_H = session_max_age_hours()

_llm_cache: dict[str, dict] = {}
_LLM_CACHE_MAX = llm_cache_max_entries()


def _llm_cache_key(prompt: str, table_fingerprint: str) -> str:
    raw = f"{prompt.lower().strip()}|{table_fingerprint}"
    return hashlib.md5(raw.encode()).hexdigest()


def _llm_cache_get(key: str) -> dict | None:
    return _llm_cache.get(key)


def _llm_cache_set(key: str, value: dict) -> None:
    if len(_llm_cache) >= _LLM_CACHE_MAX:
        for k in list(_llm_cache.keys())[: _LLM_CACHE_MAX // 4]:
            _llm_cache.pop(k, None)
    _llm_cache[key] = value


def _llm_cache_pop(key: str) -> None:
    _llm_cache.pop(key, None)


def _load_sessions() -> dict[str, list[dict]]:
    if not _SESSION_FILE.exists():
        return {}
    try:
        data = _json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - _SESSION_MAX_AGE_H * 3600
        return {
            sid: entry["turns"]
            for sid, entry in data.items()
            if entry.get("updated_at", 0) >= cutoff
        }
    except Exception as e:
        log.warning("Session file load failed (%s) — starting fresh.", e)
        return {}


def _save_sessions(store: dict[str, list[dict]]) -> None:
    snapshot = dict(store)

    def _write():
        try:
            _SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                sid: {"turns": turns, "updated_at": time.time()}
                for sid, turns in snapshot.items()
            }
            _SESSION_FILE.write_text(_json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            log.warning("Session file save failed: %s", e)

    threading.Thread(target=_write, daemon=True).start()


sessions: dict[str, list[dict]] = _load_sessions()
log.info("Chat session store loaded — %s active session(s).", len(sessions))

_AGENT_TABLE_THRESHOLD = agent_table_threshold()

_ID_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")


def _last_assistant_sql(history: list[dict]) -> Optional[str]:
    """Most recent assistant message that looks like executable SQL."""
    for turn in reversed(history):
        if turn.get("role") != "assistant":
            continue
        c = (turn.get("content") or "").strip()
        if not c:
            continue
        cu = c.upper()
        if cu.startswith("SELECT") or cu.startswith("WITH"):
            return c
    return None


def _is_tables_used_meta_question(prompt: str) -> bool:
    """
    True when the user is asking which tables the assistant used, not asking for
    data from tables named "which…". Those meta questions are a poor fit for the
    strict JSON SQL generator and often yield empty model output.
    """
    t = (prompt or "").lower()
    if not t.strip():
        return False
    if "which tables have" in t or "what tables have" in t:
        return False
    if "which tables" not in t and "what tables" not in t:
        return False
    triggers = (
        "did you use",
        "you use",
        "were used",
        "you picked",
        "you selected",
        "did you pick",
        "did you select",
        "chose",
        "picked",
        "using for this",
    )
    if any(x in t for x in triggers):
        return True
    return bool(
        re.search(r"\b(use|used)\s+(the\s+)?(for\s+this\s+)?(query|sql)\b", t)
    )


def _is_schema_table_count_question(prompt: str) -> bool:
    """
    True for questions like "how many tables in the database/schema?" where the
    user expects a **catalog** count (information_schema), not a literal derived
    from the subset of tables loaded for NL→SQL context (often dozens, not all).
    """
    if _is_tables_used_meta_question(prompt):
        return False
    t = (prompt or "").lower()
    if "table" not in t:
        return False
    if re.search(r"\bhow\s+many\s+tables\b", t):
        return True
    if re.search(r"\bnumber\s+of\s+tables\b", t):
        return True
    if "tables" in t and re.search(r"\b(schema|database|catalog|db)\b", t):
        if re.search(r"\b(count|counting|total|totals|quantity|amount)\b", t):
            return True
        if "how many" in t:
            return True
    return False


def _information_schema_base_tables_where(selected_schemas: list[str]) -> str:
    """SQL WHERE fragment for ``information_schema.tables`` (BASE TABLE, non-system)."""
    if selected_schemas:
        parts: list[str] = []
        for raw in selected_schemas:
            sch = validate_pg_identifier(str(raw).strip())
            parts.append("'" + sch.replace("'", "''") + "'::text")
        arr = "ARRAY[" + ",".join(parts) + "]::text[]"
        return (
            "t.table_type = 'BASE TABLE' "
            f"AND t.table_schema = ANY({arr})"
        )
    return (
        "t.table_type = 'BASE TABLE' "
        "AND t.table_schema NOT IN ('pg_catalog', 'information_schema') "
        "AND t.table_schema NOT LIKE 'pg\\_%' ESCAPE '\\'"
    )


def _is_schema_table_list_question(prompt: str) -> bool:
    """
    True for "list / show tables in schema" style questions. These are answered from
    ``information_schema`` (or the in-memory schema snapshot), not the JSON SQL LLM.
    """
    if _is_tables_used_meta_question(prompt) or _is_schema_table_count_question(prompt):
        return False
    t = (prompt or "").lower()
    if "table" not in t and "tables" not in t:
        return False
    if re.search(r"\b(rows|records)\b", t) and not re.search(r"\b(list|catalog|names)\b", t):
        return False
    phrases = (
        "list tables",
        "tables list",
        "table list",
        "list of tables",
        "show tables",
        "show all tables",
        "what tables",
        "which tables exist",
        "which tables are in",
        "schema tables",
        "tables in schema",
        "tables in this schema",
        "tables in the schema",
        "enumerate tables",
        "table names",
        "name of tables",
        "catalog of tables",
        "list the tables",
    )
    if any(p in t for p in phrases):
        return True
    if "list" in t and "table" in t and re.search(r"\b(schema|catalog|database|db)\b", t):
        return True
    if "show" in t and "table" in t and ("list" in t or "schema" in t):
        return True
    return False


def _live_database_table_list_sql(selected_schemas: list[str], row_limit: int, offset: int) -> str:
    """List BASE TABLE names from information_schema (paged)."""
    wh = _information_schema_base_tables_where(selected_schemas)
    lim = max(1, min(int(row_limit or 20), 5000))
    off = max(0, int(offset or 0))
    return (
        "SELECT t.table_schema::text AS table_schema, t.table_name::text AS table_name "
        "FROM information_schema.tables AS t "
        f"WHERE {wh} "
        "ORDER BY t.table_schema, t.table_name "
        f"LIMIT {lim} OFFSET {off}"
    )


def _live_database_table_count_sql(selected_schemas: list[str]) -> str:
    """COUNT base tables visible in PostgreSQL (scoped to activated schemas when set)."""
    wh = _information_schema_base_tables_where(selected_schemas)
    return (
        "SELECT COUNT(*)::bigint AS number_of_tables "
        "FROM information_schema.tables AS t "
        f"WHERE {wh} "
        "LIMIT 1 OFFSET 0"
    )


def _schema_table_count_meta_payload(
    sess: dict[str, Any],
    schema: dict,
    session_id: str,
    prompt: str,
) -> dict:
    """Real DB table count via information_schema when connected; else in-memory count."""
    n_loaded = len((schema or {}).get("tables") or {})
    live = bool(sess.get("execution_enabled")) and has_pool(session_id)
    if live:
        sch_list = list(sess.get("selected_schemas") or [])
        sql = _live_database_table_count_sql(sch_list)
        scope = (
            f"schemas {', '.join(f'`{s}`' for s in sch_list)}"
            if sch_list
            else "all non-system schemas"
        )
        expl = f"Live information_schema count for {scope}."
    else:
        sql = f"SELECT {int(n_loaded)}::bigint AS number_of_tables LIMIT 1 OFFSET 0"
        expl = f"Snapshot: {n_loaded} table(s) loaded. Connect the database to get a live count."
    return {
        "sql": sql,
        "explanation": (expl + f" — «{prompt.strip()[:100]}».")[:650],
        "chart_suggestion": "kpi",
        "viz_config": {
            "x": None,
            "y": "number_of_tables",
            "color": None,
            "title": "Table count",
        },
        "_meta_tables_used": ["information_schema.tables"] if live else [],
    }


def _schema_table_list_meta_payload(
    sess: dict[str, Any],
    schema: dict,
    session_id: str,
    prompt: str,
    row_limit: int,
    offset: int,
) -> dict:
    """Paged table catalog from information_schema when live; else activated keys only."""
    raw_keys = sorted((schema or {}).get("tables") or ())
    n_loaded = len(raw_keys)
    keys = raw_keys or ["(no tables in loaded schema)"]
    live = bool(sess.get("execution_enabled")) and has_pool(session_id)
    lim = max(1, min(int(row_limit or 20), 5000))
    off = max(0, int(offset or 0))
    if live:
        sch_list = list(sess.get("selected_schemas") or [])
        sql = _live_database_table_list_sql(sch_list, lim, off)
        scope = (
            f"schemas {', '.join(f'`{s}`' for s in sch_list)}"
            if sch_list
            else "all non-system schemas"
        )
        expl = (
            f"Tables in PostgreSQL ({scope}) from information_schema — "
            f"paged (LIMIT {lim} OFFSET {off}). NL→SQL still uses your **{n_loaded}** activated table(s) for generation."
        )
        meta_used = ["information_schema.tables"]
    else:
        lit = ",".join("'" + str(k).replace("'", "''") + "'" for k in keys[:500])
        sql = (
            f"SELECT u::text AS table_key FROM unnest(ARRAY[{lit}]::text[]) AS u "
            f"ORDER BY 1 LIMIT {lim} OFFSET {off}"
        )
        expl = (
            f"{n_loaded} table key(s) in this session's loaded schema (file / in-memory snapshot). "
            "Connect and activate on PostgreSQL for a live information_schema listing."
        )
        meta_used = []
    return {
        "sql": sql,
        "explanation": (expl + f" — «{prompt.strip()[:100]}».")[:700],
        "chart_suggestion": "table",
        "viz_config": {
            "x": "table_schema" if live else "table_key",
            "y": None,
            "color": None,
            "title": "Tables in schema",
        },
        "_meta_tables_used": meta_used,
    }


def _tables_used_meta_payload(
    prompt: str,
    history: list[dict],
    schema: dict,
    selected_tables: list[str],
    row_limit: int,
    offset: int,
) -> dict:
    """Deterministic SQL + explanation so the UI can run and show table names."""
    last_sql = _last_assistant_sql(history)
    if last_sql:
        names = canonical_tables_referenced_in_sql(last_sql, schema)
        basis = "the last SQL returned in this chat"
    else:
        names = list(dict.fromkeys(selected_tables))
        basis = "the current retrieval scope (no prior assistant SQL in this chat yet)"
    if not names:
        names = ["(none resolved)"]
    lit = ", ".join("'" + str(n).replace("'", "''") + "'" for n in names[:80])
    lim = max(1, min(int(row_limit or 20), 1000))
    off = max(0, int(offset or 0))
    # ARRAY + unnest keeps one shape for any row count. (Plain ``UNION ALL SELECT``
    # trips the read-only SQL anti-injection rule that blocks ``UNION … SELECT``.)
    sql = (
        f"SELECT u AS table_name FROM unnest(ARRAY[{lit}]::text[]) "
        f"AS u LIMIT {lim} OFFSET {off}"
    )
    listed = ", ".join(f"`{n}`" for n in names[:36]) + (" …" if len(names) > 36 else "")
    expl = (
        f"Answer to «{prompt.strip()[:120]}»: based on {basis}, these tables apply: {listed}."
    )
    return {
        "sql": sql,
        "explanation": expl[:600],
        "chart_suggestion": "table",
        "viz_config": {
            "x": "table_name",
            "y": None,
            "color": None,
            "title": "Tables used",
        },
        "_meta_tables_used": names,
    }


# ── Pydantic models ────────────────────────────────────────────────────────────


class DbConnectBody(BaseModel):
    session_id: str
    host: str
    port: int = 5432
    username: str
    password: str
    catalog_database: Optional[str] = None
    # Optional: reach Postgres through an SSH bastion (key-based). If ssh_host is set, tunnel first.
    ssh_host: Optional[str] = None
    ssh_port: int = 22
    ssh_username: Optional[str] = None
    ssh_private_key: Optional[str] = None
    ssh_private_key_passphrase: Optional[str] = None

    @field_validator("catalog_database", "username", "host")
    @classmethod
    def strip_s(cls, v: str | None) -> str | None:
        return v.strip() if isinstance(v, str) else v

    @field_validator("port", "ssh_port")
    @classmethod
    def port_ok(cls, v: int) -> int:
        if not (1 <= v <= 65535):
            raise ValueError("port must be 1–65535")
        return v


class DbUseDatabaseBody(BaseModel):
    session_id: str
    database: str

    @field_validator("database")
    @classmethod
    def db_strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("database must not be empty")
        return v


class TableIdent(BaseModel):
    """JSON still uses ``schema`` — not ``schema_name`` — to avoid shadowing ``BaseModel.schema``."""

    model_config = ConfigDict(populate_by_name=True)

    schema_name: str = Field(validation_alias="schema", serialization_alias="schema")
    name: str

    @field_validator("schema_name", "name")
    @classmethod
    def ident(cls, v: str) -> str:
        return validate_pg_identifier(v)


class DbActivateBody(BaseModel):
    session_id: str
    database: str
    tables: list[TableIdent]

    @field_validator("database")
    @classmethod
    def db_strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("database must not be empty")
        return v

    @field_validator("tables")
    @classmethod
    def tables_non_empty(cls, v: list) -> list:
        if not v:
            raise ValueError("Select at least one table.")
        if len(v) > 500:
            raise ValueError("Too many tables (max 500).")
        return v


class QueryRequest(BaseModel):
    prompt: str
    session_id: Optional[str] = None
    top_k: Optional[int] = 3
    row_limit: Optional[int] = 20
    offset: Optional[int] = 0

    @field_validator("prompt")
    @classmethod
    def prompt_ok(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 3:
            raise ValueError("Prompt is too short (minimum 3 characters).")
        if len(v) > 500:
            raise ValueError("Prompt is too long (maximum 500 characters).")
        return v

    @field_validator("top_k")
    @classmethod
    def top_k_ok(cls, v: int) -> int:
        if v is not None and not (1 <= v <= 10):
            raise ValueError("top_k must be between 1 and 10.")
        return v

    @field_validator("row_limit")
    @classmethod
    def rl_ok(cls, v: int) -> int:
        if v is not None and not (1 <= v <= 1000):
            raise ValueError("row_limit must be between 1 and 1000.")
        return v

    @field_validator("offset")
    @classmethod
    def off_ok(cls, v: int) -> int:
        if v is not None and v < 0:
            raise ValueError("offset must be 0 or greater.")
        return v


class QueryResponse(BaseModel):
    sql: str
    explanation: str
    chart_suggestion: str
    viz_config: Optional[dict] = None
    columns: list[str]
    rows: list[dict]
    row_count: int
    total_count: int
    has_more: bool
    execution_ms: int
    tables_used: list[str]
    execution_skipped: bool = False


class PageRequest(BaseModel):
    sql: str
    session_id: str
    page: Optional[int] = 1
    page_size: Optional[int] = 500

    @field_validator("sql")
    @classmethod
    def sql_ok(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("sql field must not be empty.")
        return v.strip()

    @field_validator("session_id")
    @classmethod
    def sid(cls, v: str) -> str:
        if not (v or "").strip():
            raise ValueError("session_id is required.")
        return v.strip()


class PageResponse(BaseModel):
    columns: list[str]
    rows: list[dict]
    row_count: int
    total_count: int
    page: int
    page_size: int
    total_pages: int
    has_next: bool
    has_prev: bool
    execution_ms: int


class ImportRequest(BaseModel):
    session_id: str
    table_name: str
    query: str
    sync_schema: str
    api_url: Optional[str] = None

    @field_validator("table_name")
    @classmethod
    def tn(cls, v: str) -> str:
        v = v.strip().lower()
        if not _ID_RE.match(v):
            raise ValueError("Invalid table_name.")
        return v

    @field_validator("sync_schema")
    @classmethod
    def ss(cls, v: str) -> str:
        return validate_pg_identifier(v)

    @field_validator("query")
    @classmethod
    def q(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Query must not be empty.")
        if len(v) > 2000:
            raise ValueError("Query is too long (max 2000 characters).")
        return v


class SyncRequest(BaseModel):
    session_id: str
    tables: list[str]
    sync_schema: str
    row_limit: Optional[int] = 1000
    api_url: Optional[str] = None

    @field_validator("tables")
    @classmethod
    def tbls(cls, v: list) -> list:
        bad = [t for t in v if not _ID_RE.match(str(t).strip())]
        if bad:
            raise ValueError(f"Invalid table name(s): {bad}")
        if len(v) > 50:
            raise ValueError("Cannot sync more than 50 tables at once.")
        return [str(t).strip().lower() for t in v]

    @field_validator("sync_schema")
    @classmethod
    def ss(cls, v: str) -> str:
        return validate_pg_identifier(v)

    @field_validator("row_limit")
    @classmethod
    def rl(cls, v: int) -> int:
        if v is not None and not (0 <= v <= 10000):
            raise ValueError("row_limit must be 0–10000.")
        return v


class SignUpRequest(BaseModel):
    email: str
    company_name: str
    username: str
    password: str
    confirm_password: str

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Email is required.")
        if not re.match(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$", v):
            raise ValueError("Please enter a valid email address.")
        return v

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = v.strip()
        if not re.match(r"^[A-Za-z0-9_]{3,32}$", v):
            raise ValueError(
                "Username must be 3-32 characters and contain only letters, numbers, or underscores."
            )
        return v

    @field_validator("company_name")
    @classmethod
    def validate_company_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Company name is required.")
        if len(v) > 255:
            raise ValueError("Company name is too long (max 255 characters).")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters.")
        return v

    @field_validator("confirm_password")
    @classmethod
    def validate_confirm_password(cls, v: str) -> str:
        if not v:
            raise ValueError("Confirm password is required.")
        return v


class SignUpResponse(BaseModel):
    ok: bool
    message: str
    user_id: Optional[int] = None


class SignInRequest(BaseModel):
    username: str
    password: str

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Username is required.")
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        if not v:
            raise ValueError("Password is required.")
        return v


class SignInResponse(BaseModel):
    ok: bool
    message: str
    user_id: Optional[int] = None
    username: Optional[str] = None
    email: Optional[str] = None
    company_name: Optional[str] = None


# ── Endpoints ─────────────────────────────────────────────────────────────────
class ReloadBody(BaseModel):
    session_id: str


# ── DB endpoints ───────────────────────────────────────────────────────────────


@app.post("/db/connect")
@limiter.limit("30/minute")
def db_connect(request: Request, body: DbConnectBody):
    """
    Open a pooled read-only connection. ``catalog_database`` is the initial
    PostgreSQL database to attach to (to list other databases). If omitted,
    the server uses ``username`` as the database name (common PostgreSQL default).
    When ``ssh_host`` + ``ssh_private_key`` are set, the API opens a local forward
    to ``host:port`` (Postgres as seen from the SSH server) and connects via ``127.0.0.1``.
    """
    sid = body.session_id.strip()
    close_pool(sid)
    initial_db = (body.catalog_database or body.username).strip()
    if not initial_db:
        raise HTTPException(
            status_code=400,
            detail="Missing catalog database or username.",
        )
    h = (body.host or "").strip()
    p = int(body.port)
    use_ssh = bool((body.ssh_host or "").strip() and (body.ssh_private_key or "").strip())
    if use_ssh:
        try:
            ch, cport = start_ssh_pg_tunnel(
                sid,
                db_host=h,
                db_port=p,
                ssh_host=(body.ssh_host or "").strip(),
                ssh_port=int(body.ssh_port or 22),
                ssh_username=(body.ssh_username or body.username or "").strip(),
                ssh_private_key_pem=body.ssh_private_key or "",
                ssh_private_key_passphrase=body.ssh_private_key_passphrase,
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            log.warning("ssh tunnel failed: %s", e)
            raise HTTPException(status_code=400, detail=f"SSH tunnel failed: {e}") from e
        h, p = ch, cport
    creds = PgCredentials(
        host=h,
        port=p,
        user=body.username,
        password=body.password,
        database=initial_db,
    )
    try:
        names = list_database_names(creds)
    except Exception as e:
        log.warning("db_connect failed: %s", e)
        close_pool(sid)
        raise HTTPException(status_code=400, detail=f"Connection failed: {e}") from e

    close_pool_only(sid)
    register_pool(sid, creds, read_only=True)
    sess = _ensure_nl(sid)
    sess["credentials"] = creds
    sess["database"] = creds.database
    sess["via_ssh"] = use_ssh
    if use_ssh and body.ssh_host:
        sess["ssh_bastion"] = (body.ssh_host or "").strip()
    return {
        "status": "connected",
        "database": creds.database,
        "databases": names,
        "via_ssh": use_ssh,
    }


@app.post("/db/use-database")
@limiter.limit("30/minute")
def db_use_database(request: Request, body: DbUseDatabaseBody):
    sid = body.session_id.strip()
    sess = _get_nl(sid)
    if not sess or not sess.get("credentials"):
        raise HTTPException(status_code=400, detail="Connect with POST /db/connect first.")
    c = sess["credentials"]
    creds = PgCredentials(
        host=c.host,
        port=c.port,
        user=c.user,
        password=c.password,
        database=body.database.strip(),
    )
    try:
        list_schema_names(creds)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Cannot open database: {e}") from e

    close_pool_only(sid)
    register_pool(sid, creds, read_only=True)
    sess["credentials"] = creds
    sess["database"] = creds.database
    return {"status": "ok", "database": creds.database}


@app.get("/db/schemas")
@limiter.limit("60/minute")
def db_list_schemas(request: Request, session_id: str):
    sid = session_id.strip()
    sess = _get_nl(sid)
    if not sess or not sess.get("credentials"):
        raise HTTPException(status_code=400, detail="No connection for this session_id.")
    try:
        names = list_schema_names(sess["credentials"])
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {"schemas": names}


@app.get("/db/tables")
@limiter.limit("60/minute")
def db_list_tables(request: Request, session_id: str, schemas: str = ""):
    """Comma-separated schema names; empty = all non-system schemas visible to the user."""
    sid = session_id.strip()
    if not has_pool(sid):
        raise HTTPException(status_code=400, detail="No connection for this session_id.")
    allow = [validate_pg_identifier(s) for s in schemas.split(",") if s.strip()] or None

    pairs = get_tables(sid, allowed_schemas=allow)
    grouped: dict[str, list[str]] = {}
    for sch, tbl in pairs:
        grouped.setdefault(sch, []).append(tbl)
    for sch in grouped:
        grouped[sch].sort()
    return {"tables_by_schema": grouped, "flat": [{"schema": a, "name": b} for a, b in pairs]}


@app.post("/db/activate")
@limiter.limit("10/minute")
def db_activate(request: Request, body: DbActivateBody):
    """Load metadata + embeddings for the selected tables (live database)."""
    sid = body.session_id.strip()
    if not has_pool(sid):
        raise HTTPException(status_code=400, detail="Connect with POST /db/connect first.")
    sess = _ensure_nl(sid)
    creds = sess.get("credentials")
    if not creds or creds.database.strip() != body.database.strip():
        raise HTTPException(
            status_code=400,
            detail="Database does not match the active connection. Use /db/use-database first.",
        )
    pairs = [(t.schema_name, t.name) for t in body.tables]
    sch_set = sorted({p[0] for p in pairs})
    try:
        schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    _rebuild_nl_state(sid, schema)
    sess["execution_enabled"] = True
    sess["source"] = "live"
    sess["logical_database_name"] = creds.database
    sess["selected_schemas"] = sch_set
    sess["selected_pairs"] = pairs
    cache_clear()
    return {
        "status": "active",
        "database": creds.database,
        "table_count": len(schema.get("tables", {})),
        "tables": list(schema.get("tables", {}).keys()),
    }


@app.post("/schema/from-file")
@limiter.limit("10/minute")
async def schema_from_file_upload(
    request: Request,
    session_id: str = Form(...),
    database_name: str = Form(...),
    file: UploadFile = File(...),
    keep_connection: str = Form("false"),
    materialize: str = Form("false"),
    target_database: str = Form(""),
    remote_data_url: str = Form(""),
    remote_row_limit: str = Form("5000"),
):
    """
    Upload a JSON schema (tables + columns).

    - Default: load in-memory for NL→SQL; execution off unless you connect and activate.
    - ``keep_connection``: keep a prior ``/db/connect`` pool and credentials.
    - ``materialize``: create ``target_database`` (or ``database_name``) on the server,
      run DDL from the JSON, reconnect the pool to that DB, reload metadata, enable execution.
      Requires ``keep_connection`` and an active session with credentials.
    - ``remote_data_url``: optional SQL passthrough API (POST JSON ``{"query":"SELECT …"}``).
      After materializing, fetches each table from the remote API and upserts into PostgreSQL.
    """
    sid = session_id.strip()
    _kc = (keep_connection or "").strip().lower() in ("true", "1", "yes", "on")
    _mat = (materialize or "").strip().lower() in ("true", "1", "yes", "on")
    _target = (target_database or database_name or "").strip()
    raw = await file.read()
    try:
        data = _json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}") from e
    try:
        schema, skipped_tables = schema_from_uploaded_json(data)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) from e

    return _schema_upload_core(
        None,
        sid,
        database_name.strip(),
        schema,
        skipped_tables,
        _kc,
        _mat,
        _target,
        remote_data_url,
        remote_row_limit,
    )


def _schema_job_worker(
    job_id: str,
    sid: str,
    database_name: str,
    raw: bytes,
    _kc: bool,
    _mat: bool,
    _target: str,
    remote_data_url: str,
    remote_row_limit: str,
) -> None:
    try:
        data = _json.loads(raw.decode("utf-8"))
        schema, skipped_tables = schema_from_uploaded_json(data)
    except Exception as e:
        with _schema_upload_jobs_lock:
            j = _schema_upload_jobs.get(job_id)
            if j:
                j["status"] = "error"
                j["error"] = str(e)
                j["phase"] = "error"
        return

    with _schema_upload_jobs_lock:
        j = _schema_upload_jobs.get(job_id)
        if j:
            j["status"] = "running"
            j["phase"] = "running"
            j["message"] = "Applying schema…"

    try:
        result = _schema_upload_core(
            job_id,
            sid,
            database_name,
            schema,
            skipped_tables,
            _kc,
            _mat,
            _target,
            remote_data_url,
            remote_row_limit,
        )
        with _schema_upload_jobs_lock:
            j = _schema_upload_jobs.get(job_id)
            if j:
                j["status"] = "done"
                j["phase"] = "done"
                j["result"] = result
                j["message"] = "Finished"
    except HTTPException as he:
        detail = he.detail
        if not isinstance(detail, str):
            detail = str(detail)
        with _schema_upload_jobs_lock:
            j = _schema_upload_jobs.get(job_id)
            if j:
                j["status"] = "error"
                j["error"] = detail
                j["phase"] = "error"
    except Exception as e:
        log.exception("schema job %s", job_id)
        with _schema_upload_jobs_lock:
            j = _schema_upload_jobs.get(job_id)
            if j:
                j["status"] = "error"
                j["error"] = str(e)
                j["phase"] = "error"


class SchemaUploadJobControlBody(BaseModel):
    session_id: str = Field(..., min_length=1)
    action: str = Field(..., description="cancel, pause, or resume")


@app.post("/schema/from-file/async")
@limiter.limit("10/minute")
async def schema_from_file_upload_async(
    request: Request,
    session_id: str = Form(...),
    database_name: str = Form(...),
    file: UploadFile = File(...),
    keep_connection: str = Form("false"),
    materialize: str = Form("false"),
    target_database: str = Form(""),
    remote_data_url: str = Form(""),
    remote_row_limit: str = Form("5000"),
):
    """
    Same inputs as ``/schema/from-file``, but returns immediately with a ``job_id``.
    Poll ``GET /schema/from-file/job/{job_id}`` and use ``POST .../control`` to
    cancel / pause / resume (pause applies between remote table syncs).
    """
    sid = session_id.strip()
    _kc = (keep_connection or "").strip().lower() in ("true", "1", "yes", "on")
    _mat = (materialize or "").strip().lower() in ("true", "1", "yes", "on")
    _target = (target_database or database_name or "").strip()
    raw = await file.read()

    job_id = str(uuid.uuid4())
    with _schema_upload_jobs_lock:
        _schema_upload_jobs[job_id] = {
            "session_id": sid,
            "status": "queued",
            "phase": "queued",
            "message": "Queued",
            "sync_current": 0,
            "sync_total": 0,
            "current_table": None,
            "cancel": threading.Event(),
            "pause": threading.Event(),
            "result": None,
            "error": None,
        }

    t = threading.Thread(
        target=_schema_job_worker,
        args=(
            job_id,
            sid,
            database_name.strip(),
            raw,
            _kc,
            _mat,
            _target,
            remote_data_url,
            remote_row_limit,
        ),
        daemon=True,
    )
    t.start()
    return {
        "job_id": job_id,
        "status": "queued",
        "poll": f"/schema/from-file/job/{job_id}",
    }


@app.get("/schema/from-file/job/{job_id}")
@limiter.limit("120/minute")
def schema_upload_job_status(request: Request, job_id: str, session_id: str):
    """Return job status; ``session_id`` must match the job owner."""
    sid = (session_id or "").strip()
    with _schema_upload_jobs_lock:
        job = _schema_upload_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Unknown job_id")
    if job["session_id"] != sid:
        raise HTTPException(status_code=403, detail="session_id does not match this job")
    out = {
        "job_id": job_id,
        "status": job["status"],
        "phase": job["phase"],
        "message": job.get("message"),
        "sync_current": job.get("sync_current", 0),
        "sync_total": job.get("sync_total", 0),
        "current_table": job.get("current_table"),
        "paused": job["pause"].is_set(),
    }
    if job["status"] == "done" and job.get("result") is not None:
        out["result"] = job["result"]
    if job["status"] == "error" and job.get("error"):
        out["error"] = job["error"]
    return out


@app.post("/schema/from-file/job/{job_id}/control")
@limiter.limit("60/minute")
def schema_upload_job_control(
    request: Request, job_id: str, body: SchemaUploadJobControlBody
):
    """Cancel, pause, or resume a background schema upload job."""
    sid = body.session_id.strip()
    action = (body.action or "").strip().lower()
    if action not in ("cancel", "pause", "resume"):
        raise HTTPException(
            status_code=400,
            detail='action must be "cancel", "pause", or "resume"',
        )
    with _schema_upload_jobs_lock:
        job = _schema_upload_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Unknown job_id")
    if job["session_id"] != sid:
        raise HTTPException(status_code=403, detail="session_id does not match this job")

    if action == "cancel":
        job["cancel"].set()
        return {"ok": True, "action": "cancel", "detail": "Stop requested (applies between tables)."}
    if action == "pause":
        job["pause"].set()
        return {"ok": True, "action": "pause", "detail": "Pause requested (applies between tables)."}
    job["pause"].clear()
    return {"ok": True, "action": "resume", "detail": "Resume syncing."}


# ── Health / schema views ──────────────────────────────────────────────────────


@app.get("/health")
@limiter.exempt
def health(request: Request, session_id: Optional[str] = None):
    sid = (session_id or "").strip()
    if not sid:
        return {
            "status": "ok",
            "message": "Provide ?session_id= to see NL→SQL session status.",
        }
    s = _get_nl(sid)
    if not s:
        return {
            "status": "ok",
            "session_id": sid,
            "activated": False,
            "has_tables": False,
        }
    tables = list(s.get("schema", {}).get("tables", {}).keys())
    return {
        "status": "ok",
        "session_id": sid,
        "activated": bool(s.get("retriever")),
        "has_tables": len(tables) > 0,
        "table_count": len(tables),
        "tables_loaded": tables,
        "database": s.get("logical_database_name") or s.get("database"),
        "execution_enabled": s.get("execution_enabled", False),
        "db_schema_scan": schema_scan_description(s.get("selected_schemas") or None),
        "source": s.get("source"),
        "empty_message": None if tables else "Activate a session (POST /db/activate or /schema/from-file).",
    }


@app.get("/schema")
@limiter.limit("30/minute")
def get_schema(request: Request, session_id: str):
    sid = session_id.strip()
    s = _get_nl(sid)
    if not s:
        raise HTTPException(status_code=400, detail="Unknown session_id.")
    schema = s.get("schema", {})
    clean = {}
    for t, meta in schema.get("tables", {}).items():
        clean[t] = {
            "columns": meta["columns"],
            "foreign_keys": meta["foreign_keys"],
        }
    return clean


@app.get("/schema/tables")
@limiter.limit("60/minute")
def get_schema_tables(request: Request, session_id: str):
    s = _get_nl(session_id.strip())
    if not s:
        raise HTTPException(status_code=400, detail="Unknown session_id.")
    schema = s.get("schema", {})
    result = []
    for key, meta in schema.get("tables", {}).items():
        cols = meta.get("columns", [])
        result.append({
            "name": key,
            "schema_name": meta.get("schema_name", ""),
            "table_name": meta.get("table_name", key),
            "column_count": len(cols),
            "columns": [
                {"name": c["column_name"], "type": c["data_type"], "is_pk": c.get("is_primary_key", False)}
                for c in cols[:20]
            ],
            "fk_count": len(meta.get("foreign_keys", [])),
            "has_sample": bool(meta.get("sample_rows")),
        })
    result.sort(key=lambda x: x["name"])
    return {"tables": result, "total": len(result)}


@app.get("/suggest-prompts")
@limiter.limit("30/minute")
def suggest_prompts_endpoint(request: Request, session_id: str, last_query: str = ""):
    s = _get_nl(session_id.strip())
    if not s:
        raise HTTPException(status_code=400, detail="Unknown session_id.")
    tc = s.get("table_catalog") or ""
    prompts = suggest_prompts(tc, last_query)
    return {"prompts": prompts}


@app.post("/api/platform/auth/signup", response_model=SignUpResponse)
@limiter.limit("20/minute")
def signup_endpoint(request: Request, req: SignUpRequest):
    if req.password != req.confirm_password:
        raise HTTPException(status_code=400, detail="Confirm password does not match.")

    try:
        prepare_app_auth_backend()
    except Exception as e:
        log.error("Auth backend (signup): %s", e, exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Auth database unavailable. Check DB_* credentials in .env and restart the API.",
        ) from e

    try:
        with get_app_db_cursor(dict_cursor=True) as cur:
            cur.execute(
                "SELECT id FROM public.auth_users WHERE LOWER(email) = LOWER(%s) LIMIT 1",
                (req.email,),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Email is already registered.")

            cur.execute(
                "SELECT id FROM public.auth_users WHERE LOWER(username) = LOWER(%s) LIMIT 1",
                (req.username,),
            )
            if cur.fetchone():
                raise HTTPException(status_code=409, detail="Username is already taken.")

            password_hash = bcrypt.hashpw(
                req.password.encode("utf-8"), bcrypt.gensalt()
            ).decode("utf-8")

            cur.execute(
                """
                INSERT INTO public.auth_users (email, company_name, username, password_hash)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                (req.email, req.company_name, req.username, password_hash),
            )
            row = cur.fetchone()
            user_id = int(row["id"]) if row else None
            if user_id is not None:
                tenant_name = (req.company_name or "").strip()
                cur.execute(
                    """
                    INSERT INTO public.app_workspace_tenants (user_id, id, name, created_at, updated_at)
                    VALUES (%s, %s, %s, NOW(), NOW())
                    ON CONFLICT (user_id, id) DO UPDATE
                    SET name = EXCLUDED.name, updated_at = NOW()
                    """,
                    (user_id, "ten-default", tenant_name),
                )

        return SignUpResponse(ok=True, message="Account created successfully.", user_id=user_id)
    except HTTPException:
        raise
    except Exception as e:
        log.error("Signup failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not create account.")


@app.post("/api/platform/auth/signin", response_model=SignInResponse)
@limiter.limit("30/minute")
def signin_endpoint(request: Request, req: SignInRequest):
    try:
        prepare_app_auth_backend()
    except Exception as e:
        log.error("Auth backend (signin): %s", e, exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Auth database unavailable. Check DB_* credentials in .env and restart the API.",
        ) from e

    try:
        with get_app_db_cursor(dict_cursor=True) as cur:
            cur.execute(
                """
                SELECT id, username, email, company_name, password_hash, is_active
                FROM public.auth_users
                WHERE LOWER(username) = LOWER(%s)
                LIMIT 1
                """,
                (req.username,),
            )
            user = cur.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="Invalid username or password.")

            if not user.get("is_active", True):
                raise HTTPException(status_code=403, detail="This account is inactive.")

            stored_hash = str(user.get("password_hash") or "")
            ok = bcrypt.checkpw(req.password.encode("utf-8"), stored_hash.encode("utf-8"))
            if not ok:
                raise HTTPException(status_code=401, detail="Invalid username or password.")

            cur.execute(
                "UPDATE public.auth_users SET last_login_at = NOW() WHERE id = %s",
                (user["id"],),
            )

        return SignInResponse(
            ok=True,
            message="Signed in successfully.",
            user_id=int(user["id"]),
            username=str(user["username"]),
            email=str(user["email"]),
            company_name=str(user.get("company_name") or ""),
        )
    except HTTPException:
        raise
    except Exception as e:
        log.error("Signin failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail="Could not sign in.")


@app.post("/generate-sql", response_model=QueryResponse)
@limiter.limit("10/minute")
def generate_sql_endpoint(request: Request, req: QueryRequest):
    sid = (req.session_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="session_id is required in the JSON body.")
    s = _get_nl(sid)
    if not s or not s.get("retriever"):
        raise HTTPException(
            status_code=400,
            detail="No activated schema for this session.",
        )

    schema = s["schema"]
    descriptions = s["descriptions"]
    table_catalog = s["table_catalog"]
    retriever: SchemaRetriever = s["retriever"]
    all_tables = list(schema["tables"].keys())

    history = sessions.get(sid, [])
    prior_context = ""
    for turn in reversed(history[-4:]):
        if turn["role"] == "user":
            prior_context = turn["content"]
            break
    agent_query = (
        f"Previous question: {prior_context}\nCurrent question: {req.prompt}"
        if prior_context else req.prompt
    )

    top_k = inferred_top_k_for_query(req.prompt, req.top_k or 3)

    if len(all_tables) <= _AGENT_TABLE_THRESHOLD:
        selected_tables = retriever.retrieve_with_fk_expansion(
            req.prompt, schema, top_k=top_k
        )
        log.info(
            "[%s] FAISS (%s tables, top_k=%s): %s",
            sid[:8],
            len(all_tables),
            top_k,
            selected_tables,
        )
    else:
        selected_tables = select_tables_agent(
            user_query=agent_query,
            table_catalog=table_catalog,
            all_table_names=all_tables,
        )
        if not selected_tables:
            selected_tables = retriever.retrieve_with_fk_expansion(
                req.prompt, schema, top_k=top_k
            )

    selected_tables = expand_selected_tables_for_nl_query(
        req.prompt, selected_tables, schema
    )
    selected_tables = fk_expand_seed_tables(selected_tables, schema)

    table_fp = ",".join(sorted(selected_tables))
    ckey = _llm_cache_key(req.prompt, table_fp)
    repair_hint: str | None = None
    llm_result: dict = {}
    cached_used = False

    meta_tables_reply = _is_tables_used_meta_question(req.prompt)
    meta_schema_list = _is_schema_table_list_question(req.prompt)
    meta_schema_count = _is_schema_table_count_question(req.prompt)
    if meta_tables_reply:
        llm_result = _tables_used_meta_payload(
            req.prompt, history, schema, selected_tables, req.row_limit or 20, req.offset or 0,
        )
        cached_used = True
        log.info("[%s] Meta reply (tables used) — skipping LLM SQL generation.", sid[:8])
    elif meta_schema_list:
        llm_result = _schema_table_list_meta_payload(
            s, schema, sid, req.prompt, req.row_limit or 20, req.offset or 0,
        )
        cached_used = True
        log.info("[%s] Meta reply (schema table list) — skipping LLM SQL generation.", sid[:8])
    elif meta_schema_count:
        llm_result = _schema_table_count_meta_payload(s, schema, sid, req.prompt)
        cached_used = True
        log.info("[%s] Meta reply (schema table count) — skipping LLM SQL generation.", sid[:8])
    else:
        for attempt in range(2):
            use_cache = attempt == 0 and not repair_hint
            cached = _llm_cache_get(ckey) if use_cache else None
            if cached is not None:
                llm_result = cached
                cached_used = True
            else:
                try:
                    llm_result = generate_sql(
                        user_query=req.prompt,
                        selected_tables=selected_tables,
                        table_descriptions=descriptions,
                        schema=schema,
                        chat_history=history,
                        row_limit=req.row_limit or 20,
                        offset=req.offset or 0,
                        repair_hint=repair_hint,
                    )
                except genai_errors.ClientError as e:
                    code = getattr(e, "status_code", None)
                    if code == 429 or "RESOURCE_EXHAUSTED" in str(e).upper():
                        raise HTTPException(
                            status_code=503,
                            detail=f"Gemini rate limit (429): {e}",
                        ) from e
                    raise
                except ValueError as e:
                    raise HTTPException(status_code=502, detail=str(e)) from e
                cached_used = False

            sql = (llm_result.get("sql") or "").strip()
            sql = fix_postgresql_mixed_case_identifiers(sql, schema)

            try:
                sql = validate_sql(sql)
            except SQLValidationError as e:
                raise HTTPException(status_code=400, detail=str(e)) from e

            try:
                validate_sql_tables_against_schema(sql, schema)
            except SQLValidationError as e:
                _llm_cache_pop(ckey)
                if cached_used:
                    log.warning(
                        "[%s] Cached SQL failed schema-table validation; regenerating.",
                        sid[:8],
                    )
                if attempt >= 1:
                    raise HTTPException(status_code=400, detail=str(e)) from e
                bad = unknown_tables_in_sql(sql, schema)
                allow = ", ".join(f"`{t}`" for t in sorted(all_tables)[:72])
                if len(all_tables) > 72:
                    allow += ", …"
                repair_hint = f"{e} Remove invalid names {bad!s}; use only: {allow}"
                continue

            if not cached_used:
                _llm_cache_set(ckey, llm_result)
            break

    if meta_tables_reply or meta_schema_count or meta_schema_list:
        sql = (llm_result.get("sql") or "").strip()
        sql = fix_postgresql_mixed_case_identifiers(sql, schema)
        try:
            sql = validate_sql(sql)
        except SQLValidationError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        # Skip validate_sql_tables_against_schema for trusted meta SQL (unnest /
        # information_schema are not keys in the activated schema dict).

    explanation = llm_result.get("explanation", "")
    chart_suggestion = llm_result.get("chart_suggestion", "table")
    viz_config = llm_result.get("viz_config") or {}
    meta_names = llm_result.get("_meta_tables_used")
    if (
        (meta_tables_reply or meta_schema_count or meta_schema_list)
        and isinstance(meta_names, list)
        and meta_names
    ):
        tables_used = meta_names
    else:
        tables_used = canonical_tables_referenced_in_sql(sql, schema) or selected_tables

    if not s.get("execution_enabled"):
        return QueryResponse(
            sql=sql,
            explanation=explanation + " (SQL not executed: no active database for this session.)",
            chart_suggestion=chart_suggestion,
            viz_config=viz_config,
            columns=[],
            rows=[],
            row_count=0,
            total_count=0,
            has_more=False,
            execution_ms=0,
            tables_used=tables_used,
            execution_skipped=True,
        )

    try:
        exec_result = execute_sql(sql, session_id=sid)
    except Exception as e:
        log.error("SQL execution error: %s", e)
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}") from e

    if sid:
        sessions.setdefault(sid, [])
        sessions[sid].append({"role": "user", "content": req.prompt})
        sessions[sid].append({"role": "assistant", "content": sql})
        sessions[sid] = sessions[sid][-_SESSION_MAX_TURNS:]
        _save_sessions(sessions)

    return QueryResponse(
        sql=sql,
        explanation=explanation,
        chart_suggestion=chart_suggestion,
        viz_config=viz_config,
        tables_used=tables_used,
        total_count=exec_result.get("total_count", exec_result.get("row_count", 0)),
        has_more=exec_result.get("has_more", False),
        execution_skipped=False,
        **{k: v for k, v in exec_result.items() if k not in ("total_count", "has_more")},
    )


@app.post("/sql/page", response_model=PageResponse)
@limiter.limit("20/minute")
def paginate_sql(request: Request, req: PageRequest):
    try:
        result = execute_sql_page(req.sql, session_id=req.session_id, page=req.page, page_size=req.page_size)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        log.error("Pagination error: %s", e)
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}") from e
    return PageResponse(**result)


@app.delete("/session/{session_id}")
@limiter.limit("10/minute")
def clear_session(request: Request, session_id: str):
    sessions.pop(session_id, None)
    _save_sessions(sessions)
    return {"cleared": session_id}


@app.get("/cache/stats")
@limiter.limit("30/minute")
def get_cache_stats(request: Request):
    return cache_stats()


@app.delete("/cache/clear")
@limiter.limit("10/minute")
def clear_cache_endpoint(request: Request):
    removed = cache_clear()
    log.info("SQL cache cleared — %s entries.", removed)
    return {"cleared_entries": removed}


@app.post("/reload-schema")
@limiter.limit("5/minute")
def reload_schema(request: Request, body: ReloadBody):
    sid = body.session_id.strip()
    s = _get_nl(sid)
    if not s:
        raise HTTPException(status_code=400, detail="Unknown session_id.")
    if s.get("source") == "file":
        raise HTTPException(status_code=400, detail="Cannot reload file-based schema from the database.")
    if not has_pool(sid):
        raise HTTPException(status_code=400, detail="No database connection for this session.")
    pairs = s.get("selected_pairs") or []
    if not pairs:
        raise HTTPException(status_code=400, detail="Nothing to reload — activate tables first.")
    sch_set = sorted({p[0] for p in pairs})
    try:
        schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    _rebuild_nl_state(sid, schema)
    cleared = cache_clear()
    return {
        "status": "reloaded",
        "tables_found": list(schema.get("tables", {}).keys()),
        "table_count": len(schema.get("tables", {})),
        "sql_cache_cleared": cleared,
    }


@app.post("/import-table")
@limiter.limit("5/minute")
def import_table_endpoint(req: ImportRequest, request: Request):
    if not allow_data_ingestion_to_connected_db():
        raise HTTPException(
            status_code=403,
            detail="Importing rows is disabled. Set SMART_QUERY_ALLOW_DATA_INGESTION=1 to opt in, or use Connect database + /generate-sql to query the client directly.",
        )
    sid = req.session_id.strip()
    sess = _get_nl(sid)
    if not sess or not sess.get("credentials"):
        raise HTTPException(status_code=400, detail="Connect a database session first.")
    creds = sess["credentials"]
    try:
        records = fetch_from_remote_api(req.query, req.api_url)
        if not records:
            raise HTTPException(status_code=404, detail="Remote API returned 0 records.")
        rows_imported = import_table_to_db(
            req.table_name,
            records,
            creds=creds,
            sync_schema=req.sync_schema,
        )
        pairs = sess.get("selected_pairs") or []
        if (req.sync_schema, req.table_name) not in pairs:
            pairs = list(pairs) + [(req.sync_schema, req.table_name)]
        sess["selected_pairs"] = pairs
        sch_set = sorted({p[0] for p in pairs})
        schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
        _rebuild_nl_state(sid, schema)
        sess["execution_enabled"] = True
        return {
            "status": "imported",
            "table_name": req.table_name,
            "rows_imported": rows_imported,
            "tables_in_db": list(schema.get("tables", {}).keys()),
        }
    except HTTPException:
        raise
    except Exception as e:
        log.error("Import failed: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/sync-tables")
@limiter.limit("30/minute")
def sync_tables_endpoint(req: SyncRequest, request: Request):
    if not allow_data_ingestion_to_connected_db():
        raise HTTPException(
            status_code=403,
            detail="Table row sync is disabled. Set SMART_QUERY_ALLOW_DATA_INGESTION=1 to opt in, or use Connect database + /generate-sql to query the client directly.",
        )
    sid = req.session_id.strip()
    sess = _get_nl(sid)
    if not sess or not sess.get("credentials"):
        raise HTTPException(status_code=400, detail="Connect a database session first.")
    creds = sess["credentials"]
    row_limit = req.row_limit if req.row_limit is not None else 1000
    results = {}
    for table in req.tables:
        query = (
            f'SELECT * FROM "{table}"'
            if row_limit == 0
            else f'SELECT * FROM "{table}" ORDER BY id LIMIT {row_limit}'
        )
        try:
            records = fetch_from_remote_api(query, req.api_url)
            if not records:
                results[table] = {"status": "skipped", "reason": "API returned 0 rows"}
                continue
            status = sync_table(table, records, creds=creds, sync_schema=req.sync_schema)
            results[table] = {"status": "ok", **status}
        except Exception as e:
            log.error("Sync failed for %s: %s", table, e, exc_info=True)
            results[table] = {"status": "error", "reason": str(e)}

    pairs = sess.get("selected_pairs") or []
    for t in req.tables:
        if (req.sync_schema, t) not in pairs:
            pairs.append((req.sync_schema, t))
    sess["selected_pairs"] = pairs
    sch_set = sorted({p[0] for p in pairs})
    try:
        schema = extract_full_schema(sid, allowed_schemas=sch_set, only_tables=pairs)
        _rebuild_nl_state(sid, schema)
        sess["execution_enabled"] = True
        db_tables = list(schema.get("tables", {}).keys())
    except Exception as e:
        log.error("Schema reload after sync failed: %s", e)
        db_tables = []

    return {"results": results, "tables_in_db": db_tables, "table_count": len(db_tables)}
