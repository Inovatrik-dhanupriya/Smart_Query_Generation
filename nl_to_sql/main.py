"""
main.py  –  NL → SQL FastAPI Application
Schema and table names come from the live database and uploaded schema JSON — not from code.

Run:
    uvicorn main:app --reload --port 8000
"""
import logging
import os
import re
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import bcrypt
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from google.genai import errors as genai_errors

from utils.config import (
    agent_table_threshold,
    cors_origins,
    llm_cache_max_entries,
    session_max_age_hours,
    session_max_turns,
)
from utils.env import load_app_env

load_app_env()

from llm import generate_sql, select_tables_agent, suggest_prompts
from schema.extractor import (
    build_table_catalog,
    extract_full_schema,
    get_sync_target_schema,
    schema_scan_description,
    schema_to_text,
)
from schema.importer import (
    count_public_physical_tables_admin,
    fetch_from_remote_api,
    import_table as import_table_to_db,
    parse_schema_json,
    repair_reader_grants_public,
    sync_table,
)
from schema.retriever import SchemaRetriever
from sql_engine import (
    SQLValidationError,
    cache_clear,
    cache_stats,
    execute_sql,
    execute_sql_page,
    validate_sql,
)
from db import ensure_auth_tables, ensure_userdetails_database, get_app_db_cursor

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
)
log = logging.getLogger(__name__)

# ── Startup env-var validation ────────────────────────────────────────────────
_REQUIRED_ENV = ["GEMINI_API_KEY", "DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]

def _check_env() -> None:
    missing = [k for k in _REQUIRED_ENV if not os.getenv(k)]
    if missing:
        log.critical(f"Missing required environment variables: {missing}")
        sys.exit(1)

_check_env()


def _extract_schema_with_reader_repair() -> tuple[dict, dict]:
    """
    Run extract_full_schema(); if the read-only DB_USER sees no tables but the
    admin connection still finds physical tables in DB_SYNC_SCHEMA, repair GRANTs and
    extract again so the UI and FAISS match what pgAdmin shows.
    """
    meta: dict = {"reader_grants_repaired": False, "admin_public_table_count": None}
    schema = extract_full_schema()
    if schema.get("tables"):
        return schema, meta
    try:
        n_admin = count_public_physical_tables_admin()
    except Exception as e:
        log.warning("count_public_physical_tables_admin failed: %s", e)
        n_admin = -1
    meta["admin_public_table_count"] = n_admin
    reader = os.getenv("DB_USER", "").strip()
    sync_s = get_sync_target_schema()
    if n_admin <= 0 or not reader:
        meta["hint"] = (
            f"No tables visible to the API user and admin found none in schema {sync_s!r}. "
            "Confirm DB_NAME / DB_HOST match pgAdmin, DB_SYNC_SCHEMA matches where tables live, "
            "and objects are ordinary tables (relkind 'r')."
        )
        return schema, meta
    admin_user = os.getenv("DB_ADMIN_USER", "postgres").strip()
    if reader == admin_user:
        if n_admin > 0:
            meta["hint"] = (
                "DB_USER is the same as DB_ADMIN_USER, yet no tables in metadata — "
                "this is not a GRANT issue. Check DB_SCHEMAS in .env or that the API "
                "points at the same database you use in pgAdmin."
            )
        return schema, meta
    ok, err = repair_reader_grants_public()
    if not ok and err:
        meta["repair_error"] = err
        meta["hint"] = (
            f"Admin role sees ~{n_admin} table(s) in schema {sync_s!r}, but '{reader}' cannot read them "
            f"(information_schema is empty for that role). Grant repair failed: {err}. "
            f"Grant SELECT ON ALL TABLES IN SCHEMA {sync_s} TO your DB_USER manually."
        )
        return schema, meta
    if ok:
        meta["reader_grants_repaired"] = True
        log.info(
            "Applied GRANT SELECT to DB_USER for schema %s (reader/admin visibility fix).",
            sync_s,
        )
    schema = extract_full_schema()
    if not schema.get("tables") and n_admin > 0:
        meta["hint"] = (
            f"After GRANT, '{reader}' still sees 0 tables. Check DB_SCHEMAS in .env "
            "(must include the schema where tables live) or confirm DB_USER name spelling."
        )
    return schema, meta


# ── App state (loaded once at startup) ───────────────────────────────────────
app_state: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ensure app-level auth/project database exists (separate from DB_NAME / medicine).
    try:
        created = ensure_userdetails_database()
        if created:
            log.info("App-level database created successfully.")
        else:
            log.info("App-level database already present.")
        ensure_auth_tables()
        log.info("Auth schema is ready in app-level database.")
    except Exception as e:
        log.warning(
            "Could not ensure app-level database/auth schema: %s. "
            "Continuing with the main NL→SQL database startup.",
            e,
        )
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
        _ss = get_sync_target_schema()
        _du = os.getenv("DB_USER", "")
        _dn = os.getenv("DB_NAME", "")
        log.warning(
            "⚠️  No tables visible to the API (information_schema empty for DB_USER). "
            "Possible causes:\n"
            "  1. Tables were created under DB_ADMIN_USER in schema %r but DB_USER %r "
            "has no USAGE/SELECT — grant on that schema (sync target is DB_SYNC_SCHEMA).\n"
            "     Example fix: GRANT USAGE ON SCHEMA %s TO %s; "
            "GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s;\n"
            "  2. Database %r is empty or DB_SCHEMAS excludes every schema that has tables.\n"
            "  3. Wrong DB_HOST / DB_NAME / DB_USER in .env vs pgAdmin.\n"
            "Server will start but NL→SQL will fail until you fix permissions or sync data, "
            "then POST /reload-schema.",
            _ss,
            _du,
            _ss,
            _du,
            _ss,
            _du,
            _dn,
        )
    else:
        log.info(f"Schema loaded — {len(tables)} table(s): {tables}")
    yield
    log.info("Shutting down.")


# ── Rate limiter (per client IP) ──────────────────────────────────────────────
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(
    title="NL → SQL API",
    description="Natural Language to SQL — automatically adapts to all tables in the database.",
    version="2.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS — origins from ``CORS_ORIGINS`` (see ``utils.config.cors_origins``) ──
_ALLOWED_ORIGINS = cors_origins()

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Content-Type"],
)

# ── Session store — persisted to disk so history survives server restarts ─────
import json as _json
import threading

_SESSION_FILE = Path(os.getenv(
    "SESSION_STORE_FILE",
    Path(__file__).parent.parent / ".session_store.json"
))

_SESSION_MAX_TURNS = session_max_turns()
_SESSION_MAX_AGE_H = session_max_age_hours()

# ── LLM response cache (question → {sql, chart, viz_config}) ──────────────────
# Avoids a full 2–4 s LLM round-trip for repeated identical questions.
import hashlib as _hashlib
_llm_cache: dict[str, dict] = {}
_LLM_CACHE_MAX = llm_cache_max_entries()

def _llm_cache_key(prompt: str, table_fingerprint: str) -> str:
    raw = f"{prompt.lower().strip()}|{table_fingerprint}"
    return _hashlib.md5(raw.encode()).hexdigest()

def _llm_cache_get(key: str) -> dict | None:
    return _llm_cache.get(key)

def _llm_cache_set(key: str, value: dict) -> None:
    if len(_llm_cache) >= _LLM_CACHE_MAX:
        # evict oldest quarter
        for k in list(_llm_cache.keys())[:_LLM_CACHE_MAX // 4]:
            _llm_cache.pop(k, None)
    _llm_cache[key] = value

# ── Agent table-selection threshold ───────────────────────────────────────────
# When the DB has ≤ this many tables, skip the LLM agent (which costs ~1-2 s)
# and use the fast FAISS retriever directly.
_AGENT_TABLE_THRESHOLD = agent_table_threshold()


def _load_sessions() -> dict[str, list[dict]]:
    """Load sessions from disk. Drops sessions older than _SESSION_MAX_AGE_H."""
    if not _SESSION_FILE.exists():
        return {}
    try:
        data = _json.loads(_SESSION_FILE.read_text(encoding="utf-8"))
        cutoff = time.time() - _SESSION_MAX_AGE_H * 3600
        # Each session entry is {turns: [...], updated_at: float}
        return {
            sid: entry["turns"]
            for sid, entry in data.items()
            if entry.get("updated_at", 0) >= cutoff
        }
    except Exception as e:
        log.warning(f"Session file load failed ({e}) — starting fresh.")
        return {}


def _save_sessions(store: dict[str, list[dict]]) -> None:
    """Persist sessions to disk in a background thread — never blocks the response."""
    snapshot = dict(store)  # shallow copy so we don't race on the live dict
    def _write():
        try:
            _SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                sid: {"turns": turns, "updated_at": time.time()}
                for sid, turns in snapshot.items()
            }
            _SESSION_FILE.write_text(_json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        except Exception as e:
            log.warning(f"Session file save failed: {e}")
    threading.Thread(target=_write, daemon=True).start()


import time

sessions: dict[str, list[dict]] = _load_sessions()
log.info(f"Session store loaded — {len(sessions)} active session(s).")


# ── Request / Response models ─────────────────────────────────────────────────

class QueryRequest(BaseModel):
    prompt:     str
    session_id: Optional[str] = "default"
    top_k:      Optional[int] = 3
    row_limit:  Optional[int] = 20    # default rows returned per page
    offset:     Optional[int] = 0     # pagination offset (0 = first page)

    @field_validator("prompt")
    @classmethod
    def prompt_must_be_valid(cls, v: str) -> str:
        v = v.strip()
        if len(v) < 3:
            raise ValueError("Prompt is too short (minimum 3 characters).")
        if len(v) > 500:
            raise ValueError("Prompt is too long (maximum 500 characters).")
        return v

    @field_validator("top_k")
    @classmethod
    def top_k_must_be_valid(cls, v: int) -> int:
        if v is not None and not (1 <= v <= 10):
            raise ValueError("top_k must be between 1 and 10.")
        return v

    @field_validator("row_limit")
    @classmethod
    def row_limit_must_be_valid(cls, v: int) -> int:
        if v is not None and not (1 <= v <= 1000):
            raise ValueError("row_limit must be between 1 and 1000.")
        return v

    @field_validator("offset")
    @classmethod
    def offset_must_be_valid(cls, v: int) -> int:
        if v is not None and v < 0:
            raise ValueError("offset must be 0 or greater.")
        return v

class QueryResponse(BaseModel):
    sql:              str
    explanation:      str
    chart_suggestion: str
    viz_config:       Optional[dict] = None
    columns:          list[str]
    rows:             list[dict]
    row_count:        int
    total_count:      int
    has_more:         bool
    execution_ms:     int
    tables_used:      list[str]

class PageRequest(BaseModel):
    sql:       str
    page:      Optional[int] = 1
    page_size: Optional[int] = 500

    @field_validator("sql")
    @classmethod
    def sql_must_be_present(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("sql field must not be empty.")
        return v.strip()

class PageResponse(BaseModel):
    columns:      list[str]
    rows:         list[dict]
    row_count:    int
    total_count:  int
    page:         int
    page_size:    int
    total_pages:  int
    has_next:     bool
    has_prev:     bool
    execution_ms: int

class ImportRequest(BaseModel):
    table_name: str
    query:      str
    api_url:    Optional[str] = None   # defaults to REMOTE_API_URL in .env

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v: str) -> str:
        import re as _re
        v = v.strip().lower()
        if not _re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", v):
            raise ValueError(
                "Table name must start with a letter/underscore and contain "
                "only letters, digits, or underscores (max 63 chars)."
            )
        return v

    @field_validator("query")
    @classmethod
    def validate_query(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("Query must not be empty.")
        if len(v) > 2000:
            raise ValueError("Query is too long (max 2000 characters).")
        return v


class SyncRequest(BaseModel):
    """
    Sync multiple tables from the remote API in one call.
    tables    — list of table names to sync (from uploaded schema JSON)
    row_limit — rows to fetch per table (default 1000; 0 = no limit / all rows)
    api_url   — optional override for the remote API URL
    """
    tables:    list[str]
    row_limit: Optional[int] = 1000
    api_url:   Optional[str] = None

    @field_validator("tables")
    @classmethod
    def validate_tables(cls, v: list) -> list:
        import re as _re
        bad = [t for t in v if not _re.match(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$", str(t).strip())]
        if bad:
            raise ValueError(f"Invalid table name(s): {bad}")
        if len(v) > 50:
            raise ValueError("Cannot sync more than 50 tables at once.")
        return [str(t).strip().lower() for t in v]

    @field_validator("row_limit")
    @classmethod
    def validate_row_limit(cls, v: int) -> int:
        if v is not None and not (1 <= v <= 10000):
            raise ValueError("row_limit must be between 1 and 10000.")
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

@app.get("/health")
@limiter.exempt
def health(request: Request):
    tables      = list(app_state.get("schema", {}).get("tables", {}).keys())
    db_name     = os.getenv("DB_NAME", "")
    scan_desc   = schema_scan_description()
    sync_s      = get_sync_target_schema()
    has_tables  = len(tables) > 0
    return {
        "status":       "ok",
        "has_tables":   has_tables,
        "table_count":  len(tables),
        "tables_loaded": tables,
        "db_name":      db_name,
        # NL→SQL metadata scan (empty DB_SCHEMAS = all non-system schemas)
        "db_schema_scan": scan_desc,
        # Importer CREATE / GRANT target (see DB_SYNC_SCHEMA)
        "db_sync_schema": sync_s,
        # Backward-compatible alias for UIs that expect db_schemas
        "db_schemas":   scan_desc,
        # Clear message for the UI to display when no tables exist
        "empty_message": (
            None if has_tables else
            f"No tables visible to DB_USER in database {db_name!r} "
            f"(metadata scan: {scan_desc}; importer/sync schema: {sync_s!r}). "
            "Align DB_* with pgAdmin, include every schema that holds tables in DB_SCHEMAS, "
            "set DB_SYNC_SCHEMA where imports create tables, then click 🔄 Reload DB."
        ),
    }


@app.get("/schema")
@limiter.limit("30/minute")
def get_schema(request: Request):
    """Returns the full schema metadata (columns, FKs, sample rows)."""
    schema = app_state.get("schema", {})
    # strip sample rows for brevity
    clean = {}
    for t, meta in schema.get("tables", {}).items():
        clean[t] = {
            "columns":      meta["columns"],
            "foreign_keys": meta["foreign_keys"],
        }
    return clean


@app.get("/schema/tables")
@limiter.limit("60/minute")
def get_schema_tables(request: Request):
    """
    Returns a lightweight summary of every loaded table — used by the
    sidebar Schema Browser for instant client-side search.
    Each entry: { name, schema_name, column_count, columns: [name, type, is_pk], row_estimate }
    """
    schema = app_state.get("schema", {})
    result = []
    for key, meta in schema.get("tables", {}).items():
        cols = meta.get("columns", [])
        result.append({
            "name":         key,                         # "table" or "schema.table"
            "schema_name":  meta.get("schema_name", "public"),
            "table_name":   meta.get("table_name", key),
            "column_count": len(cols),
            "columns": [
                {
                    "name":  c["column_name"],
                    "type":  c["data_type"],
                    "is_pk": c.get("is_primary_key", False),
                }
                for c in cols[:20]          # first 20 columns for display
            ],
            "fk_count":     len(meta.get("foreign_keys", [])),
            "has_sample":   bool(meta.get("sample_rows")),
        })
    result.sort(key=lambda x: x["name"])
    return {"tables": result, "total": len(result)}


@app.get("/suggest-prompts")
@limiter.limit("30/minute")
def suggest_prompts_endpoint(request: Request, last_query: str = ""):
    """
    Returns 6 dynamic example prompts based on the live DB schema.
    If last_query is provided, 3 of them are follow-ups to that question.
    """
    table_catalog = app_state.get("table_catalog", "")
    prompts = suggest_prompts(table_catalog, last_query)
    return {"prompts": prompts}


@app.post("/api/platform/auth/signup", response_model=SignUpResponse)
@limiter.limit("20/minute")
def signup_endpoint(request: Request, req: SignUpRequest):
    if req.password != req.confirm_password:
        raise HTTPException(status_code=400, detail="Confirm password does not match.")

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
    schema        = app_state["schema"]
    descriptions  = app_state["descriptions"]
    table_catalog = app_state["table_catalog"]
    retriever: SchemaRetriever = app_state["retriever"]
    all_tables    = list(schema["tables"].keys())

    # ── Step 1: Load chat history FIRST (needed for follow-up context) ──────────
    history = sessions.get(req.session_id, [])

    # Build a context string from the last user turn for the table agent
    prior_context = ""
    for turn in reversed(history[-4:]):
        if turn["role"] == "user":
            prior_context = turn["content"]
            break
    agent_query = (
        f"Previous question: {prior_context}\nCurrent question: {req.prompt}"
        if prior_context else req.prompt
    )

    # ── Step 2: Table selection — fast path vs LLM agent ─────────────────────
    # Small schemas (≤ threshold): FAISS is instant (~5 ms).
    # Large schemas: LLM agent reads full catalog and reasons (~1–2 s extra).
    if len(all_tables) <= _AGENT_TABLE_THRESHOLD:
        selected_tables = retriever.retrieve_with_fk_expansion(
            req.prompt, schema, top_k=req.top_k
        )
        log.info(f"[{req.session_id}] FAISS (fast path, {len(all_tables)} tables): {selected_tables}")
    else:
        selected_tables = select_tables_agent(
            user_query      = agent_query,
            table_catalog   = table_catalog,
            all_table_names = all_tables,
        )
        log.info(f"[{req.session_id}] Agent selected: {selected_tables}")
        if not selected_tables:
            selected_tables = retriever.retrieve_with_fk_expansion(
                req.prompt, schema, top_k=req.top_k
            )
            log.info(f"[{req.session_id}] FAISS fallback: {selected_tables}")

    # ── Auto-expand via FK relationships ─────────────────────────────────────
    fk_expanded = set(selected_tables)
    for table in list(fk_expanded):
        for fk in schema["tables"].get(table, {}).get("foreign_keys", []):
            fk_expanded.add(fk["foreign_table"])
    selected_tables = list(fk_expanded)
    log.info(f"[{req.session_id}] Final tables: {selected_tables}")

    # ── Step 3: LLM response cache check ─────────────────────────────────────
    # Identical question + same set of tables → return cached result instantly.
    table_fp  = ",".join(sorted(selected_tables))
    cache_key = _llm_cache_key(req.prompt, table_fp)
    cached    = _llm_cache_get(cache_key)
    if cached:
        log.info(f"[{req.session_id}] LLM cache HIT — skipping Gemini call")
        llm_result = cached
    else:
        try:
            llm_result = generate_sql(
                user_query         = req.prompt,
                selected_tables    = selected_tables,
                table_descriptions = descriptions,
                schema             = schema,
                chat_history       = history,
                row_limit          = req.row_limit or 20,
                offset             = req.offset or 0,
            )
        except genai_errors.ClientError as e:
            code = getattr(e, "status_code", None)
            if code == 429 or "RESOURCE_EXHAUSTED" in str(e).upper():
                log.warning("[%s] Gemini rate limit after retries: %s", req.session_id, e)
                raise HTTPException(
                    status_code=503,
                    detail=(
                        "Google Gemini returned HTTP 429 (quota / rate limit). "
                        "Wait a short time and try again, or spread out requests. "
                        "See https://ai.google.dev/gemini-api/docs/rate-limits — "
                        f"Technical detail: {e}"
                    ),
                )
            raise
        except ValueError as e:
            log.warning("[%s] LLM output could not be parsed: %s", req.session_id, e)
            raise HTTPException(
                status_code=502,
                detail=str(e),
            )
        _llm_cache_set(cache_key, llm_result)
        log.info(f"[{req.session_id}] LLM cache SET")

    sql              = llm_result["sql"]
    explanation      = llm_result.get("explanation", "")
    chart_suggestion = llm_result.get("chart_suggestion", "table")
    viz_config       = llm_result.get("viz_config") or {}

    log.info(f"[{req.session_id}] Generated SQL: {sql}")

    # 4. Validate
    try:
        sql = validate_sql(sql)
    except SQLValidationError as e:
        log.warning(f"SQL validation failed for prompt '{req.prompt}': {e} | Generated SQL: {sql}")
        # Return a friendly message that guides the user to rephrase
        raise HTTPException(
            status_code=400,
            detail=(
                f"{e} — The AI generated an unsafe or unsupported query for your request.\n"
                f"Generated SQL was: {sql[:200]}{'…' if len(sql) > 200 else ''}\n"
                "Try rephrasing your question, e.g. 'Show me all users' or "
                "'List users and their role names'."
            ),
        )

    # 5. Execute
    try:
        exec_result = execute_sql(sql)
    except Exception as e:
        log.error(f"SQL execution error: {e}")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}")

    # 6. Save to session history (in-memory + persisted to disk)
    if req.session_id:
        if req.session_id not in sessions:
            sessions[req.session_id] = []
        sessions[req.session_id].append({"role": "user",      "content": req.prompt})
        sessions[req.session_id].append({"role": "assistant", "content": sql})
        # keep only last N turns
        sessions[req.session_id] = sessions[req.session_id][-_SESSION_MAX_TURNS:]
        _save_sessions(sessions)   # persist to disk for restart survival

    log.info(
        f"[{req.session_id}] Returned {exec_result['row_count']} rows "
        f"in {exec_result['execution_ms']}ms"
    )

    return QueryResponse(
        sql=sql,
        explanation=explanation,
        chart_suggestion=chart_suggestion,
        viz_config=viz_config,
        tables_used=selected_tables,
        total_count=exec_result.get("total_count", exec_result.get("row_count", 0)),
        has_more=exec_result.get("has_more", False),
        **{k: v for k, v in exec_result.items() if k not in ("total_count", "has_more")},
    )


@app.post("/sql/page", response_model=PageResponse)
@limiter.limit("20/minute")
def paginate_sql(request: Request, req: PageRequest):
    """
    Run a raw SELECT with pagination.
    Use this to fetch large result sets page by page.

    Example:
        POST /sql/page
        { "sql": "SELECT * FROM your_table", "page": 2, "page_size": 1000 }
    """
    try:
        result = execute_sql_page(req.sql, page=req.page, page_size=req.page_size)
    except SQLValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        log.error(f"Pagination error: {e}")
        raise HTTPException(status_code=500, detail=f"SQL execution failed: {e}")
    return PageResponse(**result)


@app.delete("/session/{session_id}")
@limiter.limit("10/minute")
def clear_session(request: Request, session_id: str):
    """Clears conversation history for a session."""
    sessions.pop(session_id, None)
    _save_sessions(sessions)
    return {"cleared": session_id}


@app.get("/cache/stats")
@limiter.limit("30/minute")
def get_cache_stats(request: Request):
    """Returns SQL result cache statistics (hit count, live entries, TTL)."""
    return cache_stats()


@app.delete("/cache/clear")
@limiter.limit("10/minute")
def clear_cache(request: Request):
    """Clears all SQL result cache entries immediately."""
    removed = cache_clear()
    log.info(f"SQL cache cleared — {removed} entries removed.")
    return {"cleared_entries": removed}


@app.post("/reload-schema")
@limiter.limit("5/minute")
def reload_schema(request: Request):
    """
    Hot-reload: re-scan all DB tables and rebuild the FAISS index.
    Call this after adding new tables to the database — no server restart needed.
    """
    try:
        log.info("Schema hot-reload triggered …")
        schema, rmeta = _extract_schema_with_reader_repair()
        descriptions  = schema_to_text(schema)
        table_catalog = build_table_catalog(schema)
        retriever: SchemaRetriever = app_state["retriever"]
        retriever.rebuild(descriptions)

        app_state["schema"]        = schema
        app_state["descriptions"]  = descriptions
        app_state["table_catalog"] = table_catalog
        cleared = cache_clear()   # invalidate stale SQL results after schema change

        tables = list(schema["tables"].keys())
        log.info(f"Schema reloaded — {len(tables)} table(s): {tables}")
        payload = {
            "status":              "reloaded",
            "tables_found":        tables,
            "table_count":         len(tables),
            "sql_cache_cleared":   cleared,
            "reader_grants_repaired": rmeta.get("reader_grants_repaired", False),
            "admin_public_table_count": rmeta.get("admin_public_table_count"),
        }
        if rmeta.get("hint"):
            payload["hint"] = rmeta["hint"]
        if rmeta.get("repair_error"):
            payload["repair_error"] = rmeta["repair_error"]
        return payload
    except Exception as e:
        log.error(f"Schema reload failed: {e}")
        raise HTTPException(status_code=500, detail=f"Reload failed: {e}")


# ── Import table from remote API ──────────────────────────────────────────────

@app.post("/import-table")
@limiter.limit("5/minute")
def import_table_endpoint(req: ImportRequest, request: Request):
    """
    1. POSTs req.query to the remote SQL-passthrough API (REMOTE_API_URL).
    2. Creates (or updates) the table req.table_name in local PostgreSQL.
    3. Upserts all returned rows.
    4. Hot-reloads the schema so the NL-to-SQL system sees the new table immediately.
    """
    try:
        log.info(
            f"Import request: table='{req.table_name}' "
            f"query={req.query[:80]!r} api_url={req.api_url!r}"
        )

        # Step 1 — fetch from remote API
        records = fetch_from_remote_api(req.query, req.api_url)
        if not records:
            raise HTTPException(
                status_code=404,
                detail="Remote API returned 0 records. Check the query.",
            )
        log.info(f"Remote API returned {len(records)} records.")

        # Step 2 — import into local PostgreSQL (admin connection)
        rows_imported = import_table_to_db(req.table_name, records)

        # Step 3 — hot-reload schema so the new table is queryable immediately
        schema, rmeta = _extract_schema_with_reader_repair()
        descriptions  = schema_to_text(schema)
        table_catalog = build_table_catalog(schema)
        retriever: SchemaRetriever = app_state["retriever"]
        retriever.rebuild(descriptions)
        app_state["schema"]        = schema
        app_state["descriptions"]  = descriptions
        app_state["table_catalog"] = table_catalog

        tables = list(schema["tables"].keys())
        log.info(
            f"Import done: {rows_imported} rows into '{req.table_name}'. "
            f"DB tables: {tables}"
        )
        out = {
            "status":        "imported",
            "table_name":    req.table_name,
            "rows_imported": rows_imported,
            "tables_in_db":  tables,
            "table_count":   len(tables),
            "reader_grants_repaired": rmeta.get("reader_grants_repaired", False),
        }
        if rmeta.get("hint"):
            out["hint"] = rmeta["hint"]
        return out

    except HTTPException:
        raise
    except Exception as e:
        log.error(f"Import failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ── Sync multiple tables from uploaded schema JSON ────────────────────────────

@app.post("/sync-tables")
@limiter.limit("30/minute")
def sync_tables_endpoint(req: SyncRequest, request: Request):
    """
    For each table in req.tables:
      1. Fetch up to req.row_limit rows from the remote API.
      2. If the table doesn't exist locally → CREATE TABLE + INSERT all rows.
      3. If it exists → ADD any missing columns + UPSERT all rows.
      4. After all tables are done, hot-reload the schema.

    Returns per-table status + final DB table list.
    """
    results   = {}
    # row_limit=0 means fetch ALL rows (no LIMIT clause)
    row_limit = req.row_limit if req.row_limit is not None else 1000

    for table in req.tables:
        query = (
            f'SELECT * FROM "{table}"'
            if row_limit == 0
            else f'SELECT * FROM "{table}" ORDER BY id LIMIT {row_limit}'
        )
        try:
            log.info(f"Syncing table '{table}' (limit={row_limit})…")
            records = fetch_from_remote_api(query, req.api_url)
            if not records:
                results[table] = {"status": "skipped", "reason": "API returned 0 rows"}
                continue

            status = sync_table(table, records)
            results[table] = {"status": "ok", **status}
            log.info(f"Sync '{table}' done: {status}")

        except Exception as e:
            log.error(f"Sync failed for '{table}': {e}", exc_info=True)
            results[table] = {"status": "error", "reason": str(e)}

    # Hot-reload schema so all new/updated tables are immediately queryable
    sync_meta: dict = {}
    try:
        schema, sync_meta = _extract_schema_with_reader_repair()
        descriptions  = schema_to_text(schema)
        table_catalog = build_table_catalog(schema)
        retriever: SchemaRetriever = app_state["retriever"]
        retriever.rebuild(descriptions)
        app_state["schema"]        = schema
        app_state["descriptions"]  = descriptions
        app_state["table_catalog"] = table_catalog
        db_tables = list(schema["tables"].keys())
    except Exception as e:
        log.error(f"Schema reload after sync failed: {e}")
        db_tables = []

    out = {
        "results":      results,
        "tables_in_db": db_tables,
        "table_count":  len(db_tables),
        "reader_grants_repaired": sync_meta.get("reader_grants_repaired", False),
    }
    if sync_meta.get("hint"):
        out["hint"] = sync_meta["hint"]
    if sync_meta.get("repair_error"):
        out["repair_error"] = sync_meta["repair_error"]
    if sync_meta.get("admin_public_table_count") is not None:
        out["admin_public_table_count"] = sync_meta["admin_public_table_count"]
    return out