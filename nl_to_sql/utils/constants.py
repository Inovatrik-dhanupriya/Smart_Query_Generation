"""
Named defaults for literals that were previously inline ("static" module-level
constants). Environment variables and call-site overrides still take precedence
where they did before; this file is the single source for those default values.
"""
from __future__ import annotations

# ── HTTP API defaults (``utils.config`` may fall back to these) ─────────────
DEFAULT_NL_SQL_API_BASE = "http://127.0.0.1:8000"
DEFAULT_CORS_DEV_ORIGINS: tuple[str, ...] = (
    "http://localhost:8501",
    "http://127.0.0.1:8501",
)

# ── slowapi / ``main`` route limits (same strings as before) ─────────────────
RATE_LIMIT_DEFAULT = "60/minute"
RL_DB_CONNECT = "30/minute"
RL_DB_USE_DATABASE = "30/minute"
RL_DB_LIST_SCHEMAS = "60/minute"
RL_DB_LIST_TABLES = "60/minute"
RL_DB_ACTIVATE = "10/minute"
RL_SCHEMA_FROM_FILE = "10/minute"
RL_SCHEMA_FROM_FILE_ASYNC = "10/minute"
RL_SCHEMA_JOB_STATUS = "120/minute"
RL_SCHEMA_JOB_CONTROL = "60/minute"
RL_GET_SCHEMA = "30/minute"
RL_GET_SCHEMA_TABLES = "60/minute"
RL_SUGGEST_PROMPTS = "30/minute"
RL_AUTH_SIGNUP = "20/minute"
RL_AUTH_SIGNIN = "30/minute"
RL_GENERATE_SQL = "10/minute"
RL_SQL_PAGE = "20/minute"
RL_CLEAR_SESSION = "10/minute"
RL_CACHE_STATS = "30/minute"
RL_CACHE_CLEAR = "10/minute"
RL_RELOAD_SCHEMA = "5/minute"
RL_IMPORT_TABLE = "5/minute"
RL_SYNC_TABLES = "30/minute"

# ── Streamlit client → API (``ui/auth/service.py``) ──────────────────────────
UI_AUTH_HTTP_TIMEOUT_SEC = 15

# ── Streamlit auth persistence (``ui/auth/session.py``) ──────────────────────
AUTH_SESSION_TTL_MINUTES_DEFAULT = 30
AUTH_QUERY_PARAM_USER = "sqg_auth_u"
AUTH_QUERY_PARAM_EXP = "sqg_auth_e"
AUTH_LOCAL_SESSION_FILENAME = ".auth_session.json"

# ── Google GenAI (defaults for ``os.getenv`` fallbacks in ``llm/client``) ──
DEFAULT_GEMINI_TEXT_MODEL = "gemini-2.5-pro"
DEFAULT_GEMINI_EMBED_MODEL = "models/gemini-embedding-001"

# ── FAISS / schema retrieval (``schema/retriever``) ──────────────────────────
GEMINI_EMBEDDING_DIMENSION = 3072
FAISS_EMBED_BATCH_SIZE = 100
FAISS_MAX_FK_CLOSURE = 24
FAISS_DEFAULT_TOP_K = 3
FAISS_RETRIEVE_SCORE_THRESHOLD = 0.3
FK_EXPAND_MAX_ITERATIONS = 4
FK_EXPAND_MIN_MAX_TOTAL = 12

# ── LLM (``llm/service`` table selector) ──────────────────────────────────
TABLE_SELECTOR_MAX_OUTPUT_TOKENS = 150
