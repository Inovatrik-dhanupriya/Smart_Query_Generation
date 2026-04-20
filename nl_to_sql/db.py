# Any file needing DB access →
# calls get_cursor() →
# gets DB connection →
# auto commit/rollback →
# returns connection to pool

import logging
import os

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from utils.env import load_app_env

load_app_env()

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host":             os.getenv("DB_HOST", "localhost"),
    "port":             int(os.getenv("DB_PORT", 5432)),
    # DB_NAME must be set in .env (validated at app startup in main.py).
    "dbname":           os.getenv("DB_NAME", ""),
    "user":             os.getenv("DB_USER", "postgres"),
    "password":         os.getenv("DB_PASSWORD", ""),
    "connect_timeout":  int(os.getenv("DB_CONNECT_TIMEOUT", 10)),  # seconds
    "options":          "-c default_transaction_read_only=on",      # read-only session
}

_pool: psycopg2.pool.ThreadedConnectionPool | None = None


def get_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        if not DB_CONFIG["user"] or not DB_CONFIG["dbname"]:
            raise RuntimeError("DB_USER and DB_NAME must be set in environment.")
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            **DB_CONFIG,
        )
        log.info(
            f"DB pool created — {DB_CONFIG['user']}@{DB_CONFIG['host']}:"
            f"{DB_CONFIG['port']}/{DB_CONFIG['dbname']} (read-only session)"
        )
    return _pool


@contextmanager
def get_cursor(dict_cursor: bool = True):
    """Thread-safe cursor from the connection pool. Always rolls back on error."""
    pool = get_pool()
    conn = pool.getconn()
    try:
        factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)