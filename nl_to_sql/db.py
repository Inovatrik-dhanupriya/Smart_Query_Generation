# Session-scoped PostgreSQL pools — credentials come from the UI / API, not from .env.
# Any module needing DB access calls get_cursor(session_id, …).

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

log = logging.getLogger(__name__)

_pools: dict[str, psycopg2.pool.ThreadedConnectionPool] = {}
_lock = threading.Lock()


@dataclass(frozen=True)
class PgCredentials:
    host: str
    port: int
    user: str
    password: str
    database: str


def _pool_key(session_id: str) -> str:
    return session_id.strip()


def close_pool(session_id: str) -> None:
    """Close and remove the pool for this session, if any."""
    key = _pool_key(session_id)
    with _lock:
        pool = _pools.pop(key, None)
    if pool is not None:
        try:
            pool.closeall()
        except Exception as e:
            log.warning("close_pool(%s): %s", key, e)


def register_pool(session_id: str, creds: PgCredentials, *, read_only: bool = True) -> None:
    """
    Create (or replace) a connection pool for ``session_id``.
    ``read_only=True`` sets default_transaction_read_only=on (NL→SQL execution path).
    """
    key = _pool_key(session_id)
    close_pool(key)
    opts = (
        "-c default_transaction_read_only=on"
        if read_only
        else "-c default_transaction_read_only=off"
    )
    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=10,
        host=creds.host.strip(),
        port=int(creds.port),
        dbname=creds.database.strip(),
        user=creds.user,
        password=creds.password,
        connect_timeout=10,
        options=opts,
    )
    with _lock:
        _pools[key] = pool
    mode = "read-only" if read_only else "read-write"
    log.info(
        "DB pool for session %s — %s@%s:%s/%s (%s)",
        key[:8] + "…",
        creds.user,
        creds.host,
        creds.port,
        creds.database,
        mode,
    )


def has_pool(session_id: str) -> bool:
    return _pool_key(session_id) in _pools


def get_pool(session_id: str) -> psycopg2.pool.ThreadedConnectionPool:
    key = _pool_key(session_id)
    pool = _pools.get(key)
    if pool is None:
        raise RuntimeError(
            "No database connection for this session. Connect from the UI first."
        )
    return pool


@contextmanager
def get_cursor(session_id: str, dict_cursor: bool = True):
    """Thread-safe cursor from the session pool. Always rolls back on error."""
    pool = get_pool(session_id)
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


def open_write_connection(creds: PgCredentials):
    """Single read-write connection (DDL/DML). Caller must close()."""
    return psycopg2.connect(
        host=creds.host.strip(),
        port=int(creds.port),
        dbname=creds.database.strip(),
        user=creds.user,
        password=creds.password,
        connect_timeout=10,
        options="-c default_transaction_read_only=off",
    )


def one_shot_connect(creds: PgCredentials, *, read_only: bool = True):
    """
    Single connection (not pooled). Caller must close().
    Used to list databases/schemas before a session pool exists.
    """
    opts = (
        "-c default_transaction_read_only=on"
        if read_only
        else "-c default_transaction_read_only=off"
    )
    return psycopg2.connect(
        host=creds.host.strip(),
        port=int(creds.port),
        dbname=creds.database.strip(),
        user=creds.user,
        password=creds.password,
        connect_timeout=10,
        options=opts,
    )
