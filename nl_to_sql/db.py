# Session-scoped PostgreSQL pools — credentials come from the UI / API, not from .env.
# Any module needing DB access calls get_cursor(session_id, …).

from __future__ import annotations

import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import psycopg2
import psycopg2.pool
from psycopg2 import sql as pg_sql
from psycopg2.extras import RealDictCursor

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
_SAFE_DB_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")
_warned_auth_runtime_fallback = False


def _app_db_name() -> str:
    target_db = os.getenv("USER_DETAILS_DATABASE_NAME", "Userdetails").strip()
    if not target_db:
        target_db = "Userdetails"
    if not _SAFE_DB_NAME_RE.match(target_db):
        raise RuntimeError(
            "USER_DETAILS_DATABASE_NAME is invalid. "
            "Use letters/numbers/underscore and start with a letter/underscore."
        )
    return target_db


def _pg_bootstrap_credentials() -> tuple[str, str]:
    """
    Credentials for app bootstrap (CREATE DATABASE, auth DDL, role grants).

    Order:
    1. DB_ADMIN_USER + DB_ADMIN_PASSWORD (explicit superuser/owner)
    2. DB_USER + DB_PASSWORD (typical local dev: single ``postgres`` account)

    No exception here — connect may still fail if the password is wrong.
    """
    admin_user = os.getenv("DB_ADMIN_USER", "").strip()
    admin_password = os.getenv("DB_ADMIN_PASSWORD", "").strip()
    if admin_user and admin_password:
        return admin_user, admin_password
    u = (os.getenv("DB_USER", "") or "postgres").strip() or "postgres"
    p = os.getenv("DB_PASSWORD", "")
    return u, p


def _auth_runtime_credentials() -> tuple[str, str]:
    """
    Runtime credentials for auth reads/writes on USER_DETAILS_DATABASE_NAME.

    Order:
    1. AUTH_DB_USER / AUTH_DB_PASSWORD (least-privilege role, preferred in production)
    2. DB_ADMIN_USER / DB_ADMIN_PASSWORD
    3. DB_USER / DB_PASSWORD (same as bootstrap)
    """
    global _warned_auth_runtime_fallback
    auth_user = os.getenv("AUTH_DB_USER", "").strip()
    auth_password = os.getenv("AUTH_DB_PASSWORD", "").strip()
    if auth_user and auth_password:
        return auth_user, auth_password

    admin_user = os.getenv("DB_ADMIN_USER", "").strip()
    admin_password = os.getenv("DB_ADMIN_PASSWORD", "").strip()
    if admin_user and admin_password:
        if not _warned_auth_runtime_fallback:
            log.warning(
                "AUTH_DB_USER/AUTH_DB_PASSWORD not set. "
                "Using DB_ADMIN_* for app auth database (set AUTH_DB_* in production)."
            )
            _warned_auth_runtime_fallback = True
        return admin_user, admin_password

    u, p = _pg_bootstrap_credentials()
    if not _warned_auth_runtime_fallback:
        log.warning(
            "AUTH_DB_USER/AUTH_DB_PASSWORD not set. "
            "Using DB_USER/DB_PASSWORD for app auth database (set AUTH_DB_* in production)."
        )
        _warned_auth_runtime_fallback = True
    return u, p


def _connect_bootstrap(dbname: str) -> psycopg2.extensions.connection:
    b_user, b_password = _pg_bootstrap_credentials()
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=dbname,
        user=b_user,
        password=b_password,
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    )


def _connect_auth_runtime(dbname: str) -> psycopg2.extensions.connection:
    auth_user, auth_password = _auth_runtime_credentials()
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=dbname,
        user=auth_user,
        password=auth_password,
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    )
_pools: dict[str, psycopg2.pool.ThreadedConnectionPool] = {}
_ssh_tunnels: dict[str, Any] = {}
_lock = threading.Lock()


@dataclass(frozen=True)
class PgCredentials:
    host: str
    port: int
    user: str
    password: str
    database: str


def start_ssh_pg_tunnel(
    session_id: str,
    *,
    db_host: str,
    db_port: int,
    ssh_host: str,
    ssh_port: int = 22,
    ssh_username: str,
    ssh_private_key_pem: str,
    ssh_private_key_passphrase: str | None = None,
) -> tuple[str, int]:
    """
    Open an SSH local forward to PostgreSQL. Returns ``(connect_host, connect_port)`` to
    pass to :func:`register_pool` (typically ``127.0.0.1`` and a free local port).
    ``db_host`` / ``db_port`` is where Postgres listens *from the SSH server* (e.g. ``localhost`` or a VPC IP).
    """
    from io import StringIO

    try:
        import paramiko  # type: ignore[import-not-found]
        # Compatibility: Paramiko 3.x removed DSSKey; sshtunnel may still reference it.
        # We don't support DSA keys here, but defining the attribute prevents sshtunnel import errors.
        if not hasattr(paramiko, "DSSKey"):
            paramiko.DSSKey = paramiko.RSAKey  # type: ignore[attr-defined]
        from sshtunnel import SSHTunnelForwarder  # type: ignore[import-not-found]
    except ImportError as e:
        raise ValueError(
            "SSH tunnel requires the 'paramiko' and 'sshtunnel' packages on the machine that runs "
            "the API. Install them in the same Python environment as uvicorn, e.g.: "
            "pip install paramiko sshtunnel  "
            f"(import error: {e})"
        ) from e

    _stop_ssh_tunnel(session_id)
    pem = (ssh_private_key_pem or "").strip()
    if not pem:
        raise ValueError("SSH private key (PEM) is required for SSH mode.")

    pkey: paramiko.pkey.PKey | None = None
    for key_cls in (paramiko.RSAKey, paramiko.Ed25519Key, paramiko.ECDSAKey):
        try:
            pkey = key_cls.from_private_key(
                StringIO(pem),
                password=(ssh_private_key_passphrase or None) or None,
            )
            break
        except Exception:
            continue
    if pkey is None:
        raise ValueError("Could not load SSH private key. Paste a PEM (RSA, ECDSA, or Ed25519).")

    ch = (db_host or "").strip()
    cp = int(db_port)
    if not (1 <= cp <= 65535):
        raise ValueError("Invalid database port for SSH tunnel.")

    tunnel = SSHTunnelForwarder(
        (ssh_host.strip(), int(ssh_port)),
        ssh_username=(ssh_username or "").strip(),
        ssh_pkey=pkey,
        remote_bind_address=(ch, cp),
        local_bind_address=("127.0.0.1", 0),
    )
    try:
        tunnel.start()
    except Exception as e:
        try:
            tunnel.stop()
        except Exception:
            pass
        raise RuntimeError(f"SSH tunnel could not start: {e}") from e
    lport = tunnel.local_bind_port
    if lport is None:
        try:
            tunnel.stop()
        except Exception:
            pass
        raise RuntimeError("SSH tunnel did not return a local port.")
    if isinstance(lport, (list, tuple)):
        lport = lport[0]

    # Defensive check: ensure the local forward is actually listening before we try psycopg2.
    try:
        import socket

        with socket.create_connection(("127.0.0.1", int(lport)), timeout=1.5):
            pass
    except Exception as e:
        try:
            tunnel.stop()
        except Exception:
            pass
        raise RuntimeError(
            f"SSH tunnel started but local port {lport} is not reachable: {e}. "
            f"Check that PostgreSQL is reachable from the SSH host at {ch}:{cp}."
        ) from e

    key = _pool_key(session_id)
    with _lock:
        _ssh_tunnels[key] = tunnel
    return "127.0.0.1", int(lport)


def _pool_key(session_id: str) -> str:
    return session_id.strip()


def _stop_ssh_tunnel(session_id: str) -> None:
    key = _pool_key(session_id)
    t = _ssh_tunnels.pop(key, None)
    if t is not None:
        try:
            t.stop()  # sshtunnel.SSHTunnelForwarder
        except Exception as e:
            log.warning("ssh tunnel stop(%s): %s", key[:8], e)


def close_pool_only(session_id: str) -> None:
    """Close only the client DB pool, keep SSH tunnel (if any) for the same session."""
    key = _pool_key(session_id)
    with _lock:
        pool = _pools.pop(key, None)
    if pool is not None:
        try:
            pool.closeall()
        except Exception as e:
            log.warning("close_pool_only(%s): %s", key, e)


def close_pool(session_id: str) -> None:
    """Close the client pool and any SSH tunnel for this session (full reset)."""
    close_pool_only(session_id)
    _stop_ssh_tunnel(session_id)


def register_pool(session_id: str, creds: PgCredentials, *, read_only: bool = True) -> None:
    """
    Create (or replace) a connection pool for ``session_id``.
    ``read_only=True`` sets default_transaction_read_only=on (NL→SQL execution path).
    """
    key = _pool_key(session_id)
    # IMPORTANT: when using SSH tunneling, `start_ssh_pg_tunnel()` is called right before
    # `register_pool()`. Do NOT stop the tunnel here; only replace the connection pool.
    close_pool_only(key)
    opts = (
        "-c default_transaction_read_only=on"
        if read_only
        else "-c default_transaction_read_only=off"
    )
    # When connecting through an SSH tunnel, the local forwarder can take a moment
    # to become ready on Windows. Retry a few times instead of crashing the API with a 500.
    last_err: Exception | None = None
    for attempt in range(3):
        try:
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
            break
        except psycopg2.OperationalError as e:
            last_err = e
            # Only retry for sessions that have an SSH tunnel and only for "connection refused".
            msg = str(e)
            if _pool_key(session_id) not in _ssh_tunnels or "Connection refused" not in msg:
                raise
            time.sleep(0.35 * (attempt + 1))
    else:
        assert last_err is not None
        raise last_err
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


def ensure_userdetails_database() -> bool:
    """
    Ensure the app-level database exists (default: USER_DETAILS_DATABASE_NAME=Userdetails).

    Uses :func:`_pg_bootstrap_credentials` and connects to the maintenance database
    ``postgres`` only to check/create the target database. This does not modify DB_NAME.

    Returns:
        True  -> database was created
        False -> database already existed
    """
    target_db = _app_db_name()
    b_user, _ = _pg_bootstrap_credentials()

    conn = _connect_bootstrap("postgres")
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            exists = cur.fetchone() is not None
            if exists:
                log.info("App database %r already exists.", target_db)
                return False
            cur.execute(
                pg_sql.SQL("CREATE DATABASE {}").format(pg_sql.Identifier(target_db))
            )
            log.info("Created app database %r using bootstrap role %r.", target_db, b_user)
            return True
    finally:
        conn.close()


def ensure_auth_tables() -> None:
    """
    Create minimal auth table(s) in USER_DETAILS_DATABASE_NAME if missing.

    Current scope: fields required for Sign Up / Sign In.
    """
    target_db = _app_db_name()
    conn = _connect_bootstrap(target_db)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.auth_users (
                    id BIGSERIAL PRIMARY KEY,
                    email VARCHAR(255) NOT NULL,
                    company_name VARCHAR(255) NOT NULL DEFAULT '',
                    username VARCHAR(64) NOT NULL,
                    password_hash TEXT NOT NULL,
                    is_active BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    last_login_at TIMESTAMP WITHOUT TIME ZONE NULL
                )
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_auth_users_email_lower
                ON public.auth_users (LOWER(email))
                """
            )
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_auth_users_username_lower
                ON public.auth_users (LOWER(username))
                """
            )
            # Backfill column for older installs where auth_users was already created.
            cur.execute(
                """
                ALTER TABLE public.auth_users
                ADD COLUMN IF NOT EXISTS company_name VARCHAR(255) NOT NULL DEFAULT ''
                """
            )
            # Per-user tenant (company) and project list — Streamlit / workspace API
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.app_workspace_tenants (
                    user_id BIGINT NOT NULL REFERENCES public.auth_users(id) ON DELETE CASCADE,
                    id VARCHAR(64) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, id)
                )
                """
            )
            # Older installs included app_workspace_tenants.code; no longer used.
            cur.execute(
                """
                ALTER TABLE public.app_workspace_tenants
                DROP COLUMN IF EXISTS code
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.app_workspace_projects (
                    user_id BIGINT NOT NULL REFERENCES public.auth_users(id) ON DELETE CASCADE,
                    id VARCHAR(64) NOT NULL,
                    tenant_id VARCHAR(64) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status VARCHAR(32) NOT NULL DEFAULT 'Draft',
                    client_code VARCHAR(64) NOT NULL DEFAULT '',
                    nl_session_id VARCHAR(64) NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, id),
                    CONSTRAINT fk_app_ws_proj_tenant
                        FOREIGN KEY (user_id, tenant_id)
                        REFERENCES public.app_workspace_tenants (user_id, id)
                        ON DELETE CASCADE
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_app_ws_proj_user ON public.app_workspace_projects (user_id)"
            )
            # Cached catalog of activated tables (from client DB introspection) for support / re-hydration
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.app_project_schema_cache (
                    user_id BIGINT NOT NULL REFERENCES public.auth_users(id) ON DELETE CASCADE,
                    project_id VARCHAR(64) NOT NULL,
                    schema_json JSONB NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, project_id),
                    CONSTRAINT fk_app_schema_proj
                        FOREIGN KEY (user_id, project_id)
                        REFERENCES public.app_workspace_projects (user_id, id) ON DELETE CASCADE
                )
                """
            )
            cur.execute(
                "CREATE INDEX IF NOT EXISTS ix_app_schema_cache_user ON public.app_project_schema_cache (user_id)"
            )
            auth_user = os.getenv("AUTH_DB_USER", "").strip()
            auth_password = os.getenv("AUTH_DB_PASSWORD", "").strip()
            if bool(auth_user) != bool(auth_password):
                raise RuntimeError(
                    "Set both AUTH_DB_USER and AUTH_DB_PASSWORD, or leave both empty."
                )
            if auth_user:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (auth_user,))
                role_exists = cur.fetchone() is not None
                if role_exists:
                    cur.execute(
                        pg_sql.SQL("ALTER ROLE {} WITH LOGIN PASSWORD {}").format(
                            pg_sql.Identifier(auth_user),
                            pg_sql.Literal(auth_password),
                        )
                    )
                else:
                    cur.execute(
                        pg_sql.SQL("CREATE ROLE {} WITH LOGIN PASSWORD {}").format(
                            pg_sql.Identifier(auth_user),
                            pg_sql.Literal(auth_password),
                        )
                    )

                cur.execute(
                    pg_sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(
                        pg_sql.Identifier(auth_user)
                    )
                )
                cur.execute(
                    pg_sql.SQL("GRANT SELECT, INSERT, UPDATE ON TABLE public.auth_users TO {}").format(
                        pg_sql.Identifier(auth_user)
                    )
                )
                cur.execute(
                    pg_sql.SQL("GRANT USAGE, SELECT ON SEQUENCE public.auth_users_id_seq TO {}").format(
                        pg_sql.Identifier(auth_user)
                    )
                )
                for _tbl in (
                    "app_workspace_tenants",
                    "app_workspace_projects",
                    "app_project_schema_cache",
                ):
                    cur.execute(
                        pg_sql.SQL(
                            "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.{} TO {}"
                        ).format(pg_sql.Identifier(_tbl), pg_sql.Identifier(auth_user))
                    )
        conn.commit()
        log.info("Ensured auth tables in app database %r.", target_db)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ------start of new code------------------------------------------------------------------------------------
def ensure_projects_table() -> None:
    """
    Create projects table in USER_DETAILS_DATABASE_NAME if missing.

    Each project belongs to an auth user via ``user_id -> auth_users(id)``.
    """
    target_db = _app_db_name()
    conn = _connect_bootstrap(target_db)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.projects (
                    id BIGSERIAL PRIMARY KEY,
                    user_id BIGINT NOT NULL REFERENCES public.auth_users(id) ON DELETE CASCADE,
                    project_name VARCHAR(255) NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status VARCHAR(16) NOT NULL DEFAULT 'active'
                        CHECK (status IN ('active', 'inactive')),
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW()
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS ix_projects_user_id
                ON public.projects (user_id)
                """
            )
        conn.commit()
        log.info("Ensured projects table in app database %r.", target_db)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_project_db_connections_table() -> None:
    """
    Create project_db_connections table in USER_DETAILS_DATABASE_NAME if missing.

    One project maps to one DB connection (UNIQUE project_id).
    """
    target_db = _app_db_name()
    conn = _connect_bootstrap(target_db)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.project_db_connections (
                    id BIGSERIAL PRIMARY KEY,
                    project_id BIGINT NOT NULL
                        REFERENCES public.projects(id) ON DELETE CASCADE,
                    db_type VARCHAR(32) NOT NULL DEFAULT 'postgres',
                    host VARCHAR(255) NOT NULL,
                    port INTEGER NOT NULL DEFAULT 5432,
                    database_name VARCHAR(255) NOT NULL,
                    username VARCHAR(255) NOT NULL,
                    password TEXT NOT NULL,
                    schemas_to_scan TEXT NOT NULL DEFAULT '',
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT NOW(),
                    CONSTRAINT uq_project_db_connections_project_id UNIQUE (project_id)
                )
                """
            )
        conn.commit()
        log.info("Ensured project_db_connections table in app database %r.", target_db)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ------end of new code------------------------------------------------------------------------------------

_app_auth_backend_prepared: bool = False
_app_auth_backend_lock = threading.Lock()


def prepare_app_auth_backend() -> None:
    """
    Create the Userdetails (or ``USER_DETAILS_DATABASE_NAME``) database and
    ``public.auth_users`` if they are missing. Idempotent: succeeds once per process
    and then no-ops. Use from FastAPI startup and from auth routes so a failed
    startup (e.g. wrong .env) can self-heal on first sign-up.
    """
    global _app_auth_backend_prepared
    if _app_auth_backend_prepared:
        return
    with _app_auth_backend_lock:
        if _app_auth_backend_prepared:
            return
        ensure_userdetails_database()
        ensure_auth_tables()
        _app_auth_backend_prepared = True


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


@contextmanager
def get_app_db_cursor(dict_cursor: bool = True):
    """
    Cursor for app-level database (USER_DETAILS_DATABASE_NAME), using AUTH_DB_* runtime credentials.
    """
    conn = _connect_auth_runtime(_app_db_name())
    try:
        factory = RealDictCursor if dict_cursor else None
        with conn.cursor(cursor_factory=factory) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
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
