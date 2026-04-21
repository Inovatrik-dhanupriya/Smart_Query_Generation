# Any file needing DB access →
# calls get_cursor() →
# gets DB connection →
# auto commit/rollback →
# returns connection to pool

import logging
import os
import re

import psycopg2
import psycopg2.pool
from psycopg2 import sql as pg_sql
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


def _admin_credentials() -> tuple[str, str]:
    admin_user = os.getenv("DB_ADMIN_USER", "").strip()
    admin_password = os.getenv("DB_ADMIN_PASSWORD", "").strip()
    if not admin_user or not admin_password:
        raise RuntimeError("DB_ADMIN_USER and DB_ADMIN_PASSWORD must be set.")
    return admin_user, admin_password


def _auth_runtime_credentials() -> tuple[str, str]:
    """
    Runtime credentials for auth reads/writes on USER_DETAILS_DATABASE_NAME.

    Preferred:
      AUTH_DB_USER / AUTH_DB_PASSWORD (non-admin role with scoped grants)
    Fallback:
      DB_ADMIN_USER / DB_ADMIN_PASSWORD (kept for backward compatibility)
    """
    global _warned_auth_runtime_fallback
    auth_user = os.getenv("AUTH_DB_USER", "").strip()
    auth_password = os.getenv("AUTH_DB_PASSWORD", "").strip()
    if auth_user and auth_password:
        return auth_user, auth_password

    admin_user, admin_password = _admin_credentials()
    if not _warned_auth_runtime_fallback:
        log.warning(
            "AUTH_DB_USER/AUTH_DB_PASSWORD not set. "
            "Falling back to admin credentials for auth runtime operations."
        )
        _warned_auth_runtime_fallback = True
    return admin_user, admin_password


def _connect_admin(dbname: str) -> psycopg2.extensions.connection:
    admin_user, admin_password = _admin_credentials()
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=dbname,
        user=admin_user,
        password=admin_password,
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


def ensure_userdetails_database() -> bool:
    """
    Ensure the app-level database exists (default: USER_DETAILS_DATABASE_NAME=Userdetails).

    Uses DB_ADMIN_USER / DB_ADMIN_PASSWORD and connects to the maintenance database
    ``postgres`` only to check/create the target database. This does not modify DB_NAME.

    Returns:
        True  -> database was created
        False -> database already existed
    """
    target_db = _app_db_name()
    admin_user, _ = _admin_credentials()

    conn = _connect_admin("postgres")
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
            log.info("Created app database %r using admin role %r.", target_db, admin_user)
            return True
    finally:
        conn.close()


def ensure_auth_tables() -> None:
    """
    Create minimal auth table(s) in USER_DETAILS_DATABASE_NAME if missing.

    Current scope: fields required for Sign Up / Sign In.
    """
    target_db = _app_db_name()
    conn = _connect_admin(target_db)
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
        conn.commit()
        log.info("Ensured auth tables in app database %r.", target_db)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


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