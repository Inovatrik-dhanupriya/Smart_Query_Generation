"""Load `.env` and resolve repo paths (single place for all modules)."""
from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

_loaded = False


def package_root() -> Path:
    """Directory containing `main.py` (the `nl_to_sql/` folder)."""
    return Path(__file__).resolve().parent.parent


def project_root() -> Path:
    """Repository root (`Table_automation/`) — parent of `nl_to_sql/`."""
    return package_root().parent


def load_app_env() -> None:
    """Load `Table_automation/.env` once."""
    global _loaded
    if _loaded:
        return
    load_dotenv(project_root() / ".env")
    _loaded = True
