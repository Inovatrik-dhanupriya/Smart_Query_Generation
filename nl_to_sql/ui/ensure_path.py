"""Streamlit import bootstrap: add ``nl_to_sql/`` to :data:`sys.path` once.

`utils` and top-level packages live under ``nl_to_sql/``; when Streamlit runs
``ui/pages/…``, that folder may be the only entry on ``sys.path``. Scripts
in ``ui/pages/`` add ``ui/`` to the path, then import and call ``install()``.
Scripts directly under ``ui/`` add their own directory first.
"""

from __future__ import annotations

import sys
from pathlib import Path


def install() -> Path:
    """Insert ``nl_to_sql/`` (the parent of this ``ui/`` tree) on ``sys.path`` if needed."""
    root = Path(__file__).resolve().parent.parent
    s = str(root)
    if s not in sys.path:
        sys.path.insert(0, s)
    return root
