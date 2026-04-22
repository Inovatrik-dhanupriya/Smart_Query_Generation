"""Module 2 — Ask questions, generate SQL, execute, show results (separate from Configuration)."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from ui import nl_workbench

st.set_page_config(page_title="Module 2 — Chat", page_icon="💬", layout="wide")
nl_workbench.set_workbench_page("chat")
nl_workbench.run()
