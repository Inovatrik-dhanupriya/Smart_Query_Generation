"""Project chat: natural language to SQL, run queries, see results."""

from __future__ import annotations

import sys
from pathlib import Path

_U = Path(__file__).resolve().parent.parent
if str(_U) not in sys.path:
    sys.path.insert(0, str(_U))
from ensure_path import install

install()

import streamlit as st

from ui import nl_workbench

st.set_page_config(page_title="Chat", page_icon="💬", layout="wide")
nl_workbench.set_workbench_page("chat")
nl_workbench.run()
