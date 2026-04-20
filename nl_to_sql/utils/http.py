"""HTTP helpers shared by the Streamlit UI (safe JSON parsing)."""
from __future__ import annotations

import requests


def safe_response_payload(resp: requests.Response):
    """
    Parse JSON from an HTTP response without raising JSONDecodeError.
    Returns (payload_dict_or_list | None, error_message | None).
    """
    text = (resp.text or "").strip()
    if not text:
        return None, f"Empty response body (HTTP {resp.status_code})."
    try:
        return resp.json(), None
    except Exception:
        return None, (
            f"Non-JSON response (HTTP {resp.status_code}). "
            f"First 600 chars:\n{text[:600]}"
        )
