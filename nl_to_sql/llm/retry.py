"""Retry wrapper for Gemini `generate_content` on HTTP 429."""
from __future__ import annotations

import logging
import os
import random
import time

from google.genai import errors as genai_errors

from llm.client import get_gemini_client

_log = logging.getLogger(__name__)


def is_gemini_quota_error(exc: BaseException) -> bool:
    """True for HTTP 429 / RESOURCE_EXHAUSTED from the Generative Language API."""
    if isinstance(exc, genai_errors.ClientError):
        code = getattr(exc, "status_code", None)
        if code == 429:
            return True
    text = str(exc).upper()
    return "429" in text or "RESOURCE_EXHAUSTED" in text


def generate_content_with_retry(**kwargs):
    """
    Wraps models.generate_content with exponential backoff on rate limits.
    Env: GEMINI_QUOTA_RETRIES (default 8), GEMINI_RETRY_DELAY_SECONDS (default 1.5).
    """
    max_retries = max(1, int(os.getenv("GEMINI_QUOTA_RETRIES", "8")))
    base_delay = float(os.getenv("GEMINI_RETRY_DELAY_SECONDS", "1.5"))
    last_exc: BaseException | None = None
    client = get_gemini_client()
    for attempt in range(max_retries):
        try:
            return client.models.generate_content(**kwargs)
        except BaseException as e:
            last_exc = e
            if not is_gemini_quota_error(e) or attempt >= max_retries - 1:
                raise
            wait = min(base_delay * (2**attempt) + random.uniform(0, 1.0), 90.0)
            _log.warning(
                "Gemini quota/rate limit — attempt %s/%s, sleeping %.1fs then retrying",
                attempt + 1,
                max_retries,
                wait,
            )
            time.sleep(wait)
    assert last_exc is not None
    raise last_exc
