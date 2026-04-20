"""Single Google GenAI client for text + embeddings."""
from __future__ import annotations

import os

from google import genai

from utils.env import load_app_env

_client: genai.Client | None = None


def get_gemini_client() -> genai.Client:
    global _client
    load_app_env()
    if _client is None:
        key = os.getenv("GEMINI_API_KEY")
        if not key:
            raise RuntimeError("GEMINI_API_KEY is not set.")
        _client = genai.Client(api_key=key)
    return _client


def get_text_model() -> str:
    load_app_env()
    return os.getenv("GEMINI_TEXT_MODEL", "gemini-2.5-pro")


def get_embed_model() -> str:
    return os.getenv("GEMINI_EMBED_MODEL", "models/gemini-embedding-001")
