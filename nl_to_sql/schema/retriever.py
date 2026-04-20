"""
schema/retriever.py
Finds the most relevant tables for a user query using:
  - Google Gemini embeddings (``GEMINI_EMBED_MODEL``) → dense vectors
  - FAISS (IndexFlatIP) → fast inner-product similarity search

FAISS disk persistence:
  - Index and table list are saved to CACHE_DIR on first build.
  - On restart, they are loaded from disk — no re-embedding needed.
  - Cache is invalidated when the set of table names changes.
"""


# Smart table selection using embeddings + FAISS.
# Instead of sending ALL tables to LLM: It retrieves only relevant tables(RAG)
# Embed Table Descriptions -> FAISS Index -> Retrieve relevant tables -> Send to LLM
# Build FAISS Index -> Stores vectors for similarity search.
# Query Embedding -> Converts user prompt into vector.
# Similarity Search -> Finds closest vectors in FAISS Index.
# Also adds related FK tables automatically.

from __future__ import annotations

import hashlib
import json
import os
import numpy as np
import faiss
from pathlib import Path

from google.genai import types

from llm.client import get_embed_model, get_gemini_client
from utils.env import load_app_env, project_root

load_app_env()

# Directory where the FAISS index + table list are persisted between restarts
_CACHE_DIR = Path(os.getenv("FAISS_CACHE_DIR", project_root() / ".faiss_cache"))
_INDEX_FILE  = _CACHE_DIR / "schema.index"
_TABLES_FILE = _CACHE_DIR / "tables.json"   # {tables: [...], hash: "..."}


def _table_set_hash(table_names: list[str]) -> str:
    """Stable hash of the sorted table name list — used to detect schema changes."""
    key = ",".join(sorted(table_names))
    return hashlib.md5(key.encode()).hexdigest()


def _save_cache(index: faiss.IndexFlatIP, tables: list[str]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    faiss.write_index(index, str(_INDEX_FILE))
    meta = {"tables": tables, "hash": _table_set_hash(tables)}
    _TABLES_FILE.write_text(json.dumps(meta), encoding="utf-8")
    print(f"[SchemaRetriever] FAISS index saved → {_INDEX_FILE}")


def _load_cache(expected_tables: list[str]) -> tuple[faiss.IndexFlatIP, list[str]] | None:
    """
    Returns (index, tables) if a valid cache exists for the current table set.
    Returns None if cache is missing or the table set has changed.
    """
    if not _INDEX_FILE.exists() or not _TABLES_FILE.exists():
        return None
    try:
        meta = json.loads(_TABLES_FILE.read_text(encoding="utf-8"))
        if meta.get("hash") != _table_set_hash(expected_tables):
            print("[SchemaRetriever] Table set changed — rebuilding FAISS index.")
            return None
        index = faiss.read_index(str(_INDEX_FILE))
        print(f"[SchemaRetriever] FAISS index loaded from disk — "
              f"{index.ntotal} vector(s), skipping re-embedding.")
        return index, meta["tables"]
    except Exception as e:
        print(f"[SchemaRetriever] Cache load failed ({e}) — rebuilding.")
        return None


# ── Embedding helpers ─────────────────────────────────────────────────────────

_EMBED_BATCH_SIZE = 100   # Gemini API hard limit: max 100 texts per batch call


def _embed_documents(texts: list[str]) -> np.ndarray:
    """
    Embed a batch of document texts → float32 array (N, dim).
    Automatically splits into chunks of 100 to stay within the Gemini API limit.
    """
    if not texts:
        return np.empty((0, 3072), dtype=np.float32)

    all_vecs: list[np.ndarray] = []

    for i in range(0, len(texts), _EMBED_BATCH_SIZE):
        chunk = texts[i : i + _EMBED_BATCH_SIZE]
        print(f"[SchemaRetriever] Embedding batch {i // _EMBED_BATCH_SIZE + 1}"
              f"/{-(-len(texts) // _EMBED_BATCH_SIZE)}"
              f" ({len(chunk)} texts) …")
        resp = get_gemini_client().models.embed_content(
            model=get_embed_model(),
            contents=chunk,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        vecs = np.array([e.values for e in resp.embeddings], dtype=np.float32)
        all_vecs.append(vecs)

    combined = np.vstack(all_vecs)
    faiss.normalize_L2(combined)      # unit-normalise so IP == cosine
    return combined


def _embed_query(text: str) -> np.ndarray:
    """Embed a single query text → float32 array (1, dim)."""
    resp = get_gemini_client().models.embed_content(
        model=get_embed_model(),
        contents=[text],
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
    )
    vec = np.array([resp.embeddings[0].values], dtype=np.float32)
    faiss.normalize_L2(vec)
    return vec


# ── Main class ────────────────────────────────────────────────────────────────

class SchemaRetriever:
    def __init__(self, table_descriptions: dict[str, str]):
        """
        Embeds every table description once at startup and loads them
        into a FAISS IndexFlatIP (exact inner-product / cosine search).

        table_descriptions: {table_name: plain_text_description}
        Works for ANY number of tables — fully dynamic, no hardcoded names.
        """
        self.tables: list[str] = []
        self._index: faiss.IndexFlatIP | None = None
        self._build(table_descriptions)

    def _build(self, table_descriptions: dict[str, str], force: bool = False) -> None:
        """
        Internal: embed all tables and build FAISS index.
        Loads from disk cache if available and the table set hasn't changed.
        Set force=True to skip cache and always re-embed (used after schema sync).
        """
        table_names = list(table_descriptions.keys())
        texts       = list(table_descriptions.values())

        # Try loading from disk first (skips expensive Gemini embedding calls)
        if not force:
            cached = _load_cache(table_names)
            if cached is not None:
                self._index, self.tables = cached
                return

        print(f"[SchemaRetriever] Embedding {len(table_names)} table(s) with Gemini …")
        self.tables = table_names

        if not table_names:
            print("[SchemaRetriever] WARNING: No tables found in the database. "
                  "Check that DB_USER has SELECT permission on information_schema.tables. "
                  "FAISS index will be empty — call /reload-schema after fixing permissions.")
            self._index = None
            return

        doc_vecs     = _embed_documents(texts)          # (N, 3072) float32

        dim          = doc_vecs.shape[1]
        self._index  = faiss.IndexFlatIP(dim)
        self._index.add(doc_vecs)

        print(f"[SchemaRetriever] FAISS index built — {self._index.ntotal} vector(s), dim={dim}")
        print(f"[SchemaRetriever] Tables: {self.tables}")

        # Persist to disk for next restart
        _save_cache(self._index, self.tables)

    def rebuild(self, table_descriptions: dict[str, str]) -> None:
        """
        Hot-reload: re-embed ALL tables and replace the FAISS index in-place.
        Always bypasses the disk cache and saves a fresh index afterward.
        Call this after adding new tables to the DB without restarting the server.
        """
        print("[SchemaRetriever] Rebuilding FAISS index with updated schema …")
        self._build(table_descriptions, force=True)
        print("[SchemaRetriever] Rebuild complete.")

    # ── Public API ────────────────────────────────────────────────────────────

    def retrieve( # Finds most relevant tables based on user prompt.
        self,
        query: str,
        top_k: int = 3,
        threshold: float = 0.3,
    ) -> list[str]:
        """
        Returns table names ranked by cosine similarity to the query.
        Always returns at least 1 table even if all scores are below threshold.
        Returns empty list if FAISS index is not available (0 tables in DB).
        """
        if self._index is None or self._index.ntotal == 0:
            return []

        q_vec = _embed_query(query)                         # (1, dim)
        k     = min(top_k, self._index.ntotal)
        scores, indices = self._index.search(q_vec, k)     # (1, k) each

        ranked   = list(zip(indices[0], scores[0]))         # [(idx, score), ...]
        selected = [self.tables[i] for i, s in ranked if s >= threshold]

        # fallback: always include at least the top result
        if not selected and ranked:
            selected = [self.tables[ranked[0][0]]]

        return selected

    def retrieve_with_fk_expansion(
        self,
        query: str,
        schema: dict,
        top_k: int = 3,
    ) -> list[str]:
        """
        Retrieves relevant tables AND automatically expands to FK-linked tables.
        """
        base_tables = set(self.retrieve(query, top_k=top_k))

        for table in list(base_tables):
            fks = schema["tables"].get(table, {}).get("foreign_keys", [])
            for fk in fks:
                base_tables.add(fk["foreign_table"])

        return list(base_tables)
