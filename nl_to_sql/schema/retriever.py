"""
schema/retriever.py
Table selection via Gemini embeddings + FAISS (IndexFlatIP).
``cache_dir`` can be scoped per client session to avoid collisions.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import faiss
import numpy as np
from google.genai import types

from llm.client import get_embed_model, get_gemini_client
from utils.constants import (
    FAISS_DEFAULT_TOP_K,
    FAISS_EMBED_BATCH_SIZE,
    FAISS_MAX_FK_CLOSURE,
    FAISS_RETRIEVE_SCORE_THRESHOLD,
    FK_EXPAND_MAX_ITERATIONS,
    FK_EXPAND_MIN_MAX_TOTAL,
    GEMINI_EMBEDDING_DIMENSION,
)
from utils.env import load_app_env, project_root

load_app_env()


def _default_cache_root() -> Path:
    return Path(os.getenv("FAISS_CACHE_DIR", project_root() / ".faiss_cache"))


def fk_expand_seed_tables(
    seed_tables: list[str],
    schema: dict,
    *,
    max_tables: int = FAISS_MAX_FK_CLOSURE,
) -> list[str]:
    """
    Given an initial set of table keys, add every table reachable by one foreign-key
    hop in **either** direction (parent or child). Used after token-based expansion
    so ``departments`` + ``users`` both participate in SQL generation.
    """
    base_tables = set(seed_tables)
    all_meta = schema.get("tables", {})
    max_total = min(max_tables, max(FK_EXPAND_MIN_MAX_TOTAL, len(all_meta)))

    for _ in range(FK_EXPAND_MAX_ITERATIONS):
        if len(base_tables) >= max_total:
            break
        before = len(base_tables)
        for table in list(base_tables):
            for fk in all_meta.get(table, {}).get("foreign_keys") or []:
                ft = fk.get("foreign_table")
                if ft:
                    base_tables.add(ft)
        for tkey, meta in all_meta.items():
            if tkey in base_tables:
                continue
            for fk in meta.get("foreign_keys") or []:
                if fk.get("foreign_table") in base_tables:
                    base_tables.add(tkey)
                    break
        if len(base_tables) == before:
            break

    return list(base_tables)[:max_total]


def _embed_documents(texts: list[str]) -> np.ndarray:
    if not texts:
        return np.empty((0, GEMINI_EMBEDDING_DIMENSION), dtype=np.float32)

    all_vecs: list[np.ndarray] = []

    for i in range(0, len(texts), FAISS_EMBED_BATCH_SIZE):
        chunk = texts[i : i + FAISS_EMBED_BATCH_SIZE]
        print(
            f"[SchemaRetriever] Embedding batch {i // FAISS_EMBED_BATCH_SIZE + 1}"
            f"/{-(-len(texts) // FAISS_EMBED_BATCH_SIZE)} ({len(chunk)} texts) …"
        )
        resp = get_gemini_client().models.embed_content(
            model=get_embed_model(),
            contents=chunk,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        vecs = np.array([e.values for e in resp.embeddings], dtype=np.float32)
        all_vecs.append(vecs)

    combined = np.vstack(all_vecs)
    faiss.normalize_L2(combined)
    return combined


def _embed_query(text: str) -> np.ndarray:
    resp = get_gemini_client().models.embed_content(
        model=get_embed_model(),
        contents=[text],
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
    )
    vec = np.array([resp.embeddings[0].values], dtype=np.float32)
    faiss.normalize_L2(vec)
    return vec


def _table_set_hash(table_names: list[str]) -> str:
    key = ",".join(sorted(table_names))
    return hashlib.md5(key.encode()).hexdigest()


class SchemaRetriever:
    def __init__(
        self,
        table_descriptions: dict[str, str],
        *,
        cache_dir: Path | None = None,
    ):
        self._cache_dir = cache_dir if cache_dir is not None else _default_cache_root()
        self._index_file = self._cache_dir / "schema.index"
        self._tables_file = self._cache_dir / "tables.json"
        self.tables: list[str] = []
        self._index: faiss.IndexFlatIP | None = None
        self._build(table_descriptions)

    def _save_disk_cache(self, index: faiss.IndexFlatIP, tables: list[str]) -> None:
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        faiss.write_index(index, str(self._index_file))
        meta = {"tables": tables, "hash": _table_set_hash(tables)}
        self._tables_file.write_text(json.dumps(meta), encoding="utf-8")
        print(f"[SchemaRetriever] FAISS index saved → {self._index_file}")

    def _load_disk_cache(
        self, expected_tables: list[str]
    ) -> tuple[faiss.IndexFlatIP, list[str]] | None:
        if not self._index_file.exists() or not self._tables_file.exists():
            return None
        try:
            meta = json.loads(self._tables_file.read_text(encoding="utf-8"))
            if meta.get("hash") != _table_set_hash(expected_tables):
                print("[SchemaRetriever] Table set changed — rebuilding FAISS index.")
                return None
            index = faiss.read_index(str(self._index_file))
            print(
                f"[SchemaRetriever] FAISS index loaded from disk — "
                f"{index.ntotal} vector(s), skipping re-embedding."
            )
            return index, meta["tables"]
        except Exception as e:
            print(f"[SchemaRetriever] Cache load failed ({e}) — rebuilding.")
            return None

    def _build(self, table_descriptions: dict[str, str], force: bool = False) -> None:
        table_names = list(table_descriptions.keys())
        texts = list(table_descriptions.values())

        if not force:
            cached = self._load_disk_cache(table_names)
            if cached is not None:
                self._index, self.tables = cached
                return

        print(f"[SchemaRetriever] Embedding {len(table_names)} table(s) with Gemini …")
        self.tables = table_names

        if not table_names:
            print("[SchemaRetriever] No tables in scope — FAISS index empty.")
            self._index = None
            return

        doc_vecs = _embed_documents(texts)

        dim = doc_vecs.shape[1]
        self._index = faiss.IndexFlatIP(dim)
        self._index.add(doc_vecs)

        print(f"[SchemaRetriever] FAISS index built — {self._index.ntotal} vector(s), dim={dim}")
        print(f"[SchemaRetriever] Tables: {self.tables}")

        self._save_disk_cache(self._index, self.tables)

    def rebuild(self, table_descriptions: dict[str, str]) -> None:
        print("[SchemaRetriever] Rebuilding FAISS index with updated schema …")
        self._build(table_descriptions, force=True)
        print("[SchemaRetriever] Rebuild complete.")

    def retrieve(
        self,
        query: str,
        top_k: int = FAISS_DEFAULT_TOP_K,
        threshold: float = FAISS_RETRIEVE_SCORE_THRESHOLD,
    ) -> list[str]:
        if self._index is None or self._index.ntotal == 0:
            return []

        q_vec = _embed_query(query)
        k = min(top_k, self._index.ntotal)
        scores, indices = self._index.search(q_vec, k)

        ranked = list(zip(indices[0], scores[0]))
        selected = [self.tables[i] for i, s in ranked if s >= threshold]

        if not selected and ranked:
            selected = [self.tables[ranked[0][0]]]

        return selected

    def retrieve_with_fk_expansion(
        self,
        query: str,
        schema: dict,
        top_k: int = FAISS_DEFAULT_TOP_K,
    ) -> list[str]:
        """
        Seed from FAISS, then expand along foreign keys in **both** directions:
        parents (FK targets) and children (tables that reference a selected table).
        A few iterations cover multi-hop star schemas without pulling the whole DB
        (capped by ``FAISS_MAX_FK_CLOSURE``).
        """
        seed = self.retrieve(query, top_k=top_k)
        return fk_expand_seed_tables(seed, schema, max_tables=FAISS_MAX_FK_CLOSURE)
