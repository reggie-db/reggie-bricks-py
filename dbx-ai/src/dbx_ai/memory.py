from __future__ import annotations

import base64
import functools
import hashlib
import os
import pickle
from pathlib import Path
from typing import Any

import chromadb
from lfp_logging import logs
from sentence_transformers import SentenceTransformer

LOG = logs.logger()

_DEFAULT_DB_PATH = Path.home() / ".dbx-ai/memory_v2"
_KV_COLLECTION_NAME = "kv_store"
_SEMANTIC_COLLECTION_NAME = "semantic_store"


@functools.cache
def _db_path() -> Path:
    """
    Local persistent storage directory for memory.

    Environment override:
    - DBX_AI_MEMORY_PATH: alternate directory path (useful for tests)
    """
    override = os.environ.get("DBX_AI_MEMORY_PATH")
    return Path(override).expanduser() if override else _DEFAULT_DB_PATH


@functools.cache
def _client() -> chromadb.ClientAPI:
    path = _db_path()
    path.mkdir(parents=True, exist_ok=True)
    return chromadb.PersistentClient(path=str(path))


@functools.cache
def _semantic_collection():
    return _client().get_or_create_collection(_SEMANTIC_COLLECTION_NAME)


@functools.cache
def _kv_collection():
    return _client().get_or_create_collection(_KV_COLLECTION_NAME)


# ---------- shared encoding ----------


def _encode_doc_id(type: str, key: Any) -> str:
    key_bytes = pickle.dumps(key, protocol=pickle.HIGHEST_PROTOCOL)
    key_encoded = f"{type}_kv_{hashlib.sha256(key_bytes).hexdigest()}"
    return key_encoded


def _encode_value(value: Any) -> str:
    raw = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
    return base64.b64encode(raw).decode("ascii")


def _decode_value(encoded: str) -> Any:
    raw = base64.b64decode(encoded.encode("ascii"))
    return pickle.loads(raw)


# ---------- chroma helpers ----------


def _extract_documents(collection: chromadb.Collection, doc_id: Any) -> list[Any]:
    """
    Normalize all chroma get() return shapes.

    Possible shapes:
    {'ids': [], ...}
    {'ids': [[]], 'documents': [[]]}
    {'ids': [['id']], 'documents': [['data']]}
    """
    if existing := _kv_collection().get(ids=[doc_id]):
        if documents := existing.get("documents", None):
            return documents
    return []


# ---------- public API ----------


def kv_exists(type: str, key: Any) -> bool:
    doc_id = _encode_doc_id(type, key)
    documents = _extract_documents(_kv_collection(), doc_id)
    return len(documents) > 0


def kv_read(type: str, key: Any) -> Any | None:
    doc_id = _encode_doc_id(type, key)
    documents = _extract_documents(_kv_collection(), doc_id)
    if documents:
        return _decode_value(documents[0])
    return None


def kv_write(type: str, key: Any, value: Any) -> None:
    doc_id = _encode_doc_id(type, key)
    encoded = _encode_value(value)
    _kv_collection().add(ids=[doc_id], documents=[encoded], metadatas=[{"type": type}])


@functools.cache
def _model():
    return SentenceTransformer("all-MiniLM-L6-v2")


def recall(type: str, query: str, k: int = 5) -> list[str]:
    if not query.strip():
        return []
    LOG.debug(f"Recalling %s - dquery:%s", type, query)
    emb = _model().encode(query).tolist()

    results = _semantic_collection().query(
        query_embeddings=[emb], n_results=k, where={"type": type}
    )

    docs = results.get("documents", [[]])[0]
    return docs or []


def write(type: str, query: str, answer: str) -> None:
    doc_id = "_".join((type, hashlib.sha256(query.lower().encode("utf-8")).hexdigest()))

    # already indexed
    existing = _semantic_collection().get(ids=[doc_id])
    if existing and existing["ids"]:
        return
    LOG.debug(
        f"Indexing %s - doc_id:%s query:%s, answer:%s",
        type,
        doc_id,
        query,
        answer[:100] + "...",
    )
    emb = _model().encode(answer).tolist()

    _semantic_collection().add(
        ids=[doc_id],
        documents=[answer],
        embeddings=[emb],
        metadatas=[{"type": type, "source": "agent"}],
    )
