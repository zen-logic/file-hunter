#!/usr/bin/env python3
"""One-time backfill: copy document chunk text from ChromaDB into text.db.

Documents embedded before text.db existed have their chunk text only in
ChromaDB. This copies it across so full-text search covers them. Safe to
re-run: existing chunks are updated in place.

Stop File Hunter first, then from the File Hunter folder:

    venv/bin/python scripts/backfill_text.py
"""

import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
sys.path.insert(0, ROOT)

import chromadb  # noqa: E402

from file_hunter.services.similarity import SIMILARITY_DB_DIR  # noqa: E402
from file_hunter.text_db import SCHEMA, text_db_path  # noqa: E402

text = sqlite3.connect(text_db_path())
text.execute("PRAGMA journal_mode=WAL")
text.executescript(SCHEMA)

client = chromadb.PersistentClient(path=SIMILARITY_DB_DIR)
try:
    doc = client.get_collection("document_embeddings")
except Exception as e:
    sys.exit(f"No document collection: {e}")

n = doc.count()
print(f"Document chunks in ChromaDB: {n:,}")

copied = 0
files = set()
offset = 0
while offset < n:
    result = doc.get(limit=5000, offset=offset, include=["documents", "metadatas"])
    if not result["ids"]:
        break
    rows = []
    for chunk_id, document, meta in zip(result["ids"], result["documents"], result["metadatas"]):
        if not document or meta.get("file_id") is None:
            continue
        # chunk ids are "{file_id}_chunk{i}"; chunk_index is also in metadata
        index = meta.get("chunk_index", int(chunk_id.rsplit("_chunk", 1)[-1]))
        rows.append((int(meta["file_id"]), int(index), meta.get("meta", "") or "", document))
        files.add(int(meta["file_id"]))
    text.executemany(
        """INSERT INTO chunks (file_id, chunk_index, headings, text) VALUES (?, ?, ?, ?)
           ON CONFLICT (file_id, chunk_index) DO UPDATE SET headings = excluded.headings, text = excluded.text""",
        rows,
    )
    text.commit()
    copied += len(rows)
    offset += len(result["ids"])
    print(f"  {offset:,}/{n:,}")

text.close()
print(f"Copied {copied:,} chunks from {len(files):,} documents")
