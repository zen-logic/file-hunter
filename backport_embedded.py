#!/usr/bin/env python3
"""One-time backport: set embedded=1 for files already in ChromaDB."""

import sqlite3
import chromadb

cat = sqlite3.connect("data/file_hunter.db")
cat.execute("PRAGMA journal_mode=WAL")
client = chromadb.PersistentClient(path="data/similarity")

file_ids = set()

try:
    img = client.get_collection("image_embeddings")
    n = img.count()
    print(f"Image embeddings: {n:,}")
    offset = 0
    while offset < n:
        result = img.get(limit=5000, offset=offset, include=[])
        if not result["ids"]:
            break
        file_ids.update(int(x) for x in result["ids"])
        offset += len(result["ids"])
except Exception as e:
    print(f"Image collection: {e}")

try:
    doc = client.get_collection("document_embeddings")
    n = doc.count()
    print(f"Document chunks: {n:,}")
    offset = 0
    while offset < n:
        result = doc.get(limit=5000, offset=offset, include=["metadatas"])
        if not result["ids"]:
            break
        for m in result["metadatas"]:
            fid = m.get("file_id")
            if fid is not None:
                file_ids.add(int(fid))
        offset += len(result["ids"])
except Exception as e:
    print(f"Document collection: {e}")

print(f"Files to flag: {len(file_ids):,}")

updated = 0
ids = list(file_ids)
for i in range(0, len(ids), 1000):
    batch = ids[i : i + 1000]
    ph = ",".join("?" for _ in batch)
    cur = cat.execute(f"UPDATE files SET embedded = 1 WHERE id IN ({ph}) AND embedded = 0", batch)
    updated += cur.rowcount
cat.commit()
cat.close()

print(f"Updated: {updated:,}")
