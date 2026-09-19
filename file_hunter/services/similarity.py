"""Similarity search support — optional chromadb dependency."""

import logging
import os
import re
import subprocess
import sys

import httpx

logger = logging.getLogger(__name__)

_chromadb_available: bool | None = None
_chroma_client = None

SIMILARITY_DB_DIR = "data/similarity"


def is_chromadb_available() -> bool:
    """Check whether chromadb is importable."""
    global _chromadb_available
    if _chromadb_available is None:
        try:
            import chromadb  # noqa: F401
            _chromadb_available = True
        except ImportError:
            _chromadb_available = False
    return _chromadb_available


def ensure_chromadb() -> bool:
    """Install chromadb if missing. Returns True if available after check."""
    if is_chromadb_available():
        return True
    logger.info("Installing chromadb...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "chromadb"],
            stdout=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError as e:
        logger.error("Failed to install chromadb: %s", e)
        return False
    global _chromadb_available
    _chromadb_available = True
    logger.info("chromadb installed")
    return True


def _get_client():
    global _chroma_client
    if not ensure_chromadb():
        raise RuntimeError("chromadb is not available and could not be installed")
    import chromadb
    os.makedirs(SIMILARITY_DB_DIR, exist_ok=True)
    if _chroma_client is None:
        _chroma_client = chromadb.PersistentClient(path=SIMILARITY_DB_DIR)
    return _chroma_client


def get_collection():
    """Get or create the image embeddings collection."""
    return _get_client().get_or_create_collection(
        name="image_embeddings", metadata={"hnsw:space": "cosine"}
    )


def get_document_collection():
    """Get or create the document chunk embeddings collection."""
    return _get_client().get_or_create_collection(
        name="document_embeddings", metadata={"hnsw:space": "cosine"}
    )


async def fetch_embedding(embed_url: str, image_bytes: bytes, path: str = "") -> list[float] | None:
    """Send image bytes to the embedding service, return the vector."""
    url = f"{embed_url.rstrip('/')}/api/embed/image"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                url,
                content=image_bytes,
                headers={"Content-Type": "image/jpeg"},
            )
        if resp.status_code != 200:
            detail = resp.text[:200] if resp.text else ""
            logger.warning("Embedding service returned %d for %s: %s", resp.status_code, path, detail)
            return None
        data = resp.json()
        return data.get("embedding")
    except Exception as e:
        logger.warning("Embedding service error for %s: %s", path, e)
        return None


def parse_composite_query(query):
    """Parse query with vector arithmetic syntax.

    Supports:
        (red socks) - (yellow skirt)     subtract embeddings
        (red socks) + (blue hat)         add embeddings
        2*(red socks) + (blue hat)       weighted addition
        red + socks                      add individual term embeddings

    Operators require surrounding spaces to distinguish from hyphenated
    words (red-eye is a phrase, red - eye is subtraction).

    Returns None for plain queries (no operators found).
    Returns list of (sign, weight, phrase) tuples for composite queries.
    """
    query = query.strip()
    parts = re.split(r"\s([+-])\s", query)
    if len(parts) == 1:
        return None

    terms = []
    sign = 1.0

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part == "+":
            sign = 1.0
            continue
        if part == "-":
            sign = -1.0
            continue

        weight = 1.0
        weight_match = re.match(r"^(\d+(?:\.\d+)?)\s*\*\s*", part)
        if weight_match:
            weight = float(weight_match.group(1))
            part = part[weight_match.end():]

        if part.startswith("(") and part.endswith(")"):
            part = part[1:-1].strip()

        if part:
            terms.append((sign, weight, part))

        sign = 1.0

    return terms if len(terms) > 1 else None


async def embed_text_query(embed_url: str, query: str) -> list[float] | None:
    """Embed a text query, handling composite syntax with vector arithmetic."""
    composite = parse_composite_query(query)

    if not composite:
        # Plain query — single embedding
        url = f"{embed_url.rstrip('/')}/api/embed/text"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json={"text": query})
            if resp.status_code == 200:
                return resp.json().get("embedding")
        except Exception as e:
            logger.warning("Text embedding failed: %s", e)
        return None

    # Composite query — embed each term, combine with arithmetic
    import numpy as np
    url = f"{embed_url.rstrip('/')}/api/embed/text"
    combined = None
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            for sign, weight, phrase in composite:
                resp = await client.post(url, json={"text": phrase})
                if resp.status_code != 200:
                    logger.warning("Embedding failed for phrase: %s", phrase)
                    continue
                emb = np.array(resp.json().get("embedding"), dtype=np.float32)
                weighted = emb * sign * weight
                combined = weighted if combined is None else combined + weighted
    except Exception as e:
        logger.warning("Composite text embedding failed: %s", e)
        return None

    if combined is None:
        return None

    # Re-normalise
    norm = np.linalg.norm(combined)
    if norm > 0:
        combined = combined / norm
    return combined.tolist()


async def fetch_document_embeddings(
    embed_url: str, file_bytes: bytes, filename: str,
) -> list[dict] | None:
    """Send document bytes to the embedding service, return chunks with embeddings."""
    url = f"{embed_url.rstrip('/')}/api/embed/document"
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                url, content=file_bytes,
                headers={"X-Filename": filename},
            )
        if resp.status_code != 200:
            detail = resp.text[:200] if resp.text else ""
            logger.warning("Document embedding returned %d: %s", resp.status_code, detail)
            return None
        data = resp.json()
        return data.get("chunks")
    except Exception as e:
        logger.warning("Document embedding error: %s", e)
        return None


async def run_embed_file(op_id: int, agent_id: int | None, params: dict):
    """Embed a single document/text file — runs as a queued operation."""
    from file_hunter.services.content_proxy import fetch_agent_bytes
    from file_hunter.ws.scan import broadcast

    file_id = params["file_id"]
    filename = params["filename"]
    full_path = params["path"]
    location_id = params["location_id"]
    embed_url = params["embed_url"]

    await broadcast({
        "type": "embed_started",
        "fileId": file_id,
        "filename": filename,
    })

    file_bytes = await fetch_agent_bytes(full_path, location_id)
    if file_bytes is None:
        await broadcast({
            "type": "embed_completed",
            "fileId": file_id,
            "filename": filename,
            "error": "File not available (agent offline)",
        })
        return

    chunks = await fetch_document_embeddings(embed_url, file_bytes, filename)
    if chunks is None or len(chunks) == 0:
        await broadcast({
            "type": "embed_completed",
            "fileId": file_id,
            "filename": filename,
            "error": "No content could be extracted",
        })
        return

    collection = get_document_collection()
    chunk_ids = [f"{file_id}_chunk{i}" for i in range(len(chunks))]
    embeddings = [c["embedding"] for c in chunks]
    documents = [c["text"] for c in chunks]
    metadatas = [
        {
            "file_id": file_id,
            "location_id": location_id,
            "chunk_index": i,
            "meta": c.get("meta", ""),
        }
        for i, c in enumerate(chunks)
    ]
    collection.upsert(
        ids=chunk_ids,
        embeddings=embeddings,
        documents=documents,
        metadatas=metadatas,
    )

    logger.info("Embedded %s: %d chunks", filename, len(chunks))
    await broadcast({
        "type": "embed_completed",
        "fileId": file_id,
        "filename": filename,
        "chunks": len(chunks),
    })


async def run_similarity_scan(op_id: int, agent_id: int | None, params: dict):
    """Walk catalogued images and documents and index their embeddings."""
    from file_hunter.db import read_db
    from file_hunter.services.content_proxy import fetch_agent_bytes
    from file_hunter.ws.scan import broadcast

    location_id = params["location_id"]
    location_name = params["location_name"]
    root_path = params["root_path"]
    scan_path = params.get("path", root_path)
    recursive = params.get("recursive", True)
    embed_url = params["embed_url"]

    await broadcast({
        "type": "scan_started",
        "locationId": location_id,
        "location": f"Similarity: {location_name}",
    })

    _EMBEDDABLE_TYPES = ("image", "document", "text")
    _EMBEDDABLE_IMAGE_SUBTYPES = {"jpg", "png", "gif", "bmp", "webp", "tiff"}

    # Find all embeddable files in the catalogue for this scope
    type_placeholders = ",".join("?" for _ in _EMBEDDABLE_TYPES)
    type_params = list(_EMBEDDABLE_TYPES)

    async with read_db() as db:
        if recursive:
            if scan_path == root_path:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low FROM files "
                    f"WHERE location_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0",
                    [location_id] + type_params,
                )
            else:
                prefix = scan_path.rstrip("/") + "/"
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low FROM files "
                    f"WHERE location_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0 "
                    f"AND (full_path = ? OR full_path LIKE ?)",
                    [location_id] + type_params + [scan_path, prefix + "%"],
                )
        else:
            folder_id = params.get("folder_id")
            if folder_id:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low FROM files "
                    f"WHERE folder_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0",
                    [folder_id] + type_params,
                )
            else:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low FROM files "
                    f"WHERE location_id = ? AND folder_id IS NULL "
                    f"AND file_type_high IN ({type_placeholders}) AND stale = 0",
                    [location_id] + type_params,
                )

    image_rows = [r for r in rows if r["file_type_high"] == "image"
                  and (r.get("file_type_low") or "") in _EMBEDDABLE_IMAGE_SUBTYPES]
    doc_rows = [r for r in rows if r["file_type_high"] in ("document", "text")]
    total = len(rows)
    logger.info(
        "Similarity scan: %d files (%d images, %d documents) in %s",
        total, len(image_rows), len(doc_rows), location_name,
    )

    if total == 0:
        await broadcast({
            "type": "scan_completed",
            "locationId": location_id,
            "location": f"Similarity: {location_name}",
            "filesFound": 0,
        })
        return

    img_collection = get_collection()
    doc_collection = get_document_collection()

    # Check which files are already indexed
    def _get_existing(collection, file_ids):
        existing = set()
        for i in range(0, len(file_ids), 500):
            batch = file_ids[i : i + 500]
            try:
                result = collection.get(ids=batch)
                existing.update(result["ids"])
            except Exception:
                pass
        return existing

    img_ids = [str(r["id"]) for r in image_rows]
    img_existing = _get_existing(img_collection, img_ids) if img_ids else set()
    # For documents, chunk IDs are "fileId_chunkN" — check by file ID prefix
    doc_ids = [str(r["id"]) for r in doc_rows]
    doc_existing = set()
    for fid in doc_ids:
        try:
            result = doc_collection.get(where={"file_id": int(fid)}, limit=1)
            if result["ids"]:
                doc_existing.add(fid)
        except Exception:
            pass

    images_to_process = [r for r in image_rows if str(r["id"]) not in img_existing]
    docs_to_process = [r for r in doc_rows if str(r["id"]) not in doc_existing]
    to_process_count = len(images_to_process) + len(docs_to_process)
    skipped = total - to_process_count
    if skipped:
        logger.info("Similarity scan: %d already indexed, %d to process", skipped, to_process_count)

    done = 0
    errors = 0

    # Process images
    for row in images_to_process:
        file_id = row["id"]
        full_path = row["full_path"]

        file_bytes = await fetch_agent_bytes(full_path, row["location_id"])
        if file_bytes is None:
            errors += 1
            done += 1
            continue

        embedding = await fetch_embedding(embed_url, file_bytes, full_path)
        if embedding is None:
            errors += 1
            done += 1
            continue

        try:
            img_collection.upsert(
                ids=[str(file_id)],
                embeddings=[embedding],
                documents=[full_path],
                metadatas=[{"file_id": file_id, "location_id": location_id}],
            )
        except Exception as e:
            logger.warning("ChromaDB upsert failed for file %d: %s", file_id, e)
            errors += 1

        done += 1
        if done % 10 == 0 or done == to_process_count:
            await broadcast({
                "type": "scan_progress",
                "locationId": location_id,
                "location": f"Similarity: {location_name}",
                "phase": "cataloging",
                "catalogDone": done + skipped,
                "catalogTotal": total,
            })

    # Process documents
    for row in docs_to_process:
        file_id = row["id"]
        full_path = row["full_path"]
        filename = row["filename"]

        file_bytes = await fetch_agent_bytes(full_path, row["location_id"])
        if file_bytes is None:
            errors += 1
            done += 1
            continue

        chunks = await fetch_document_embeddings(embed_url, file_bytes, filename)
        if chunks is None or len(chunks) == 0:
            errors += 1
            done += 1
            continue

        try:
            chunk_ids = [f"{file_id}_chunk{i}" for i in range(len(chunks))]
            embeddings = [c["embedding"] for c in chunks]
            documents = [c["text"] for c in chunks]
            metadatas = [
                {
                    "file_id": file_id,
                    "location_id": location_id,
                    "chunk_index": i,
                    "meta": c.get("meta", ""),
                }
                for i, c in enumerate(chunks)
            ]
            doc_collection.upsert(
                ids=chunk_ids,
                embeddings=embeddings,
                documents=documents,
                metadatas=metadatas,
            )
        except Exception as e:
            logger.warning("ChromaDB upsert failed for document %d: %s", file_id, e)
            errors += 1

        done += 1
        if done % 10 == 0 or done == to_process_count:
            await broadcast({
                "type": "scan_progress",
                "locationId": location_id,
                "location": f"Similarity: {location_name}",
                "phase": "cataloging",
                "catalogDone": done + skipped,
                "catalogTotal": total,
            })

    logger.info(
        "Similarity scan complete: %d indexed, %d skipped, %d errors",
        done - errors, skipped, errors,
    )
    await broadcast({
        "type": "scan_completed",
        "locationId": location_id,
        "location": f"Similarity: {location_name}",
        "filesFound": done - errors + skipped,
    })
