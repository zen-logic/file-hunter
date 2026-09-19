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


def get_collection():
    """Get or create the similarity ChromaDB collection."""
    global _chroma_client
    if not ensure_chromadb():
        raise RuntimeError("chromadb is not available and could not be installed")
    import chromadb

    os.makedirs(SIMILARITY_DB_DIR, exist_ok=True)
    if _chroma_client is None:
        _chroma_client = chromadb.PersistentClient(path=SIMILARITY_DB_DIR)
    return _chroma_client.get_or_create_collection(
        name="image_embeddings", metadata={"hnsw:space": "cosine"}
    )


async def fetch_embedding(embed_url: str, image_bytes: bytes) -> list[float] | None:
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
            logger.warning("Embedding service returned %d", resp.status_code)
            return None
        data = resp.json()
        return data.get("embedding")
    except Exception as e:
        logger.warning("Embedding service error: %s", e)
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


async def run_similarity_scan(op_id: int, agent_id: int | None, params: dict):
    """Walk catalogued images and index their embeddings."""
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

    # Find all image files in the catalogue for this scope
    async with read_db() as db:
        if recursive:
            # All images under this location, optionally under a subfolder
            if scan_path == root_path:
                rows = await db.execute_fetchall(
                    "SELECT id, full_path, location_id FROM files "
                    "WHERE location_id = ? AND file_type_high = 'image' AND stale = 0",
                    (location_id,),
                )
            else:
                # Subfolder: match on full_path prefix
                prefix = scan_path.rstrip("/") + "/"
                rows = await db.execute_fetchall(
                    "SELECT id, full_path, location_id FROM files "
                    "WHERE location_id = ? AND file_type_high = 'image' AND stale = 0 "
                    "AND (full_path = ? OR full_path LIKE ?)",
                    (location_id, scan_path, prefix + "%"),
                )
        else:
            # Non-recursive: only files directly in this folder
            folder_id = params.get("folder_id")
            if folder_id:
                rows = await db.execute_fetchall(
                    "SELECT id, full_path, location_id FROM files "
                    "WHERE folder_id = ? AND file_type_high = 'image' AND stale = 0",
                    (folder_id,),
                )
            else:
                # Location root, no subfolder
                rows = await db.execute_fetchall(
                    "SELECT id, full_path, location_id FROM files "
                    "WHERE location_id = ? AND folder_id IS NULL "
                    "AND file_type_high = 'image' AND stale = 0",
                    (location_id,),
                )

    total = len(rows)
    logger.info("Similarity scan: %d images in %s", total, location_name)

    if total == 0:
        await broadcast({
            "type": "scan_completed",
            "locationId": location_id,
            "location": f"Similarity: {location_name}",
            "filesFound": 0,
        })
        return

    collection = get_collection()

    # Check which files are already indexed
    file_ids = [str(r["id"]) for r in rows]
    existing = set()
    # ChromaDB get() in batches
    for i in range(0, len(file_ids), 500):
        batch = file_ids[i : i + 500]
        try:
            result = collection.get(ids=batch)
            existing.update(result["ids"])
        except Exception:
            pass

    to_process = [r for r in rows if str(r["id"]) not in existing]
    skipped = total - len(to_process)
    if skipped:
        logger.info("Similarity scan: %d already indexed, %d to process", skipped, len(to_process))

    done = 0
    errors = 0

    for row in to_process:
        file_id = row["id"]
        full_path = row["full_path"]

        # Fetch image bytes from agent
        image_bytes = await fetch_agent_bytes(full_path, row["location_id"])
        if image_bytes is None:
            errors += 1
            done += 1
            continue

        # Get embedding
        embedding = await fetch_embedding(embed_url, image_bytes)
        if embedding is None:
            errors += 1
            done += 1
            continue

        # Store in ChromaDB
        try:
            collection.upsert(
                ids=[str(file_id)],
                embeddings=[embedding],
                documents=[full_path],
                metadatas=[{"file_id": file_id, "location_id": location_id}],
            )
        except Exception as e:
            logger.warning("ChromaDB upsert failed for file %d: %s", file_id, e)
            errors += 1

        done += 1
        if done % 10 == 0 or done == len(to_process):
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
