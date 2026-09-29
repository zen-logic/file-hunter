"""Similarity search support — optional chromadb dependency."""

import logging
import os
import re
import subprocess
import sys

import httpx

from file_hunter.core import BadRequest
from file_hunter.services import settings as settings_svc
from file_hunter import text_db
from file_hunter.db import execute_write, read_db
from file_hunter.services import fs
from file_hunter.services.content_proxy import fetch_agent_bytes
from file_hunter.services.op_result_log import add_to_catalog
from file_hunter.ws.scan import broadcast

logger = logging.getLogger(__name__)

chromadb_available: bool | None = None
chroma_client = None

SIMILARITY_DB_DIR = "data/similarity"


def is_chromadb_available() -> bool:
    """Check whether chromadb is importable."""
    global chromadb_available
    if chromadb_available is None:
        try:
            import chromadb  # noqa: F401
            chromadb_available = True
        except ImportError:
            chromadb_available = False
    return chromadb_available


def require_chromadb():
    """BadRequest unless chromadb is available."""
    if not is_chromadb_available():
        raise BadRequest("Similarity search is not available.")


async def embedding_url(db) -> str:
    """The configured embedding service URL; BadRequest if there isn't one."""
    url = await settings_svc.get_setting(db, "similaritySearchUrl")
    if not url:
        raise BadRequest("Embedding service URL not configured.")
    return url


def get_embedding_counts() -> dict | None:
    """Return image and document embedding counts, or None if unavailable."""
    if not is_chromadb_available():
        return None
    try:
        img_count = get_collection().count()
        doc_count = get_document_collection().count()
        return {"imageEmbeddings": img_count, "documentEmbeddings": doc_count}
    except Exception:
        return None


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
    global chromadb_available
    chromadb_available = True
    logger.info("chromadb installed")
    return True


def get_client():
    global chroma_client
    if not ensure_chromadb():
        raise RuntimeError("chromadb is not available and could not be installed")
    import chromadb
    os.makedirs(SIMILARITY_DB_DIR, exist_ok=True)
    if chroma_client is None:
        chroma_client = chromadb.PersistentClient(path=SIMILARITY_DB_DIR)
    return chroma_client


def get_collection():
    """Get or create the image embeddings collection."""
    return get_client().get_or_create_collection(
        name="image_embeddings", metadata={"hnsw:space": "cosine"}
    )


def get_document_collection():
    """Get or create the document chunk embeddings collection."""
    return get_client().get_or_create_collection(
        name="document_embeddings", metadata={"hnsw:space": "cosine"}
    )


async def remove_embeddings(file_ids: list[int]):
    """Remove embeddings for the given file IDs from both collections, and
    their document text from text.db.

    Safe to call when similarity search is not enabled — returns silently
    if chromadb is not available.
    """
    if not file_ids:
        return
    await text_db.delete_files(file_ids)
    if not is_chromadb_available():
        return
    try:
        img_coll = get_collection()
        img_ids = [str(fid) for fid in file_ids]
        # ChromaDB silently ignores IDs that don't exist
        for i in range(0, len(img_ids), 500):
            img_coll.delete(ids=img_ids[i : i + 500])
    except Exception as e:
        logger.warning("Failed to remove image embeddings: %s", e)

    try:
        doc_coll = get_document_collection()
        for fid in file_ids:
            # Document chunks are stored as "fileId_chunkN"
            try:
                result = doc_coll.get(where={"file_id": fid})
                if result["ids"]:
                    doc_coll.delete(ids=result["ids"])
            except Exception:
                pass
    except Exception as e:
        logger.warning("Failed to remove document embeddings: %s", e)


def update_embedding_location(file_id: int, new_location_id: int):
    """Update the location_id metadata for a file's embeddings after a cross-location move.

    Safe to call when similarity search is not enabled.
    """
    if not is_chromadb_available():
        return
    str_id = str(file_id)
    try:
        img_coll = get_collection()
        result = img_coll.get(ids=[str_id])
        if result["ids"]:
            img_coll.update(ids=[str_id], metadatas=[{"file_id": file_id, "location_id": new_location_id}])
    except Exception as e:
        logger.warning("Failed to update image embedding location: %s", e)

    try:
        doc_coll = get_document_collection()
        result = doc_coll.get(where={"file_id": file_id})
        if result["ids"]:
            metadatas = [
                {**m, "location_id": new_location_id}
                for m in result["metadatas"]
            ]
            doc_coll.update(ids=result["ids"], metadatas=metadatas)
    except Exception as e:
        logger.warning("Failed to update document embedding location: %s", e)


async def mark_embedded(file_id: int, embedded: bool):
    """Set or clear the embedded flag on a single file."""
    await mark_embedded_batch([file_id], embedded)


async def mark_embedded_batch(file_ids: list[int], embedded: bool):
    """Set or clear the embedded flag on a batch of files."""
    if not file_ids:
        return
    async def do_update(db, ids, val):
        placeholders = ",".join("?" for _ in ids)
        await db.execute(f"UPDATE files SET embedded = ? WHERE id IN ({placeholders})", [val] + ids)
        await db.commit()
    await execute_write(do_update, file_ids, 1 if embedded else 0)


async def fetch_embedding(embed_url: str, image_bytes: bytes, path: str = "") -> list[float] | None:
    """Send image bytes to the embedding service, return the vector.

    Returns None for unembeddable content (bad image, unsupported format).
    Raises ConnectionError when the embedding service is unreachable.
    """
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
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        raise ConnectionError(f"Embedding service unavailable: {e}") from e
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


async def embed_text_query(embed_url: str, query: str):
    """Embed a text query, handling composite syntax.

    Returns (positive_emb, negative_embs) where:
    - positive_emb is the query vector (additive terms combined)
    - negative_embs is a list of vectors for subtractive terms

    For plain queries, negative_embs is empty.
    Returns (None, []) on failure.
    """
    import numpy as np

    composite = parse_composite_query(query)

    if not composite:
        # Plain query — single embedding
        url = f"{embed_url.rstrip('/')}/api/embed/text"
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(url, json={"text": query})
            if resp.status_code == 200:
                return resp.json().get("embedding"), []
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            raise ConnectionError(f"Embedding service unavailable: {e}") from e
        except Exception as e:
            logger.warning("Text embedding failed: %s", e)
        return None, []

    # Composite query — separate positive and negative terms
    url = f"{embed_url.rstrip('/')}/api/embed/text"
    positive = None
    negatives = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            for sign, weight, phrase in composite:
                resp = await client.post(url, json={"text": phrase})
                if resp.status_code != 200:
                    logger.warning("Embedding failed for phrase: %s", phrase)
                    continue
                emb = np.array(resp.json().get("embedding"), dtype=np.float32)
                if sign > 0:
                    weighted = emb * weight
                    positive = weighted if positive is None else positive + weighted
                else:
                    negatives.append(emb * weight)
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        raise ConnectionError(f"Embedding service unavailable: {e}") from e
    except Exception as e:
        logger.warning("Composite text embedding failed: %s", e)
        return None, []

    if positive is None:
        return None, []

    # Re-normalise the positive vector
    norm = np.linalg.norm(positive)
    if norm > 0:
        positive = positive / norm

    return positive.tolist(), [n.tolist() for n in negatives]


async def fetch_document_embeddings(
    embed_url: str, file_bytes: bytes, filename: str,
) -> list[dict] | None:
    """Send document bytes to the embedding service, return chunks with embeddings.

    Returns None for unextractable content (unsupported format, empty document).
    Raises ConnectionError when the embedding service is unreachable.
    """
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
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        raise ConnectionError(f"Embedding service unavailable: {e}") from e
    except Exception as e:
        logger.warning("Document embedding error: %s", e)
        return None


def scan_label(embed_types, name):
    """The label a similarity scan of name is shown with."""
    kind = (
        "Image similarity" if embed_types == "image"
        else "Document content" if embed_types == "document"
        else "Similarity"
    )
    return f"{kind}: {name}"


async def store_images(items):
    """Store image embeddings, items being (file_id, location_id, full_path,
    embedding), and mark the files embedded."""
    file_ids, location_ids, paths, embeddings = zip(*items)
    get_collection().upsert(
        ids=[str(fid) for fid in file_ids],
        embeddings=list(embeddings),
        documents=list(paths),
        metadatas=[
            {"file_id": fid, "location_id": loc}
            for fid, loc in zip(file_ids, location_ids)
        ],
    )
    await mark_embedded_batch(list(file_ids), True)


async def store_document(file_id, location_id, chunks):
    """Store a document's chunk embeddings and chunk text, and mark it
    embedded."""
    get_document_collection().upsert(
        ids=[f"{file_id}_chunk{i}" for i in range(len(chunks))],
        embeddings=[c["embedding"] for c in chunks],
        documents=[c["text"] for c in chunks],
        metadatas=[
            {
                "file_id": file_id,
                "location_id": location_id,
                "chunk_index": i,
                "meta": c.get("meta", ""),
            }
            for i, c in enumerate(chunks)
        ],
    )
    await text_db.store_chunks(file_id, chunks)
    await mark_embedded(file_id, True)


async def run_embed_file(op_id: int, agent_id: int | None, params: dict):
    """Embed a single file — image or document — runs as a queued operation."""

    file_id = params["file_id"]
    filename = params["filename"]
    full_path = params["path"]
    location_id = params["location_id"]
    embed_url = params["embed_url"]
    embed_type = params.get("type", "document")

    async def finish(**fields):
        await broadcast(
            {"type": "embed_completed", "fileId": file_id, "filename": filename, **fields}
        )

    await broadcast({
        "type": "embed_started",
        "fileId": file_id,
        "filename": filename,
    })

    file_bytes = await fetch_agent_bytes(full_path, location_id)
    if file_bytes is None:
        await finish(error="File not available (agent offline)")
        return

    if embed_type == "image":
        try:
            embedding = await fetch_embedding(embed_url, file_bytes, full_path)
        except ConnectionError:
            await finish(error="Embedding service unavailable")
            return
        if embedding is None:
            await finish(error="Could not generate image embedding")
            return
        await store_images([(file_id, location_id, full_path, embedding)])
        logger.info("Embedded image %s", filename)
        await finish(chunks=1)
    else:
        try:
            chunks = await fetch_document_embeddings(embed_url, file_bytes, filename)
        except ConnectionError:
            await finish(error="Embedding service unavailable")
            return
        if chunks is None or len(chunks) == 0:
            await finish(error="No content could be extracted")
            return
        await store_document(file_id, location_id, chunks)
        logger.info("Embedded %s: %d chunks", filename, len(chunks))
        await finish(chunks=len(chunks))


async def fetch_markdown(embed_url: str, file_bytes: bytes, filename: str) -> str:
    """Convert a document to markdown with the embedding service.

    Raises RuntimeError with a message fit to show the user.
    """
    url = f"{embed_url.rstrip('/')}/api/extract/markdown"
    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.post(url, content=file_bytes, headers={"X-Filename": filename})
    except (httpx.ConnectError, httpx.ConnectTimeout):
        raise RuntimeError("The embedding service is not running or can't be reached.")
    except httpx.TimeoutException:
        raise RuntimeError("The embedding service took too long (over 10 minutes) to convert this document.")
    except httpx.HTTPError as e:
        logger.warning("Markdown extraction request failed: %s", e)
        raise RuntimeError("The embedding service connection failed during extraction.")

    if resp.status_code == 404:
        raise RuntimeError("The embedding service is too old to extract documents. Update it and try again.")
    if resp.status_code != 200:
        try:
            detail = resp.json().get("error", "")
        except ValueError:
            detail = resp.text[:200]
        logger.warning("Markdown extraction returned %d: %s", resp.status_code, detail)
        if resp.status_code == 422 and detail:
            raise RuntimeError(detail)  # service's own user-facing message
        raise RuntimeError("The embedding service could not convert this document.")
    return resp.json().get("markdown", "")


async def run_extract_markdown(op_id: int, agent_id: int | None, params: dict):
    """Extract a document to markdown beside the source file — runs as a queued operation.

    Every outcome ends in an extract_completed broadcast, with an error the
    user can act on if it failed.
    """

    file_id = params["file_id"]
    filename = params["filename"]

    await broadcast({"type": "extract_started", "fileId": file_id, "filename": filename})
    try:
        result = await extract_markdown(params)
    except Exception:
        logger.exception("Markdown extraction failed for %s", filename)
        result = {"error": "Something went wrong. The server log has the details."}
    await broadcast({"type": "extract_completed", "fileId": file_id, "filename": filename, **result})


async def extract_markdown(params: dict) -> dict:
    """Do the extraction. Returns the completion fields, or {"error": message}."""
    file_id = params["file_id"]
    filename = params["filename"]
    full_path = params["path"]
    location_id = params["location_id"]

    file_bytes = await fetch_agent_bytes(full_path, location_id)
    if file_bytes is None:
        return {"error": "The file could not be read. Its location may be offline."}

    try:
        markdown = await fetch_markdown(params["embed_url"], file_bytes, filename)
    except RuntimeError as e:
        return {"error": str(e)}
    if not markdown.strip():
        return {"error": "No text was found in this document. It may be a scanned image."}

    try:
        dest = await fs.unique_dest_path(os.path.splitext(full_path)[0] + ".md", location_id)
        await fs.file_write_text(dest, markdown, location_id)
    except Exception as e:
        logger.warning("Could not write markdown for %s: %s", full_path, e)
        return {"error": "The markdown file could not be saved. The location may be offline or read-only."}

    async with read_db() as db:
        rows = await db.execute_fetchall("SELECT folder_id FROM files WHERE id = ?", (file_id,))
    folder_id = rows[0]["folder_id"] if rows else None
    new_file_id = await add_to_catalog(dest, location_id, folder_id)

    logger.info("Extracted %s to %s", filename, dest)
    return {
        "newFileId": new_file_id,
        "newFilename": os.path.basename(dest),
        "folderId": folder_id,
        "locationId": location_id,
    }


async def run_similarity_scan(op_id: int, agent_id: int | None, params: dict):
    """Walk catalogued images and documents and index their embeddings."""

    location_id = params["location_id"]
    location_name = params["location_name"]
    root_path = params["root_path"]
    scan_path = params.get("path", root_path)
    recursive = params.get("recursive", True)
    embed_url = params["embed_url"]
    embed_types = params.get("embed_types")  # "image", "document", or None (all)

    label = scan_label(embed_types, location_name)

    await broadcast({
        "type": "scan_started",
        "locationId": location_id,
        "location": label,
    })

    EMBEDDABLE_IMAGE_SUBTYPES = {"jpg", "png", "gif", "bmp", "webp", "tiff"}
    MIN_IMAGE_SIZE = 10000  # skip images under 10KB (thumbnails, icons)

    if embed_types == "image":
        EMBEDDABLE_TYPES = ("image",)
    elif embed_types == "document":
        EMBEDDABLE_TYPES = ("document", "text")
    else:
        EMBEDDABLE_TYPES = ("image", "document", "text")

    # Find all embeddable files in the catalogue for this scope
    type_placeholders = ",".join("?" for _ in EMBEDDABLE_TYPES)
    type_params = list(EMBEDDABLE_TYPES)

    async with read_db() as db:
        if recursive:
            if scan_path == root_path:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low, file_size FROM files "
                    f"WHERE location_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0 AND embedded = 0",
                    [location_id] + type_params,
                )
            else:
                prefix = scan_path.rstrip("/") + "/"
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low, file_size FROM files "
                    f"WHERE location_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0 AND embedded = 0 "
                    f"AND (full_path = ? OR full_path LIKE ?)",
                    [location_id] + type_params + [scan_path, prefix + "%"],
                )
        else:
            folder_id = params.get("folder_id")
            if folder_id:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low, file_size FROM files "
                    f"WHERE folder_id = ? AND file_type_high IN ({type_placeholders}) AND stale = 0 AND embedded = 0",
                    [folder_id] + type_params,
                )
            else:
                rows = await db.execute_fetchall(
                    f"SELECT id, full_path, filename, location_id, file_type_high, file_type_low, file_size FROM files "
                    f"WHERE location_id = ? AND folder_id IS NULL "
                    f"AND file_type_high IN ({type_placeholders}) AND stale = 0 AND embedded = 0",
                    [location_id] + type_params,
                )

    images_to_process = [r for r in rows if r["file_type_high"] == "image"
                         and (r["file_type_low"] or "") in EMBEDDABLE_IMAGE_SUBTYPES
                         and (r["file_size"] or 0) >= MIN_IMAGE_SIZE]
    docs_to_process = [r for r in rows if r["file_type_high"] in ("document", "text")]
    to_process_count = len(images_to_process) + len(docs_to_process)
    logger.info(
        "Similarity scan: %d files (%d images, %d documents) in %s",
        to_process_count, len(images_to_process), len(docs_to_process), location_name,
    )

    if to_process_count == 0:
        await broadcast({
            "type": "scan_completed",
            "locationId": location_id,
            "location": label,
            "filesFound": 0,
        })
        return

    # open both collections up front: a ChromaDB that can't open fails the
    # scan here, before any file is processed
    get_collection()
    get_document_collection()

    done = 0
    errors = 0
    BATCH_SIZE = 100

    # Images are stored in batches of (file_id, location_id, path, embedding)
    pending_images = []

    async def flush_img_batch():
        nonlocal pending_images
        if not pending_images:
            return
        try:
            await store_images(pending_images)
        except Exception as e:
            logger.warning(
                "ChromaDB batch upsert failed (%d images): %s", len(pending_images), e
            )
        pending_images = []

    async def report_progress():
        if done % 10 == 0 or done == to_process_count:
            await broadcast({
                "type": "scan_progress",
                "locationId": location_id,
                "location": label,
                "phase": "cataloging",
                "catalogDone": done,
                "catalogTotal": to_process_count,
            })

    async def abort_unavailable():
        logger.warning("Embedding service unavailable — aborting scan")
        await flush_img_batch()
        await broadcast({
            "type": "scan_completed",
            "locationId": location_id,
            "location": label,
            "error": "Embedding service unavailable",
        })

    for row in images_to_process:
        file_id = row["id"]
        full_path = row["full_path"]

        file_bytes = await fetch_agent_bytes(full_path, row["location_id"])
        if file_bytes is None:
            errors += 1
            done += 1
            continue

        try:
            embedding = await fetch_embedding(embed_url, file_bytes, full_path)
        except ConnectionError:
            await abort_unavailable()
            return
        if embedding is None:
            errors += 1
            done += 1
            continue

        pending_images.append((file_id, location_id, full_path, embedding))
        if len(pending_images) >= BATCH_SIZE:
            await flush_img_batch()

        done += 1
        await report_progress()

    await flush_img_batch()

    # Documents: each file's chunks are stored together
    for row in docs_to_process:
        file_id = row["id"]
        full_path = row["full_path"]
        filename = row["filename"]

        file_bytes = await fetch_agent_bytes(full_path, row["location_id"])
        if file_bytes is None:
            errors += 1
            done += 1
            continue

        try:
            chunks = await fetch_document_embeddings(embed_url, file_bytes, filename)
        except ConnectionError:
            await abort_unavailable()
            return
        if chunks is None or len(chunks) == 0:
            errors += 1
            done += 1
            continue

        try:
            await store_document(file_id, location_id, chunks)
        except Exception as e:
            logger.warning("ChromaDB upsert failed for document %d: %s", file_id, e)
            errors += 1

        done += 1
        await report_progress()

    logger.info(
        "Similarity scan complete: %d indexed, %d errors",
        done - errors, errors,
    )
    await broadcast({
        "type": "scan_completed",
        "locationId": location_id,
        "location": label,
        "filesFound": done - errors,
    })
