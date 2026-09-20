import json
import logging

from starlette.requests import Request
from file_hunter.db import read_db, execute_write
from file_hunter.core import json_ok, json_error

logger = logging.getLogger("file_hunter")
from file_hunter.services.activity import register as _act_reg, unregister as _act_unreg
from file_hunter.services.search import (
    search_files,
    search_files_advanced,
    search_by_hash,
    parse_conditions_from_params,
)


async def search(request: Request):
    page = int(request.query_params.get("page", 0))
    sort = request.query_params.get("sort", "name")
    sort_dir = request.query_params.get("sortDir", "asc")
    focus_file = request.query_params.get("focusFile")
    focus_file_id = int(focus_file) if focus_file else None

    scope_type = request.query_params.get("scopeType")
    scope_id_raw = request.query_params.get("scopeId", "")
    # Node IDs are prefixed (e.g. "fld-123", "loc-42") — strip to numeric
    scope_id = scope_id_raw.split("-", 1)[-1] if "-" in scope_id_raw else scope_id_raw
    location_id = int(scope_id) if scope_type == "location" and scope_id else None
    folder_id = int(scope_id) if scope_type == "folder" and scope_id else None

    # Only track new searches, not cached page fetches
    is_new_search = not request.query_params.get("searchId")
    act_name = f"search-{id(request)}"
    if is_new_search:
        _act_reg(act_name, "Search")

    try:
        return await _do_search(request, page, sort, sort_dir, location_id, folder_id, focus_file_id)
    except Exception as e:
        if "interrupted" in str(e):
            return json_ok(
                {
                    "items": [],
                    "folders": [],
                    "total": 0,
                    "page": 0,
                    "pageSize": 120,
                    "cancelled": True,
                }
            )
        raise
    finally:
        if is_new_search:
            _act_unreg(act_name)


async def _semantic_file_ids(semantic_query: str, embed_url: str, threshold: float = 0.3, location_ids: list[int] | None = None) -> list[int] | None:
    """Query document embeddings and return matching file IDs, or None if unavailable.
    Supports composite syntax: (legal action) + invoices - complaints
    """
    import httpx
    import numpy as np
    try:
        from file_hunter.services.similarity import (
            is_chromadb_available, get_document_collection, parse_composite_query,
        )
        if not is_chromadb_available():
            return None
        doc_coll = get_document_collection()
        doc_count = doc_coll.count()
        if doc_count == 0:
            return None

        search_url = f"{embed_url.rstrip('/')}/api/embed/search"
        composite = parse_composite_query(semantic_query)

        if composite:
            # Embed each term, combine with arithmetic
            combined = None
            async with httpx.AsyncClient(timeout=30.0) as client:
                for sign, weight, phrase in composite:
                    resp = await client.post(search_url, json={"query": phrase})
                    if resp.status_code != 200:
                        continue
                    emb = np.array(resp.json().get("embedding"), dtype=np.float32)
                    weighted = emb * sign * weight
                    combined = weighted if combined is None else combined + weighted
            if combined is None:
                return None
            norm = np.linalg.norm(combined)
            if norm > 0:
                combined = combined / norm
            query_emb = combined.tolist()
        else:
            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(search_url, json={"query": semantic_query})
            if resp.status_code != 200:
                return None
            query_emb = resp.json().get("embedding")

        if not query_emb:
            return None

        query_kwargs = {}
        if location_ids and len(location_ids) == 1:
            query_kwargs["where"] = {"location_id": location_ids[0]}
        elif location_ids and len(location_ids) > 1:
            query_kwargs["where"] = {"location_id": {"$in": location_ids}}

        results = doc_coll.query(
            query_embeddings=[query_emb],
            n_results=min(200, doc_count),
            include=["distances", "metadatas", "documents"],
            **query_kwargs,
        )
        max_distance = 1.0 - threshold
        # Split query into terms for keyword boosting
        query_terms = [t.lower() for t in semantic_query.split() if len(t) >= 2]
        logger.info("Semantic search: query=%r, terms=%s, threshold=%.3f, %d chunks in collection",
                     semantic_query[:80], query_terms, threshold, doc_count)
        # Two passes: first collect best raw distance per file and all chunk
        # text per file; then apply keyword boost across all of a file's chunks.
        best_raw = {}
        file_texts = {}
        for i, chunk_id in enumerate(results["ids"][0]):
            distance = results["distances"][0][i]
            fid = results["metadatas"][0][i].get("file_id")
            if distance <= max_distance and fid:
                if fid not in best_raw or distance < best_raw[fid]:
                    best_raw[fid] = distance
                chunk_text = (results["documents"][0][i] or "").lower()
                if fid not in file_texts:
                    file_texts[fid] = chunk_text
                else:
                    file_texts[fid] += " " + chunk_text
        # Keyword boost: check all of a file's matched chunks for query terms.
        # Each term found anywhere reduces distance by 15%.
        best = {}
        for fid, raw_dist in best_raw.items():
            all_text = file_texts.get(fid, "")
            matched = sum(1 for t in query_terms if t in all_text)
            boost = matched / max(len(query_terms), 1)
            boosted = raw_dist * (1.0 - 0.15 * boost)
            best[fid] = boosted
            logger.info("  file_id=%s raw=%.4f terms=%d/%d boost=%.0f%% adj=%.4f",
                         fid, raw_dist, matched, len(query_terms), boost * 15, boosted)
        file_ids = [fid for fid, _ in sorted(best.items(), key=lambda x: x[1])]
        # Look up filenames for the log
        if file_ids:
            from file_hunter.db import read_db as _rdb
            async with _rdb() as _db:
                ph = ",".join("?" for _ in file_ids)
                _rows = await _db.execute_fetchall(
                    f"SELECT id, filename FROM files WHERE id IN ({ph})", file_ids
                )
            _names = {r["id"]: r["filename"] for r in _rows}
        else:
            _names = {}
        logger.info("Semantic search: %d files matched (from %d chunks within threshold)",
                     len(file_ids), sum(1 for d in results["distances"][0] if d <= max_distance))
        for rank, fid in enumerate(file_ids[:5]):
            logger.info("  result %d: file_id=%s distance=%.4f  %s",
                         rank + 1, fid, best[fid], _names.get(fid, "???"))
        return file_ids if file_ids else None
    except Exception as e:
        logger.warning("Semantic search failed: %s", e)
        return None


async def _do_search(request, page, sort, sort_dir, location_id, folder_id, focus_file_id=None):
    # Semantic search — completely separate path
    semantic = request.query_params.get("semantic", "").strip()
    if semantic:
        from file_hunter.services import settings as settings_svc
        async with read_db() as db:
            enabled = await settings_svc.get_setting(db, "similaritySearchEnabled")
            embed_url = await settings_svc.get_setting(db, "similaritySearchUrl")
        if enabled != "1" or not embed_url:
            return json_ok({"items": [], "total": 0, "folders": []})
        sem_threshold = float(request.query_params.get("semanticThreshold", "0.3"))
        sem_loc_raw = request.query_params.get("semanticLocations", "").strip()
        sem_location_ids = [int(x) for x in sem_loc_raw.split(",") if x.strip()] if sem_loc_raw else None
        sem_ids = await _semantic_file_ids(semantic, embed_url, threshold=sem_threshold, location_ids=sem_location_ids)
        if not sem_ids:
            return json_ok({"items": [], "total": 0, "folders": []})
        placeholders = ",".join("?" for _ in sem_ids)
        async with read_db() as db:
            rows = await db.execute_fetchall(
                f"""SELECT id, filename AS name, file_type_high AS typeHigh,
                           file_type_low AS typeLow, file_size AS size,
                           modified_date AS date, dup_count AS dups,
                           stale, location_id AS locationId,
                           full_path, hidden
                    FROM files WHERE id IN ({placeholders}) AND stale = 0""",
                sem_ids,
            )
        file_map = {r["id"]: dict(r) for r in rows}
        items = []
        for fid in sem_ids:
            if fid in file_map:
                item = file_map[fid]
                item["type"] = "file"
                items.append(item)
        return json_ok({"items": items, "total": len(items), "folders": []})

    # Fast path: hash-only search (dup badge click)
    hash_val = request.query_params.get("hash")
    if hash_val and not any(
        request.query_params.get(k)
        for k in ("name", "type", "description", "tags", "sizeMin", "sizeMax",
                   "dateFrom", "dateTo", "dupes", "mode")
    ):
        return json_ok(await search_by_hash(hash_val, page=page, sort=sort, sort_dir=sort_dir))

    async with read_db() as db:
        if request.query_params.get("mode") == "advanced":
            conditions = parse_conditions_from_params(request.query_params)
            include_folders = request.query_params.get("folders") == "true"
            folder_only_fields = {"files"}
            if not include_folders and any(
                c["field"] in folder_only_fields
                and (c.get("from") or c.get("to"))
                for c in conditions
            ):
                return json_error(
                    "File count filter requires 'Include folders' to be enabled."
                )
            results = await search_files_advanced(
                db,
                conditions=conditions,
                include_files=request.query_params.get("files") != "false",
                include_folders=include_folders,
                location_id=location_id,
                folder_id=folder_id,
                page=page,
                sort=sort,
                sort_dir=sort_dir,
                cached_total=int(request.query_params["cachedTotal"])
                if "cachedTotal" in request.query_params
                else None,
                search_id=request.query_params.get("searchId"),
                focus_file_id=focus_file_id,
            )
        else:
            results = await search_files(
                db,
                name=request.query_params.get("name"),
                file_type=request.query_params.get("type"),
                description=request.query_params.get("description"),
                tags=request.query_params.get("tags"),
                size_min=request.query_params.get("sizeMin"),
                size_max=request.query_params.get("sizeMax"),
                date_from=request.query_params.get("dateFrom"),
                date_to=request.query_params.get("dateTo"),
                name_match=request.query_params.get("nameMatch", "anywhere"),
                include_files=request.query_params.get("files") != "false",
                dupes_only=bool(request.query_params.get("dupes")),
                min_dups=request.query_params.get("minDups"),
                max_dups=request.query_params.get("maxDups"),
                min_files=request.query_params.get("minFiles"),
                max_files=request.query_params.get("maxFiles"),
                include_folders=request.query_params.get("folders") == "true",
                hash_strong=request.query_params.get("hash"),
                location_id=location_id,
                folder_id=folder_id,
                page=page,
                sort=sort,
                sort_dir=sort_dir,
                cached_total=int(request.query_params["cachedTotal"])
                if "cachedTotal" in request.query_params
                else None,
                search_id=request.query_params.get("searchId"),
                focus_file_id=focus_file_id,
            )

    return json_ok(results)


async def list_saved_searches(request: Request):
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, name, params, created_at FROM saved_searches ORDER BY created_at DESC"
        )
    return json_ok([dict(r) for r in rows])


async def create_saved_search(request: Request):
    data = await request.json()
    name = data.get("name", "").strip()
    params = data.get("params")
    if not name or not params:
        return json_error("name and params required")

    async def _insert(conn, n, p):
        cursor = await conn.execute(
            "INSERT INTO saved_searches (name, params) VALUES (?, ?)",
            (n, json.dumps(p) if isinstance(p, dict) else str(p)),
        )
        await conn.commit()
        return cursor.lastrowid

    row_id = await execute_write(_insert, name, params)
    return json_ok({"id": row_id})


async def delete_saved_search(request: Request):
    search_id = request.path_params["id"]

    async def _delete(conn, sid):
        await conn.execute("DELETE FROM saved_searches WHERE id = ?", (sid,))
        await conn.commit()

    await execute_write(_delete, search_id)
    return json_ok({})


async def similarity_search(request: Request):
    """POST /api/search/similarity — search by image similarity or text features.

    Uses the LocalLens approach: separate queries per modality, merge candidates,
    score each candidate against both embeddings independently, average the
    cosine similarities for the combined score.
    """
    from file_hunter.services.similarity import (
        is_chromadb_available,
        get_collection,
        fetch_embedding,
        embed_text_query,
    )
    from file_hunter.services.content_proxy import fetch_agent_bytes
    from file_hunter.services import settings as settings_svc
    import numpy as np

    if not is_chromadb_available():
        return json_error("Similarity search is not available.", 400)

    import base64

    body = await request.json()
    text = body.get("text", "").strip()
    file_id = body.get("file_id")
    image_data = body.get("image_data")  # base64-encoded uploaded image
    threshold = body.get("threshold", 0.3)
    location_ids = body.get("location_ids")  # list of ints, or None for all

    async with read_db() as db:
        embed_url = await settings_svc.get_setting(db, "similaritySearchUrl")
    if not embed_url:
        return json_error("Embedding service URL not configured.", 400)

    text_emb = None
    image_emb = None

    # Text embedding (supports composite syntax: (red socks) - shoes)
    if text:
        text_emb = await embed_text_query(embed_url, text)

    # Image embedding — use stored embedding from ChromaDB
    if file_id:
        collection = get_collection()
        try:
            result = collection.get(ids=[str(file_id)], include=["embeddings"])
            if result["ids"] and len(result["embeddings"]) > 0:
                image_emb = result["embeddings"][0]
        except Exception as e:
            logger.warning("Failed to retrieve image embedding: %s", e)
        # Fall back: fetch and embed
        if image_emb is None:
            async with read_db() as db:
                row = await db.execute_fetchall(
                    "SELECT full_path, location_id FROM files WHERE id = ?",
                    (file_id,),
                )
            if row:
                image_bytes = await fetch_agent_bytes(
                    row[0]["full_path"], row[0]["location_id"]
                )
                if image_bytes:
                    image_emb = await fetch_embedding(embed_url, image_bytes)

    # Uploaded image — decode base64 and embed
    if image_data and image_emb is None:
        try:
            image_bytes = base64.b64decode(image_data)
            image_emb = await fetch_embedding(embed_url, image_bytes)
        except Exception as e:
            logger.warning("Uploaded image embedding failed: %s", e)

    if text_emb is None and image_emb is None:
        return json_error("Could not generate embedding for search.", 400)

    collection = get_collection()
    n_results = min(100, collection.count() or 100)
    if n_results == 0:
        return json_ok({"items": [], "total": 0, "folders": []})

    # Build ChromaDB where filter for location scoping
    where_filter = None
    if location_ids and len(location_ids) == 1:
        where_filter = {"location_id": location_ids[0]}
    elif location_ids and len(location_ids) > 1:
        where_filter = {"location_id": {"$in": location_ids}}

    # Collect candidates from each modality
    candidates = {}  # doc_id -> stored embedding
    query_kwargs = {}
    if where_filter:
        query_kwargs["where"] = where_filter

    if text_emb is not None:
        results = collection.query(
            query_embeddings=[text_emb],
            n_results=n_results,
            include=["embeddings"],
            **query_kwargs,
        )
        for i, doc_id in enumerate(results["ids"][0]):
            if doc_id not in candidates:
                candidates[doc_id] = np.array(results["embeddings"][0][i], dtype=np.float32)

    if image_emb is not None:
        results = collection.query(
            query_embeddings=[image_emb],
            n_results=n_results,
            include=["embeddings"],
            **query_kwargs,
        )
        for i, doc_id in enumerate(results["ids"][0]):
            if doc_id not in candidates:
                candidates[doc_id] = np.array(results["embeddings"][0][i], dtype=np.float32)

    if not candidates:
        return json_ok({"items": [], "total": 0, "folders": []})

    # Score each candidate against both query embeddings
    text_vec = np.array(text_emb, dtype=np.float32) if text_emb is not None else None
    image_vec = np.array(image_emb, dtype=np.float32) if image_emb is not None else None
    combined = text_vec is not None and image_vec is not None

    scored = []
    for doc_id, db_emb in candidates.items():
        if combined:
            text_sim = float(np.dot(text_vec, db_emb))
            image_sim = float(np.dot(image_vec, db_emb))
            score = (text_sim + image_sim) / 2.0
        elif text_vec is not None:
            score = float(np.dot(text_vec, db_emb))
        else:
            score = float(np.dot(image_vec, db_emb))

        if score >= threshold:
            scored.append((doc_id, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    if not scored:
        return json_ok({"items": [], "total": 0, "folders": []})

    matched_ids = [int(doc_id) for doc_id, _ in scored]

    # Fetch file details from catalogue
    placeholders = ",".join("?" for _ in matched_ids)
    async with read_db() as db:
        rows = await db.execute_fetchall(
            f"""SELECT id, filename AS name, file_type_high AS typeHigh,
                       file_type_low AS typeLow, file_size AS size,
                       modified_date AS date, dup_count AS dups,
                       stale, location_id AS locationId,
                       full_path, hidden
                FROM files WHERE id IN ({placeholders})""",
            matched_ids,
        )

    # Preserve score ranking order
    file_map = {r["id"]: dict(r) for r in rows}
    items = []
    for fid in matched_ids:
        if fid in file_map:
            item = file_map[fid]
            item["type"] = "file"
            items.append(item)

    return json_ok({"items": items, "total": len(items), "folders": []})
