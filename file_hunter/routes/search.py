import json
import logging

from starlette.requests import Request
from file_hunter.db import read_db, execute_write
from file_hunter.core import BadRequest, json_error, json_ok, parse_float, parse_int, parse_int_array, parse_int_list, parse_node_id, parse_str, read_body
from file_hunter.helpers import resolve_target
from file_hunter.services.activity import register as act_reg, unregister as act_unreg
from file_hunter.services.similarity import embedding_url, require_chromadb, is_chromadb_available, get_document_collection, parse_composite_query, get_collection, fetch_embedding, embed_text_query
from file_hunter.services.search import (
    search_files,
    search_files_advanced,
    search_by_hash,
    parse_conditions_from_params,
    build_scope_sql,
)
from file_hunter.text_db import read_text
from file_hunter.services import settings as settings_svc
from file_hunter.services.content_proxy import fetch_agent_bytes
import re
import httpx
import base64

logger = logging.getLogger("file_hunter")


async def search(request: Request):
    page = parse_int(request.query_params.get("page"), "page", 0, minimum=0)
    sort = request.query_params.get("sort", "name")
    sort_dir = request.query_params.get("sortDir", "asc")
    focus_file = request.query_params.get("focusFile")
    focus_file_id = parse_int(focus_file, "focusFile", None)

    scope_type = request.query_params.get("scopeType")
    scope_id_raw = request.query_params.get("scopeId", "")
    # Node IDs are prefixed (e.g. "fld-123", "loc-42") — strip to numeric
    scope_id = scope_id_raw.split("-", 1)[-1] if "-" in scope_id_raw else scope_id_raw
    location_id = parse_int(scope_id, "scopeId", None) if scope_type == "location" else None
    folder_id = parse_int(scope_id, "scopeId", None) if scope_type == "folder" else None

    # Only track new searches, not cached page fetches
    is_new_search = not request.query_params.get("searchId")
    act_name = f"search-{id(request)}"
    if is_new_search:
        act_reg(act_name, "Search")

    try:
        return await do_search(request, page, sort, sort_dir, location_id, folder_id, focus_file_id)
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
            act_unreg(act_name)


CONTENT_RESULT_LIMIT = 200

# ChromaDB 1.5 fails with "too many SQL variables" above ~16,380 ids in one
# $in (each id costs two SQLite variables), fewer when combined with $and
CHROMA_IN_BATCH = 10_000


def chroma_query(collection, embedding, n_results: int, include: list[str],
                  where: dict | None = None, file_ids: list[int] | None = None) -> dict:
    """collection.query for one embedding, in ChromaDB's result shape.

    file_ids ("Search within") may be any length: they are sent in batches
    and the results merged by distance. The best n_results of every batch
    contain the best n_results overall, so the merge is exact.
    """
    include = list(dict.fromkeys([*include, "distances"]))
    if file_ids is None:
        kwargs = {"where": where} if where else {}
        return collection.query(query_embeddings=[embedding], n_results=n_results, include=include, **kwargs)
    keys = ["ids", *include]
    rows = []
    for start in range(0, len(file_ids), CHROMA_IN_BATCH):
        batch = file_ids[start:start + CHROMA_IN_BATCH]
        r = collection.query(query_embeddings=[embedding], n_results=n_results, include=include,
                             where={"file_id": {"$in": batch}})
        rows.extend({k: r[k][0][i] for k in keys} for i in range(len(r["ids"][0])))
    rows.sort(key=lambda row: row["distances"])
    rows = rows[:n_results]
    return {k: [[row[k] for row in rows]] for k in keys}


async def scope_embedded_ids(location_id: int | None, folder_id: int | None, types: tuple[str, ...]) -> list[int]:
    """Ids of embedded files of the given types inside a location or folder
    (subfolders included), from the catalogue. "Search within" hands these to
    ChromaDB, so the scope is always current, whatever has moved."""
    async with read_db() as db:
        scope_frag, _, scope_params = await build_scope_sql(db, location_id=location_id, folder_id=folder_id)
        ph = ",".join("?" for _ in types)
        rows = await db.execute_fetchall(
            f"SELECT f.id FROM files f WHERE {scope_frag} AND f.embedded = 1 AND f.file_type_high IN ({ph})",
            scope_params + list(types),
        )
    return [r["id"] for r in rows]


def fts_query(text: str) -> str:
    """Turn a user's search text into a safe FTS5 query.

    "quoted phrases" stay phrases, a trailing * is a prefix match, a leading
    - excludes a word or phrase, every other word must appear. Anything that
    isn't a word is dropped, so user input can never be an FTS syntax error.
    """

    include, exclude = [], []
    negate_next = False  # a standalone "-", as in "(a) + b - c"
    for neg, phrase, word in re.findall(r'(-?)(?:"([^"]*)"|(\S+))', text):
        words = re.findall(r"\w+", phrase or word)
        if not words:
            negate_next = negate_next or (neg == "-" or word == "-")
            continue
        neg = neg or negate_next
        negate_next = False
        # a quoted phrase, or a word like x-ray / o'brien, matches as a phrase
        term = '"' + " ".join(words) + '"'
        if word.endswith("*"):
            term += "*"
        (exclude if neg else include).append(term)
    if not include:
        return ""  # FTS5 can't search for exclusions alone
    return " ".join(include) + "".join(f" NOT {t}" for t in exclude)


async def text_file_ids(query: str) -> list[int] | None:
    """Full-text search over document chunk text. File ids, best match first."""

    match = fts_query(query)
    if not match:
        return None
    async with read_text() as db:
        rows = await db.execute_fetchall(
            """WITH hits AS MATERIALIZED (
                   SELECT rowid AS id, bm25(chunks_fts) AS score
                   FROM chunks_fts WHERE chunks_fts MATCH ?)
               SELECT c.file_id, MIN(h.score) AS best
               FROM hits h JOIN chunks c ON c.id = h.id
               GROUP BY c.file_id ORDER BY best""",
            (match,),
        )
    logger.info("Text search: %r -> %s, %d files", query[:80], match, len(rows))
    return [r["file_id"] for r in rows] or None


async def semantic_file_ids(semantic_query: str, embed_url: str, threshold: float = 0.3, location_ids: list[int] | None = None, file_ids: list[int] | None = None) -> list[int] | None:
    """Query document embeddings and return matching file IDs, or None if unavailable.
    Supports composite syntax: (legal action) + invoices - complaints
    """
    import numpy as np
    try:
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

        # file_ids ("Search within") ranks only the scope's documents
        where = None
        if location_ids and len(location_ids) == 1:
            where = {"location_id": location_ids[0]}
        elif location_ids and len(location_ids) > 1:
            where = {"location_id": {"$in": location_ids}}

        results = chroma_query(
            doc_coll, query_emb, min(200, doc_count),
            ["distances", "metadatas", "documents"],
            where=None if file_ids is not None else where, file_ids=file_ids,
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
            async with read_db() as db:
                ph = ",".join("?" for _ in file_ids)
                rows = await db.execute_fetchall(
                    f"SELECT id, filename FROM files WHERE id IN ({ph})", file_ids
                )
            names = {r["id"]: r["filename"] for r in rows}
        else:
            names = {}
        logger.info("Semantic search: %d files matched (from %d chunks within threshold)",
                     len(file_ids), sum(1 for d in results["distances"][0] if d <= max_distance))
        for rank, fid in enumerate(file_ids[:5]):
            logger.info("  result %d: file_id=%s distance=%.4f  %s",
                         rank + 1, fid, best[fid], names.get(fid, "???"))
        return file_ids if file_ids else None
    except (httpx.ConnectError, httpx.ConnectTimeout) as e:
        raise ConnectionError(f"Embedding service unavailable: {e}") from e
    except Exception as e:
        logger.warning("Semantic search failed: %s", e)
        return None


async def do_search(request, page, sort, sort_dir, location_id, folder_id, focus_file_id=None):
    # Semantic search — completely separate path
    semantic = request.query_params.get("semantic", "").strip()
    if semantic:
        text_mode = request.query_params.get("semanticMode") == "text"
        async with read_db() as db:
            enabled = await settings_svc.get_setting(db, "similaritySearchEnabled")
            embed_url = await settings_svc.get_setting(db, "similaritySearchUrl")
        if enabled != "1" or (not embed_url and not text_mode):
            return json_ok({"items": [], "total": 0, "folders": [], "page": 0})
        sem_threshold = parse_float(request.query_params.get("semanticThreshold"), "semanticThreshold", 0.3)
        sem_loc_raw = request.query_params.get("semanticLocations", "").strip()
        sem_location_ids = parse_int_list(sem_loc_raw, "semanticLocations") if sem_loc_raw else None
        # "Search within" a location or folder, resolved against the catalogue
        scoped = bool(location_id or folder_id)
        scope_frag, scope_params = "", []
        if scoped:
            sem_location_ids = None
            async with read_db() as db:
                scope_frag, _, scope_params = await build_scope_sql(db, location_id=location_id, folder_id=folder_id)
        if text_mode:
            sem_ids = await text_file_ids(semantic)
        else:
            scope_file_ids = None
            if scoped:
                scope_file_ids = await scope_embedded_ids(location_id, folder_id, ("document", "text"))
                if not scope_file_ids:
                    return json_ok({"items": [], "total": 0, "folders": [], "page": 0})
            try:
                sem_ids = await semantic_file_ids(
                    semantic, embed_url, threshold=sem_threshold,
                    location_ids=sem_location_ids, file_ids=scope_file_ids,
                )
            except ConnectionError:
                return json_error("Embedding service unavailable.", 503)
        if not sem_ids:
            return json_ok({"items": [], "total": 0, "folders": [], "page": 0})
        # Location filter applies before the result cap, so a scoped search
        # returns the best matches inside the scope
        loc_sql, loc_params = "", []
        if scoped:
            loc_sql, loc_params = f" AND {scope_frag}", scope_params
        elif sem_location_ids:
            loc_sql = f" AND f.location_id IN ({','.join('?' for _ in sem_location_ids)})"
            loc_params = sem_location_ids
        file_map = {}
        async with read_db() as db:
            for start in range(0, len(sem_ids), 900):
                batch = sem_ids[start:start + 900]
                placeholders = ",".join("?" for _ in batch)
                rows = await db.execute_fetchall(
                    f"""SELECT id, filename AS name, file_type_high AS typeHigh,
                               file_type_low AS typeLow, file_size AS size,
                               modified_date AS date, dup_count AS dups,
                               stale, location_id AS locationId,
                               full_path, hidden
                        FROM files f WHERE f.id IN ({placeholders}) AND f.stale = 0{loc_sql}""",
                    batch + loc_params,
                )
                file_map.update((r["id"], dict(r)) for r in rows)
        items = []
        for fid in sem_ids:
            if fid in file_map:
                item = file_map[fid]
                item["type"] = "file"
                items.append(item)
                if len(items) == CONTENT_RESULT_LIMIT:
                    break
        return json_ok({"items": items, "total": len(items), "folders": [], "page": 0})

    # Fast path: hash-only search (dup badge click)
    hash_val = request.query_params.get("hash")
    if hash_val and not any(
        request.query_params.get(k)
        for k in ("name", "type", "description", "tags", "sizeMin", "sizeMax",
                   "dateFrom", "dateTo", "dupes", "mode")
    ):
        return json_ok(await search_by_hash(hash_val, page=page, sort=sort, sort_dir=sort_dir))

    qp = request.query_params
    common = dict(
        include_files=qp.get("files") != "false",
        include_folders=qp.get("folders") == "true",
        location_id=location_id,
        folder_id=folder_id,
        page=page,
        sort=sort,
        sort_dir=sort_dir,
        cached_total=parse_int(qp.get("cachedTotal"), "cachedTotal", None),
        search_id=qp.get("searchId"),
        focus_file_id=focus_file_id,
    )
    async with read_db() as db:
        if qp.get("mode") == "advanced":
            conditions = parse_conditions_from_params(qp)
            folder_only_fields = {"files"}
            if not common["include_folders"] and any(
                c["field"] in folder_only_fields
                and (c.get("from") or c.get("to"))
                for c in conditions
            ):
                return json_error(
                    "File count filter requires 'Include folders' to be enabled."
                )
            results = await search_files_advanced(db, conditions=conditions, **common)
        else:
            results = await search_files(
                db,
                name=qp.get("name"),
                file_type=qp.get("type"),
                description=qp.get("description"),
                tags=qp.get("tags"),
                size_min=qp.get("sizeMin"),
                size_max=qp.get("sizeMax"),
                date_from=qp.get("dateFrom"),
                date_to=qp.get("dateTo"),
                name_match=qp.get("nameMatch", "anywhere"),
                dupes_only=bool(qp.get("dupes")),
                min_dups=qp.get("minDups"),
                max_dups=qp.get("maxDups"),
                min_files=qp.get("minFiles"),
                max_files=qp.get("maxFiles"),
                hash_strong=qp.get("hash"),
                **common,
            )

    return json_ok(results)


async def list_saved_searches(request: Request):
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, name, params, created_at FROM saved_searches ORDER BY created_at DESC"
        )
    return json_ok([dict(r) for r in rows])


async def create_saved_search(request: Request):
    data = await read_body(request)
    name = parse_str(data.get("name"), "name").strip()
    params = data.get("params")
    if params is not None and not isinstance(params, (dict, str)):
        raise BadRequest("params must be an object or text.")
    if not name or not params:
        return json_error("name and params required")

    async def insert(conn, n, p):
        cursor = await conn.execute(
            "INSERT INTO saved_searches (name, params) VALUES (?, ?)",
            (n, json.dumps(p) if isinstance(p, dict) else str(p)),
        )
        await conn.commit()
        return cursor.lastrowid

    row_id = await execute_write(insert, name, params)
    return json_ok({"id": row_id})


async def delete_saved_search(request: Request):
    search_id = request.path_params["id"]

    async def delete(conn, sid):
        await conn.execute("DELETE FROM saved_searches WHERE id = ?", (sid,))
        await conn.commit()

    await execute_write(delete, search_id)
    return json_ok({})


async def similarity_search(request: Request):
    """POST /api/search/similarity — search by image similarity or text features.

    Uses the LocalLens approach: separate queries per modality, merge candidates,
    score each candidate against both embeddings independently, average the
    cosine similarities for the combined score.
    """
    import numpy as np

    require_chromadb()


    body = await read_body(request)
    text = parse_str(body.get("text"), "text").strip()
    file_id = parse_int(body.get("file_id"), "file_id", None)
    image_data = parse_str(body.get("image_data"), "image_data", None)  # base64
    threshold = parse_float(body.get("threshold"), "threshold", 0.3)
    location_ids = parse_int_array(body.get("location_ids"), "location_ids", None) or None
    # "Search within" — only sent when the checkbox is ticked
    scope_id = (
        parse_node_id(body.get("scopeId"), "scopeId", None)
        if body.get("scopeType")
        else None
    )

    async with read_db() as db:
        scope = await resolve_target(db, scope_id) if scope_id else None
        embed_url = await embedding_url(db)

    text_emb = None
    negative_embs = []
    image_emb = None

    # Text embedding (supports composite syntax: (red socks) - shoes)
    if text:
        try:
            text_emb, negative_embs = await embed_text_query(embed_url, text)
        except ConnectionError:
            return json_error("Embedding service unavailable.", 503)

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
                    try:
                        image_emb = await fetch_embedding(embed_url, image_bytes)
                    except ConnectionError:
                        return json_error("Embedding service unavailable.", 503)

    # Uploaded image — decode base64 and embed
    if image_data and image_emb is None:
        try:
            image_bytes = base64.b64decode(image_data)
            image_emb = await fetch_embedding(embed_url, image_bytes)
        except ConnectionError:
            return json_error("Embedding service unavailable.", 503)
        except Exception as e:
            logger.warning("Uploaded image embedding failed: %s", e)

    if text_emb is None and image_emb is None:
        return json_error("Could not generate embedding for search.", 400)

    collection = get_collection()
    n_results = min(100, collection.count() or 100)
    if n_results == 0:
        return json_ok({"items": [], "total": 0, "folders": [], "page": 0})

    # Build ChromaDB filters. "Search within" a folder passes the folder's
    # embedded images from the catalogue (subfolders included); a location
    # filters on location_id; otherwise the location dropdown applies.
    where_filter = None
    scope_ids = None
    if scope and scope["folder_id"]:
        scope_ids = await scope_embedded_ids(None, scope["folder_id"], ("image",))
        if not scope_ids:
            return json_ok({"items": [], "total": 0, "folders": [], "page": 0})
    elif scope:
        where_filter = {"location_id": scope["location_id"]}
    elif location_ids and len(location_ids) == 1:
        where_filter = {"location_id": location_ids[0]}
    elif location_ids and len(location_ids) > 1:
        where_filter = {"location_id": {"$in": location_ids}}

    # Collect candidates from each modality
    candidates = {}  # doc_id -> stored embedding

    if text_emb is not None:
        results = chroma_query(collection, text_emb, n_results, ["embeddings"],
                                where=where_filter, file_ids=scope_ids)
        for i, doc_id in enumerate(results["ids"][0]):
            if doc_id not in candidates:
                candidates[doc_id] = np.array(results["embeddings"][0][i], dtype=np.float32)

    if image_emb is not None:
        results = chroma_query(collection, image_emb, n_results, ["embeddings"],
                                where=where_filter, file_ids=scope_ids)
        for i, doc_id in enumerate(results["ids"][0]):
            if doc_id not in candidates:
                candidates[doc_id] = np.array(results["embeddings"][0][i], dtype=np.float32)

    if not candidates:
        return json_ok({"items": [], "total": 0, "folders": [], "page": 0})

    # Score each candidate against query embeddings, penalise negatives
    text_vec = np.array(text_emb, dtype=np.float32) if text_emb is not None else None
    image_vec = np.array(image_emb, dtype=np.float32) if image_emb is not None else None
    neg_vecs = [np.array(n, dtype=np.float32) for n in negative_embs]
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

        if score < threshold:
            continue

        # Negative terms demote matching candidates in the ranking
        # but don't filter them out — threshold applies to the
        # positive score only
        for neg_vec in neg_vecs:
            neg_sim = float(np.dot(neg_vec, db_emb))
            if neg_sim > 0:
                score *= (1.0 - neg_sim)

        scored.append((doc_id, score))

    scored.sort(key=lambda x: x[1], reverse=True)

    if not scored:
        return json_ok({"items": [], "total": 0, "folders": [], "page": 0})

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

    # Preserve score ranking order, cap results
    file_map = {r["id"]: dict(r) for r in rows}
    items = []
    for fid in matched_ids:
        if fid in file_map:
            item = file_map[fid]
            item["type"] = "file"
            items.append(item)
    items = items[:100]

    return json_ok({"items": items, "total": len(items), "folders": [], "page": 0})
