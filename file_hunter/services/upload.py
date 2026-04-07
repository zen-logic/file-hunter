"""Upload processing — hash uploaded files, detect duplicates, catalog."""

import logging
from datetime import datetime, timezone

from file_hunter.core import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import hashes_writer, read_hashes
from file_hunter.helpers import post_op_stats
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update as act_update
from file_hunter.services.agent_ops import dispatch, hash_partial_batch
from file_hunter.services.dup_counts import update_dup_counts_inline
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

SMALL_FILE_THRESHOLD = 128 * 1024  # 128KB — hash_partial == hash_fast


async def run_upload(
    location_id: int,
    location_name: str,
    root_path: str,
    folder_id: int | None,
    saved_files: list[dict],
):
    """Background task: hash each saved file, detect duplicates, catalog.

    saved_files: list of { 'filename': str, 'full_path': str, 'rel_path': str }
    """
    total = len(saved_files)
    cataloged = 0
    duplicates = 0
    uploaded_file_ids: list[int] = []
    act_name = f"upload-{location_id}-{id(saved_files)}"

    # Look up agent_id for hash_partial computation
    async with read_db() as db:
        loc_row = await db.execute_fetchall(
            "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
        )
    agent_id = loc_row[0]["agent_id"] if loc_row else None

    register(act_name, f"Uploading: {location_name}", f"0/{total} files")

    await broadcast(
        {
            "type": "upload_started",
            "locationId": location_id,
            "location": location_name,
            "fileCount": total,
        }
    )

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for i, sf in enumerate(saved_files):
        try:
            (hash_fast,) = await fs.file_hash(sf["full_path"], location_id)
            hash_strong = None

            # Compute hash_partial — needed for dup candidate detection
            hash_partial = None
            if agent_id is not None:
                try:
                    hp_result = await hash_partial_batch(agent_id, [sf["full_path"]])
                    for hr in hp_result.get("results", []):
                        if hr.get("path") == sf["full_path"]:
                            hash_partial = hr.get("hash_partial")
                            break
                except Exception:
                    pass  # hash_partial is optional — hash_fast/strong still work

            # Check for existing file with same hash (read)
            async with read_hashes() as hdb:
                hash_rows = await hdb.execute_fetchall(
                    "SELECT file_id FROM active_hashes WHERE hash_fast = ? LIMIT 1",
                    (hash_fast,),
                )
            rows = []
            if hash_rows:
                async with read_db() as db:
                    rows = await db.execute_fetchall(
                        """SELECT f.id, f.filename, f.full_path, l.name as location_name
                           FROM files f
                           JOIN locations l ON l.id = f.location_id
                           WHERE f.id = ?""",
                        (hash_rows[0]["file_id"],),
                    )

            is_duplicate = bool(rows)

            # Always catalog the file — uploads are intentional
            type_high, type_low = classify_file(sf["filename"])
            st = await fs.file_stat(sf["full_path"], location_id)
            if st:
                file_size = st["size"]
            else:
                file_size = 0

            # Use the original modified time from the browser if available,
            # otherwise fall back to the on-disk mtime
            original_mtime = sf.get("mtime")
            if original_mtime:
                modified = datetime.fromtimestamp(
                    original_mtime, tz=timezone.utc
                ).isoformat(timespec="seconds")
            elif st:
                modified = datetime.fromtimestamp(
                    st["mtime"], tz=timezone.utc
                ).isoformat(timespec="seconds")
            else:
                modified = now_iso
            created = modified

            async with db_writer() as wdb:
                await wdb.execute(
                    """INSERT INTO files
                       (filename, full_path, rel_path, location_id, folder_id,
                        file_type_high, file_type_low, file_size,
                        description, tags,
                        created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '',
                               ?, ?, ?, ?, NULL)""",
                    (
                        sf["filename"],
                        sf["full_path"],
                        sf["rel_path"],
                        location_id,
                        folder_id,
                        type_high,
                        type_low,
                        file_size,
                        created,
                        modified,
                        now_iso,
                        now_iso,
                    ),
                )
                cursor_lid = await wdb.execute("SELECT last_insert_rowid()")
                row_lid = await cursor_lid.fetchone()
                file_id = row_lid[0]

            # Register in hashes.db for dup detection
            async with hashes_writer() as hdb:
                await hdb.execute(
                    "INSERT INTO file_hashes "
                    "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                    "VALUES (?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(file_id) DO UPDATE SET "
                    "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
                    "hash_fast=excluded.hash_fast, "
                    "hash_strong=excluded.hash_strong",
                    (
                        file_id,
                        location_id,
                        file_size,
                        hash_partial,
                        hash_fast,
                        hash_strong,
                    ),
                )

            uploaded_file_ids.append(file_id)

            if is_duplicate:
                existing = dict(rows[0])
                duplicates += 1
                await broadcast(
                    {
                        "type": "upload_duplicate",
                        "locationId": location_id,
                        "filename": sf["filename"],
                        "existingLocation": existing["location_name"],
                        "existingPath": existing["full_path"],
                    }
                )
            else:
                cataloged += 1

            # Broadcast so the file list can show the new file immediately
            await broadcast(
                {
                    "type": "file_added",
                    "locationId": location_id,
                    "folderId": folder_id,
                    "file": {
                        "id": file_id,
                        "name": sf["filename"],
                        "typeHigh": type_high,
                        "typeLow": type_low,
                        "size": file_size,
                        "date": modified,
                        "dups": 0,
                        "hashStrong": hash_strong,
                        "hashFast": hash_fast,
                        "stale": False,
                        "missing": False,
                        "hidden": bool(sf["filename"].startswith(".")),
                    },
                }
            )

            # Stats: new file added
            is_hidden = 1 if sf["filename"].startswith(".") else 0
            await update_stats_for_files(
                location_id,
                added=[(folder_id, file_size, type_high, is_hidden)],
            )

            act_update(act_name, progress=f"{i + 1}/{total} {sf['filename']}")
            await broadcast(
                {
                    "type": "upload_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "processed": i + 1,
                    "total": total,
                    "cataloged": cataloged,
                    "duplicates": duplicates,
                    "currentFile": sf["filename"],
                }
            )

        except Exception as exc:
            await broadcast(
                {
                    "type": "upload_file_error",
                    "locationId": location_id,
                    "filename": sf["filename"],
                    "error": str(exc),
                }
            )

    # Check for dup candidates before completing — keeps the upload activity
    # alive in the UI so the user sees work is still happening
    if uploaded_file_ids:
        try:
            act_update(act_name, progress="checking duplicates...")
            await broadcast({
                "type": "upload_progress",
                "locationId": location_id,
                "location": location_name,
                "processed": total,
                "total": total,
                "cataloged": cataloged,
                "duplicates": duplicates,
                "currentFile": "checking duplicates...",
            })
            dup_result = await _check_dup_candidates(
                uploaded_file_ids, location_id, location_name
            )
            duplicates += dup_result
        except Exception:
            log.exception("Upload dup candidate check failed")

    await broadcast(
        {
            "type": "upload_completed",
            "locationId": location_id,
            "location": location_name,
            "total": total,
            "cataloged": cataloged,
            "duplicates": duplicates,
        }
    )
    unregister(act_name)
    await post_op_stats()

    try:
        await recalculate_location_sizes(location_id)
    except Exception:
        pass


async def _check_dup_candidates(
    uploaded_file_ids: list[int],
    location_id: int,
    location_name: str,
) -> int:
    """Find files sharing hash_partial+size with uploaded files and hash them
    so duplicate detection works cross-agent.

    Uploaded files already have hash_partial and hash_fast. Other files in the
    catalog may only have hash_partial. This function finds those candidates
    across all agents, hashes them immediately via their agent, recounts
    dup_count, and broadcasts so the UI updates badges in-place.

    Called before upload_completed so the upload activity indicator stays alive.

    Returns the number of new duplicates confirmed.
    """
    # Get hash_partial + file_size for each uploaded file
    async with read_hashes() as hdb:
        ph = ",".join("?" for _ in uploaded_file_ids)
        uploaded_rows = await hdb.execute_fetchall(
            f"SELECT file_id, hash_partial, hash_fast, file_size "
            f"FROM file_hashes WHERE file_id IN ({ph})",
            uploaded_file_ids,
        )

    if not uploaded_rows:
        return 0

    # Build set of (hash_partial, file_size) pairs to search for candidates
    pairs = set()
    all_fast_hashes: set[str] = set()
    for r in uploaded_rows:
        if r["hash_partial"] and r["file_size"] and r["file_size"] > 0:
            pairs.add((r["hash_partial"], r["file_size"]))
        if r["hash_fast"]:
            all_fast_hashes.add(r["hash_fast"])

    if not pairs:
        await update_dup_counts_inline(all_fast_hashes)
        return 0

    # Find candidate files: same hash_partial + size, no hash_fast yet
    candidates = []
    for hp, fsize in pairs:
        async with read_hashes() as hdb:
            rows = await hdb.execute_fetchall(
                "SELECT file_id, location_id, file_size, hash_partial "
                "FROM file_hashes "
                "WHERE hash_partial = ? AND file_size = ? "
                "AND hash_fast IS NULL "
                "AND excluded = 0 AND stale = 0",
                (hp, fsize),
            )
        candidates.extend(rows)

    if not candidates:
        log.info("Upload dup check: no unmatched candidates found")
        await update_dup_counts_inline(all_fast_hashes)
        return 0

    log.info(
        "Upload dup check: %d candidates need hash_fast across %d locations",
        len(candidates),
        len({c["location_id"] for c in candidates}),
    )

    # Get full_path for each candidate from catalog
    cand_fids = [c["file_id"] for c in candidates]
    file_paths: dict[int, str] = {}
    async with read_db() as db:
        for i in range(0, len(cand_fids), 500):
            batch = cand_fids[i : i + 500]
            fph = ",".join("?" for _ in batch)
            rows = await db.execute_fetchall(
                f"SELECT id, full_path FROM files "
                f"WHERE id IN ({fph}) AND stale = 0",
                batch,
            )
            for r in rows:
                file_paths[r["id"]] = r["full_path"]

    # Hash each candidate via its agent
    small = [c for c in candidates if c["file_size"] <= SMALL_FILE_THRESHOLD]
    large = [c for c in candidates if c["file_size"] > SMALL_FILE_THRESHOLD]
    hashed = 0

    # Small files: promote hash_partial → hash_fast (no agent call needed)
    if small:
        async with hashes_writer() as wdb:
            for c in small:
                await wdb.execute(
                    "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
                    (c["hash_partial"], c["file_id"]),
                )
                all_fast_hashes.add(c["hash_partial"])
                hashed += 1
        log.info("Upload dup check: %d small files promoted", len(small))

    # Large files: dispatch hash to the correct agent now
    for c in large:
        fid = c["file_id"]
        full_path = file_paths.get(fid)
        if not full_path:
            continue

        try:
            result = await dispatch(
                "file_hash", c["location_id"], path=full_path
            )
            hash_fast = result["hash_fast"]
            async with hashes_writer() as wdb:
                await wdb.execute(
                    "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
                    (hash_fast, fid),
                )
            all_fast_hashes.add(hash_fast)
            hashed += 1
        except Exception:
            log.warning(
                "Upload dup check: failed to hash file %d at %s",
                fid, full_path,
            )

    log.info("Upload dup check: %d/%d candidates hashed", hashed, len(candidates))

    # Recount dup_count for all affected hash_fast values
    affected_locs = await update_dup_counts_inline(all_fast_hashes)
    affected_locs.add(location_id)

    # Tell the UI to update dup badges in-place
    await broadcast({
        "type": "dup_recalc_completed",
        "hashCount": len(all_fast_hashes),
        "locationIds": list(affected_locs),
    })

    return hashed
