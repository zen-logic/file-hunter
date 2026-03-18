"""Server-driven scan orchestrator.

Consumes a streamed TSV tree from the agent's /tree endpoint. Each
directory arrives as a complete chunk (D record + F records, inode-sorted
with partial hashes). The server reconciles each directory against the
catalog, upserts new/changed files, marks gone files stale, and inserts
dup candidates into the pending_hashes table for concurrent hash_fast
processing.

All writes go through db_writer() — the single write connection
serialized by asyncio.Lock. No SQLite lock contention, no timeouts.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone

import httpx

from file_hunter.db import db_writer, get_db
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    upsert_file,
    mark_stale_files,
)
from file_hunter.services.agent_ops import stream_tree
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast
from file_hunter_core.classify import classify_file

logger = logging.getLogger("file_hunter")


async def run_scan(op_id: int, agent_id: int, params: dict):
    """Execute a scan operation by consuming the agent's /tree stream.

    Called by queue_manager as the "scan_dir" handler.

    params:
        location_id: int
        path: str -- root_path or scan subfolder
        root_path: str -- location root for rel_path computation
    """
    location_id = params["location_id"]
    scan_path = params.get("path") or params["root_path"]
    root_path = params["root_path"]

    read_db = await get_db()

    # --- Initialisation ---
    async with db_writer() as db:
        loc_row = await db.execute_fetchall(
            "SELECT name, file_count, total_size, type_counts "
            "FROM locations WHERE id = ?",
            (location_id,),
        )
        location_name = loc_row[0]["name"] if loc_row else f"Location #{location_id}"
        running_file_count = (loc_row[0]["file_count"] or 0) if loc_row else 0
        running_total_size = (loc_row[0]["total_size"] or 0) if loc_row else 0
        running_type_counts = (
            json.loads(loc_row[0]["type_counts"] or "{}") if loc_row else {}
        )

        now_iso = _now()
        saved_scan_id = params.get("scan_id")
        if saved_scan_id:
            scan_id = saved_scan_id
            await db.execute(
                "UPDATE scans SET status = 'running' WHERE id = ?",
                (scan_id,),
            )
            logger.info(
                "Resuming scan #%d for location #%d (%s)",
                scan_id,
                location_id,
                location_name,
            )
        else:
            cursor = await db.execute(
                "INSERT INTO scans (location_id, status, started_at) "
                "VALUES (?, 'running', ?)",
                (location_id, now_iso),
            )
            scan_id = cursor.lastrowid

    # Scan prefix for stale marking (subfolder scans only)
    scan_prefix = None
    prefix_for_agent = None
    if scan_path != root_path:
        scan_prefix = os.path.relpath(scan_path, root_path)
        prefix_for_agent = scan_prefix

    # State
    folder_cache: dict[str, tuple] = {}
    files_found = 0
    files_new = 0
    dirs_processed = 0
    last_progress_broadcast = time.monotonic()

    # Mutable container for running counters (shared with _process_directory)
    counters = {
        "file_count": running_file_count,
        "total_size": running_total_size,
        "type_counts": running_type_counts,
    }

    await broadcast(
        {
            "type": "scan_started",
            "locationId": location_id,
            "location": location_name,
            "scanId": scan_id,
        }
    )

    # Save scan_id to params for crash recovery
    params["scan_id"] = scan_id
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET params = ? WHERE id = ?",
            (json.dumps(params), op_id),
        )

    # Launch hash drainer as concurrent task
    scan_done_event = asyncio.Event()
    drainer_task = asyncio.create_task(
        _drain_pending_hashes(agent_id, location_id, location_name, scan_done_event)
    )

    try:
        # --- Stream: metadata phase then hash phase ---
        current_rel_dir: str | None = None
        current_files: list[dict] = []
        hashes_applied = 0
        hashes_total = 0

        async for record in stream_tree(agent_id, root_path, prefix=prefix_for_agent):
            # Checkpoint: block while queue is paused
            from file_hunter.services.queue_manager import wait_if_paused

            await wait_if_paused()

            rtype = record.get("type")

            if rtype == "dir":
                # Process the previous directory (if any)
                if current_rel_dir is not None:
                    df, dn = await _process_directory(
                        current_rel_dir,
                        current_files,
                        location_id,
                        agent_id,
                        scan_id,
                        root_path,
                        now_iso,
                        folder_cache,
                        counters,
                    )
                    files_found += df
                    files_new += dn
                    dirs_processed += 1

                    # Broadcast progress periodically (not every directory)
                    now_mono = time.monotonic()
                    if now_mono - last_progress_broadcast >= 2.0:
                        await read_db.commit()
                        await _broadcast_progress(
                            read_db,
                            location_id,
                            location_name,
                            files_found,
                            files_new,
                            dirs_processed,
                        )
                        last_progress_broadcast = now_mono

                # Start new directory
                current_rel_dir = record["rel_dir"]
                current_files = []

            elif rtype == "file":
                current_files.append(record)

            elif rtype == "phase":
                # Flush final metadata directory before switching to hash phase
                if current_rel_dir is not None:
                    df, dn = await _process_directory(
                        current_rel_dir,
                        current_files,
                        location_id,
                        agent_id,
                        scan_id,
                        root_path,
                        now_iso,
                        folder_cache,
                        counters,
                    )
                    files_found += df
                    files_new += dn
                    dirs_processed += 1
                    current_rel_dir = None
                    current_files = []

                hashes_total = record.get("total", 0)
                logger.info(
                    "Scan hash phase: %d files to hash for %s",
                    hashes_total,
                    location_name,
                )
                await broadcast(
                    {
                        "type": "scan_progress",
                        "locationId": location_id,
                        "location": location_name,
                        "phase": "hashing_partial",
                        "filesFound": files_found,
                        "filesHashed": 0,
                        "filesToHash": hashes_total,
                    }
                )

            elif rtype == "hash":
                # Apply partial hash to existing file record + dup candidate check
                await _apply_hash(
                    record["rel_path"],
                    record["hash_partial"],
                    location_id,
                    agent_id,
                    root_path,
                    now_iso,
                )
                hashes_applied += 1

                # Broadcast hash progress periodically
                now_mono = time.monotonic()
                if now_mono - last_progress_broadcast >= 2.0:
                    await broadcast(
                        {
                            "type": "scan_progress",
                            "locationId": location_id,
                            "location": location_name,
                            "phase": "hashing_partial",
                            "filesFound": files_found,
                            "filesHashed": hashes_applied,
                            "filesToHash": hashes_total,
                        }
                    )
                    last_progress_broadcast = now_mono

            elif rtype == "end":
                pass  # Final record — proceed to finalization

        # --- Finalization ---
        await broadcast(
            {
                "type": "scan_finalizing",
                "locationId": location_id,
                "location": location_name,
            }
        )

        async with db_writer() as db:
            stale_count = await mark_stale_files(
                db, location_id, scan_id, scan_prefix
            )
            completed_iso = _now()
            await db.execute(
                "UPDATE scans SET status = 'completed', completed_at = ?, "
                "files_found = ?, stale_files = ? WHERE id = ?",
                (completed_iso, files_found, stale_count, scan_id),
            )
            await db.execute(
                "UPDATE locations SET date_last_scanned = ? WHERE id = ?",
                (completed_iso, location_id),
            )

        logger.info(
            "Scan completed: location #%d (%s), %d files found, %d new, %d stale",
            location_id,
            location_name,
            files_found,
            files_new,
            stale_count,
        )

        # Signal hash drainer that no more entries are coming, wait for it to drain
        scan_done_event.set()
        logger.info("Waiting for hash drainer to finish for %s", location_name)
        try:
            await drainer_task
        except asyncio.CancelledError:
            pass
        logger.info("Hash drainer finished for %s", location_name)

        # --- Correction pass: dup recount + size recalc ---
        from file_hunter.services.dup_counts import full_dup_recount
        from file_hunter.services.sizes import recalculate_location_sizes

        logger.info("Running dup recount for %s", location_name)
        await full_dup_recount(location_id=location_id)

        logger.info("Running size recalc for %s", location_name)
        await recalculate_location_sizes(location_id)

        invalidate_stats_cache()

        await read_db.commit()
        dup_row = await read_db.execute_fetchall(
            "SELECT duplicate_count FROM locations WHERE id = ?", (location_id,)
        )
        final_dup_count = (dup_row[0]["duplicate_count"] or 0) if dup_row else 0

        await broadcast(
            {
                "type": "scan_completed",
                "locationId": location_id,
                "location": location_name,
                "filesFound": files_found,
                "filesHashed": files_found,
                "filesNew": files_new,
                "staleFiles": stale_count,
                "duplicatesFound": final_dup_count,
            }
        )

    except asyncio.CancelledError:
        drainer_task.cancel()
        completed_iso = _now()
        async with db_writer() as db:
            await db.execute(
                "UPDATE scans SET status = 'cancelled', completed_at = ? WHERE id = ?",
                (completed_iso, scan_id),
            )
        invalidate_stats_cache()
        await broadcast(
            {
                "type": "scan_cancelled",
                "locationId": location_id,
                "location": location_name,
                "filesHashed": files_found,
                "filesFound": files_found,
            }
        )
        logger.info("Scan cancelled: location #%d (%s)", location_id, location_name)
        raise

    except (ConnectionError, OSError, httpx.ConnectError) as e:
        drainer_task.cancel()
        # Agent disconnected — save scan_id for resumption
        async with db_writer() as db:
            params["scan_id"] = scan_id
            await db.execute(
                "UPDATE operation_queue SET params = ? WHERE id = ?",
                (json.dumps(params), op_id),
            )
            await db.execute(
                "UPDATE scans SET status = 'interrupted' WHERE id = ?",
                (scan_id,),
            )

        logger.warning(
            "Scan interrupted: location #%d (%s): %s — scan_id=%d saved",
            location_id,
            location_name,
            e,
            scan_id,
        )
        await broadcast(
            {
                "type": "scan_interrupted",
                "locationId": location_id,
                "location": location_name,
            }
        )
        raise

    except Exception as e:
        drainer_task.cancel()
        try:
            async with db_writer() as db:
                await db.execute(
                    "UPDATE scans SET status = 'error', error = ?, "
                    "completed_at = ? WHERE id = ?",
                    (str(e), _now(), scan_id),
                )
        except Exception:
            pass
        invalidate_stats_cache()
        await broadcast(
            {
                "type": "scan_error",
                "locationId": location_id,
                "location": location_name,
                "error": str(e),
            }
        )
        logger.error(
            "Scan failed: location #%d (%s): %s",
            location_id,
            location_name,
            e,
            exc_info=True,
        )
        raise


async def _process_directory(
    rel_dir: str,
    agent_files: list[dict],
    location_id: int,
    agent_id: int,
    scan_id: int,
    root_path: str,
    now_iso: str,
    folder_cache: dict,
    counters: dict,
) -> tuple[int, int]:
    """Reconcile one directory's files against the catalog.

    counters is a mutable dict with keys: file_count, total_size, type_counts.
    Updated in place so the caller sees changes.

    Returns (files_in_dir, new_files_in_dir).
    """
    read_db = await get_db()

    # Build set of agent files keyed by rel_path
    agent_by_path: dict[str, dict] = {}
    for f in agent_files:
        agent_by_path[f["rel_path"]] = f

    # Query expected files from catalog for this directory
    expected = await _get_expected_files(read_db, location_id, rel_dir)
    expected_by_path = {e["rel_path"]: e for e in expected}

    # Classify: unchanged, changed, new, gone
    unchanged_paths = []
    changed: list[dict] = []
    new: list[dict] = []
    gone_paths = []

    for rp, af in agent_by_path.items():
        old = expected_by_path.get(rp)
        if old is None:
            new.append(af)
        elif af["size"] != old["file_size"] or af["mtime"] != old["modified_date"]:
            changed.append(af)
        else:
            unchanged_paths.append(rp)

    for rp in expected_by_path:
        if rp not in agent_by_path:
            gone_paths.append(rp)

    # --- Counter deltas ---
    delta_count = len(new) - len(gone_paths)
    delta_size = 0
    delta_types: dict[str, int] = {}

    for f in new:
        delta_size += f.get("size", 0)
        ft = classify_file(f["rel_path"])[0]
        delta_types[ft] = delta_types.get(ft, 0) + 1

    for rp in gone_paths:
        old = expected_by_path[rp]
        delta_size -= old["file_size"]
        ft = old.get("file_type_high", "") or ""
        delta_types[ft] = delta_types.get(ft, 0) - 1

    for f in changed:
        old = expected_by_path[f["rel_path"]]
        delta_size += f.get("size", 0) - old["file_size"]
        old_ft = old.get("file_type_high", "") or ""
        new_ft = classify_file(f["rel_path"])[0]
        if old_ft != new_ft:
            delta_types[old_ft] = delta_types.get(old_ft, 0) - 1
            delta_types[new_ft] = delta_types.get(new_ft, 0) + 1

    # --- DB writes ---
    dir_affected_strong: set[str] = set()

    # Unchanged — batch update scan_id
    if unchanged_paths:
        for i in range(0, len(unchanged_paths), 500):
            batch = unchanged_paths[i : i + 500]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                await db.execute(
                    f"UPDATE files SET date_last_seen = ?, scan_id = ? "
                    f"WHERE location_id = ? AND stale = 0 "
                    f"AND rel_path IN ({ph})",
                    [now_iso, scan_id, location_id] + batch,
                )
            await asyncio.sleep(0)

    # Gone — collect affected hashes, mark stale
    if gone_paths:
        for i in range(0, len(gone_paths), 500):
            batch = gone_paths[i : i + 500]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                hash_rows = await db.execute_fetchall(
                    f"SELECT DISTINCT hash_strong, hash_fast FROM files "
                    f"WHERE location_id = ? AND rel_path IN ({ph}) "
                    f"AND (hash_strong IS NOT NULL OR hash_fast IS NOT NULL)",
                    [location_id] + batch,
                )
                for hr in hash_rows:
                    if hr["hash_strong"]:
                        dir_affected_strong.add(hr["hash_strong"])
                    elif hr["hash_fast"]:
                        dir_affected_fast.add(hr["hash_fast"])
                await db.execute(
                    f"UPDATE files SET stale = 1 "
                    f"WHERE location_id = ? AND rel_path IN ({ph})",
                    [location_id] + batch,
                )
            await asyncio.sleep(0)

    # Changed — invalidate hashes if size changed, then upsert
    # New — upsert directly
    all_upserts = [("changed", f) for f in changed] + [("new", f) for f in new]

    for i in range(0, len(all_upserts), 500):
        batch = all_upserts[i : i + 500]
        async with db_writer() as db:
            for kind, f in batch:
                rel_path = f["rel_path"]
                file_type_high, file_type_low = classify_file(rel_path)
                filename = os.path.basename(rel_path)
                full_path = os.path.join(root_path, rel_path)
                is_hidden = 1 if filename.startswith(".") else 0
                inode = f.get("inode", 0)

                if kind == "changed":
                    old_row = await db.execute_fetchall(
                        "SELECT file_size, hash_strong FROM files "
                        "WHERE location_id = ? AND rel_path = ?",
                        (location_id, rel_path),
                    )
                    size_changed = old_row and old_row[0]["file_size"] != f.get("size")
                    if size_changed:
                        await db.execute(
                            "UPDATE files SET hash_fast = NULL, hash_strong = NULL "
                            "WHERE location_id = ? AND rel_path = ?",
                            (location_id, rel_path),
                        )
                        old_hs = old_row[0]["hash_strong"]
                        if old_hs:
                            dir_affected_strong.add(old_hs)

                folder_id = None
                dup_exclude = 0
                if rel_dir:
                    folder_id, dup_exclude = await ensure_folder_hierarchy(
                        db, location_id, rel_dir, folder_cache
                    )

                await upsert_file(
                    db,
                    location_id=location_id,
                    scan_id=scan_id,
                    filename=filename,
                    full_path=full_path,
                    rel_path=rel_path,
                    folder_id=folder_id,
                    file_size=f.get("size", 0),
                    created_date=f.get("ctime", ""),
                    modified_date=f.get("mtime", ""),
                    file_type_high=file_type_high,
                    file_type_low=file_type_low,
                    hash_partial=None,
                    hash_fast=None,
                    hash_strong=None,
                    now_iso=now_iso,
                    hidden=is_hidden,
                    dup_exclude=dup_exclude,
                    inode=inode,
                )

        await asyncio.sleep(0)

    # Submit affected hashes to dup recalc (gone/changed files)
    if dir_affected_strong:
        from file_hunter.services.dup_counts import submit_hashes_for_recalc

        submit_hashes_for_recalc(
            strong_hashes=dir_affected_strong,
            fast_hashes=None,
            source="scan dir",
            location_ids={location_id},
        )

    # Update folder chain counters
    async with db_writer() as db:
        if rel_dir:
            await _update_folder_chain(
                db, folder_cache, rel_dir,
                delta_count, delta_size, 0, delta_types,
            )

        # Update location counters (mutate shared counters dict)
        counters["file_count"] = max(0, counters["file_count"] + delta_count)
        counters["total_size"] = max(0, counters["total_size"] + delta_size)
        tc = counters["type_counts"]
        for ft, d in delta_types.items():
            tc[ft] = tc.get(ft, 0) + d
        clean_tc = {k: v for k, v in tc.items() if v > 0}
        tc.clear()
        tc.update(clean_tc)

        await db.execute(
            "UPDATE locations SET file_count = ?, total_size = ?, "
            "type_counts = ? WHERE id = ?",
            (
                counters["file_count"],
                counters["total_size"],
                json.dumps(tc),
                location_id,
            ),
        )

    dir_file_count = len(unchanged_paths) + len(changed) + len(new)
    return dir_file_count, len(new)


async def _apply_hash(
    rel_path: str,
    hash_partial: str,
    location_id: int,
    agent_id: int,
    root_path: str,
    now_iso: str,
):
    """Apply a partial hash to an existing file record and check for dup candidates.

    Called during the hash phase of the scan stream. The file already exists
    in the catalog from the metadata phase.
    """
    read_db = await get_db()

    # Look up the file
    row = await read_db.execute_fetchall(
        "SELECT id, file_size, inode FROM files "
        "WHERE location_id = ? AND rel_path = ? AND stale = 0",
        (location_id, rel_path),
    )
    if not row:
        return

    file_id = row[0]["id"]
    file_size = row[0]["file_size"]
    inode = row[0]["inode"]
    full_path = os.path.join(root_path, rel_path)

    # Write hash_partial
    async with db_writer() as db:
        await db.execute(
            "UPDATE files SET hash_partial = ? WHERE id = ?",
            (hash_partial, file_id),
        )

        # Dup candidate check
        dup_match = await db.execute_fetchall(
            "SELECT 1 FROM files "
            "WHERE file_size = ? AND hash_partial = ? "
            "AND id != ? AND stale = 0 LIMIT 1",
            (file_size, hash_partial, file_id),
        )
        if dup_match:
            if file_size <= 131072:
                # Small file: hash_partial == hash_fast
                await db.execute(
                    "UPDATE files SET hash_fast = ? WHERE id = ?",
                    (hash_partial, file_id),
                )
                from file_hunter.services.dup_counts import submit_hashes_for_recalc

                submit_hashes_for_recalc(
                    strong_hashes=None,
                    fast_hashes={hash_partial},
                    source="scan hash",
                    location_ids={location_id},
                )
            else:
                # Large file: queue for hash_fast via agent
                await db.execute(
                    "INSERT INTO pending_hashes "
                    "(file_id, location_id, agent_id, full_path, inode, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (file_id, location_id, agent_id, full_path, inode, now_iso),
                )


async def _get_expected_files(
    db, location_id: int, rel_dir: str
) -> list[dict]:
    """Query catalog for non-stale files in a specific directory.

    Returns list of {rel_path, file_size, modified_date, file_type_high}.
    """
    if rel_dir == "":
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date, file_type_high "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id IS NULL",
            (location_id,),
        )
    else:
        folder_row = await db.execute_fetchall(
            "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, rel_dir),
        )
        if not folder_row:
            return []
        folder_id = folder_row[0]["id"]
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date, file_type_high "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id = ?",
            (location_id, folder_id),
        )

    return [
        {
            "rel_path": r["rel_path"],
            "file_size": r["file_size"],
            "modified_date": r["modified_date"],
            "file_type_high": r["file_type_high"] or "",
        }
        for r in rows
    ]


async def _update_folder_chain(
    db, folder_cache, rel_dir, delta_count, delta_size, dup_delta, delta_types
):
    """Apply counter deltas to the current folder and all ancestors."""
    if not rel_dir:
        return

    parts = rel_dir.replace("\\", "/").split("/")
    folder_ids = []
    for i in range(len(parts)):
        ancestor_path = "/".join(parts[: i + 1])
        entry = folder_cache.get(ancestor_path)
        if entry:
            folder_ids.append(entry[0])

    if not folder_ids:
        return

    ph = ",".join("?" for _ in folder_ids)
    rows = await db.execute_fetchall(
        f"SELECT id, type_counts FROM folders WHERE id IN ({ph})",
        folder_ids,
    )
    tc_map = {r["id"]: json.loads(r["type_counts"] or "{}") for r in rows}

    for fid in folder_ids:
        current_tc = tc_map.get(fid, {})
        for ft, d in delta_types.items():
            current_tc[ft] = current_tc.get(ft, 0) + d
        current_tc = {k: v for k, v in current_tc.items() if v > 0}

        await db.execute(
            "UPDATE folders SET "
            "file_count = MAX(0, COALESCE(file_count, 0) + ?), "
            "total_size = MAX(0, COALESCE(total_size, 0) + ?), "
            "duplicate_count = MAX(0, COALESCE(duplicate_count, 0) + ?), "
            "type_counts = ? "
            "WHERE id = ?",
            (delta_count, delta_size, dup_delta, json.dumps(current_tc), fid),
        )


async def _broadcast_progress(
    db,
    location_id: int,
    location_name: str,
    files_found: int,
    files_new: int,
    dirs_processed: int,
):
    """Broadcast scan progress with live counters from the DB."""
    loc_rows = await db.execute_fetchall(
        "SELECT id, file_count, total_size, duplicate_count, type_counts "
        "FROM locations",
    )
    global_file_count = 0
    global_total_size = 0
    global_dup_count = 0
    loc_file_count = 0
    loc_total_size = 0
    loc_dup_count = 0
    loc_tc = {}
    global_types: dict[str, int] = {}
    for row in loc_rows:
        fc = row["file_count"] or 0
        ts = row["total_size"] or 0
        dc = row["duplicate_count"] or 0
        global_file_count += fc
        global_total_size += ts
        global_dup_count += dc
        tc = json.loads(row["type_counts"] or "{}")
        for t, c in tc.items():
            global_types[t] = global_types.get(t, 0) + c
        if row["id"] == location_id:
            loc_file_count = fc
            loc_total_size = ts
            loc_dup_count = dc
            loc_tc = tc

    type_breakdown = [
        {"type": t, "count": c} for t, c in sorted(loc_tc.items(), key=lambda x: -x[1])
    ]
    global_type_breakdown = [
        {"type": t, "count": c}
        for t, c in sorted(global_types.items(), key=lambda x: -x[1])
    ]

    await broadcast(
        {
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "filesFound": files_found,
            "filesHashed": files_found,
            "filesNew": files_new,
            "dirsProcessed": dirs_processed,
            "fileCount": loc_file_count,
            "totalSize": loc_total_size,
            "duplicateCount": loc_dup_count,
            "globalFileCount": global_file_count,
            "globalTotalSize": global_total_size,
            "globalDuplicateCount": global_dup_count,
            "typeBreakdown": type_breakdown,
            "globalTypeBreakdown": global_type_breakdown,
        }
    )


async def _drain_pending_hashes(
    agent_id: int, location_id: int, location_name: str,
    scan_done: asyncio.Event,
):
    """Background task: drain pending_hashes by sending batches to the agent.

    Runs concurrently with the scan on a separate HTTP connection to the agent.
    Polls the pending_hashes table, sends inode-sorted batches to the agent's
    /files/hash-batch endpoint, writes results back, and submits to the
    coalesced dup recalc writer.

    Exits when scan_done is set AND the table is empty.
    """
    from file_hunter.services.agent_ops import hash_fast_batch
    from file_hunter.services.dup_counts import submit_hashes_for_recalc

    BATCH_SIZE = 200

    try:
        while True:
            # Fetch a batch of pending hashes for this agent/location
            read_db = await get_db()
            await read_db.commit()  # refresh snapshot
            rows = await read_db.execute_fetchall(
                "SELECT id, file_id, full_path, inode FROM pending_hashes "
                "WHERE agent_id = ? AND location_id = ? "
                "ORDER BY inode "
                "LIMIT ?",
                (agent_id, location_id, BATCH_SIZE),
            )

            if not rows:
                if scan_done.is_set():
                    # Scan finished and nothing left to drain
                    logger.info("Hash drainer: done for %s", location_name)
                    return
                # Scan still running — wait and check again
                await asyncio.sleep(2)
                continue

            # Build path list and mappings
            paths = [r["full_path"] for r in rows]
            path_to_file_id = {r["full_path"]: r["file_id"] for r in rows}
            pending_ids = [r["id"] for r in rows]

            # Call agent for hash_fast
            try:
                result = await hash_fast_batch(agent_id, paths)
            except (ConnectionError, OSError, httpx.ConnectError):
                # Agent went offline — stop draining, entries persist for later
                logger.warning(
                    "Hash drainer: agent %d offline, stopping", agent_id
                )
                return

            # Write results
            hash_results = result.get("results", [])
            affected_fast: set[str] = set()

            if hash_results:
                async with db_writer() as db:
                    for hr in hash_results:
                        fid = path_to_file_id.get(hr["path"])
                        hf = hr.get("hash_fast")
                        if fid and hf:
                            await db.execute(
                                "UPDATE files SET hash_fast = ? WHERE id = ?",
                                (hf, fid),
                            )
                            affected_fast.add(hf)

            # Delete processed entries
            if pending_ids:
                for i in range(0, len(pending_ids), 500):
                    batch = pending_ids[i : i + 500]
                    ph = ",".join("?" for _ in batch)
                    async with db_writer() as db:
                        await db.execute(
                            f"DELETE FROM pending_hashes WHERE id IN ({ph})",
                            batch,
                        )

            # Submit to dup recalc
            if affected_fast:
                submit_hashes_for_recalc(
                    strong_hashes=None,
                    fast_hashes=affected_fast,
                    source=f"hash drainer {location_name}",
                    location_ids={location_id},
                )

            logger.info(
                "Hash drainer: %d hashed for %s, %d remaining",
                len(hash_results),
                location_name,
                max(0, len(rows) - len(hash_results)),
            )

            # Yield to event loop
            await asyncio.sleep(0)

    except asyncio.CancelledError:
        # Normal shutdown — drain remaining entries on next scan or startup
        return


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
