"""Server-driven scan orchestrator.

Drives BFS traversal of a location by calling the agent's /reconcile
endpoint per directory.  Uses scanner.py's proven DB functions for all
catalog writes — no reimplementation of folder hierarchy, file upsert,
or stale marking.

All writes go through db_writer() — the single write connection
serialized by asyncio.Lock. No SQLite lock contention, no timeouts.
"""

import asyncio
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

import httpx

from file_hunter.db import db_writer, get_db
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    upsert_file,
    mark_stale_files,
)
from file_hunter.services.agent_ops import reconcile_directory, dispatch
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def run_scan(op_id: int, agent_id: int, params: dict):
    """Execute a server-driven scan operation.

    Called by queue_manager as the "scan_dir" handler.

    params:
        location_id: int
        path: str -- root_path or scan subfolder
        root_path: str -- location root for rel_path computation
        traversal: list[str] | None -- saved traversal state for resumption
    """
    location_id = params["location_id"]
    scan_path = params.get("path") or params["root_path"]
    root_path = params["root_path"]
    saved_traversal = params.get("traversal")

    read_db = await get_db()

    # --- Initialisation (read location info, create/resume scan record) ---
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
        if saved_scan_id and saved_traversal:
            scan_id = saved_scan_id
            await db.execute(
                "UPDATE scans SET status = 'running' WHERE id = ?",
                (scan_id,),
            )
            logger.info(
                "Resuming scan #%d for location #%d (%s), %d dirs remaining",
                scan_id,
                location_id,
                location_name,
                len(saved_traversal),
            )
        else:
            cursor = await db.execute(
                "INSERT INTO scans (location_id, status, started_at) "
                "VALUES (?, 'running', ?)",
                (location_id, now_iso),
            )
            scan_id = cursor.lastrowid

    # Scan prefix for stale marking
    scan_prefix = None
    if scan_path != root_path:
        scan_prefix = os.path.relpath(scan_path, root_path)

    # State (restore counters on resume)
    folder_cache: dict[str, tuple] = {}
    files_found = params.get("files_found", 0) if saved_traversal else 0
    files_new = params.get("files_new", 0) if saved_traversal else 0
    dirs_processed = 0
    last_stats_update = time.monotonic()
    last_state_save = time.monotonic()

    # BFS traversal queue
    dirs_to_visit: deque[str] = deque(
        saved_traversal if saved_traversal else [scan_path]
    )

    await broadcast(
        {
            "type": "scan_started",
            "locationId": location_id,
            "location": location_name,
            "scanId": scan_id,
        }
    )

    # Refresh read snapshot so _broadcast_progress sees the scan record
    await read_db.commit()
    await _broadcast_progress(
        read_db,
        location_id,
        location_name,
        files_found,
        files_new,
        0,
        len(dirs_to_visit),
    )

    try:
        while dirs_to_visit:
            # Checkpoint: block here while queue is paused (e.g. during import)
            from file_hunter.services.queue_manager import wait_if_paused
            await wait_if_paused()

            current_dir = dirs_to_visit.popleft()
            dir_affected_strong: set[str] = set()
            dir_affected_fast: set[str] = set()

            # --- READ: expected files from catalog ---
            expected = await _get_expected_files(
                read_db, location_id, current_dir, root_path
            )
            expected_by_path = {e["rel_path"]: e for e in expected}

            # --- PAGINATED RECONCILE ---
            reconcile_cursor = 0  # first page (signals "paginate please")
            is_first_page = True
            gone = []
            delta_count = 0
            delta_size = 0
            delta_types: dict[str, int] = {}

            while True:
                result = await reconcile_directory(
                    agent_id,
                    current_dir,
                    root_path,
                    expected,
                    cursor=reconcile_cursor,
                )

                # First page: subdirs, unchanged, gone
                if is_first_page:
                    for subdir in sorted(result.get("subdirs", [])):
                        dirs_to_visit.append(os.path.join(current_dir, subdir))

                    # Unchanged — batched in 500s with yields
                    unchanged = result.get("unchanged", [])
                    if unchanged:
                        for i in range(0, len(unchanged), 500):
                            batch = unchanged[i : i + 500]
                            ph = ",".join("?" for _ in batch)
                            async with db_writer() as db:
                                await db.execute(
                                    f"UPDATE files SET date_last_seen = ?, scan_id = ? "
                                    f"WHERE location_id = ? AND stale = 0 "
                                    f"AND rel_path IN ({ph})",
                                    [now_iso, scan_id, location_id] + batch,
                                )
                            await asyncio.sleep(0)
                        files_found += len(unchanged)

                    # Gone — compute deltas + batch mark stale
                    gone = result.get("gone", [])
                    delta_count -= len(gone)
                    for rp in gone:
                        old = expected_by_path.get(rp, {})
                        delta_size -= old.get("file_size", 0)
                        ft = old.get("file_type_high", "") or ""
                        delta_types[ft] = delta_types.get(ft, 0) - 1

                    if gone:
                        for i in range(0, len(gone), 500):
                            batch = gone[i : i + 500]
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

                # --- Compute deltas for this page's new + changed ---
                page_new = result.get("new", [])
                page_changed = result.get("changed", [])

                delta_count += len(page_new)
                for f in page_new:
                    delta_size += f.get("file_size", 0)
                    ft = f.get("file_type_high", "") or ""
                    delta_types[ft] = delta_types.get(ft, 0) + 1

                for f in page_changed:
                    old = expected_by_path.get(f["rel_path"], {})
                    delta_size += f.get("file_size", 0) - old.get("file_size", 0)
                    old_ft = old.get("file_type_high", "") or ""
                    new_ft = f.get("file_type_high", "") or ""
                    if old_ft != new_ft:
                        delta_types[old_ft] = delta_types.get(old_ft, 0) - 1
                        delta_types[new_ft] = delta_types.get(new_ft, 0) + 1

                # --- BATCHED WRITES for new + changed (WRITE_BATCH_SIZE = 500) ---
                all_items = [("changed", f) for f in page_changed] + [
                    ("new", f) for f in page_new
                ]
                for i in range(0, len(all_items), 500):
                    batch = all_items[i : i + 500]
                    async with db_writer() as db:
                        for kind, f in batch:
                            rel_path = f["rel_path"]
                            rel_dir = f.get("rel_dir", "")

                            if kind == "changed":
                                old_row = await db.execute_fetchall(
                                    "SELECT file_size, hash_strong FROM files "
                                    "WHERE location_id = ? AND rel_path = ?",
                                    (location_id, rel_path),
                                )
                                size_changed = old_row and old_row[0][
                                    "file_size"
                                ] != f.get("file_size")
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
                                filename=f.get("filename", ""),
                                full_path=f.get("full_path", ""),
                                rel_path=rel_path,
                                folder_id=folder_id,
                                file_size=f.get("file_size", 0),
                                created_date=f.get("created_date", ""),
                                modified_date=f.get("modified_date", ""),
                                file_type_high=f.get("file_type_high", ""),
                                file_type_low=f.get("file_type_low", ""),
                                hash_partial=f.get("hash_partial"),
                                hash_fast=None,
                                hash_strong=None,
                                now_iso=now_iso,
                                hidden=f.get("hidden", 0),
                                dup_exclude=dup_exclude,
                            )
                            files_found += 1
                            if kind == "new":
                                files_new += 1
                    await asyncio.sleep(0)  # yield between write batches

                # Check for more pages
                reconcile_cursor = result.get("cursor")
                is_first_page = False
                if reconcile_cursor is None:
                    break

            # --- After all pages: apply running counters ---
            running_file_count = max(0, running_file_count + delta_count)
            running_total_size = max(0, running_total_size + delta_size)
            for ft, d in delta_types.items():
                running_type_counts[ft] = running_type_counts.get(ft, 0) + d
            running_type_counts = {
                k: v for k, v in running_type_counts.items() if v > 0
            }

            # --- BACKFILL CANDIDATE QUERY ---
            dir_rel = os.path.relpath(current_dir, root_path)
            if dir_rel == ".":
                dir_rel = ""
            current_folder_id = (
                folder_cache[dir_rel][0]
                if dir_rel and dir_rel in folder_cache
                else None
            )
            async with db_writer() as db:
                await db.commit()  # ensure reads see all prior writes
                backfill_candidates = await _query_backfill_candidates(
                    db, location_id, current_folder_id
                )

            # --- INLINE BACKFILL: per-candidate HTTP + write (lock released) ---
            new_hashes: set[str] = set()
            for row in backfill_candidates:
                try:
                    hash_result = await dispatch(
                        "file_hash", location_id, path=row["full_path"]
                    )
                    async with db_writer() as db:
                        await db.execute(
                            "UPDATE files SET hash_fast = ? WHERE id = ?",
                            (hash_result["hash_fast"], row["id"]),
                        )
                    new_hashes.add(hash_result["hash_fast"])
                except (ConnectionError, OSError):
                    break
                except Exception as e:
                    logger.debug(
                        "Inline backfill: hash failed for %s: %r",
                        row["full_path"],
                        e,
                    )
            dir_affected_fast.update(new_hashes)

            # Submit to coalesced dup recalc writer
            if dir_affected_strong or dir_affected_fast:
                from file_hunter.services.dup_counts import (
                    submit_hashes_for_recalc,
                )

                submit_hashes_for_recalc(
                    strong_hashes=dir_affected_strong or None,
                    fast_hashes=dir_affected_fast or None,
                    source=f"scan {location_name}",
                    location_ids={location_id},
                )

            # --- WRITE PHASE 2: counters + params (same transaction) ---
            async with db_writer() as db:
                if dir_rel:
                    await _update_folder_chain(
                        db,
                        folder_cache,
                        dir_rel,
                        delta_count,
                        delta_size,
                        0,
                        delta_types,
                    )

                await db.execute(
                    "UPDATE locations SET file_count = ?, total_size = ?, "
                    "type_counts = ? WHERE id = ?",
                    (
                        running_file_count,
                        running_total_size,
                        json.dumps(running_type_counts),
                        location_id,
                    ),
                )

                # Persist traversal state atomically with catalog writes
                now_mono = time.monotonic()
                if now_mono - last_state_save > 0.5:
                    params["traversal"] = list(dirs_to_visit)
                    params["scan_id"] = scan_id
                    params["files_found"] = files_found
                    params["files_new"] = files_new
                    await db.execute(
                        "UPDATE operation_queue SET params = ? WHERE id = ?",
                        (json.dumps(params), op_id),
                    )
                    last_state_save = now_mono

            dirs_processed += 1

            # Refresh read snapshot and broadcast progress
            await read_db.commit()
            await _broadcast_progress(
                read_db,
                location_id,
                location_name,
                files_found,
                files_new,
                dirs_processed,
                len(dirs_to_visit),
            )

            if now_mono - last_stats_update > 5.0:
                invalidate_stats_cache()
                last_stats_update = now_mono

            # Yield to event loop
            await asyncio.sleep(0)

        # --- Finalization ---
        await broadcast(
            {
                "type": "scan_finalizing",
                "locationId": location_id,
                "location": location_name,
            }
        )

        async with db_writer() as db:
            stale_count = await mark_stale_files(db, location_id, scan_id, scan_prefix)
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

        from file_hunter.services.sizes import schedule_size_recalc

        schedule_size_recalc(location_id)
        invalidate_stats_cache()

        asyncio.create_task(
            _launch_cross_agent_backfill(agent_id, location_id, location_name)
        )

    except asyncio.CancelledError:
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
        # Agent disconnected — save state for resumption, re-raise
        # so queue_manager re-queues as pending
        async with db_writer() as db:
            params["scan_id"] = scan_id
            params["traversal"] = list(dirs_to_visit)
            params["files_found"] = files_found
            params["files_new"] = files_new
            await db.execute(
                "UPDATE operation_queue SET params = ? WHERE id = ?",
                (json.dumps(params), op_id),
            )
            await db.execute(
                "UPDATE scans SET status = 'interrupted' WHERE id = ?",
                (scan_id,),
            )

        logger.warning(
            "Scan interrupted: location #%d (%s): %s -- "
            "scan_id=%d saved for resumption, %d dirs remaining",
            location_id,
            location_name,
            e,
            scan_id,
            len(dirs_to_visit),
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


async def _get_expected_files(
    db, location_id: int, dir_path: str, root_path: str
) -> list[dict]:
    """Query the catalog for non-stale files in a specific directory.

    Returns list of {rel_path, file_size, modified_date, file_type_high}
    for the agent to compare against disk.  file_type_high is used by the
    scan loop to compute incremental counter deltas for gone/changed files.
    """
    rel_dir = os.path.relpath(dir_path, root_path)
    if rel_dir == ".":
        rel_dir = ""

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


async def _query_backfill_candidates(
    db, location_id: int, folder_id: int | None
) -> list:
    """Query backfill candidates for a directory.

    Must be called within db_writer() so it sees just-committed catalog writes.
    """
    if folder_id is not None:
        return await db.execute_fetchall(
            """SELECT f.id, f.full_path
               FROM files f
               WHERE f.folder_id = ?
                 AND f.location_id = ?
                 AND f.hash_fast IS NULL
                 AND f.hash_partial IS NOT NULL
                 AND f.file_size > 0
                 AND f.stale = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.file_size = f.file_size
                       AND f2.hash_partial = f.hash_partial
                       AND f2.id != f.id
                 )""",
            (folder_id, location_id),
        )
    else:
        return await db.execute_fetchall(
            """SELECT f.id, f.full_path
               FROM files f
               WHERE f.folder_id IS NULL
                 AND f.location_id = ?
                 AND f.hash_fast IS NULL
                 AND f.hash_partial IS NOT NULL
                 AND f.file_size > 0
                 AND f.stale = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.file_size = f.file_size
                       AND f2.hash_partial = f.hash_partial
                       AND f2.id != f.id
                 )""",
            (location_id,),
        )


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


async def _launch_cross_agent_backfill(
    agent_id: int, location_id: int, location_name: str
):
    """Background: hash matching files on other agents after scan."""
    from file_hunter.services.hash_backfill import _backfill_agents

    read_db = await get_db()
    try:
        affected_hashes: set[str] = set()
        hashed = await _backfill_agents(
            read_db, agent_id, location_id, location_name, affected_hashes
        )
        if affected_hashes:
            from file_hunter.services.dup_counts import submit_hashes_for_recalc

            submit_hashes_for_recalc(
                fast_hashes=affected_hashes,
                source=f"cross-agent {location_name}",
                location_ids={location_id},
            )
        if hashed > 0:
            from file_hunter.services.sizes import schedule_size_recalc

            loc_ids = {location_id}
            if affected_hashes:
                ah_list = list(affected_hashes)
                ah_ph = ",".join("?" for _ in ah_list)
                cross_rows = await read_db.execute_fetchall(
                    f"SELECT DISTINCT location_id FROM files "
                    f"WHERE hash_fast IN ({ah_ph}) AND location_id != ?",
                    ah_list + [location_id],
                )
                for r in cross_rows:
                    loc_ids.add(r["location_id"])
            schedule_size_recalc(*loc_ids)
            invalidate_stats_cache()
    except Exception:
        logger.error("Cross-agent backfill failed for %s", location_name, exc_info=True)


async def _broadcast_progress(
    db,
    location_id: int,
    location_name: str,
    files_found: int,
    files_new: int,
    dirs_processed: int,
    dirs_remaining: int,
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
            "dirsRemaining": dirs_remaining,
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
