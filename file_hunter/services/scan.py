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
from file_hunter.services.agent_ops import reconcile_directory, stream_tree
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

# Dedicated debug log for diagnosing scan hangs — writes to data/scan_debug.log
_debug_log = logging.getLogger("scan_debug")
_debug_log.setLevel(logging.DEBUG)
if not _debug_log.handlers:
    from pathlib import Path as _P

    _debug_path = (
        _P(__file__).resolve().parent.parent.parent / "data" / "scan_debug.log"
    )
    _debug_path.parent.mkdir(parents=True, exist_ok=True)
    _fh = logging.FileHandler(str(_debug_path), mode="a")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%H:%M:%S"))
    _debug_log.addHandler(_fh)


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
            "SELECT name, file_count, total_size, type_counts, date_last_scanned "
            "FROM locations WHERE id = ?",
            (location_id,),
        )
        location_name = loc_row[0]["name"] if loc_row else f"Location #{location_id}"
        running_file_count = (loc_row[0]["file_count"] or 0) if loc_row else 0
        running_total_size = (loc_row[0]["total_size"] or 0) if loc_row else 0
        running_type_counts = (
            json.loads(loc_row[0]["type_counts"] or "{}") if loc_row else {}
        )
        is_first_scan = not (loc_row and loc_row[0]["date_last_scanned"])

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

    # --- First scan streamed path: skip per-directory reconcile ---
    from file_hunter.ws.agent import get_agent_capabilities

    agent_caps = get_agent_capabilities(agent_id)
    can_stream_first = (
        is_first_scan
        and not saved_traversal
        and not scan_prefix
        and "stream_first_scan" in agent_caps
    )

    if can_stream_first:
        try:
            await _run_first_scan_streamed(
                agent_id, location_id, location_name, root_path, scan_id, now_iso
            )
            return
        except (ConnectionError, OSError, httpx.ConnectError):
            raise
        except Exception:
            logger.warning(
                "Streamed first scan failed for %s — falling back to BFS",
                location_name,
                exc_info=True,
            )

    # --- Tree-diff fast path: skip unchanged directories ---
    tree_diff_dirs: set[str] | None = None
    can_tree_diff = not is_first_scan and not saved_traversal and not scan_prefix

    try:
        if can_tree_diff:
            try:
                logger.info(
                    "Tree diff: starting for location #%d (%s)",
                    location_id,
                    location_name,
                )
                tree_diff_dirs = await _tree_diff(
                    agent_id,
                    location_id,
                    root_path,
                    read_db,
                    location_name=location_name,
                    broadcast_fn=lambda dirs_done, files_seen, changed: broadcast(
                        {
                            "type": "scan_progress",
                            "locationId": location_id,
                            "location": location_name,
                            "phase": "tree_diff",
                            "dirsProcessed": dirs_done,
                            "filesFound": files_seen,
                            "dirsChanged": changed,
                        }
                    ),
                )
                if tree_diff_dirs is not None:
                    if not tree_diff_dirs:
                        dirs_to_visit.clear()
                        logger.info(
                            "Tree diff: 0 changed directories for location #%d (%s) — "
                            "skipping BFS entirely",
                            location_id,
                            location_name,
                        )
                    else:
                        # Rebuild BFS queue to only visit changed directories
                        changed_abs = {
                            os.path.join(root_path, d) if d else root_path
                            for d in tree_diff_dirs
                        }
                        dirs_to_visit = deque(sorted(changed_abs))
                        logger.info(
                            "Tree diff: %d changed directories for location #%d (%s)",
                            len(tree_diff_dirs),
                            location_id,
                            location_name,
                        )
                else:
                    logger.info(
                        "Tree diff: agent does not support /tree for location #%d (%s) — "
                        "falling back to full BFS",
                        location_id,
                        location_name,
                    )
            except (ConnectionError, OSError, httpx.ConnectError):
                raise  # agent offline — let outer handler deal with it
            except Exception:
                logger.warning(
                    "Tree diff failed for location #%d (%s) — falling back to full BFS",
                    location_id,
                    location_name,
                    exc_info=True,
                )

        # Bulk-confirm unchanged files so mark_stale_files doesn't falsely stale them
        if tree_diff_dirs is not None:
            async with db_writer() as db:
                await db.execute(
                    "UPDATE files SET scan_id = ?, date_last_seen = ? "
                    "WHERE location_id = ? AND stale = 0",
                    (scan_id, now_iso, location_id),
                )
            # Use stored counter instead of COUNT(*) against files table
            files_found = running_file_count
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
                    # When tree-diff is active, don't BFS into subdirs —
                    # we already know exactly which dirs need reconcile
                    if tree_diff_dirs is None:
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

            # --- INLINE BACKFILL: batch hash_fast for candidates ---
            new_hashes: set[str] = set()
            if backfill_candidates:
                from file_hunter.services.agent_ops import hash_fast_batch

                paths = [r["full_path"] for r in backfill_candidates]
                id_by_path = {r["full_path"]: r["id"] for r in backfill_candidates}
                try:
                    result = await hash_fast_batch(agent_id, paths)
                    hash_map = {
                        r["path"]: r["hash_fast"] for r in result.get("results", [])
                    }
                    if hash_map:
                        async with db_writer() as db:
                            for path, h in hash_map.items():
                                fid = id_by_path.get(path)
                                if fid:
                                    await db.execute(
                                        "UPDATE files SET hash_fast = ? WHERE id = ?",
                                        (h, fid),
                                    )
                                    new_hashes.add(h)
                except (ConnectionError, OSError):
                    logger.warning("Agent offline during backfill batch")
                except Exception as e:
                    logger.debug("Batch backfill failed: %r", e)
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
            agent_id, location_id, location_name, affected_hashes
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


async def _get_catalog_dir(
    db, location_id: int, rel_dir: str
) -> dict[str, tuple[int, str]]:
    """Load catalog files for one directory, keyed by rel_path.

    Returns {rel_path: (file_size, modified_date)} for non-stale files.
    """
    if rel_dir == "":
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id IS NULL",
            (location_id,),
        )
    else:
        folder_row = await db.execute_fetchall(
            "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, rel_dir),
        )
        if not folder_row:
            return {}
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id = ?",
            (location_id, folder_row[0]["id"]),
        )
    return {r["rel_path"]: (r["file_size"], r["modified_date"]) for r in rows}


def _diff_directory(
    agent_files: list[dict], catalog: dict[str, tuple[int, str]]
) -> bool:
    """Compare agent files against catalog for one directory.

    Returns True if the directory has changed (needs reconcile).
    """
    if len(agent_files) != len(catalog):
        return True
    for af in agent_files:
        entry = catalog.get(af["rel_path"])
        if entry is None:
            return True
        cat_size, cat_mtime = entry
        if af["size"] != cat_size:
            return True
        if af["mtime"] != cat_mtime:
            return True
    return False


async def _tree_diff(
    agent_id: int,
    location_id: int,
    root_path: str,
    read_db,
    location_name: str = "",
    broadcast_fn=None,
) -> set[str] | None:
    """Stream the agent tree and diff against catalog.

    Populates a temp table with catalog data for the location, then diffs
    against the agent stream using indexed lookups on the temp table.
    Memory is bounded by SQLite's temp storage (in-memory for small locations,
    spills to disk for large ones).

    Returns set of changed rel_dir strings (directories needing reconcile),
    or None if tree streaming is not supported (agent returned 404).
    """
    from file_hunter.db import open_connection

    conn = await open_connection()
    try:
        # Create temp table and populate from catalog in one bulk INSERT
        await conn.execute(
            "CREATE TEMP TABLE _diff_catalog ("
            "  rel_path TEXT PRIMARY KEY,"
            "  dir_path TEXT NOT NULL,"
            "  file_size INTEGER NOT NULL,"
            "  modified_date TEXT NOT NULL"
            ")"
        )
        await conn.execute("CREATE INDEX _diff_catalog_dir ON _diff_catalog(dir_path)")

        # Populate: join files with folders to get dir_path
        await conn.execute(
            "INSERT INTO _diff_catalog (rel_path, dir_path, file_size, modified_date) "
            "SELECT f.rel_path, COALESCE(fld.rel_path, ''), f.file_size, f.modified_date "
            "FROM files f "
            "LEFT JOIN folders fld ON fld.id = f.folder_id "
            "WHERE f.location_id = ? AND f.stale = 0",
            (location_id,),
        )
        await conn.commit()

        # Get catalog directory set
        dir_rows = await conn.execute_fetchall(
            "SELECT DISTINCT rel_path FROM folders WHERE location_id = ?",
            (location_id,),
        )
        catalog_dir_set = {r["rel_path"] for r in dir_rows}

        changed_dirs: set[str] = set()
        all_dirs: list[str] = []
        current_rel_dir: str | None = None
        current_files: list[dict] = []
        dirs_done = 0
        files_seen = 0
        last_broadcast = time.monotonic()

        async def _flush_dir():
            """Diff the buffered directory against the temp table."""
            nonlocal dirs_done
            if current_rel_dir is None:
                return

            # Count catalog files in this directory
            count_row = await conn.execute_fetchall(
                "SELECT COUNT(*) as cnt FROM _diff_catalog WHERE dir_path = ?",
                (current_rel_dir,),
            )
            cat_count = count_row[0]["cnt"] if count_row else 0

            # Quick check: different file count = changed
            if len(current_files) != cat_count:
                changed_dirs.add(current_rel_dir)
                dirs_done += 1
                return

            # Detailed check: compare each file
            for af in current_files:
                row = await conn.execute_fetchall(
                    "SELECT file_size, modified_date FROM _diff_catalog "
                    "WHERE rel_path = ?",
                    (af["rel_path"],),
                )
                if not row:
                    changed_dirs.add(current_rel_dir)
                    dirs_done += 1
                    return
                if (
                    af["size"] != row[0]["file_size"]
                    or af["mtime"] != row[0]["modified_date"]
                ):
                    changed_dirs.add(current_rel_dir)
                    dirs_done += 1
                    return

            dirs_done += 1

        got_records = False
        async for record in stream_tree(agent_id, root_path):
            got_records = True
            rtype = record.get("type")

            if rtype == "dir":
                await _flush_dir()
                now = time.monotonic()
                if now - last_broadcast >= 2.0:
                    logger.info(
                        "Tree diff: %d dirs diffed, %d files seen, %d changed so far — %s",
                        dirs_done,
                        files_seen,
                        len(changed_dirs),
                        location_name,
                    )
                    if broadcast_fn:
                        await broadcast_fn(dirs_done, files_seen, len(changed_dirs))
                    last_broadcast = now
                current_rel_dir = record["rel_dir"]
                current_files = []
                all_dirs.append(current_rel_dir)

            elif rtype == "file":
                current_files.append(record)
                files_seen += 1

            elif rtype == "end":
                await _flush_dir()

        if not got_records:
            return None

        logger.info(
            "Tree diff complete: %d dirs, %d files, %d changed — %s",
            dirs_done,
            files_seen,
            len(changed_dirs),
            location_name,
        )

        # Detect deleted directories
        agent_dir_set = set(all_dirs)
        for d in catalog_dir_set:
            if d not in agent_dir_set:
                changed_dirs.add(d)

        if "" not in agent_dir_set:
            changed_dirs.add("")

        return changed_dirs

    finally:
        try:
            await conn.execute("DROP TABLE IF EXISTS _diff_catalog")
        except Exception:
            pass
        await conn.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


STREAM_BATCH_SIZE = 5000
HASH_BATCH_SIZE = 2000
HASH_BATCH_SIZE_TURBO = 10000


async def _get_hash_batch_size() -> int:
    """Return hash batch size based on turbo mode setting."""
    from file_hunter.services.settings import is_turbo_mode

    db = await get_db()
    if await is_turbo_mode(db):
        return HASH_BATCH_SIZE_TURBO
    return HASH_BATCH_SIZE


async def _run_first_scan_streamed(
    agent_id: int,
    location_id: int,
    location_name: str,
    root_path: str,
    scan_id: int,
    now_iso: str,
):
    """Stream-based first scan: tree walk → bulk insert → hash_partial → candidates → dups.

    Uses the agent's /tree endpoint for metadata, bulk-inserts via db_writer,
    then batch-computes hash_partial and hash_fast for candidates.
    """
    from file_hunter_core.classify import classify_file
    from file_hunter.services.agent_ops import (
        hash_fast_batch,
        hash_partial_batch,
        stream_tree,
    )
    from file_hunter.services.sizes import recalculate_location_sizes

    logger.info(
        "Streamed first scan starting for location #%d (%s)", location_id, location_name
    )

    # Clean any leftover data from interrupted previous scans
    async with db_writer() as db:
        existing = await db.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM files WHERE location_id = ?",
            (location_id,),
        )
        if existing[0]["cnt"] > 0:
            logger.info(
                "Streamed first scan: deleting %d leftover files for location %d",
                existing[0]["cnt"],
                location_id,
            )
            await db.execute("DELETE FROM files WHERE location_id = ?", (location_id,))
            await db.execute(
                "DELETE FROM folders WHERE location_id = ?", (location_id,)
            )

    # --- Phase 1: Stream metadata, bulk insert ---
    folder_cache: dict[str, int] = {}  # rel_dir -> folder_id
    file_batch: list[tuple] = []
    files_found = 0
    folders_found = 0
    last_broadcast = time.monotonic()

    async for record in stream_tree(agent_id, root_path):
        rtype = record.get("type")

        if rtype == "dir":
            rel_dir = record["rel_dir"]
            if rel_dir:
                async with db_writer() as db:
                    folder_id, _ = await ensure_folder_hierarchy(
                        db, location_id, rel_dir, folder_cache
                    )
                folders_found += 1

        elif rtype == "file":
            rel_path = record["rel_path"]
            size = record["size"]
            mtime = record["mtime"]
            ctime = record.get("ctime", mtime)

            filename = os.path.basename(rel_path)
            rel_dir = os.path.dirname(rel_path)
            full_path = os.path.join(root_path, rel_path)
            type_high, type_low = classify_file(filename)
            hidden = (
                1
                if (
                    filename.startswith(".")
                    or any(p.startswith(".") for p in rel_dir.split(os.sep) if p)
                )
                else 0
            )

            # Resolve folder_id from cache
            folder_id = folder_cache.get(rel_dir, (None,))[0] if rel_dir else None

            file_batch.append(
                (
                    filename,
                    full_path,
                    rel_path,
                    location_id,
                    folder_id,
                    type_high,
                    type_low,
                    size,
                    "",  # description
                    "",  # tags
                    ctime,  # created_date
                    mtime,  # modified_date
                    now_iso,  # date_cataloged
                    now_iso,  # date_last_seen
                    hidden,
                    0,  # stale
                    scan_id,
                )
            )
            files_found += 1

            if len(file_batch) >= STREAM_BATCH_SIZE:
                await _bulk_insert_files(file_batch)
                file_batch.clear()

            # Broadcast progress
            now_t = time.monotonic()
            if now_t - last_broadcast >= 2.0:
                await broadcast(
                    {
                        "type": "scan_progress",
                        "locationId": location_id,
                        "location": location_name,
                        "phase": "streaming",
                        "filesFound": files_found,
                        "foldersFound": folders_found,
                    }
                )
                last_broadcast = now_t

        elif rtype == "end":
            break

    # Flush remaining
    if file_batch:
        await _bulk_insert_files(file_batch)

    logger.info(
        "Streamed first scan phase 1 complete: %d files, %d folders for %s",
        files_found,
        folders_found,
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
        }
    )

    # --- Phase 2: Batch hash_partial ---
    hash_batch_size = await _get_hash_batch_size()
    read_db = await get_db()
    unhashed = await read_db.execute_fetchall(
        "SELECT id, full_path FROM files "
        "WHERE location_id = ? AND hash_partial IS NULL AND file_size > 0 AND stale = 0",
        (location_id,),
    )

    total_to_hash = len(unhashed)
    hashed = 0
    logger.info(
        "Streamed first scan phase 2: %d files to partial-hash for %s",
        total_to_hash,
        location_name,
    )

    for i in range(0, total_to_hash, hash_batch_size):
        batch = unhashed[i : i + hash_batch_size]
        paths = [r["full_path"] for r in batch]
        try:
            result = await hash_partial_batch(agent_id, paths)
        except (ConnectionError, OSError):
            logger.warning("Agent offline during hash_partial batch — stopping")
            break

        # Build path → hash map
        hash_map = {r["path"]: r["hash_partial"] for r in result.get("results", [])}

        if hash_map:
            async with db_writer() as db:
                for r in batch:
                    h = hash_map.get(r["full_path"])
                    if h:
                        await db.execute(
                            "UPDATE files SET hash_partial = ? WHERE id = ?",
                            (h, r["id"]),
                        )
            hashed += len(hash_map)

        now_t = time.monotonic()
        if now_t - last_broadcast >= 2.0:
            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "hashing_partial",
                    "filesFound": files_found,
                    "filesHashed": hashed,
                    "filesToHash": total_to_hash,
                }
            )
            last_broadcast = now_t

    logger.info(
        "Streamed first scan phase 2 complete: %d partial hashes for %s",
        hashed,
        location_name,
    )
    _debug_log.debug(
        "PHASE2_COMPLETE: %d partial hashes done, entering candidate detection", hashed
    )

    # --- Phase 3: Candidate detection + hash_fast ---
    await broadcast(
        {
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "phase": "finding_candidates",
            "filesFound": files_found,
            "filesHashed": 0,
        }
    )

    # Find candidate groups: get this location's (hash_partial, file_size) pairs,
    # then check which have matches elsewhere in the catalog
    _debug_log.debug("CANDIDATE: getting get_db()")
    t_cand = time.monotonic()
    read_db = await get_db()
    _debug_log.debug("CANDIDATE: get_db() returned in %.3fs", time.monotonic() - t_cand)

    _debug_log.debug("CANDIDATE: commit() to refresh snapshot")
    t_cand = time.monotonic()
    await read_db.commit()
    _debug_log.debug("CANDIDATE: commit() done in %.3fs", time.monotonic() - t_cand)

    _debug_log.debug("CANDIDATE: executing DISTINCT query")
    t_cand = time.monotonic()
    local_pairs = await read_db.execute_fetchall(
        "SELECT DISTINCT hash_partial, file_size FROM files "
        "WHERE location_id = ? AND hash_partial IS NOT NULL AND file_size > 0 AND stale = 0",
        (location_id,),
    )
    _debug_log.debug(
        "CANDIDATE: DISTINCT query returned %d rows in %.3fs",
        len(local_pairs),
        time.monotonic() - t_cand,
    )

    # Batch-check which pairs have duplicates globally
    dup_groups = []
    total_batches = (len(local_pairs) + 499) // 500
    _debug_log.debug(
        "CANDIDATE: batch-checking %d pairs in %d batches",
        len(local_pairs),
        total_batches,
    )
    batch_num = 0
    for i in range(0, len(local_pairs), 500):
        batch_num += 1
        t_batch = time.monotonic()
        chunk = local_pairs[i : i + 500]
        conditions = " OR ".join("(hash_partial = ? AND file_size = ?)" for _ in chunk)
        params = []
        for g in chunk:
            params.extend([g["hash_partial"], g["file_size"]])
        rows = await read_db.execute_fetchall(
            f"SELECT hash_partial, file_size, COUNT(*) as cnt FROM files "
            f"WHERE ({conditions}) AND stale = 0 "
            f"GROUP BY hash_partial, file_size HAVING COUNT(*) > 1",
            params,
        )
        dup_groups.extend(rows)
        _debug_log.debug(
            "CANDIDATE: batch %d/%d done in %.3fs (%d dup groups so far)",
            batch_num,
            total_batches,
            time.monotonic() - t_batch,
            len(dup_groups),
        )

    _debug_log.debug(
        "CANDIDATE: batch-check complete, %d dup groups found", len(dup_groups)
    )

    # Fetch candidate files — we already have the dup (hash_partial, file_size)
    # pairs. Query files matching those pairs. Use batched lookups on the
    # (file_size, hash_partial) index — but without the hash_fast IS NULL
    # filter that caused full table scans.
    _debug_log.debug(
        "FETCH_CANDIDATES: fetching files for %d dup groups", len(dup_groups)
    )
    t_fetch = time.monotonic()

    candidates = []
    for i in range(0, len(dup_groups), 500):
        chunk = dup_groups[i : i + 500]
        conditions = " OR ".join("(hash_partial = ? AND file_size = ?)" for _ in chunk)
        params = []
        for g in chunk:
            params.extend([g["hash_partial"], g["file_size"]])
        rows = await read_db.execute_fetchall(
            f"SELECT id, full_path FROM files WHERE ({conditions}) AND stale = 0",
            params,
        )
        candidates.extend(rows)

    _debug_log.debug(
        "FETCH_CANDIDATES: %d candidates fetched in %.3fs",
        len(candidates),
        time.monotonic() - t_fetch,
    )

    total_candidates = len(candidates)
    confirmed = 0
    logger.info(
        "Streamed first scan phase 3: %d candidates to full-hash for %s",
        total_candidates,
        location_name,
    )

    new_fast_hashes: set[str] = set()
    for i in range(0, total_candidates, hash_batch_size):
        batch = candidates[i : i + hash_batch_size]
        paths = [r["full_path"] for r in batch]
        try:
            result = await hash_fast_batch(agent_id, paths)
        except (ConnectionError, OSError):
            logger.warning("Agent offline during hash_fast batch — stopping")
            break

        hash_map = {r["path"]: r["hash_fast"] for r in result.get("results", [])}

        if hash_map:
            async with db_writer() as db:
                for r in batch:
                    h = hash_map.get(r["full_path"])
                    if h:
                        await db.execute(
                            "UPDATE files SET hash_fast = ? WHERE id = ?",
                            (h, r["id"]),
                        )
                        new_fast_hashes.add(h)
            confirmed += len(hash_map)

        now_t = time.monotonic()
        if now_t - last_broadcast >= 2.0:
            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "confirming",
                    "filesFound": files_found,
                    "filesHashed": confirmed,
                    "filesToHash": total_candidates,
                }
            )
            last_broadcast = now_t

    logger.info(
        "Streamed first scan phase 3 complete: %d confirmed for %s",
        confirmed,
        location_name,
    )

    # --- Phase 4: Dup recount (synchronous, must complete before size rebuild) ---
    if new_fast_hashes:
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "recounting",
                "filesFound": files_found,
                "filesHashed": 0,
            }
        )
        from file_hunter.services.dup_counts import optimized_dup_recount

        await optimized_dup_recount(location_id=location_id)

    # --- Phase 5: Finalize ---
    await broadcast(
        {
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "phase": "rebuilding",
            "filesFound": files_found,
            "filesHashed": 0,
        }
    )
    await recalculate_location_sizes(location_id)
    invalidate_stats_cache()

    async with db_writer() as db:
        await db.execute(
            "UPDATE scans SET status = 'completed', completed_at = ?, "
            "files_found = ?, files_hashed = ? WHERE id = ?",
            (now_iso, files_found, confirmed, scan_id),
        )
        await db.execute(
            "UPDATE locations SET date_last_scanned = ? WHERE id = ?",
            (now_iso, location_id),
        )

    await broadcast(
        {
            "type": "scan_completed",
            "locationId": location_id,
            "location": location_name,
            "filesFound": files_found,
            "filesHashed": confirmed,
            "duplicatesFound": len(new_fast_hashes),
        }
    )

    logger.info(
        "Streamed first scan complete: %s — %d files, %d hashed, %d dup groups",
        location_name,
        files_found,
        confirmed,
        len(new_fast_hashes),
    )


async def _bulk_insert_files(batch: list[tuple]):
    """Bulk insert files via executemany through db_writer."""
    async with db_writer() as db:
        await db.executemany(
            "INSERT INTO files "
            "(filename, full_path, rel_path, location_id, folder_id, "
            "file_type_high, file_type_low, file_size, "
            "description, tags, created_date, modified_date, "
            "date_cataloged, date_last_seen, hidden, stale, scan_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            batch,
        )
