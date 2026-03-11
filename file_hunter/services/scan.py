"""Server-driven scan orchestrator.

Drives BFS traversal of a location by calling the agent's /reconcile
endpoint per directory.  Uses scanner.py's proven DB functions for all
catalog writes — no reimplementation of folder hierarchy, file upsert,
or stale marking.
"""

import asyncio
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

from file_hunter.db import open_connection, check_write_lock_requested
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    upsert_file,
    mark_stale_files,
)
from file_hunter.services.agent_ops import reconcile_directory
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def run_scan(op_id: int, agent_id: int, params: dict):
    """Execute a server-driven scan operation.

    Called by queue_manager as the "scan_dir" handler.

    params:
        location_id: int
        path: str — root_path or scan subfolder
        root_path: str — location root for rel_path computation
        traversal: list[str] | None — saved traversal state for resumption
    """
    location_id = params["location_id"]
    scan_path = params.get("path") or params["root_path"]
    root_path = params["root_path"]
    saved_traversal = params.get("traversal")

    db = await open_connection()
    try:
        # Resolve location name for broadcasts
        loc_row = await db.execute_fetchall(
            "SELECT name FROM locations WHERE id = ?", (location_id,)
        )
        location_name = loc_row[0]["name"] if loc_row else f"Location #{location_id}"

        # Create scan record
        now_iso = _now()
        cursor = await db.execute(
            "INSERT INTO scans (location_id, status, started_at) VALUES (?, 'running', ?)",
            (location_id, now_iso),
        )
        scan_id = cursor.lastrowid
        await db.commit()

        # Determine scan prefix for stale marking
        scan_prefix = None
        if scan_path != root_path:
            scan_prefix = os.path.relpath(scan_path, root_path)

        # State
        folder_cache: dict[str, tuple] = {}
        affected_hashes: set[str] = set()
        files_found = 0
        files_new = 0
        dirs_processed = 0

        # BFS traversal queue
        dirs_to_visit: deque[str] = deque(
            saved_traversal if saved_traversal else [scan_path]
        )
        last_state_save = time.monotonic()

        await broadcast(
            {
                "type": "scan_started",
                "locationId": location_id,
                "location": location_name,
                "scanId": scan_id,
            }
        )

        while dirs_to_visit:
            current_dir = dirs_to_visit.popleft()

            # Get expected files for this directory from the catalog
            expected = await _get_expected_files(
                db, location_id, current_dir, root_path
            )

            # Call agent /reconcile
            result = await reconcile_directory(
                agent_id, current_dir, root_path, expected
            )

            # Enqueue subdirectories for BFS
            for subdir in sorted(result.get("subdirs", [])):
                dirs_to_visit.append(os.path.join(current_dir, subdir))

            # --- Process unchanged files ---
            unchanged = result.get("unchanged", [])
            if unchanged:
                # Batch update scan_id and date_last_seen
                for i in range(0, len(unchanged), 500):
                    batch = unchanged[i : i + 500]
                    ph = ",".join("?" for _ in batch)
                    await db.execute(
                        f"UPDATE files SET date_last_seen = ?, scan_id = ? "
                        f"WHERE location_id = ? AND stale = 0 AND rel_path IN ({ph})",
                        [now_iso, scan_id, location_id] + batch,
                    )
                files_found += len(unchanged)

            # --- Process changed files ---
            for f in result.get("changed", []):
                rel_path = f["rel_path"]
                rel_dir = f.get("rel_dir", "")

                # If file size changed, clear stale full hashes before upsert
                # (COALESCE would preserve them otherwise)
                old_row = await db.execute_fetchall(
                    "SELECT file_size, hash_strong FROM files "
                    "WHERE location_id = ? AND rel_path = ?",
                    (location_id, rel_path),
                )
                size_changed = old_row and old_row[0]["file_size"] != f.get(
                    "file_size"
                )
                if size_changed:
                    await db.execute(
                        "UPDATE files SET hash_fast = NULL, hash_strong = NULL "
                        "WHERE location_id = ? AND rel_path = ?",
                        (location_id, rel_path),
                    )
                    old_hs = old_row[0]["hash_strong"]
                    if old_hs:
                        affected_hashes.add(old_hs)

                # Resolve folder
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

            # --- Process new files ---
            for f in result.get("new", []):
                rel_path = f["rel_path"]
                rel_dir = f.get("rel_dir", "")

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
                files_new += 1

            # --- Process gone files ---
            gone = result.get("gone", [])
            if gone:
                for i in range(0, len(gone), 500):
                    batch = gone[i : i + 500]
                    ph = ",".join("?" for _ in batch)
                    # Collect affected hashes before marking stale
                    hash_rows = await db.execute_fetchall(
                        f"SELECT DISTINCT hash_strong FROM files "
                        f"WHERE location_id = ? AND rel_path IN ({ph}) "
                        f"AND hash_strong IS NOT NULL",
                        [location_id] + batch,
                    )
                    for hr in hash_rows:
                        affected_hashes.add(hr["hash_strong"])
                    await db.execute(
                        f"UPDATE files SET stale = 1 "
                        f"WHERE location_id = ? AND rel_path IN ({ph})",
                        [location_id] + batch,
                    )

            await db.commit()
            dirs_processed += 1

            # Broadcast progress after every directory
            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "filesFound": files_found,
                    "filesNew": files_new,
                    "dirsProcessed": dirs_processed,
                    "dirsRemaining": len(dirs_to_visit),
                }
            )

            # Persist traversal state for crash recovery
            now_mono = time.monotonic()
            if now_mono - last_state_save > 0.5:
                from file_hunter.services.queue_manager import update_params

                params["traversal"] = list(dirs_to_visit)
                await update_params(op_id, params)
                last_state_save = now_mono

            # Yield so DB reads (tree nav, file browsing) aren't blocked
            if check_write_lock_requested():
                await asyncio.sleep(0.15)
            else:
                await asyncio.sleep(0)

        # --- Finalization ---

        # Mark files not seen in this scan as stale
        stale_count = await mark_stale_files(db, location_id, scan_id, scan_prefix)

        # Flag new files for backfill
        if files_new > 0:
            await db.execute(
                "UPDATE locations SET backfill_needed = 1 WHERE id = ?",
                (location_id,),
            )

        # Complete the scan record
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
        await db.commit()

        logger.info(
            "Scan completed: location #%d (%s), %d files found, %d new, %d stale",
            location_id,
            location_name,
            files_found,
            files_new,
            stale_count,
        )

        await broadcast(
            {
                "type": "scan_completed",
                "locationId": location_id,
                "location": location_name,
                "filesFound": files_found,
                "filesNew": files_new,
                "staleFiles": stale_count,
            }
        )

        # Schedule post-scan work (fire-and-forget on own connections)
        from file_hunter.services.sizes import schedule_size_recalc

        schedule_size_recalc(location_id)

        if affected_hashes:
            from file_hunter.services.dup_counts import schedule_dup_recalc

            schedule_dup_recalc(affected_hashes, location_name)

        invalidate_stats_cache()

        # Launch cross-agent backfill if needed
        await _maybe_launch_backfill(
            agent_id, location_id, location_name, scan_prefix
        )

    except asyncio.CancelledError:
        # Operation was cancelled via queue_manager
        completed_iso = _now()
        await db.execute(
            "UPDATE scans SET status = 'cancelled', completed_at = ? WHERE id = ?",
            (completed_iso, scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        await broadcast(
            {
                "type": "scan_cancelled",
                "locationId": location_id,
                "location": location_name,
            }
        )
        logger.info("Scan cancelled: location #%d (%s)", location_id, location_name)
        raise

    except Exception as e:
        # Scan failed
        try:
            await db.execute(
                "UPDATE scans SET status = 'error', error = ?, completed_at = ? "
                "WHERE id = ?",
                (str(e), _now(), scan_id),
            )
            await db.commit()
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

    finally:
        await db.close()


async def _get_expected_files(
    db, location_id: int, dir_path: str, root_path: str
) -> list[dict]:
    """Query the catalog for non-stale files in a specific directory.

    Returns list of {rel_path, file_size, modified_date} for the agent
    to compare against disk.
    """
    rel_dir = os.path.relpath(dir_path, root_path)
    if rel_dir == ".":
        rel_dir = ""

    if rel_dir == "":
        # Root directory — files with no folder
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id IS NULL",
            (location_id,),
        )
    else:
        # Find folder_id, then query its files
        folder_row = await db.execute_fetchall(
            "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, rel_dir),
        )
        if not folder_row:
            return []
        folder_id = folder_row[0]["id"]
        rows = await db.execute_fetchall(
            "SELECT rel_path, file_size, modified_date "
            "FROM files WHERE location_id = ? AND stale = 0 AND folder_id = ?",
            (location_id, folder_id),
        )

    return [
        {
            "rel_path": r["rel_path"],
            "file_size": r["file_size"],
            "modified_date": r["modified_date"],
        }
        for r in rows
    ]


async def _maybe_launch_backfill(
    agent_id: int, location_id: int, location_name: str, scan_prefix: str | None
):
    """Check if cross-agent backfill is needed and launch it."""
    from file_hunter.db import get_db

    db = await get_db()
    row = await db.execute_fetchall(
        "SELECT backfill_needed FROM locations WHERE id = ?", (location_id,)
    )
    if not row or not row[0]["backfill_needed"]:
        return

    from file_hunter.services.hash_backfill import run_backfill

    logger.info(
        "Launching post-scan backfill for location #%d (%s)",
        location_id,
        location_name,
    )
    asyncio.create_task(
        run_backfill(agent_id, location_id, location_name, scan_prefix)
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
