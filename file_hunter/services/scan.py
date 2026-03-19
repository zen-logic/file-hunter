"""Server-driven scan orchestrator.

Two-phase streamed scan from agent's /tree endpoint:

Phase 1 (metadata): Agent streams D+F records. Server writes to a
temporary SQLite DB per agent — no catalog lock contention, full stream
speed. Then bulk-ingests from temp DB into catalog using batched upserts.

Phase 2 (hashing): Agent streams H records with partial hashes. Server
applies them to catalog records and runs dup candidate checks.

All catalog writes go through db_writer() — the single write connection
serialized by asyncio.Lock. No SQLite lock contention, no timeouts.
"""

import asyncio
import json
import logging
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timezone

import httpx

from file_hunter.db import db_writer, read_db
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    mark_stale_files,
)
from file_hunter.services.agent_ops import stream_tree
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast
from file_hunter_core.classify import classify_file

logger = logging.getLogger("file_hunter")

INGEST_BATCH_SIZE = 2000


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

    # --- Initialisation ---
    async with db_writer() as db:
        loc_row = await db.execute_fetchall(
            "SELECT name, file_count, total_size, type_counts "
            "FROM locations WHERE id = ?",
            (location_id,),
        )
        location_name = loc_row[0]["name"] if loc_row else f"Location #{location_id}"

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
                scan_id, location_id, location_name,
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

    # State — initialised before try so exception handlers can reference them
    files_found = 0
    files_new = 0
    hashes_applied = 0
    stale_count = 0

    # Create temp DB for fast stream capture
    tmp_file = tempfile.NamedTemporaryFile(
        suffix=".db", prefix=f"scan-{location_id}-", delete=False
    )
    tmp_path = tmp_file.name
    tmp_file.close()

    try:
        # --- Phase 1: stream metadata into temp DB ---
        files_found, dirs_found = await _stream_to_temp_db(
            tmp_path, agent_id, root_path, prefix_for_agent,
            location_id, location_name,
        )

        logger.info(
            "Stream captured: %d files, %d dirs for %s",
            files_found, dirs_found, location_name,
        )

        # --- Phase 2: bulk ingest from temp DB into catalog ---
        files_new = await _bulk_ingest(
            tmp_path, location_id, scan_id, root_path, now_iso,
            location_name, files_found,
        )

        logger.info(
            "Ingest complete: %d files (%d new) for %s",
            files_found, files_new, location_name,
        )

        # --- Phase 3: hash phase (H records are in temp DB) ---
        hashes_applied = await _apply_hashes_from_temp(
            tmp_path, location_id, agent_id, root_path, now_iso,
            location_name,
        )

        logger.info(
            "Hash phase complete: %d hashes applied for %s",
            hashes_applied, location_name,
        )

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
            location_id, location_name, files_found, files_new, stale_count,
        )

        # Wait for hash drainer to finish remaining work
        scan_done_event.set()
        logger.info("Waiting for hash drainer to finish for %s", location_name)
        try:
            await drainer_task
        except asyncio.CancelledError:
            pass
        logger.info("Hash drainer finished for %s", location_name)

        # --- Correction pass ---
        from file_hunter.services.dup_counts import full_dup_recount
        from file_hunter.services.sizes import recalculate_location_sizes

        recount_total = 0
        last_recount_broadcast = time.monotonic()

        async def _on_recount_total(total):
            nonlocal recount_total
            recount_total = total

        async def _on_recount_progress(processed):
            nonlocal last_recount_broadcast
            now = time.monotonic()
            if now - last_recount_broadcast >= 2.0:
                await broadcast({
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "recounting",
                    "checksDone": processed,
                    "checksTotal": recount_total,
                })
                last_recount_broadcast = now

        logger.info("Running dup recount for %s", location_name)
        await broadcast({
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "phase": "recounting",
            "checksDone": 0,
            "checksTotal": 0,
        })
        await full_dup_recount(
            location_id=location_id,
            on_progress=_on_recount_progress,
            on_total=_on_recount_total,
        )

        logger.info("Running size recalc for %s", location_name)
        await broadcast({
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "phase": "rebuilding",
        })
        await recalculate_location_sizes(location_id)

        invalidate_stats_cache()

        async with read_db() as rdb:
            dup_row = await rdb.execute_fetchall(
                "SELECT duplicate_count FROM locations WHERE id = ?", (location_id,)
            )
        final_dup_count = (dup_row[0]["duplicate_count"] or 0) if dup_row else 0

        await broadcast(
            {
                "type": "scan_completed",
                "locationId": location_id,
                "location": location_name,
                "filesFound": files_found,
                "filesHashed": hashes_applied,
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
            location_id, location_name, e, scan_id,
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
            location_id, location_name, e, exc_info=True,
        )
        raise

    finally:
        # Clean up temp DB
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


async def _stream_to_temp_db(
    tmp_path: str,
    agent_id: int,
    root_path: str,
    prefix: str | None,
    location_id: int,
    location_name: str,
) -> tuple[int, int]:
    """Stream the agent's /tree response into a temporary SQLite DB.

    Runs in a thread to avoid blocking the event loop with synchronous
    SQLite writes. Returns (total_files, total_dirs).
    """
    tmp_db = sqlite3.connect(tmp_path)
    tmp_db.execute("PRAGMA journal_mode=WAL")
    tmp_db.execute("PRAGMA synchronous=OFF")  # temp DB, speed over safety
    tmp_db.execute(
        """CREATE TABLE files (
            rel_dir TEXT NOT NULL,
            rel_path TEXT NOT NULL,
            file_size INTEGER NOT NULL,
            mtime TEXT NOT NULL,
            ctime TEXT NOT NULL,
            inode INTEGER NOT NULL
        )"""
    )
    tmp_db.execute(
        """CREATE TABLE hashes (
            rel_path TEXT PRIMARY KEY,
            hash_partial TEXT NOT NULL
        )"""
    )
    tmp_db.execute("CREATE TABLE dirs (rel_dir TEXT PRIMARY KEY)")
    tmp_db.commit()

    total_files = 0
    total_dirs = 0
    current_rel_dir = ""
    file_batch: list[tuple] = []
    last_broadcast = time.monotonic()
    hash_phase = False
    hashes_total = 0
    hash_batch: list[tuple] = []

    async for record in stream_tree(agent_id, root_path, prefix=prefix):
        from file_hunter.services.queue_manager import wait_if_paused
        await wait_if_paused()

        rtype = record.get("type")

        if rtype == "dir":
            current_rel_dir = record["rel_dir"]
            tmp_db.execute(
                "INSERT OR IGNORE INTO dirs VALUES (?)", (current_rel_dir,)
            )
            total_dirs += 1

        elif rtype == "file":
            file_batch.append((
                current_rel_dir,
                record["rel_path"],
                record["size"],
                record["mtime"],
                record["ctime"],
                record["inode"],
            ))
            total_files += 1

            # Flush batch to temp DB periodically
            if len(file_batch) >= 5000:
                tmp_db.executemany(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)", file_batch
                )
                tmp_db.commit()
                file_batch.clear()

        elif rtype == "phase":
            # Flush remaining file batch
            if file_batch:
                tmp_db.executemany(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)", file_batch
                )
                tmp_db.commit()
                file_batch.clear()
            hash_phase = True
            hashes_total = record.get("total", 0)
            logger.info(
                "Scan hash phase: %d files to hash for %s",
                hashes_total, location_name,
            )

        elif rtype == "hash":
            hash_batch.append((record["rel_path"], record["hash_partial"]))
            if len(hash_batch) >= 5000:
                tmp_db.executemany(
                    "INSERT OR REPLACE INTO hashes VALUES (?, ?)", hash_batch
                )
                tmp_db.commit()
                hash_batch.clear()

        elif rtype == "end":
            pass

        # Broadcast progress periodically
        now_mono = time.monotonic()
        if now_mono - last_broadcast >= 2.0:
            if hash_phase:
                hash_count = tmp_db.execute("SELECT COUNT(*) FROM hashes").fetchone()[0]
                await broadcast({
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "hashing",
                    "filesFound": total_files,
                    "hashesDone": hash_count,
                    "hashesTotal": hashes_total,
                })
            else:
                await broadcast({
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "scanning",
                    "filesFound": total_files,
                    "dirsFound": total_dirs,
                })
            last_broadcast = now_mono

    # Flush remaining batches
    if file_batch:
        tmp_db.executemany(
            "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?)", file_batch
        )
    if hash_batch:
        tmp_db.executemany(
            "INSERT OR REPLACE INTO hashes VALUES (?, ?)", hash_batch
        )
    tmp_db.commit()

    # Index for ingest phase
    tmp_db.execute("CREATE INDEX idx_tmp_files_dir ON files(rel_dir)")
    tmp_db.commit()
    tmp_db.close()

    return total_files, total_dirs


async def _bulk_ingest(
    tmp_path: str,
    location_id: int,
    scan_id: int,
    root_path: str,
    now_iso: str,
    location_name: str,
    total_files: int,
) -> int:
    """Bulk ingest files from temp DB into catalog.

    Uses batched INSERT ... ON CONFLICT DO UPDATE, same pattern as import.
    Returns count of new files.
    """
    tmp_db = sqlite3.connect(tmp_path)
    tmp_db.row_factory = sqlite3.Row

    # Count existing files for this location to determine new count
    async with read_db() as rdb:
        before_rows = await rdb.execute_fetchall(
            "SELECT COUNT(*) as c FROM files WHERE location_id = ? AND stale = 0",
            (location_id,),
        )
    files_before = before_rows[0]["c"]

    # Build folder hierarchy from temp DB dirs — single writer call
    folder_cache: dict[str, tuple] = {}
    dir_rows = tmp_db.execute(
        "SELECT rel_dir FROM dirs ORDER BY length(rel_dir)"
    ).fetchall()

    async with db_writer() as db:
        for row in dir_rows:
            rel_dir = row["rel_dir"]
            if not rel_dir:
                continue
            await ensure_folder_hierarchy(db, location_id, rel_dir, folder_cache)

    logger.info("Folders created for %s: %d", location_name, len(folder_cache))

    # Batch ingest files
    offset = 0
    ingested = 0
    last_broadcast = time.monotonic()

    while True:
        rows = tmp_db.execute(
            "SELECT rel_dir, rel_path, file_size, mtime, ctime, inode "
            "FROM files LIMIT ? OFFSET ?",
            (INGEST_BATCH_SIZE, offset),
        ).fetchall()

        if not rows:
            break

        batch = []
        for r in rows:
            rel_path = r["rel_path"]
            filename = os.path.basename(rel_path)
            full_path = os.path.join(root_path, rel_path)
            file_type_high, file_type_low = classify_file(rel_path)
            is_hidden = 1 if filename.startswith(".") else 0
            rel_dir = r["rel_dir"]

            folder_id = None
            dup_exclude = 0
            if rel_dir and rel_dir in folder_cache:
                folder_id, dup_exclude = folder_cache[rel_dir]

            batch.append((
                filename,
                full_path,
                rel_path,
                location_id,
                folder_id,
                file_type_high,
                file_type_low,
                r["file_size"],
                r["ctime"],
                r["mtime"],
                now_iso,
                now_iso,
                scan_id,
                is_hidden,
                dup_exclude,
                r["inode"],
            ))

        async with db_writer() as db:
            await db.executemany(
                "INSERT INTO files "
                "(filename, full_path, rel_path, location_id, folder_id, "
                "file_type_high, file_type_low, file_size, "
                "created_date, modified_date, "
                "date_cataloged, date_last_seen, scan_id, "
                "hidden, dup_exclude, inode) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(location_id, rel_path) DO UPDATE SET "
                "filename=excluded.filename, full_path=excluded.full_path, "
                "folder_id=excluded.folder_id, "
                "file_type_high=excluded.file_type_high, "
                "file_type_low=excluded.file_type_low, "
                "file_size=excluded.file_size, "
                "created_date=excluded.created_date, "
                "modified_date=excluded.modified_date, "
                "date_last_seen=excluded.date_last_seen, "
                "scan_id=excluded.scan_id, "
                "hidden=excluded.hidden, "
                "inode=excluded.inode, "
                "stale=0, "
                # Preserve hashes if file unchanged (same size + mtime)
                "hash_partial=CASE "
                "  WHEN excluded.file_size = files.file_size "
                "    AND excluded.modified_date = files.modified_date "
                "  THEN hash_partial ELSE NULL END, "
                "hash_fast=CASE "
                "  WHEN excluded.file_size = files.file_size "
                "    AND excluded.modified_date = files.modified_date "
                "  THEN hash_fast ELSE NULL END, "
                "hash_strong=CASE "
                "  WHEN excluded.file_size = files.file_size "
                "    AND excluded.modified_date = files.modified_date "
                "  THEN hash_strong ELSE NULL END",
                batch,
            )

        ingested += len(batch)
        offset += INGEST_BATCH_SIZE

        now_mono = time.monotonic()
        if now_mono - last_broadcast >= 2.0:
            logger.info(
                "Ingest progress: %d / %d files for %s",
                ingested, total_files, location_name,
            )
            await broadcast({
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "cataloging",
                "catalogDone": ingested,
                "catalogTotal": total_files,
            })
            last_broadcast = now_mono

        await asyncio.sleep(0)

    tmp_db.close()

    # Count new files
    async with read_db() as rdb:
        after_rows = await rdb.execute_fetchall(
            "SELECT COUNT(*) as c FROM files WHERE location_id = ? AND stale = 0",
            (location_id,),
        )
    files_after = after_rows[0]["c"]

    return max(0, files_after - files_before)


async def _apply_hashes_from_temp(
    tmp_path: str,
    location_id: int,
    agent_id: int,
    root_path: str,
    now_iso: str,
    location_name: str,
) -> int:
    """Apply partial hashes from temp DB to catalog, then find dup candidates.

    1. Batch-write hash_partials from temp DB to catalog
    2. Call hash_candidates_for_location() — existing shared function
       that finds candidates, copies hash_fast for small files,
       queues large files for agent hash_fast
    """
    tmp_db = sqlite3.connect(tmp_path)
    tmp_db.row_factory = sqlite3.Row

    total = tmp_db.execute("SELECT COUNT(*) FROM hashes").fetchone()[0]
    if total == 0:
        tmp_db.close()
        return 0

    logger.info("Applying %d partial hashes for %s", total, location_name)

    # Batch write hash_partials to catalog
    offset = 0
    applied = 0
    last_broadcast = time.monotonic()

    while True:
        rows = tmp_db.execute(
            "SELECT rel_path, hash_partial FROM hashes LIMIT ? OFFSET ?",
            (INGEST_BATCH_SIZE, offset),
        ).fetchall()

        if not rows:
            break

        update_batch = []
        for r in rows:
            update_batch.append((r["hash_partial"], location_id, r["rel_path"]))

        async with db_writer() as db:
            await db.executemany(
                "UPDATE files SET hash_partial = ? "
                "WHERE location_id = ? AND rel_path = ? AND stale = 0",
                update_batch,
            )

        applied += len(rows)
        offset += INGEST_BATCH_SIZE

        now_mono = time.monotonic()
        if now_mono - last_broadcast >= 2.0:
            pct = round(applied / total * 100) if total else 0
            logger.info(
                "Hash partial write: %d / %d (%d%%) for %s",
                applied, total, pct, location_name,
            )
            await broadcast({
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "cataloging_hashes",
                "catalogDone": applied,
                "catalogTotal": total,
            })
            last_broadcast = now_mono

        await asyncio.sleep(0)

    tmp_db.close()

    logger.info("Hash partials written, finding dup candidates for %s", location_name)

    await broadcast({
        "type": "scan_progress",
        "locationId": location_id,
        "location": location_name,
        "phase": "checking_duplicates",
        "checksDone": 0,
        "checksTotal": 0,
    })

    # Find and process dup candidates — reuse import's shared function
    from file_hunter.services.dup_counts import hash_candidates_for_location

    candidates_total, candidates_hashed, new_hashes = await hash_candidates_for_location(
        location_id=location_id,
        agent_id=agent_id,
    )

    logger.info(
        "Dup candidates: %d found, %d hashed for %s",
        candidates_total, candidates_hashed, location_name,
    )

    return applied


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
            async with read_db() as rdb:
                rows = await rdb.execute_fetchall(
                    "SELECT id, file_id, full_path, inode FROM pending_hashes "
                    "WHERE agent_id = ? AND location_id = ? "
                    "ORDER BY inode "
                    "LIMIT ?",
                    (agent_id, location_id, BATCH_SIZE),
                )

            if not rows:
                if scan_done.is_set():
                    logger.info("Hash drainer: done for %s", location_name)
                    return
                await asyncio.sleep(2)
                continue

            paths = [r["full_path"] for r in rows]
            path_to_file_id = {r["full_path"]: r["file_id"] for r in rows}
            pending_ids = [r["id"] for r in rows]

            try:
                result = await hash_fast_batch(agent_id, paths)
            except (ConnectionError, OSError, httpx.ConnectError):
                logger.warning(
                    "Hash drainer: agent %d offline, stopping", agent_id
                )
                return

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

            if pending_ids:
                for i in range(0, len(pending_ids), 500):
                    batch = pending_ids[i : i + 500]
                    ph = ",".join("?" for _ in batch)
                    async with db_writer() as db:
                        await db.execute(
                            f"DELETE FROM pending_hashes WHERE id IN ({ph})",
                            batch,
                        )

            if affected_fast:
                submit_hashes_for_recalc(
                    strong_hashes=None,
                    fast_hashes=affected_fast,
                    source=f"hash drainer {location_name}",
                    location_ids={location_id},
                )

            logger.info(
                "Hash drainer: %d hashed for %s",
                len(hash_results), location_name,
            )

            await asyncio.sleep(0)

    except asyncio.CancelledError:
        return


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
