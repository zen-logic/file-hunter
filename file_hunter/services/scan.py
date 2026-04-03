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
import time
from pathlib import Path
from datetime import datetime, timezone

import httpx

from file_hunter.db import db_writer, open_connection, read_db
from file_hunter.hashes_db import (
    clear_hashes_stale,
    hashes_writer,
    mark_hashes_stale,
    open_hashes_connection,
)
from file_hunter.helpers import post_op_stats
from file_hunter.services.activity import update as activity_update
from file_hunter.services.agent_ops import hash_partial_batch, stream_tree
from file_hunter.services.dup_counts import (
    HASH_BATCH_BYTES,
    drain_pending_hashes,
    post_ingest_dup_processing,
)
from file_hunter.services.queue_manager import wait_if_paused
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    mark_stale_files,
    mark_stale_folders,
)
from file_hunter.services.settings import get_setting
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.stats_db import apply_file_deltas, read_stats as read_stats_db
from file_hunter.ws.scan import broadcast
from file_hunter_core.classify import classify_file

logger = logging.getLogger("file_hunter")

INGEST_BATCH_SIZE = 2000

# Temp DB directory — relative to package root, same as catalog DB
_TEMP_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "temp"


async def run_scan(op_id: int, agent_id: int, params: dict):
    """Execute a full scan or rescan for a location by consuming the agent's /tree stream.

    Orchestrates the entire scan lifecycle: stream capture, catalog update (ingest or
    diff), dup candidate detection, stale marking, stats reconciliation, and cleanup.
    Branches into two paths depending on whether the location has been scanned before:

    - **First scan**: streams metadata+hashes into temp DB, bulk-inserts into catalog,
      then marks stale files and runs dup processing.
    - **Rescan**: streams metadata only into temp DB, diffs against existing catalog
      (new/changed/stale/recovered), hashes new+changed files via agent, then runs
      dup processing.

    A concurrent ``drain_pending_hashes`` task runs alongside to process dup candidates
    as they arrive.

    Args:
        op_id: Row ID in operation_queue for this operation. Used for crash recovery
            (params are persisted back to the queue row).
        agent_id: ID of the agent to stream from.
        params: Dict containing:
            - location_id (int): Target location.
            - path (str, optional): Subfolder to scan. Falls back to root_path.
            - root_path (str): Location root for rel_path computation.
            - scan_id (int, optional): If present, resumes a previously interrupted scan.

    Returns:
        None.

    Side effects:
        - DB writes: creates/updates scans row, inserts/updates files and folders in
          catalog, writes hashes to hashes.db, updates stats.db incrementally, persists
          scan_id and tmp_path to operation_queue params for crash recovery.
        - WebSocket broadcasts: scan_started, scan_progress (multiple phases),
          scan_finalizing, scan_completed, location_children. On failure: scan_cancelled,
          scan_interrupted, or scan_error.
        - File I/O: creates and removes a temp SQLite DB in data/temp/.
        - Spawns drain_pending_hashes as a concurrent asyncio task.

    Called by queue_manager as the ``scan_dir`` operation handler.
    """
    location_id = params["location_id"]
    scan_path = params.get("path") or params["root_path"]
    root_path = params["root_path"]

    # --- Initialisation ---
    async with db_writer() as db:
        loc_row = await db.execute_fetchall(
            "SELECT name, file_count, total_size, type_counts, date_last_scanned "
            "FROM locations WHERE id = ?",
            (location_id,),
        )
        location_name = params.get("location_name") or (
            loc_row[0]["name"] if loc_row else f"Location #{location_id}"
        )
        is_rescan = bool(loc_row and loc_row[0]["date_last_scanned"])

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

    await broadcast(
        {
            "type": "scan_started",
            "locationId": location_id,
            "location": location_name,
            "scanId": scan_id,
        }
    )

    # Save scan_id and temp DB path to params for crash recovery
    params["scan_id"] = scan_id
    # Create temp DB for fast stream capture — in data/temp/ for persistence
    _TEMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = str(_TEMP_DIR / f"scan-{location_id}-{scan_id}.db")
    params["tmp_path"] = tmp_path
    async with db_writer() as db:
        await db.execute(
            "UPDATE operation_queue SET params = ? WHERE id = ?",
            (json.dumps(params), op_id),
        )

    # Launch hash drainer as concurrent task
    scan_done_event = asyncio.Event()

    async def _drainer_progress(done, total):
        pct = f" ({round(done / total * 100)}%)" if total > 0 else ""
        activity_update(
            _act_name,
            progress=f"confirming duplicates: {done:,} / {total:,}{pct}",
        )
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "checking_duplicates",
                "checksDone": done,
                "checksTotal": total,
            }
        )

    drainer_task = asyncio.create_task(
        drain_pending_hashes(
            agent_id,
            location_id,
            location_name,
            scan_done=scan_done_event,
            on_progress=_drainer_progress,
        )
    )

    # State — initialised before try so exception handlers can reference them
    files_found = 0
    files_new = 0
    candidates_total = 0
    stale_count = 0

    try:
        if is_rescan:
            # === RESCAN PATH ===
            logger.info("Rescan starting for %s", location_name)
            _act_name = f"op-{op_id}"

            activity_update(
                _act_name, label=f"Scanning {location_name}", progress="tree walk"
            )

            # --- Phase 1: stream metadata only into temp DB ---
            files_found, dirs_found = await _stream_to_temp_db(
                tmp_path,
                agent_id,
                root_path,
                prefix_for_agent,
                location_id,
                location_name,
                metadata_only=True,
            )

            logger.info(
                "Rescan stream captured: %d files, %d dirs for %s",
                files_found,
                dirs_found,
                location_name,
            )
            activity_update(_act_name, progress=f"{files_found:,} files, diffing")

            # --- Phase 2: diff temp DB against catalog, apply changes ---
            (
                new_count,
                changed_count,
                stale_count,
                recovered_count,
                affected_file_ids,
            ) = await _diff_and_update(
                tmp_path,
                location_id,
                scan_id,
                root_path,
                now_iso,
                agent_id,
                location_name,
                files_found,
                scan_prefix=scan_prefix,
            )
            files_new = new_count

            logger.info(
                "Rescan diff complete: %d new, %d changed, %d stale, %d recovered for %s",
                new_count,
                changed_count,
                stale_count,
                recovered_count,
                location_name,
            )
            activity_update(
                _act_name, progress=f"+{new_count:,} new, {changed_count:,} changed"
            )

            # --- Phase 3: find dup candidates for new/changed/recovered files ---
            if affected_file_ids:
                activity_update(_act_name, progress="checking duplicates")
                candidates_total = await post_ingest_dup_processing(
                    location_id,
                    agent_id,
                    location_name,
                    file_ids=affected_file_ids,
                )

            # --- Phase 3b: catch unprocessed files from interrupted scans ---
            # Files may have hash_partial but no hash_fast if a previous scan
            # was interrupted during dup processing. A user-initiated rescan
            # should leave things correct regardless of prior history.
            hconn = await open_hashes_connection()
            try:
                unprocessed = await hconn.execute_fetchall(
                    "SELECT file_id FROM active_hashes "
                    "WHERE location_id = ? AND hash_fast IS NULL "
                    "AND hash_partial IS NOT NULL",
                    (location_id,),
                )
            finally:
                await hconn.close()

            unprocessed_ids = [r["file_id"] for r in unprocessed]
            if unprocessed_ids:
                logger.info(
                    "Rescan: %d unprocessed files (hash_partial, no hash_fast) "
                    "for %s — running dup processing",
                    len(unprocessed_ids),
                    location_name,
                )
                activity_update(_act_name, progress="checking duplicates")
                candidates_total = await post_ingest_dup_processing(
                    location_id,
                    agent_id,
                    location_name,
                    file_ids=unprocessed_ids,
                )

        else:
            # === FIRST SCAN PATH ===
            _act_name = f"op-{op_id}"
            activity_update(
                _act_name, label=f"Scanning {location_name}", progress="tree walk"
            )

            # --- Phase 1: stream metadata + hashes into temp DB ---
            files_found, dirs_found = await _stream_to_temp_db(
                tmp_path,
                agent_id,
                root_path,
                prefix_for_agent,
                location_id,
                location_name,
            )

            logger.info(
                "Stream captured: %d files, %d dirs for %s",
                files_found,
                dirs_found,
                location_name,
            )
            activity_update(_act_name, progress=f"{files_found:,} files, ingesting")

            # --- Phase 2: bulk ingest from temp DB into catalog ---
            files_new = await _bulk_ingest(
                tmp_path,
                location_id,
                scan_id,
                root_path,
                now_iso,
                location_name,
                files_found,
            )

            logger.info(
                "Ingest complete: %d files (%d new) for %s",
                files_found,
                files_new,
                location_name,
            )

            # Broadcast root folders so frontend can populate the tree
            await _broadcast_location_children(location_id)

            # --- Phase 3: find dup candidates and queue for hashing ---
            activity_update(
                _act_name, progress=f"{files_found:,} files, checking duplicates"
            )
            candidates_total = await post_ingest_dup_processing(
                location_id,
                agent_id,
                location_name,
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
            if not is_rescan:
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

        # Wait for hash drainer to finish remaining work
        scan_done_event.set()
        if not drainer_task.done():
            activity_update(_act_name, progress="hashing")
        logger.info("Waiting for hash drainer to finish for %s", location_name)
        try:
            await drainer_task
        except asyncio.CancelledError:
            pass
        except Exception as drainer_err:
            logger.warning(
                "Hash drainer failed for %s: %s — scan data is intact, "
                "pending hashes will be retried next scan",
                location_name,
                drainer_err,
            )
        logger.info("Hash drainer finished for %s", location_name)

        await recalculate_location_sizes(location_id)
        await post_op_stats()

        scan_folder_id = params.get("folder_id")
        if scan_folder_id:
            # Subfolder scan — report folder-scoped stats
            async with read_db() as rdb:
                fld_stats = await rdb.execute_fetchall(
                    "SELECT duplicate_count, total_size FROM folders WHERE id = ?",
                    (scan_folder_id,),
                )
            final_dup_count = (fld_stats[0]["duplicate_count"] or 0) if fld_stats else 0
            final_total_size = (fld_stats[0]["total_size"] or 0) if fld_stats else 0
        else:
            # Full location scan — report location stats
            async with read_stats_db() as sdb:
                loc_stats = await sdb.execute_fetchall(
                    "SELECT duplicate_count, total_size FROM location_stats WHERE location_id = ?",
                    (location_id,),
                )
            final_dup_count = (loc_stats[0]["duplicate_count"] or 0) if loc_stats else 0
            final_total_size = (loc_stats[0]["total_size"] or 0) if loc_stats else 0

        await broadcast(
            {
                "type": "scan_completed",
                "locationId": location_id,
                "location": location_name,
                "filesFound": files_found,
                "filesHashed": candidates_total,
                "filesNew": files_new,
                "staleFiles": stale_count,
                "duplicatesFound": final_dup_count,
                "totalSize": final_total_size,
            }
        )

        # Success — clean up temp DB
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    except asyncio.CancelledError:
        drainer_task.cancel()
        completed_iso = _now()
        async with db_writer() as db:
            await db.execute(
                "UPDATE scans SET status = 'cancelled', completed_at = ? WHERE id = ?",
                (completed_iso, scan_id),
            )
            await db.execute(
                "DELETE FROM pending_hashes WHERE location_id = ?",
                (location_id,),
            )
        await post_op_stats()
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
        await post_op_stats()
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
        pass  # Temp DB preserved for resume on cancel/interrupt/error


async def _stream_to_temp_db(
    tmp_path: str,
    agent_id: int,
    root_path: str,
    prefix: str | None,
    location_id: int,
    location_name: str,
    metadata_only: bool = False,
) -> tuple[int, int]:
    """Stream the agent's /tree endpoint into a temporary SQLite DB for later ingest or diff.

    Consumes the NDJSON stream from ``stream_tree()`` and writes records into a local
    SQLite temp DB (synchronous writes — fast, no catalog lock contention). Handles
    four record types from the agent: ``dir``, ``file``, ``phase`` (transition to hash
    phase), and ``hash`` (partial hash results). Batches writes (5000 records) and
    indexes the temp DB on completion for efficient ingest/diff queries.

    Respects pause state via ``wait_if_paused()`` between records.

    Args:
        tmp_path: Filesystem path for the temporary SQLite DB. Any existing file at
            this path is deleted first (stale from interrupted scans).
        agent_id: ID of the agent to stream from.
        root_path: Location root path, passed to the agent's /tree endpoint.
        prefix: Subfolder prefix for subfolder scans (None for full scans).
        location_id: Target location ID (used for WebSocket broadcasts).
        location_name: Human-readable location name (used for broadcasts and logging).
        metadata_only: If True, agent skips the hash phase and only streams D+F records.
            Used by rescan path (hashing is done separately in _diff_and_update).

    Returns:
        tuple[int, int]: (total_files, total_dirs) captured in the temp DB.

    Side effects:
        - File I/O: creates a SQLite DB at tmp_path with ``files`` and ``dirs`` tables.
        - WebSocket broadcasts: periodic scan_progress messages (every 2 seconds) with
          scanning phase (files/dirs found) or hashing phase (hashes done/total).
        - No catalog writes — all data stays in the temp DB.

    Called by run_scan for both first-scan and rescan paths.
    """
    # Remove stale temp file from interrupted scans
    if os.path.exists(tmp_path):
        os.unlink(tmp_path)

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
            inode INTEGER NOT NULL,
            hash_partial TEXT
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

    async for record in stream_tree(
        agent_id, root_path, prefix=prefix, metadata_only=metadata_only
    ):
        await wait_if_paused()

        rtype = record.get("type")

        if rtype == "dir":
            current_rel_dir = record["rel_dir"]
            tmp_db.execute("INSERT OR IGNORE INTO dirs VALUES (?)", (current_rel_dir,))
            total_dirs += 1

        elif rtype == "file":
            file_batch.append(
                (
                    current_rel_dir,
                    record["rel_path"],
                    record["size"],
                    record["mtime"],
                    record["ctime"],
                    record["inode"],
                    None,  # hash_partial — filled in by H records
                )
            )
            total_files += 1

            # Flush batch to temp DB periodically
            if len(file_batch) >= 5000:
                tmp_db.executemany(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)", file_batch
                )
                tmp_db.commit()
                file_batch.clear()

        elif rtype == "phase":
            # Flush remaining file batch and index for hash lookups
            if file_batch:
                tmp_db.executemany(
                    "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)", file_batch
                )
                tmp_db.commit()
                file_batch.clear()
            tmp_db.execute(
                "CREATE INDEX IF NOT EXISTS idx_tmp_files_relpath ON files(rel_path)"
            )
            tmp_db.commit()
            hash_phase = True
            hashes_total = record.get("total", 0)
            logger.info(
                "Scan hash phase: %d files to hash for %s",
                hashes_total,
                location_name,
            )

        elif rtype == "hash":
            hash_batch.append((record["hash_partial"], record["rel_path"]))
            if len(hash_batch) >= 5000:
                tmp_db.executemany(
                    "UPDATE files SET hash_partial = ? WHERE rel_path = ?",
                    hash_batch,
                )
                tmp_db.commit()
                hash_batch.clear()

        elif rtype == "end":
            pass

        # Broadcast progress periodically
        now_mono = time.monotonic()
        if now_mono - last_broadcast >= 2.0:
            if hash_phase:
                if hash_batch:
                    tmp_db.executemany(
                        "UPDATE files SET hash_partial = ? WHERE rel_path = ?",
                        hash_batch,
                    )
                    tmp_db.commit()
                    hash_batch.clear()
                hash_count = tmp_db.execute(
                    "SELECT COUNT(*) FROM files WHERE hash_partial IS NOT NULL"
                ).fetchone()[0]
                await broadcast(
                    {
                        "type": "scan_progress",
                        "locationId": location_id,
                        "location": location_name,
                        "phase": "hashing",
                        "filesFound": total_files,
                        "hashesDone": hash_count,
                        "hashesTotal": hashes_total,
                    }
                )
            else:
                await broadcast(
                    {
                        "type": "scan_progress",
                        "locationId": location_id,
                        "location": location_name,
                        "phase": "scanning",
                        "filesFound": total_files,
                        "dirsFound": total_dirs,
                    }
                )
            last_broadcast = now_mono

    # Flush remaining batches
    if file_batch:
        tmp_db.executemany("INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)", file_batch)
    if hash_batch:
        tmp_db.executemany(
            "UPDATE files SET hash_partial = ? WHERE rel_path = ?",
            hash_batch,
        )
    tmp_db.commit()

    # Indexes for ingest and diff phases
    tmp_db.execute("CREATE INDEX IF NOT EXISTS idx_tmp_files_dir ON files(rel_dir)")
    tmp_db.execute(
        "CREATE INDEX IF NOT EXISTS idx_tmp_files_relpath ON files(rel_path)"
    )
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
    """Bulk-ingest all files from the temp DB into the catalog (first-scan path).

    Reads files from the temp DB in batches of INGEST_BATCH_SIZE, inserts them into
    the catalog via INSERT ... ON CONFLICT DO UPDATE (upsert), and registers hash
    entries in hashes.db. Builds the folder hierarchy first, then ingests files with
    incremental stats updates via ``apply_file_deltas`` after each batch.

    New file count is determined by comparing non-stale file counts before and after
    ingest (not by tracking INSERT vs UPDATE, since upserts blur the distinction).

    Args:
        tmp_path: Path to the temp SQLite DB produced by _stream_to_temp_db.
        location_id: Target location ID.
        scan_id: Current scan row ID (stamped on every file as scan_id).
        root_path: Location root path for constructing full_path values.
        now_iso: ISO timestamp for date_cataloged and date_last_seen.
        location_name: Human-readable name for logging and broadcasts.
        total_files: Total file count from temp DB (used for progress denominator).

    Returns:
        int: Number of new (previously uncatalogued) files added.

    Side effects:
        - DB writes: upserts files into catalog via db_writer, creates folder hierarchy
          via ensure_folder_hierarchy, inserts/updates file_hashes in hashes.db.
        - Stats: calls apply_file_deltas on stats.db after each batch (incremental,
          no full recalc).
        - WebSocket broadcasts: periodic scan_progress (cataloging phase) with live
          file counts and global totals every 2 seconds.

    Called by run_scan on the first-scan path only (not rescan).
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

    # Build folder_parents for stats cascade
    async with read_db() as rdb:
        fp_rows = await rdb.execute_fetchall(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
            (location_id,),
        )
    folder_parents = {r["id"]: r["parent_id"] for r in fp_rows}

    # Batch ingest files — with real-time stats updates
    offset = 0
    ingested = 0
    last_broadcast = time.monotonic()

    while True:
        rows = tmp_db.execute(
            "SELECT rel_dir, rel_path, file_size, mtime, ctime, inode, hash_partial "
            "FROM files LIMIT ? OFFSET ?",
            (INGEST_BATCH_SIZE, offset),
        ).fetchall()

        if not rows:
            break

        batch = []
        batch_deltas: list[tuple] = []  # (folder_id, file_size, type_high, is_hidden)
        batch_hashes: list[tuple] = []  # (rel_path, file_size, hash_partial)
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

            batch.append(
                (
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
                )
            )

            batch_deltas.append((folder_id, r["file_size"], file_type_high, is_hidden))

            # Collect hash data for hashes.db
            if r["hash_partial"] or r["file_size"] > 0:
                batch_hashes.append(
                    (
                        rel_path,
                        r["file_size"],
                        r["hash_partial"],
                    )
                )

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
                "stale=0",
                batch,
            )

        # Register hashes in hashes.db directly
        if batch_hashes:
            # Resolve file_ids from catalog for this batch
            rel_paths = [h[0] for h in batch_hashes]
            hash_by_rel = {h[0]: h for h in batch_hashes}
            async with read_db() as rdb:
                ph = ",".join("?" for _ in rel_paths)
                id_rows = await rdb.execute_fetchall(
                    f"SELECT id, rel_path, file_size FROM files "
                    f"WHERE location_id = ? AND rel_path IN ({ph})",
                    [location_id] + rel_paths,
                )
            hashes_to_insert = []
            for ir in id_rows:
                h = hash_by_rel.get(ir["rel_path"])
                if h:
                    hashes_to_insert.append(
                        (
                            ir["id"],
                            location_id,
                            ir["file_size"],
                            h[2],
                            None,
                            None,  # hash_partial, hash_fast, hash_strong
                        )
                    )
            if hashes_to_insert:
                async with hashes_writer() as hdb:
                    await hdb.executemany(
                        "INSERT INTO file_hashes "
                        "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                        "VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(file_id) DO UPDATE SET "
                        "file_size=excluded.file_size, "
                        "hash_partial=COALESCE(excluded.hash_partial, file_hashes.hash_partial), "
                        "hash_fast=COALESCE(excluded.hash_fast, file_hashes.hash_fast), "
                        "hash_strong=COALESCE(excluded.hash_strong, file_hashes.hash_strong)",
                        hashes_to_insert,
                    )

        # Update stats.db with deltas — runs on stats writer, no catalog contention
        live_totals = await apply_file_deltas(
            location_id,
            folder_parents,
            added=batch_deltas,
        )

        ingested += len(batch)
        offset += INGEST_BATCH_SIZE

        now_mono = time.monotonic()
        if now_mono - last_broadcast >= 2.0:
            logger.info(
                "Ingest progress: %d / %d files for %s",
                ingested,
                total_files,
                location_name,
            )

            # Read global totals from stats.db for status bar
            async with read_stats_db() as sdb:
                global_row = await sdb.execute_fetchall(
                    "SELECT COALESCE(SUM(file_count), 0) as fc, "
                    "COALESCE(SUM(total_size), 0) as ts "
                    "FROM location_stats"
                )
            global_fc = global_row[0]["fc"] if global_row else 0
            global_ts = global_row[0]["ts"] if global_row else 0

            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "cataloging",
                    "catalogDone": ingested,
                    "catalogTotal": total_files,
                    "fileCount": live_totals.get("fileCount", ingested)
                    if live_totals
                    else ingested,
                    "totalSize": live_totals.get("totalSize", 0) if live_totals else 0,
                    "folderCount": len(folder_cache),
                    "globalFileCount": global_fc,
                    "globalTotalSize": global_ts,
                }
            )
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


async def _diff_and_update(
    tmp_path: str,
    location_id: int,
    scan_id: int,
    root_path: str,
    now_iso: str,
    agent_id: int,
    location_name: str,
    total_files: int,
    scan_prefix: str | None = None,
) -> tuple[int, int, int, int, list[int]]:
    """Diff the temp DB against the existing catalog and apply only the delta (rescan path).

    Uses ATTACH DATABASE on a dedicated read connection to efficiently JOIN the temp DB
    against the catalog in SQL, identifying four categories of files:
    - **New**: present in temp DB but absent from catalog.
    - **Changed**: present in both but size or mtime differs.
    - **Stale**: present in catalog but absent from temp DB (scoped to scan_prefix).
    - **Recovered**: marked stale in catalog but found again on disk.

    After the diff, applies changes through db_writer in batches of 5000:
    stale marking, recovery (un-stale), scan_id stamping on seen files, new file
    insertion, changed file updates, and hash clearing for changed files. Then
    requests hash_partial from the agent for new+changed files (batched by bytes)
    and writes results to hashes.db.

    Stats are updated incrementally via apply_file_deltas for each category
    (stale removed, new added, changed = remove old + add new).

    Args:
        tmp_path: Path to the temp SQLite DB from _stream_to_temp_db.
        location_id: Target location ID.
        scan_id: Current scan row ID.
        root_path: Location root path for full_path construction and rel_path hashing.
        now_iso: ISO timestamp for date_last_seen.
        agent_id: Agent ID for hash_partial_batch requests.
        location_name: Human-readable name for logging and broadcasts.
        total_files: Total files captured in temp DB (for logging context).
        scan_prefix: If set, scopes stale/recovery detection to this subfolder.
            None for full-location rescans.

    Returns:
        tuple[int, int, int, int]: (new_count, changed_count, stale_count,
        recovered_count). Note: the type annotation on the function signature says
        tuple[int, int, int] but actually returns four values.

    Side effects:
        - DB writes: updates stale flags, scan_id, date_last_seen on catalog files;
          inserts new files; updates changed files; creates folder hierarchy; marks
          stale folders. Writes hash_partial values to hashes.db. Clears hashes for
          changed files.
        - Stats: incremental deltas via apply_file_deltas for stale removals, new
          additions, and changed file size adjustments.
        - WebSocket broadcasts: scan_progress (comparing, cataloging, hashing phases),
          location_children at completion.
        - Opens a dedicated read connection (open_connection) for ATTACH queries —
          does not use the shared reader.

    Called by run_scan on the rescan path only.
    """
    affected_file_ids: list[int] = []

    # --- Phase 2a: ensure folder hierarchy ---
    tmp_db = sqlite3.connect(tmp_path)
    tmp_db.row_factory = sqlite3.Row
    dir_rows = tmp_db.execute(
        "SELECT rel_dir FROM dirs ORDER BY length(rel_dir)"
    ).fetchall()
    tmp_db.close()

    folder_cache: dict[str, tuple] = {}
    async with db_writer() as db:
        for row in dir_rows:
            rel_dir = row["rel_dir"]
            if not rel_dir:
                continue
            await ensure_folder_hierarchy(db, location_id, rel_dir, folder_cache)

    logger.info("Rescan folders ensured for %s: %d", location_name, len(folder_cache))

    # Mark folders not seen on disk as stale
    seen_folder_paths = {row["rel_dir"] for row in dir_rows if row["rel_dir"]}
    async with db_writer() as db:
        await mark_stale_folders(db, location_id, seen_folder_paths, scan_prefix)

    # Build folder_parents for stats cascade
    async with read_db() as rdb:
        fp_rows = await rdb.execute_fetchall(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
            (location_id,),
        )
    folder_parents = {r["id"]: r["parent_id"] for r in fp_rows}

    # --- Phase 2b: diff using ATTACH ---
    await broadcast(
        {
            "type": "scan_progress",
            "locationId": location_id,
            "location": location_name,
            "phase": "comparing",
        }
    )

    # Use dedicated read connection with ATTACH for diff queries
    conn = await open_connection()
    try:
        await conn.execute(f"ATTACH DATABASE '{tmp_path}' AS temp_scan")

        # New files: in temp DB but not in catalog
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "comparing",
                "compareStep": "Finding new files...",
            }
        )
        new_rows = await conn.execute_fetchall(
            "SELECT t.rel_dir, t.rel_path, t.file_size, t.mtime, t.ctime, t.inode "
            "FROM temp_scan.files t "
            "LEFT JOIN main.files c "
            "  ON c.location_id = ? AND c.rel_path = t.rel_path "
            "WHERE c.id IS NULL",
            (location_id,),
        )
        new_count = len(new_rows)
        logger.info("Rescan diff: %d new files for %s", new_count, location_name)

        # Changed files: in both but size or mtime differs
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "comparing",
                "compareStep": f"{new_count:,} new — finding changed files...",
            }
        )
        changed_rows = await conn.execute_fetchall(
            "SELECT t.rel_dir, t.rel_path, t.file_size, t.mtime, t.ctime, t.inode, "
            "c.id as file_id, c.file_size as old_size, c.file_type_high as old_type, "
            "c.folder_id as old_folder_id, c.hidden as old_hidden "
            "FROM temp_scan.files t "
            "INNER JOIN main.files c "
            "  ON c.location_id = ? AND c.rel_path = t.rel_path "
            "WHERE c.stale = 0 "
            "  AND (c.file_size != t.file_size OR c.modified_date != t.mtime)",
            (location_id,),
        )
        changed_count = len(changed_rows)
        logger.info(
            "Rescan diff: %d changed files for %s", changed_count, location_name
        )

        # Stale files: in catalog but not in temp DB (with details for stats)
        # Scoped to scan_prefix for subfolder scans
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "comparing",
                "compareStep": f"{new_count:,} new, {changed_count:,} changed — finding stale files...",
            }
        )
        stale_scope = ""
        stale_params: list = [location_id]
        if scan_prefix:
            stale_scope = (
                " AND c.folder_id IN ("
                "SELECT id FROM main.folders "
                "WHERE location_id = ? AND (rel_path = ? OR rel_path LIKE ?)"
                ")"
            )
            stale_params.extend([location_id, scan_prefix, scan_prefix + "/%"])
        stale_ids = await conn.execute_fetchall(
            "SELECT c.id, c.folder_id, c.file_size, c.file_type_high, c.hidden "
            "FROM main.files c "
            "LEFT JOIN temp_scan.files t ON t.rel_path = c.rel_path "
            f"WHERE c.location_id = ? AND c.stale = 0 AND t.rel_path IS NULL{stale_scope}",
            stale_params,
        )
        stale_count = len(stale_ids)
        logger.info("Rescan diff: %d stale files for %s", stale_count, location_name)

        # Recovered files: stale in catalog but exist on disk (in temp DB)
        # Un-stale them and update scan_id
        recovered_scope = ""
        recovered_params: list = [location_id]
        if scan_prefix:
            recovered_scope = (
                " AND c.folder_id IN ("
                "SELECT id FROM main.folders "
                "WHERE location_id = ? AND (rel_path = ? OR rel_path LIKE ?)"
                ")"
            )
            recovered_params.extend([location_id, scan_prefix, scan_prefix + "/%"])
        recovered_rows = await conn.execute_fetchall(
            "SELECT c.id "
            "FROM main.files c "
            "INNER JOIN temp_scan.files t ON t.rel_path = c.rel_path "
            f"WHERE c.location_id = ? AND c.stale = 1{recovered_scope}",
            recovered_params,
        )
        recovered_count = len(recovered_rows)
        if recovered_count > 0:
            logger.info(
                "Rescan diff: %d recovered (un-staled) files for %s",
                recovered_count,
                location_name,
            )

        await conn.execute("DETACH DATABASE temp_scan")
    finally:
        await conn.close()

    # Write stale marks and scan_id updates through db_writer
    if stale_ids:
        stale_id_list = [r["id"] for r in stale_ids]
        for i in range(0, len(stale_id_list), 5000):
            batch = stale_id_list[i : i + 5000]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                await db.execute(
                    f"UPDATE files SET stale = 1, scan_id = ? WHERE id IN ({ph})",
                    [scan_id] + batch,
                )
            await asyncio.sleep(0)

        # Flag stale in hashes.db (preserves hash data for recovery)
        await mark_hashes_stale(stale_id_list)

        # Stats: remove stale file deltas
        stale_removed = [
            (r["folder_id"], r["file_size"] or 0, r["file_type_high"], r["hidden"])
            for r in stale_ids
        ]
        await apply_file_deltas(location_id, folder_parents, removed=stale_removed)

    # Un-stale recovered files
    if recovered_rows:
        recovered_id_list = [r["id"] for r in recovered_rows]
        affected_file_ids.extend(recovered_id_list)
        for i in range(0, len(recovered_id_list), 5000):
            batch = recovered_id_list[i : i + 5000]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                await db.execute(
                    f"UPDATE files SET stale = 0, scan_id = ? WHERE id IN ({ph})",
                    [scan_id] + batch,
                )
            await asyncio.sleep(0)

        # Clear stale flag in hashes.db — hash data was preserved
        await clear_hashes_stale(recovered_id_list)

    # Mark all seen files with current scan_id — batched to avoid holding writer
    # Scoped to scan_prefix for subfolder scans (don't touch files we didn't scan)
    seen_scope = ""
    seen_params: list = [location_id]
    if scan_prefix:
        seen_scope = (
            " AND folder_id IN ("
            "SELECT id FROM folders "
            "WHERE location_id = ? AND (rel_path = ? OR rel_path LIKE ?)"
            ")"
        )
        seen_params.extend([location_id, scan_prefix, scan_prefix + "/%"])
    async with read_db() as rdb:
        seen_ids = await rdb.execute_fetchall(
            f"SELECT id FROM files WHERE location_id = ? AND stale = 0{seen_scope}",
            seen_params,
        )
    if seen_ids:
        seen_id_list = [r["id"] for r in seen_ids]
        for i in range(0, len(seen_id_list), 5000):
            batch = seen_id_list[i : i + 5000]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                await db.execute(
                    f"UPDATE files SET scan_id = ?, date_last_seen = ? "
                    f"WHERE id IN ({ph})",
                    [scan_id, now_iso] + batch,
                )
            await asyncio.sleep(0)

    # --- Phase 2c: insert new files ---
    if new_rows:
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "cataloging",
                "catalogDone": 0,
                "catalogTotal": new_count,
            }
        )

        for i in range(0, len(new_rows), INGEST_BATCH_SIZE):
            batch_rows = new_rows[i : i + INGEST_BATCH_SIZE]
            batch = []
            for r in batch_rows:
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

                batch.append(
                    (
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
                    )
                )

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
                    "inode=excluded.inode, stale=0",
                    batch,
                )

            # Stats: new files added
            batch_deltas = [
                (b[4], b[7], b[5], b[13])  # folder_id, file_size, type_high, hidden
                for b in batch
            ]
            await apply_file_deltas(location_id, folder_parents, added=batch_deltas)

            # Register in hashes.db (no hash values yet — populated in phase 2e)
            rel_paths = [b[2] for b in batch]  # rel_path
            async with read_db() as rdb:
                ph = ",".join("?" for _ in rel_paths)
                id_rows = await rdb.execute_fetchall(
                    f"SELECT id, file_size FROM files "
                    f"WHERE location_id = ? AND rel_path IN ({ph})",
                    [location_id] + rel_paths,
                )
            if id_rows:
                h_batch = [
                    (ir["id"], location_id, ir["file_size"], None, None, None)
                    for ir in id_rows
                ]
                affected_file_ids.extend(ir["id"] for ir in id_rows)
                async with hashes_writer() as hdb:
                    await hdb.executemany(
                        "INSERT INTO file_hashes "
                        "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                        "VALUES (?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(file_id) DO UPDATE SET file_size=excluded.file_size",
                        h_batch,
                    )

            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "cataloging",
                    "catalogDone": min(i + INGEST_BATCH_SIZE, new_count),
                    "catalogTotal": new_count,
                }
            )
            await asyncio.sleep(0)

    # --- Phase 2d: update changed files ---
    if changed_rows:
        affected_file_ids.extend(r["file_id"] for r in changed_rows)
        for i in range(0, len(changed_rows), INGEST_BATCH_SIZE):
            batch_rows = changed_rows[i : i + INGEST_BATCH_SIZE]
            update_batch = []
            for r in batch_rows:
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

                update_batch.append(
                    (
                        filename,
                        full_path,
                        folder_id,
                        file_type_high,
                        file_type_low,
                        r["file_size"],
                        r["ctime"],
                        r["mtime"],
                        now_iso,
                        scan_id,
                        is_hidden,
                        r["inode"],
                        r["file_id"],
                    )
                )

            async with db_writer() as db:
                await db.executemany(
                    "UPDATE files SET "
                    "filename=?, full_path=?, folder_id=?, "
                    "file_type_high=?, file_type_low=?, file_size=?, "
                    "created_date=?, modified_date=?, date_last_seen=?, scan_id=?, "
                    "hidden=?, inode=?, "
                    "stale=0 "
                    "WHERE id=?",
                    update_batch,
                )

            # Clear hashes in hashes.db for changed files (need re-hashing)
            changed_ids = [r["file_id"] for r in batch_rows]
            for ci in range(0, len(changed_ids), 500):
                cbatch = changed_ids[ci : ci + 500]
                cph = ",".join("?" for _ in cbatch)
                async with hashes_writer() as hdb:
                    await hdb.execute(
                        f"UPDATE file_hashes SET hash_partial=NULL, hash_fast=NULL, "
                        f"hash_strong=NULL WHERE file_id IN ({cph})",
                        cbatch,
                    )
            await asyncio.sleep(0)

        # Stats: changed files — remove old sizes, add new sizes
        changed_removed = [
            (r["old_folder_id"], r["old_size"] or 0, r["old_type"], r["old_hidden"])
            for r in changed_rows
        ]
        changed_added = []
        for r in changed_rows:
            rel_dir = r["rel_dir"]
            folder_id = None
            if rel_dir and rel_dir in folder_cache:
                folder_id = folder_cache[rel_dir][0]
            file_type_high = classify_file(r["rel_path"])[0]
            is_hidden = 1 if os.path.basename(r["rel_path"]).startswith(".") else 0
            changed_added.append((folder_id, r["file_size"], file_type_high, is_hidden))

        await apply_file_deltas(
            location_id,
            folder_parents,
            removed=changed_removed,
            added=changed_added,
        )

        logger.info(
            "Rescan: %d changed files updated for %s", changed_count, location_name
        )

    # --- Phase 2e: hash partials for new + changed files ---
    files_needing_hash = []
    for r in new_rows:
        full_path = os.path.join(root_path, r["rel_path"])
        if r["file_size"] > 0:
            files_needing_hash.append(full_path)
    for r in changed_rows:
        full_path = os.path.join(root_path, r["rel_path"])
        if r["file_size"] > 0:
            files_needing_hash.append(full_path)

    if files_needing_hash:
        total_to_hash = len(files_needing_hash)
        hashed = 0
        last_broadcast = time.monotonic()

        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "hashing",
                "hashesDone": 0,
                "hashesTotal": total_to_hash,
            }
        )

        # Batch by bytes using HASH_BATCH_BYTES
        # Get file sizes for byte-based batching
        size_map: dict[str, int] = {}
        for r in new_rows:
            size_map[os.path.join(root_path, r["rel_path"])] = r["file_size"]
        for r in changed_rows:
            size_map[os.path.join(root_path, r["rel_path"])] = r["file_size"]

        batch_paths: list[str] = []
        batch_bytes = 0

        for path in files_needing_hash:
            batch_paths.append(path)
            batch_bytes += size_map.get(path, 0)

            if batch_bytes >= HASH_BATCH_BYTES:
                result = await hash_partial_batch(agent_id, batch_paths)
                await _write_hash_partials_to_hashes_db(
                    result,
                    root_path,
                    location_id,
                )
                hashed += len(batch_paths)
                batch_paths = []
                batch_bytes = 0

                now_mono = time.monotonic()
                if now_mono - last_broadcast >= 2.0:
                    await broadcast(
                        {
                            "type": "scan_progress",
                            "locationId": location_id,
                            "location": location_name,
                            "phase": "hashing",
                            "hashesDone": hashed,
                            "hashesTotal": total_to_hash,
                        }
                    )
                    last_broadcast = now_mono

        # Flush remaining
        if batch_paths:
            result = await hash_partial_batch(agent_id, batch_paths)
            await _write_hash_partials_to_hashes_db(
                result,
                root_path,
                location_id,
            )
            hashed += len(batch_paths)

        logger.info("Rescan: %d hash partials applied for %s", hashed, location_name)

    # --- Phase 2f: recover files missing hash_partial from interrupted scans ---
    # Files may exist in the catalog but have no hash_partial if a previous scan
    # was interrupted during the hash phase. Query catalog for file_ids in scope,
    # check hashes.db for missing hash_partial, send to agent for hashing.
    prefix_filter = ""
    prefix_params: list = [location_id]
    if scan_prefix:
        prefix_filter = " AND f.rel_path LIKE ?"
        prefix_params.append(scan_prefix + "%")

    async with read_db() as rdb:
        catalog_rows = await rdb.execute_fetchall(
            "SELECT f.id, f.full_path, f.file_size, f.inode "
            "FROM files f "
            "WHERE f.location_id = ? AND f.stale = 0 "
            f"AND f.file_size > 0{prefix_filter}",
            prefix_params,
        )

    if catalog_rows:
        catalog_ids = [r["id"] for r in catalog_rows]

        # Find which of these have no hash_partial in hashes.db
        hconn = await open_hashes_connection()
        try:
            has_partial: set[int] = set()
            for i in range(0, len(catalog_ids), 500):
                batch = catalog_ids[i : i + 500]
                ph = ",".join("?" for _ in batch)
                rows = await hconn.execute_fetchall(
                    f"SELECT file_id FROM file_hashes "
                    f"WHERE file_id IN ({ph}) AND hash_partial IS NOT NULL",
                    batch,
                )
                has_partial.update(r["file_id"] for r in rows)
        finally:
            await hconn.close()

        missing = [r for r in catalog_rows if r["id"] not in has_partial]

        if missing:
            # Sort by inode for spinning disk performance
            missing.sort(key=lambda r: r["inode"] or 0)

            logger.info(
                "Rescan: %d files missing hash_partial for %s — sending to agent",
                len(missing),
                location_name,
            )

            # Ensure file_hashes rows exist — broken scans may have never
            # created them. INSERT ON CONFLICT so we don't clobber any
            # existing data.
            async with hashes_writer() as hdb:
                await hdb.executemany(
                    "INSERT INTO file_hashes "
                    "(file_id, location_id, file_size) "
                    "VALUES (?, ?, ?) "
                    "ON CONFLICT(file_id) DO NOTHING",
                    [(r["id"], location_id, r["file_size"]) for r in missing],
                )

            recovery_ids = [r["id"] for r in missing]
            batch_paths = []
            batch_bytes = 0
            hashed_recovery = 0

            for r in missing:
                batch_paths.append(r["full_path"])
                batch_bytes += r["file_size"]

                if batch_bytes >= HASH_BATCH_BYTES:
                    result = await hash_partial_batch(agent_id, batch_paths)
                    await _write_hash_partials_to_hashes_db(
                        result, root_path, location_id
                    )
                    hashed_recovery += len(batch_paths)
                    batch_paths = []
                    batch_bytes = 0

            if batch_paths:
                result = await hash_partial_batch(agent_id, batch_paths)
                await _write_hash_partials_to_hashes_db(result, root_path, location_id)
                hashed_recovery += len(batch_paths)

            logger.info(
                "Rescan: %d recovered hash partials for %s",
                hashed_recovery,
                location_name,
            )

            # Include in affected_file_ids so they flow into dup processing
            affected_file_ids.extend(recovery_ids)

    # Broadcast updated tree children
    await _broadcast_location_children(location_id)

    return new_count, changed_count, stale_count, recovered_count, affected_file_ids


async def _write_hash_partials_to_hashes_db(
    result: dict,
    root_path: str,
    location_id: int,
):
    """Write hash_partial results from agent directly to hashes.db.

    Resolves file_ids from catalog by rel_path, then updates hash_partial
    in hashes.db. Used by rescan phase 2e for new/changed files.
    """
    hash_results = result.get("results", [])
    if not hash_results:
        return

    updates = []
    for hr in hash_results:
        hp = hr.get("hash_partial")
        if hp:
            rel = os.path.relpath(hr["path"], root_path)
            updates.append((rel, hp))

    if not updates:
        return

    rel_paths = [u[0] for u in updates]
    hash_by_rel = {u[0]: u[1] for u in updates}

    async with read_db() as rdb:
        ph = ",".join("?" for _ in rel_paths)
        id_rows = await rdb.execute_fetchall(
            f"SELECT id, rel_path, file_size FROM files "
            f"WHERE location_id = ? AND rel_path IN ({ph})",
            [location_id] + rel_paths,
        )

    if id_rows:
        h_updates = []
        for ir in id_rows:
            hp = hash_by_rel.get(ir["rel_path"])
            if hp:
                h_updates.append((hp, ir["id"]))
        if h_updates:
            async with hashes_writer() as hdb:
                await hdb.executemany(
                    "UPDATE file_hashes SET hash_partial = ? WHERE file_id = ?",
                    h_updates,
                )


async def _broadcast_location_children(location_id: int):
    """Broadcast root folders for a location so frontend can populate the tree."""
    async with read_db() as rdb:
        show_hidden = await get_setting(rdb, "showHiddenFiles") == "1"
        hidden_filter = "" if show_hidden else " AND f.hidden = 0"
        child_hidden_filter = "" if show_hidden else " AND c.hidden = 0"
        root_folders = await rdb.execute_fetchall(
            f"""SELECT f.id, f.name, f.total_size, f.hidden, f.dup_exclude,
                      EXISTS(SELECT 1 FROM folders c WHERE c.parent_id = f.id{child_hidden_filter}) AS has_children
               FROM folders f
               WHERE f.location_id = ? AND f.parent_id IS NULL{hidden_filter}
               ORDER BY f.name""",
            (location_id,),
        )
    children = []
    for f in root_folders:
        child_node = {
            "id": f"fld-{f['id']}",
            "type": "folder",
            "label": f["name"],
            "hasChildren": bool(f["has_children"]),
            "totalSize": f["total_size"],
            "children": None,
        }
        if f["hidden"]:
            child_node["hidden"] = True
        if f["dup_exclude"]:
            child_node["dupExcluded"] = True
        children.append(child_node)
    await broadcast(
        {
            "type": "location_children",
            "locationId": location_id,
            "children": children,
        }
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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
