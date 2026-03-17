"""Fast scan — direct filesystem walk for local locations.

Bypasses the agent HTTP layer. Only for locations on the Local Agent.
Pauses the queue for exclusive DB access, walks the filesystem directly,
hashes in inode order, then recounts duplicates and rebuilds sizes.

This is the ONLY scenario where the server touches the filesystem directly.
All other file operations always go through the agent.

Progress is stored in a module-level dict, pollable via GET /api/scan/fast/progress.
"""

import asyncio
import logging
import os
import sqlite3
import stat
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

BATCH_SIZE = 5000

# Pollable progress — any client can query at any time
_progress = {
    "status": "idle",
    "location": None,
    "phase": None,
    "files_found": 0,
    "folders_found": 0,
    "bytes_found": 0,
    "files_to_hash": 0,
    "files_hashed": 0,
    "error": None,
}


def get_progress() -> dict:
    return dict(_progress)


def is_running() -> bool:
    return _progress["status"] not in ("idle", "complete", "error")


async def restore_pending():
    """Resume interrupted fast scan post-processing on startup.

    If the walk/hash completed but dup recount or size rebuild was
    interrupted, re-runs just the post-processing (recount + sizes).
    The file data is already committed — only counts need fixing.
    """
    from file_hunter.db import db_writer, get_db
    from file_hunter.services.settings import get_setting

    db = await get_db()
    pending = await get_setting(db, "fast_scan_pending")
    if not pending:
        return

    try:
        location_id = int(pending)
    except (ValueError, TypeError):
        log.warning("Invalid fast_scan_pending value: %s, clearing", pending)
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'fast_scan_pending'")
        return

    row = await db.execute_fetchall(
        "SELECT name, root_path FROM locations WHERE id = ?", (location_id,)
    )
    if not row:
        log.warning("fast_scan_pending location %d not found, clearing", location_id)
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'fast_scan_pending'")
        return

    location_name = row[0]["name"]
    log.info(
        "Resuming interrupted fast scan post-processing for '%s' (location %d)",
        location_name,
        location_id,
    )

    asyncio.create_task(_resume_post_processing(location_id, location_name))


async def _resume_post_processing(location_id: int, location_name: str):
    """Run dup recount + size rebuild for an interrupted fast scan.

    Uses GROUP BY HAVING COUNT > 1 to find duplicate hashes, then updates
    only those files. Same optimized approach as the inline fast scan, but
    without the temp table (which was lost on restart).
    """
    from file_hunter.db import open_connection
    from file_hunter.services.sizes import recalculate_location_sizes

    _progress.update(
        status="running",
        location=location_name,
        phase="recounting",
        files_found=0,
        folders_found=0,
        bytes_found=0,
        files_to_hash=0,
        files_hashed=0,
        error=None,
    )

    try:
        log.info("Fast scan resume: recounting duplicates for location %d", location_id)

        conn = await open_connection()
        try:
            # Find hashes on this location that have duplicates anywhere
            hash_rows = await conn.execute_fetchall(
                "SELECT DISTINCT COALESCE(hash_strong, hash_fast) as effective_hash "
                "FROM files WHERE location_id = ? "
                "AND COALESCE(hash_strong, hash_fast) IS NOT NULL",
                (location_id,),
            )
            location_hashes = {r["effective_hash"] for r in hash_rows}
        finally:
            await conn.close()

        # For each location hash, check global count
        from file_hunter.db import db_writer
        from file_hunter.services.dup_counts import SQL_VAR_LIMIT

        dup_hashes: list[tuple[str, int]] = []  # (hash, dup_count)
        hash_list = list(location_hashes)
        _progress["files_to_hash"] = len(hash_list)

        conn = await open_connection()
        try:
            for i in range(0, len(hash_list), SQL_VAR_LIMIT):
                chunk = hash_list[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in chunk)
                rows = await conn.execute_fetchall(
                    f"SELECT COALESCE(hash_strong, hash_fast) as h, COUNT(*) as cnt "
                    f"FROM files WHERE COALESCE(hash_strong, hash_fast) IN ({ph}) "
                    f"AND stale = 0 AND dup_exclude = 0 "
                    f"GROUP BY h HAVING COUNT(*) > 1",
                    chunk,
                )
                for r in rows:
                    dup_hashes.append((r["h"], r["cnt"] - 1))
                _progress["files_hashed"] = min(i + SQL_VAR_LIMIT, len(hash_list))
        finally:
            await conn.close()

        log.info("Fast scan resume: %d duplicate hashes to update", len(dup_hashes))

        # Update dup_count for duplicate files only
        for h, dc in dup_hashes:
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE files SET dup_count = ? "
                    "WHERE COALESCE(hash_strong, hash_fast) = ? "
                    "AND stale = 0 AND dup_exclude = 0",
                    (dc, h),
                )

        _progress["phase"] = "rebuilding"
        log.info("Fast scan resume: rebuilding sizes for location %d", location_id)
        await recalculate_location_sizes(location_id)
        invalidate_stats_cache()

        # Record scan and clear pending marker
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        async with db_writer() as wdb:
            await wdb.execute(
                "UPDATE locations SET date_last_scanned = ? WHERE id = ?",
                (now, location_id),
            )
            await wdb.execute("DELETE FROM settings WHERE key = 'fast_scan_pending'")

        _progress["status"] = "complete"
        _progress["phase"] = None
        log.info("Fast scan resume complete for location '%s'", location_name)

    except Exception as e:
        log.exception("Fast scan resume failed for location %d", location_id)
        _progress.update(status="error", error=str(e))

    invalidate_stats_cache()
    await broadcast({"type": "stats_changed"})


def _get_db_path() -> Path:
    """Resolve the database path (same logic as db.py)."""
    from file_hunter.config import load_config

    config = load_config()
    db_path = Path(config.get("database", "file_hunter.db"))
    if not db_path.is_absolute():
        db_path = Path(__file__).resolve().parent.parent.parent / db_path
    return db_path


def _open_sync_connection() -> sqlite3.Connection:
    """Open a synchronous SQLite connection for use in the scan thread."""
    db_path = _get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


async def run_fast_scan(location_id: int, root_path: str, location_name: str):
    """Pause queue, walk filesystem, hash, recount dups, rebuild sizes, resume.

    Runs the synchronous walk/hash in a thread. Updates _progress for polling.
    """
    from file_hunter.services.dup_counts import stop_writer
    from file_hunter.services.queue_manager import pause, resume
    from file_hunter.services.sizes import recalculate_location_sizes

    _progress.update(
        status="pausing",
        location=location_name,
        phase="pausing",
        files_found=0,
        folders_found=0,
        bytes_found=0,
        files_to_hash=0,
        files_hashed=0,
        error=None,
    )

    try:
        await pause()
        await stop_writer()
        await broadcast(
            {"type": "queue_paused", "reason": "fast_scan", "location": location_name}
        )

        _progress["status"] = "running"

        # Save pending marker so post-processing resumes if interrupted
        from file_hunter.db import db_writer
        from file_hunter.services.settings import set_setting

        async with db_writer() as wdb:
            await set_setting(wdb, "fast_scan_pending", str(location_id))

        # Run the synchronous walk + hash in a thread
        await asyncio.to_thread(
            _sync_walk_and_hash, location_id, root_path, location_name
        )

        if _progress["status"] == "error":
            return

        # Dup counting is done inline during pass 2+3 in the sync thread.
        # Just rebuild location sizes.

        # Rebuild location sizes
        _progress["phase"] = "rebuilding"
        log.info("Fast scan: rebuilding sizes for location %d", location_id)
        await recalculate_location_sizes(location_id)
        invalidate_stats_cache()

        # Record scan and clear pending marker
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        async with db_writer() as wdb:
            await wdb.execute(
                "UPDATE locations SET date_last_scanned = ? WHERE id = ?",
                (now, location_id),
            )
            await wdb.execute(
                "INSERT INTO scans (location_id, status, started_at, completed_at, "
                "files_found, files_hashed) VALUES (?, 'completed', ?, ?, ?, ?)",
                (
                    location_id,
                    now,
                    now,
                    _progress["files_found"],
                    _progress["files_hashed"],
                ),
            )
            await wdb.execute("DELETE FROM settings WHERE key = 'fast_scan_pending'")

        _progress["status"] = "complete"
        _progress["phase"] = None
        log.info(
            "Fast scan complete: location '%s' — %d files, %d folders",
            location_name,
            _progress["files_found"],
            _progress["folders_found"],
        )
        await broadcast(
            {
                "type": "scan_completed",
                "location": location_name,
                "locationId": location_id,
                "fileCount": _progress["files_found"],
            }
        )

    except asyncio.CancelledError:
        log.info("Fast scan cancelled for location %d", location_id)
        _progress.update(status="error", error="Cancelled")
    except Exception as e:
        log.exception("Fast scan failed for location %d", location_id)
        _progress.update(status="error", error=str(e))
    finally:
        resume()
        await broadcast({"type": "queue_resumed"})
        invalidate_stats_cache()
        await broadcast({"type": "stats_changed"})


def _sync_walk_and_hash(location_id: int, root_path: str, location_name: str):
    """Synchronous walk + hash. Runs in a thread via asyncio.to_thread().

    Updates _progress dict for polling by async HTTP handlers.
    """
    from file_hunter_core.classify import classify_file
    from file_hunter_core.hasher import hash_file_partial_sync

    conn = _open_sync_connection()
    try:
        # --- Delete existing data for this location (if any) ---
        existing = conn.execute(
            "SELECT COUNT(*) FROM files WHERE location_id = ?", (location_id,)
        ).fetchone()[0]
        if existing > 0:
            _progress["phase"] = "deleting"
            log.info(
                "Fast scan: deleting %d existing files for location %d",
                existing,
                location_id,
            )
            conn.execute("DELETE FROM files WHERE location_id = ?", (location_id,))
            conn.execute("DELETE FROM folders WHERE location_id = ?", (location_id,))
            conn.commit()

        # Create temp table for inode ordering (populated during pass 1)
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _fast_scan_inodes ("
            "  rel_path TEXT PRIMARY KEY,"
            "  inode INTEGER NOT NULL"
            ")"
        )
        conn.execute("DELETE FROM _fast_scan_inodes")
        conn.commit()

        # --- Pass 1: Walk filesystem, insert folders and files ---
        _progress["phase"] = "walking"
        log.info("Fast scan pass 1: walking %s", root_path)

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        dirs_to_visit = deque([root_path])
        folder_cache: dict[str, int] = {}
        files_found = 0
        folders_found = 0
        bytes_found = 0
        file_batch: list[tuple] = []
        inode_batch: list[tuple] = []
        t0 = time.monotonic()
        last_log = t0

        while dirs_to_visit:
            dirpath = dirs_to_visit.popleft()
            rel_dir = os.path.relpath(dirpath, root_path)
            if rel_dir == ".":
                rel_dir = ""

            parent_hidden = (
                any(p.startswith(".") for p in rel_dir.split(os.sep))
                if rel_dir
                else False
            )

            # Create folder record (except root)
            folder_id = None
            if rel_dir:
                folder_name = os.path.basename(dirpath)
                dir_hidden = parent_hidden or folder_name.startswith(".")
                folder_id = _get_or_create_folder(
                    conn, location_id, rel_dir, folder_name, dir_hidden, folder_cache
                )
                folders_found += 1
                _progress["folders_found"] = folders_found

            try:
                entries = os.listdir(dirpath)
            except (PermissionError, OSError):
                continue

            for name in entries:
                full_path = os.path.join(dirpath, name)

                try:
                    is_link = os.path.islink(full_path)
                except OSError:
                    continue
                if is_link:
                    continue

                try:
                    is_dir = os.path.isdir(full_path)
                except OSError:
                    continue
                if is_dir:
                    dirs_to_visit.append(full_path)
                    continue

                try:
                    st = os.stat(full_path)
                except OSError:
                    continue
                if not stat.S_ISREG(st.st_mode):
                    continue

                hidden = 1 if (parent_hidden or name.startswith(".")) else 0
                rel_path = os.path.join(rel_dir, name) if rel_dir else name
                type_high, type_low = classify_file(name)

                created = datetime.fromtimestamp(
                    st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                    tz=timezone.utc,
                ).isoformat(timespec="seconds")
                modified = datetime.fromtimestamp(
                    st.st_mtime, tz=timezone.utc
                ).isoformat(timespec="seconds")

                file_batch.append(
                    (
                        name,
                        full_path,
                        rel_path,
                        location_id,
                        folder_id,
                        type_high,
                        type_low,
                        st.st_size,
                        "",  # description
                        "",  # tags
                        created,
                        modified,
                        now,  # date_cataloged
                        now,  # date_last_seen
                        hidden,
                        0,  # stale
                    )
                )

                # Store inode for pass 2 ordering (only for hashable files)
                if st.st_size > 0:
                    inode_batch.append(
                        (
                            rel_path,
                            st.st_ino & 0x7FFFFFFFFFFFFFFF,
                        )
                    )

                files_found += 1
                bytes_found += st.st_size
                _progress["files_found"] = files_found
                _progress["bytes_found"] = bytes_found

                if len(file_batch) >= BATCH_SIZE:
                    _insert_file_batch(conn, file_batch)
                    file_batch.clear()

                if len(inode_batch) >= BATCH_SIZE:
                    conn.executemany(
                        "INSERT INTO _fast_scan_inodes (rel_path, inode) VALUES (?, ?)",
                        inode_batch,
                    )
                    inode_batch.clear()

            # Periodic logging
            now_t = time.monotonic()
            if now_t - last_log >= 5:
                rate = files_found / (now_t - t0) if (now_t - t0) > 0 else 0
                log.info(
                    "Fast scan walk: %d files, %d folders, %.0f files/sec",
                    files_found,
                    folders_found,
                    rate,
                )
                last_log = now_t

        # Flush remaining
        if file_batch:
            _insert_file_batch(conn, file_batch)
        if inode_batch:
            conn.executemany(
                "INSERT INTO _fast_scan_inodes (rel_path, inode) VALUES (?, ?)",
                inode_batch,
            )
        conn.commit()

        elapsed = time.monotonic() - t0
        log.info(
            "Fast scan pass 1 complete: %d files, %d folders in %.1fs",
            files_found,
            folders_found,
            elapsed,
        )

        # --- Pass 2: Hash in inode order + inline dup tracking ---
        _progress["phase"] = "hashing"
        log.info("Fast scan pass 2: hashing (inode-sorted)")

        # Create temp table for duplicate hashes (only hashes with COUNT > 1)
        conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS _dup_hashes (  hash TEXT PRIMARY KEY)"
        )
        conn.execute("DELETE FROM _dup_hashes")
        conn.commit()

        # Join temp table with files to get (file_id, rel_path) in inode order
        to_hash = conn.execute(
            "SELECT f.id, f.rel_path FROM files f "
            "JOIN _fast_scan_inodes i ON i.rel_path = f.rel_path "
            "WHERE f.location_id = ? AND f.hash_fast IS NULL AND f.file_size > 0 "
            "ORDER BY i.inode",
            (location_id,),
        ).fetchall()
        total_to_hash = len(to_hash)
        _progress["files_to_hash"] = total_to_hash

        log.info("Fast scan pass 2: %d files to hash", total_to_hash)

        hashed = 0
        t1 = time.monotonic()
        last_log = t1

        for row in to_hash:
            file_id = row["id"]
            rel_path = row["rel_path"]
            full_path = os.path.join(root_path, rel_path)
            try:
                h = hash_file_partial_sync(full_path)
            except OSError:
                continue

            # Write hash immediately so the dup check sees it
            conn.execute("UPDATE files SET hash_fast = ? WHERE id = ?", (h, file_id))
            hashed += 1
            _progress["files_hashed"] = hashed

            # Inline dup check — single indexed lookup via expression index
            cnt = conn.execute(
                "SELECT COUNT(*) FROM files "
                "WHERE COALESCE(hash_strong, hash_fast) = ? "
                "AND stale = 0 AND dup_exclude = 0",
                (h,),
            ).fetchone()[0]
            if cnt > 1:
                conn.execute(
                    "INSERT OR IGNORE INTO _dup_hashes (hash) VALUES (?)", (h,)
                )

            # Commit periodically
            if hashed % BATCH_SIZE == 0:
                conn.commit()

            now_t = time.monotonic()
            if now_t - last_log >= 5:
                rate = hashed / (now_t - t1) if (now_t - t1) > 0 else 0
                log.info(
                    "Fast scan hash: %d / %d (%.0f files/sec)",
                    hashed,
                    total_to_hash,
                    rate,
                )
                last_log = now_t

        conn.commit()

        # Clean up inode temp table
        conn.execute("DROP TABLE IF EXISTS _fast_scan_inodes")
        conn.commit()

        elapsed2 = time.monotonic() - t1
        log.info(
            "Fast scan pass 2 complete: %d files hashed in %.1fs",
            hashed,
            elapsed2,
        )

        # --- Pass 3: Process duplicate hashes ---
        _progress["phase"] = "recounting"
        _progress["files_to_hash"] = 0
        _progress["files_hashed"] = 0

        dup_count = conn.execute("SELECT COUNT(*) FROM _dup_hashes").fetchone()[0]
        _progress["files_to_hash"] = dup_count
        log.info("Fast scan pass 3: %d duplicate hashes to process", dup_count)

        dup_rows = conn.execute("SELECT hash FROM _dup_hashes").fetchall()
        processed = 0
        for row in dup_rows:
            h = row["hash"]
            cnt = conn.execute(
                "SELECT COUNT(*) FROM files "
                "WHERE COALESCE(hash_strong, hash_fast) = ? "
                "AND stale = 0 AND dup_exclude = 0",
                (h,),
            ).fetchone()[0]
            dc = cnt - 1 if cnt > 1 else 0
            conn.execute(
                "UPDATE files SET dup_count = ? "
                "WHERE COALESCE(hash_strong, hash_fast) = ? "
                "AND stale = 0 AND dup_exclude = 0",
                (dc, h),
            )
            processed += 1
            _progress["files_hashed"] = processed
            if processed % 1000 == 0:
                conn.commit()
                log.info(
                    "Fast scan dup update: %d / %d hashes",
                    processed,
                    dup_count,
                )

        conn.commit()

        # Clean up dup temp table
        conn.execute("DROP TABLE IF EXISTS _dup_hashes")
        conn.commit()

        log.info("Fast scan pass 3 complete: %d duplicate hashes processed", processed)

    except Exception as e:
        log.exception("Fast scan sync phase failed")
        _progress.update(status="error", error=str(e))
    finally:
        conn.close()


def _get_or_create_folder(
    conn: sqlite3.Connection,
    location_id: int,
    rel_path: str,
    name: str,
    hidden: bool,
    cache: dict[str, int],
) -> int:
    """Get or create a folder, returning its ID. Uses an in-memory cache."""
    cached = cache.get(rel_path)
    if cached is not None:
        return cached

    parent_path = "/".join(rel_path.split("/")[:-1]) if "/" in rel_path else ""
    parent_id = cache.get(parent_path) if parent_path else None

    conn.execute(
        "INSERT OR IGNORE INTO folders (location_id, parent_id, name, rel_path, hidden) "
        "VALUES (?, ?, ?, ?, ?)",
        (location_id, parent_id, name, rel_path, 1 if hidden else 0),
    )
    row = conn.execute(
        "SELECT id FROM folders WHERE location_id = ? AND rel_path = ?",
        (location_id, rel_path),
    ).fetchone()
    folder_id = row["id"]
    cache[rel_path] = folder_id
    return folder_id


def _insert_file_batch(conn: sqlite3.Connection, batch: list[tuple]):
    """Bulk insert files into the files table."""
    conn.executemany(
        "INSERT INTO files "
        "(filename, full_path, rel_path, location_id, folder_id, "
        "file_type_high, file_type_low, file_size, "
        "description, tags, created_date, modified_date, "
        "date_cataloged, date_last_seen, hidden, stale) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        batch,
    )
    conn.commit()
