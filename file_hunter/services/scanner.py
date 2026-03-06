"""Real filesystem scanner — walk, hash, upsert, broadcast.

All filesystem I/O runs in threads (via asyncio.to_thread) to keep the
event loop responsive for API requests, WebSocket messages, and signals.
"""

import asyncio
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone

from file_hunter_core.walker import scan_directory as _scan_directory
from file_hunter.db import check_write_lock_requested
from file_hunter.services.consolidate import drain_pending_jobs
from file_hunter.services.hasher import hash_file, hash_file_partial
from file_hunter.ws.scan import broadcast

log = logging.getLogger("file_hunter")

# location_id → scan_id for currently running scans
_active_scans: dict[int, int] = {}
_active_tasks: dict[int, asyncio.Task] = {}
_cancel_requested: set[int] = set()
_shutting_down: bool = False


def is_scan_running(location_id: int) -> bool:
    return location_id in _active_scans


def register_task(location_id: int, task: asyncio.Task):
    _active_tasks[location_id] = task


def request_cancel(location_id: int) -> bool:
    """Request cancellation of a running scan. Returns True if scan was running."""
    if location_id not in _active_scans:
        return False
    _cancel_requested.add(location_id)
    return True


async def cancel_all_scans():
    """Cancel all running scan tasks and wait for them to finish.

    Sets _shutting_down so scan handlers leave status as 'running' —
    restore_queue will re-queue them on next startup.
    """
    global _shutting_down
    _shutting_down = True
    tasks = list(_active_tasks.values())
    for loc_id in list(_active_scans):
        _cancel_requested.add(loc_id)
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


class _ProgressTracker:
    """Throttles WebSocket broadcasts to at most once every interval seconds."""

    def __init__(
        self,
        location_id: int,
        location_name: str,
        scan_path_label: str | None = None,
        interval: float = 0.5,
    ):
        self.location_id = location_id
        self.location_name = location_name
        self.scan_path_label = scan_path_label
        self.interval = interval
        self.files_found = 0
        self.files_hashed = 0
        self.files_skipped = 0
        self.duplicates_found = 0
        self.stale_files = 0
        self._last_broadcast = 0.0

    async def maybe_broadcast(self, force: bool = False):
        now = time.monotonic()
        if force or (now - self._last_broadcast) >= self.interval:
            self._last_broadcast = now
            log.info(
                "%s — %d found, %d hashed, %d dups",
                self.scan_path_label or self.location_name,
                self.files_found,
                self.files_hashed,
                self.duplicates_found,
            )
            msg = {
                "type": "scan_progress",
                "locationId": self.location_id,
                "location": self.location_name,
                "filesFound": self.files_found,
                "filesHashed": self.files_hashed,
                "filesSkipped": self.files_skipped,
                "duplicatesFound": self.duplicates_found,
            }
            if self.scan_path_label:
                msg["scanPath"] = self.scan_path_label
            await broadcast(msg)


# ---------------------------------------------------------------------------
# Optimization helpers
# ---------------------------------------------------------------------------


async def _load_existing_files(db, location_id: int) -> dict:
    """Bulk-load all files for this location into a dict keyed by rel_path.

    Returns dict[rel_path → {id, file_size, modified_date, hash_partial, hash_strong}].
    """
    rows = await db.execute_fetchall(
        """SELECT id, rel_path, file_size, modified_date, hash_partial, hash_strong
           FROM files WHERE location_id = ?""",
        (location_id,),
    )
    return {
        row["rel_path"]: {
            "id": row["id"],
            "file_size": row["file_size"],
            "modified_date": row["modified_date"],
            "hash_partial": row["hash_partial"],
            "hash_strong": row["hash_strong"],
        }
        for row in rows
    }


async def _touch_file(db, file_id: int, scan_id: int, now_iso: str):
    """Lightweight update for unchanged files — mark as seen, clear stale flag."""
    await db.execute(
        "UPDATE files SET date_last_seen=?, scan_id=?, stale=0 WHERE id=?",
        (now_iso, scan_id, file_id),
    )


def _build_batch_size_counts(discovered_files: list[dict]) -> dict[int, int]:
    """Returns dict[file_size → count] for within-batch duplicate detection."""
    counts: dict[int, int] = {}
    for f in discovered_files:
        size = f["file_size"]
        counts[size] = counts.get(size, 0) + 1
    return counts


async def _load_catalog_size_set(db, location_id: int) -> set[int]:
    """Returns set of all distinct file_size values from files in OTHER locations."""
    rows = await db.execute_fetchall(
        "SELECT DISTINCT file_size FROM files WHERE location_id != ?",
        (location_id,),
    )
    return {row["file_size"] for row in rows}


async def _check_partial_match(
    db, file_size: int, hash_partial: str, exclude_id: int | None
) -> bool:
    """Check if any other file in DB has same file_size AND hash_partial."""
    if exclude_id is not None:
        rows = await db.execute_fetchall(
            """SELECT 1 FROM files
               WHERE file_size = ? AND hash_partial = ? AND id != ?
               LIMIT 1""",
            (file_size, hash_partial, exclude_id),
        )
    else:
        rows = await db.execute_fetchall(
            """SELECT 1 FROM files
               WHERE file_size = ? AND hash_partial = ?
               LIMIT 1""",
            (file_size, hash_partial),
        )
    return len(rows) > 0


async def _backfill_partial_matches(
    db, file_size: int, hash_partial: str, exclude_id: int
) -> list[dict]:
    """Find files with same size + partial hash but NULL hash_strong.

    These were previously skipped because they appeared unique at the time.
    Returns list of {id, full_path} for files that need full hashing.
    """
    rows = await db.execute_fetchall(
        """SELECT id, full_path FROM files
           WHERE file_size = ? AND hash_partial = ? AND hash_strong IS NULL AND id != ?""",
        (file_size, hash_partial, exclude_id),
    )
    return [{"id": row["id"], "full_path": row["full_path"]} for row in rows]


async def _mark_stale_files(
    db, location_id: int, scan_id: int, scan_prefix: str | None = None
) -> int:
    """Mark files not seen in this scan as stale. Returns count.

    When scan_prefix is set, only marks files within that subtree stale.
    Files in the folder have rel_path like "prefix/filename", and files
    in subdirectories have "prefix/sub/filename" — both matched by
    ``prefix/%``.
    """
    if scan_prefix:
        cursor = await db.execute(
            """UPDATE files SET stale=1
               WHERE location_id=? AND scan_id!=? AND stale=0
               AND rel_path LIKE ?""",
            (location_id, scan_id, scan_prefix + "/%"),
        )
        return cursor.rowcount
    else:
        cursor = await db.execute(
            "UPDATE files SET stale=1 WHERE location_id=? AND scan_id!=? AND stale=0",
            (location_id, scan_id),
        )
        return cursor.rowcount


async def _cleanup_orphan_folders(db, location_id: int) -> int:
    """Delete folders that contain no non-stale files (recursively).

    Keeps folders that are direct parents of non-stale files or ancestors
    of such folders. Returns the number of folders deleted.
    """
    cursor = await db.execute(
        """DELETE FROM folders
           WHERE location_id = ?
             AND id NOT IN (
                 WITH RECURSIVE needed(id) AS (
                     SELECT DISTINCT f.folder_id
                     FROM files f
                     WHERE f.location_id = ?
                       AND f.stale = 0
                       AND f.folder_id IS NOT NULL
                     UNION
                     SELECT fld.parent_id
                     FROM folders fld
                     JOIN needed n ON fld.id = n.id
                     WHERE fld.parent_id IS NOT NULL
                 )
                 SELECT id FROM needed
             )""",
        (location_id, location_id),
    )
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Main scan coroutine
# ---------------------------------------------------------------------------


async def run_scan(
    db,
    location_id: int,
    location_name: str,
    root_path: str,
    scan_path: str | None = None,
):
    """Full scan lifecycle — discovery, hashing, upsert, broadcast.

    When *scan_path* is set, only the subtree rooted at that directory is
    walked and hashed.  ``rel_path`` calculations remain relative to
    *root_path* (unchanged).  Stale marking is scoped to the subtree.
    """
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    scan_id = None
    affected_hashes: set[str] = set()
    # scan_prefix is computed after setup; pass label later
    _scan_prefix_label = os.path.relpath(scan_path, root_path) if scan_path else None
    progress = _ProgressTracker(location_id, location_name, _scan_prefix_label)

    try:
        # --- Setup ---
        cursor = await db.execute(
            "INSERT INTO scans (location_id, status, started_at) VALUES (?, 'running', ?)",
            (location_id, now_iso),
        )
        scan_id = cursor.lastrowid
        await db.commit()
        _active_scans[location_id] = scan_id

        # Compute scan_prefix for scoped stale marking
        scan_prefix = None
        if scan_path:
            scan_prefix = os.path.relpath(scan_path, root_path)

        log.info(
            "Scan started: %s%s",
            location_name,
            f" ({scan_prefix})" if scan_prefix else "",
        )
        await broadcast(
            {
                "type": "scan_started",
                "locationId": location_id,
                "location": location_name,
                "scanPath": scan_prefix,
            }
        )

        # --- Drain pending consolidation jobs ---
        await drain_pending_jobs(db, location_id, root_path)

        # --- Discovery phase ---
        # Manual breadth-first walk: each directory is scanned in a thread,
        # keeping the event loop free between directories for API/WS/signals.
        folder_cache: dict[str, tuple] = {}  # rel_path → (folder_id, dup_exclude)
        discovered_files: list[dict] = []
        dir_queue: deque[tuple[str, bool]] = deque([(scan_path or root_path, False)])

        while dir_queue:
            if location_id in _cancel_requested:
                break

            dirpath, dir_hidden = dir_queue.popleft()

            # Run filesystem I/O in a thread — this is the key fix
            subdirs, file_infos = await asyncio.to_thread(
                _scan_directory, dirpath, root_path, dir_hidden
            )

            # Ensure folder hierarchy for this directory (DB operations in event loop)
            rel_dir = os.path.relpath(dirpath, root_path)
            if rel_dir == ".":
                rel_dir = ""

            folder_id = None
            folder_dup_exclude = 0
            if rel_dir:
                folder_id, folder_dup_exclude = await _ensure_folder_hierarchy(
                    db, location_id, rel_dir, folder_cache
                )

            # Queue subdirectories for traversal
            for subdir in sorted(subdirs):
                sub_hidden = dir_hidden or os.path.basename(subdir).startswith(".")
                dir_queue.append((subdir, sub_hidden))

            # Attach folder_id and dup_exclude to each file and accumulate
            for finfo in file_infos:
                finfo["folder_id"] = folder_id
                finfo["dup_exclude"] = folder_dup_exclude
                discovered_files.append(finfo)
                progress.files_found += 1

            await progress.maybe_broadcast()

            # Commit after every directory to release the DB write lock
            # immediately — prioritise UI responsiveness over scan speed.
            await db.commit()
            if check_write_lock_requested():
                await asyncio.sleep(0.2)

        # --- Hashing phase (with optimization gates) ---
        cancelled = location_id in _cancel_requested
        if not cancelled:
            existing_files = await _load_existing_files(db, location_id)
            batch_size_counts = _build_batch_size_counts(discovered_files)
            catalog_sizes = await _load_catalog_size_set(db, location_id)
        batch_partials: dict[tuple[int, str], int] = {}  # (size, hash_partial) → count
        for finfo in discovered_files:
            if location_id in _cancel_requested:
                cancelled = True
                break

            rel_path = finfo["rel_path"]
            file_size = finfo["file_size"]
            existing = existing_files.get(rel_path)

            # --- GATE 0: Hidden files — never hash ---
            if finfo.get("hidden"):
                file_id = await _upsert_file(
                    db,
                    location_id=location_id,
                    scan_id=scan_id,
                    filename=finfo["filename"],
                    full_path=finfo["full_path"],
                    rel_path=rel_path,
                    folder_id=finfo["folder_id"],
                    file_size=file_size,
                    created_date=finfo["created_date"],
                    modified_date=finfo["modified_date"],
                    file_type_high=finfo["file_type_high"],
                    file_type_low=finfo["file_type_low"],
                    hash_partial=None,
                    hash_fast=None,
                    hash_strong=None,
                    now_iso=now_iso,
                    hidden=1,
                    dup_exclude=finfo.get("dup_exclude", 0),
                )
                progress.files_skipped += 1
                await progress.maybe_broadcast()
                await db.commit()
                if check_write_lock_requested():
                    await asyncio.sleep(0.2)
                else:
                    await asyncio.sleep(0)
                continue

            # --- GATE 1: Incremental rescan ---
            # File exists in DB, same size + mtime, already fully hashed → skip
            if (
                existing
                and existing["file_size"] == file_size
                and existing["modified_date"] == finfo["modified_date"]
                and existing["hash_strong"]
            ):
                await _touch_file(db, existing["id"], scan_id, now_iso)
                progress.files_skipped += 1
                await progress.maybe_broadcast()
                await db.commit()
                if check_write_lock_requested():
                    await asyncio.sleep(0.2)
                else:
                    await asyncio.sleep(0)
                continue

            # --- Compute partial hash ---
            hash_partial = None
            if (
                existing
                and existing["file_size"] == file_size
                and existing["modified_date"] == finfo["modified_date"]
                and existing["hash_partial"]
            ):
                # Unchanged file with existing partial hash — reuse it
                hash_partial = existing["hash_partial"]
            else:
                try:
                    hash_partial = await hash_file_partial(finfo["full_path"])
                except (PermissionError, FileNotFoundError, OSError):
                    continue

            existing_id = existing["id"] if existing else None

            # --- GATE 2: Size pre-filter ---
            # If file_size is unique within batch AND across catalog → skip full hash
            if (
                batch_size_counts.get(file_size, 0) <= 1
                and file_size not in catalog_sizes
            ):
                file_id = await _upsert_file(
                    db,
                    location_id=location_id,
                    scan_id=scan_id,
                    filename=finfo["filename"],
                    full_path=finfo["full_path"],
                    rel_path=rel_path,
                    folder_id=finfo["folder_id"],
                    file_size=file_size,
                    created_date=finfo["created_date"],
                    modified_date=finfo["modified_date"],
                    file_type_high=finfo["file_type_high"],
                    file_type_low=finfo["file_type_low"],
                    hash_partial=hash_partial,
                    hash_fast=None,
                    hash_strong=None,
                    now_iso=now_iso,
                    dup_exclude=finfo.get("dup_exclude", 0),
                )
                batch_partials[(file_size, hash_partial)] = (
                    batch_partials.get((file_size, hash_partial), 0) + 1
                )
                progress.files_skipped += 1
                await progress.maybe_broadcast()
                await db.commit()
                if check_write_lock_requested():
                    await asyncio.sleep(0.2)
                else:
                    await asyncio.sleep(0)
                continue

            # --- GATE 3: Partial hash match check ---
            # Size matches exist. Do any have same size + partial hash?
            partial_match = False

            # Check within-batch
            if batch_partials.get((file_size, hash_partial), 0) > 0:
                partial_match = True

            # Check catalog (other locations)
            if not partial_match and file_size in catalog_sizes:
                partial_match = await _check_partial_match(
                    db, file_size, hash_partial, existing_id
                )

            if not partial_match:
                # No partial match — skip full hash
                file_id = await _upsert_file(
                    db,
                    location_id=location_id,
                    scan_id=scan_id,
                    filename=finfo["filename"],
                    full_path=finfo["full_path"],
                    rel_path=rel_path,
                    folder_id=finfo["folder_id"],
                    file_size=file_size,
                    created_date=finfo["created_date"],
                    modified_date=finfo["modified_date"],
                    file_type_high=finfo["file_type_high"],
                    file_type_low=finfo["file_type_low"],
                    hash_partial=hash_partial,
                    hash_fast=None,
                    hash_strong=None,
                    now_iso=now_iso,
                    dup_exclude=finfo.get("dup_exclude", 0),
                )
                batch_partials[(file_size, hash_partial)] = (
                    batch_partials.get((file_size, hash_partial), 0) + 1
                )
                progress.files_skipped += 1
                await progress.maybe_broadcast()
                await db.commit()
                if check_write_lock_requested():
                    await asyncio.sleep(0.2)
                else:
                    await asyncio.sleep(0)
                continue

            # --- FULL HASH: potential duplicate confirmed by size + partial ---
            try:
                hash_fast, hash_strong = await hash_file(finfo["full_path"])
            except (PermissionError, FileNotFoundError, OSError):
                continue

            file_id = await _upsert_file(
                db,
                location_id=location_id,
                scan_id=scan_id,
                filename=finfo["filename"],
                full_path=finfo["full_path"],
                rel_path=rel_path,
                folder_id=finfo["folder_id"],
                file_size=file_size,
                created_date=finfo["created_date"],
                modified_date=finfo["modified_date"],
                file_type_high=finfo["file_type_high"],
                file_type_low=finfo["file_type_low"],
                hash_partial=hash_partial,
                hash_fast=hash_fast,
                hash_strong=hash_strong,
                now_iso=now_iso,
                dup_exclude=finfo.get("dup_exclude", 0),
            )
            affected_hashes.add(hash_strong)

            # Backfill: hash any previously-skipped files that share this
            # (size, partial) pair but still have NULL hash_strong.
            backfill = await _backfill_partial_matches(
                db, file_size, hash_partial, file_id
            )
            for bf in backfill:
                if location_id in _cancel_requested:
                    break
                try:
                    bf_fast, bf_strong = await hash_file(bf["full_path"])
                except (PermissionError, FileNotFoundError, OSError):
                    continue
                await db.execute(
                    "UPDATE files SET hash_fast=?, hash_strong=? WHERE id=?",
                    (bf_fast, bf_strong, bf["id"]),
                )
                affected_hashes.add(bf_strong)
                progress.files_hashed += 1

            dup_count = await _count_duplicates(db, hash_strong, file_id)
            if dup_count > 0:
                progress.duplicates_found += 1

            # Add size to catalog set so subsequent files see it
            catalog_sizes.add(file_size)
            batch_partials[(file_size, hash_partial)] = (
                batch_partials.get((file_size, hash_partial), 0) + 1
            )

            progress.files_hashed += 1
            await progress.maybe_broadcast()

            await db.commit()
            if check_write_lock_requested():
                await asyncio.sleep(0.2)
            else:
                await asyncio.sleep(0)

        await db.commit()

        # --- Check cancellation ---
        if not cancelled:
            cancelled = location_id in _cancel_requested

        # --- Stale detection ---
        if not cancelled:
            # Collect hashes of files about to be marked stale
            if scan_prefix:
                stale_hash_rows = await db.execute_fetchall(
                    """SELECT DISTINCT hash_strong FROM files
                       WHERE location_id=? AND scan_id!=? AND stale=0
                       AND hash_strong IS NOT NULL AND rel_path LIKE ?""",
                    (location_id, scan_id, scan_prefix + "/%"),
                )
            else:
                stale_hash_rows = await db.execute_fetchall(
                    """SELECT DISTINCT hash_strong FROM files
                       WHERE location_id=? AND scan_id!=? AND stale=0
                       AND hash_strong IS NOT NULL""",
                    (location_id, scan_id),
                )
            for r in stale_hash_rows:
                affected_hashes.add(r["hash_strong"])

            stale_count = await _mark_stale_files(db, location_id, scan_id, scan_prefix)
            progress.stale_files = stale_count
            await db.commit()

        # --- Finalize ---
        # During shutdown, leave status as 'running' so restore_queue
        # re-queues the scan on next startup.
        if cancelled and _shutting_down:
            pass
        else:
            completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
            final_status = "cancelled" if cancelled else "completed"
            await db.execute(
                """UPDATE scans SET status=?, completed_at=?,
                   files_found=?, files_hashed=?, files_skipped=?,
                   duplicates_found=?, stale_files=?
                   WHERE id=?""",
                (
                    final_status,
                    completed_iso,
                    progress.files_found,
                    progress.files_hashed,
                    progress.files_skipped,
                    progress.duplicates_found,
                    progress.stale_files,
                    scan_id,
                ),
            )
            await db.execute(
                "UPDATE locations SET date_last_scanned=? WHERE id=?",
                (completed_iso, location_id),
            )
            await db.commit()

        if cancelled:
            log.info(
                "Scan cancelled: %s (found=%d, hashed=%d, skipped=%d)",
                location_name,
                progress.files_found,
                progress.files_hashed,
                progress.files_skipped,
            )
            _cancelled_msg = {
                "type": "scan_cancelled",
                "locationId": location_id,
                "location": location_name,
                "filesFound": progress.files_found,
                "filesHashed": progress.files_hashed,
                "filesSkipped": progress.files_skipped,
                "duplicatesFound": progress.duplicates_found,
            }
            if scan_prefix:
                _cancelled_msg["scanPath"] = scan_prefix
            await broadcast(_cancelled_msg)
        else:
            log.info(
                "Scan completed: %s (found=%d, hashed=%d, skipped=%d, dups=%d, stale=%d)",
                location_name,
                progress.files_found,
                progress.files_hashed,
                progress.files_skipped,
                progress.duplicates_found,
                progress.stale_files,
            )
            _completed_msg = {
                "type": "scan_completed",
                "locationId": location_id,
                "location": location_name,
                "filesFound": progress.files_found,
                "filesHashed": progress.files_hashed,
                "filesSkipped": progress.files_skipped,
                "duplicatesFound": progress.duplicates_found,
                "staleFiles": progress.stale_files,
            }
            if scan_prefix:
                _completed_msg["scanPath"] = scan_prefix
            await broadcast(_completed_msg)

    except asyncio.CancelledError:
        if scan_id is not None and not _shutting_down:
            try:
                await db.execute(
                    "UPDATE scans SET status='cancelled' WHERE id=?",
                    (scan_id,),
                )
                await db.commit()
            except Exception:
                pass

    except Exception as exc:
        log.error("Scan error: %s — %s", location_name, exc)
        error_msg = str(exc)
        if scan_id is not None:
            try:
                await db.execute(
                    "UPDATE scans SET status='error', error=? WHERE id=?",
                    (error_msg, scan_id),
                )
                await db.commit()
            except Exception:
                pass
        try:
            await broadcast(
                {
                    "type": "scan_error",
                    "locationId": location_id,
                    "location": location_name,
                    "error": error_msg,
                }
            )
        except Exception:
            pass

    finally:
        _active_scans.pop(location_id, None)
        _active_tasks.pop(location_id, None)
        _cancel_requested.discard(location_id)
        from file_hunter.services.stats import invalidate_stats_cache

        invalidate_stats_cache()
        try:
            from file_hunter.services.sizes import recalculate_location_sizes

            await recalculate_location_sizes(db, location_id)
        except Exception:
            pass
        try:
            from file_hunter.services.dup_counts import recalculate_dup_counts

            await recalculate_dup_counts(
                db, affected_hashes, source=f"scan {location_name}"
            )
        except Exception:
            pass
        try:
            await db.close()
        except Exception:
            pass


async def _ensure_folder_hierarchy(
    db, location_id: int, rel_dir_path: str, folder_cache: dict[str, tuple]
) -> tuple[int, int]:
    """Create/find folder records for a full relative directory path.

    For 'Photos/2024 Holiday', ensures both 'Photos' and 'Photos/2024 Holiday'
    exist. Returns (leaf_folder_id, dup_exclude). Sets hidden=1 for dotfolders
    and their descendants. Uses each folder's own dup_exclude flag (not
    inherited from parents — allows subfolder carve-outs).
    """
    parts = rel_dir_path.replace("\\", "/").split("/")
    current_path = ""
    parent_id = None
    is_hidden = False
    leaf_dup_exclude = 0

    for part in parts:
        current_path = f"{current_path}/{part}" if current_path else part
        is_hidden = is_hidden or part.startswith(".")

        if current_path in folder_cache:
            parent_id, cached_dup_exclude = folder_cache[current_path]
            leaf_dup_exclude = cached_dup_exclude
            continue

        # Check DB
        row = await db.execute_fetchall(
            "SELECT id, dup_exclude FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, current_path),
        )
        if row:
            folder_id = row[0]["id"]
            leaf_dup_exclude = row[0]["dup_exclude"]
        else:
            cursor = await db.execute(
                "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) VALUES (?, ?, ?, ?, ?)",
                (location_id, parent_id, part, current_path, 1 if is_hidden else 0),
            )
            folder_id = cursor.lastrowid
            leaf_dup_exclude = 0

        folder_cache[current_path] = (folder_id, leaf_dup_exclude)
        parent_id = folder_id

    return parent_id, leaf_dup_exclude


async def _upsert_file(
    db,
    *,
    location_id: int,
    scan_id: int,
    filename: str,
    full_path: str,
    rel_path: str,
    folder_id: int | None,
    file_size: int,
    created_date: str,
    modified_date: str,
    file_type_high: str,
    file_type_low: str,
    hash_partial: str | None,
    hash_fast: str | None,
    hash_strong: str | None,
    now_iso: str,
    hidden: int = 0,
    dup_exclude: int = 0,
) -> int:
    """Insert or update a file record. Preserves description and tags on update."""
    row = await db.execute_fetchall(
        "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
        (location_id, rel_path),
    )
    if row:
        file_id = row[0]["id"]
        await db.execute(
            """UPDATE files SET
                filename=?, full_path=?, folder_id=?,
                file_type_high=?, file_type_low=?, file_size=?,
                hash_partial=COALESCE(?, hash_partial),
                hash_fast=COALESCE(?, hash_fast),
                hash_strong=COALESCE(?, hash_strong),
                created_date=?, modified_date=?,
                date_last_seen=?, scan_id=?, stale=0, hidden=?, dup_exclude=?
               WHERE id=?""",
            (
                filename,
                full_path,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                hash_partial,
                hash_fast,
                hash_strong,
                created_date,
                modified_date,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
                file_id,
            ),
        )
        return file_id
    else:
        cursor = await db.execute(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                hash_partial, hash_fast, hash_strong,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen,
                scan_id, stale, hidden, dup_exclude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, 0, ?, ?)""",
            (
                filename,
                full_path,
                rel_path,
                location_id,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                hash_partial,
                hash_fast,
                hash_strong,
                created_date,
                modified_date,
                now_iso,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
            ),
        )
        return cursor.lastrowid


async def _count_duplicates(db, hash_strong: str, file_id: int) -> int:
    """Count other files with the same strong hash."""
    row = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM files WHERE hash_strong = ? AND id != ?",
        (hash_strong, file_id),
    )
    return row[0]["cnt"]


# Public aliases for pro/extension reuse (keep _-prefixed originals intact)
ensure_folder_hierarchy = _ensure_folder_hierarchy
upsert_file = _upsert_file
mark_stale_files = _mark_stale_files
cleanup_orphan_folders = _cleanup_orphan_folders
