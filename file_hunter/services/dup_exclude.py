"""Toggle duplicate exclusion for a folder and all its descendants.

Pause queue → flag folders/files → recount affected hashes → zero excluded
files (exclude only) → rebuild location sizes → resume queue.

Progress is stored in a module-level dict, pollable via GET /api/dup-exclude/progress.
"""

import asyncio
import logging

from file_hunter.core import ProgressTracker
from file_hunter.db import db_writer, open_connection, read_db
from file_hunter.hashes_db import hashes_writer, open_hashes_connection
from file_hunter.helpers import post_op_stats
from file_hunter.services.dup_counts import SQL_VAR_LIMIT, _batched_recalc, stop_writer
from file_hunter.services.queue_manager import pause, resume
from file_hunter.services.settings import get_setting
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

_progress = ProgressTracker(
    direction=None,
    folder=None,
    files_total=0,
    files_done=0,
    hashes_total=0,
    hashes_done=0,
)


def get_progress() -> dict:
    return _progress.snapshot()


def is_running() -> bool:
    return _progress.is_running


async def restore_pending():
    """Resume an interrupted dup_exclude operation on startup.

    Checks for the dup_exclude_pending settings key. If found, re-runs
    the operation from scratch on a background task. Zero cost if no
    pending operation exists.
    """
    async with read_db() as db:
        pending = await get_setting(db, "dup_exclude_pending")
    if not pending:
        return

    try:
        folder_id_str, flag_str = pending.split(":")
        folder_id = int(folder_id_str)
        exclude = flag_str == "1"
    except (ValueError, IndexError):
        log.warning("Invalid dup_exclude_pending value: %s, clearing", pending)
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'dup_exclude_pending'")
        return

    async with read_db() as db:
        row = await db.execute_fetchall(
            "SELECT name FROM folders WHERE id = ?", (folder_id,)
        )
    if not row:
        log.warning("dup_exclude_pending folder %d not found, clearing", folder_id)
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'dup_exclude_pending'")
        return

    folder_name = row[0]["name"]
    direction = "exclude" if exclude else "include"
    log.info(
        "Resuming interrupted dup_exclude %s for folder '%s' (%d)",
        direction,
        folder_name,
        folder_id,
    )

    asyncio.create_task(toggle_dup_exclude(folder_id, exclude))


async def toggle_dup_exclude(folder_id: int, exclude: bool):
    """Set dup_exclude on a folder subtree and recalculate affected dup_counts.

    Pauses queue and stops coalesced writer for exclusive db_writer access.
    All IN clauses are batched at SQL_VAR_LIMIT (500).
    Heavy reads via open_connection(). Writes via db_writer().
    Zeroes excluded files AFTER recalc succeeds (not before).
    """
    flag = 1 if exclude else 0
    direction = "exclude" if exclude else "include"

    _progress.update(
        status="pausing",
        direction=direction,
        folder=None,
        files_total=0,
        files_done=0,
        hashes_total=0,
        hashes_done=0,
        error=None,
    )

    try:
        # --- Pause queue and stop coalesced writer ---
        await pause()
        await stop_writer()
        await broadcast(
            {"type": "queue_paused", "reason": "dup_exclude", "direction": direction}
        )

        async with read_db() as db:
            # Get folder name
            folder_row = await db.execute_fetchall(
                "SELECT name, location_id FROM folders WHERE id = ?", (folder_id,)
            )
        if not folder_row:
            log.warning("toggle_dup_exclude: folder %d not found", folder_id)
            _progress.update(status="error", error="Folder not found")
            return
        folder_name = folder_row[0]["name"]
        location_id = folder_row[0]["location_id"]
        _progress["folder"] = folder_name

        # --- Phase 1: Flag folders ---
        _progress["status"] = "flagging"
        log.info(
            "dup_exclude %s: flagging folder tree for '%s'", direction, folder_name
        )

        # Recursive CTE on folders table (small — ~100K rows max)
        async with read_db() as db:
            desc_rows = await db.execute_fetchall(
                """WITH RECURSIVE descendants(id) AS (
                   SELECT ?
                   UNION ALL
                   SELECT fo.id FROM folders fo JOIN descendants d ON fo.parent_id = d.id
               )
               SELECT id FROM descendants""",
                (folder_id,),
            )
        folder_ids = [r["id"] for r in desc_rows]

        # Batch folder flag updates
        for i in range(0, len(folder_ids), SQL_VAR_LIMIT):
            batch = folder_ids[i : i + SQL_VAR_LIMIT]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as wdb:
                await wdb.execute(
                    f"UPDATE folders SET dup_exclude = ? WHERE id IN ({ph})",
                    [flag] + batch,
                )

        log.info("dup_exclude %s: %d folders flagged", direction, len(folder_ids))
        await post_op_stats()

        # --- Phase 2: Flag files ---
        # Fetch file IDs and hashes on a dedicated connection
        conn = await open_connection()
        try:
            # Get file count
            count_rows = []
            for i in range(0, len(folder_ids), SQL_VAR_LIMIT):
                batch = folder_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch)
                cr = await conn.execute_fetchall(
                    f"SELECT COUNT(*) as cnt FROM files "
                    f"WHERE folder_id IN ({ph}) AND stale = 0",
                    batch,
                )
                count_rows.append(cr[0]["cnt"])
            file_count = sum(count_rows)
            _progress["files_total"] = file_count

            # Get file IDs
            all_file_ids = []
            for i in range(0, len(folder_ids), SQL_VAR_LIMIT):
                batch = folder_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch)
                id_rows = await conn.execute_fetchall(
                    f"SELECT id FROM files WHERE folder_id IN ({ph}) AND stale = 0",
                    batch,
                )
                all_file_ids.extend(r["id"] for r in id_rows)

            # Get distinct hashes from files in the folder tree
            all_strong = set()
            all_fast = set()
            # Get file_ids from catalog by folder scope
            all_file_ids = []
            for i in range(0, len(folder_ids), SQL_VAR_LIMIT):
                batch = folder_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch)
                id_rows = await conn.execute_fetchall(
                    f"SELECT id FROM files WHERE folder_id IN ({ph}) AND stale = 0",
                    batch,
                )
                all_file_ids.extend(r["id"] for r in id_rows)
        finally:
            await conn.close()

        # Read hashes from hashes.db
        hconn = await open_hashes_connection()
        try:
            for i in range(0, len(all_file_ids), SQL_VAR_LIMIT):
                batch = all_file_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch)
                rows = await hconn.execute_fetchall(
                    f"SELECT hash_strong, hash_fast FROM file_hashes "
                    f"WHERE file_id IN ({ph})",
                    batch,
                )
                for r in rows:
                    if r["hash_strong"]:
                        all_strong.add(r["hash_strong"])
                    elif r["hash_fast"]:
                        all_fast.add(r["hash_fast"])
        finally:
            await hconn.close()

        log.info(
            "dup_exclude %s: folder '%s' — %d folders, %d files, %d strong + %d fast hashes",
            direction,
            folder_name,
            len(folder_ids),
            file_count,
            len(all_strong),
            len(all_fast),
        )

        # Batch-update files dup_exclude flag
        updated = 0
        for i in range(0, len(all_file_ids), SQL_VAR_LIMIT):
            batch = all_file_ids[i : i + SQL_VAR_LIMIT]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as wdb:
                await wdb.execute(
                    f"UPDATE files SET dup_exclude = ? WHERE id IN ({ph})",
                    [flag] + batch,
                )
            updated += len(batch)
            _progress["files_done"] = updated

            if updated % 50000 < SQL_VAR_LIMIT:
                log.info(
                    "dup_exclude %s: %d / %d files flagged",
                    direction,
                    updated,
                    file_count,
                )

            await asyncio.sleep(0)

        # Toggle excluded flag in hashes.db — preserves hash data
        for i in range(0, len(all_file_ids), 500):
            batch = all_file_ids[i : i + 500]
            ph = ",".join("?" for _ in batch)
            async with hashes_writer() as hdb:
                await hdb.execute(
                    f"UPDATE file_hashes SET excluded = ? WHERE file_id IN ({ph})",
                    [flag] + batch,
                )

        await post_op_stats()
        await broadcast({"type": "stats_changed"})

        # --- Phase 3: Recount affected hashes ---
        _progress["status"] = "recounting"
        total_hashes = len(all_strong) + len(all_fast)
        _progress["hashes_total"] = total_hashes

        log.info(
            "dup_exclude %s: recounting %d hashes",
            direction,
            total_hashes,
        )

        recalculated = 0
        if total_hashes > 0:

            async def _on_progress(processed, _batch_total):
                nonlocal recalculated
                recalculated = processed
                _progress["hashes_done"] = processed
                # Invalidate stats periodically for real-time UI
                if processed % 1000 < SQL_VAR_LIMIT:
                    await post_op_stats()

            if all_strong:
                await _batched_recalc(
                    all_strong,
                    hash_column="hash_strong",
                    on_progress=_on_progress,
                )
                recalculated = len(all_strong)
                _progress["hashes_done"] = recalculated

            if all_fast:
                strong_offset = len(all_strong)

                async def _on_fast_progress(processed, _batch_total):
                    nonlocal recalculated
                    recalculated = strong_offset + processed
                    _progress["hashes_done"] = recalculated
                    if processed % 1000 < SQL_VAR_LIMIT:
                        await post_op_stats()

                await _batched_recalc(
                    all_fast,
                    hash_column="hash_fast",
                    on_progress=_on_fast_progress,
                )

        _progress["hashes_done"] = total_hashes

        # --- Phase 4 (EXCLUDE only): Zero dup_count on excluded files ---
        # AFTER recalc so if anything above fails, old counts are preserved
        if exclude:
            _progress["status"] = "zeroing"
            log.info("dup_exclude: zeroing dup_count on %d excluded files", file_count)
            for i in range(0, len(all_file_ids), SQL_VAR_LIMIT):
                batch = all_file_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch)
                async with db_writer() as wdb:
                    await wdb.execute(
                        f"UPDATE files SET dup_count = 0 WHERE id IN ({ph})",
                        batch,
                    )
                await asyncio.sleep(0)

        # --- Phase 5: Rebuild location sizes ---
        _progress["status"] = "rebuilding"
        log.info("dup_exclude: rebuilding location sizes for location %d", location_id)
        await recalculate_location_sizes(location_id)
        await post_op_stats()

        # Clear the pending marker — operation completed successfully
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'dup_exclude_pending'")

        _progress["status"] = "complete"
        log.info(
            "dup_exclude %s complete: folder '%s', %d files, %d hashes recalculated",
            direction,
            folder_name,
            updated,
            total_hashes,
        )
        await broadcast(
            {
                "type": "dup_exclude_completed",
                "folder": folder_name,
                "direction": direction,
                "fileCount": updated,
                "hashCount": total_hashes,
            }
        )

    except asyncio.CancelledError:
        log.info(
            "dup_exclude %s cancelled (shutdown) for folder %d", direction, folder_id
        )
        _progress.update(status="error", error="Cancelled")
    except Exception as e:
        log.exception("toggle_dup_exclude failed for folder %d", folder_id)
        _progress.update(status="error", error=str(e))
    finally:
        resume()
        await broadcast({"type": "queue_resumed"})
        await broadcast({"type": "stats_changed"})
