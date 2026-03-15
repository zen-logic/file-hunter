"""Toggle duplicate exclusion for a folder and all its descendants."""

import asyncio
import logging

from file_hunter.db import db_writer, get_db
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

FLAG_UPDATE_BATCH = 5000


async def restore_pending():
    """Resume an interrupted dup_exclude operation on startup.

    Checks for the dup_exclude_pending settings key. If found, re-runs
    the operation from scratch on a background task. Zero cost if no
    pending operation exists.
    """
    from file_hunter.services.settings import get_setting

    db = await get_db()
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
    await broadcast(
        {
            "type": "dup_exclude_started",
            "folder": folder_name,
            "direction": direction,
        }
    )

    asyncio.create_task(toggle_dup_exclude(folder_id, exclude))


async def toggle_dup_exclude(folder_id: int, exclude: bool):
    """Set dup_exclude on a folder subtree and recalculate affected dup_counts.

    The route handler has already updated the top-level folder flag and
    broadcast dup_exclude_started, so this function handles descendant
    folders, files, and dup_count recalc.

    Heavy reads via open_connection(). Writes via db_writer() in large batches.
    """
    from file_hunter.db import open_connection

    flag = 1 if exclude else 0
    direction = "exclude" if exclude else "include"

    try:
        db = await get_db()

        # Get folder name for logging (fast indexed lookup on shared reader)
        folder_row = await db.execute_fetchall(
            "SELECT name FROM folders WHERE id = ?", (folder_id,)
        )
        if not folder_row:
            log.warning("toggle_dup_exclude: folder %d not found", folder_id)
            return
        folder_name = folder_row[0]["name"]

        # Recursive CTE on folders table (small — ~100K rows max, not files)
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
        placeholders = ",".join("?" for _ in folder_ids)

        # Update all descendant folders (top-level already done by route)
        async with db_writer() as wdb:
            await wdb.execute(
                f"UPDATE folders SET dup_exclude = ? WHERE id IN ({placeholders})",
                [flag] + folder_ids,
            )
        invalidate_stats_cache()

        # Fetch file IDs on a dedicated connection (could be millions)
        conn = await open_connection()
        try:
            count_row = await conn.execute_fetchall(
                f"SELECT COUNT(*) as cnt FROM files "
                f"WHERE folder_id IN ({placeholders}) AND stale = 0",
                folder_ids,
            )
            file_count = count_row[0]["cnt"] if count_row else 0

            file_id_rows = await conn.execute_fetchall(
                f"SELECT id FROM files WHERE folder_id IN ({placeholders}) AND stale = 0",
                folder_ids,
            )
        finally:
            await conn.close()

        all_file_ids = [r["id"] for r in file_id_rows]

        log.info(
            "dup_exclude %s: folder '%s' (%d folders, %d files)",
            direction,
            folder_name,
            len(folder_ids),
            file_count,
        )

        # --- Phase 1: Batch-update files dup_exclude flag ---
        updated = 0
        last_pct_step = -1

        for i in range(0, len(all_file_ids), FLAG_UPDATE_BATCH):
            batch = all_file_ids[i : i + FLAG_UPDATE_BATCH]
            batch_ph = ",".join("?" for _ in batch)
            async with db_writer() as wdb:
                await wdb.execute(
                    f"UPDATE files SET dup_exclude = ? WHERE id IN ({batch_ph})",
                    [flag] + batch,
                )
            updated += len(batch)

            # Broadcast progress at 5% intervals (phase 1 = 0-50%)
            if file_count > 0:
                step = updated * 50 // file_count // 5
                if step > last_pct_step:
                    last_pct_step = step
                    await broadcast({"type": "dup_exclude_progress", "pct": step * 5})

            if updated % 50000 < FLAG_UPDATE_BATCH:
                log.info(
                    "dup_exclude %s: phase 1 — %d / %d files flagged",
                    direction,
                    updated,
                    file_count,
                )

            await asyncio.sleep(0)

        # Bulk-set dup_count=0 on all files in affected folders
        for i in range(0, len(all_file_ids), FLAG_UPDATE_BATCH):
            batch = all_file_ids[i : i + FLAG_UPDATE_BATCH]
            batch_ph = ",".join("?" for _ in batch)
            async with db_writer() as wdb:
                await wdb.execute(
                    f"UPDATE files SET dup_count = 0 WHERE id IN ({batch_ph})",
                    batch,
                )
            await asyncio.sleep(0)

        invalidate_stats_cache()

        # --- Phase 2: Recalculate dup_counts for shared hashes ---
        # Find hashes that have files BOTH inside and outside the affected
        # folders. Hashes unique to the affected folders are already correct
        # (dup_count=0 from the bulk set above).
        # Use dedicated connection for these heavy queries.
        conn = await open_connection()
        try:
            shared_strong_rows = await conn.execute_fetchall(
                f"""SELECT DISTINCT f.hash_strong FROM files f
                    WHERE f.folder_id IN ({placeholders})
                      AND f.hash_strong IS NOT NULL AND f.hash_strong != ''
                      AND EXISTS (
                          SELECT 1 FROM files f2
                          WHERE f2.hash_strong = f.hash_strong
                            AND f2.folder_id NOT IN ({placeholders})
                            AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                      )""",
                folder_ids + folder_ids,
            )
            shared_strong = {r["hash_strong"] for r in shared_strong_rows}

            shared_fast_rows = await conn.execute_fetchall(
                f"""SELECT DISTINCT f.hash_fast FROM files f
                    WHERE f.folder_id IN ({placeholders})
                      AND f.hash_fast IS NOT NULL AND f.hash_fast != ''
                      AND f.hash_strong IS NULL
                      AND EXISTS (
                          SELECT 1 FROM files f2
                          WHERE f2.hash_fast = f.hash_fast
                            AND f2.folder_id NOT IN ({placeholders})
                            AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                      )""",
                folder_ids + folder_ids,
            )
            shared_fast = {r["hash_fast"] for r in shared_fast_rows}
        finally:
            await conn.close()

        shared_total = len(shared_strong) + len(shared_fast)
        log.info(
            "dup_exclude %s: %d strong + %d fast shared hashes to recalculate",
            direction,
            len(shared_strong),
            len(shared_fast),
        )

        recalculated = 0
        if shared_total > 0:
            from file_hunter.services.dup_counts import _batched_recalc

            # Use large batch size for >10K hashes to reduce commit frequency
            bs = 5000 if shared_total > 10000 else 0
            last_hash_step = -1

            async def _on_progress(processed, total):
                nonlocal last_hash_step
                if total > 0:
                    step = (processed * 50 // total) // 5
                    if step > last_hash_step:
                        last_hash_step = step
                        await broadcast(
                            {"type": "dup_exclude_progress", "pct": 50 + step * 5}
                        )

            if shared_strong:
                recalculated += await _batched_recalc(
                    shared_strong,
                    hash_column="hash_strong",
                    on_progress=_on_progress,
                    batch_size=bs,
                )
            if shared_fast:
                recalculated += await _batched_recalc(
                    shared_fast,
                    hash_column="hash_fast",
                    on_progress=_on_progress,
                    batch_size=bs,
                )

        invalidate_stats_cache()

        # Clear the pending marker — operation completed successfully
        async with db_writer() as wdb:
            await wdb.execute("DELETE FROM settings WHERE key = 'dup_exclude_pending'")

        log.info(
            "dup_exclude %s complete: folder '%s', %d files, %d shared hashes recalculated",
            direction,
            folder_name,
            updated,
            recalculated,
        )
        await broadcast(
            {
                "type": "dup_exclude_completed",
                "folder": folder_name,
                "direction": direction,
                "fileCount": updated,
                "hashCount": recalculated,
            }
        )

    except asyncio.CancelledError:
        log.info(
            "dup_exclude %s cancelled (shutdown) for folder %d", direction, folder_id
        )
    except Exception:
        log.exception("toggle_dup_exclude failed for folder %d", folder_id)
