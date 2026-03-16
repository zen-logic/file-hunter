"""Stored dup_count maintenance — recalculate and backfill.

Write serialization: all dup recalc work is submitted to a shared queue
and processed by a single long-lived background task. All writes go
through the single db_writer() connection — no independent writers.

Dual-hash support: files with hash_strong use hash_strong for dup grouping.
Files without hash_strong use hash_fast. The dup_count on each file
reflects duplicates found via its effective hash.
"""

import asyncio
import logging
from collections import defaultdict

from file_hunter.db import db_writer, get_db
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

RECALC_BATCH = 200

# Coalesced writer state — single background task
_recalc_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None


async def _batched_recalc(
    hashes, *, hash_column: str = "hash_strong", on_progress=None, batch_size: int = 0
):
    """Recalculate dup_count for files sharing the given hashes.

    hash_column: "hash_strong" or "hash_fast".
    - hash_strong: GROUP BY hash_strong, UPDATE all matching files.
    - hash_fast: GROUP BY hash_fast (counting ALL files with that hash),
      but UPDATE only files WHERE hash_strong IS NULL (verified files
      get their dup_count from hash_strong grouping).

    Uses batched GROUP BY for counts (one query per batch of RECALC_BATCH
    hashes) and grouped UPDATEs by dup_count value. Yields between batches.

    Reads via get_db(), writes via db_writer() per batch.

    Returns the number of hashes processed.
    """
    hash_list = list(hashes)
    total = len(hash_list)
    processed = 0
    effective_batch = batch_size if batch_size > 0 else RECALC_BATCH
    db = await get_db()

    # For hash_fast updates, only target files without hash_strong
    update_extra = " AND hash_strong IS NULL" if hash_column == "hash_fast" else ""

    for i in range(0, total, effective_batch):
        batch = hash_list[i : i + effective_batch]
        ph = ",".join("?" for _ in batch)

        # One GROUP BY query gives counts for the whole batch (read)
        rows = await db.execute_fetchall(
            f"""SELECT {hash_column}, COUNT(*) as cnt FROM files
                WHERE {hash_column} IN ({ph})
                  AND stale = 0 AND hidden = 0 AND dup_exclude = 0
                GROUP BY {hash_column}""",
            batch,
        )
        count_map = {r[hash_column]: r["cnt"] for r in rows}

        # Group hashes by their dup_count value for batched UPDATEs
        by_dup_count = defaultdict(list)
        for h in batch:
            cnt = count_map.get(h, 0)
            dc = cnt - 1 if cnt > 1 else 0
            by_dup_count[dc].append(h)

        # Write phase: db_writer() per batch
        async with db_writer() as wdb:
            # Batched UPDATE per dup_count value (active files)
            for dc, dc_hashes in by_dup_count.items():
                dc_ph = ",".join("?" for _ in dc_hashes)
                await wdb.execute(
                    f"UPDATE files SET dup_count = ? "
                    f"WHERE {hash_column} IN ({dc_ph}) "
                    f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0"
                    f"{update_extra}",
                    [dc] + dc_hashes,
                )

        processed += len(batch)

        if on_progress:
            await on_progress(processed, total)

        if processed % 10000 < effective_batch or processed == total:
            log.info(
                "_batched_recalc: %s — %d / %d hashes",
                hash_column,
                processed,
                total,
            )

        await asyncio.sleep(0.05)

    return processed


SQL_VAR_LIMIT = 500
FULL_RECOUNT_WRITE_BATCH = SQL_VAR_LIMIT


async def full_dup_recount(
    *, location_id: int | None = None, on_progress=None, on_total=None
):
    """Recount dup_count for hashes. Used by catalog repair and import.

    Instead of thousands of small batches each with a commit (death on
    spinning disk), this does:
    1. One GROUP BY query per hash type on a dedicated connection
    2. Builds complete count map in memory
    3. Writes in large batches (5,000 hashes per commit)

    location_id: when set, only recount hashes that appear on this location.
    The GROUP BY is scoped to find those hashes, but the UPDATE targets all
    files with those hashes (dups span locations).

    on_progress(total_processed): called after each write batch.
    on_total(total): called once after both GROUP BY queries, before writes.
    """
    from file_hunter.db import open_connection

    scope_label = f"location #{location_id}" if location_id else "all"

    # Query both hash types first to get total before writes start
    hash_configs = [
        ("hash_strong", "", ""),
        ("hash_fast", " AND hash_strong IS NULL", " AND hash_strong IS NULL"),
    ]
    count_maps: list[dict[str, int]] = []

    for hash_column, extra_where, _ in hash_configs:
        log.info("full_dup_recount (%s): querying %s counts", scope_label, hash_column)
        conn = await open_connection()
        try:
            if location_id:
                # Find hashes on this location, then count across ALL locations
                hash_rows = await conn.execute_fetchall(
                    f"SELECT DISTINCT {hash_column} FROM files "
                    f"WHERE location_id = ? AND {hash_column} IS NOT NULL "
                    f"AND {hash_column} != ''{extra_where}",
                    (location_id,),
                )
                target_hashes = [r[hash_column] for r in hash_rows]
                if target_hashes:
                    # Count globally for these hashes — batched to stay under SQL variable limit
                    rows = []
                    for bi in range(0, len(target_hashes), SQL_VAR_LIMIT):
                        chunk = target_hashes[bi : bi + SQL_VAR_LIMIT]
                        ph = ",".join("?" for _ in chunk)
                        chunk_rows = await conn.execute_fetchall(
                            f"SELECT {hash_column}, COUNT(*) as cnt FROM files "
                            f"WHERE {hash_column} IN ({ph}) "
                            f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0 "
                            f"GROUP BY {hash_column}",
                            chunk,
                        )
                        rows.extend(chunk_rows)
                    log.info(
                        "full_dup_recount (%s): counted %d %s hashes in %d batches",
                        scope_label,
                        len(target_hashes),
                        hash_column,
                        (len(target_hashes) + SQL_VAR_LIMIT - 1) // SQL_VAR_LIMIT,
                    )
                else:
                    rows = []
            else:
                # Collect all distinct hashes, then batch the COUNT queries
                hash_rows = await conn.execute_fetchall(
                    f"SELECT DISTINCT {hash_column} FROM files "
                    f"WHERE {hash_column} IS NOT NULL AND {hash_column} != ''"
                    f"{extra_where}",
                )
                all_hashes = [r[hash_column] for r in hash_rows]
                rows = []
                if all_hashes:
                    for bi in range(0, len(all_hashes), SQL_VAR_LIMIT):
                        chunk = all_hashes[bi : bi + SQL_VAR_LIMIT]
                        ph = ",".join("?" for _ in chunk)
                        chunk_rows = await conn.execute_fetchall(
                            f"SELECT {hash_column}, COUNT(*) as cnt FROM files "
                            f"WHERE {hash_column} IN ({ph}) "
                            f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0 "
                            f"GROUP BY {hash_column}",
                            chunk,
                        )
                        rows.extend(chunk_rows)
                    log.info(
                        "full_dup_recount (%s): counted %d %s hashes in %d batches",
                        scope_label,
                        len(all_hashes),
                        hash_column,
                        (len(all_hashes) + SQL_VAR_LIMIT - 1) // SQL_VAR_LIMIT,
                    )
        finally:
            await conn.close()

        cm: dict[str, int] = {}
        for r in rows:
            cnt = r["cnt"]
            dc = cnt - 1 if cnt > 1 else 0
            cm[r[hash_column]] = dc
        count_maps.append(cm)
        log.info(
            "full_dup_recount (%s): %d distinct %s values",
            scope_label,
            len(cm),
            hash_column,
        )

    grand_total = sum(len(cm) for cm in count_maps)
    if on_total:
        await on_total(grand_total)
    log.info(
        "full_dup_recount (%s): %d total hashes, writing", scope_label, grand_total
    )

    total_processed = 0

    for (hash_column, _, update_extra), count_map in zip(hash_configs, count_maps):
        total_hashes = len(count_map)
        log.info(
            "full_dup_recount: writing %s — %d hashes in batches of %d",
            hash_column,
            total_hashes,
            FULL_RECOUNT_WRITE_BATCH,
        )

        # Group by dup_count value for efficient UPDATEs
        by_dc: dict[int, list[str]] = defaultdict(list)
        for h, dc in count_map.items():
            by_dc[dc].append(h)

        # Write in large batches — far fewer commits
        written = 0
        for dc, dc_hashes in by_dc.items():
            for i in range(0, len(dc_hashes), FULL_RECOUNT_WRITE_BATCH):
                batch = dc_hashes[i : i + FULL_RECOUNT_WRITE_BATCH]
                ph = ",".join("?" for _ in batch)
                async with db_writer() as wdb:
                    await wdb.execute(
                        f"UPDATE files SET dup_count = ? "
                        f"WHERE {hash_column} IN ({ph}) "
                        f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0"
                        f"{update_extra}",
                        [dc] + batch,
                    )
                written += len(batch)
                total_processed += len(batch)
                if on_progress:
                    await on_progress(total_processed)
                if written % 50000 < FULL_RECOUNT_WRITE_BATCH:
                    log.info(
                        "full_dup_recount: %s — %d / %d hashes written",
                        hash_column,
                        written,
                        total_hashes,
                    )
                await asyncio.sleep(0)

        # No zero-out for inactive files (stale/hidden/excluded) — their
        # dup_count is never read. If they become active again, their hash
        # gets recounted at that point.

        log.info(
            "full_dup_recount: %s complete — %d hashes, %d rows written",
            hash_column,
            total_hashes,
            written,
        )

    return total_processed


def submit_hashes_for_recalc(
    *,
    strong_hashes: set[str] | None = None,
    fast_hashes: set[str] | None = None,
    source: str = "",
    location_ids: set[int] | None = None,
):
    """Submit hashes to the coalesced dup recalc writer.

    Non-blocking. Hashes are merged with any pending work and processed
    by a single background task on one DB connection. Safe to call from
    any context — scan loops, route handlers, backfill tasks.

    strong_hashes: hash_strong values — files grouped by hash_strong.
    fast_hashes: hash_fast values — files grouped by hash_fast (only
    updates files without hash_strong).

    Also updates stored locations.duplicate_count for all affected
    locations after recalculating files.dup_count.
    """
    global _recalc_queue, _writer_task
    strong = {h for h in (strong_hashes or set()) if h}
    fast = {h for h in (fast_hashes or set()) if h}
    if not strong and not fast:
        return
    if _recalc_queue is None:
        _recalc_queue = asyncio.Queue()
    _recalc_queue.put_nowait((strong, fast, source, location_ids or set()))
    if _writer_task is None or _writer_task.done():
        _writer_task = asyncio.create_task(_dup_recalc_writer())


async def stop_writer():
    """Wait for the dup recalc writer to finish current work, then stop it."""
    global _writer_task
    if _writer_task and not _writer_task.done():
        log.info("Waiting for dup recalc writer to complete...")
        try:
            await asyncio.wait_for(_writer_task, timeout=10)
        except asyncio.TimeoutError:
            _writer_task.cancel()
            try:
                await _writer_task
            except (asyncio.CancelledError, Exception):
                pass
    _writer_task = None


async def _dup_recalc_writer():
    """Single long-lived task that drains the hash queue.

    Processes work items, coalesces rapid submissions into larger batches.
    All writes go through db_writer(). Shuts down after 10s idle.
    """
    try:
        while True:
            # Wait for work (shut down after 10s idle)
            try:
                item = await asyncio.wait_for(_recalc_queue.get(), timeout=10.0)
            except asyncio.TimeoutError:
                break

            # Coalesce: drain any additional items that accumulated
            merged_strong: set[str] = set(item[0])
            merged_fast: set[str] = set(item[1])
            merged_sources: list[str] = [item[2]] if item[2] else []
            merged_location_ids: set[int] = set(item[3])

            while not _recalc_queue.empty():
                try:
                    more = _recalc_queue.get_nowait()
                    merged_strong.update(more[0])
                    merged_fast.update(more[1])
                    if more[2]:
                        merged_sources.append(more[2])
                    merged_location_ids.update(more[3])
                except asyncio.QueueEmpty:
                    break

            source_label = ", ".join(merged_sources[:3])
            if len(merged_sources) > 3:
                source_label += f" +{len(merged_sources) - 3}"

            log.info(
                "Coalesced dup recalc: %d strong + %d fast hashes (%s)",
                len(merged_strong),
                len(merged_fast),
                source_label or "unknown",
            )

            # Recalculate dup_count on files
            await recalculate_dup_counts(
                strong_hashes=merged_strong,
                fast_hashes=merged_fast,
                source=source_label,
            )

            # Update stored duplicate_count for affected locations
            db = await get_db()
            affected: set[int] = set(merged_location_ids)

            if merged_strong:
                h_list = list(merged_strong)
                h_ph = ",".join("?" for _ in h_list)
                rows = await db.execute_fetchall(
                    f"SELECT DISTINCT location_id FROM files WHERE hash_strong IN ({h_ph})",
                    h_list,
                )
                affected |= {r["location_id"] for r in rows}

            if merged_fast:
                h_list = list(merged_fast)
                h_ph = ",".join("?" for _ in h_list)
                rows = await db.execute_fetchall(
                    f"SELECT DISTINCT location_id FROM files WHERE hash_fast IN ({h_ph})",
                    h_list,
                )
                affected |= {r["location_id"] for r in rows}

            # Read counts per location, then batch-write
            loc_updates = []
            for lid in affected:
                dc_rows = await db.execute_fetchall(
                    "SELECT COUNT(*) as c FROM files "
                    "WHERE location_id = ? AND stale = 0 AND hidden = 0 "
                    "AND dup_exclude = 0 AND dup_count > 0",
                    (lid,),
                )
                loc_updates.append((dc_rows[0]["c"], lid))

            async with db_writer() as wdb:
                for dc, lid in loc_updates:
                    await wdb.execute(
                        "UPDATE locations SET duplicate_count = ? WHERE id = ?",
                        (dc, lid),
                    )

            invalidate_stats_cache()

            total_hashes = len(merged_strong) + len(merged_fast)
            await broadcast({"type": "dup_recalc_completed", "hashCount": total_hashes})
    except Exception:
        log.error("Coalesced dup recalc writer failed", exc_info=True)


async def batch_dup_counts(
    db,
    strong_hashes: list[str] | None = None,
    fast_hashes: list[str] | None = None,
) -> dict[str, int]:
    """Return live dup counts for a batch of hashes.

    strong_hashes: GROUP BY hash_strong for verified files.
    fast_hashes: GROUP BY hash_fast for unverified files.

    Returns {hash: count} where count = total non-stale files - 1.
    Only includes hashes with count > 0.  Designed for page-sized batches
    (~120 items) so always fast.
    """
    result: dict[str, int] = {}

    unique_strong = {h for h in (strong_hashes or []) if h}
    if unique_strong:
        ph = ",".join("?" for _ in unique_strong)
        rows = await db.execute_fetchall(
            f"""SELECT hash_strong, COUNT(*) as cnt FROM files
                WHERE hash_strong IN ({ph}) AND stale = 0 AND hidden = 0 AND dup_exclude = 0
                GROUP BY hash_strong HAVING COUNT(*) > 1""",
            list(unique_strong),
        )
        for r in rows:
            result[r["hash_strong"]] = r["cnt"] - 1

    unique_fast = {h for h in (fast_hashes or []) if h}
    if unique_fast:
        ph = ",".join("?" for _ in unique_fast)
        rows = await db.execute_fetchall(
            f"""SELECT hash_fast, COUNT(*) as cnt FROM files
                WHERE hash_fast IN ({ph}) AND stale = 0 AND hidden = 0 AND dup_exclude = 0
                GROUP BY hash_fast HAVING COUNT(*) > 1""",
            list(unique_fast),
        )
        for r in rows:
            result[r["hash_fast"]] = r["cnt"] - 1

    return result


async def recalculate_dup_counts(
    strong_hashes: set[str] | None = None,
    fast_hashes: set[str] | None = None,
    source: str = "",
):
    """Recalculate dup_count for all files sharing the given hashes.

    Uses batched GROUP BY queries with yields between batches.
    All writes go through db_writer(). Safe to call with empty sets (no-op).
    """
    strong = {h for h in (strong_hashes or set()) if h}
    fast = {h for h in (fast_hashes or set()) if h}
    if not strong and not fast:
        return

    total = len(strong) + len(fast)
    log.info(
        "recalculate_dup_counts: %d strong + %d fast hashes (%s)",
        len(strong),
        len(fast),
        source or "inline",
    )

    progress_offset = 0

    async def _on_progress(processed, _batch_total):
        actual = progress_offset + processed
        if actual % 1000 < RECALC_BATCH:
            log.info(
                "recalculate_dup_counts: %d/%d hashes (%s)",
                actual,
                total,
                source or "inline",
            )

    if strong:
        await _batched_recalc(
            strong, hash_column="hash_strong", on_progress=_on_progress
        )
        progress_offset = len(strong)

    if fast:
        await _batched_recalc(fast, hash_column="hash_fast", on_progress=_on_progress)


async def backfill_dup_counts():
    """Backfill dup_count for all files on startup.

    Reads via get_db(), writes via db_writer() (inside _batched_recalc).
    Skips if no files have stale dup_counts (quick consistency check).
    Checks both hash_strong and hash_fast groupings.
    """
    db = await get_db()
    try:
        # Quick check: any file with dup_count=0 that actually has duplicates?
        # Check hash_strong grouping
        stale_strong_check = await db.execute_fetchall(
            """SELECT 1 FROM files f
               WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )
               LIMIT 1"""
        )
        # Check hash_fast grouping (files without hash_strong)
        stale_fast_check = await db.execute_fetchall(
            """SELECT 1 FROM files f
               WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                 AND f.hash_strong IS NULL
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_fast = f.hash_fast
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )
               LIMIT 1"""
        )

        if not stale_strong_check and not stale_fast_check:
            log.info("dup_count backfill: counts consistent, skipping")
            await broadcast({"type": "dup_backfill_completed", "skipped": True})
            return

        # Collect stale hash_strong values
        strong_hashes: set[str] = set()
        if stale_strong_check:
            rows = await db.execute_fetchall(
                """SELECT DISTINCT f.hash_strong
                   FROM files f
                   WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                     AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                     AND EXISTS (
                         SELECT 1 FROM files f2
                         WHERE f2.hash_strong = f.hash_strong
                           AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                     )"""
            )
            strong_hashes = {r["hash_strong"] for r in rows}

            # Also find false positives for hash_strong
            fp_rows = await db.execute_fetchall(
                """SELECT DISTINCT f.hash_strong
                   FROM files f
                   WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                     AND f.dup_count > 0 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0
                     AND NOT EXISTS (
                         SELECT 1 FROM files f2
                         WHERE f2.hash_strong = f.hash_strong
                           AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                     )"""
            )
            strong_hashes |= {r["hash_strong"] for r in fp_rows}

        # Collect stale hash_fast values (files without hash_strong)
        fast_hashes: set[str] = set()
        if stale_fast_check:
            rows = await db.execute_fetchall(
                """SELECT DISTINCT f.hash_fast
                   FROM files f
                   WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                     AND f.hash_strong IS NULL
                     AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                     AND EXISTS (
                         SELECT 1 FROM files f2
                         WHERE f2.hash_fast = f.hash_fast
                           AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                     )"""
            )
            fast_hashes = {r["hash_fast"] for r in rows}

            # Also find false positives for hash_fast
            fp_rows = await db.execute_fetchall(
                """SELECT DISTINCT f.hash_fast
                   FROM files f
                   WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                     AND f.hash_strong IS NULL
                     AND f.dup_count > 0 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0
                     AND NOT EXISTS (
                         SELECT 1 FROM files f2
                         WHERE f2.hash_fast = f.hash_fast
                           AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                     )"""
            )
            fast_hashes |= {r["hash_fast"] for r in fp_rows}

        total_hashes = len(strong_hashes) + len(fast_hashes)
        if total_hashes == 0:
            log.info("dup_count backfill: counts consistent, skipping")
            await broadcast({"type": "dup_backfill_completed", "skipped": True})
            return

        log.info(
            "dup_count backfill: %d strong + %d fast stale hashes",
            len(strong_hashes),
            len(fast_hashes),
        )
        await broadcast(
            {
                "type": "dup_backfill_started",
                "totalHashes": total_hashes,
            }
        )

        progress_offset = 0

        async def _on_progress(processed, _batch_total):
            nonlocal progress_offset
            actual = progress_offset + processed
            if actual % 10000 < RECALC_BATCH:
                log.info("dup_count backfill: %d / %d hashes", actual, total_hashes)
                await broadcast(
                    {
                        "type": "dup_backfill_progress",
                        "processed": actual,
                        "totalHashes": total_hashes,
                    }
                )

        updated = 0
        if strong_hashes:
            updated += await _batched_recalc(
                strong_hashes, hash_column="hash_strong", on_progress=_on_progress
            )
            progress_offset = len(strong_hashes)

        if fast_hashes:
            updated += await _batched_recalc(
                fast_hashes, hash_column="hash_fast", on_progress=_on_progress
            )

        invalidate_stats_cache()

        log.info(
            "dup_count backfill: complete, fixed %d hashes",
            updated,
        )
        await broadcast({"type": "dup_backfill_completed", "updated": updated})

    except Exception:
        log.exception("dup_count backfill failed")
