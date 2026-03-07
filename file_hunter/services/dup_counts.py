"""Stored dup_count maintenance — recalculate and backfill."""

import asyncio
import logging
from collections import defaultdict

from file_hunter.db import check_write_lock_requested, open_connection
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

RECALC_BATCH = 200


async def _batched_recalc(db, hashes, *, on_progress=None):
    """Recalculate dup_count for files sharing the given hashes.

    Uses batched GROUP BY for counts (one query per batch of RECALC_BATCH
    hashes) and grouped UPDATEs by dup_count value. Yields between batches
    and checks the cooperative write lock.

    Returns the number of hashes processed.
    """
    hash_list = list(hashes)
    total = len(hash_list)
    processed = 0

    for i in range(0, total, RECALC_BATCH):
        batch = hash_list[i : i + RECALC_BATCH]
        ph = ",".join("?" for _ in batch)

        # One GROUP BY query gives counts for the whole batch
        rows = await db.execute_fetchall(
            f"""SELECT hash_strong, COUNT(*) as cnt FROM files
                WHERE hash_strong IN ({ph})
                  AND stale = 0 AND hidden = 0 AND dup_exclude = 0
                GROUP BY hash_strong""",
            batch,
        )
        count_map = {r["hash_strong"]: r["cnt"] for r in rows}

        # Group hashes by their dup_count value for batched UPDATEs
        by_dup_count = defaultdict(list)
        zero_hashes = []
        for h in batch:
            cnt = count_map.get(h, 0)
            dc = cnt - 1 if cnt > 1 else 0
            by_dup_count[dc].append(h)
            if dc == 0:
                zero_hashes.append(h)

        # Batched UPDATE per dup_count value (active files)
        for dc, dc_hashes in by_dup_count.items():
            dc_ph = ",".join("?" for _ in dc_hashes)
            await db.execute(
                f"UPDATE files SET dup_count = ? "
                f"WHERE hash_strong IN ({dc_ph}) "
                f"AND stale = 0 AND hidden = 0 AND dup_exclude = 0",
                [dc] + dc_hashes,
            )

        # Zero out inactive files (stale/hidden/excluded) for hashes with no dups
        if zero_hashes:
            z_ph = ",".join("?" for _ in zero_hashes)
            await db.execute(
                f"UPDATE files SET dup_count = 0 "
                f"WHERE hash_strong IN ({z_ph}) "
                f"AND (stale = 1 OR hidden = 1 OR dup_exclude = 1)",
                zero_hashes,
            )

        await db.commit()
        processed += len(batch)

        if on_progress:
            await on_progress(processed, total)

        if check_write_lock_requested():
            await asyncio.sleep(0.2)
        else:
            await asyncio.sleep(0.05)

    return processed


def schedule_dup_recalc(hashes: set[str], source: str = ""):
    """Fire-and-forget dup_count recalculation on its own connection.

    Safe to call from route handlers — runs in a background task so the
    HTTP response is not blocked by per-hash UPDATE loops.
    source: human-readable label for logs/activity (e.g. location name).
    """
    hashes = {h for h in hashes if h}
    if hashes:
        asyncio.create_task(_bg_recalc_dups(hashes, source))


async def _bg_recalc_dups(hashes: set[str], source: str = ""):
    db = await open_connection()
    try:
        log.info(
            "Recalculating duplicate counts for %d hashes (%s)",
            len(hashes),
            source or "unknown",
        )
        await recalculate_dup_counts(db, hashes)
        msg = {"type": "dup_recalc_completed", "hashCount": len(hashes)}
        if source:
            msg["source"] = source
        await broadcast(msg)
    except Exception:
        log.error("Background dup_count recalc failed", exc_info=True)
    finally:
        await db.close()


async def batch_dup_counts(db, hashes: list[str]) -> dict[str, int]:
    """Return live dup counts for a batch of hash_strong values.

    Returns {hash_strong: count} where count = total non-stale files - 1.
    Only includes hashes with count > 0.  Designed for page-sized batches
    (~120 items) so always fast.
    """
    unique = {h for h in hashes if h}
    if not unique:
        return {}
    placeholders = ",".join("?" for _ in unique)
    rows = await db.execute_fetchall(
        f"""SELECT hash_strong, COUNT(*) as cnt FROM files
            WHERE hash_strong IN ({placeholders}) AND stale = 0 AND hidden = 0 AND dup_exclude = 0
            GROUP BY hash_strong HAVING COUNT(*) > 1""",
        list(unique),
    )
    return {r["hash_strong"]: r["cnt"] - 1 for r in rows}


async def recalculate_dup_counts(db, hashes: set[str], source: str = ""):
    """Recalculate dup_count for all files sharing the given hashes.

    Uses batched GROUP BY queries with yields between batches.
    Safe to call with an empty set (no-op).
    """
    if not hashes:
        return
    hashes = {h for h in hashes if h}
    if not hashes:
        return
    log.info("recalculate_dup_counts: %d hashes (%s)", len(hashes), source or "inline")
    await _batched_recalc(db, hashes)


async def backfill_dup_counts():
    """Backfill dup_count for all files on startup.

    Runs on a separate connection so the main DB stays responsive.
    Skips if no files have stale dup_counts (quick consistency check).
    """
    conn = await open_connection()
    try:
        # Quick check: any file with dup_count=0 that actually has duplicates?
        stale = await conn.execute_fetchall(
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
        if not stale:
            log.info("dup_count backfill: counts consistent, skipping")
            await broadcast({"type": "dup_backfill_completed", "skipped": True})
            return

        # Find which locations have stale counts
        stale_locs = await conn.execute_fetchall(
            """SELECT DISTINCT l.name
               FROM files f
               JOIN locations l ON l.id = f.location_id
               WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0 AND f.dup_count = 0
                 AND EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )"""
        )
        stale_loc_names = [r["name"] for r in stale_locs]

        # Only recalculate hashes that are actually wrong, not all hash groups
        dup_rows = await conn.execute_fetchall(
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
        stale_hashes = {r["hash_strong"] for r in dup_rows}

        # Also find hashes where dup_count > 0 but no longer have duplicates
        false_positive_rows = await conn.execute_fetchall(
            """SELECT DISTINCT f.hash_strong
               FROM files f
               WHERE f.dup_count > 0 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0
                 AND NOT EXISTS (
                     SELECT 1 FROM files f2
                     WHERE f2.hash_strong = f.hash_strong
                       AND f2.id != f.id AND f2.stale = 0 AND f2.hidden = 0 AND f2.dup_exclude = 0
                 )"""
        )
        false_positive_hashes = {r["hash_strong"] for r in false_positive_rows}

        all_stale = stale_hashes | false_positive_hashes
        total_hashes = len(all_stale)

        loc_label = ", ".join(stale_loc_names[:5])
        if len(stale_loc_names) > 5:
            loc_label += f" + {len(stale_loc_names) - 5} more"
        log.info(
            "dup_count backfill: %d stale hashes across %d locations (%s)",
            total_hashes,
            len(stale_loc_names),
            loc_label,
        )
        await broadcast(
            {
                "type": "dup_backfill_started",
                "totalHashes": total_hashes,
                "locations": stale_loc_names,
            }
        )

        async def _on_progress(processed, total):
            if processed % 10000 < RECALC_BATCH:
                log.info("dup_count backfill: %d / %d hashes", processed, total)
                await broadcast(
                    {
                        "type": "dup_backfill_progress",
                        "processed": processed,
                        "totalHashes": total,
                    }
                )

        updated = await _batched_recalc(conn, all_stale, on_progress=_on_progress)

        invalidate_stats_cache()

        log.info(
            "dup_count backfill: complete, fixed %d hashes",
            updated,
        )
        await broadcast({"type": "dup_backfill_completed", "updated": updated})

    except Exception:
        log.exception("dup_count backfill failed")
    finally:
        await conn.close()
