"""Stored dup_count maintenance — recalculate and backfill.

Write serialization: all dup recalc work is submitted to a shared queue
and processed by a single long-lived background task. All writes go
through hashes_writer() — the hashes DB's own writer, independent of
the catalog writer. No write contention with scan ingest or file ops.

Dual-hash support: files with hash_strong use hash_strong for dup grouping.
Files without hash_strong use hash_fast. The dup_count on each file_hashes
row reflects duplicates found via its effective hash.

Excluded files are flagged in file_hashes (excluded=1) and filtered
via the active_hashes view. All read queries use active_hashes;
writes target file_hashes directly.
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, Awaitable

import httpx

from file_hunter.db import db_writer, open_connection, read_db
from file_hunter.hashes_db import hashes_writer, read_hashes, open_hashes_connection
from file_hunter.helpers import post_op_stats
from file_hunter.services.activity import (
    register as _act_reg,
    unregister as _act_unreg,
    update as _act_upd,
)
from file_hunter.services.agent_ops import dispatch, hash_fast_batch, hash_partial_batch
from file_hunter.stats_db import apply_dup_deltas, stats_writer
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

RECALC_BATCH = 200

# Coalesced writer state — single background task
_recalc_queue: asyncio.Queue | None = None
_writer_task: asyncio.Task | None = None
_active_recalc_locations: set[int] = set()


def get_active_recalc_locations() -> set[int]:
    """Return a copy of location IDs currently undergoing dup recalculation.

    Returns:
        set[int]: Location IDs with in-progress dup recalc. Always a copy,
        safe to mutate.

    Side effects: None (read-only).

    Callers: ws/scan.py late-join handler — sends current recalc state to
    newly connected WebSocket clients.
    """
    return set(_active_recalc_locations)


async def update_dup_counts_inline(
    hashes: set[str],
    on_progress: Callable[[int, int, int], Awaitable[None]] | None = None,
    hash_column: str = "hash_fast",
) -> set[int]:
    """Update dup_count for files sharing the given hashes. Returns affected location_ids.

    Queries active_hashes for actual duplicate counts, updates file_hashes and
    the denormalized catalog dup_count. Applies incremental dup deltas to
    stats.db so folder and location duplicate counts update in real time.

    hash_column: "hash_fast" or "hash_strong". When "hash_fast", only targets
    rows WHERE hash_strong IS NULL (verified files get dup_count from
    hash_strong grouping, not hash_fast).

    on_progress: async callback(processed, total, dups_confirmed) called
    after each batch of SQL_VAR_LIMIT hashes.
    """
    if not hashes:
        return set()

    update_extra = " AND hash_strong IS NULL" if hash_column == "hash_fast" else ""

    affected_locs: set[int] = set()
    dup_deltas_by_loc: dict[int, list[tuple[int | None, int]]] = defaultdict(list)
    dups_confirmed = 0

    h_list = list(hashes)
    total = len(h_list)

    for i in range(0, len(h_list), SQL_VAR_LIMIT):
        batch = h_list[i : i + SQL_VAR_LIMIT]
        ph = ",".join("?" for _ in batch)

        # Count files per hash
        async with read_hashes() as hdb:
            rows = await hdb.execute_fetchall(
                f"SELECT {hash_column}, COUNT(*) as cnt FROM active_hashes "
                f"WHERE {hash_column} IN ({ph}){update_extra} "
                f"GROUP BY {hash_column}",
                batch,
            )

        if not rows:
            continue

        for r in rows:
            hv = r[hash_column]
            cnt = r["cnt"]
            dc = cnt - 1 if cnt > 1 else 0

            # Read current dup_count before update
            async with hashes_writer() as wdb:
                old_rows_cur = await wdb.execute(
                    f"SELECT file_id, location_id, dup_count FROM file_hashes "
                    f"WHERE {hash_column} = ?{update_extra}",
                    (hv,),
                )
                old_rows = await old_rows_cur.fetchall()

                await wdb.execute(
                    f"UPDATE file_hashes SET dup_count = ? "
                    f"WHERE {hash_column} = ?{update_extra}",
                    (dc, hv),
                )

            if not old_rows:
                continue

            fids = [fr["file_id"] for fr in old_rows]
            affected_locs.update(fr["location_id"] for fr in old_rows)

            # Get folder_ids from catalog
            fid_to_old_dc = {fr["file_id"]: fr["dup_count"] for fr in old_rows}
            fid_to_loc = {fr["file_id"]: fr["location_id"] for fr in old_rows}

            for j in range(0, len(fids), 500):
                id_batch = fids[j : j + 500]
                id_ph = ",".join("?" for _ in id_batch)

                async with db_writer() as cdb:
                    await cdb.execute(
                        f"UPDATE files SET dup_count = ? WHERE id IN ({id_ph})",
                        [dc] + id_batch,
                    )

                async with read_db() as rdb:
                    folder_rows = await rdb.execute_fetchall(
                        f"SELECT id, folder_id FROM files WHERE id IN ({id_ph})",
                        id_batch,
                    )

                for fr in folder_rows:
                    fid = fr["id"]
                    old_dc = fid_to_old_dc.get(fid, 0)
                    loc_id = fid_to_loc.get(fid)
                    was_dup = old_dc > 0
                    is_dup = dc > 0
                    if was_dup != is_dup:
                        delta = 1 if is_dup else -1
                        dup_deltas_by_loc[loc_id].append((fr["folder_id"], delta))
                        if is_dup:
                            dups_confirmed += 1

        if on_progress:
            processed = min(i + len(batch), total)
            await on_progress(processed, total, dups_confirmed)

    # Apply accumulated dup deltas per location
    if dup_deltas_by_loc:
        loc_ids = list(dup_deltas_by_loc.keys())
        async with read_db() as rdb:
            ph = ",".join("?" for _ in loc_ids)
            loc_names = await rdb.execute_fetchall(
                f"SELECT id, name FROM locations WHERE id IN ({ph})", loc_ids
            )
        name_map = {r["id"]: r["name"] for r in loc_names}

        for loc_id, deltas in dup_deltas_by_loc.items():
            net = sum(d for _, d in deltas)
            async with read_db() as rdb:
                fp_rows = await rdb.execute_fetchall(
                    "SELECT id, parent_id FROM folders WHERE location_id = ?",
                    (loc_id,),
                )
            folder_parents = {r["id"]: r["parent_id"] for r in fp_rows}
            await apply_dup_deltas(loc_id, folder_parents, deltas)
            loc_name = name_map.get(loc_id, f"location {loc_id}")
            log.info(
                "Dup delta: %s %+d (%d files changed)",
                loc_name,
                net,
                len(deltas),
            )

    return affected_locs


SQL_VAR_LIMIT = 500
FULL_RECOUNT_WRITE_BATCH = 5000


async def full_dup_recount(
    *, location_id: int | None = None, on_progress=None, on_total=None
):
    """Recount dup_count for every hash in hashes.db (full rebuild).

    Args:
        location_id: When set, only recount hashes that appear on this
            location. The GROUP BY is scoped to find those hashes, but the
            UPDATE targets all rows with those hashes (dups span locations).
            None = recount all hashes globally.
        on_progress: Optional async callback(total_processed) called after
            each write batch.
        on_total: Optional async callback(total) called once after both
            GROUP BY queries complete, before writes begin.

    Returns:
        int: Total number of hashes written.

    Side effects:
        - Writes dup_count to hashes.db via hashes_writer().
        - Writes denormalized dup_count to catalog files table via db_writer().

    Callers: routes/stats.py catalog repair endpoint, routes/import_catalog.py
    post-import recount.

    Implementation: one GROUP BY per hash type (hash_strong, hash_fast) on a
    dedicated connection. Builds a complete {hash: count} map in memory, then
    groups hashes by dup_count value and writes in FULL_RECOUNT_WRITE_BATCH
    (5,000) chunks. Location-scoped recounts use a temp table + JOIN to
    identify the affected hashes, but still UPDATE globally.
    """
    scope_label = f"location #{location_id}" if location_id else "all"

    hash_configs = [
        ("hash_strong", ""),
        ("hash_fast", " AND hash_strong IS NULL"),
    ]
    count_maps: list[dict[str, int]] = []

    for hash_column, update_extra in hash_configs:
        log.info("full_dup_recount (%s): querying %s counts", scope_label, hash_column)
        conn = await open_hashes_connection()
        try:
            if location_id:
                await conn.execute("CREATE TEMP TABLE _recount_hashes (hash_val TEXT)")
                await conn.execute(
                    f"INSERT INTO _recount_hashes "
                    f"SELECT DISTINCT {hash_column} FROM active_hashes "
                    f"WHERE location_id = ? AND {hash_column} IS NOT NULL "
                    f"AND {hash_column} != ''",
                    (location_id,),
                )
                await conn.execute(
                    "CREATE INDEX _recount_hashes_idx ON _recount_hashes(hash_val)"
                )
                await conn.commit()

                rows = await conn.execute_fetchall(
                    f"SELECT f.{hash_column} as hash_val, COUNT(*) as cnt "
                    f"FROM _recount_hashes h "
                    f"CROSS JOIN file_hashes f "
                    f"WHERE f.excluded = 0 AND f.stale = 0 "
                    f"AND f.{hash_column} = h.hash_val "
                    f"GROUP BY f.{hash_column}"
                )
                await conn.execute("DROP TABLE IF EXISTS _recount_hashes")
            else:
                rows = await conn.execute_fetchall(
                    f"SELECT {hash_column} as hash_val, COUNT(*) as cnt "
                    f"FROM active_hashes "
                    f"WHERE {hash_column} IS NOT NULL AND {hash_column} != ''"
                    f"{update_extra} "
                    f"GROUP BY {hash_column}"
                )
        finally:
            try:
                await conn.execute("DROP TABLE IF EXISTS _recount_hashes")
            except Exception:
                pass
            await conn.close()

        cm: dict[str, int] = {}
        for r in rows:
            cnt = r["cnt"]
            cm[r["hash_val"]] = cnt - 1 if cnt > 1 else 0
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

    for (hash_column, update_extra), count_map in zip(hash_configs, count_maps):
        total_hashes = len(count_map)
        log.info(
            "full_dup_recount: writing %s — %d hashes in batches of %d",
            hash_column,
            total_hashes,
            FULL_RECOUNT_WRITE_BATCH,
        )

        by_dc: dict[int, list[str]] = defaultdict(list)
        for h, dc in count_map.items():
            by_dc[dc].append(h)

        written = 0
        for dc, dc_hashes in by_dc.items():
            for i in range(0, len(dc_hashes), FULL_RECOUNT_WRITE_BATCH):
                batch = dc_hashes[i : i + FULL_RECOUNT_WRITE_BATCH]
                ph = ",".join("?" for _ in batch)
                async with hashes_writer() as wdb:
                    await wdb.execute(
                        f"UPDATE file_hashes SET dup_count = ? "
                        f"WHERE {hash_column} IN ({ph})"
                        f"{update_extra}",
                        [dc] + batch,
                    )
                    affected_rows = await wdb.execute(
                        f"SELECT file_id FROM file_hashes "
                        f"WHERE {hash_column} IN ({ph})"
                        f"{update_extra}",
                        batch,
                    )
                    affected_ids = [r[0] for r in await affected_rows.fetchall()]

                if affected_ids:
                    for j in range(0, len(affected_ids), 500):
                        id_batch = affected_ids[j : j + 500]
                        id_ph = ",".join("?" for _ in id_batch)
                        async with db_writer() as cdb:
                            await cdb.execute(
                                f"UPDATE files SET dup_count = ? WHERE id IN ({id_ph})",
                                [dc] + id_batch,
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

        log.info(
            "full_dup_recount: %s complete — %d hashes written",
            hash_column,
            written,
        )

    return total_processed


RECOUNT_BATCH_ROWS = 1_500_000


async def optimized_dup_recount(*, on_progress=None):
    """Recount dup_count for all hashes using bulk SQL operations.

    Uses batched GROUP BY for discovery, bulk UPDATE for writes, and
    batched catalog sync. All operations report progress via on_progress
    so the UI stays responsive (no silent period exceeds ~15 seconds).

    Args:
        on_progress: Optional async callback(step, done, total) called
            between batches. step is one of: "discovery", "reset",
            "write", "catalog".

    Returns:
        int: Number of duplicate hash groups found.

    Side effects:
        - Creates/drops temporary tables in hashes.db (_partial_strong,
          _partial_fast, _dup_strong, _dup_fast).
        - Resets all dup_count to 0 then writes correct values.
        - Writes denormalized dup_count to catalog files table.

    Callers: routes/stats.py catalog repair.
    """
    log.info("optimized_dup_recount: starting")

    # Get file_id boundaries for equal-row batches
    conn = await open_hashes_connection()
    try:
        row = await conn.execute_fetchall(
            "SELECT COUNT(*) as c, MIN(file_id) as mn, "
            "MAX(file_id) as mx FROM file_hashes"
        )
        total_rows = row[0]["c"]
        min_fid = row[0]["mn"] or 0
        max_fid = row[0]["mx"] or 0

        boundaries = []
        for offset in range(
            RECOUNT_BATCH_ROWS, total_rows, RECOUNT_BATCH_ROWS
        ):
            brow = await conn.execute_fetchall(
                "SELECT file_id FROM file_hashes "
                "ORDER BY file_id LIMIT 1 OFFSET ?",
                (offset,),
            )
            if brow:
                boundaries.append(brow[0]["file_id"])
    finally:
        await conn.close()

    ranges = []
    prev = min_fid
    for b in boundaries:
        ranges.append((prev, b))
        prev = b
    ranges.append((prev, max_fid + 1))
    num_batches = len(ranges)

    log.info(
        "optimized_dup_recount: %d rows, %d batches of ~%d",
        total_rows,
        num_batches,
        RECOUNT_BATCH_ROWS,
    )

    # Clean up from any previous failed run
    async with hashes_writer() as wdb:
        await wdb.execute("DROP TABLE IF EXISTS _partial_strong")
        await wdb.execute("DROP TABLE IF EXISTS _partial_fast")
        await wdb.execute("DROP TABLE IF EXISTS _dup_strong")
        await wdb.execute("DROP TABLE IF EXISTS _dup_fast")
        await wdb.execute(
            "CREATE TABLE _partial_strong (hash_val TEXT, cnt INTEGER)"
        )
        await wdb.execute(
            "CREATE TABLE _partial_fast (hash_val TEXT, cnt INTEGER)"
        )

    # Discovery: batched partial GROUP BY
    if on_progress:
        await on_progress("discovery", 0, num_batches)

    for batch_idx, (start, end) in enumerate(ranges):
        async with hashes_writer() as wdb:
            await wdb.execute(
                "INSERT INTO _partial_strong "
                "SELECT hash_strong, COUNT(*) FROM file_hashes "
                "WHERE excluded = 0 AND stale = 0 "
                "AND hash_strong IS NOT NULL AND hash_strong != '' "
                "AND file_id >= ? AND file_id < ? "
                "GROUP BY hash_strong",
                (start, end),
            )
            await wdb.execute(
                "INSERT INTO _partial_fast "
                "SELECT hash_fast, COUNT(*) FROM file_hashes "
                "WHERE excluded = 0 AND stale = 0 "
                "AND hash_fast IS NOT NULL AND hash_fast != '' "
                "AND hash_strong IS NULL "
                "AND file_id >= ? AND file_id < ? "
                "GROUP BY hash_fast",
                (start, end),
            )
        if on_progress:
            await on_progress("discovery", batch_idx + 1, num_batches)
        await asyncio.sleep(0)

    # Aggregate partial counts into final dup tables
    async with hashes_writer() as wdb:
        await wdb.execute(
            "CREATE TABLE _dup_strong "
            "(hash_val TEXT PRIMARY KEY, dc INTEGER)"
        )
        await wdb.execute(
            "INSERT INTO _dup_strong "
            "SELECT hash_val, SUM(cnt) - 1 FROM _partial_strong "
            "GROUP BY hash_val HAVING SUM(cnt) > 1"
        )
        await wdb.execute(
            "CREATE TABLE _dup_fast "
            "(hash_val TEXT PRIMARY KEY, dc INTEGER)"
        )
        await wdb.execute(
            "INSERT INTO _dup_fast "
            "SELECT hash_val, SUM(cnt) - 1 FROM _partial_fast "
            "GROUP BY hash_val HAVING SUM(cnt) > 1"
        )
        await wdb.execute("DROP TABLE _partial_strong")
        await wdb.execute("DROP TABLE _partial_fast")

        cur_s = await wdb.execute("SELECT COUNT(*) as c FROM _dup_strong")
        strong_count = (await cur_s.fetchone())["c"]
        cur_f = await wdb.execute("SELECT COUNT(*) as c FROM _dup_fast")
        fast_count = (await cur_f.fetchone())["c"]

    total_groups = strong_count + fast_count
    log.info(
        "optimized_dup_recount: %d dup groups "
        "(hash_strong=%d, hash_fast=%d)",
        total_groups,
        strong_count,
        fast_count,
    )

    # Reset all dup_counts to 0, batched for progress
    log.info("optimized_dup_recount: resetting dup_counts")
    if on_progress:
        await on_progress("reset", 0, num_batches * 2)

    for batch_idx, (start, end) in enumerate(ranges):
        async with hashes_writer() as wdb:
            await wdb.execute(
                "UPDATE file_hashes SET dup_count = 0 "
                "WHERE file_id >= ? AND file_id < ? AND dup_count != 0",
                (start, end),
            )
        async with db_writer() as cdb:
            await cdb.execute(
                "UPDATE files SET dup_count = 0 "
                "WHERE id >= ? AND id < ? AND dup_count != 0",
                (start, end),
            )
        if on_progress:
            await on_progress("reset", (batch_idx + 1) * 2, num_batches * 2)
        await asyncio.sleep(0)

    # Batched bulk UPDATE on file_hashes
    if on_progress:
        await on_progress("write", 0, total_rows)

    rows_updated = 0
    for batch_idx, (start, end) in enumerate(ranges):
        async with hashes_writer() as wdb:
            if strong_count > 0:
                cur_s = await wdb.execute(
                    "UPDATE file_hashes SET dup_count = ("
                    "  SELECT dc FROM _dup_strong "
                    "  WHERE hash_val = file_hashes.hash_strong"
                    ") WHERE file_id >= ? AND file_id < ? "
                    "AND hash_strong IN "
                    "(SELECT hash_val FROM _dup_strong)",
                    (start, end),
                )
                rows_updated += cur_s.rowcount
            cur = await wdb.execute(
                "UPDATE file_hashes SET dup_count = ("
                "  SELECT dc FROM _dup_fast "
                "  WHERE hash_val = file_hashes.hash_fast"
                ") WHERE file_id >= ? AND file_id < ? "
                "AND hash_fast IN "
                "(SELECT hash_val FROM _dup_fast) "
                "AND hash_strong IS NULL",
                (start, end),
            )
            rows_updated += cur.rowcount

        if on_progress:
            await on_progress(
                "write",
                min((batch_idx + 1) * RECOUNT_BATCH_ROWS, total_rows),
                total_rows,
            )
        if (batch_idx + 1) % 3 == 0 or batch_idx == num_batches - 1:
            log.info(
                "optimized_dup_recount: write %d/%d batches",
                batch_idx + 1,
                num_batches,
            )
        await asyncio.sleep(0)

    # Cleanup dup tables
    async with hashes_writer() as wdb:
        await wdb.execute("DROP TABLE IF EXISTS _dup_strong")
        await wdb.execute("DROP TABLE IF EXISTS _dup_fast")

    # Catalog sync: read dup_counts from hashes.db, batch write to catalog
    if on_progress:
        await on_progress("catalog", 0, rows_updated)

    synced = 0
    for batch_idx, (start, end) in enumerate(ranges):
        async with read_hashes() as rdb:
            rows = await rdb.execute_fetchall(
                "SELECT file_id, dup_count FROM file_hashes "
                "WHERE file_id >= ? AND file_id < ? AND dup_count > 0",
                (start, end),
            )
        if rows:
            async with db_writer() as cdb:
                await cdb.executemany(
                    "UPDATE files SET dup_count = ? WHERE id = ?",
                    [(r["dup_count"], r["file_id"]) for r in rows],
                )
            synced += len(rows)
        if on_progress:
            await on_progress("catalog", synced, rows_updated)
        await asyncio.sleep(0)

    log.info(
        "optimized_dup_recount: complete — %d groups, %d rows, %d catalog",
        total_groups,
        rows_updated,
        synced,
    )
    return total_groups


def submit_hashes_for_recalc(
    *,
    strong_hashes: set[str] | None = None,
    fast_hashes: set[str] | None = None,
    source: str = "",
    location_ids: set[int] | None = None,
):
    """Submit hashes to the coalesced dup recalc writer (non-blocking).

    Enqueues hashes for the single background writer task. Multiple rapid
    submissions are coalesced into larger batches. Safe to call from any
    async context: scan loops, route handlers, backfill tasks.

    Args:
        strong_hashes: hash_strong values to recalculate. Files grouped by
            hash_strong.
        fast_hashes: hash_fast values to recalculate. Only updates files
            without hash_strong.
        source: Label for logging (e.g. "hash drainer Backups").
        location_ids: Location IDs affected by these hashes. Used to
            broadcast dup_recalc_started immediately and to scope the
            post-recalc location stats update.

    Returns:
        None. Work is enqueued, not awaited.

    Side effects:
        - Enqueues work to _recalc_queue.
        - Adds location_ids to _active_recalc_locations.
        - Broadcasts "dup_recalc_started" WebSocket message immediately.
        - Starts _dup_recalc_writer task if not already running.

    Callers: helpers.py (hash ingest), hash_candidates_for_location() (small
    files), drain_pending_hashes() (after agent hashing), hash_backfill.py.
    """
    global _recalc_queue, _writer_task
    strong = {h for h in (strong_hashes or set()) if h}
    fast = {h for h in (fast_hashes or set()) if h}
    if not strong and not fast:
        return
    lids = location_ids or set()
    if _recalc_queue is None:
        _recalc_queue = asyncio.Queue()
    _recalc_queue.put_nowait((strong, fast, source, lids))

    # Mark locations as recalculating immediately (not when writer picks up)
    if lids:
        _active_recalc_locations.update(lids)
        asyncio.get_running_loop().create_task(
            broadcast(
                {
                    "type": "dup_recalc_started",
                    "locationIds": list(lids),
                }
            )
        )

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
    All writes go through db_writer(). Stats rebuild happens once when the
    queue drains, not per batch.
    """
    all_affected: set[int] = set()
    total_hashes_processed = 0

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

            # Collect affected locations for stats rebuild at the end
            all_affected.update(merged_location_ids)

            async with read_hashes() as hdb:
                for hash_set, col in (
                    (merged_strong, "hash_strong"),
                    (merged_fast, "hash_fast"),
                ):
                    if not hash_set:
                        continue
                    h_list = list(hash_set)
                    for i in range(0, len(h_list), SQL_VAR_LIMIT):
                        batch = h_list[i : i + SQL_VAR_LIMIT]
                        ph = ",".join("?" for _ in batch)
                        rows = await hdb.execute_fetchall(
                            f"SELECT DISTINCT location_id FROM active_hashes "
                            f"WHERE {col} IN ({ph})",
                            batch,
                        )
                        all_affected |= {r["location_id"] for r in rows}

            total_hashes_processed += len(merged_strong) + len(merged_fast)

        # Queue drained — rebuild stats once for all affected locations
        if all_affected:
            log.info(
                "Dup recalc complete: %d hashes, rebuilding stats for %d locations",
                total_hashes_processed,
                len(all_affected),
            )
            await update_location_dup_counts(all_affected)

        _active_recalc_locations.difference_update(all_affected)
        await broadcast(
            {
                "type": "dup_recalc_completed",
                "hashCount": total_hashes_processed,
                "locationIds": list(all_affected),
            }
        )
    except Exception:
        log.error("Coalesced dup recalc writer failed", exc_info=True)


async def find_dup_candidates(
    location_id: int | None = None,
    file_ids: list[int] | None = None,
    on_progress=None,
) -> list[dict]:
    """Find files needing hash_fast that are in duplicate (hash_partial, file_size) groups.

    Reads from hashes.db to find dup groups, then fetches file info
    (full_path, inode) from catalog for the candidate files.

    file_ids: when set, only looks at dup groups involving these specific
    files. Used by rescan/quick scan to scope to new/changed files.
    location_id: when set (and file_ids is None), scans dup groups for the
    entire location. Used by first scan and housekeeping.
    Neither: all dup groups globally (for repair).

    Returns [{id, full_path, location_id, file_size, hash_partial, inode}, ...]
    """
    if file_ids is not None:
        scope = f"{len(file_ids)} files"
    elif location_id is not None:
        scope = f"location {location_id}"
    else:
        scope = "global"

    log.info("find_dup_candidates: starting (scope=%s) — querying hashes.db", scope)
    t0 = time.monotonic()

    # Step 1-2: find dup groups in hashes.db using GROUP BY + HAVING (no Python counting)
    conn = await open_hashes_connection()
    try:
        t1 = time.monotonic()

        # Build _dup_groups directly — only pairs with count > 1
        await conn.execute(
            "CREATE TEMP TABLE _dup_groups (hash_partial TEXT, file_size INTEGER)"
        )

        if file_ids is not None:
            # Scoped to specific files — find their (hash_partial, file_size) pairs,
            # then keep only pairs that appear more than once globally
            await conn.execute("CREATE TEMP TABLE _seed_ids (file_id INTEGER)")
            await conn.executemany(
                "INSERT INTO _seed_ids VALUES (?)",
                [(fid,) for fid in file_ids],
            )
            await conn.execute(
                "CREATE TEMP TABLE _dup_pairs (hash_partial TEXT, file_size INTEGER)"
            )
            await conn.execute(
                "INSERT INTO _dup_pairs "
                "SELECT DISTINCT f.hash_partial, f.file_size FROM _seed_ids s "
                "CROSS JOIN file_hashes f "
                "WHERE f.file_id = s.file_id "
                "AND f.excluded = 0 AND f.stale = 0 "
                "AND f.hash_partial IS NOT NULL AND f.file_size > 0"
            )
            await conn.execute(
                "CREATE INDEX _dup_pairs_idx ON _dup_pairs(hash_partial, file_size)"
            )
            await conn.commit()

            await conn.execute(
                "INSERT INTO _dup_groups "
                "SELECT p.hash_partial, p.file_size FROM _dup_pairs p "
                "WHERE ("
                "  SELECT COUNT(*) FROM file_hashes f "
                "  WHERE f.excluded = 0 AND f.stale = 0 "
                "  AND f.hash_partial = p.hash_partial "
                "  AND f.file_size = p.file_size"
                ") > 1"
            )
            await conn.execute("DROP TABLE IF EXISTS _seed_ids")
            await conn.execute("DROP TABLE IF EXISTS _dup_pairs")

        elif location_id is not None:
            await conn.execute(
                "CREATE TEMP TABLE _dup_pairs (hash_partial TEXT, file_size INTEGER)"
            )
            await conn.execute(
                "INSERT INTO _dup_pairs "
                "SELECT DISTINCT hash_partial, file_size FROM file_hashes "
                "WHERE excluded = 0 AND stale = 0 "
                "AND location_id = ? AND hash_partial IS NOT NULL "
                "AND file_size > 0",
                (location_id,),
            )
            await conn.execute(
                "CREATE INDEX _dup_pairs_idx ON _dup_pairs(hash_partial, file_size)"
            )
            await conn.commit()

            await conn.execute(
                "INSERT INTO _dup_groups "
                "SELECT p.hash_partial, p.file_size FROM _dup_pairs p "
                "WHERE ("
                "  SELECT COUNT(*) FROM file_hashes f "
                "  WHERE f.excluded = 0 AND f.stale = 0 "
                "  AND f.hash_partial = p.hash_partial "
                "  AND f.file_size = p.file_size"
                ") > 1"
            )
            await conn.execute("DROP TABLE IF EXISTS _dup_pairs")

        else:
            await conn.execute(
                "INSERT INTO _dup_groups "
                "SELECT hash_partial, file_size FROM active_hashes "
                "WHERE hash_partial IS NOT NULL AND file_size > 0 "
                "GROUP BY hash_partial, file_size HAVING COUNT(*) > 1"
            )

        await conn.execute(
            "CREATE INDEX _dup_groups_idx ON _dup_groups(hash_partial, file_size)"
        )
        await conn.commit()

        dup_count_row = await conn.execute_fetchall(
            "SELECT COUNT(*) as c FROM _dup_groups"
        )
        dup_group_count = dup_count_row[0]["c"] if dup_count_row else 0

        log.info(
            "find_dup_candidates: %d dup groups found in %.1fs",
            dup_group_count,
            time.monotonic() - t1,
        )

        if dup_group_count == 0:
            log.info(
                "find_dup_candidates: no dup groups, done in %.1fs",
                time.monotonic() - t0,
            )
            await conn.execute("DROP TABLE IF EXISTS _dup_groups")
            return []

        # Step 3: find candidate file_ids (hash_fast IS NULL) in hashes.db,
        # batched by dup_group rowid for progress reporting
        t4 = time.monotonic()
        DUP_CANDIDATE_BATCH = 100_000
        candidate_rows = []

        if on_progress:
            await on_progress(0, dup_group_count)

        for batch_start in range(1, dup_group_count + 1, DUP_CANDIDATE_BATCH):
            batch_end = min(batch_start + DUP_CANDIDATE_BATCH - 1, dup_group_count)
            batch_rows = await conn.execute_fetchall(
                "SELECT f.file_id, f.location_id, f.file_size, f.hash_partial "
                "FROM _dup_groups g "
                "CROSS JOIN file_hashes f "
                "WHERE g.rowid BETWEEN ? AND ? "
                "AND f.excluded = 0 AND f.stale = 0 "
                "AND f.hash_partial = g.hash_partial "
                "AND f.file_size = g.file_size "
                "AND f.hash_fast IS NULL",
                (batch_start, batch_end),
            )
            candidate_rows.extend(batch_rows)
            if on_progress:
                await on_progress(batch_end, dup_group_count)
            await asyncio.sleep(0)

        await conn.execute("DROP TABLE IF EXISTS _dup_groups")

        log.info(
            "find_dup_candidates: %d candidate files from hashes.db in %.1fs",
            len(candidate_rows),
            time.monotonic() - t4,
        )
    finally:
        try:
            await conn.execute("DROP TABLE IF EXISTS _seed_ids")
            await conn.execute("DROP TABLE IF EXISTS _dup_pairs")
            await conn.execute("DROP TABLE IF EXISTS _dup_groups")
        except Exception:
            pass
        await conn.close()

    if not candidate_rows:
        return []

    # Step 4: fetch full_path and inode from catalog for candidate files
    file_ids = [r["file_id"] for r in candidate_rows]
    file_info: dict[int, dict] = {}

    cat_conn = await open_connection()
    try:
        for i in range(0, len(file_ids), 500):
            batch = file_ids[i : i + 500]
            ph = ",".join("?" for _ in batch)
            rows = await cat_conn.execute_fetchall(
                f"SELECT id, full_path, inode FROM files WHERE id IN ({ph}) AND stale = 0",
                batch,
            )
            for r in rows:
                file_info[r["id"]] = {"full_path": r["full_path"], "inode": r["inode"]}
    finally:
        await cat_conn.close()

    # Merge: hash data from hashes.db + file data from catalog
    candidates = []
    for r in candidate_rows:
        fid = r["file_id"]
        fi = file_info.get(fid)
        if fi:
            candidates.append(
                {
                    "id": fid,
                    "full_path": fi["full_path"],
                    "location_id": r["location_id"],
                    "file_size": r["file_size"],
                    "hash_partial": r["hash_partial"],
                    "inode": fi["inode"],
                }
            )

    log.info(
        "find_dup_candidates: %d candidates total in %.1fs (scope=%s)",
        len(candidates),
        time.monotonic() - t0,
        scope,
    )
    return candidates


async def update_location_dup_counts(location_ids: set[int]):
    """Recount and write locations.duplicate_count for the given locations.

    Reads dup counts from hashes.db, writes to stats.db.
    """
    if not location_ids:
        return

    conn = await open_hashes_connection()
    try:
        loc_updates = []
        for lid in location_ids:
            rows = await conn.execute_fetchall(
                "SELECT COUNT(*) as c FROM active_hashes "
                "WHERE location_id = ? AND dup_count > 0",
                (lid,),
            )
            loc_updates.append((rows[0]["c"], lid))
    finally:
        await conn.close()

    async with stats_writer() as sdb:
        for dc, lid in loc_updates:
            await sdb.execute(
                "UPDATE location_stats SET duplicate_count = ? WHERE location_id = ?",
                (dc, lid),
            )


async def batch_dup_counts(
    strong_hashes: list[str] | None = None,
    fast_hashes: list[str] | None = None,
) -> dict[str, int]:
    """Return live dup counts for a batch of hashes from hashes.db.

    strong_hashes: GROUP BY hash_strong for verified files.
    fast_hashes: GROUP BY hash_fast for unverified files.

    Returns {hash: count} where count = total files - 1.
    Only includes hashes with count > 0.  Designed for page-sized batches
    (~120 items) so always fast.

    Hashes DB only contains active, non-excluded files so no filtering needed.
    """
    result: dict[str, int] = {}

    unique_strong = {h for h in (strong_hashes or []) if h}
    unique_fast = {h for h in (fast_hashes or []) if h}

    if not unique_strong and not unique_fast:
        return result

    async with read_hashes() as hdb:
        if unique_strong:
            ph = ",".join("?" for _ in unique_strong)
            rows = await hdb.execute_fetchall(
                f"""SELECT hash_strong, COUNT(*) as cnt FROM active_hashes
                    WHERE hash_strong IN ({ph})
                    GROUP BY hash_strong HAVING COUNT(*) > 1""",
                list(unique_strong),
            )
            for r in rows:
                result[r["hash_strong"]] = r["cnt"] - 1

        if unique_fast:
            ph = ",".join("?" for _ in unique_fast)
            rows = await hdb.execute_fetchall(
                f"""SELECT hash_fast, COUNT(*) as cnt FROM active_hashes
                    WHERE hash_fast IN ({ph})
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

    activity_name = f"dup-recalc-{id(strong)}"
    label = f"Dup recalc ({source})" if source else "Dup recalc"
    _act_reg(activity_name, label, f"0/{total}")

    progress_offset = 0

    async def _on_progress(processed, _batch_total, dups_confirmed):
        actual = progress_offset + processed
        _act_upd(activity_name, progress=f"{actual}/{total}")

    try:
        if strong:
            await update_dup_counts_inline(
                strong, hash_column="hash_strong", on_progress=_on_progress
            )
            progress_offset = len(strong)

        if fast:
            await update_dup_counts_inline(
                fast, hash_column="hash_fast", on_progress=_on_progress
            )
    finally:
        _act_unreg(activity_name)


async def backfill_dup_counts():
    """Backfill dup_count for all entries in hashes.db on startup.

    Reads and writes within hashes.db — no catalog contention.
    Skips if no entries have stale dup_counts (quick consistency check).
    """
    try:
        async with read_hashes() as hdb:
            # Quick check: any entry with dup_count=0 that actually has duplicates?
            stale_strong_check = await hdb.execute_fetchall(
                """SELECT 1 FROM active_hashes f
                   WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                     AND f.dup_count = 0
                     AND EXISTS (
                         SELECT 1 FROM active_hashes f2
                         WHERE f2.hash_strong = f.hash_strong
                           AND f2.file_id != f.file_id
                     )
                   LIMIT 1"""
            )
            stale_fast_check = await hdb.execute_fetchall(
                """SELECT 1 FROM active_hashes f
                   WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                     AND f.hash_strong IS NULL
                     AND f.dup_count = 0
                     AND EXISTS (
                         SELECT 1 FROM active_hashes f2
                         WHERE f2.hash_fast = f.hash_fast
                           AND f2.file_id != f.file_id
                     )
                   LIMIT 1"""
            )

            if not stale_strong_check and not stale_fast_check:
                log.info("dup_count backfill: counts consistent, skipping")
                await broadcast({"type": "dup_backfill_completed", "skipped": True})
                return

            strong_hashes: set[str] = set()
            if stale_strong_check:
                rows = await hdb.execute_fetchall(
                    """SELECT DISTINCT f.hash_strong
                       FROM active_hashes f
                       WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                         AND f.dup_count = 0
                         AND EXISTS (
                             SELECT 1 FROM active_hashes f2
                             WHERE f2.hash_strong = f.hash_strong
                               AND f2.file_id != f.file_id
                         )"""
                )
                strong_hashes = {r["hash_strong"] for r in rows}

                fp_rows = await hdb.execute_fetchall(
                    """SELECT DISTINCT f.hash_strong
                       FROM active_hashes f
                       WHERE f.hash_strong IS NOT NULL AND f.hash_strong != ''
                         AND f.dup_count > 0
                         AND NOT EXISTS (
                             SELECT 1 FROM active_hashes f2
                             WHERE f2.hash_strong = f.hash_strong
                               AND f2.file_id != f.file_id
                         )"""
                )
                strong_hashes |= {r["hash_strong"] for r in fp_rows}

            fast_hashes: set[str] = set()
            if stale_fast_check:
                rows = await hdb.execute_fetchall(
                    """SELECT DISTINCT f.hash_fast
                       FROM active_hashes f
                       WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                         AND f.hash_strong IS NULL
                         AND f.dup_count = 0
                         AND EXISTS (
                             SELECT 1 FROM active_hashes f2
                             WHERE f2.hash_fast = f.hash_fast
                               AND f2.file_id != f.file_id
                         )"""
                )
                fast_hashes = {r["hash_fast"] for r in rows}

                fp_rows = await hdb.execute_fetchall(
                    """SELECT DISTINCT f.hash_fast
                       FROM active_hashes f
                       WHERE f.hash_fast IS NOT NULL AND f.hash_fast != ''
                         AND f.hash_strong IS NULL
                         AND f.dup_count > 0
                         AND NOT EXISTS (
                             SELECT 1 FROM active_hashes f2
                             WHERE f2.hash_fast = f.hash_fast
                               AND f2.file_id != f.file_id
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

        async def _on_progress(processed, _batch_total, _dups_confirmed):
            nonlocal progress_offset
            actual = progress_offset + processed
            if actual % 10000 < SQL_VAR_LIMIT:
                log.info("dup_count backfill: %d / %d hashes", actual, total_hashes)
                await broadcast(
                    {
                        "type": "dup_backfill_progress",
                        "processed": actual,
                        "totalHashes": total_hashes,
                    }
                )

        if strong_hashes:
            await update_dup_counts_inline(
                strong_hashes, hash_column="hash_strong", on_progress=_on_progress
            )
            progress_offset = len(strong_hashes)

        if fast_hashes:
            await update_dup_counts_inline(
                fast_hashes, hash_column="hash_fast", on_progress=_on_progress
            )

        await post_op_stats()

        log.info(
            "dup_count backfill: complete, processed %d hashes",
            total_hashes,
        )
        await broadcast({"type": "dup_backfill_completed", "updated": total_hashes})

    except Exception:
        log.exception("dup_count backfill failed")


SMALL_FILE_THRESHOLD = 128 * 1024  # 128KB — hash_partial == hash_fast, no agent needed
HASH_BATCH_BYTES = 500 * 1024 * 1024  # 500MB — max total bytes per batch request
MAX_RETRIES = 3
RETRY_DELAY = 5


async def hash_candidates_for_location(
    location_id: int,
    agent_id: int,
    location_name: str = "",
    file_ids: list[int] | None = None,
    activity_name: str | None = None,
) -> tuple[int, int, int]:
    """Find dup candidates for a location, handle small files, queue large.

    Finds files needing hash_fast that share (size, hash_partial) with other
    files.  Small files (<=128KB): copy hash_partial to hash_fast and submit
    to coalesced dup writer.  Large files: insert into pending_hashes for
    the hash drainer to process.

    file_ids: when set, scopes candidate search to dup groups involving
    these specific files (rescan/quick scan). None = full location scan
    (first scan, housekeeping).

    Returns (total_candidates, small_handled, large_queued).
    """
    candidates = await find_dup_candidates(location_id=location_id, file_ids=file_ids)

    log.info(
        "hash_candidates_for_location: %d raw candidates for location %d",
        len(candidates),
        location_id,
    )

    # Broadcast candidate count immediately so UI updates
    if candidates:
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "checking_duplicates",
                "candidates": len(candidates),
            }
        )
        if activity_name:
            _act_upd(activity_name, progress=f"{len(candidates):,} candidates found")

    # Filter to files on this agent's locations only
    async with read_db() as db:
        agent_loc_rows = await db.execute_fetchall(
            "SELECT id FROM locations WHERE agent_id = ?",
            (agent_id,),
        )
    agent_loc_ids = {r["id"] for r in agent_loc_rows}
    candidates = [c for c in candidates if c["location_id"] in agent_loc_ids]

    log.info(
        "hash_candidates_for_location: %d candidates on agent %d for location %d",
        len(candidates),
        agent_id,
        location_id,
    )

    total = len(candidates)
    if total == 0:
        return 0, 0, 0

    small_files = [c for c in candidates if c["file_size"] <= SMALL_FILE_THRESHOLD]
    large_files = [c for c in candidates if c["file_size"] > SMALL_FILE_THRESHOLD]

    # Small files: hash_partial == hash_fast, bulk copy in hashes.db + submit to coalesced writer
    small_fast_hashes: set[str] = set()
    if small_files:
        async with hashes_writer() as wdb:
            for c in small_files:
                await wdb.execute(
                    "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
                    (c["hash_partial"], c["id"]),
                )
                small_fast_hashes.add(c["hash_partial"])
        log.info(
            "hash_candidates_for_location: %d small files hash_fast copied from hash_partial",
            len(small_files),
        )

        # Progress callback for status bar + indicator
        async def _dup_progress(processed, batch_total, dups_confirmed):
            pct = (
                f" ({round(processed / batch_total * 100)}%)" if batch_total > 0 else ""
            )
            if activity_name:
                _act_upd(
                    activity_name,
                    progress=f"confirming duplicates: {processed:,} / {batch_total:,}{pct}",
                )
            await broadcast(
                {
                    "type": "scan_progress",
                    "locationId": location_id,
                    "location": location_name,
                    "phase": "confirming_duplicates",
                    "dupsProcessed": processed,
                    "dupsTotal": batch_total,
                    "dupsConfirmed": dups_confirmed,
                }
            )

        # Inline dup count update for small files
        await update_dup_counts_inline(small_fast_hashes, on_progress=_dup_progress)

    # Large files: insert into pending_hashes for the drainer
    if large_files:
        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        batch = []
        for c in large_files:
            batch.append(
                (
                    c["id"],
                    c["location_id"],
                    agent_id,
                    c["full_path"],
                    c.get("inode", 0),
                    now_iso,
                )
            )
            if len(batch) >= 5000:
                async with db_writer() as wdb:
                    await wdb.executemany(
                        "INSERT INTO pending_hashes "
                        "(file_id, location_id, agent_id, full_path, inode, created_at) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        batch,
                    )
                batch.clear()
        if batch:
            async with db_writer() as wdb:
                await wdb.executemany(
                    "INSERT INTO pending_hashes "
                    "(file_id, location_id, agent_id, full_path, inode, created_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    batch,
                )
        log.info(
            "hash_candidates_for_location: %d large files queued in pending_hashes",
            len(large_files),
        )

    log.info(
        "hash_candidates_for_location: %d total (%d small handled, %d large queued) "
        "for location %d",
        total,
        len(small_files),
        len(large_files),
        location_id,
    )

    return total, len(small_files), len(large_files)


async def write_hash_partials(
    result: dict,
    location_id: int,
    root_path: str,
):
    """Write hash_partial results from agent to hashes.db.

    Resolves file_ids from catalog by rel_path, then updates hash_partial
    in hashes.db. Shared by rescan, quick scan, and recovery paths.
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


async def recover_missing_hash_partials(
    location_id: int,
    agent_id: int,
    root_path: str,
    location_name: str,
    *,
    folder_id: int | None = None,
    scan_prefix: str | None = None,
) -> list[int]:
    """Find files in the catalog with no hash_partial and hash them via agent.

    Files may exist in the catalog but have no hash_partial if a previous scan
    was interrupted. This ensures a user-initiated scan leaves things correct
    regardless of prior history.

    Scoping: folder_id (quick scan), scan_prefix (subfolder rescan), or
    neither (full location).

    Returns list of recovered file IDs (for inclusion in affected_file_ids).
    """
    scope_filter = ""
    scope_params: list = [location_id]
    if folder_id:
        scope_filter = " AND f.folder_id = ?"
        scope_params.append(folder_id)
    elif scan_prefix:
        scope_filter = " AND f.rel_path LIKE ?"
        scope_params.append(scan_prefix + "%")

    async with read_db() as rdb:
        catalog_rows = await rdb.execute_fetchall(
            "SELECT f.id, f.full_path, f.file_size, f.inode "
            "FROM files f "
            "WHERE f.location_id = ? AND f.stale = 0 "
            f"AND f.file_size > 0{scope_filter}",
            scope_params,
        )

    if not catalog_rows:
        return []

    catalog_ids = [r["id"] for r in catalog_rows]

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
    if not missing:
        return []

    missing.sort(key=lambda r: r["inode"] or 0)

    log.info(
        "Scan recovery: %d files missing hash_partial for %s — sending to agent",
        len(missing),
        location_name,
    )

    # Ensure file_hashes rows exist
    async with hashes_writer() as hdb:
        await hdb.executemany(
            "INSERT INTO file_hashes "
            "(file_id, location_id, file_size) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(file_id) DO NOTHING",
            [(r["id"], location_id, r["file_size"]) for r in missing],
        )

    recovery_ids = [r["id"] for r in missing]
    batch_paths: list[str] = []
    batch_bytes = 0

    for r in missing:
        batch_paths.append(r["full_path"])
        batch_bytes += r["file_size"]

        if batch_bytes >= HASH_BATCH_BYTES:
            result = await hash_partial_batch(agent_id, batch_paths)
            await write_hash_partials(result, location_id, root_path)
            batch_paths = []
            batch_bytes = 0

    if batch_paths:
        result = await hash_partial_batch(agent_id, batch_paths)
        await write_hash_partials(result, location_id, root_path)

    log.info(
        "Scan recovery: %d hash partials recovered for %s",
        len(recovery_ids),
        location_name,
    )

    return recovery_ids


async def recover_unprocessed_dup_candidates(
    location_id: int,
    agent_id: int,
    location_name: str,
    *,
    exclude_file_ids: list[int] | None = None,
) -> int:
    """Find files with hash_partial but no hash_fast and run dup processing.

    Catches files left unprocessed by interrupted scans. Returns the number
    of candidates found.
    """
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

    already_processed = set(exclude_file_ids) if exclude_file_ids else set()
    unprocessed_ids = [
        r["file_id"] for r in unprocessed if r["file_id"] not in already_processed
    ]

    if not unprocessed_ids:
        return 0

    log.info(
        "Scan recovery: %d unprocessed files (hash_partial, no hash_fast) "
        "for %s — running dup processing",
        len(unprocessed_ids),
        location_name,
    )

    await post_ingest_dup_processing(
        location_id,
        agent_id,
        location_name,
        file_ids=unprocessed_ids,
    )

    return len(unprocessed_ids)


async def post_ingest_dup_processing(
    location_id: int,
    agent_id: int,
    location_name: str,
    on_progress=None,
    broadcast_scan_progress: bool = True,
    file_ids: list[int] | None = None,
    activity_name: str | None = None,
):
    """Shared post-ingest dup processing for scan, rescan, quick scan, and import.

    1. Find dup candidates (files sharing hash_partial that need hash_fast)
    2. Handle small files (copy hash_partial → hash_fast)
    3. Queue large files for hash drainer
    4. Recount dup_count for affected hashes

    Called after files + hashes are in the catalog and hashes.db.
    file_ids: when set, scopes candidate search to dup groups involving
    these specific files (rescan/quick scan). None = full location scan
    (first scan, housekeeping).
    broadcast_scan_progress: False when called from housekeeping (not a scan).
    activity_name: server-side activity key to update with phase progress.
    """
    log.info("Post-ingest dup processing for %s", location_name)

    if activity_name:
        _act_upd(activity_name, progress="checking duplicates")

    if broadcast_scan_progress:
        await broadcast(
            {
                "type": "scan_progress",
                "locationId": location_id,
                "location": location_name,
                "phase": "checking_duplicates",
            }
        )

    candidates_total, small_handled, large_queued = await hash_candidates_for_location(
        location_id=location_id,
        agent_id=agent_id,
        location_name=location_name,
        file_ids=file_ids,
        activity_name=activity_name,
    )

    log.info(
        "Dup candidates: %d total (%d small handled, %d large queued) for %s",
        candidates_total,
        small_handled,
        large_queued,
        location_name,
    )

    if on_progress:
        await on_progress(candidates_total, small_handled, large_queued)

    return candidates_total


async def run_hash_file(op_id: int, agent_id: int, params: dict):
    """Queue operation handler: hash a single file via agent dispatch.

    Called by queue_manager for the 'hash_file' operation type.
    Dispatches file_hash to the agent, writes hash_fast, submits to dup recalc.
    """
    file_id = params["file_id"]
    location_id = params["location_id"]
    full_path = params["full_path"]

    result = await dispatch("file_hash", location_id, path=full_path)
    hash_fast = result["hash_fast"]

    async with hashes_writer() as wdb:
        await wdb.execute(
            "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
            (hash_fast, file_id),
        )

    await update_dup_counts_inline({hash_fast})


async def drain_pending_hashes(
    agent_id: int,
    location_id: int,
    location_name: str,
    *,
    scan_done: asyncio.Event | None = None,
    on_progress: Callable[[int, int], Awaitable[None]] | None = None,
):
    """Drain pending_hashes by sending batches to the agent for hash_fast.

    Shared by scan (concurrent with ingest) and import (sequential after ingest).

    scan_done: if provided, polls until event is set AND table is empty (scan mode).
               if None, drains to empty and returns (import mode).

    on_progress: async callback(done, total) for UI updates. Scan broadcasts
                 via WebSocket, import updates the _progress dict.

    Scoped to agent_id — processes all pending_hashes accessible to this
    agent, including cross-location candidates from dup detection. When a
    scan finds duplicates spanning multiple locations, all candidates must
    be hashed so the user sees correct dup counts everywhere, not just
    on the scanned location.
    """
    FETCH_LIMIT = 2000
    total_hashed = 0
    total_pending = 0
    all_affected_locs: set[int] = set()
    drainer_act = f"hash-drainer-{location_id}"

    where_clause = "WHERE agent_id = ?"
    where_params: tuple = (agent_id,)

    async def _process_batch(batch_rows):
        nonlocal total_hashed
        paths = [r["full_path"] for r in batch_rows]
        path_to_file_id = {r["full_path"]: r["file_id"] for r in batch_rows}
        pending_ids = [r["id"] for r in batch_rows]

        try:
            result = await hash_fast_batch(agent_id, paths)
        except (ConnectionError, OSError, httpx.ConnectError):
            log.warning("Hash drainer: agent %d offline, stopping", agent_id)
            raise

        hash_results = result.get("results", [])
        affected_fast: set[str] = set()

        if hash_results:
            async with hashes_writer() as hdb:
                for hr in hash_results:
                    fid = path_to_file_id.get(hr["path"])
                    hf = hr.get("hash_fast")
                    if fid and hf:
                        await hdb.execute(
                            "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
                            (hf, fid),
                        )
                        affected_fast.add(hf)

        # Remove processed entries
        for i in range(0, len(pending_ids), SQL_VAR_LIMIT):
            batch = pending_ids[i : i + SQL_VAR_LIMIT]
            ph = ",".join("?" for _ in batch)
            async with db_writer() as db:
                await db.execute(
                    f"DELETE FROM pending_hashes WHERE id IN ({ph})",
                    batch,
                )

        total_hashed += len(hash_results)

        if affected_fast:
            batch_affected_locs = await update_dup_counts_inline(affected_fast)
            all_affected_locs.update(batch_affected_locs)

        log.info(
            "Hash drainer: %d hashed (%d / %d) for %s",
            len(hash_results),
            total_hashed,
            total_pending,
            location_name,
        )
        _act_upd(drainer_act, progress=f"{total_hashed}/{total_pending}")

        if on_progress:
            await on_progress(total_hashed, total_pending)

    try:
        _act_reg(drainer_act, f"Hashing: {location_name}")

        while True:
            async with read_db() as rdb:
                rows = await rdb.execute_fetchall(
                    "SELECT id, file_id, full_path, inode, location_id "
                    f"FROM pending_hashes {where_clause} "
                    "ORDER BY inode LIMIT ?",
                    (*where_params, FETCH_LIMIT),
                )
                if total_pending == 0 or len(rows) == FETCH_LIMIT:
                    count_row = await rdb.execute_fetchall(
                        f"SELECT COUNT(*) as c FROM pending_hashes {where_clause}",
                        where_params,
                    )
                    total_pending = total_hashed + count_row[0]["c"]

            if not rows:
                if scan_done is None or scan_done.is_set():
                    # Rebuild folder dup counts for all affected locations
                    log.info(
                        "Hash drainer: done for %s (%d locations affected)",
                        location_name,
                        len(all_affected_locs) + 1,
                    )
                    _act_unreg(drainer_act)
                    return
                await asyncio.sleep(2)
                continue

            # Get file sizes for byte-based batching
            file_ids = [r["file_id"] for r in rows]
            size_map: dict[int, int] = {}
            for i in range(0, len(file_ids), SQL_VAR_LIMIT):
                batch_ids = file_ids[i : i + SQL_VAR_LIMIT]
                ph = ",".join("?" for _ in batch_ids)
                async with read_db() as rdb:
                    size_rows = await rdb.execute_fetchall(
                        f"SELECT id, file_size FROM files WHERE id IN ({ph})",
                        batch_ids,
                    )
                for sr in size_rows:
                    size_map[sr["id"]] = sr["file_size"]

            # Build byte-sized batches and process
            batch_rows: list[dict] = []
            batch_bytes = 0

            for r in rows:
                fsize = size_map.get(r["file_id"], 0)
                batch_rows.append(dict(r))
                batch_bytes += fsize

                if batch_bytes >= HASH_BATCH_BYTES:
                    await _process_batch(batch_rows)
                    batch_rows = []
                    batch_bytes = 0

            if batch_rows:
                await _process_batch(batch_rows)

            await asyncio.sleep(0)

    except (ConnectionError, OSError, httpx.ConnectError):
        _act_unreg(drainer_act)
        return
    except asyncio.CancelledError:
        _act_unreg(drainer_act)
        return
