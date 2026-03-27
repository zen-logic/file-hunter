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
import time
from collections import Counter, defaultdict
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
from file_hunter.services.agent_ops import dispatch, hash_fast_batch
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.stats_db import stats_writer
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


async def _batched_recalc(
    hashes, *, hash_column: str = "hash_strong", on_progress=None, batch_size: int = 0
):
    """Recalculate dup_count for file_hashes rows sharing the given hashes.

    Args:
        hashes: Iterable of hash values to recalculate counts for.
        hash_column: "hash_strong" or "hash_fast". When "hash_fast", UPDATE
            only targets rows WHERE hash_strong IS NULL (verified files get
            their dup_count from hash_strong grouping, not hash_fast).
        on_progress: Optional async callback(processed, total) invoked after
            each write batch.
        batch_size: Override RECALC_BATCH for write batching. 0 = use default.

    Returns:
        int: Number of hashes processed.

    Side effects:
        - Writes dup_count to hashes.db via hashes_writer().
        - Writes denormalized dup_count to catalog files table via db_writer().

    Callers: recalculate_dup_counts(), backfill_dup_counts(),
    dup_exclude.py toggle handler.

    Implementation: reads all rows for the given hashes into a Counter on a
    dedicated connection (temp table + JOIN), then groups hashes by target
    dup_count value and writes in batches. Yields between batches.
    """
    hash_list = list(hashes)
    total = len(hash_list)
    if not total:
        return 0

    # For hash_fast updates, only target files without hash_strong
    update_extra = " AND hash_strong IS NULL" if hash_column == "hash_fast" else ""

    # Read phase: temp table + JOIN + Counter — all within hashes.db
    conn = await open_hashes_connection()
    try:
        await conn.execute("CREATE TEMP TABLE _recalc_hashes (hash_val TEXT)")
        await conn.executemany(
            "INSERT INTO _recalc_hashes VALUES (?)",
            [(h,) for h in hash_list],
        )
        await conn.execute(
            "CREATE INDEX _recalc_hashes_idx ON _recalc_hashes(hash_val)"
        )
        await conn.commit()

        rows = await conn.execute_fetchall(
            f"SELECT f.{hash_column} FROM active_hashes f "
            f"INNER JOIN _recalc_hashes h ON f.{hash_column} = h.hash_val"
        )
    finally:
        try:
            await conn.execute("DROP TABLE IF EXISTS _recalc_hashes")
        except Exception:
            pass
        await conn.close()

    hash_counts = Counter(r[hash_column] for r in rows)

    # Build dup_count map: group hashes by their target dup_count value
    by_dup_count: dict[int, list[str]] = defaultdict(list)
    for h in hash_list:
        cnt = hash_counts.get(h, 0)
        dc = cnt - 1 if cnt > 1 else 0
        by_dup_count[dc].append(h)

    # Write phase: batched UPDATEs via hashes_writer()
    processed = 0
    effective_batch = batch_size if batch_size > 0 else RECALC_BATCH
    for dc, dc_hashes in by_dup_count.items():
        for i in range(0, len(dc_hashes), effective_batch):
            batch = dc_hashes[i : i + effective_batch]
            ph = ",".join("?" for _ in batch)
            # Source of truth: hashes.db
            async with hashes_writer() as wdb:
                await wdb.execute(
                    f"UPDATE file_hashes SET dup_count = ? "
                    f"WHERE {hash_column} IN ({ph})"
                    f"{update_extra}",
                    [dc] + batch,
                )
                # Get affected file_ids for catalog sync
                affected_rows = await wdb.execute(
                    f"SELECT file_id FROM file_hashes "
                    f"WHERE {hash_column} IN ({ph})"
                    f"{update_extra}",
                    batch,
                )
                affected_ids = [r[0] for r in await affected_rows.fetchall()]

            # Denormalized copy: catalog files.dup_count (for search/sort indexing)
            if affected_ids:
                for j in range(0, len(affected_ids), 500):
                    id_batch = affected_ids[j : j + 500]
                    id_ph = ",".join("?" for _ in id_batch)
                    async with db_writer() as cdb:
                        await cdb.execute(
                            f"UPDATE files SET dup_count = ? WHERE id IN ({id_ph})",
                            [dc] + id_batch,
                        )

            processed += len(batch)

            if on_progress:
                await on_progress(processed, total)

            await asyncio.sleep(0)

    log.info(
        "_batched_recalc: %s — %d hashes counted from %d rows",
        hash_column,
        total,
        len(rows),
    )

    return processed


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
                    f"FROM active_hashes f "
                    f"INNER JOIN _recount_hashes h ON f.{hash_column} = h.hash_val "
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


async def optimized_dup_recount(
    *, location_id: int | None = None, on_progress=None, on_total=None
):
    """Recount dup_count using two-pass, only touching actual duplicate groups.

    Unlike full_dup_recount which processes every hash, this only writes
    updates for hashes with COUNT > 1. Faster when most files are unique.

    Args:
        location_id: When set, only find hashes present on this location.
            The UPDATE still targets all rows with those hashes (dups span
            locations). None = all hashes globally.
        on_progress: Optional async callback(total_processed) called after
            each write batch.
        on_total: Optional async callback(total) called once after discovery,
            before writes.

    Returns:
        int: Number of duplicate hash groups written.

    Side effects:
        - Writes dup_count to hashes.db via hashes_writer().
        - Writes denormalized dup_count to catalog files table via db_writer().

    Callers: routes/stats.py catalog repair (optimized variant).

    Implementation: reads all rows per hash type into a Counter on a dedicated
    connection, filters to groups with count > 1, then writes per-hash UPDATEs
    in SQL_VAR_LIMIT (500) batches with catalog sync.
    """
    scope_label = f"location #{location_id}" if location_id else "all"
    log.info("optimized_dup_recount (%s): finding duplicate hashes", scope_label)

    hash_configs = [
        ("hash_strong", ""),
        ("hash_fast", " AND hash_strong IS NULL"),
    ]
    all_dup_hashes: list[tuple[str, str, int]] = []  # (hash_col, hash_val, dup_count)

    conn = await open_hashes_connection()
    try:
        for hash_column, update_extra in hash_configs:
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
                    f"SELECT f.{hash_column} FROM active_hashes f "
                    f"INNER JOIN _recount_hashes h ON f.{hash_column} = h.hash_val"
                )
                await conn.execute("DROP TABLE IF EXISTS _recount_hashes")
            else:
                rows = await conn.execute_fetchall(
                    f"SELECT {hash_column} FROM active_hashes "
                    f"WHERE {hash_column} IS NOT NULL AND {hash_column} != ''"
                    f"{update_extra}"
                )

            hash_counts = Counter(r[hash_column] for r in rows)
            for h, cnt in hash_counts.items():
                if cnt > 1:
                    all_dup_hashes.append((hash_column, h, cnt - 1))

            log.info(
                "optimized_dup_recount (%s): %s — %d duplicate groups from %d rows",
                scope_label,
                hash_column,
                sum(1 for dh in all_dup_hashes if dh[0] == hash_column),
                len(rows),
            )
    finally:
        try:
            await conn.execute("DROP TABLE IF EXISTS _recount_hashes")
        except Exception:
            pass
        await conn.close()

    total = len(all_dup_hashes)
    log.info(
        "optimized_dup_recount (%s): %d total duplicate hash groups", scope_label, total
    )
    if on_total:
        await on_total(total)

    processed = 0
    for i in range(0, total, SQL_VAR_LIMIT):
        batch = all_dup_hashes[i : i + SQL_VAR_LIMIT]
        catalog_updates: list[tuple[int, int]] = []  # (dc, file_id)
        async with hashes_writer() as wdb:
            for hash_column, h, dc in batch:
                update_extra = (
                    " AND hash_strong IS NULL" if hash_column == "hash_fast" else ""
                )
                await wdb.execute(
                    f"UPDATE file_hashes SET dup_count = ? "
                    f"WHERE {hash_column} = ?{update_extra}",
                    (dc, h),
                )
                rows = await wdb.execute(
                    f"SELECT file_id FROM file_hashes "
                    f"WHERE {hash_column} = ?{update_extra}",
                    (h,),
                )
                for r in await rows.fetchall():
                    catalog_updates.append((dc, r[0]))

        # Sync to catalog
        if catalog_updates:
            for j in range(0, len(catalog_updates), 500):
                sub = catalog_updates[j : j + 500]
                async with db_writer() as cdb:
                    await cdb.executemany(
                        "UPDATE files SET dup_count = ? WHERE id = ?",
                        sub,
                    )

        processed += len(batch)
        if on_progress:
            await on_progress(processed)
        if processed % 5000 < SQL_VAR_LIMIT:
            log.info(
                "optimized_dup_recount (%s): %d / %d hashes written",
                scope_label,
                processed,
                total,
            )
        await asyncio.sleep(0)

    log.info(
        "optimized_dup_recount (%s): complete — %d duplicate hashes", scope_label, total
    )
    return total


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
            affected: set[int] = set(merged_location_ids)

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
                        affected |= {r["location_id"] for r in rows}

            await update_location_dup_counts(affected)

            # Rebuild folder-level dup counts now that hashes.db is up to date
            for lid in affected:
                await recalculate_location_sizes(lid)

            total_hashes = len(merged_strong) + len(merged_fast)
            _active_recalc_locations.difference_update(affected)
            await broadcast(
                {
                    "type": "dup_recalc_completed",
                    "hashCount": total_hashes,
                    "locationIds": list(affected),
                }
            )
    except Exception:
        log.error("Coalesced dup recalc writer failed", exc_info=True)


async def find_dup_candidates(
    location_id: int | None = None,
) -> list[dict]:
    """Find files needing hash_fast that are in duplicate (hash_partial, file_size) groups.

    Reads from hashes.db to find dup groups, then fetches file info
    (full_path, inode) from catalog for the candidate files.

    location_id: when set, only returns candidates from dup groups that
    involve at least one file in this location.  None = all dup groups
    globally (for repair).

    Returns [{id, full_path, location_id, file_size, hash_partial, inode}, ...]
    """
    scope = f"location {location_id}" if location_id else "global"

    log.info("find_dup_candidates: starting (scope=%s)", scope)
    t0 = time.monotonic()

    # Step 1-2: find dup groups in hashes.db
    conn = await open_hashes_connection()
    try:
        t1 = time.monotonic()
        if location_id is not None:
            await conn.execute(
                "CREATE TEMP TABLE _dup_pairs (hash_partial TEXT, file_size INTEGER)"
            )
            await conn.execute(
                "INSERT INTO _dup_pairs "
                "SELECT DISTINCT hash_partial, file_size FROM active_hashes "
                "WHERE location_id = ? AND hash_partial IS NOT NULL "
                "AND file_size > 0",
                (location_id,),
            )
            await conn.execute(
                "CREATE INDEX _dup_pairs_idx ON _dup_pairs(hash_partial, file_size)"
            )
            await conn.commit()

            rows = await conn.execute_fetchall(
                "SELECT f.hash_partial, f.file_size FROM active_hashes f "
                "INNER JOIN _dup_pairs p "
                "ON f.hash_partial = p.hash_partial AND f.file_size = p.file_size"
            )
            await conn.execute("DROP TABLE IF EXISTS _dup_pairs")
        else:
            rows = await conn.execute_fetchall(
                "SELECT hash_partial, file_size FROM active_hashes "
                "WHERE hash_partial IS NOT NULL AND file_size > 0"
            )

        log.info(
            "find_dup_candidates: hashes query returned %d rows in %.1fs",
            len(rows),
            time.monotonic() - t1,
        )

        pair_counts = Counter((r["hash_partial"], r["file_size"]) for r in rows)
        dup_pairs = {k for k, v in pair_counts.items() if v > 1}
        log.info(
            "find_dup_candidates: %d total pairs, %d with duplicates",
            len(pair_counts),
            len(dup_pairs),
        )

        if not dup_pairs:
            log.info(
                "find_dup_candidates: no dup groups, done in %.1fs",
                time.monotonic() - t0,
            )
            return []

        # Step 3: find candidate file_ids (hash_fast IS NULL) in hashes.db
        t4 = time.monotonic()
        await conn.execute(
            "CREATE TEMP TABLE _dup_groups (hash_partial TEXT, file_size INTEGER)"
        )
        await conn.executemany(
            "INSERT INTO _dup_groups VALUES (?, ?)",
            list(dup_pairs),
        )
        await conn.execute(
            "CREATE INDEX _dup_groups_idx ON _dup_groups(hash_partial, file_size)"
        )
        await conn.commit()

        candidate_rows = await conn.execute_fetchall(
            "SELECT f.file_id, f.location_id, f.file_size, f.hash_partial "
            "FROM active_hashes f "
            "INNER JOIN _dup_groups g "
            "ON f.hash_partial = g.hash_partial AND f.file_size = g.file_size "
            "WHERE f.hash_fast IS NULL"
        )
        await conn.execute("DROP TABLE IF EXISTS _dup_groups")

        log.info(
            "find_dup_candidates: %d candidate files from hashes.db in %.1fs",
            len(candidate_rows),
            time.monotonic() - t4,
        )
    finally:
        try:
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
                f"SELECT id, full_path, inode FROM files WHERE id IN ({ph})",
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
    _act_reg(activity_name, "Dup recalc", f"0/{total}")

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
            _act_upd(activity_name, progress=f"{actual}/{total}")

    try:
        if strong:
            await _batched_recalc(
                strong, hash_column="hash_strong", on_progress=_on_progress
            )
            progress_offset = len(strong)

        if fast:
            await _batched_recalc(
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

        await post_op_stats()

        log.info(
            "dup_count backfill: complete, fixed %d hashes",
            updated,
        )
        await broadcast({"type": "dup_backfill_completed", "updated": updated})

    except Exception:
        log.exception("dup_count backfill failed")


SMALL_FILE_THRESHOLD = 128 * 1024  # 128KB — hash_partial == hash_fast, no agent needed
HASH_BATCH_BYTES = 500 * 1024 * 1024  # 500MB — max total bytes per batch request
MAX_RETRIES = 3
RETRY_DELAY = 5


async def hash_candidates_for_location(
    location_id: int,
    agent_id: int,
) -> tuple[int, int, int]:
    """Find dup candidates for a location, handle small files, queue large.

    Finds files needing hash_fast that share (size, hash_partial) with other
    files.  Small files (<=128KB): copy hash_partial to hash_fast and submit
    to coalesced dup writer.  Large files: insert into pending_hashes for
    the hash drainer to process.

    Returns (total_candidates, small_handled, large_queued).
    """
    candidates = await find_dup_candidates(location_id=location_id)

    log.info(
        "hash_candidates_for_location: %d raw candidates for location %d",
        len(candidates),
        location_id,
    )

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
        # Submit to coalesced writer for incremental dup recounting
        submit_hashes_for_recalc(
            fast_hashes=small_fast_hashes,
            source=f"small file candidates location {location_id}",
            location_ids={location_id},
        )

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


async def post_ingest_dup_processing(
    location_id: int,
    agent_id: int,
    location_name: str,
    on_progress=None,
    broadcast_scan_progress: bool = True,
):
    """Shared post-ingest dup processing for scan, rescan, and import.

    1. Find dup candidates (files sharing hash_partial that need hash_fast)
    2. Handle small files (copy hash_partial → hash_fast)
    3. Queue large files for hash drainer
    4. Recount dup_count for affected hashes

    Called after files + hashes are in the catalog and hashes.db.
    broadcast_scan_progress: False when called from housekeeping (not a scan).
    """
    log.info("Post-ingest dup processing for %s", location_name)

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

    submit_hashes_for_recalc(
        fast_hashes={hash_fast},
        source="hash_file",
        location_ids={location_id},
    )


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

    Always scoped to location_id — the drainer only processes pending_hashes
    for the location being scanned/imported. Leftover cleanup for other
    locations is a housekeeping concern, not a scan/import concern.
    """
    FETCH_LIMIT = 2000
    total_hashed = 0
    total_pending = 0

    where_clause = "WHERE agent_id = ? AND location_id = ?"
    where_params: tuple = (agent_id, location_id)

    async def _process_batch(batch_rows):
        nonlocal total_hashed
        paths = [r["full_path"] for r in batch_rows]
        path_to_file_id = {r["full_path"]: r["file_id"] for r in batch_rows}
        pending_ids = [r["id"] for r in batch_rows]
        affected_locs = {r["location_id"] for r in batch_rows}

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
            submit_hashes_for_recalc(
                strong_hashes=None,
                fast_hashes=affected_fast,
                source=f"hash drainer {location_name}",
                location_ids=affected_locs,
            )

        log.info(
            "Hash drainer: %d hashed (%d / %d) for %s",
            len(hash_results),
            total_hashed,
            total_pending,
            location_name,
        )

        if on_progress:
            await on_progress(total_hashed, total_pending)

    try:
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
                    log.info("Hash drainer: done for %s", location_name)
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
        return
    except asyncio.CancelledError:
        return
