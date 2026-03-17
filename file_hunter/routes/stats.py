import asyncio
import logging

from starlette.requests import Request
from file_hunter.db import get_db
from file_hunter.core import json_ok, json_error
from file_hunter.services.stats import get_stats, get_location_stats, get_folder_stats

log = logging.getLogger("file_hunter")

# In-memory repair progress — polled by the frontend
_repair_progress = {
    "status": "idle",
    "phase": "",
    "hashed": 0,
    "skipped": 0,
    "errors": 0,
    "total": 0,
    "dup_hashes_done": 0,
    "dup_hashes_total": 0,
    "locations_done": 0,
    "locations_total": 0,
    "error": None,
}

# Tracked so on_shutdown can cancel it
_repair_task: asyncio.Task | None = None


def _reset_progress():
    _repair_progress.update(
        status="idle",
        phase="",
        phases=[],
        hashed=0,
        skipped=0,
        errors=0,
        total=0,
        dup_hashes_done=0,
        dup_hashes_total=0,
        locations_done=0,
        locations_total=0,
        error=None,
    )


async def stop_repair():
    """Cancel a running repair and resume the queue. Called from on_shutdown."""
    global _repair_task
    if _repair_task and not _repair_task.done():
        _repair_task.cancel()
        try:
            await _repair_task
        except (asyncio.CancelledError, Exception):
            pass
    _repair_task = None


async def stats(request: Request):
    db = await get_db()
    data = await get_stats(db)
    return json_ok(data)


async def recalculate_stats(request: Request):
    """Force a full recalculation of all location sizes and stats cache.

    Returns immediately — work runs in background, broadcasts when done.
    """
    asyncio.create_task(_bg_recalculate())
    return json_ok({"status": "started"})


async def _bg_recalculate():
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    try:
        db = await get_db()
        loc_rows = await db.execute_fetchall("SELECT id FROM locations")
        for loc in loc_rows:
            await recalculate_location_sizes(loc["id"])
        invalidate_stats_cache()
        log.info("Stats recalculated for %d locations", len(loc_rows))
        await broadcast(
            {
                "type": "size_recalc_completed",
                "locationIds": [r["id"] for r in loc_rows],
            }
        )
    except Exception:
        log.exception("Stats recalculation failed")


async def repair_catalog(request: Request):
    """Repair catalog with selectable phases.

    Accepts JSON body with optional "phases" list:
    - "hashes": hash promotion (phase 1)
    - "duplicates": full dup recount (phase 2)
    - "sizes": recalculate sizes (phase 3)

    Default (no phases specified): all three.
    Pauses the queue, runs selected phases, resumes.
    """
    global _repair_task

    if _repair_progress["status"] not in ("idle", "complete", "error"):
        return json_error("A repair is already in progress")

    body = {}
    try:
        body = await request.json()
    except Exception:
        pass

    valid_phases = {"hashes", "duplicates", "sizes"}
    phases = body.get("phases")
    if phases:
        phases = [p for p in phases if p in valid_phases]
        if not phases:
            return json_error("No valid phases specified")
    else:
        phases = ["hashes", "duplicates", "sizes"]

    _reset_progress()
    _repair_progress["phases"] = phases
    _repair_task = asyncio.create_task(_bg_repair(phases))
    return json_ok({"status": "started", "phases": phases})


async def repair_catalog_progress(request: Request):
    """Return current repair progress."""
    return json_ok(dict(_repair_progress))


async def _bg_repair(phases: list[str] | None = None):
    """Repair catalog with selectable phases, queue paused.

    Phase 1 ("hashes"): Hash promotion — find files on ONLINE locations with
    matching (file_size, hash_partial) that lack hash_fast. Compute via agent.
    Phase 2 ("duplicates"): Full dup recount — recalculate dup_count for all hashes.
    Phase 3 ("sizes"): Recalculate all location sizes.
    """
    if phases is None:
        phases = ["hashes", "duplicates", "sizes"]
    from collections import defaultdict

    from file_hunter.db import db_writer, open_connection
    from file_hunter.services.agent_ops import hash_fast_batch
    from file_hunter.services.dup_counts import optimized_dup_recount
    from file_hunter.services.online_check import agent_online_check
    from file_hunter.services.queue_manager import pause, resume
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    HASH_BATCH_SIZE = 50

    try:
        _repair_progress["status"] = "running"
        _repair_progress["phase"] = "pausing"
        await broadcast({"type": "repair_started"})

        await pause()
        await broadcast(
            {"type": "queue_paused", "reason": "repair", "location": "Catalog Repair"}
        )

        hashed = 0
        errors = 0
        skipped = 0
        total = 0
        dup_total = 0
        all_locs = []

        # ── Phase 1: Hash promotion ──────────────────────────────────
        if "hashes" not in phases:
            log.info("Catalog repair: skipping phase 1 (hashes)")
        else:
            _repair_progress["phase"] = "querying"
            log.info("Catalog repair: phase 1 — querying candidates")

            db = await open_connection()
            try:
                loc_rows = await db.execute_fetchall(
                    "SELECT id, name, agent_id FROM locations"
                )
                loc_agent_map: dict[int, int] = {}
                online_loc_ids = []
                for loc in loc_rows:
                    if loc["agent_id"]:
                        loc_agent_map[loc["id"]] = loc["agent_id"]
                        if agent_online_check(loc):
                            online_loc_ids.append(loc["id"])

                if online_loc_ids:
                    loc_ph = ",".join("?" for _ in online_loc_ids)
                    candidates = await db.execute_fetchall(
                        f"""SELECT f.id, f.full_path, f.location_id
                           FROM files f
                           WHERE f.hash_fast IS NULL
                             AND f.hash_partial IS NOT NULL
                             AND f.file_size > 0
                             AND f.stale = 0
                             AND f.location_id IN ({loc_ph})
                             AND EXISTS (
                                 SELECT 1 FROM files f2
                                 WHERE f2.file_size = f.file_size
                                   AND f2.hash_partial = f.hash_partial
                                   AND f2.id != f.id
                                   AND f2.stale = 0
                             )""",
                        online_loc_ids,
                    )
                else:
                    candidates = []
            finally:
                await db.close()

            total = len(candidates)
            _repair_progress["phase"] = "hashing"
            _repair_progress["total"] = total
            log.info(
                "Catalog repair: %d files need hash_fast on online locations", total
            )

            path_to_id: dict[str, int] = {}
            agent_batches: dict[int, list[str]] = defaultdict(list)
            for row in candidates:
                path_to_id[row["full_path"]] = row["id"]
                agent_id = loc_agent_map.get(row["location_id"])
                if agent_id:
                    agent_batches[agent_id].append(row["full_path"])

            pending_writes: list[tuple[int, str]] = []
            affected_fast_hashes: set[str] = set()
            failed_agents: set[int] = set()

            for agent_id, paths in agent_batches.items():
                if agent_id in failed_agents:
                    skipped += len(paths)
                    _repair_progress["skipped"] = skipped
                    continue

                for i in range(0, len(paths), HASH_BATCH_SIZE):
                    await asyncio.sleep(0)

                    if agent_id in failed_agents:
                        skipped += len(paths) - i
                        _repair_progress["skipped"] = skipped
                        break

                    batch = paths[i : i + HASH_BATCH_SIZE]
                    try:
                        result = await hash_fast_batch(agent_id, batch)
                    except ConnectionError:
                        failed_agents.add(agent_id)
                        remaining = len(paths) - i
                        skipped += remaining
                        _repair_progress["skipped"] = skipped
                        log.warning(
                            "Catalog repair: agent %d offline, skipping %d files",
                            agent_id,
                            remaining,
                        )
                        break

                    for item in result.get("results", []):
                        file_id = path_to_id.get(item["path"])
                        if file_id:
                            pending_writes.append((file_id, item["hash_fast"]))
                            affected_fast_hashes.add(item["hash_fast"])
                            hashed += 1

                    for item in result.get("errors", []):
                        errors += 1

                    _repair_progress["hashed"] = hashed
                    _repair_progress["errors"] = errors

                    if len(pending_writes) >= 50:
                        async with db_writer() as wdb:
                            for file_id, hash_fast in pending_writes:
                                await wdb.execute(
                                    "UPDATE files SET hash_fast = ? WHERE id = ?",
                                    (hash_fast, file_id),
                                )
                        pending_writes.clear()

                    done = hashed + errors + skipped
                    if done % 500 == 0 and done > 0:
                        log.info(
                            "Catalog repair: phase 1 — %d/%d "
                            "(hashed=%d, errors=%d, skipped=%d)",
                            done,
                            total,
                            hashed,
                            errors,
                            skipped,
                        )

            if pending_writes:
                async with db_writer() as wdb:
                    for file_id, hash_fast in pending_writes:
                        await wdb.execute(
                            "UPDATE files SET hash_fast = ? WHERE id = ?",
                            (hash_fast, file_id),
                        )
                pending_writes.clear()

            log.info(
                "Catalog repair: phase 1 complete — hashed=%d, errors=%d, skipped=%d of %d",
                hashed,
                errors,
                skipped,
                total,
            )

        # ── Phase 2: Full dup recount ────────────────────────────────
        if "duplicates" not in phases:
            log.info("Catalog repair: skipping phase 2 (duplicates)")
        else:
            _repair_progress["phase"] = "dup_recount"
            log.info("Catalog repair: phase 2 — full dup recount")

            async def _on_dup_total(t):
                _repair_progress["dup_hashes_total"] = t

            async def _on_dup_progress(total_processed):
                _repair_progress["dup_hashes_done"] = total_processed

            dup_total = await optimized_dup_recount(
                on_progress=_on_dup_progress, on_total=_on_dup_total
            )

            rdb = await open_connection()
            try:
                all_locs = await rdb.execute_fetchall("SELECT id FROM locations")
                loc_updates = []
                for loc in all_locs:
                    dc_rows = await rdb.execute_fetchall(
                        "SELECT COUNT(*) as c FROM files "
                        "WHERE location_id = ? AND stale = 0 "
                        "AND dup_exclude = 0 AND dup_count > 0",
                        (loc["id"],),
                    )
                    loc_updates.append((dc_rows[0]["c"], loc["id"]))
            finally:
                await rdb.close()

            async with db_writer() as wdb:
                for dc, lid in loc_updates:
                    await wdb.execute(
                        "UPDATE locations SET duplicate_count = ? WHERE id = ?",
                        (dc, lid),
                    )

            log.info(
                "Catalog repair: phase 2 complete — %d hashes recounted", dup_total
            )

        # ── Phase 3: Recalculate sizes ───────────────────────────────
        if "sizes" not in phases:
            log.info("Catalog repair: skipping phase 3 (sizes)")
        else:
            _repair_progress["phase"] = "sizes"
            all_locs = await (await get_db()).execute_fetchall(
                "SELECT id FROM locations"
            )
            _repair_progress["locations_total"] = len(all_locs)
            log.info(
                "Catalog repair: phase 3 — recalculating sizes for %d locations",
                len(all_locs),
            )

            for i, loc in enumerate(all_locs):
                await recalculate_location_sizes(loc["id"])
                _repair_progress["locations_done"] = i + 1

            invalidate_stats_cache()
            log.info("Catalog repair: phase 3 complete")

        # ── Done ─────────────────────────────────────────────────────
        _repair_progress["status"] = "complete"
        _repair_progress["phase"] = "complete"

        await broadcast(
            {
                "type": "repair_completed",
                "phases": phases,
                "hashed": hashed,
                "skipped": skipped,
                "errors": errors,
                "dupHashes": dup_total,
                "locations": len(all_locs),
            }
        )
        log.info("Catalog repair: phases %s complete", phases)

    except asyncio.CancelledError:
        log.info("Catalog repair: cancelled")
        _repair_progress["status"] = "error"
        _repair_progress["error"] = "Cancelled (server shutting down)"
    except Exception as e:
        log.exception("Catalog repair failed")
        _repair_progress["status"] = "error"
        _repair_progress["error"] = str(e)
        await broadcast({"type": "repair_failed"})
    finally:
        resume()
        await broadcast({"type": "queue_resumed"})


async def rehash_partial(request: Request):
    """Enqueue a rehash_partial operation for every agent-backed location."""
    from file_hunter.services.queue_manager import enqueue

    db = await get_db()
    rows = await db.execute_fetchall(
        "SELECT id, name, agent_id FROM locations WHERE agent_id IS NOT NULL"
    )
    if not rows:
        return json_error("No agent-backed locations found.")

    op_ids = []
    for loc in rows:
        op_id = await enqueue(
            "rehash_partial",
            loc["agent_id"],
            {"location_id": loc["id"], "location_name": loc["name"]},
        )
        op_ids.append(op_id)

    return json_ok({"queued": len(op_ids), "operation_ids": op_ids})


async def location_stats(request: Request):
    loc_id = int(request.path_params["id"])
    db = await get_db()
    data = await get_location_stats(db, loc_id)
    if not data:
        return json_error("Location not found.", 404)
    return json_ok(data)


async def folder_stats(request: Request):
    folder_id = int(request.path_params["id"])
    db = await get_db()
    data = await get_folder_stats(db, folder_id)
    if not data:
        return json_error("Folder not found.", 404)
    return json_ok(data)
