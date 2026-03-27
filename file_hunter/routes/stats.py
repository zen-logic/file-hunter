import asyncio
import logging
from collections import defaultdict

from starlette.requests import Request
from file_hunter.core import ProgressTracker, json_ok, json_error
from file_hunter.db import read_db, open_connection
from file_hunter.hashes_db import hashes_writer
from file_hunter.services.agent_ops import hash_fast_batch
from file_hunter.services.dup_counts import (
    find_dup_candidates,
    optimized_dup_recount,
    update_location_dup_counts,
)
from file_hunter.services.online_check import agent_online_check
from file_hunter.services.queue_manager import enqueue, paused_queue
from file_hunter.services.sizes import recalculate_location_sizes
from file_hunter.services.stats import (
    get_stats,
    get_location_stats,
    get_folder_stats,
    invalidate_stats_cache,
)
from file_hunter.ws.scan import broadcast

log = logging.getLogger("file_hunter")

_repair_progress = ProgressTracker(
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
)

# Tracked so on_shutdown can cancel it
_repair_task: asyncio.Task | None = None


def _reset_progress():
    _repair_progress.reset()
    _repair_progress["phases"] = []


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
    async with read_db() as db:
        data = await get_stats(db)
    return json_ok(data)


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

    HASH_BATCH_SIZE = 200

    try:
        _repair_progress["status"] = "running"
        _repair_progress["phase"] = "pausing"
        await broadcast({"type": "repair_started"})

        hashed = 0
        errors = 0
        skipped = 0
        total = 0
        dup_total = 0
        all_locs = []

        async with paused_queue("repair", "Catalog Repair"):
            # ── Phase 1: Hash promotion ──────────────────────────────
            if "hashes" not in phases:
                log.info("Catalog repair: skipping phase 1 (hashes)")
            else:
                _repair_progress["phase"] = "querying"
                log.info("Catalog repair: phase 1 — querying candidates")
                all_candidates = await find_dup_candidates()

                # Build location → agent mapping, filter to online agents
                rdb = await open_connection()
                try:
                    loc_rows = await rdb.execute_fetchall(
                        "SELECT id, name, agent_id FROM locations"
                    )
                finally:
                    await rdb.close()

                loc_agent_map: dict[int, int] = {}
                online_loc_ids = set()
                for loc in loc_rows:
                    if loc["agent_id"]:
                        loc_agent_map[loc["id"]] = loc["agent_id"]
                        if agent_online_check(loc):
                            online_loc_ids.add(loc["id"])

                candidates = [
                    c for c in all_candidates if c["location_id"] in online_loc_ids
                ]

                total = len(candidates)
                _repair_progress["phase"] = "hashing"
                _repair_progress["total"] = total
                log.info(
                    "Catalog repair: %d files need hash_fast on online locations",
                    total,
                )

                path_to_id: dict[str, int] = {}
                agent_batches: dict[int, list[str]] = defaultdict(list)
                for row in candidates:
                    path_to_id[row["full_path"]] = row["id"]
                    agent_id = loc_agent_map.get(row["location_id"])
                    if agent_id:
                        agent_batches[agent_id].append(row["full_path"])

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
                        except (ConnectionError, OSError):
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

                        # Write hash_fast results to hashes.db
                        hash_results = result.get("results", [])
                        written = 0
                        if hash_results:
                            async with hashes_writer() as hdb:
                                for hr in hash_results:
                                    fid = path_to_id.get(hr["path"])
                                    hf = hr.get("hash_fast")
                                    if fid and hf:
                                        await hdb.execute(
                                            "UPDATE file_hashes SET hash_fast = ? WHERE file_id = ?",
                                            (hf, fid),
                                        )
                                        written += 1
                        hashed += written
                        errors += len(result.get("errors", []))
                        _repair_progress["hashed"] = hashed
                        _repair_progress["errors"] = errors

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

                log.info(
                    "Catalog repair: phase 1 complete — "
                    "hashed=%d, errors=%d, skipped=%d of %d",
                    hashed,
                    errors,
                    skipped,
                    total,
                )

            # ── Phase 2: Full dup recount ────────────────────────────
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
                finally:
                    await rdb.close()

                await update_location_dup_counts({loc["id"] for loc in all_locs})

                log.info(
                    "Catalog repair: phase 2 complete — %d hashes recounted",
                    dup_total,
                )

            # ── Phase 3: Recalculate sizes ───────────────────────────
            if "sizes" not in phases:
                log.info("Catalog repair: skipping phase 3 (sizes)")
            else:
                _repair_progress["phase"] = "sizes"
                async with read_db() as _rdb:
                    all_locs = await _rdb.execute_fetchall("SELECT id FROM locations")
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


async def rehash_partial(request: Request):
    """Enqueue a rehash_partial operation for every agent-backed location."""
    async with read_db() as db:
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
    async with read_db() as db:
        data = await get_location_stats(db, loc_id)
    if not data:
        return json_error("Location not found.", 404)
    return json_ok(data)


async def folder_stats(request: Request):
    folder_id = int(request.path_params["id"])
    async with read_db() as db:
        data = await get_folder_stats(db, folder_id)
    if not data:
        return json_error("Folder not found.", 404)
    return json_ok(data)
