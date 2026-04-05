import asyncio
import logging
from collections import defaultdict

from starlette.requests import Request
from file_hunter.core import ProgressTracker, json_ok, json_error
from file_hunter.db import db_writer, read_db, open_connection
from file_hunter.hashes_db import hashes_writer, mark_hashes_stale, _hashes_db_path
from file_hunter.services.agent_ops import hash_fast_batch, hash_partial_batch
from file_hunter.services.dup_counts import (
    find_dup_candidates,
    optimized_dup_recount,
    update_location_dup_counts,
)
from file_hunter.ws.agent import get_online_agent_ids
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
    # partials phase
    partials_found=0,
    partials_total=0,
    partials_hashed=0,
    partials_skipped=0,
    partials_errors=0,
    partials_stale=0,
    partials_scan_done=0,
    partials_scan_total=0,
    # hashes phase
    hashed=0,
    skipped=0,
    errors=0,
    stale=0,
    total=0,
    query_done=0,
    query_total=0,
    # duplicates phase
    dup_step="",
    dup_step_done=0,
    dup_step_total=0,
    dup_groups=0,
    # shared
    error_details=[],
    # sizes phase
    locations_done=0,
    locations_total=0,
)

# Tracked so on_shutdown can cancel it
_repair_task: asyncio.Task | None = None


def _reset_progress():
    _repair_progress.reset()
    _repair_progress["phases"] = []
    _repair_progress["error_details"] = []


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

    valid_phases = {"partials", "hashes", "duplicates", "sizes"}
    phases = body.get("phases")
    if phases:
        phases = [p for p in phases if p in valid_phases]
        if not phases:
            return json_error("No valid phases specified")
    else:
        phases = ["partials", "hashes", "duplicates", "sizes"]

    _reset_progress()
    _repair_progress["phases"] = phases
    _repair_task = asyncio.create_task(_bg_repair(phases))
    return json_ok({"status": "started", "phases": phases})


async def repair_catalog_progress(request: Request):
    """Return current repair progress."""
    return json_ok(dict(_repair_progress))


async def _bg_repair(phases: list[str] | None = None):
    """Repair catalog with selectable phases, queue paused.

    Phase: "partials" — find catalog files with no file_hashes row, compute
    hash_partial via agent for each online location.
    Phase: "hashes" — hash promotion: find files in duplicate groups that
    lack hash_fast, compute via agent.
    Phase: "duplicates" — full dup recount for all hashes.
    Phase: "sizes" — recalculate all location sizes.
    """
    if phases is None:
        phases = ["partials", "hashes", "duplicates", "sizes"]

    HASH_BATCH_SIZE = 200

    try:
        _repair_progress["status"] = "running"
        _repair_progress["phase"] = "pausing"
        await broadcast({"type": "repair_started"})

        hashed = 0
        errors = 0
        skipped = 0
        stale = 0
        total = 0
        dup_total = 0
        all_locs = []

        async with paused_queue("repair", "Catalog Repair"):
            # ── Phase: Find missing partials ─────────────────────────
            if "partials" not in phases:
                log.info("Catalog repair: skipping phase (partials)")
            else:
                _repair_progress["phase"] = "finding_partials"
                log.info("Catalog repair: finding files missing from hashes.db")

                # Get all locations
                rdb = await open_connection()
                try:
                    loc_rows = await rdb.execute_fetchall(
                        "SELECT id, name, agent_id FROM locations"
                    )
                finally:
                    await rdb.close()

                _repair_progress["partials_scan_total"] = len(loc_rows)

                # Find missing files per location via ATTACH
                hashes_path = str(_hashes_db_path())
                all_missing: list[dict] = []
                cat_conn = await open_connection()
                try:
                    await cat_conn.execute(
                        "ATTACH ? AS h", (hashes_path,)
                    )
                    for i, loc in enumerate(loc_rows):
                        rows = await cat_conn.execute_fetchall(
                            "SELECT f.id, f.full_path, f.file_size, f.inode "
                            "FROM files f "
                            "WHERE f.location_id = ? AND f.file_size > 0 "
                            "AND f.stale = 0 "
                            "AND NOT EXISTS ("
                            "  SELECT 1 FROM h.file_hashes fh "
                            "  WHERE fh.file_id = f.id"
                            ")",
                            (loc["id"],),
                        )
                        for r in rows:
                            all_missing.append(
                                {
                                    "id": r["id"],
                                    "full_path": r["full_path"],
                                    "location_id": loc["id"],
                                    "file_size": r["file_size"],
                                    "inode": r["inode"],
                                }
                            )
                        _repair_progress["partials_scan_done"] = i + 1
                        _repair_progress["partials_found"] = len(all_missing)
                        await asyncio.sleep(0)
                    await cat_conn.execute("DETACH h")
                finally:
                    await cat_conn.close()

                log.info(
                    "Catalog repair: %d files missing from hashes.db",
                    len(all_missing),
                )

                # Dispatch to agents for hash_partial
                partials_total = len(all_missing)
                _repair_progress["phase"] = "hashing_partials"
                _repair_progress["partials_total"] = partials_total

                loc_agent_map: dict[int, int] = {}
                for loc in loc_rows:
                    if loc["agent_id"]:
                        loc_agent_map[loc["id"]] = loc["agent_id"]

                path_to_file: dict[str, dict] = {}
                agent_batches: dict[int, list[dict]] = defaultdict(list)
                for f in all_missing:
                    path_to_file[f["full_path"]] = f
                    agent_id = loc_agent_map.get(f["location_id"])
                    if agent_id:
                        agent_batches[agent_id].append(f)

                online_agents = get_online_agent_ids()
                p_hashed = 0
                p_skipped = 0
                p_errors = 0
                p_stale = 0

                for agent_id, files in agent_batches.items():
                    if agent_id not in online_agents:
                        p_skipped += len(files)
                        _repair_progress["partials_skipped"] = p_skipped
                        log.info(
                            "Catalog repair: agent %d offline, "
                            "skipping %d partials",
                            agent_id,
                            len(files),
                        )
                        continue

                    # Sort by inode for disk locality
                    files.sort(key=lambda f: f.get("inode") or 0)
                    paths = [f["full_path"] for f in files]

                    for j in range(0, len(paths), HASH_BATCH_SIZE):
                        await asyncio.sleep(0)
                        batch_paths = paths[j : j + HASH_BATCH_SIZE]
                        batch_files = files[j : j + HASH_BATCH_SIZE]

                        try:
                            result = await hash_partial_batch(
                                agent_id, batch_paths
                            )
                        except (ConnectionError, OSError):
                            p_skipped += len(paths[j:])
                            _repair_progress["partials_skipped"] = p_skipped
                            log.warning(
                                "Catalog repair: agent %d failed, "
                                "skipping %d partials",
                                agent_id,
                                len(paths[j:]),
                            )
                            break

                        hash_results = result.get("results", [])
                        if hash_results:
                            path_hash = {
                                r["path"]: r["hash_partial"]
                                for r in hash_results
                                if r.get("hash_partial")
                            }
                            inserts = []
                            for bf in batch_files:
                                hp = path_hash.get(bf["full_path"])
                                if hp:
                                    inserts.append(
                                        (
                                            bf["id"],
                                            bf["location_id"],
                                            bf["file_size"],
                                            hp,
                                        )
                                    )
                            if inserts:
                                async with hashes_writer() as hdb:
                                    await hdb.executemany(
                                        "INSERT INTO file_hashes "
                                        "(file_id, location_id, file_size, "
                                        "hash_partial) VALUES (?, ?, ?, ?) "
                                        "ON CONFLICT(file_id) DO UPDATE SET "
                                        "hash_partial=excluded.hash_partial, "
                                        "file_size=excluded.file_size",
                                        inserts,
                                    )
                                p_hashed += len(inserts)

                        batch_errors = result.get("errors", [])
                        not_found_ids = []
                        for err in batch_errors:
                            if err.get("error") == "File not found":
                                # Mark catalog entry stale
                                fp = err.get("path")
                                bf_match = path_to_file.get(fp)
                                if bf_match:
                                    not_found_ids.append(bf_match["id"])
                                log.info(
                                    "Catalog repair: marking stale "
                                    "(not found): %s",
                                    fp,
                                )
                            else:
                                log.warning(
                                    "Catalog repair: partial hash "
                                    "error: %s",
                                    err,
                                )
                                p_errors += 1
                                _repair_progress["error_details"].append(
                                    {
                                        "phase": "partials",
                                        "path": err.get("path", ""),
                                        "error": err.get("error", ""),
                                    }
                                )
                        if not_found_ids:
                            async with db_writer() as cdb:
                                ph = ",".join("?" for _ in not_found_ids)
                                await cdb.execute(
                                    f"UPDATE files SET stale = 1 "
                                    f"WHERE id IN ({ph})",
                                    not_found_ids,
                                )
                            p_stale += len(not_found_ids)
                        _repair_progress["partials_hashed"] = p_hashed
                        _repair_progress["partials_errors"] = p_errors
                        _repair_progress["partials_stale"] = p_stale

                log.info(
                    "Catalog repair: partials complete — "
                    "hashed=%d, stale=%d, errors=%d, skipped=%d of %d",
                    p_hashed,
                    p_stale,
                    p_errors,
                    p_skipped,
                    partials_total,
                )

            # ── Phase: Hash promotion ────────────────────────────────
            if "hashes" not in phases:
                log.info("Catalog repair: skipping phase 1 (hashes)")
            else:
                _repair_progress["phase"] = "querying"
                log.info("Catalog repair: phase 1 — querying candidates")

                async def _on_query_progress(done, total_groups):
                    _repair_progress["query_done"] = done
                    _repair_progress["query_total"] = total_groups

                all_candidates = await find_dup_candidates(
                    on_progress=_on_query_progress
                )

                # Build location → agent mapping
                rdb = await open_connection()
                try:
                    loc_rows = await rdb.execute_fetchall(
                        "SELECT id, name, agent_id FROM locations"
                    )
                finally:
                    await rdb.close()

                loc_agent_map: dict[int, int] = {}
                for loc in loc_rows:
                    if loc["agent_id"]:
                        loc_agent_map[loc["id"]] = loc["agent_id"]

                total = len(all_candidates)
                _repair_progress["phase"] = "hashing"
                _repair_progress["total"] = total
                log.info(
                    "Catalog repair: %d files need hash_fast",
                    total,
                )

                path_to_id: dict[str, int] = {}
                agent_batches: dict[int, list[str]] = defaultdict(list)
                for row in all_candidates:
                    path_to_id[row["full_path"]] = row["id"]
                    agent_id = loc_agent_map.get(row["location_id"])
                    if agent_id:
                        agent_batches[agent_id].append(row["full_path"])

                failed_agents: set[int] = set()
                online_agents = get_online_agent_ids()

                for agent_id, paths in agent_batches.items():
                    if agent_id not in online_agents or agent_id in failed_agents:
                        skipped += len(paths)
                        _repair_progress["skipped"] = skipped
                        log.info(
                            "Catalog repair: agent %d offline, skipping %d files",
                            agent_id,
                            len(paths),
                        )
                        continue

                    for i in range(0, len(paths), HASH_BATCH_SIZE):
                        await asyncio.sleep(0)

                        if agent_id in failed_agents:
                            skipped += len(paths[i:])
                            _repair_progress["skipped"] = skipped
                            break

                        batch = paths[i : i + HASH_BATCH_SIZE]
                        try:
                            result = await hash_fast_batch(agent_id, batch)
                        except (ConnectionError, OSError):
                            failed_agents.add(agent_id)
                            skipped += len(paths[i:])
                            _repair_progress["skipped"] = skipped
                            log.warning(
                                "Catalog repair: agent %d failed, skipping %d files",
                                agent_id,
                                len(paths[i:]),
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
                        batch_errors = result.get("errors", [])
                        not_found_ids = []
                        for err in batch_errors:
                            if err.get("error") == "File not found":
                                fp = err.get("path")
                                fid = path_to_id.get(fp)
                                if fid:
                                    not_found_ids.append(fid)
                                log.info(
                                    "Catalog repair: marking stale "
                                    "(not found): %s",
                                    fp,
                                )
                            else:
                                log.warning(
                                    "Catalog repair: hash error: %s",
                                    err,
                                )
                                errors += 1
                                _repair_progress["error_details"].append(
                                    {
                                        "phase": "hashes",
                                        "path": err.get("path", ""),
                                        "error": err.get("error", ""),
                                    }
                                )
                        if not_found_ids:
                            async with db_writer() as cdb:
                                ph = ",".join("?" for _ in not_found_ids)
                                await cdb.execute(
                                    f"UPDATE files SET stale = 1 "
                                    f"WHERE id IN ({ph})",
                                    not_found_ids,
                                )
                            await mark_hashes_stale(not_found_ids)
                            stale += len(not_found_ids)
                        _repair_progress["hashed"] = hashed
                        _repair_progress["errors"] = errors
                        _repair_progress["stale"] = stale

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
                log.info("Catalog repair: full dup recount")

                async def _on_recount_progress(step, done, total):
                    _repair_progress["dup_step"] = step
                    _repair_progress["dup_step_done"] = done
                    _repair_progress["dup_step_total"] = total

                dup_total = await optimized_dup_recount(
                    on_progress=_on_recount_progress
                )
                _repair_progress["dup_groups"] = dup_total

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
