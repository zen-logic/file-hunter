"""Routes for catalog import."""

import asyncio
import os
import tempfile

from starlette.requests import Request

from file_hunter.core import json_ok, json_error
from file_hunter.db import get_db
from file_hunter.services.import_catalog import (
    get_progress,
    read_catalog_meta,
    run_import,
)


async def import_catalog_upload(request: Request):
    """Upload a catalog.db file and return its metadata."""
    form = await request.form()
    catalog_file = form.get("catalog")

    if not catalog_file or not hasattr(catalog_file, "filename"):
        return json_error("No catalog file provided")

    # Save to temp file
    tmp = tempfile.NamedTemporaryFile(
        suffix=".db", prefix="catalog-import-", delete=False
    )
    try:
        content = await catalog_file.read()
        tmp.write(content)
        tmp.close()

        # Validate it's a catalog DB
        try:
            meta = read_catalog_meta(tmp.name)
        except Exception:
            os.unlink(tmp.name)
            return json_error("Invalid catalog file")

        # Fetch available agents and locations for the UI
        db = await get_db()
        agents = await db.execute_fetchall(
            "SELECT id, name, status FROM agents ORDER BY id"
        )
        locations = await db.execute_fetchall(
            "SELECT id, name, root_path, agent_id FROM locations ORDER BY name"
        )

        return json_ok(
            {
                "temp_path": tmp.name,
                "meta": meta,
                "agents": [dict(a) for a in agents],
                "locations": [dict(loc) for loc in locations],
            }
        )

    except Exception:
        try:
            os.unlink(tmp.name)
        except OSError:
            pass
        raise


async def import_catalog_run(request: Request):
    """Start the catalog import."""
    body = await request.json()
    temp_path = body.get("temp_path", "")
    location_id = body.get("location_id")
    agent_id = body.get("agent_id")
    location_name = body.get("location_name")
    root_path = body.get("root_path")

    # Validate temp path is one we created
    temp_dir = tempfile.gettempdir()
    if (
        not temp_path
        or not temp_path.startswith(os.path.join(temp_dir, "catalog-import-"))
        or not temp_path.endswith(".db")
        or not os.path.exists(temp_path)
    ):
        return json_error("Catalog file not found (expired or already imported)")

    # Check if an import is already running
    progress = get_progress()
    if progress["status"] not in ("idle", "complete", "error"):
        return json_error("An import is already in progress")

    if location_id:
        # Use existing location
        db = await get_db()
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path, agent_id FROM locations WHERE id = ?",
            (location_id,),
        )
        row = rows[0] if rows else None
        if not row:
            return json_error("Location not found")
        root_path = row["root_path"]
        agent_id = row["agent_id"]
        location_name = row["name"]
    elif agent_id and root_path and location_name:
        # Create new location
        from file_hunter.db import db_writer
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        async with db_writer() as wdb:
            cursor = await wdb.execute(
                "INSERT INTO locations (name, root_path, agent_id, date_added, total_size) "
                "VALUES (?, ?, ?, ?, 0)",
                (location_name, root_path, agent_id, now),
            )
            location_id = cursor.lastrowid

        # Push location to agent config so it knows about the new path
        from file_hunter.services.agent_ops import _resolve_agent, _post

        resolved = _resolve_agent(agent_id)
        if resolved:
            host, port, token = resolved
            try:
                await _post(
                    host,
                    port,
                    token,
                    "/locations/add",
                    {"name": location_name, "path": root_path},
                )
            except Exception:
                import logging

                logging.getLogger("file_hunter").warning(
                    "Failed to push imported location to agent"
                )
    else:
        return json_error(
            "Must specify location_id or agent_id + root_path + location_name"
        )

    # Launch import in background
    asyncio.create_task(
        _run_and_notify(temp_path, location_id, root_path, agent_id, location_name)
    )

    return json_ok({"location_id": location_id})


async def _run_and_notify(
    catalog_path: str,
    location_id: int,
    root_path: str,
    agent_id: int,
    location_name: str,
):
    """Pause queue, run import, recount dups, resume queue."""
    import logging

    log = logging.getLogger("file_hunter")

    from file_hunter.services.import_catalog import _progress
    from file_hunter.services.queue_manager import paused_queue
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    try:
        _progress["status"] = "pausing"
        async with paused_queue("import", location_name):
            await run_import(catalog_path, location_id, root_path)

            # Recount dups for imported location before resuming queue
            log.info(
                "Import post-processing: status=%s, starting dup recount for location %d",
                _progress["status"],
                location_id,
            )
            if _progress["status"] == "complete":
                # --- Candidate detection + hash_fast ---
                from file_hunter.services.dup_counts import find_dup_candidates

                _progress["status"] = "candidates"
                candidates = await find_dup_candidates(location_id=location_id)

                log.info(
                    "Import: %d candidates to full-hash for location %d",
                    len(candidates),
                    location_id,
                )

                # Filter to files on this agent's locations only
                from file_hunter.db import get_db

                db = await get_db()
                agent_loc_rows = await db.execute_fetchall(
                    "SELECT id FROM locations WHERE agent_id = ?",
                    (agent_id,),
                )
                agent_loc_ids = {r["id"] for r in agent_loc_rows}
                candidates = [
                    c for c in candidates if c["location_id"] in agent_loc_ids
                ]
                log.info(
                    "Import: %d candidates on this agent for location %d",
                    len(candidates),
                    location_id,
                )

                # Hash candidates: small files copied from hash_partial,
                # large files sent to agent one at a time
                from file_hunter.db import db_writer
                from file_hunter.services.agent_ops import dispatch

                SMALL_FILE_THRESHOLD = 128 * 1024  # 128KB

                small_files = [
                    c for c in candidates if c["file_size"] <= SMALL_FILE_THRESHOLD
                ]
                large_files = [
                    c for c in candidates if c["file_size"] > SMALL_FILE_THRESHOLD
                ]

                total = len(candidates)
                hashed = 0

                if candidates and agent_id:
                    _progress["status"] = "hashing"
                    _progress["dup_hashes_total"] = total
                    _progress["dup_hashes_done"] = 0

                    # Small files: hash_partial == hash_fast, bulk copy
                    if small_files:
                        async with db_writer() as wdb:
                            for c in small_files:
                                await wdb.execute(
                                    "UPDATE files SET hash_fast = ? WHERE id = ?",
                                    (c["hash_partial"], c["id"]),
                                )
                        hashed += len(small_files)
                        _progress["dup_hashes_done"] = hashed
                        log.info(
                            "Import: %d small files hash_fast copied from hash_partial",
                            len(small_files),
                        )

                    # Large files: one at a time via agent, retry on failure
                    MAX_RETRIES = 3
                    RETRY_DELAY = 5
                    for c in large_files:
                        for attempt in range(1, MAX_RETRIES + 1):
                            try:
                                result = await dispatch(
                                    "file_hash",
                                    c["location_id"],
                                    path=c["full_path"],
                                )
                                async with db_writer() as wdb:
                                    await wdb.execute(
                                        "UPDATE files SET hash_fast = ? WHERE id = ?",
                                        (result["hash_fast"], c["id"]),
                                    )
                                hashed += 1
                                break
                            except Exception as e:
                                if attempt < MAX_RETRIES:
                                    log.warning(
                                        "Import hash_fast attempt %d/%d failed for %s: %s — retrying in %ds",
                                        attempt,
                                        MAX_RETRIES,
                                        c["full_path"],
                                        e,
                                        RETRY_DELAY,
                                    )
                                    await asyncio.sleep(RETRY_DELAY)
                                else:
                                    log.error(
                                        "Import hash_fast FAILED after %d attempts for %s: %s",
                                        MAX_RETRIES,
                                        c["full_path"],
                                        e,
                                    )
                        _progress["dup_hashes_done"] = hashed

                    log.info(
                        "Import: %d files hashed (%d small, %d large) for location %d",
                        hashed,
                        len(small_files),
                        len(large_files),
                        location_id,
                    )

                # --- Dup recount ---
                from file_hunter.services.dup_counts import optimized_dup_recount

                _progress["status"] = "dup_recount"
                _progress["dup_hashes_done"] = 0
                _progress["dup_hashes_total"] = 0

                async def _on_dup_total(total):
                    _progress["dup_hashes_total"] = total

                async def _on_dup_progress(total_processed):
                    _progress["dup_hashes_done"] = total_processed

                await optimized_dup_recount(
                    location_id=location_id,
                    on_progress=_on_dup_progress,
                    on_total=_on_dup_total,
                )
                log.info("Import dup recount complete for location %d", location_id)
                _progress["status"] = "complete"
            else:
                log.warning(
                    "Import dup recount skipped — status was '%s' not 'complete'",
                    _progress["status"],
                )

    except Exception as e:
        log.exception("Import post-processing failed")
        _progress["status"] = "error"
        _progress["error"] = str(e)

    invalidate_stats_cache()
    await broadcast(
        {
            "type": "location_changed",
            "action": "imported",
            "location": {"label": location_name},
        }
    )


async def import_catalog_progress(request: Request):
    """Return current import progress."""
    return json_ok(get_progress())
