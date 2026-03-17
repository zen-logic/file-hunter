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
    from file_hunter.services.queue_manager import pause, resume
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    try:
        _progress["status"] = "pausing"
        await pause()
        await broadcast(
            {"type": "queue_paused", "reason": "import", "location": location_name}
        )
        await run_import(catalog_path, location_id, root_path)

        # Recount dups for imported location before resuming queue
        log.info(
            "Import post-processing: status=%s, starting dup recount for location %d",
            _progress["status"],
            location_id,
        )
        if _progress["status"] == "complete":
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
    finally:
        resume()
        await broadcast({"type": "queue_resumed"})

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
