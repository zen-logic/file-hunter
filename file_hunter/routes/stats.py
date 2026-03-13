import asyncio

from starlette.requests import Request
from file_hunter.db import get_db
from file_hunter.core import json_ok, json_error
from file_hunter.services.stats import get_stats, get_location_stats, get_folder_stats


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
    import logging
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    log = logging.getLogger("file_hunter")

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
    """Repair catalog: clear false stale flags, recount sizes, recount dups.

    Runs on a dedicated connection. Returns immediately — broadcasts
    repair_completed when done.
    """
    asyncio.create_task(_bg_repair())
    return json_ok({"status": "started"})


async def _bg_repair():
    import logging
    from file_hunter.db import db_writer
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.dup_counts import recalculate_dup_counts
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    log = logging.getLogger("file_hunter")

    try:
        db = await get_db()
        log.info("Catalog repair: starting")
        await broadcast({"type": "repair_started"})

        # 1. Clear all stale flags — next scan will re-mark correctly
        async with db_writer() as wdb:
            cursor = await wdb.execute("UPDATE files SET stale = 0 WHERE stale = 1")
            stale_cleared = cursor.rowcount
        log.info("Catalog repair: cleared %d stale flags", stale_cleared)

        # 2. Recalculate all location sizes
        loc_rows = await db.execute_fetchall("SELECT id FROM locations")
        for loc in loc_rows:
            await recalculate_location_sizes(loc["id"])
        log.info("Catalog repair: recalculated sizes for %d locations", len(loc_rows))

        # 3. Full dup recount — both hash types
        strong_rows = await db.execute_fetchall(
            "SELECT DISTINCT hash_strong FROM files "
            "WHERE hash_strong IS NOT NULL AND hash_strong != ''"
        )
        strong_hashes = {r["hash_strong"] for r in strong_rows}

        fast_rows = await db.execute_fetchall(
            "SELECT DISTINCT hash_fast FROM files "
            "WHERE hash_fast IS NOT NULL AND hash_fast != '' AND hash_strong IS NULL"
        )
        fast_hashes = {r["hash_fast"] for r in fast_rows}

        total_hashes = len(strong_hashes) + len(fast_hashes)
        if total_hashes:
            log.info("Catalog repair: recounting dups for %d hashes", total_hashes)
            await recalculate_dup_counts(
                strong_hashes=strong_hashes,
                fast_hashes=fast_hashes,
                source="catalog repair",
            )

        invalidate_stats_cache()

        log.info(
            "Catalog repair: complete (stale cleared=%d, locations=%d, hashes=%d)",
            stale_cleared,
            len(loc_rows),
            total_hashes,
        )
        await broadcast(
            {
                "type": "repair_completed",
                "staleCleared": stale_cleared,
                "locations": len(loc_rows),
                "hashes": total_hashes,
            }
        )
    except Exception:
        log.exception("Catalog repair failed")
        await broadcast({"type": "repair_failed"})


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
