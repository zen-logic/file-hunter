from starlette.requests import Request
from file_hunter.db import open_connection
from file_hunter.core import json_ok, json_error
from file_hunter.services.stats import get_stats, get_location_stats, get_folder_stats


async def stats(request: Request):
    conn = await open_connection()
    try:
        data = await get_stats(conn)
    finally:
        await conn.close()
    return json_ok(data)


async def recalculate_stats(request: Request):
    """Force a full recalculation of all location sizes and stats cache."""
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.stats import invalidate_stats_cache

    conn = await open_connection()
    try:
        loc_rows = await conn.execute_fetchall("SELECT id FROM locations")
        for loc in loc_rows:
            await recalculate_location_sizes(conn, loc["id"])
    finally:
        await conn.close()
    invalidate_stats_cache()
    return json_ok({"recalculated": len(loc_rows)})


async def repair_catalog(request: Request):
    """Repair catalog: clear false stale flags, recount sizes, recount dups.

    Runs on a dedicated connection. Returns immediately — broadcasts
    repair_completed when done.
    """
    import asyncio

    asyncio.create_task(_bg_repair())
    return json_ok({"status": "started"})


async def _bg_repair():
    import logging
    from file_hunter.services.sizes import recalculate_location_sizes
    from file_hunter.services.dup_counts import recalculate_dup_counts
    from file_hunter.services.stats import invalidate_stats_cache
    from file_hunter.ws.scan import broadcast

    log = logging.getLogger("file_hunter")

    conn = await open_connection()
    try:
        log.info("Catalog repair: starting")
        await broadcast({"type": "repair_started"})

        # 1. Clear all stale flags — next scan will re-mark correctly
        cursor = await conn.execute("UPDATE files SET stale = 0 WHERE stale = 1")
        stale_cleared = cursor.rowcount
        await conn.commit()
        log.info("Catalog repair: cleared %d stale flags", stale_cleared)

        # 2. Recalculate all location sizes
        loc_rows = await conn.execute_fetchall("SELECT id FROM locations")
        for loc in loc_rows:
            await recalculate_location_sizes(conn, loc["id"])
        log.info("Catalog repair: recalculated sizes for %d locations", len(loc_rows))

        # 3. Full dup recount — get all hash_strong values and recount
        hash_rows = await conn.execute_fetchall(
            "SELECT DISTINCT hash_strong FROM files "
            "WHERE hash_strong IS NOT NULL AND hash_strong != ''"
        )
        all_hashes = {r["hash_strong"] for r in hash_rows}
        if all_hashes:
            log.info("Catalog repair: recounting dups for %d hashes", len(all_hashes))
            await recalculate_dup_counts(conn, all_hashes, source="catalog repair")

        invalidate_stats_cache()

        log.info(
            "Catalog repair: complete (stale cleared=%d, locations=%d, hashes=%d)",
            stale_cleared,
            len(loc_rows),
            len(all_hashes),
        )
        await broadcast(
            {
                "type": "repair_completed",
                "staleCleared": stale_cleared,
                "locations": len(loc_rows),
                "hashes": len(all_hashes),
            }
        )
    except Exception:
        log.exception("Catalog repair failed")
        await broadcast({"type": "repair_failed"})
    finally:
        await conn.close()


async def location_stats(request: Request):
    loc_id = int(request.path_params["id"])
    conn = await open_connection()
    try:
        data = await get_location_stats(conn, loc_id)
    finally:
        await conn.close()
    if not data:
        return json_error("Location not found.", 404)
    return json_ok(data)


async def folder_stats(request: Request):
    folder_id = int(request.path_params["id"])
    conn = await open_connection()
    try:
        data = await get_folder_stats(conn, folder_id)
    finally:
        await conn.close()
    if not data:
        return json_error("Folder not found.", 404)
    return json_ok(data)
