"""Aggregate statistics queries with in-memory caching.

Results are cached on first access and refreshed in the background after any
catalog mutation.  invalidate_stats_cache() schedules an async refresh rather
than clearing the cache, so the UI always gets an instant response.

All four counters (file_count, total_size, duplicate_count, type_counts) are
stored on locations and folders tables — no aggregate queries against files
at read time.
"""

import asyncio
import json
import logging

from file_hunter.core import format_size
from file_hunter.services.locations import check_location_online, get_disk_stats

logger = logging.getLogger(__name__)

# Module-level cache: key → cached result dict
_cache: dict[str, dict] = {}

# Background refresh task handle (to avoid duplicate refreshes)
_refresh_task: asyncio.Task | None = None


def invalidate_stats_cache():
    """Schedule a background refresh of all cached stats.

    Does NOT clear the cache — stale data is served until the refresh completes,
    keeping the UI responsive even on multi-million-row catalogs.
    """
    global _refresh_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        # No event loop — just clear (e.g. called from a non-async context)
        _cache.clear()
        return

    # If a refresh is already running, cancel and restart
    if _refresh_task and not _refresh_task.done():
        _refresh_task.cancel()
    _refresh_task = loop.create_task(_refresh_all())


async def warm_stats_cache():
    """Warm the cache on startup. Called from on_startup as a background task."""
    await _refresh_all()


async def _refresh_all():
    """Clear cache and refresh dashboard and location entries.

    Folder entries are cleared but not repopulated — they are lazily
    re-fetched on next access.
    """
    import time as _time

    from file_hunter.db import get_db

    _debug = logging.getLogger("scan_debug")
    _debug.debug("STATS_REFRESH: starting _refresh_all")

    _cache.clear()

    try:
        _t0 = _time.monotonic()
        _debug.debug("STATS_REFRESH: getting get_db()")
        conn = await get_db()
        _debug.debug("STATS_REFRESH: get_db() in %.3fs", _time.monotonic() - _t0)

        _t0 = _time.monotonic()
        _debug.debug("STATS_REFRESH: commit()")
        await conn.commit()
        _debug.debug("STATS_REFRESH: commit() in %.3fs", _time.monotonic() - _t0)

        _t0 = _time.monotonic()
        _debug.debug("STATS_REFRESH: _refresh_dashboard")
        await _refresh_dashboard(conn)
        _debug.debug("STATS_REFRESH: dashboard in %.3fs", _time.monotonic() - _t0)

        _t0 = _time.monotonic()
        _debug.debug("STATS_REFRESH: _refresh_all_locations")
        await _refresh_all_locations(conn)
        _debug.debug("STATS_REFRESH: locations in %.3fs", _time.monotonic() - _t0)

        # Notify connected browsers that cached stats have been updated
        from file_hunter.ws.scan import broadcast

        await broadcast({"type": "stats_updated"})
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Background stats refresh failed")


async def _refresh_dashboard(db):
    """Refresh dashboard stats from stored counters on locations table."""
    loc_agg_rows, recent_scans_rows = await asyncio.gather(
        db.execute_fetchall(
            "SELECT COUNT(*) as c, COALESCE(SUM(total_size), 0) as s, "
            "COALESCE(SUM(file_count), 0) as fc, "
            "COALESCE(SUM(duplicate_count), 0) as dc "
            "FROM locations"
        ),
        db.execute_fetchall(
            """SELECT s.id, l.name as location_name, s.status, s.started_at,
                      s.completed_at, s.files_found, s.files_hashed, s.duplicates_found
               FROM scans s
               JOIN locations l ON l.id = s.location_id
               ORDER BY s.started_at DESC
               LIMIT 5"""
        ),
    )

    total_locations = loc_agg_rows[0]["c"]
    total_size = loc_agg_rows[0]["s"]
    total_files = loc_agg_rows[0]["fc"]
    dup_count = loc_agg_rows[0]["dc"]

    # Aggregate type_counts from all locations
    loc_type_rows = await db.execute_fetchall(
        "SELECT type_counts FROM locations WHERE type_counts != '{}'"
    )
    merged_types: dict[str, int] = {}
    for r in loc_type_rows:
        for ftype, cnt in json.loads(r["type_counts"] or "{}").items():
            merged_types[ftype] = merged_types.get(ftype, 0) + cnt
    type_breakdown = sorted(
        [{"type": t, "count": c} for t, c in merged_types.items()],
        key=lambda x: x["count"],
        reverse=True,
    )

    recent_scans = [
        {
            "id": r["id"],
            "location": r["location_name"],
            "status": r["status"],
            "startedAt": r["started_at"],
            "completedAt": r["completed_at"],
            "filesFound": r["files_found"],
            "filesHashed": r["files_hashed"],
            "duplicatesFound": r["duplicates_found"],
        }
        for r in recent_scans_rows
    ]

    from file_hunter.services.deferred_ops import get_pending_ops_count

    pending_ops = await get_pending_ops_count(db)

    _cache["dashboard"] = {
        "totalFiles": total_files,
        "totalLocations": total_locations,
        "duplicateFiles": dup_count,
        "totalSize": total_size,
        "totalSizeFormatted": format_size(total_size),
        "typeBreakdown": type_breakdown,
        "recentScans": recent_scans,
        "pendingOps": pending_ops,
    }


async def _refresh_all_locations(db):
    """Refresh cached stats for all locations from stored counters."""
    loc_rows = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, "
        "total_size, file_count, duplicate_count, hidden_count, type_counts, "
        "scan_schedule_enabled, scan_schedule_days, scan_schedule_time, "
        "scan_schedule_last_run FROM locations"
    )
    for loc in loc_rows:
        await _refresh_location(db, loc)


async def _refresh_location(db, loc):
    """Refresh cached stats for a single location using stored counters."""
    location_id = loc["id"]

    folder_rows = await db.execute_fetchall(
        "SELECT COUNT(*) as c FROM folders WHERE location_id = ?",
        (location_id,),
    )

    file_count = loc["file_count"] or 0
    total_size = loc["total_size"] or 0
    dup_count = loc["duplicate_count"] or 0
    folder_count = folder_rows[0]["c"]

    tc = json.loads(loc["type_counts"] or "{}")
    type_breakdown = sorted(
        [{"type": t, "count": c} for t, c in tc.items()],
        key=lambda x: x["count"],
        reverse=True,
    )

    days_str = loc["scan_schedule_days"] or ""
    schedule_days = [int(d) for d in days_str.split(",") if d.strip()]

    _cache[f"loc:{location_id}"] = {
        "name": loc["name"],
        "rootPath": loc["root_path"],
        "dateAdded": loc["date_added"],
        "fileCount": file_count,
        "folderCount": folder_count,
        "totalSize": total_size,
        "totalSizeFormatted": format_size(total_size),
        "duplicateFiles": dup_count,
        "hiddenFiles": loc["hidden_count"] or 0,
        "typeBreakdown": type_breakdown,
        "scheduleEnabled": bool(loc["scan_schedule_enabled"]),
        "scheduleDays": schedule_days,
        "scheduleTime": loc["scan_schedule_time"] or "03:00",
        "scheduleLastRun": loc["scan_schedule_last_run"],
    }


async def get_stats(db):
    """Return dashboard stats. Serves from cache if available.

    On cache miss, returns fast partial data from the locations table
    (which has stored counts) and skips the expensive files-table queries.
    The background refresh will fill in complete data shortly.
    """
    cached = _cache.get("dashboard")
    if cached is not None:
        return cached

    # Cache miss — all four counters are stored on locations, so this is fast.
    loc_agg_rows, loc_type_rows, recent_scans_rows = await asyncio.gather(
        db.execute_fetchall(
            "SELECT COUNT(*) as c, COALESCE(SUM(total_size), 0) as s, "
            "COALESCE(SUM(file_count), 0) as fc, "
            "COALESCE(SUM(duplicate_count), 0) as dc "
            "FROM locations"
        ),
        db.execute_fetchall(
            "SELECT type_counts FROM locations WHERE type_counts != '{}'"
        ),
        db.execute_fetchall(
            """SELECT s.id, l.name as location_name, s.status, s.started_at,
                      s.completed_at, s.files_found, s.files_hashed, s.duplicates_found
               FROM scans s
               JOIN locations l ON l.id = s.location_id
               ORDER BY s.started_at DESC
               LIMIT 5"""
        ),
    )

    merged_types: dict[str, int] = {}
    for r in loc_type_rows:
        for ftype, cnt in json.loads(r["type_counts"] or "{}").items():
            merged_types[ftype] = merged_types.get(ftype, 0) + cnt
    type_breakdown = sorted(
        [{"type": t, "count": c} for t, c in merged_types.items()],
        key=lambda x: x["count"],
        reverse=True,
    )

    recent_scans = [
        {
            "id": r["id"],
            "location": r["location_name"],
            "status": r["status"],
            "startedAt": r["started_at"],
            "completedAt": r["completed_at"],
            "filesFound": r["files_found"],
            "filesHashed": r["files_hashed"],
            "duplicatesFound": r["duplicates_found"],
        }
        for r in recent_scans_rows
    ]

    from file_hunter.services.deferred_ops import get_pending_ops_count

    pending_ops = await get_pending_ops_count(db)

    total_files = loc_agg_rows[0]["fc"]
    total_size = loc_agg_rows[0]["s"]
    return {
        "totalFiles": total_files,
        "totalLocations": loc_agg_rows[0]["c"],
        "duplicateFiles": loc_agg_rows[0]["dc"],
        "totalSize": total_size,
        "totalSizeFormatted": format_size(total_size),
        "typeBreakdown": type_breakdown,
        "recentScans": recent_scans,
        "pendingOps": pending_ops,
    }


async def get_location_stats(db, location_id: int):
    """Return stats for a single location."""
    cache_key = f"loc:{location_id}"
    cached = _cache.get(cache_key)

    if cached is not None:
        # online status must always be live — a drive could be unplugged at any moment
        root_path = cached["rootPath"]
        online = await asyncio.to_thread(check_location_online, location_id, root_path)
        # Disk stats are live too — capacity changes between requests
        disk_stats = await get_disk_stats(location_id, root_path) if online else None
        # dateLastScanned and schedule fields change after scans without affecting stats
        live_row = await db.execute_fetchall(
            "SELECT date_last_scanned, scan_schedule_enabled, scan_schedule_days, "
            "scan_schedule_time, scan_schedule_last_run "
            "FROM locations WHERE id = ?",
            (location_id,),
        )
        if live_row:
            lr = live_row[0]
            days_str = lr["scan_schedule_days"] or ""
            sched_days = [int(d) for d in days_str.split(",") if d.strip()]
            result = {
                **cached,
                "online": online,
                "diskStats": disk_stats,
                "dateLastScanned": lr["date_last_scanned"],
                "lastScanStatus": None,
                "scheduleEnabled": bool(lr["scan_schedule_enabled"]),
                "scheduleDays": sched_days,
                "scheduleTime": lr["scan_schedule_time"] or "03:00",
                "scheduleLastRun": lr["scan_schedule_last_run"],
            }
            if not lr["date_last_scanned"]:
                last_scan = await db.execute_fetchall(
                    "SELECT status, started_at, completed_at FROM scans "
                    "WHERE location_id = ? ORDER BY started_at DESC LIMIT 1",
                    (location_id,),
                )
                if last_scan:
                    result["lastScanStatus"] = last_scan[0]["status"]
                    result["dateLastScanned"] = (
                        last_scan[0]["completed_at"] or last_scan[0]["started_at"]
                    )
            return result
        return {
            **cached,
            "online": online,
            "diskStats": disk_stats,
            "dateLastScanned": None,
            "lastScanStatus": None,
        }

    # Cache miss — all four counters stored on locations table, so this is fast.
    loc_row = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, date_last_scanned, "
        "total_size, file_count, duplicate_count, hidden_count, type_counts, "
        "scan_schedule_enabled, scan_schedule_days, scan_schedule_time, "
        "scan_schedule_last_run FROM locations WHERE id = ?",
        (location_id,),
    )
    if not loc_row:
        return None
    loc = loc_row[0]

    folder_rows, online = await asyncio.gather(
        db.execute_fetchall(
            "SELECT COUNT(*) as c FROM folders WHERE location_id = ?",
            (location_id,),
        ),
        asyncio.to_thread(check_location_online, location_id, loc["root_path"]),
    )

    disk_stats = await get_disk_stats(location_id, loc["root_path"]) if online else None

    days_str = loc["scan_schedule_days"] or ""
    schedule_days = [int(d) for d in days_str.split(",") if d.strip()]

    tc = json.loads(loc["type_counts"] or "{}")
    type_breakdown = sorted(
        [{"type": t, "count": c} for t, c in tc.items()],
        key=lambda x: x["count"],
        reverse=True,
    )

    result = {
        "name": loc["name"],
        "rootPath": loc["root_path"],
        "dateAdded": loc["date_added"],
        "fileCount": loc["file_count"] or 0,
        "folderCount": folder_rows[0]["c"],
        "totalSize": loc["total_size"] or 0,
        "totalSizeFormatted": format_size(loc["total_size"] or 0),
        "duplicateFiles": loc["duplicate_count"] or 0,
        "hiddenFiles": loc["hidden_count"] or 0,
        "typeBreakdown": type_breakdown,
        "scheduleEnabled": bool(loc["scan_schedule_enabled"]),
        "scheduleDays": schedule_days,
        "scheduleTime": loc["scan_schedule_time"] or "03:00",
        "scheduleLastRun": loc["scan_schedule_last_run"],
        "online": online,
        "diskStats": disk_stats,
        "dateLastScanned": loc["date_last_scanned"],
        "lastScanStatus": None,
    }

    # If date_last_scanned is NULL, check the scans table for the most recent attempt
    if not loc["date_last_scanned"]:
        last_scan = await db.execute_fetchall(
            "SELECT status, started_at, completed_at FROM scans "
            "WHERE location_id = ? ORDER BY started_at DESC LIMIT 1",
            (location_id,),
        )
        if last_scan:
            result["lastScanStatus"] = last_scan[0]["status"]
            result["dateLastScanned"] = (
                last_scan[0]["completed_at"] or last_scan[0]["started_at"]
            )

    return result


async def get_folder_stats(db, folder_id: int):
    """Return stats for a folder (including all descendants)."""
    cache_key = f"folder:{folder_id}"
    cached = _cache.get(cache_key)

    if cached is not None:
        # locationOnline and online must always be live
        root_path = cached["_root_path"]
        loc_id_num = cached["_location_id"]
        loc_online = await asyncio.to_thread(
            check_location_online, loc_id_num, root_path
        )
        # All locations are agent-backed — if agent is online, folder is online
        folder_online = loc_online
        result = {k: v for k, v in cached.items() if not k.startswith("_")}
        result["locationOnline"] = loc_online
        result["online"] = folder_online
        return result

    # Folder metadata — includes stored counters (duplicate_count is cumulative)
    folder_row = await db.execute_fetchall(
        """SELECT f.id, f.name, f.rel_path, f.location_id,
                  f.total_size, f.file_count, f.duplicate_count, f.hidden_count,
                  f.dup_exclude,
                  l.name as location_name, l.root_path as location_root_path
           FROM folders f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (folder_id,),
    )
    if not folder_row:
        return None
    fld = folder_row[0]

    # Run remaining queries concurrently — no recursive CTE against files needed
    (
        subfolder_rows,
        chain_rows,
        loc_online,
    ) = await asyncio.gather(
        db.execute_fetchall(
            """WITH RECURSIVE descendants(id) AS (
                       SELECT fo.id FROM folders fo WHERE fo.parent_id = ?
                       UNION ALL
                       SELECT fo.id FROM folders fo JOIN descendants d ON fo.parent_id = d.id
                   )
                   SELECT COUNT(*) as c FROM descendants""",
            (folder_id,),
        ),
        db.execute_fetchall(
            """WITH RECURSIVE chain(id, name, parent_id, depth) AS (
                       SELECT id, name, parent_id, 0 FROM folders WHERE id = ?
                       UNION ALL
                       SELECT f.id, f.name, f.parent_id, c.depth + 1
                       FROM folders f JOIN chain c ON f.id = c.parent_id
                   )
                   SELECT id, name FROM chain ORDER BY depth DESC""",
            (folder_id,),
        ),
        asyncio.to_thread(
            check_location_online, fld["location_id"], fld["location_root_path"]
        ),
    )

    dup_count = fld["duplicate_count"] or 0
    subfolder_count = subfolder_rows[0]["c"]

    # Build breadcrumb from chain query results
    breadcrumb = [{"nodeId": f"loc-{fld['location_id']}", "name": fld["location_name"]}]
    breadcrumb.extend(
        {"nodeId": f"fld-{r['id']}", "name": r["name"]} for r in chain_rows
    )

    # All locations are agent-backed — if agent is online, folder is online
    folder_online = loc_online

    # Cache everything except live-computed locationOnline
    # _root_path is a private field used to compute online status on cache hit
    cached_result = {
        "name": fld["name"],
        "relPath": fld["rel_path"],
        "location": fld["location_name"],
        "locationId": f"loc-{fld['location_id']}",
        "fileCount": fld["file_count"] or 0,
        "totalSize": fld["total_size"] or 0,
        "totalSizeFormatted": format_size(fld["total_size"] or 0),
        "duplicateFiles": dup_count,
        "hiddenFiles": fld["hidden_count"] or 0,
        "subfolderCount": subfolder_count,
        "dupExcluded": bool(fld["dup_exclude"]),
        "breadcrumb": breadcrumb,
        "_root_path": fld["location_root_path"],
        "_location_id": fld["location_id"],
    }
    _cache[cache_key] = cached_result

    # Return without the private underscore fields
    result = {k: v for k, v in cached_result.items() if not k.startswith("_")}
    result["locationOnline"] = loc_online
    result["online"] = folder_online
    return result
