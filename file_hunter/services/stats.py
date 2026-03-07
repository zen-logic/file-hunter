"""Aggregate statistics queries with in-memory caching.

Results are cached on first access and served from cache on subsequent requests.
Any catalog mutation (scan, consolidate, merge, upload, delete, location change)
calls invalidate_stats_cache() to clear everything — this is intentionally
aggressive because duplicate counts are global.
"""

import asyncio
import os

from file_hunter.core import format_size
from file_hunter.extensions import is_agent_location, get_agent_status
from file_hunter.services.locations import check_location_online, get_disk_stats

# Module-level cache: key → cached result dict
_cache: dict[str, dict] = {}


def invalidate_stats_cache():
    """Clear all cached stats. Called after any catalog mutation."""
    _cache.clear()


async def get_stats(db):
    """Return dashboard stats."""
    cached = _cache.get("dashboard")
    if cached is not None:
        return cached

    # Run all independent queries concurrently (WAL mode supports concurrent reads)
    loc_agg_rows, dup_rows, recent_scans_rows, type_rows = await asyncio.gather(
        db.execute_fetchall(
            "SELECT COUNT(*) as c, COALESCE(SUM(total_size), 0) as s, "
            "COALESCE(SUM(file_count), 0) as fc FROM locations"
        ),
        db.execute_fetchall(
            "SELECT COUNT(*) as c FROM files WHERE dup_count > 0 AND stale = 0 AND hidden = 0 AND dup_exclude = 0"
        ),
        db.execute_fetchall(
            """SELECT s.id, l.name as location_name, s.status, s.started_at,
                      s.completed_at, s.files_found, s.files_hashed, s.duplicates_found
               FROM scans s
               JOIN locations l ON l.id = s.location_id
               ORDER BY s.started_at DESC
               LIMIT 5"""
        ),
        db.execute_fetchall(
            """SELECT file_type_high, COUNT(*) as c
               FROM files WHERE stale = 0
               GROUP BY file_type_high ORDER BY c DESC"""
        ),
    )

    total_locations = loc_agg_rows[0]["c"]
    total_size = loc_agg_rows[0]["s"]
    total_files = loc_agg_rows[0]["fc"]
    dup_count = dup_rows[0]["c"]
    type_breakdown = [{"type": r["file_type_high"], "count": r["c"]} for r in type_rows]

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

    result = {
        "totalFiles": total_files,
        "totalLocations": total_locations,
        "duplicateFiles": dup_count,
        "totalSize": total_size,
        "totalSizeFormatted": format_size(total_size),
        "typeBreakdown": type_breakdown,
        "recentScans": recent_scans,
    }
    _cache["dashboard"] = result
    return result


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
        agent_status = None
        if online and is_agent_location(location_id):
            agent_status = await get_agent_status(location_id)

        if live_row:
            lr = live_row[0]
            days_str = lr["scan_schedule_days"] or ""
            sched_days = [int(d) for d in days_str.split(",") if d.strip()]
            return {
                **cached,
                "online": online,
                "diskStats": disk_stats,
                "agentStatus": agent_status,
                "dateLastScanned": lr["date_last_scanned"],
                "scheduleEnabled": bool(lr["scan_schedule_enabled"]),
                "scheduleDays": sched_days,
                "scheduleTime": lr["scan_schedule_time"] or "03:00",
                "scheduleLastRun": lr["scan_schedule_last_run"],
            }
        return {
            **cached,
            "online": online,
            "diskStats": disk_stats,
            "agentStatus": agent_status,
            "dateLastScanned": None,
        }

    # Location metadata must be fetched first (need root_path for online check,
    # and must bail early if location doesn't exist)
    loc_row = await db.execute_fetchall(
        "SELECT id, name, root_path, date_added, date_last_scanned, "
        "total_size, file_count, "
        "scan_schedule_enabled, scan_schedule_days, scan_schedule_time, "
        "scan_schedule_last_run FROM locations WHERE id = ?",
        (location_id,),
    )
    if not loc_row:
        return None
    loc = loc_row[0]

    # Run remaining queries concurrently
    dup_rows, folder_rows, type_rows, online = await asyncio.gather(
        db.execute_fetchall(
            "SELECT COUNT(*) as c FROM files WHERE location_id = ? AND dup_count > 0 AND stale = 0 AND hidden = 0 AND dup_exclude = 0",
            (location_id,),
        ),
        db.execute_fetchall(
            "SELECT COUNT(*) as c FROM folders WHERE location_id = ?",
            (location_id,),
        ),
        db.execute_fetchall(
            """SELECT file_type_high, COUNT(*) as c
               FROM files WHERE location_id = ? AND stale = 0
               GROUP BY file_type_high ORDER BY c DESC""",
            (location_id,),
        ),
        asyncio.to_thread(check_location_online, location_id, loc["root_path"]),
    )

    # Fetch disk stats only when online
    disk_stats = await get_disk_stats(location_id, loc["root_path"]) if online else None

    file_count = loc["file_count"] or 0
    total_size = loc["total_size"] or 0
    dup_count = dup_rows[0]["c"]
    folder_count = folder_rows[0]["c"]
    type_breakdown = [{"type": r["file_type_high"], "count": r["c"]} for r in type_rows]

    # Parse schedule days into list of ints
    days_str = loc["scan_schedule_days"] or ""
    schedule_days = [int(d) for d in days_str.split(",") if d.strip()]

    # Cache everything except live-computed fields
    result = {
        "name": loc["name"],
        "rootPath": loc["root_path"],
        "dateAdded": loc["date_added"],
        "fileCount": file_count,
        "folderCount": folder_count,
        "totalSize": total_size,
        "totalSizeFormatted": format_size(total_size),
        "duplicateFiles": dup_count,
        "typeBreakdown": type_breakdown,
        "scheduleEnabled": bool(loc["scan_schedule_enabled"]),
        "scheduleDays": schedule_days,
        "scheduleTime": loc["scan_schedule_time"] or "03:00",
        "scheduleLastRun": loc["scan_schedule_last_run"],
    }
    _cache[cache_key] = result

    # Agent status — live field, only for online agent-backed locations
    agent_status = None
    if online and is_agent_location(location_id):
        agent_status = await get_agent_status(location_id)

    # Merge live fields into the returned result
    return {
        **result,
        "online": online,
        "diskStats": disk_stats,
        "agentStatus": agent_status,
        "dateLastScanned": loc["date_last_scanned"],
    }


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
        if loc_online:
            from file_hunter.extensions import is_agent_location

            if is_agent_location(loc_id_num):
                folder_online = True  # agent online — assume folder exists
            else:
                folder_path = os.path.join(root_path, cached["relPath"])
                folder_online = await asyncio.to_thread(os.path.isdir, folder_path)
        else:
            folder_online = False
        result = {k: v for k, v in cached.items() if not k.startswith("_")}
        result["locationOnline"] = loc_online
        result["online"] = folder_online
        return result

    # Folder metadata must be fetched first (need location_id, root_path, etc.)
    folder_row = await db.execute_fetchall(
        """SELECT f.id, f.name, f.rel_path, f.location_id,
                  f.total_size, f.file_count, f.dup_exclude,
                  l.name as location_name, l.root_path as location_root_path
           FROM folders f JOIN locations l ON l.id = f.location_id
           WHERE f.id = ?""",
        (folder_id,),
    )
    if not folder_row:
        return None
    fld = folder_row[0]

    # Run remaining queries concurrently
    (
        dup_rows,
        subfolder_rows,
        chain_rows,
        loc_online,
    ) = await asyncio.gather(
        db.execute_fetchall(
            """WITH RECURSIVE descendants(id) AS (
                       SELECT ?
                       UNION ALL
                       SELECT fo.id FROM folders fo JOIN descendants d ON fo.parent_id = d.id
                   )
                   SELECT COUNT(*) as c FROM files f
                   WHERE f.folder_id IN (SELECT id FROM descendants)
                     AND f.dup_count > 0 AND f.stale = 0 AND f.hidden = 0 AND f.dup_exclude = 0""",
            (folder_id,),
        ),
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

    dup_count = dup_rows[0]["c"]
    subfolder_count = subfolder_rows[0]["c"]

    # Build breadcrumb from chain query results
    breadcrumb = [{"nodeId": f"loc-{fld['location_id']}", "name": fld["location_name"]}]
    breadcrumb.extend(
        {"nodeId": f"fld-{r['id']}", "name": r["name"]} for r in chain_rows
    )

    # Compute folder online status
    if loc_online:
        from file_hunter.extensions import is_agent_location

        if is_agent_location(fld["location_id"]):
            folder_online = True  # agent online — assume folder exists
        else:
            folder_path = os.path.join(fld["location_root_path"], fld["rel_path"])
            folder_online = await asyncio.to_thread(os.path.isdir, folder_path)
    else:
        folder_online = False

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
