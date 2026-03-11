"""Per-location scheduled scan loop.

Wakes every 60 seconds and checks if any location with an enabled schedule
is due for a scan. Uses the existing scan queue so scheduled scans are
indistinguishable from manual ones (full WebSocket progress, status bar, etc.).

All scanning is agent-based — locations are enqueued and the queue handles
triggering the appropriate agent.
"""

import asyncio
from datetime import datetime

from file_hunter.db import get_db, execute_write


async def start_scheduler():
    """Start the scheduler loop. Call from on_startup."""
    asyncio.create_task(_scheduler_loop())


async def _scheduler_loop():
    """Wake every 60s, check if any location is due for a scheduled scan."""
    while True:
        await asyncio.sleep(60)
        try:
            await _check_schedules()
        except Exception:
            pass  # don't crash the loop


async def _check_schedules():
    db = await get_db()
    now = datetime.now()
    current_day = now.weekday()  # 0=Mon
    current_time = now.strftime("%H:%M")

    rows = await db.execute_fetchall(
        "SELECT id, name, root_path, scan_schedule_days, scan_schedule_time, "
        "scan_schedule_last_run "
        "FROM locations WHERE scan_schedule_enabled = 1"
    )

    for row in rows:
        days_str = row["scan_schedule_days"]
        if not days_str or not days_str.strip():
            continue
        days = [int(d) for d in days_str.split(",") if d.strip()]
        if current_day not in days:
            continue
        if current_time < row["scan_schedule_time"]:
            continue
        if _already_ran_today(row["scan_schedule_last_run"], now):
            continue

        try:
            loc_row = await db.execute_fetchall(
                "SELECT agent_id FROM locations WHERE id = ?", (row["id"],)
            )
            agent_id = loc_row[0]["agent_id"] if loc_row else None
            if not agent_id:
                continue

            from file_hunter.services.queue_manager import enqueue, get_queue_status

            # Skip if already queued/running
            status = await get_queue_status()
            if any(item.get("location_id") == row["id"] for item in status):
                continue

            await enqueue(
                "scan_dir",
                agent_id,
                {
                    "location_id": row["id"],
                    "path": row["root_path"],
                    "root_path": row["root_path"],
                },
            )
            await _update_last_run(row["id"], now)
        except Exception:
            pass  # don't crash the loop


def _already_ran_today(last_run_iso, now):
    if not last_run_iso:
        return False
    try:
        last_run = datetime.fromisoformat(last_run_iso)
        return last_run.date() == now.date()
    except (ValueError, TypeError):
        return False


async def _update_last_run(location_id, now):
    async def _write(conn, lid, ts):
        await conn.execute(
            "UPDATE locations SET scan_schedule_last_run = ? WHERE id = ?",
            (ts, lid),
        )
        await conn.commit()

    await execute_write(_write, location_id, now.isoformat(timespec="seconds"))
