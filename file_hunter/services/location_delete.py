"""Queued location deletion — agent-first, then catalog cleanup.

Runs as a queue_manager operation so it survives server/agent restarts.
The agent must confirm deletion from its config before the server
removes anything from the catalog.
"""

import logging

from file_hunter.db import execute_write
from file_hunter.services.agent_ops import delete_agent_location, invalidate_loc_cache
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def run_delete_location(op_id: int, agent_id: int | None, params: dict):
    """Execute a location delete operation.

    1. Tell agent to remove location from its config
    2. Agent confirms (or agent is None for local locations)
    3. Clean up catalog: files, folders, scans, queue entries, backfills
    4. Broadcast completion
    """
    location_id = params["location_id"]
    location_name = params.get("location_name", "")
    root_path = params.get("root_path", "")

    # Step 1: Tell agent to delete (if agent-backed)
    if agent_id is not None:
        await delete_agent_location(agent_id, root_path, location_id=location_id)
        logger.info(
            "Agent #%d confirmed delete of '%s' (location %d)",
            agent_id,
            location_name,
            location_id,
        )

    # Step 2: Clean up catalog
    await execute_write(_purge_location, location_id)

    # Step 3: Clean up in-memory state
    invalidate_loc_cache(location_id)
    invalidate_stats_cache()

    # Step 4: Broadcast completion
    await broadcast(
        {
            "type": "location_deleted",
            "locationId": f"loc-{location_id}",
            "name": location_name,
        }
    )

    logger.info(
        "Location deleted: #%d '%s'",
        location_id,
        location_name,
    )


async def _purge_location(db, location_id: int):
    """Remove all traces of a location from the database."""
    # Operation queue entries (params is JSON, no FK cascade)
    await db.execute(
        "DELETE FROM operation_queue WHERE params LIKE ?",
        (f'%"location_id": {location_id}%',),
    )

    # Pending backfills (no FK cascade)
    await db.execute(
        "DELETE FROM pending_backfills WHERE location_id = ?", (location_id,)
    )

    # Pending hashes (no FK cascade)
    await db.execute(
        "DELETE FROM pending_hashes WHERE location_id = ?", (location_id,)
    )

    # Location row (CASCADE handles files, folders, scans, consolidation_jobs, ignored_files)
    await db.execute("DELETE FROM locations WHERE id = ?", (location_id,))
    await db.commit()
