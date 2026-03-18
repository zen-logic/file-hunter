"""Rehash partial hashes for a location after PARTIAL_SIZE change.

Queries files with existing hash_partial, sends batches to the agent
for recomputation, and writes results back via db_writer().
Keyset pagination via last_id cursor. Persists cursor to operation
params for resume on restart.
"""

import asyncio
import logging

from file_hunter.db import db_writer, read_db
from file_hunter.services.agent_ops import hash_partial_batch
from file_hunter.services.queue_manager import update_params
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")

BATCH_SIZE = 500


async def run_rehash_partial(op_id: int, agent_id: int, params: dict):
    """Rehash all hash_partial values for a single location."""
    location_id = params["location_id"]
    location_name = params.get("location_name", f"location {location_id}")
    last_id = params.get("last_id", 0)

    async with read_db() as db:
        # Count total candidates for progress
        count_row = await db.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM files "
            "WHERE location_id = ? AND id > ? AND hash_partial IS NOT NULL "
            "AND stale = 0 AND file_size > 0",
            (location_id, last_id),
        )
        remaining = count_row[0]["cnt"] if count_row else 0

        # Also count already-done (for accurate progress after resume)
        done_row = await db.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM files "
            "WHERE location_id = ? AND id <= ? AND hash_partial IS NOT NULL "
            "AND stale = 0 AND file_size > 0",
            (location_id, last_id),
        )
    already_done = done_row[0]["cnt"] if done_row else 0
    total = already_done + remaining

    logger.info(
        "Rehash partial: starting %s (location %d), %d files remaining, %d total",
        location_name,
        location_id,
        remaining,
        total,
    )

    await broadcast(
        {
            "type": "rehash_progress",
            "locationId": location_id,
            "location": location_name,
            "filesProcessed": already_done,
            "totalFiles": total,
        }
    )

    files_processed = already_done
    errors_total = 0

    while True:
        await asyncio.sleep(0)  # yield to event loop

        async with read_db() as db:
            rows = await db.execute_fetchall(
                "SELECT id, full_path FROM files "
                "WHERE location_id = ? AND id > ? AND hash_partial IS NOT NULL "
                "AND stale = 0 AND file_size > 0 "
                "ORDER BY id LIMIT ?",
                (location_id, last_id, BATCH_SIZE),
            )

        if not rows:
            break

        paths = [r["full_path"] for r in rows]
        file_ids = {r["full_path"]: r["id"] for r in rows}

        result = await hash_partial_batch(agent_id, paths)

        # Write results
        async with db_writer() as wdb:
            for item in result.get("results", []):
                fid = file_ids.get(item["path"])
                if fid:
                    await wdb.execute(
                        "UPDATE files SET hash_partial = ? WHERE id = ?",
                        (item["hash_partial"], fid),
                    )

        batch_errors = len(result.get("errors", []))
        errors_total += batch_errors
        for err in result.get("errors", []):
            logger.warning("Rehash partial error: %s: %s", err["path"], err["error"])

        files_processed += len(rows)
        last_id = rows[-1]["id"]

        # Persist cursor for resume
        params["last_id"] = last_id
        await update_params(op_id, params)

        await broadcast(
            {
                "type": "rehash_progress",
                "locationId": location_id,
                "location": location_name,
                "filesProcessed": files_processed,
                "totalFiles": total,
            }
        )

    # Set backfill_needed so dup detection picks up the new groupings
    async with db_writer() as wdb:
        await wdb.execute(
            "UPDATE locations SET backfill_needed = 1 WHERE id = ?",
            (location_id,),
        )

    logger.info(
        "Rehash partial: completed %s (location %d), %d files, %d errors",
        location_name,
        location_id,
        files_processed,
        errors_total,
    )

    await broadcast(
        {
            "type": "rehash_progress",
            "locationId": location_id,
            "location": location_name,
            "filesProcessed": files_processed,
            "totalFiles": total,
            "completed": True,
        }
    )
