"""Rehash partial hashes for a location after PARTIAL_SIZE change.

Queries files with existing hash_partial from hashes.db, sends batches
to the agent for recomputation, and writes results back to hashes.db.
Keyset pagination via last_id cursor. Persists cursor to operation
params for resume on restart.
"""

import asyncio
import logging

from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import hashes_writer, open_hashes_connection
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

    conn = await open_hashes_connection()
    try:
        count_row = await conn.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM file_hashes "
            "WHERE location_id = ? AND file_id > ? AND hash_partial IS NOT NULL "
            "AND stale = 0 AND file_size > 0",
            (location_id, last_id),
        )
        remaining = count_row[0]["cnt"] if count_row else 0

        done_row = await conn.execute_fetchall(
            "SELECT COUNT(*) as cnt FROM file_hashes "
            "WHERE location_id = ? AND file_id <= ? AND hash_partial IS NOT NULL "
            "AND stale = 0 AND file_size > 0",
            (location_id, last_id),
        )
    finally:
        await conn.close()

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

    # Need full_path from catalog for agent dispatch
    while True:
        await asyncio.sleep(0)

        # Get file_ids from hashes.db, full_paths from catalog
        hconn = await open_hashes_connection()
        try:
            hrows = await hconn.execute_fetchall(
                "SELECT file_id FROM file_hashes "
                "WHERE location_id = ? AND file_id > ? AND hash_partial IS NOT NULL "
                "AND stale = 0 AND file_size > 0 "
                "ORDER BY file_id LIMIT ?",
                (location_id, last_id, BATCH_SIZE),
            )
        finally:
            await hconn.close()

        if not hrows:
            break

        file_ids = [r["file_id"] for r in hrows]
        ph = ",".join("?" for _ in file_ids)
        async with read_db() as db:
            path_rows = await db.execute_fetchall(
                f"SELECT id, full_path FROM files WHERE id IN ({ph})",
                file_ids,
            )

        paths = [r["full_path"] for r in path_rows]
        id_by_path = {r["full_path"]: r["id"] for r in path_rows}

        result = await hash_partial_batch(agent_id, paths)

        # Write results to hashes.db
        async with hashes_writer() as hdb:
            for item in result.get("results", []):
                fid = id_by_path.get(item["path"])
                if fid:
                    await hdb.execute(
                        "UPDATE file_hashes SET hash_partial = ? WHERE file_id = ?",
                        (item["hash_partial"], fid),
                    )

        batch_errors = len(result.get("errors", []))
        errors_total += batch_errors
        for err in result.get("errors", []):
            logger.warning("Rehash partial error: %s: %s", err["path"], err["error"])

        files_processed += len(hrows)
        last_id = hrows[-1]["file_id"]

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
