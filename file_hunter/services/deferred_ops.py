"""Deferred file operations — queue, drain, cancel for offline locations."""

import json
import logging

from file_hunter.core import classify_file
from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import (
    get_file_hashes,
    hashes_writer,
    update_file_hash,
)
from file_hunter.helpers import parse_mtime, post_op_stats, utc_now
from file_hunter.services import fs
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast

logger = logging.getLogger("file_hunter")


async def queue_deferred_op(
    db, file_id: int, location_id: int, op_type: str, params: dict | None = None
):
    """Insert a pending_file_ops row and set pending_op on the file.

    Must be called inside db_writer() — db is the write connection.
    """
    now = utc_now()
    await db.execute(
        "INSERT INTO pending_file_ops (op_type, file_id, location_id, params, status, date_created) "
        "VALUES (?, ?, ?, ?, 'pending', ?)",
        (op_type, file_id, location_id, json.dumps(params or {}), now),
    )
    await db.execute(
        "UPDATE files SET pending_op = ? WHERE id = ?",
        (op_type, file_id),
    )


async def cancel_pending_op(file_id: int):
    """Cancel a pending operation — clear pending_op and delete the queue row."""
    async with db_writer() as db:
        await db.execute(
            "DELETE FROM pending_file_ops WHERE file_id = ? AND status = 'pending'",
            (file_id,),
        )
        await db.execute(
            "UPDATE files SET pending_op = NULL WHERE id = ?",
            (file_id,),
        )

    await post_op_stats()

    await broadcast({"type": "deferred_op_cancelled", "fileId": file_id})


async def complete_op(db, op_id, now_iso):
    """Mark a pending file op completed, on the caller's write connection."""
    await db.execute(
        "UPDATE pending_file_ops SET status = 'completed', "
        "date_completed = ? WHERE id = ?",
        (now_iso, op_id),
    )


async def deferred_destination(f, params, kind):
    """(dst_full_path, dst_rel_path, dst_location_id, dst_folder_id,
    dst_filename) for a deferred move or copy of f. ValueError if the params
    have no destination, the source is gone from disk, or the destination
    already exists."""
    dst_full_path = params.get("dst_full_path")
    dst_rel_path = params.get("dst_rel_path")
    dst_location_id = params.get("dst_location_id", f["location_id"])
    if not dst_full_path or not dst_rel_path:
        raise ValueError(f"Missing destination path in deferred {kind} params")
    if not await fs.file_exists(f["full_path"], f["location_id"]):
        raise ValueError(f"Source file not found on disk: {f['full_path']}")
    if await fs.path_exists(dst_full_path, dst_location_id):
        raise ValueError(f"Destination already exists: {dst_full_path}")
    return (
        dst_full_path,
        dst_rel_path,
        dst_location_id,
        params.get("dst_folder_id"),
        params.get("dst_filename", f["filename"]),
    )


async def drain_pending_ops(location_id: int, root_path: str):
    """Execute pending file ops for a location that's now online.
    """
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT id, op_type, file_id, params FROM pending_file_ops "
            "WHERE location_id = ? AND status = 'pending'",
            (location_id,),
        )

    if not rows:
        return

    logger.info("Draining %d pending file ops for location #%d", len(rows), location_id)

    now_iso = utc_now()
    completed = 0
    failed = 0
    affected_strong = set()
    affected_fast = set()
    affected_location_ids = {location_id}

    for op in rows:
        op_id = op["id"]
        op_type = op["op_type"]
        file_id = op["file_id"]
        params = json.loads(op["params"] or "{}")

        try:
            # Re-read file — it may have been cancelled or changed
            async with read_db() as db:
                file_row = await db.execute_fetchall(
                    "SELECT id, full_path, filename, "
                    "location_id, folder_id, file_size, file_type_high, hidden, "
                    "modified_date, pending_op FROM files WHERE id = ?",
                    (file_id,),
                )
            if not file_row or file_row[0]["pending_op"] != op_type:
                # File gone, or op cancelled or changed: nothing to do
                async with db_writer() as wdb:
                    await complete_op(wdb, op_id, now_iso)
                completed += 1
                continue

            f = file_row[0]

            if op_type == "delete":
                await drain_delete(f, op_id, now_iso)
                h = (await get_file_hashes([file_id])).get(file_id, {})
                if h.get("hash_strong"):
                    affected_strong.add(h["hash_strong"])
                elif h.get("hash_fast"):
                    affected_fast.add(h["hash_fast"])

            elif op_type == "move":
                dst_location_id = await drain_move(f, params, op_id, now_iso)
                if dst_location_id and dst_location_id != location_id:
                    affected_location_ids.add(dst_location_id)

            elif op_type == "copy":
                dst_location_id = await drain_copy(f, params, op_id, now_iso)
                if dst_location_id and dst_location_id != location_id:
                    affected_location_ids.add(dst_location_id)

            elif op_type == "verify":
                result = await drain_verify(f, op_id, now_iso)
                if result:
                    affected_strong.add(result["hash_strong"])
                    if result.get("old_hash_fast"):
                        affected_fast.add(result["old_hash_fast"])

            completed += 1

        except Exception as exc:
            logger.warning(
                "Deferred op #%d (%s) for file #%d failed: %s",
                op_id,
                op_type,
                file_id,
                exc,
            )
            async with db_writer() as wdb:
                await wdb.execute(
                    "UPDATE pending_file_ops SET status = 'failed', "
                    "error = ?, date_completed = ? WHERE id = ?",
                    (str(exc), now_iso, op_id),
                )
            failed += 1

    # Post-drain: recalc sizes and dup counts
    await post_op_stats(
        location_ids=affected_location_ids,
        strong_hashes=affected_strong or None,
        fast_hashes=affected_fast or None,
        source=f"drain_pending_ops loc#{location_id}",
    )

    logger.info(
        "Drained pending ops for location #%d: %d completed, %d failed",
        location_id,
        completed,
        failed,
    )

    await broadcast(
        {
            "type": "deferred_ops_drained",
            "locationId": location_id,
            "completed": completed,
            "failed": failed,
        }
    )


async def drain_delete(f, op_id: int, now_iso: str):
    """Execute a deferred delete: remove from disk, then remove DB record."""
    file_id = f["id"]
    full_path = f["full_path"]
    location_id = f["location_id"]

    if await fs.file_exists(full_path, location_id):
        await fs.file_delete(full_path, location_id)

    async with db_writer() as wdb:
        await wdb.execute("DELETE FROM files WHERE id = ?", (file_id,))
        await complete_op(wdb, op_id, now_iso)

    # delete imports this module
    from file_hunter.services.delete import settle_deleted_files

    await settle_deleted_files([f])


async def drain_move(f, params: dict, op_id: int, now_iso: str) -> int | None:
    """Execute a deferred move: move on disk, update DB record.

    Returns destination location_id if different from source, else None.
    """
    file_id = f["id"]
    full_path = f["full_path"]
    location_id = f["location_id"]
    (
        dst_full_path, dst_rel_path, dst_location_id, dst_folder_id, dst_filename
    ) = await deferred_destination(f, params, "move")

    # Execute the move
    cross_location = dst_location_id != location_id
    if cross_location:
        # Check if both locations are on the same agent
        async with read_db() as rdb:
            src_row = await rdb.execute_fetchall(
                "SELECT agent_id FROM locations WHERE id = ?", (location_id,)
            )
            dst_row = await rdb.execute_fetchall(
                "SELECT agent_id FROM locations WHERE id = ?", (dst_location_id,)
            )
        src_agent = src_row[0]["agent_id"] if src_row else None
        dst_agent = dst_row[0]["agent_id"] if dst_row else None
        same_agent = src_agent is not None and src_agent == dst_agent

        if same_agent:
            await fs.file_move(full_path, dst_full_path, location_id)
        else:
            await fs.copy_file(
                full_path,
                location_id,
                dst_full_path,
                dst_location_id,
                mtime=parse_mtime(f["modified_date"]),
            )
            await fs.file_delete(full_path, location_id)
    else:
        await fs.file_move(full_path, dst_full_path, location_id)

    # Reclassify if name changed
    new_type_high, new_type_low = classify_file(dst_filename)

    async with db_writer() as wdb:
        await wdb.execute(
            """UPDATE files SET filename = ?, full_path = ?, rel_path = ?,
                  location_id = ?, folder_id = ?,
                  file_type_high = ?, file_type_low = ?,
                  pending_op = NULL
               WHERE id = ?""",
            (
                dst_filename,
                dst_full_path,
                dst_rel_path,
                dst_location_id,
                dst_folder_id,
                new_type_high,
                new_type_low,
                file_id,
            ),
        )
        await complete_op(wdb, op_id, now_iso)

    # Sync hashes.db location_id for cross-location moves
    if cross_location:
        async with hashes_writer() as hdb:
            await hdb.execute(
                "UPDATE file_hashes SET location_id = ? WHERE file_id = ?",
                (dst_location_id, file_id),
            )

    # Update folder stats: file left source folder, arrived at dest folder
    if dst_folder_id != f["folder_id"]:
        file_size = f["file_size"] or 0
        old_type = f["file_type_high"]
        hidden = f["hidden"]
        await update_stats_for_files(
            location_id,
            removed=[(f["folder_id"], file_size, old_type, hidden)],
        )
        await update_stats_for_files(
            dst_location_id,
            added=[(dst_folder_id, file_size, new_type_high, hidden)],
        )

    return dst_location_id if cross_location else None


async def drain_copy(f, params: dict, op_id: int, now_iso: str) -> int | None:
    """Execute a deferred copy: copy file on disk, insert new DB record.

    Returns destination location_id if different from source, else None.
    Source file and catalog entry are left untouched.
    """
    from file_hunter.services.files import insert_file_copy

    file_id = f["id"]
    full_path = f["full_path"]
    location_id = f["location_id"]
    (
        dst_full_path, dst_rel_path, dst_location_id, dst_folder_id, dst_filename
    ) = await deferred_destination(f, params, "copy")

    # Copy on disk
    await fs.copy_file(
        full_path, location_id,
        dst_full_path, dst_location_id,
        mtime=parse_mtime(f["modified_date"]),
    )

    # Read source file's metadata — tags come from the source's own
    # associations inside insert_file_copy, not from this row.
    async with read_db() as rdb:
        src_row = await rdb.execute_fetchall(
            "SELECT file_size, description, created_date, modified_date FROM files WHERE id = ?",
            (file_id,),
        )
    src = dict(src_row[0]) if src_row else {"file_size": f["file_size"]}

    # Insert new catalog entry + hash copy
    async with db_writer() as wdb:
        await insert_file_copy(
            wdb,
            source_file_id=file_id,
            source_row=src,
            filename=dst_filename,
            full_path=dst_full_path,
            rel_path=dst_rel_path,
            location_id=dst_location_id,
            folder_id=dst_folder_id,
        )

        # Clear pending_op on source (it was only set for queueing)
        await wdb.execute(
            "UPDATE files SET pending_op = NULL WHERE id = ?", (file_id,)
        )
        await complete_op(wdb, op_id, now_iso)

    # Update destination folder stats
    new_type_high, _ = classify_file(dst_filename)
    await update_stats_for_files(
        dst_location_id,
        added=[(dst_folder_id, f["file_size"] or 0, new_type_high, f["hidden"])],
    )

    return dst_location_id if dst_location_id != location_id else None


async def drain_verify(f, op_id: int, now_iso: str) -> dict | None:
    """Execute a deferred verify: compute SHA-256, update DB record.

    Returns {"hash_strong": ..., "old_hash_fast": ...} on success, None on skip.
    """
    file_id = f["id"]
    full_path = f["full_path"]
    location_id = f["location_id"]

    h = (await get_file_hashes([file_id])).get(file_id, {})
    old_hash_fast = h.get("hash_fast")

    if h.get("hash_strong"):
        # Already verified — just clear pending_op
        async with db_writer() as wdb:
            await wdb.execute(
                "UPDATE files SET pending_op = NULL WHERE id = ?", (file_id,)
            )
            await complete_op(wdb, op_id, now_iso)
        return None

    if not await fs.file_exists(full_path, location_id):
        raise ValueError(f"File not found on disk: {full_path}")

    hash_fast, hash_strong = await fs.file_hash(full_path, location_id, strong=True)

    async with db_writer() as wdb:
        await wdb.execute(
            "UPDATE files SET pending_op = NULL WHERE id = ?",
            (file_id,),
        )
        await complete_op(wdb, op_id, now_iso)

    await update_file_hash(file_id, hash_fast=hash_fast, hash_strong=hash_strong)

    await broadcast(
        {
            "type": "file_verified",
            "fileId": file_id,
            "filename": f["filename"],
            "hashStrong": hash_strong,
        }
    )

    return {"hash_strong": hash_strong, "old_hash_fast": old_hash_fast}


async def get_pending_ops_count(db) -> int:
    """Return count of pending file ops across all locations."""
    row = await db.execute_fetchall(
        "SELECT COUNT(*) as cnt FROM pending_file_ops WHERE status = 'pending'"
    )
    return row[0]["cnt"] if row else 0
