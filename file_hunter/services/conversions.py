"""Conversions the agent runs on a file: ffmpeg transcode and dcraw raw convert.

Both are queued operations dispatched to the agent. The agent reports
progress and completion over its WebSocket; completion catalogs the output
file and releases the queue handler waiting on it. The two kinds differ only
in the names and texts held by each Conversion.
"""

import asyncio
import json
import logging
import sqlite3

from file_hunter.core import classify_file
from file_hunter.db import execute_write, read_db
from file_hunter.helpers import post_op_stats, utc_now
from file_hunter.services.activity import update as activity_update
from file_hunter.services.agent_ops import dispatch
from file_hunter.services.stats import invalidate_stats_cache
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast
from file_hunter_core.paths import norm_inode, safe_timestamp

logger = logging.getLogger("file_hunter")


class Conversion:
    def __init__(
        self,
        op_type,
        prefix,
        label,
        failed_text,
        terminal_types,
        progress_text,
        dispatch_args,
    ):
        self.op_type = op_type  # operation_queue type and agent command
        self.prefix = prefix  # WebSocket message type prefix
        self.label = label  # for log lines
        self.failed_text = failed_text
        self.terminal_types = terminal_types  # agent messages that end the job
        self.progress_text = progress_text
        self.dispatch_args = dispatch_args
        # Queue handlers waiting on the agent, keyed by source path
        self.pending: dict[str, dict] = {}

    def resolve_pending(self, path: str, result: dict):
        entry = self.pending.get(path)
        if entry:
            entry["result"] = result
            entry["event"].set()
        else:
            logger.warning("resolve_pending: no pending entry for %s", path)

    async def wait_for_completion(self, path: str):
        entry = self.pending.get(path)
        if not entry:
            return None
        try:
            await entry["event"].wait()
            return entry["result"]
        finally:
            self.pending.pop(path, None)

    async def run(self, op_id: int, agent_id: int | None, params: dict):
        """Queue handler: dispatch to the agent and wait for it to finish."""
        file_id = params["file_id"]
        path = params["path"]

        await broadcast(
            {
                "type": f"{self.prefix}_started",
                "fileId": file_id,
                "filename": params.get("filename", ""),
            }
        )

        self.pending[path] = {"event": asyncio.Event(), "result": None}

        try:
            await dispatch(
                self.op_type,
                params["location_id"],
                path=path,
                **self.dispatch_args(params),
            )
        except (ConnectionError, OSError) as e:
            self.pending.pop(path, None)
            await broadcast(
                {
                    "type": f"{self.prefix}_error",
                    "path": path,
                    "fileId": file_id,
                    "error": str(e),
                }
            )
            raise

        try:
            result = await self.wait_for_completion(path)
        except asyncio.CancelledError:
            self.pending.pop(path, None)
            raise

        if result and result.get("type") == f"{self.prefix}_error":
            # RuntimeError so the queue manager marks it failed, not re-queued
            # (OSError is caught as "agent unavailable" and retried forever)
            raise RuntimeError(result.get("error", self.failed_text))

    async def lookup_info(self, path: str) -> tuple[int | None, int | None]:
        """file_id and op_id of the running operation for a source path."""
        async with read_db() as db:
            row = await db.execute_fetchall(
                "SELECT id, params FROM operation_queue "
                "WHERE type = ? AND status = 'running' "
                "AND json_extract(params, '$.path') = ?",
                (self.op_type, path),
            )
        if row:
            params = json.loads(row[0]["params"])
            return params.get("file_id"), row[0]["id"]
        return None, None

    async def on_agent_message(self, agent_id: int, msg_type: str, msg: dict):
        """Handle a {prefix}_* message from the agent's WebSocket."""
        path = msg.get("path", "")

        if msg_type == f"{self.prefix}_complete":
            try:
                await self.catalog_output(agent_id, msg)
            except Exception as e:
                logger.exception(
                    "Agent #%d: %s_complete handler failed: %s",
                    agent_id, self.prefix, e,
                )
                # Unblock the queue so the operation can fail cleanly
                # rather than staying stuck as "running" forever
                error = f"Catalog entry failed: {e}"
                self.resolve_pending(
                    path, {"type": f"{self.prefix}_error", "error": error}
                )
                await broadcast(
                    {
                        "type": f"{self.prefix}_error",
                        "agentId": agent_id,
                        "path": path,
                        "error": error,
                    }
                )
            return

        if msg_type == f"{self.prefix}_progress":
            file_id, op_id = await self.lookup_info(path)
            if file_id:
                msg["fileId"] = file_id
            if op_id:
                activity_update(f"op-{op_id}", progress=self.progress_text(msg))
        elif msg_type in self.terminal_types:
            file_id, _ = await self.lookup_info(path)
            if file_id:
                msg["fileId"] = file_id
            self.resolve_pending(path, msg)

        msg["agentId"] = agent_id
        await broadcast(msg)

    async def catalog_output(self, agent_id: int, msg: dict):
        """Create a catalog entry for the output file, then broadcast."""
        output_path = msg.get("output", "")
        filename = msg.get("filename", "")
        size = msg.get("size", 0)
        mtime = msg.get("mtime")
        ctime = msg.get("ctime")
        inode = norm_inode(msg.get("inode") or 0)

        # The output is written next to the source, so it takes the source's
        # location and folder
        source_path = msg.get("path", "")
        async with read_db() as db:
            source_row = await db.execute_fetchall(
                "SELECT id, location_id, folder_id, hidden, dup_exclude "
                "FROM files WHERE full_path = ?",
                (source_path,),
            )
        if not source_row:
            logger.warning(
                "%s complete but source file not in catalog: %s",
                self.label, source_path,
            )
            await broadcast({**msg, "agentId": agent_id})
            return

        src = source_row[0]
        location_id = src["location_id"]
        folder_id = src["folder_id"]

        async with read_db() as db:
            loc_row = await db.execute_fetchall(
                "SELECT root_path FROM locations WHERE id = ?", (location_id,)
            )
        if not loc_row:
            await broadcast({**msg, "agentId": agent_id})
            return

        root_path = loc_row[0]["root_path"]
        if output_path.startswith(root_path):
            rel_path = output_path[len(root_path):].lstrip("/").lstrip("\\")
        else:
            rel_path = filename

        file_type_high, file_type_low = classify_file(filename)
        now_iso = utc_now()
        mtime_iso = safe_timestamp(mtime, rel_path) if mtime else now_iso
        ctime_iso = safe_timestamp(ctime, rel_path) if ctime else now_iso

        async def insert(conn):
            cursor = await conn.execute(
                """INSERT INTO files
                   (filename, full_path, rel_path, location_id, folder_id,
                    file_type_high, file_type_low, file_size,
                    description,
                    created_date, modified_date, date_cataloged, date_last_seen,
                    stale, hidden, dup_exclude, inode)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, 0, ?, ?, ?)""",
                (
                    filename, output_path, rel_path, location_id, folder_id,
                    file_type_high, file_type_low, size,
                    ctime_iso, mtime_iso, now_iso, now_iso,
                    src["hidden"], src["dup_exclude"], inode,
                ),
            )
            await conn.commit()
            return cursor.lastrowid

        try:
            file_id = await execute_write(insert)
        except sqlite3.IntegrityError:
            # Already cataloged: a previous attempt committed the insert but
            # crashed before resolve_pending ran, so the op was re-queued.
            # Its stats were updated then, so they aren't added again.
            async with read_db() as db:
                existing = await db.execute_fetchall(
                    "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
                    (location_id, rel_path),
                )
            if existing:
                file_id = existing[0]["id"]
                logger.info(
                    "%s output already cataloged: %s (file #%d)",
                    self.label, filename, file_id,
                )
            else:
                raise
        else:
            await update_stats_for_files(
                location_id,
                added=[(folder_id, size, file_type_high, src["hidden"])],
            )

        invalidate_stats_cache()
        await post_op_stats(location_ids={location_id}, source=self.op_type)

        await broadcast(
            {
                "type": f"{self.prefix}_complete",
                "agentId": agent_id,
                "fileId": file_id,
                "filename": filename,
                "path": output_path,
                "size": size,
                "folderId": folder_id,
                "locationId": location_id,
            }
        )
        logger.info("%s cataloged: %s (file #%d)", self.label, filename, file_id)

        # Unblock the queue handler so it can complete and free the agent slot
        self.resolve_pending(source_path, {"type": f"{self.prefix}_complete"})


def transcode_progress(msg):
    pct = msg.get("percent")
    return f"{pct}%" if pct is not None else msg.get("status", "")


transcode = Conversion(
    op_type="transcode",
    prefix="transcode",
    label="Transcode",
    failed_text="Transcode failed",
    terminal_types=("transcode_error", "transcode_cancelled"),
    progress_text=transcode_progress,
    dispatch_args=lambda params: {"quality": params.get("quality", "medium")},
)

raw_convert = Conversion(
    op_type="raw_convert",
    prefix="rawconvert",
    label="Raw convert",
    failed_text="Raw conversion failed",
    terminal_types=("rawconvert_error",),
    progress_text=lambda msg: msg.get("status", "converting"),
    dispatch_args=lambda params: {},
)

BY_PREFIX = {c.prefix: c for c in (transcode, raw_convert)}


def conversion_for(msg_type: str) -> Conversion | None:
    """The Conversion an agent message belongs to, from its type prefix."""
    return BY_PREFIX.get(msg_type.rsplit("_", 1)[0])
