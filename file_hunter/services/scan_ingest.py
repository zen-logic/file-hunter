"""Ingest agent scan results into the server catalog.

Processes scan_files batches from agents and writes file/folder records
into the database using the core scanner functions.
"""

import logging
from datetime import datetime, timezone

from file_hunter.db import open_connection
from file_hunter.services.scanner import (
    ensure_folder_hierarchy,
    upsert_file,
    mark_stale_files,
)
from file_hunter.services.stats import invalidate_stats_cache

logger = logging.getLogger("file_hunter")


class AgentScanSession:
    """Tracks state for a single agent scan."""

    def __init__(
        self,
        agent_id,
        location_id,
        location_name,
        scan_id,
        root_path,
        scan_path: str | None = None,
    ):
        self.agent_id = agent_id
        self.location_id = location_id
        self.location_name = location_name
        self.scan_id = scan_id
        self.root_path = root_path
        self.scan_path = scan_path
        self.folder_cache: dict[str, tuple] = {}
        self.files_ingested = 0
        self.potential_matches = 0
        self.affected_hashes: set[str] = set()
        self.db = None


# agent_id -> session
_active_sessions: dict[int, AgentScanSession] = {}


def has_session(agent_id: int) -> bool:
    return agent_id in _active_sessions


def get_session_location(agent_id: int) -> tuple[int, str] | None:
    """Return (location_id, location_name) for an active session, or None."""
    session = _active_sessions.get(agent_id)
    if session:
        return session.location_id, session.location_name
    return None


def get_potential_matches(agent_id: int) -> int:
    """Return running count of potential cross-location matches for this scan."""
    session = _active_sessions.get(agent_id)
    return session.potential_matches if session else 0


def is_location_scanning(location_id: int) -> bool:
    """Check if any active agent scan session covers this location."""
    for agent_id, s in _active_sessions.items():
        if s.location_id == location_id:
            return True
    return False


async def start_session(agent_id: int, path: str):
    """Start a scan ingestion session for an agent."""
    if agent_id in _active_sessions:
        return

    db = await open_connection()

    row = await db.execute_fetchall(
        "SELECT id, name, root_path FROM locations WHERE agent_id = ? AND root_path = ?",
        (agent_id, path),
    )
    if not row:
        row = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE agent_id = ?",
            (agent_id,),
        )
        matched = None
        for r in row:
            if path.startswith(r["root_path"]) or r["root_path"].startswith(path):
                matched = r
                break
        if not matched:
            logger.warning(
                "Agent #%d scan path '%s' has no matching location", agent_id, path
            )
            await db.close()
            return
        row = [matched]

    location_id = row[0]["id"]
    location_name = row[0]["name"]
    root_path = row[0]["root_path"]

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    cursor = await db.execute(
        "INSERT INTO scans (location_id, status, started_at) VALUES (?, 'running', ?)",
        (location_id, now_iso),
    )
    scan_id = cursor.lastrowid
    await db.commit()

    actual_scan_path = path if path != root_path else None
    session = AgentScanSession(
        agent_id,
        location_id,
        location_name,
        scan_id,
        root_path,
        scan_path=actual_scan_path,
    )
    session.db = db
    _active_sessions[agent_id] = session

    logger.info(
        "Agent #%d scan session started for location #%d (%s), scan_id=%d",
        agent_id,
        location_id,
        root_path,
        scan_id,
    )


async def ingest_batch(agent_id: int, files: list[dict]):
    """Ingest a batch of file records from an agent scan."""
    session = _active_sessions.get(agent_id)
    if not session:
        return

    db = session.db
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for f in files:
        rel_path = f.get("rel_path", "")
        if not rel_path:
            continue

        parts = rel_path.replace("\\", "/").rsplit("/", 1)
        if len(parts) == 2:
            rel_dir = parts[0]
            folder_id, folder_dup_exclude = await ensure_folder_hierarchy(
                db, session.location_id, rel_dir, session.folder_cache
            )
        else:
            folder_id = None
            folder_dup_exclude = 0

        await upsert_file(
            db,
            location_id=session.location_id,
            scan_id=session.scan_id,
            filename=f.get("filename", ""),
            full_path=f.get("full_path", ""),
            rel_path=rel_path,
            folder_id=folder_id,
            file_size=f.get("file_size", 0),
            created_date=f.get("created_date", ""),
            modified_date=f.get("modified_date", ""),
            file_type_high=f.get("file_type_high", ""),
            file_type_low=f.get("file_type_low", ""),
            hash_partial=f.get("hash_partial"),
            hash_fast=f.get("hash_fast"),
            hash_strong=f.get("hash_strong"),
            now_iso=now_iso,
            hidden=f.get("hidden", 0),
            dup_exclude=folder_dup_exclude,
        )
        session.files_ingested += 1
        hs = f.get("hash_strong")
        if hs:
            session.affected_hashes.add(hs)

    await db.commit()

    # Check for potential cross-location matches by (file_size, hash_partial)
    candidates = [
        (f.get("file_size", 0), f.get("hash_partial"))
        for f in files
        if f.get("hash_partial")
    ]
    if candidates:
        conditions = " OR ".join(
            "(file_size = ? AND hash_partial = ?)" for _ in candidates
        )
        params = []
        for size, hp in candidates:
            params.extend([size, hp])
        params.append(session.location_id)
        rows = await db.execute_fetchall(
            f"""SELECT DISTINCT file_size, hash_partial FROM files
                WHERE ({conditions})
                  AND location_id != ?
                  AND stale = 0""",
            params,
        )
        if rows:
            matched_pairs = {(r["file_size"], r["hash_partial"]) for r in rows}
            new_matches = sum(
                1 for size, hp in candidates if (size, hp) in matched_pairs
            )
            session.potential_matches += new_matches


async def complete_session(
    agent_id: int, path: str, incremental: bool = False, deleted: list[str] | None = None
) -> int:
    """Finalize a completed agent scan. Returns files ingested count."""
    session = _active_sessions.pop(agent_id, None)
    if not session:
        logger.warning(
            "complete_session: no session for agent #%d (path=%s)", agent_id, path
        )
        return 0

    logger.info(
        "complete_session: agent #%d (scan_id=%d, location=#%d '%s', files=%d, incremental=%s)",
        agent_id,
        session.scan_id,
        session.location_id,
        session.location_name,
        session.files_ingested,
        incremental,
    )

    db = session.db
    try:
        if incremental and deleted is not None:
            # Incremental: mark specific deleted files as stale
            stale_count = 0
            if deleted:
                for i in range(0, len(deleted), 500):
                    batch = deleted[i : i + 500]
                    placeholders = ",".join("?" * len(batch))
                    cursor = await db.execute(
                        f"""UPDATE files SET stale=1
                            WHERE location_id=? AND stale=0
                            AND rel_path IN ({placeholders})""",
                        [session.location_id] + batch,
                    )
                    stale_count += cursor.rowcount
                await db.commit()
            logger.info(
                "Incremental stale: %d files marked stale from deleted list",
                stale_count,
            )
        else:
            # Full scan: mark anything not seen in this scan as stale
            import posixpath

            scan_prefix = None
            if session.scan_path:
                scan_prefix = posixpath.relpath(session.scan_path, session.root_path)

            stale_count = await mark_stale_files(
                db, session.location_id, session.scan_id, scan_prefix
            )

        completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await db.execute(
            """UPDATE scans SET status='completed', completed_at=?,
               files_found=?, files_hashed=?, stale_files=?
               WHERE id=?""",
            (
                completed_iso,
                session.files_ingested,
                session.files_ingested,
                stale_count,
                session.scan_id,
            ),
        )
        await db.execute(
            "UPDATE locations SET date_last_scanned=? WHERE id=?",
            (completed_iso, session.location_id),
        )
        await db.commit()

        invalidate_stats_cache()

        from file_hunter.services.sizes import recalculate_location_sizes

        try:
            await recalculate_location_sizes(db, session.location_id)
        except Exception:
            pass

        try:
            from file_hunter.services.dup_counts import recalculate_dup_counts

            await recalculate_dup_counts(db, session.affected_hashes)
        except Exception:
            pass

        logger.info(
            "Agent #%d scan completed: %d files ingested, %d stale",
            agent_id,
            session.files_ingested,
            stale_count,
        )
        return session.files_ingested
    finally:
        await db.close()


async def cancel_session(agent_id: int):
    """Clean up a cancelled agent scan session (user-initiated cancel)."""
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await db.execute(
            "UPDATE scans SET status='cancelled', completed_at=? WHERE id=?",
            (completed_iso, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.info("Agent #%d scan cancelled", agent_id)
    finally:
        await db.close()


async def interrupt_session(agent_id: int):
    """Clean up an interrupted agent scan session (agent disconnect/restart).

    Unlike cancel_session, this marks the scan as 'interrupted' so it can
    be distinguished from user-initiated cancellations.
    """
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        completed_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        await db.execute(
            "UPDATE scans SET status='interrupted', completed_at=? WHERE id=?",
            (completed_iso, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.info("Agent #%d scan interrupted (agent disconnected)", agent_id)
    finally:
        await db.close()


async def error_session(agent_id: int, error: str):
    """Clean up a failed agent scan session."""
    session = _active_sessions.pop(agent_id, None)
    if not session:
        return

    db = session.db
    try:
        await db.execute(
            "UPDATE scans SET status='error', error=? WHERE id=?",
            (error, session.scan_id),
        )
        await db.commit()
        invalidate_stats_cache()
        logger.warning("Agent #%d scan error: %s", agent_id, error)
    finally:
        await db.close()
