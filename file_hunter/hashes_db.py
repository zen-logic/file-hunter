"""Hashes database — separate SQLite file for duplicate detection.

Own writer, own read connections. Never contends with the catalog writer.
Dup detection queries and dup_count maintenance happen here, not in the
catalog DB.

The hashes DB is created empty on first run. A separate migration script
populates it from the existing catalog. The app works with it empty —
dup_count reads return 0 until migration runs.
"""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from file_hunter.config import load_config

_write_db = None
_write_lock = asyncio.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS file_hashes (
    file_id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    file_size INTEGER NOT NULL,
    hash_partial TEXT,
    hash_fast TEXT,
    hash_strong TEXT,
    dup_count INTEGER NOT NULL DEFAULT 0,
    excluded INTEGER NOT NULL DEFAULT 0,
    stale INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_hashes_size_partial
    ON file_hashes(file_size, hash_partial);
CREATE INDEX IF NOT EXISTS idx_hashes_fast
    ON file_hashes(hash_fast);
CREATE INDEX IF NOT EXISTS idx_hashes_strong
    ON file_hashes(hash_strong);
CREATE INDEX IF NOT EXISTS idx_hashes_location
    ON file_hashes(location_id);
CREATE INDEX IF NOT EXISTS idx_hashes_active
    ON file_hashes(excluded, stale, hash_partial, file_size);
CREATE INDEX IF NOT EXISTS idx_hashes_active_fast
    ON file_hashes(excluded, stale, hash_fast);
CREATE INDEX IF NOT EXISTS idx_hashes_active_strong
    ON file_hashes(excluded, stale, hash_strong);

CREATE VIEW IF NOT EXISTS active_hashes AS
    SELECT * FROM file_hashes WHERE excluded = 0 AND stale = 0;
"""

_MIGRATIONS = [
    "ALTER TABLE file_hashes ADD COLUMN stale INTEGER NOT NULL DEFAULT 0",
    # Recreate view to include stale filter
    "DROP VIEW IF EXISTS active_hashes",
    "CREATE VIEW active_hashes AS SELECT * FROM file_hashes WHERE excluded = 0 AND stale = 0",
]


def _hashes_db_path() -> Path:
    config = load_config()
    catalog_path = Path(config.get("database", "data/file_hunter.db"))
    if not catalog_path.is_absolute():
        catalog_path = Path(__file__).resolve().parent.parent / catalog_path
    return catalog_path.parent / "hashes.db"


async def init_hashes_db():
    """Create hashes.db and schema if it doesn't exist.

    Called during app startup. Fast — just CREATE TABLE IF NOT EXISTS.
    No data migration.
    """
    db_path = _hashes_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        for stmt in _SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        await conn.commit()

        # Column migration (idempotent)
        for migration in [
            "ALTER TABLE file_hashes ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE file_hashes ADD COLUMN stale INTEGER NOT NULL DEFAULT 0",
        ]:
            try:
                await conn.execute(migration)
                await conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

        # Recreate view to include stale filter
        await conn.execute("DROP VIEW IF EXISTS active_hashes")
        await conn.execute(
            "CREATE VIEW IF NOT EXISTS active_hashes AS "
            "SELECT * FROM file_hashes WHERE excluded = 0 AND stale = 0"
        )
        await conn.commit()
    finally:
        await conn.close()


async def _get_write_db() -> aiosqlite.Connection:
    """Lazy-init the single hashes write connection."""
    global _write_db
    if _write_db is None:
        db_path = _hashes_db_path()
        _write_db = await aiosqlite.connect(db_path)
        _write_db.row_factory = aiosqlite.Row
        await _write_db.execute("PRAGMA journal_mode=WAL")
    return _write_db


@asynccontextmanager
async def hashes_writer():
    """Acquire exclusive write access to the hashes database.

    Same pattern as catalog db_writer() — own lock, own connection.
    Auto-commits on clean exit; rolls back on exception.
    """
    async with _write_lock:
        db = await _get_write_db()
        try:
            yield db
            await db.commit()
        except BaseException:
            try:
                await db.rollback()
            except Exception:
                pass
            raise


async def open_hashes_connection() -> aiosqlite.Connection:
    """Open a read-only hashes DB connection (caller must close it).

    For long-running read operations that need their own transaction
    lifetime.
    """
    db_path = _hashes_db_path()
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    return conn


@asynccontextmanager
async def read_hashes():
    """Open a hashes read connection, yield it, close on exit.

    WAL mode allows unlimited concurrent readers.

    Usage:
        async with read_hashes() as db:
            rows = await db.execute_fetchall("SELECT ...")
    """
    conn = await open_hashes_connection()
    try:
        yield conn
    finally:
        await conn.close()


async def get_file_hashes(file_ids: list[int]) -> dict[int, dict]:
    """Fetch hash data from hashes.db for a batch of file IDs.

    Returns {file_id: {hash_partial, hash_fast, hash_strong, dup_count}}.
    Missing IDs are omitted from the result.
    """
    if not file_ids:
        return {}
    result: dict[int, dict] = {}
    async with read_hashes() as hdb:
        for i in range(0, len(file_ids), 500):
            batch = file_ids[i : i + 500]
            ph = ",".join("?" for _ in batch)
            rows = await hdb.execute_fetchall(
                f"SELECT file_id, hash_partial, hash_fast, hash_strong, dup_count "
                f"FROM file_hashes WHERE file_id IN ({ph})",
                batch,
            )
            for r in rows:
                result[r["file_id"]] = {
                    "hash_partial": r["hash_partial"],
                    "hash_fast": r["hash_fast"],
                    "hash_strong": r["hash_strong"],
                    "dup_count": r["dup_count"],
                }
    return result


async def remove_file_hashes(file_ids: list[int]):
    """Remove entries from hashes.db for truly deleted files.

    Only use for permanent deletion (location delete, file delete).
    For stale files, use mark_hashes_stale() instead — preserves
    hash data for recovery.
    """
    if not file_ids:
        return
    for i in range(0, len(file_ids), 500):
        batch = file_ids[i : i + 500]
        ph = ",".join("?" for _ in batch)
        async with hashes_writer() as wdb:
            await wdb.execute(
                f"DELETE FROM file_hashes WHERE file_id IN ({ph})",
                batch,
            )


async def mark_hashes_stale(file_ids: list[int]):
    """Flag hashes as stale — excluded from active_hashes but data preserved.

    Used when files are marked stale (not seen on disk). The hash data
    remains so files can be recovered without re-hashing.
    """
    if not file_ids:
        return
    for i in range(0, len(file_ids), 500):
        batch = file_ids[i : i + 500]
        ph = ",".join("?" for _ in batch)
        async with hashes_writer() as wdb:
            await wdb.execute(
                f"UPDATE file_hashes SET stale = 1 WHERE file_id IN ({ph})",
                batch,
            )


async def clear_hashes_stale(file_ids: list[int]):
    """Clear stale flag — file recovered, hashes active again."""
    if not file_ids:
        return
    for i in range(0, len(file_ids), 500):
        batch = file_ids[i : i + 500]
        ph = ",".join("?" for _ in batch)
        async with hashes_writer() as wdb:
            await wdb.execute(
                f"UPDATE file_hashes SET stale = 0 WHERE file_id IN ({ph})",
                batch,
            )


async def remove_location_hashes(location_id: int):
    """Remove all hashes for a location (used during location deletion)."""
    async with hashes_writer() as wdb:
        await wdb.execute(
            "DELETE FROM file_hashes WHERE location_id = ?",
            (location_id,),
        )


async def update_file_hash(file_id: int, **kwargs):
    """Update hash values for a single file in hashes.db.

    kwargs can include: hash_partial, hash_fast, hash_strong.
    Creates the entry if it doesn't exist (requires location_id and
    file_size in kwargs for insert).
    """
    if not kwargs:
        return
    sets = []
    vals = []
    for col in ("hash_partial", "hash_fast", "hash_strong"):
        if col in kwargs:
            sets.append(f"{col} = ?")
            vals.append(kwargs[col])
    if not sets:
        return
    vals.append(file_id)
    async with hashes_writer() as wdb:
        await wdb.execute(
            f"UPDATE file_hashes SET {', '.join(sets)} WHERE file_id = ?",
            vals,
        )


async def close_hashes_db():
    """Close the hashes write connection. Called on shutdown."""
    global _write_db
    if _write_db is not None:
        await _write_db.close()
        _write_db = None
