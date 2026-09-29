"""Hashes database — separate SQLite file for duplicate detection.

Own writer, own read connections. Never contends with the catalog writer.
Dup detection queries and dup_count maintenance happen here, not in the
catalog DB.

The hashes DB is created empty on first run. A separate migration script
populates it from the existing catalog. The app works with it empty —
dup_count reads return 0 until migration runs.
"""

from pathlib import Path


from file_hunter.db import id_batches
from file_hunter.sqlite_store import create_database, SqliteStore, catalog_path


SCHEMA = """
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

MIGRATIONS = [
    "ALTER TABLE file_hashes ADD COLUMN stale INTEGER NOT NULL DEFAULT 0",
    # Recreate view to include stale filter
    "DROP VIEW IF EXISTS active_hashes",
    "CREATE VIEW active_hashes AS SELECT * FROM file_hashes WHERE excluded = 0 AND stale = 0",
]


def hashes_db_path() -> Path:
    return catalog_path("data/file_hunter.db").parent / "hashes.db"


store = SqliteStore(
    hashes_db_path,
    read_pragmas=("PRAGMA journal_mode=WAL",),
    write_pragmas=("PRAGMA journal_mode=WAL",),
)
hashes_writer = store.writer
open_hashes_connection = store.open_reader
read_hashes = store.reader
close_hashes_db = store.close


async def init_hashes_db():
    """Create hashes.db and its schema if they don't exist, and bring an
    older database up to date. Called during app startup."""
    await create_database(
        hashes_db_path(),
        SCHEMA,
        migrations=[
            "ALTER TABLE file_hashes ADD COLUMN excluded INTEGER NOT NULL DEFAULT 0",
            "ALTER TABLE file_hashes ADD COLUMN stale INTEGER NOT NULL DEFAULT 0",
            # recreate the view to include the stale filter
            "DROP VIEW IF EXISTS active_hashes",
            "CREATE VIEW IF NOT EXISTS active_hashes AS "
            "SELECT * FROM file_hashes WHERE excluded = 0 AND stale = 0",
        ],
    )


async def get_file_hashes(file_ids: list[int]) -> dict[int, dict]:
    """Fetch hash data from hashes.db for a batch of file IDs.

    Returns {file_id: {hash_partial, hash_fast, hash_strong, dup_count}}.
    Missing IDs are omitted from the result.
    """
    if not file_ids:
        return {}
    result: dict[int, dict] = {}
    async with read_hashes() as hdb:
        for batch, ph in id_batches(file_ids):
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
    for batch, ph in id_batches(file_ids):
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
    for batch, ph in id_batches(file_ids):
        async with hashes_writer() as wdb:
            await wdb.execute(
                f"UPDATE file_hashes SET stale = 1 WHERE file_id IN ({ph})",
                batch,
            )


async def clear_hashes_stale(file_ids: list[int]):
    """Clear stale flag — file recovered, hashes active again."""
    if not file_ids:
        return
    for batch, ph in id_batches(file_ids):
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


async def hashes_of_files(file_ids):
    """(strong hashes, fast hashes, ids of the files that are duplicates).
    Each file contributes its strong hash, or its fast hash if it has no
    strong one."""
    strong, fast, dup_ids = set(), set(), set()
    async with read_hashes() as hdb:
        for batch, ph in id_batches(file_ids):
            rows = await hdb.execute_fetchall(
                f"SELECT file_id, hash_strong, hash_fast, dup_count "
                f"FROM file_hashes WHERE file_id IN ({ph})",
                batch,
            )
            for r in rows:
                if r["hash_strong"]:
                    strong.add(r["hash_strong"])
                elif r["hash_fast"]:
                    fast.add(r["hash_fast"])
                if (r["dup_count"] or 0) > 0:
                    dup_ids.add(r["file_id"])
    return strong, fast, dup_ids


async def set_file_hashes(
    file_id, location_id, file_size, hash_partial, hash_fast, hash_strong
):
    """Write a file's hashes, replacing any it had."""
    async with hashes_writer() as hdb:
        await hdb.execute(
            "INSERT INTO file_hashes "
            "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(file_id) DO UPDATE SET "
            "hash_partial=excluded.hash_partial, "
            "hash_fast=excluded.hash_fast, "
            "hash_strong=excluded.hash_strong",
            (file_id, location_id, file_size, hash_partial, hash_fast, hash_strong),
        )


async def register_file_sizes(location_id, id_sizes):
    """Add rows with no hashes yet for (file_id, file_size) pairs; a file
    that already has a row only gets the new size."""
    async with hashes_writer() as hdb:
        await hdb.executemany(
            "INSERT INTO file_hashes "
            "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
            "VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(file_id) DO UPDATE SET file_size=excluded.file_size",
            [(fid, location_id, size, None, None, None) for fid, size in id_sizes],
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
