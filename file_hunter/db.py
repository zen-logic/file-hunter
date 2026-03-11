import os
import aiosqlite
from pathlib import Path
from file_hunter.config import load_config

_db = None

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL,
    date_added TEXT NOT NULL,
    date_last_scanned TEXT
);

CREATE TABLE IF NOT EXISTS scans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'running',
    started_at TEXT NOT NULL,
    completed_at TEXT,
    files_found INTEGER DEFAULT 0,
    files_hashed INTEGER DEFAULT 0,
    duplicates_found INTEGER DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS folders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    location_id INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES folders(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    UNIQUE(location_id, rel_path)
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    full_path TEXT NOT NULL,
    rel_path TEXT NOT NULL,
    location_id INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    folder_id INTEGER REFERENCES folders(id) ON DELETE SET NULL,
    file_type_high TEXT,
    file_type_low TEXT,
    file_size INTEGER,
    hash_fast TEXT,
    hash_strong TEXT,
    description TEXT DEFAULT '',
    tags TEXT DEFAULT '',
    created_date TEXT,
    modified_date TEXT,
    date_cataloged TEXT,
    date_last_seen TEXT,
    scan_id INTEGER REFERENCES scans(id) ON DELETE SET NULL,
    UNIQUE(location_id, rel_path)
);

CREATE TABLE IF NOT EXISTS consolidation_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    source_location_id INTEGER NOT NULL REFERENCES locations(id) ON DELETE CASCADE,
    source_path TEXT NOT NULL,
    destination_path TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    date_created TEXT NOT NULL,
    date_completed TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL DEFAULT '',
    password_hash TEXT NOT NULL,
    date_created TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token TEXT NOT NULL UNIQUE,
    date_created TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    token_prefix TEXT NOT NULL,
    hostname TEXT DEFAULT '',
    os TEXT DEFAULT '',
    status TEXT NOT NULL DEFAULT 'offline',
    date_created TEXT NOT NULL,
    date_last_seen TEXT,
    http_host TEXT DEFAULT '',
    http_port TEXT DEFAULT '8001'
);

CREATE TABLE IF NOT EXISTS pending_backfills (
    agent_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    location_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (agent_id, location_id)
);

CREATE TABLE IF NOT EXISTS ignored_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    location_id INTEGER REFERENCES locations(id) ON DELETE CASCADE,
    date_created TEXT NOT NULL,
    UNIQUE(filename, file_size, location_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ignored_files_global
    ON ignored_files(filename, file_size) WHERE location_id IS NULL;

CREATE INDEX IF NOT EXISTS idx_files_hash_strong ON files(hash_strong);
CREATE INDEX IF NOT EXISTS idx_files_folder_id ON files(folder_id);
CREATE INDEX IF NOT EXISTS idx_files_location_id ON files(location_id);
CREATE INDEX IF NOT EXISTS idx_files_file_size ON files(file_size);
CREATE INDEX IF NOT EXISTS idx_folders_location_parent ON folders(location_id, parent_id);
CREATE INDEX IF NOT EXISTS idx_folders_parent_id ON folders(parent_id);
CREATE INDEX IF NOT EXISTS idx_consolidation_jobs_pending ON consolidation_jobs(source_location_id, status);
CREATE INDEX IF NOT EXISTS idx_sessions_token ON sessions(token);
CREATE INDEX IF NOT EXISTS idx_files_location_hash ON files(location_id, hash_strong);

CREATE TABLE IF NOT EXISTS saved_searches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    params TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Column migrations — ALTER TABLE wrapped in try/except for idempotency
_MIGRATIONS = [
    "ALTER TABLE files ADD COLUMN hash_partial TEXT",
    "ALTER TABLE files ADD COLUMN stale INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE scans ADD COLUMN files_skipped INTEGER DEFAULT 0",
    "ALTER TABLE scans ADD COLUMN stale_files INTEGER DEFAULT 0",
    "ALTER TABLE locations ADD COLUMN scan_schedule_enabled INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE locations ADD COLUMN scan_schedule_days TEXT NOT NULL DEFAULT ''",
    "ALTER TABLE locations ADD COLUMN scan_schedule_time TEXT NOT NULL DEFAULT '03:00'",
    "ALTER TABLE locations ADD COLUMN scan_schedule_last_run TEXT",
    "ALTER TABLE folders ADD COLUMN total_size INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE folders ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE locations ADD COLUMN total_size INTEGER",
    "ALTER TABLE locations ADD COLUMN file_count INTEGER",
    "ALTER TABLE files ADD COLUMN dup_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE files ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE folders ADD COLUMN hidden INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE files ADD COLUMN dup_exclude INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE folders ADD COLUMN dup_exclude INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE locations ADD COLUMN agent_id INTEGER REFERENCES agents(id)",
    "ALTER TABLE locations ADD COLUMN backfill_needed INTEGER NOT NULL DEFAULT 1",
    "ALTER TABLE scans ADD COLUMN scan_prefix TEXT",
    "ALTER TABLE scans ADD COLUMN incremental INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE scans ADD COLUMN deleted_json TEXT",
    "ALTER TABLE pending_backfills ADD COLUMN scan_prefix TEXT",
    "ALTER TABLE locations ADD COLUMN duplicate_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE locations ADD COLUMN type_counts TEXT NOT NULL DEFAULT '{}'",
    "ALTER TABLE folders ADD COLUMN duplicate_count INTEGER NOT NULL DEFAULT 0",
    "ALTER TABLE folders ADD COLUMN type_counts TEXT NOT NULL DEFAULT '{}'",
]

_OPERATION_QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS operation_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    agent_id INTEGER REFERENCES agents(id),
    params TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    error TEXT
);
CREATE INDEX IF NOT EXISTS idx_opqueue_status ON operation_queue(status);
CREATE INDEX IF NOT EXISTS idx_opqueue_agent_status ON operation_queue(agent_id, status);
"""


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        config = load_config()
        db_path = Path(config.get("database", "file_hunter.db"))
        if not db_path.is_absolute():
            db_path = Path(__file__).resolve().parent.parent / db_path
        _db = await aiosqlite.connect(db_path)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA foreign_keys=ON")
        await _db.execute("PRAGMA busy_timeout=30000")
        fresh = await init_db(_db)
        if fresh and os.environ.get("FILE_HUNTER_DEMO"):
            from file_hunter.seed import seed_db

            await seed_db(_db)
    return _db


async def init_db(db: aiosqlite.Connection):
    import sqlite3

    # Check if tables exist already
    cursor = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='locations'"
    )
    exists = await cursor.fetchone()

    for statement in SCHEMA.split(";"):
        stmt = statement.strip()
        if stmt:
            await db.execute(stmt)
    await db.commit()

    # Run column migrations (idempotent — silently ignores "duplicate column")
    migrated = False
    for sql in _MIGRATIONS:
        try:
            await db.execute(sql)
            if not migrated:
                print("Updating database...")
                migrated = True
        except sqlite3.OperationalError:
            pass  # column already exists
    await db.commit()

    # Fix UNIQUE constraint: root_path alone → (root_path, agent_id)
    cursor = await db.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='locations'"
    )
    row = await cursor.fetchone()
    if row and "root_path TEXT NOT NULL UNIQUE" in (row[0] or ""):
        await db.execute("PRAGMA foreign_keys=OFF")
        await db.execute(
            """CREATE TABLE locations_mig (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                root_path TEXT NOT NULL,
                date_added TEXT NOT NULL,
                date_last_scanned TEXT,
                scan_schedule_enabled INTEGER NOT NULL DEFAULT 0,
                scan_schedule_days TEXT NOT NULL DEFAULT '',
                scan_schedule_time TEXT NOT NULL DEFAULT '03:00',
                scan_schedule_last_run TEXT,
                total_size INTEGER,
                file_count INTEGER,
                agent_id INTEGER REFERENCES agents(id)
            )"""
        )
        await db.execute(
            """INSERT INTO locations_mig
            SELECT id, name, root_path, date_added, date_last_scanned,
                   scan_schedule_enabled, scan_schedule_days, scan_schedule_time,
                   scan_schedule_last_run, total_size, file_count, agent_id
            FROM locations"""
        )
        await db.execute("DROP TABLE locations")
        await db.execute("ALTER TABLE locations_mig RENAME TO locations")
        await db.commit()
        await db.execute("PRAGMA foreign_keys=ON")

    # Operation queue table (idempotent via IF NOT EXISTS)
    for stmt in _OPERATION_QUEUE_SCHEMA.split(";"):
        stmt = stmt.strip()
        if stmt:
            await db.execute(stmt)
    await db.commit()

    # Post-migration indexes (columns may not exist until migrations run)
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_size_partial ON files(file_size, hash_partial)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_files_dup_count ON files(dup_count)"
    )
    await db.execute("DROP INDEX IF EXISTS idx_files_hidden")
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_locations_agent_id ON locations(agent_id)"
    )
    await db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_locations_root_agent "
        "ON locations(root_path, agent_id)"
    )
    await db.commit()

    return not exists


async def open_connection() -> aiosqlite.Connection:
    """Open a standalone database connection (caller must close it)."""
    config = load_config()
    db_path = Path(config.get("database", "file_hunter.db"))
    if not db_path.is_absolute():
        db_path = Path(__file__).resolve().parent.parent / db_path
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA foreign_keys=ON")
    await conn.execute("PRAGMA journal_mode=WAL")
    await conn.execute("PRAGMA busy_timeout=30000")
    return conn


_write_lock_requested = False


def request_write_lock():
    """Signal the scanner to yield the DB write lock."""
    global _write_lock_requested
    _write_lock_requested = True


def check_write_lock_requested():
    """Check and clear the write lock request flag. Called by the scanner."""
    global _write_lock_requested
    if _write_lock_requested:
        _write_lock_requested = False
        return True
    return False


async def execute_write(func, *args, **kwargs):
    """Run a write function on its own connection.

    Signals the scanner to yield the write lock, then lets SQLite's
    busy_timeout (30s from open_connection) handle contention.
    """
    request_write_lock()
    conn = await open_connection()
    try:
        return await func(conn, *args, **kwargs)
    finally:
        await conn.close()


async def close_db():
    global _db
    if _db is not None:
        await _db.close()
        _db = None
