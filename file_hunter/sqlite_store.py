"""Connection handling shared by the four SQLite databases (catalog, hashes,
stats, text).

Each has one write connection, opened on first use and shared behind a lock
so writes are serialised in-process (no SQLite lock contention or busy
timeouts), and a fresh connection per reader. WAL lets readers run alongside
the writer.
"""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from file_hunter.config import load_config

ROOT = Path(__file__).resolve().parent.parent


def catalog_path(default: str) -> Path:
    """The catalog database path from config.json; relative paths are
    relative to the install directory. The other databases sit beside it."""
    path = Path(load_config().get("database", default))
    return path if path.is_absolute() else ROOT / path


async def create_database(path, schema, migrations=()):
    """Create the database (and its directory) and schema in WAL mode, then
    run migrations in order, skipping any that sqlite rejects as already
    applied (a duplicate column, for example). The schema's statements are
    no-ops if already applied."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(path)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.executescript(schema)
        await conn.commit()
        for statement in migrations:
            try:
                await conn.execute(statement)
                await conn.commit()
            except sqlite3.OperationalError:
                pass
    finally:
        await conn.close()


class SqliteStore:
    def __init__(self, path_fn, read_pragmas, write_pragmas):
        self.path_fn = path_fn
        self.read_pragmas = read_pragmas
        self.write_pragmas = write_pragmas
        self.write_conn = None
        self.write_lock = asyncio.Lock()

    async def connect(self, pragmas) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.path_fn())
        conn.row_factory = aiosqlite.Row
        for pragma in pragmas:
            await conn.execute(pragma)
        return conn

    async def open_reader(self) -> aiosqlite.Connection:
        """A read connection for a long-lived read; the caller closes it."""
        return await self.connect(self.read_pragmas)

    @asynccontextmanager
    async def reader(self):
        """A read connection, closed on exit."""
        conn = await self.open_reader()
        try:
            yield conn
        finally:
            await conn.close()

    @asynccontextmanager
    async def writer(self):
        """Exclusive use of the write connection. Commits on a clean exit
        (a no-op if the caller already committed), rolls back on an
        exception."""
        async with self.write_lock:
            if self.write_conn is None:
                self.write_conn = await self.connect(self.write_pragmas)
            db = self.write_conn
            try:
                yield db
                await db.commit()
            except BaseException:
                try:
                    await db.rollback()
                except Exception:
                    pass
                raise

    async def close(self):
        """Close the write connection. Called on shutdown."""
        if self.write_conn is not None:
            await self.write_conn.close()
            self.write_conn = None
