"""Text database — separate SQLite file holding document chunk text.

The embedding service returns each document as chunks of text. ChromaDB
holds the chunk vectors; this database holds the chunk text, with an FTS5
index for full-text search. A chunk is (file_id, chunk_index), the same pair
ChromaDB's chunk id "{file_id}_chunk{i}" is built from.

Own writer, own read connections — never contends with the catalog writer.
"""

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from file_hunter.config import load_config

_write_db = None
_write_lock = asyncio.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chunks (
    id INTEGER PRIMARY KEY,
    file_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    headings TEXT NOT NULL DEFAULT '',
    text TEXT NOT NULL,
    UNIQUE (file_id, chunk_index)
);

CREATE VIRTUAL TABLE IF NOT EXISTS chunks_fts USING fts5(
    headings, text, content='chunks', content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS chunks_ai AFTER INSERT ON chunks BEGIN
    INSERT INTO chunks_fts(rowid, headings, text) VALUES (new.id, new.headings, new.text);
END;

CREATE TRIGGER IF NOT EXISTS chunks_ad AFTER DELETE ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, headings, text)
        VALUES ('delete', old.id, old.headings, old.text);
END;

CREATE TRIGGER IF NOT EXISTS chunks_au AFTER UPDATE ON chunks BEGIN
    INSERT INTO chunks_fts(chunks_fts, rowid, headings, text)
        VALUES ('delete', old.id, old.headings, old.text);
    INSERT INTO chunks_fts(rowid, headings, text) VALUES (new.id, new.headings, new.text);
END;
"""


def _text_db_path() -> Path:
    config = load_config()
    catalog_path = Path(config.get("database", "data/file_hunter.db"))
    if not catalog_path.is_absolute():
        catalog_path = Path(__file__).resolve().parent.parent / catalog_path
    return catalog_path.parent / "text.db"


async def init_text_db():
    """Create text.db and schema if it doesn't exist. Called during app startup."""
    db_path = _text_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.executescript(_SCHEMA)
        await conn.commit()
    finally:
        await conn.close()


async def _get_write_db() -> aiosqlite.Connection:
    """Lazy-init the single text write connection."""
    global _write_db
    if _write_db is None:
        _write_db = await aiosqlite.connect(_text_db_path())
        _write_db.row_factory = aiosqlite.Row
        await _write_db.execute("PRAGMA journal_mode=WAL")
    return _write_db


@asynccontextmanager
async def text_writer():
    """Exclusive write access to the text database. Commits on clean exit."""
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


@asynccontextmanager
async def read_text():
    """Open a text read connection, yield it, close on exit."""
    conn = await aiosqlite.connect(_text_db_path())
    conn.row_factory = aiosqlite.Row
    try:
        yield conn
    finally:
        await conn.close()


async def store_chunks(file_id: int, chunks: list[dict]):
    """Replace a file's chunk text. Each chunk is {"text": ..., "meta": headings}."""
    async with text_writer() as db:
        await db.execute("DELETE FROM chunks WHERE file_id = ?", (file_id,))
        await db.executemany(
            "INSERT INTO chunks (file_id, chunk_index, headings, text) VALUES (?, ?, ?, ?)",
            [(file_id, i, c.get("meta", "") or "", c["text"]) for i, c in enumerate(chunks)],
        )


async def delete_files(file_ids: list[int]):
    """Remove all chunk text for the given files."""
    if not file_ids:
        return
    async with text_writer() as db:
        for start in range(0, len(file_ids), 500):
            batch = file_ids[start:start + 500]
            ph = ",".join("?" for _ in batch)
            await db.execute(f"DELETE FROM chunks WHERE file_id IN ({ph})", batch)


async def close_text_db():
    """Close the text write connection. Called on shutdown."""
    global _write_db
    if _write_db is not None:
        await _write_db.close()
        _write_db = None
