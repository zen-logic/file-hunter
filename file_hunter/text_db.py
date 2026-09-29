"""Text database — separate SQLite file holding document chunk text.

The embedding service returns each document as chunks of text. ChromaDB
holds the chunk vectors; this database holds the chunk text, with an FTS5
index for full-text search. A chunk is (file_id, chunk_index), the same pair
ChromaDB's chunk id "{file_id}_chunk{i}" is built from.

Own writer, own read connections — never contends with the catalog writer.
"""

from pathlib import Path


from file_hunter.sqlite_store import create_database, SqliteStore, catalog_path


SCHEMA = """
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


def text_db_path() -> Path:
    return catalog_path("data/file_hunter.db").parent / "text.db"


store = SqliteStore(
    text_db_path,
    read_pragmas=(),
    write_pragmas=("PRAGMA journal_mode=WAL",),
)
text_writer = store.writer
read_text = store.reader
close_text_db = store.close


async def init_text_db():
    """Create the database and its schema if they don't exist. Called
    during app startup."""
    await create_database(text_db_path(), SCHEMA)


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
