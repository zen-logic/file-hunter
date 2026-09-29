"""Provenance files written when a file is moved or consolidated away.

The original is replaced by a .moved stub saying where it went, and the file
it went to gets a .sources file listing where its copies came from. Both are
catalogued like any other file.
"""

import os

from file_hunter.db import db_writer, read_db


def build_stub_text(filename, dest_path, dest_label, now_iso):
    """The text of a .moved stub."""
    moved_to = f"{dest_label}: {dest_path}" if dest_label else dest_path
    return (
        f"Consolidated by File Hunter\n"
        f"Original: {filename}\n"
        f"Moved to: {moved_to}\n"
        f"Date: {now_iso}\n"
    )


async def write_stub_record(
    wdb, file_id, stub_name, stub_path, stub_rel, stub_size, now_iso
):
    """Turn a file's catalog record into the record of its .moved stub."""
    await wdb.execute(
        """UPDATE files SET
            filename=?, full_path=?, rel_path=?,
            file_type_high='text', file_type_low='moved',
            file_size=?,
            modified_date=?, date_last_seen=?
           WHERE id=?""",
        (stub_name, stub_path, stub_rel, stub_size, now_iso, now_iso, file_id),
    )


async def file_folder(file_id):
    """(folder_id, rel_dir) of a catalogued file, or None."""
    async with read_db() as db:
        rows = await db.execute_fetchall(
            "SELECT folder_id, rel_path FROM files WHERE id = ?", (file_id,)
        )
    if not rows:
        return None
    return rows[0]["folder_id"], os.path.dirname(rows[0]["rel_path"])


async def upsert_sources_record(
    canonical_path, location_id, folder_id, rel_dir, sources_text, now_iso
):
    """Insert or update the catalog record of canonical_path's .sources file.

    The size is len(sources_text): exact for a new file, approximate after
    further appends, without an agent round-trip.
    """
    sources_path = canonical_path + ".sources"
    sources_name = os.path.basename(sources_path)
    sources_rel = os.path.join(rel_dir, sources_name) if rel_dir else sources_name
    sources_size = len(sources_text.encode())

    async with read_db() as db:
        existing = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
            (location_id, sources_rel),
        )

    async with db_writer() as wdb:
        if existing:
            await wdb.execute(
                "UPDATE files SET modified_date=?, date_last_seen=? WHERE id=?",
                (now_iso, now_iso, existing[0]["id"]),
            )
        else:
            await wdb.execute(
                """INSERT OR IGNORE INTO files
                   (filename, full_path, rel_path, location_id, folder_id,
                    file_type_high, file_type_low, file_size,
                    description,
                    created_date, modified_date, date_cataloged, date_last_seen, scan_id)
                   VALUES (?, ?, ?, ?, ?, 'text', 'sources', ?, '',
                           ?, ?, ?, ?, NULL)""",
                (
                    sources_name,
                    sources_path,
                    sources_rel,
                    location_id,
                    folder_id,
                    sources_size,
                    now_iso,
                    now_iso,
                    now_iso,
                    now_iso,
                ),
            )
