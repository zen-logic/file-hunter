import sqlite3
from datetime import datetime, timezone


async def add_ignore_rule(db, filename, file_size, location_id=None):
    """Insert an ignore rule. Returns the new rule dict or raises on duplicate."""
    now = datetime.now(timezone.utc).isoformat()
    try:
        cursor = await db.execute(
            """INSERT INTO ignored_files (filename, file_size, location_id, date_created)
               VALUES (?, ?, ?, ?)""",
            (filename, file_size, location_id, now),
        )
        await db.commit()
    except sqlite3.IntegrityError:
        return None  # duplicate rule

    return {
        "id": cursor.lastrowid,
        "filename": filename,
        "file_size": file_size,
        "location_id": location_id,
        "date_created": now,
    }


async def remove_ignore_rule(db, rule_id):
    """Delete an ignore rule by id. Returns True if deleted."""
    cursor = await db.execute("DELETE FROM ignored_files WHERE id = ?", (rule_id,))
    await db.commit()
    return cursor.rowcount > 0


async def list_ignore_rules(db):
    """Return all ignore rules with location name joined."""
    cursor = await db.execute(
        """SELECT i.id, i.filename, i.file_size, i.location_id, i.date_created,
                  l.name AS location_name
           FROM ignored_files i
           LEFT JOIN locations l ON l.id = i.location_id
           ORDER BY i.date_created DESC"""
    )
    rows = await cursor.fetchall()
    return [
        {
            "id": row["id"],
            "filename": row["filename"],
            "file_size": row["file_size"],
            "location_id": row["location_id"],
            "location_name": row["location_name"],
            "date_created": row["date_created"],
        }
        for row in rows
    ]


async def check_file_ignored(db, filename, file_size, location_id):
    """Check if a file matches any applicable ignore rule (global or location-scoped).
    Returns the matching rule dict or None."""
    cursor = await db.execute(
        """SELECT id, filename, file_size, location_id, date_created
           FROM ignored_files
           WHERE filename = ? AND file_size = ?
             AND (location_id IS NULL OR location_id = ?)
           LIMIT 1""",
        (filename, file_size, location_id),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "filename": row["filename"],
        "file_size": row["file_size"],
        "location_id": row["location_id"],
        "date_created": row["date_created"],
    }


async def count_matching_files(db, filename, file_size, location_id=None):
    """Count catalog files matching filename + file_size, optionally scoped to a location."""
    if location_id is not None:
        cursor = await db.execute(
            "SELECT COUNT(*) AS cnt FROM files WHERE filename = ? AND file_size = ? AND location_id = ?",
            (filename, file_size, location_id),
        )
    else:
        cursor = await db.execute(
            "SELECT COUNT(*) AS cnt FROM files WHERE filename = ? AND file_size = ?",
            (filename, file_size),
        )
    row = await cursor.fetchone()
    return row["cnt"]
