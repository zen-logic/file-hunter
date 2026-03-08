"""Scanner database helpers — folder hierarchy, file upsert, stale marking.

Filesystem I/O (walking, hashing) is handled by agents. The server only
ingests results via scan_ingest.py, which calls these functions.
"""

import logging

log = logging.getLogger("file_hunter")


async def _mark_stale_files(
    db, location_id: int, scan_id: int, scan_prefix: str | None = None
) -> int:
    """Mark files not seen in this scan as stale. Returns count.

    When scan_prefix is set, only marks files within that subtree stale.
    Files in the folder have rel_path like "prefix/filename", and files
    in subdirectories have "prefix/sub/filename" — both matched by
    ``prefix/%``.
    """
    if scan_prefix:
        cursor = await db.execute(
            """UPDATE files SET stale=1
               WHERE location_id=? AND scan_id!=? AND stale=0
               AND rel_path LIKE ?""",
            (location_id, scan_id, scan_prefix + "/%"),
        )
        return cursor.rowcount
    else:
        cursor = await db.execute(
            "UPDATE files SET stale=1 WHERE location_id=? AND scan_id!=? AND stale=0",
            (location_id, scan_id),
        )
        return cursor.rowcount


async def _ensure_folder_hierarchy(
    db, location_id: int, rel_dir_path: str, folder_cache: dict[str, tuple]
) -> tuple[int, int]:
    """Create/find folder records for a full relative directory path.

    For 'Photos/2024 Holiday', ensures both 'Photos' and 'Photos/2024 Holiday'
    exist. Returns (leaf_folder_id, dup_exclude). Sets hidden=1 for dotfolders
    and their descendants. Uses each folder's own dup_exclude flag (not
    inherited from parents — allows subfolder carve-outs).
    """
    parts = rel_dir_path.replace("\\", "/").split("/")
    current_path = ""
    parent_id = None
    is_hidden = False
    leaf_dup_exclude = 0

    for part in parts:
        current_path = f"{current_path}/{part}" if current_path else part
        is_hidden = is_hidden or part.startswith(".")

        if current_path in folder_cache:
            parent_id, cached_dup_exclude = folder_cache[current_path]
            leaf_dup_exclude = cached_dup_exclude
            continue

        # Check DB
        row = await db.execute_fetchall(
            "SELECT id, dup_exclude FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, current_path),
        )
        if row:
            folder_id = row[0]["id"]
            leaf_dup_exclude = row[0]["dup_exclude"]
        else:
            cursor = await db.execute(
                "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden) VALUES (?, ?, ?, ?, ?)",
                (location_id, parent_id, part, current_path, 1 if is_hidden else 0),
            )
            folder_id = cursor.lastrowid
            leaf_dup_exclude = 0

        folder_cache[current_path] = (folder_id, leaf_dup_exclude)
        parent_id = folder_id

    return parent_id, leaf_dup_exclude


async def _upsert_file(
    db,
    *,
    location_id: int,
    scan_id: int,
    filename: str,
    full_path: str,
    rel_path: str,
    folder_id: int | None,
    file_size: int,
    created_date: str,
    modified_date: str,
    file_type_high: str,
    file_type_low: str,
    hash_partial: str | None,
    hash_fast: str | None,
    hash_strong: str | None,
    now_iso: str,
    hidden: int = 0,
    dup_exclude: int = 0,
) -> int:
    """Insert or update a file record. Preserves description and tags on update."""
    row = await db.execute_fetchall(
        "SELECT id FROM files WHERE location_id = ? AND rel_path = ?",
        (location_id, rel_path),
    )
    if row:
        file_id = row[0]["id"]
        await db.execute(
            """UPDATE files SET
                filename=?, full_path=?, folder_id=?,
                file_type_high=?, file_type_low=?, file_size=?,
                hash_partial=COALESCE(?, hash_partial),
                hash_fast=COALESCE(?, hash_fast),
                hash_strong=COALESCE(?, hash_strong),
                created_date=?, modified_date=?,
                date_last_seen=?, scan_id=?, stale=0, hidden=?, dup_exclude=?
               WHERE id=?""",
            (
                filename,
                full_path,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                hash_partial,
                hash_fast,
                hash_strong,
                created_date,
                modified_date,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
                file_id,
            ),
        )
        return file_id
    else:
        cursor = await db.execute(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                hash_partial, hash_fast, hash_strong,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen,
                scan_id, stale, hidden, dup_exclude)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, 0, ?, ?)""",
            (
                filename,
                full_path,
                rel_path,
                location_id,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                hash_partial,
                hash_fast,
                hash_strong,
                created_date,
                modified_date,
                now_iso,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
            ),
        )
        return cursor.lastrowid


# Public aliases for pro/extension reuse (keep _-prefixed originals intact)
ensure_folder_hierarchy = _ensure_folder_hierarchy
upsert_file = _upsert_file
mark_stale_files = _mark_stale_files
