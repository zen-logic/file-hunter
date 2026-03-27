"""Scanner database helpers — folder hierarchy, file upsert, stale marking.

Filesystem I/O (walking, hashing) is handled by agents. The server only
ingests results via scan_ingest.py, which calls these functions.
"""

import logging

from file_hunter.hashes_db import mark_hashes_stale

log = logging.getLogger("file_hunter")


async def _mark_stale_files(
    db, location_id: int, scan_id: int, scan_prefix: str | None = None
) -> int:
    """Mark files not seen in this scan as stale. Returns count.

    Also removes stale files from hashes.db (only active files belong there).

    When scan_prefix is set, only marks files within that subtree stale.
    Files in the folder have rel_path like "prefix/filename", and files
    in subdirectories have "prefix/sub/filename" — both matched by
    ``prefix/%``.
    """
    # Collect IDs before marking stale — for hashes.db cleanup
    if scan_prefix:
        stale_rows = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id=? AND scan_id!=? AND stale=0 "
            "AND rel_path LIKE ?",
            (location_id, scan_id, scan_prefix + "/%"),
        )
        cursor = await db.execute(
            """UPDATE files SET stale=1
               WHERE location_id=? AND scan_id!=? AND stale=0
               AND rel_path LIKE ?""",
            (location_id, scan_id, scan_prefix + "/%"),
        )
    else:
        stale_rows = await db.execute_fetchall(
            "SELECT id FROM files WHERE location_id=? AND scan_id!=? AND stale=0",
            (location_id, scan_id),
        )
        cursor = await db.execute(
            "UPDATE files SET stale=1 WHERE location_id=? AND scan_id!=? AND stale=0",
            (location_id, scan_id),
        )

    stale_ids = [r["id"] for r in stale_rows]
    if stale_ids:
        await mark_hashes_stale(stale_ids)

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
            "SELECT id, dup_exclude, hidden FROM folders WHERE location_id = ? AND rel_path = ?",
            (location_id, current_path),
        )
        if row:
            folder_id = row[0]["id"]
            leaf_dup_exclude = row[0]["dup_exclude"]
            # Fix hidden flag on existing folders (e.g. after migration defaulted to 0)
            expected_hidden = 1 if is_hidden else 0
            if row[0]["hidden"] != expected_hidden:
                await db.execute(
                    "UPDATE folders SET hidden = ? WHERE id = ?",
                    (expected_hidden, folder_id),
                )
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
    now_iso: str,
    hidden: int = 0,
    dup_exclude: int = 0,
    inode: int = 0,
) -> int:
    """Insert or update a file record. Preserves description and tags on update.

    Hashes are NOT written here — they belong in hashes.db only.
    """
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
                created_date=?, modified_date=?,
                date_last_seen=?, scan_id=?, stale=0, hidden=?, dup_exclude=?,
                inode=?
               WHERE id=?""",
            (
                filename,
                full_path,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                created_date,
                modified_date,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
                inode,
                file_id,
            ),
        )
        return file_id
    else:
        cursor = await db.execute(
            """INSERT INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen,
                scan_id, stale, hidden, dup_exclude, inode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, ?, 0, ?, ?, ?)""",
            (
                filename,
                full_path,
                rel_path,
                location_id,
                folder_id,
                file_type_high,
                file_type_low,
                file_size,
                created_date,
                modified_date,
                now_iso,
                now_iso,
                scan_id,
                hidden,
                dup_exclude,
                inode,
            ),
        )
        return cursor.lastrowid


async def _mark_stale_folders(
    db, location_id: int, seen_rel_paths: set[str], scan_prefix: str | None = None
) -> int:
    """Mark folders not seen on disk as stale. Clear stale on folders that are back.

    seen_rel_paths: set of rel_path strings for folders found on disk.
    Returns count of newly stale folders.
    """
    if scan_prefix:
        cat_rows = await db.execute_fetchall(
            "SELECT id, rel_path, stale FROM folders WHERE location_id = ? AND rel_path LIKE ?",
            (location_id, scan_prefix + "/%"),
        )
    else:
        cat_rows = await db.execute_fetchall(
            "SELECT id, rel_path, stale FROM folders WHERE location_id = ?",
            (location_id,),
        )

    stale_count = 0
    for row in cat_rows:
        on_disk = row["rel_path"] in seen_rel_paths
        if on_disk and row["stale"]:
            await db.execute("UPDATE folders SET stale = 0 WHERE id = ?", (row["id"],))
        elif not on_disk and not row["stale"]:
            await db.execute("UPDATE folders SET stale = 1 WHERE id = ?", (row["id"],))
            stale_count += 1
    return stale_count


# Public aliases for pro/extension reuse (keep _-prefixed originals intact)
ensure_folder_hierarchy = _ensure_folder_hierarchy
upsert_file = _upsert_file
mark_stale_files = _mark_stale_files
mark_stale_folders = _mark_stale_folders
