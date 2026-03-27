"""Quick scan — shallow, non-recursive scan of a single folder or location root.

Compares a single directory listing from the agent against the catalog:
- New folders and files are inserted
- Missing folders and files are marked stale
- New files are hashed (hash_fast) and dup counts updated
- Stats updated for the scanned folder and location
"""

import logging
import os
from datetime import datetime, timezone

from file_hunter.db import db_writer, read_db
from file_hunter.hashes_db import hashes_writer
from file_hunter.helpers import post_op_stats
from file_hunter.services import fs
from file_hunter.services.activity import register, unregister, update as activity_update
from file_hunter.services.agent_ops import dispatch
from file_hunter.services.dup_counts import recalculate_dup_counts
from file_hunter.stats_db import update_stats_for_files
from file_hunter.ws.scan import broadcast
from file_hunter_core.classify import classify_file

logger = logging.getLogger("file_hunter")


async def run_quick_scan(location_id: int, folder_id: int | None = None):
    """Quick scan a single directory level.

    location_id: the location
    folder_id: specific folder, or None for location root
    """
    # Resolve paths
    async with read_db() as db:
        loc_row = await db.execute_fetchall(
            "SELECT id, name, root_path, agent_id FROM locations WHERE id = ?",
            (location_id,),
        )
        if not loc_row:
            raise ValueError("Location not found")
        loc = loc_row[0]
        root_path = loc["root_path"]
        location_name = loc["name"]
        agent_id = loc["agent_id"]
        if not agent_id:
            raise ValueError("Location has no agent assigned")

        if folder_id:
            fld_row = await db.execute_fetchall(
                "SELECT id, name, rel_path, hidden, dup_exclude FROM folders WHERE id = ?",
                (folder_id,),
            )
            if not fld_row:
                raise ValueError("Folder not found")
            fld = fld_row[0]
            scan_path = os.path.join(root_path, fld["rel_path"])
            parent_rel = fld["rel_path"]
            parent_hidden = fld["hidden"]
            parent_dup_exclude = fld["dup_exclude"]
            label = f"{location_name} / {fld['name']}"
        else:
            scan_path = root_path
            parent_rel = ""
            parent_hidden = 0
            parent_dup_exclude = 0
            label = location_name

    act_name = f"quick-scan-{location_id}-{folder_id or 'root'}"
    register(act_name, f"Quick scan {label}")

    try:
        await broadcast(
            {
                "type": "scan_started",
                "locationId": location_id,
                "location": label,
                "quickScan": True,
            }
        )

        # Get listing from agent
        activity_update(act_name, progress="listing")
        listing = await dispatch("list_dir", location_id, path=scan_path)
        disk_folders = {f["name"]: f for f in listing["folders"]}
        disk_files = {f["name"]: f for f in listing["files"]}

        now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")

        # Get current catalog state for this folder
        async with read_db() as db:
            if folder_id:
                cat_folders = await db.execute_fetchall(
                    "SELECT id, name, rel_path FROM folders WHERE parent_id = ?",
                    (folder_id,),
                )
                cat_files = await db.execute_fetchall(
                    "SELECT id, filename, rel_path, file_size, file_type_high, hidden, stale FROM files WHERE folder_id = ?",
                    (folder_id,),
                )
            else:
                cat_folders = await db.execute_fetchall(
                    "SELECT id, name, rel_path FROM folders WHERE location_id = ? AND parent_id IS NULL",
                    (location_id,),
                )
                cat_files = await db.execute_fetchall(
                    "SELECT id, filename, rel_path, file_size, file_type_high, hidden, stale FROM files WHERE location_id = ? AND folder_id IS NULL",
                    (location_id,),
                )

        cat_folder_names = {f["name"]: f for f in cat_folders}
        cat_file_names = {f["filename"]: f for f in cat_files}

        new_folders = 0
        new_files = 0
        stale_folders = 0
        stale_files = 0
        recovered_files = 0
        new_file_ids = []
        stats_added = []  # (folder_id, file_size, file_type_high, is_hidden)
        stats_removed = []  # (folder_id, file_size, file_type_high, is_hidden)

        activity_update(act_name, progress="comparing")

        async with db_writer() as db:
            # --- Folders ---

            # New folders on disk
            for name, info in disk_folders.items():
                if name not in cat_folder_names:
                    rel = os.path.join(parent_rel, name) if parent_rel else name
                    is_hidden = 1 if name.startswith(".") else parent_hidden
                    cursor = await db.execute(
                        "INSERT INTO folders (location_id, parent_id, name, rel_path, hidden, dup_exclude) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            location_id,
                            folder_id,
                            name,
                            rel,
                            is_hidden,
                            parent_dup_exclude,
                        ),
                    )
                    new_folders += 1
                    logger.info("Quick scan: new folder '%s'", rel)

            # Missing folders — mark folder and all descendant folders/files stale
            for name, cat in cat_folder_names.items():
                if name not in disk_folders:
                    await db.execute(
                        """WITH RECURSIVE desc(id) AS (
                               SELECT ? UNION ALL
                               SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
                           )
                           UPDATE folders SET stale = 1
                           WHERE id IN (SELECT id FROM desc) AND stale = 0""",
                        (cat["id"],),
                    )
                    await db.execute(
                        """WITH RECURSIVE desc(id) AS (
                               SELECT ? UNION ALL
                               SELECT f.id FROM folders f JOIN desc d ON f.parent_id = d.id
                           )
                           UPDATE files SET stale = 1
                           WHERE folder_id IN (SELECT id FROM desc) AND stale = 0""",
                        (cat["id"],),
                    )
                    stale_folders += 1
                    logger.info("Quick scan: folder missing '%s'", cat["rel_path"])

            # Recover stale folders that are back
            for name, cat in cat_folder_names.items():
                if name in disk_folders:
                    await db.execute(
                        "UPDATE folders SET stale = 0 WHERE id = ? AND stale = 1",
                        (cat["id"],),
                    )

            # --- Files ---

            # New or recovered files on disk
            for name, info in disk_files.items():
                if name not in cat_file_names:
                    # New file
                    rel = os.path.join(parent_rel, name) if parent_rel else name
                    full = os.path.join(root_path, rel)
                    type_high, type_low = classify_file(name)
                    is_hidden = 1 if name.startswith(".") else parent_hidden
                    cursor = await db.execute(
                        """INSERT INTO files
                           (filename, full_path, rel_path, location_id, folder_id,
                            file_type_high, file_type_low, file_size,
                            description, tags,
                            created_date, modified_date, date_cataloged, date_last_seen,
                            scan_id, stale, hidden, dup_exclude, inode)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, NULL, 0, ?, ?, ?)""",
                        (
                            name,
                            full,
                            rel,
                            location_id,
                            folder_id,
                            type_high,
                            type_low,
                            info["size"],
                            now_iso,
                            now_iso,
                            now_iso,
                            now_iso,
                            is_hidden,
                            parent_dup_exclude,
                            info.get("inode", 0),
                        ),
                    )
                    new_files += 1
                    new_file_ids.append(cursor.lastrowid)
                    stats_added.append((folder_id, info["size"], type_high, is_hidden))
                else:
                    cat = cat_file_names[name]
                    # Recover stale files that are back
                    if cat["stale"]:
                        await db.execute(
                            "UPDATE files SET stale = 0, file_size = ?, date_last_seen = ? WHERE id = ?",
                            (info["size"], now_iso, cat["id"]),
                        )
                        recovered_files += 1
                        stats_added.append(
                            (
                                folder_id,
                                info["size"],
                                cat["file_type_high"],
                                cat["hidden"],
                            )
                        )

            # Missing files — mark stale
            for name, cat in cat_file_names.items():
                if name not in disk_files and not cat["stale"]:
                    await db.execute(
                        "UPDATE files SET stale = 1 WHERE id = ?",
                        (cat["id"],),
                    )
                    stale_files += 1
                    stats_removed.append(
                        (
                            folder_id,
                            cat["file_size"] or 0,
                            cat["file_type_high"],
                            cat["hidden"],
                        )
                    )

            await db.commit()

        # Hash new files
        if new_file_ids:
            activity_update(act_name, progress=f"hashing {len(new_file_ids)} files")
            await _hash_new_files(new_file_ids, location_id, agent_id)

        # Update stats incrementally — only the affected folder and its ancestors
        if stats_added or stats_removed:
            activity_update(act_name, progress="updating stats")
            await update_stats_for_files(
                location_id,
                added=stats_added or None,
                removed=stats_removed or None,
            )
            await post_op_stats()

        # Recalc dup counts if we added files
        if new_file_ids:
            await recalculate_dup_counts(
                source=f"quick scan {label} ({len(new_file_ids)} new files)"
            )

        logger.info(
            "Quick scan complete: %s — %d new folders, %d new files, "
            "%d stale folders, %d stale files, %d recovered",
            label,
            new_folders,
            new_files,
            stale_folders,
            stale_files,
            recovered_files,
        )

        await broadcast(
            {
                "type": "scan_completed",
                "locationId": location_id,
                "location": label,
                "quickScan": True,
                "newFolders": new_folders,
                "newFiles": new_files,
                "staleFolders": stale_folders,
                "staleFiles": stale_files,
                "recoveredFiles": recovered_files,
            }
        )

        return {
            "new_folders": new_folders,
            "new_files": new_files,
            "stale_folders": stale_folders,
            "stale_files": stale_files,
            "recovered_files": recovered_files,
        }

    except Exception as exc:
        logger.exception("Quick scan failed for %s", label)
        await broadcast(
            {
                "type": "scan_error",
                "locationId": location_id,
                "location": label,
                "error": str(exc),
            }
        )
        raise
    finally:
        unregister(act_name)


async def _hash_new_files(file_ids: list[int], location_id: int, agent_id: int):
    """Hash new files via the agent and store in hashes.db."""
    async with read_db() as db:
        ph = ",".join("?" for _ in file_ids)
        rows = await db.execute_fetchall(
            f"SELECT id, full_path, file_size FROM files WHERE id IN ({ph})",
            file_ids,
        )

    for row in rows:
        try:
            result = await fs.file_hash(row["full_path"], location_id)
            hash_fast = (
                result[0] if isinstance(result, tuple) else result.get("hash_fast")
            )
            if hash_fast:
                async with hashes_writer() as hdb:
                    await hdb.execute(
                        "INSERT INTO file_hashes "
                        "(file_id, location_id, file_size, hash_partial, hash_fast, hash_strong) "
                        "VALUES (?, ?, ?, NULL, ?, NULL) "
                        "ON CONFLICT(file_id) DO UPDATE SET hash_fast=excluded.hash_fast",
                        (row["id"], location_id, row["file_size"], hash_fast),
                    )
        except Exception as e:
            logger.warning("Quick scan: hash failed for %s: %s", row["full_path"], e)
