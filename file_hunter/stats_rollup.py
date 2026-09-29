"""Folder and location counters computed from scratch for one location.

Pure computation and SQL, shared by the server's recalculation
(services/sizes.py, async) and scripts/repair_location_stats.py (plain
sqlite3). Each folder's counters include everything beneath it.
"""

import json
import operator
from collections import defaultdict

# Per-folder direct values, read from the catalog (parameter: location_id)
DIRECT_SQL = (
    "SELECT folder_id, SUM(file_size) AS total, COUNT(*) AS cnt "
    "FROM files WHERE location_id = ? AND stale = 0 GROUP BY folder_id"
)
HIDDEN_SQL = (
    "SELECT folder_id, COUNT(*) AS cnt "
    "FROM files WHERE location_id = ? AND stale = 0 AND hidden = 1 "
    "GROUP BY folder_id"
)
TYPES_SQL = (
    "SELECT folder_id, file_type_high, COUNT(*) AS cnt "
    "FROM files WHERE location_id = ? AND stale = 0 "
    "GROUP BY folder_id, file_type_high"
)
FOLDERS_SQL = "SELECT id, parent_id FROM folders WHERE location_id = ?"

# Duplicates: file ids from hashes.db, then their folders from the catalog
DUP_FILES_SQL = (
    "SELECT file_id FROM active_hashes WHERE location_id = ? AND dup_count > 0"
)
DUP_BATCH = 500


def dup_folders_sql(n: int) -> str:
    return f"SELECT folder_id FROM files WHERE id IN ({','.join('?' * n)})"


FOLDER_STATS_UPSERT = (
    "INSERT INTO folder_stats "
    "(folder_id, location_id, file_count, total_size, "
    "duplicate_count, hidden_count, type_counts) "
    "VALUES (?, ?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(folder_id) DO UPDATE SET "
    "file_count=excluded.file_count, total_size=excluded.total_size, "
    "duplicate_count=excluded.duplicate_count, hidden_count=excluded.hidden_count, "
    "type_counts=excluded.type_counts"
)
LOCATION_STATS_UPSERT = (
    "INSERT INTO location_stats "
    "(location_id, file_count, total_size, "
    "duplicate_count, hidden_count, type_counts) "
    "VALUES (?, ?, ?, ?, ?, ?) "
    "ON CONFLICT(location_id) DO UPDATE SET "
    "file_count=excluded.file_count, total_size=excluded.total_size, "
    "duplicate_count=excluded.duplicate_count, hidden_count=excluded.hidden_count, "
    "type_counts=excluded.type_counts"
)


def folder_tree(folder_rows):
    """(children_of, all_folder_ids) from (id, parent_id) rows; top-level
    folders are children of None."""
    children_of: dict[int | None, list[int]] = {}
    all_folder_ids: list[int] = []
    for f in folder_rows:
        all_folder_ids.append(f["id"])
        children_of.setdefault(f["parent_id"], []).append(f["id"])
    return children_of, all_folder_ids


def split_root(pairs):
    """(per-folder dict, value for files at the location root) from
    (folder_id, value) pairs, where folder_id None is the root."""
    direct = {}
    root = 0
    for fid, value in pairs:
        if fid is None:
            root = value
        else:
            direct[fid] = value
    return direct, root


def rollup(children_of, direct, zero, add):
    """Each folder's value plus everything beneath it, bottom-up."""
    cum = {}

    def accumulate(fid):
        value = direct.get(fid, zero)
        for child_id in children_of.get(fid, []):
            accumulate(child_id)
            value = add(value, cum[child_id])
        cum[fid] = value

    for root_id in children_of.get(None, []):
        accumulate(root_id)
    return cum


def merge_type_counts(a: dict, b: dict) -> dict:
    merged = dict(a)
    for k, v in b.items():
        merged[k] = merged.get(k, 0) + v
    return merged


def location_dup_counts(dup_folder_counts, folder_rows):
    """(cum_dup per folder, location total) from duplicate files per folder."""
    children_of, all_folder_ids = folder_tree(folder_rows)
    direct_dup, root_dup = split_root(dup_folder_counts.items())
    cum_dup = rollup(children_of, direct_dup, 0, operator.add)
    loc_dup = root_dup + sum(direct_dup.get(fid, 0) for fid in all_folder_ids)
    return cum_dup, loc_dup, all_folder_ids


def location_counters(
    location_id, direct_rows, dup_folder_counts, hidden_rows, type_rows, folder_rows
):
    """All counters for a location.

    Returns (folder_params, location_params, cum_dup, loc_dup): parameters
    for FOLDER_STATS_UPSERT (one tuple per folder) and LOCATION_STATS_UPSERT,
    and the duplicate counts for patching the stats cache.
    """
    children_of, all_folder_ids = folder_tree(folder_rows)

    direct_size, root_size = split_root(
        (r["folder_id"], r["total"] or 0) for r in direct_rows
    )
    direct_count, root_count = split_root(
        (r["folder_id"], r["cnt"] or 0) for r in direct_rows
    )
    direct_dup, root_dup = split_root(dup_folder_counts.items())
    direct_hidden, root_hidden = split_root(
        (r["folder_id"], r["cnt"] or 0) for r in hidden_rows
    )
    direct_types: dict[int | None, dict[str, int]] = defaultdict(dict)
    for r in type_rows:
        direct_types[r["folder_id"]][r["file_type_high"] or ""] = r["cnt"] or 0
    root_types = dict(direct_types.get(None, {}))

    cum_size = rollup(children_of, direct_size, 0, operator.add)
    cum_count = rollup(children_of, direct_count, 0, operator.add)
    cum_dup = rollup(children_of, direct_dup, 0, operator.add)
    cum_hidden = rollup(children_of, direct_hidden, 0, operator.add)
    cum_types = rollup(children_of, direct_types, {}, merge_type_counts)

    loc_size = root_size + sum(direct_size.get(fid, 0) for fid in all_folder_ids)
    loc_count = root_count + sum(direct_count.get(fid, 0) for fid in all_folder_ids)
    loc_dup = root_dup + sum(direct_dup.get(fid, 0) for fid in all_folder_ids)
    loc_hidden = root_hidden + sum(direct_hidden.get(fid, 0) for fid in all_folder_ids)
    loc_types = dict(root_types)
    for fid in all_folder_ids:
        for ftype, cnt in direct_types.get(fid, {}).items():
            loc_types[ftype] = loc_types.get(ftype, 0) + cnt

    folder_params = [
        (
            fid,
            location_id,
            cum_count.get(fid, 0),
            cum_size.get(fid, 0),
            cum_dup.get(fid, 0),
            cum_hidden.get(fid, 0),
            json.dumps(cum_types.get(fid, {})),
        )
        for fid in all_folder_ids
    ]
    location_params = (
        location_id,
        loc_count,
        loc_size,
        loc_dup,
        loc_hidden,
        json.dumps(loc_types),
    )
    return folder_params, location_params, cum_dup, loc_dup
