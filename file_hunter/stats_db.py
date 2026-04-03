"""Stats database — separate SQLite file for real-time counters.

Own writer, own read connections. Never contends with the catalog writer
or hashes writer. Counter updates cascade through the folder tree as a
background operation — the UI reads current values without blocking ingest.

Counters are maintained incrementally during scan/import/delete operations.
The correction pass (recalculate_location_sizes) becomes a repair tool,
not a required step in every scan.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import aiosqlite

from file_hunter.config import load_config
from file_hunter.core import format_size
from file_hunter.db import read_db

_write_db = None
_write_lock = asyncio.Lock()

_SCHEMA = """
CREATE TABLE IF NOT EXISTS folder_stats (
    folder_id INTEGER PRIMARY KEY,
    location_id INTEGER NOT NULL,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    type_counts TEXT NOT NULL DEFAULT '{}',
    hidden_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS location_stats (
    location_id INTEGER PRIMARY KEY,
    file_count INTEGER NOT NULL DEFAULT 0,
    total_size INTEGER NOT NULL DEFAULT 0,
    duplicate_count INTEGER NOT NULL DEFAULT 0,
    type_counts TEXT NOT NULL DEFAULT '{}',
    hidden_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_folder_stats_location
    ON folder_stats(location_id);
"""


def _stats_db_path() -> Path:
    config = load_config()
    catalog_path = Path(config.get("database", "data/file_hunter.db"))
    if not catalog_path.is_absolute():
        catalog_path = Path(__file__).resolve().parent.parent / catalog_path
    return catalog_path.parent / "stats.db"


async def init_stats_db():
    """Create stats.db and schema if it doesn't exist.

    Called during app startup. Fast — just CREATE TABLE IF NOT EXISTS.
    No data migration.
    """
    db_path = _stats_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("PRAGMA journal_mode=WAL")
        for stmt in _SCHEMA.split(";"):
            stmt = stmt.strip()
            if stmt:
                await conn.execute(stmt)
        await conn.commit()
    finally:
        await conn.close()


async def _get_write_db() -> aiosqlite.Connection:
    """Lazy-init the single stats write connection."""
    global _write_db
    if _write_db is None:
        db_path = _stats_db_path()
        _write_db = await aiosqlite.connect(db_path)
        _write_db.row_factory = aiosqlite.Row
        await _write_db.execute("PRAGMA journal_mode=WAL")
    return _write_db


@asynccontextmanager
async def stats_writer():
    """Acquire exclusive write access to the stats database.

    Same pattern as catalog db_writer() — own lock, own connection.
    Auto-commits on clean exit; rolls back on exception.
    """
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


async def open_stats_connection() -> aiosqlite.Connection:
    """Open a read-only stats DB connection (caller must close it)."""
    db_path = _stats_db_path()
    conn = await aiosqlite.connect(db_path)
    conn.row_factory = aiosqlite.Row
    await conn.execute("PRAGMA journal_mode=WAL")
    return conn


@asynccontextmanager
async def read_stats():
    """Open a stats read connection, yield it, close on exit.

    Usage:
        async with read_stats() as db:
            rows = await db.execute_fetchall("SELECT ...")
    """
    conn = await open_stats_connection()
    try:
        yield conn
    finally:
        await conn.close()


async def apply_file_deltas(
    location_id: int,
    folder_parents: dict[int, int | None],
    added: list[tuple] | None = None,
    removed: list[tuple] | None = None,
):
    """Apply file add/remove deltas to stats.db, cascading up the folder tree.

    folder_parents: {folder_id: parent_id} — the folder hierarchy.
        Read from catalog once, reused across batches.

    added: list of (folder_id, file_size, file_type_high, is_hidden)
        for newly ingested files.

    removed: list of (folder_id, file_size, file_type_high, is_hidden)
        for stale/deleted files.

    Computes per-folder deltas, walks up parent chain accumulating at
    each level, writes all affected folder_stats + location_stats.
    Runs on the stats writer — no catalog contention.
    """
    if not added and not removed:
        return

    # Step 1: compute direct deltas per folder (from actual files only)
    direct: dict[int | None, list[int, int, int, dict]] = {}
    #                           count, size, hidden, {type: count}

    def _apply(folder_id, file_size, file_type_high, is_hidden, sign):
        if folder_id not in direct:
            direct[folder_id] = [0, 0, 0, {}]
        d = direct[folder_id]
        d[0] += sign  # count
        d[1] += file_size * sign  # size
        if is_hidden:
            d[2] += sign  # hidden
        if file_type_high:
            d[3][file_type_high] = d[3].get(file_type_high, 0) + sign

    for folder_id, file_size, file_type_high, is_hidden in added or []:
        _apply(folder_id, file_size, file_type_high, is_hidden, 1)
    for folder_id, file_size, file_type_high, is_hidden in removed or []:
        _apply(folder_id, file_size, file_type_high, is_hidden, -1)

    # Step 2: cascade — each folder's delta includes its own files plus
    # all descendant folders' deltas. Build cumulative per folder by walking
    # up from each direct-delta folder.
    cumulative: dict[int, list] = {}  # folder_id -> [count, size, hidden, {types}]

    for folder_id in list(direct.keys()):
        if folder_id is None:
            continue
        d = direct[folder_id]
        # Walk up the chain, adding this folder's direct delta to every ancestor
        current = folder_id
        while current is not None:
            if current not in cumulative:
                cumulative[current] = [0, 0, 0, {}]
            c = cumulative[current]
            c[0] += d[0]
            c[1] += d[1]
            c[2] += d[2]
            for t, cnt in d[3].items():
                c[3][t] = c[3].get(t, 0) + cnt
            current = folder_parents.get(current)

    if not cumulative:
        return

    # Step 3: location delta = sum of ALL direct deltas (every file, regardless of folder)
    loc_count = 0
    loc_size = 0
    loc_hidden = 0
    loc_types: dict[str, int] = {}
    for d in direct.values():
        loc_count += d[0]
        loc_size += d[1]
        loc_hidden += d[2]
        for t, cnt in d[3].items():
            loc_types[t] = loc_types.get(t, 0) + cnt

    # Step 4: write to stats.db
    async with stats_writer() as sdb:
        for fid, delta in cumulative.items():
            if delta[0] == 0 and delta[1] == 0 and delta[2] == 0 and not delta[3]:
                continue

            row = await sdb.execute(
                "SELECT file_count, total_size, hidden_count, type_counts "
                "FROM folder_stats WHERE folder_id = ?",
                (fid,),
            )
            existing = await row.fetchone()

            if existing:
                new_count = max(0, (existing["file_count"] or 0) + delta[0])
                new_size = max(0, (existing["total_size"] or 0) + delta[1])
                new_hidden = max(0, (existing["hidden_count"] or 0) + delta[2])
                cur_types = json.loads(existing["type_counts"] or "{}")
                for t, cnt in delta[3].items():
                    cur_types[t] = cur_types.get(t, 0) + cnt
                    if cur_types[t] <= 0:
                        cur_types.pop(t, None)
                await sdb.execute(
                    "UPDATE folder_stats SET file_count=?, total_size=?, "
                    "hidden_count=?, type_counts=? WHERE folder_id=?",
                    (new_count, new_size, new_hidden, json.dumps(cur_types), fid),
                )
            else:
                init_types = {t: c for t, c in delta[3].items() if c > 0}
                await sdb.execute(
                    "INSERT INTO folder_stats "
                    "(folder_id, location_id, file_count, total_size, "
                    "hidden_count, type_counts) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        fid,
                        location_id,
                        max(0, delta[0]),
                        max(0, delta[1]),
                        max(0, delta[2]),
                        json.dumps(init_types),
                    ),
                )

        # Update location_stats
        loc_existing = await sdb.execute(
            "SELECT file_count, total_size, hidden_count, type_counts "
            "FROM location_stats WHERE location_id = ?",
            (location_id,),
        )
        loc_row = await loc_existing.fetchone()

        if loc_row:
            new_fc = max(0, (loc_row["file_count"] or 0) + loc_count)
            new_ts = max(0, (loc_row["total_size"] or 0) + loc_size)
            new_hc = max(0, (loc_row["hidden_count"] or 0) + loc_hidden)
            lt = json.loads(loc_row["type_counts"] or "{}")
            for t, cnt in loc_types.items():
                lt[t] = lt.get(t, 0) + cnt
                if lt[t] <= 0:
                    lt.pop(t, None)
            await sdb.execute(
                "UPDATE location_stats SET file_count=?, total_size=?, "
                "hidden_count=?, type_counts=? WHERE location_id=?",
                (new_fc, new_ts, new_hc, json.dumps(lt), location_id),
            )
        else:
            init_lt = {t: c for t, c in loc_types.items() if c > 0}
            await sdb.execute(
                "INSERT INTO location_stats "
                "(location_id, file_count, total_size, hidden_count, type_counts) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    location_id,
                    max(0, loc_count),
                    max(0, loc_size),
                    max(0, loc_hidden),
                    json.dumps(init_lt),
                ),
            )

    # Patch the stats cache so the API returns current values
    from file_hunter.services.stats import _cache

    loc_entry = _cache.get(f"loc:{location_id}")
    if loc_entry is not None:
        new_fc = (loc_row["file_count"] or 0) + loc_count if loc_row else loc_count
        new_ts = (loc_row["total_size"] or 0) + loc_size if loc_row else loc_size
        new_hc = (loc_row["hidden_count"] or 0) + loc_hidden if loc_row else loc_hidden
        loc_entry["fileCount"] = max(0, new_fc)
        loc_entry["totalSize"] = max(0, new_ts)
        loc_entry["totalSizeFormatted"] = format_size(max(0, new_ts))
        loc_entry["hiddenFiles"] = max(0, new_hc)

    return {
        "fileCount": (loc_row["file_count"] or 0) + loc_count if loc_row else loc_count,
        "totalSize": (loc_row["total_size"] or 0) + loc_size if loc_row else loc_size,
    }


async def update_stats_for_files(
    location_id: int,
    added: list[tuple] | None = None,
    removed: list[tuple] | None = None,
):
    """Convenience wrapper: read folder hierarchy from catalog, apply deltas.

    added/removed: list of (folder_id, file_size, file_type_high, is_hidden)

    Reads folder parents from catalog (fast indexed query), then delegates
    to apply_file_deltas for the cascade.
    """
    if not added and not removed:
        return
    async with read_db() as rdb:
        fp_rows = await rdb.execute_fetchall(
            "SELECT id, parent_id FROM folders WHERE location_id = ?",
            (location_id,),
        )
    folder_parents = {r["id"]: r["parent_id"] for r in fp_rows}
    await apply_file_deltas(location_id, folder_parents, added=added, removed=removed)


async def apply_dup_deltas(
    location_id: int,
    folder_parents: dict[int, int | None],
    deltas: list[tuple[int | None, int]],
):
    """Apply duplicate count deltas to stats.db, cascading up the folder tree.

    deltas: list of (folder_id, delta) where delta is +1 (file became a
    duplicate) or -1 (file is no longer a duplicate).

    Same cascade strategy as apply_file_deltas but only touches
    duplicate_count. Also patches the stats cache so the API returns
    current values immediately.
    """
    if not deltas:
        return

    # Accumulate direct deltas per folder
    direct: dict[int | None, int] = {}
    for folder_id, delta in deltas:
        direct[folder_id] = direct.get(folder_id, 0) + delta

    # Cascade up the tree
    cumulative: dict[int, int] = {}
    for folder_id, d in direct.items():
        if folder_id is None:
            continue
        current = folder_id
        while current is not None:
            cumulative[current] = cumulative.get(current, 0) + d
            current = folder_parents.get(current)

    # Location delta = sum of all direct deltas
    loc_delta = sum(direct.values())

    if not cumulative and loc_delta == 0:
        return

    # Write to stats.db
    async with stats_writer() as sdb:
        for fid, delta in cumulative.items():
            if delta == 0:
                continue
            await sdb.execute(
                "UPDATE folder_stats SET duplicate_count = MAX(0, duplicate_count + ?) "
                "WHERE folder_id = ?",
                (delta, fid),
            )

        if loc_delta != 0:
            await sdb.execute(
                "UPDATE location_stats SET duplicate_count = MAX(0, duplicate_count + ?) "
                "WHERE location_id = ?",
                (loc_delta, location_id),
            )

    # Patch the stats cache
    from file_hunter.services.stats import _cache

    loc_entry = _cache.get(f"loc:{location_id}")
    if loc_entry is not None:
        loc_entry["duplicateFiles"] = max(
            0, loc_entry.get("duplicateFiles", 0) + loc_delta
        )
    for fid, delta in cumulative.items():
        fld_entry = _cache.get(f"folder:{fid}")
        if fld_entry is not None:
            fld_entry["duplicateFiles"] = max(
                0, fld_entry.get("duplicateFiles", 0) + delta
            )


async def remove_folder_stats(folder_ids: list[int]):
    """Remove folder_stats entries for deleted folders."""
    if not folder_ids:
        return
    for i in range(0, len(folder_ids), 500):
        batch = folder_ids[i : i + 500]
        ph = ",".join("?" for _ in batch)
        async with stats_writer() as sdb:
            await sdb.execute(
                f"DELETE FROM folder_stats WHERE folder_id IN ({ph})",
                batch,
            )


async def remove_location_stats(location_id: int):
    """Remove all stats for a location (folder_stats + location_stats)."""
    async with stats_writer() as sdb:
        await sdb.execute(
            "DELETE FROM folder_stats WHERE location_id = ?", (location_id,)
        )
        await sdb.execute(
            "DELETE FROM location_stats WHERE location_id = ?", (location_id,)
        )


async def close_stats_db():
    """Close the stats write connection. Called on shutdown."""
    global _write_db
    if _write_db is not None:
        await _write_db.close()
        _write_db = None
