"""Dynamic search query builder.

Search results are cached in a temporary SQLite DB so paging and
re-sorting work against the small result set, not the full files table.
"""

import asyncio
import os
import re
import secrets
import sqlite3
import logging
from pathlib import Path

from file_hunter.config import load_config
from file_hunter.db import open_connection, read_db, folder_tree_ids
from file_hunter.hashes_db import get_file_hashes, read_hashes
from file_hunter.services.dup_counts import batch_dup_counts
from file_hunter.services.settings import get_setting
from file_hunter.services.tags import parse_tags, tag_filter_sql
from file_hunter.stats_db import stats_db_path

logger = logging.getLogger(__name__)

PAGE_SIZE = 120

SORT_COLUMNS = {
    "name": "f.filename COLLATE NOCASE",
    "type": "f.file_type_low",
    "size": "f.file_size",
    "date": "f.modified_date",
    "dups": "dup_count",
}


async def search_by_hash(hash_val: str, *, page=0, sort="name", sort_dir="asc") -> dict:
    """Fast path for dup badge clicks. No temp DB, no search pipeline."""
    # Find file IDs — UNION lets each branch use its own index
    async with read_hashes() as hdb:
        hash_rows = await hdb.execute_fetchall(
            "SELECT file_id FROM active_hashes WHERE hash_strong = ? "
            "UNION "
            "SELECT file_id FROM active_hashes WHERE hash_fast = ?",
            (hash_val, hash_val),
        )
    if not hash_rows:
        return {"items": [], "folders": [], "total": 0, "page": 0, "pageSize": PAGE_SIZE}

    file_ids = [r["file_id"] for r in hash_rows]
    total = len(file_ids)

    # Fetch file details from catalog (paged)
    col = SORT_COLUMNS.get(sort, "f.filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"
    # Sort by dup_count needs the hash data — sort in Python for that case
    sort_in_sql = sort != "dups"

    ph = ",".join("?" for _ in file_ids)
    async with read_db() as db:
        if sort_in_sql:
            rows = await db.execute_fetchall(
                f"SELECT f.id, f.filename, f.file_type_high, f.file_type_low, "
                f"f.file_size, f.modified_date, f.stale, f.hidden, "
                f"f.location_id, l.name as location_name "
                f"FROM files f JOIN locations l ON l.id = f.location_id "
                f"WHERE f.id IN ({ph}) "
                f"ORDER BY {col} {direction}",
                file_ids,
            )
        else:
            rows = await db.execute_fetchall(
                f"SELECT f.id, f.filename, f.file_type_high, f.file_type_low, "
                f"f.file_size, f.modified_date, f.stale, f.hidden, "
                f"f.location_id, l.name as location_name "
                f"FROM files f JOIN locations l ON l.id = f.location_id "
                f"WHERE f.id IN ({ph})",
                file_ids,
            )

    # Fetch hashes and dup counts for all results
    page_ids = [r["id"] for r in rows]
    hash_map = await get_file_hashes(page_ids)

    strong_list = [h["hash_strong"] for h in hash_map.values() if h.get("hash_strong")]
    fast_list = [
        h["hash_fast"]
        for h in hash_map.values()
        if not h.get("hash_strong") and h.get("hash_fast")
    ]
    live_dups = await batch_dup_counts(strong_hashes=strong_list, fast_hashes=fast_list)

    items = []
    for r in rows:
        h = hash_map.get(r["id"], {})
        hs = h.get("hash_strong")
        hf = h.get("hash_fast")
        items.append({
            "id": r["id"],
            "name": r["filename"],
            "typeHigh": r["file_type_high"],
            "typeLow": r["file_type_low"],
            "size": r["file_size"],
            "date": r["modified_date"],
            "dups": live_dups.get(hs or hf, 0),
            "hashStrong": hs,
            "hashFast": hf,
            "stale": bool(r["stale"]),
            "missing": False,
            "hidden": bool(r["hidden"]),
            "location": r["location_name"],
            "locationId": r["location_id"],
        })

    # Page the results
    offset = page * PAGE_SIZE
    paged = items[offset : offset + PAGE_SIZE]

    return {
        "items": paged,
        "folders": [],
        "total": total,
        "page": page,
        "pageSize": PAGE_SIZE,
    }


# ---------------------------------------------------------------------------
# Search context — holds per-session search state (cache + cancel handle)
# ---------------------------------------------------------------------------


class SearchContext:
    """Per-caller search state. Each context has its own search cache and
    cancel handle so concurrent searches don't interfere."""

    def __init__(self):
        self.search_id: str | None = None
        self.search_db_path: Path | None = None
        self.active_conn = None

    def cancel(self):
        """Interrupt any running search query on this context."""
        conn = self.active_conn
        if conn is not None:
            try:
                conn._conn.interrupt()
            except Exception:
                pass

    def set_active_conn(self, conn):
        self.active_conn = conn

    def clear_active_conn(self):
        self.active_conn = None

    def cleanup(self):
        """Remove temp search DB if it exists."""
        if self.search_db_path and os.path.exists(self.search_db_path):
            try:
                os.unlink(self.search_db_path)
            except OSError:
                pass
        self.search_id = None
        self.search_db_path = None


# Shared context for the web UI (single-user cancel-on-new behaviour)
ui_context = SearchContext()


def cancel_active_search():
    """Interrupt any running search on the shared UI context."""
    ui_context.cancel()


SEARCH_SCHEMA = """
CREATE TABLE results (
    file_id INTEGER PRIMARY KEY,
    filename TEXT NOT NULL,
    file_type_high TEXT,
    file_type_low TEXT,
    file_size INTEGER,
    modified_date TEXT,
    stale INTEGER NOT NULL DEFAULT 0,
    hidden INTEGER NOT NULL DEFAULT 0,
    location_id INTEGER,
    location_name TEXT,
    hash_strong TEXT,
    hash_fast TEXT,
    dup_count INTEGER NOT NULL DEFAULT 0,
    file_count INTEGER
);
CREATE INDEX idx_results_name ON results(filename);
CREATE INDEX idx_results_size ON results(file_size);
CREATE INDEX idx_results_date ON results(modified_date);
CREATE INDEX idx_results_type ON results(file_type_low);
CREATE INDEX idx_results_dups ON results(dup_count);
"""

RESULT_SORT_COLUMNS = {
    "name": "filename COLLATE NOCASE",
    "type": "file_type_low",
    "size": "file_size",
    "date": "modified_date",
    "dups": "dup_count",
}


def search_db_dir() -> Path:
    config = load_config()
    return Path(config.get("data_dir", "data")) / "temp"


async def populate_search_db(db, where, params, search_path, ctx=None):
    """Run the search query and populate a temp SQLite DB with results."""
    if ctx is None:
        ctx = ui_context
    ctx.set_active_conn(db)
    try:
        return await do_populate_search_db(db, where, params, search_path)
    finally:
        ctx.clear_active_conn()


async def do_populate_search_db(db, where, params, search_path):
    """Inner search population — separated so _active_search_conn is always cleared."""
    # Fetch all matching file IDs + display data
    rows = await db.execute_fetchall(
        f"""SELECT f.id, f.filename, f.file_type_high, f.file_type_low,
                   f.file_size, f.modified_date, f.stale, f.hidden,
                   f.location_id, l.name as location_name
            FROM files f
            JOIN locations l ON l.id = f.location_id
            WHERE {where}""",
        params,
    )

    if not rows:
        # Create empty DB
        sdb = sqlite3.connect(str(search_path))
        sdb.executescript(SEARCH_SCHEMA)
        sdb.close()
        return 0

    # Fetch hashes and dup counts
    file_ids = [r["id"] for r in rows]
    hash_map = await get_file_hashes(file_ids)

    strong_list = [h["hash_strong"] for h in hash_map.values() if h.get("hash_strong")]
    fast_list = [
        h["hash_fast"]
        for h in hash_map.values()
        if not h.get("hash_strong") and h.get("hash_fast")
    ]
    live_dups = await batch_dup_counts(strong_hashes=strong_list, fast_hashes=fast_list)

    # Build insert data
    insert_data = []
    for r in rows:
        h = hash_map.get(r["id"], {})
        hs = h.get("hash_strong")
        hf = h.get("hash_fast")
        dc = live_dups.get(hs or hf, 0)
        insert_data.append(
            (
                r["id"],
                r["filename"],
                r["file_type_high"],
                r["file_type_low"],
                r["file_size"],
                r["modified_date"],
                r["stale"],
                r["hidden"],
                r["location_id"],
                r["location_name"],
                hs,
                hf,
                dc,
                None,
            )
        )

    # Write to search DB in a thread (sync SQLite must not block event loop)
    await asyncio.to_thread(write_search_db, str(search_path), insert_data)
    return len(rows)


def write_search_db(search_path: str, insert_data: list):
    """Synchronous: write search results to a temp SQLite file."""
    if os.path.exists(search_path):
        os.unlink(search_path)
    sdb = sqlite3.connect(search_path)
    sdb.executescript(SEARCH_SCHEMA)
    for i in range(0, len(insert_data), 5000):
        sdb.executemany(
            "INSERT INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            insert_data[i : i + 5000],
        )
        sdb.commit()
    sdb.close()


def create_empty_search_db(search_path: str):
    """Create an empty search results DB (for folder-only searches)."""
    if os.path.exists(search_path):
        os.unlink(search_path)
    sdb = sqlite3.connect(search_path)
    sdb.executescript(SEARCH_SCHEMA)
    sdb.close()


def append_folder_results(search_path: str, folder_data: list):
    """Append folder results to an existing search DB."""
    sdb = sqlite3.connect(search_path)
    for i in range(0, len(folder_data), 5000):
        sdb.executemany(
            "INSERT OR IGNORE INTO results VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            folder_data[i : i + 5000],
        )
        sdb.commit()
    sdb.close()


def read_search_page(search_path, sort, sort_dir, page, focus_file_id=None):
    """Read a page from the search results DB."""
    col = RESULT_SORT_COLUMNS.get(sort, "filename")
    direction = "DESC" if sort_dir == "desc" else "ASC"

    sdb = sqlite3.connect(str(search_path))
    sdb.row_factory = sqlite3.Row

    # If focusing a specific file, compute which page it's on
    if focus_file_id:
        focus_row = sdb.execute(
            f"SELECT {col} as sort_val FROM results WHERE file_id = ?",
            (focus_file_id,),
        ).fetchone()
        if focus_row:
            sort_val = focus_row["sort_val"]
            op = "<" if direction == "ASC" else ">"
            pos_row = sdb.execute(
                f"SELECT COUNT(*) as pos FROM results "
                f"WHERE {col} {op} ? OR ({col} = ? AND file_id {op} ?)",
                (sort_val, sort_val, focus_file_id),
            ).fetchone()
            position = pos_row["pos"] if pos_row else 0
            page = position // PAGE_SIZE

    offset = page * PAGE_SIZE

    total_row = sdb.execute("SELECT COUNT(*) as c FROM results").fetchone()
    total = total_row["c"]

    folder_row = sdb.execute(
        "SELECT COUNT(*) as c FROM results WHERE file_type_high = 'folder'"
    ).fetchone()
    folder_total = folder_row["c"]

    rows = sdb.execute(
        f"SELECT * FROM results ORDER BY {col} {direction} LIMIT ? OFFSET ?",
        (PAGE_SIZE, offset),
    ).fetchall()

    sdb.close()

    items = []
    for r in rows:
        if r["file_type_high"] == "folder":
            items.append(
                {
                    "id": f"fld-{abs(r['file_id'])}",
                    "name": r["filename"],
                    "type": "folder",
                    "size": r["file_size"],
                    "fileCount": r["file_count"],
                    "dups": r["dup_count"],
                    "date": None,
                    "location": r["location_name"],
                    "locationId": r["location_id"],
                }
            )
        else:
            items.append(
                {
                    "id": r["file_id"],
                    "name": r["filename"],
                    "typeHigh": r["file_type_high"],
                    "typeLow": r["file_type_low"],
                    "size": r["file_size"],
                    "date": r["modified_date"],
                    "dups": r["dup_count"],
                    "hashStrong": r["hash_strong"],
                    "hashFast": r["hash_fast"],
                    "stale": bool(r["stale"]),
                    "missing": False,
                    "hidden": bool(r["hidden"]),
                    "location": r["location_name"],
                    "locationId": r["location_id"],
                }
            )

    return items, total, folder_total, page


def escape_like(value: str) -> str:
    """Escape LIKE special characters (% and _) for literal matching."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def build_scope_sql(db, location_id=None, folder_id=None):
    """Return (file_frag, folder_frag, params) for scope filtering.

    For folder scope, pre-fetches all descendant folder IDs so the main
    query uses a flat IN clause that SQLite can resolve via index.
    """
    if folder_id:
        folder_ids = await folder_tree_ids(db, folder_id)
        placeholders = ",".join("?" * len(folder_ids))
        return (
            f"f.folder_id IN ({placeholders})",
            f"fld.id IN ({placeholders})",
            folder_ids,
        )
    if location_id:
        return (
            "f.location_id = ?",
            "fld.location_id = ?",
            [location_id],
        )
    return ("", "", [])


SORT_COLUMNS = {
    "name": "f.filename COLLATE NOCASE",
    "type": "f.file_type_low",
    "size": "f.file_size",
    "date": "f.modified_date",
    "dups": "f.dup_count",
}


def parse_size(value: str) -> int | None:
    """Parse human-readable size string to bytes. E.g. '5MB' -> 5242880."""
    if not value:
        return None
    value = value.strip().upper()
    match = re.match(r"^([\d.]+)\s*(B|KB|MB|GB|TB)?$", value)
    if not match:
        # Try as raw number (bytes)
        try:
            return int(float(value))
        except ValueError:
            return None
    num = float(match.group(1))
    unit = match.group(2) or "B"
    multipliers = {
        "B": 1,
        "KB": 1024,
        "MB": 1048576,
        "GB": 1073741824,
        "TB": 1099511627776,
    }
    return int(num * multipliers[unit])


def int_text(value) -> str:
    try:
        return str(int(value))
    except (ValueError, TypeError):
        return ""


async def search_files(
    db,
    *,
    name=None,
    file_type=None,
    description=None,
    tags=None,
    size_min=None,
    size_max=None,
    date_from=None,
    date_to=None,
    name_match="anywhere",
    include_files=True,
    dupes_only=False,
    min_dups=None,
    max_dups=None,
    min_files=None,
    max_files=None,
    hash_strong=None,
    include_folders=False,
    location_id=None,
    folder_id=None,
    page=0,
    sort="name",
    sort_dir="asc",
    cached_total=None,
    search_id=None,
    focus_file_id=None,
    ctx=None,
):
    """Basic search: each filter is an include condition of advanced search,
    plus "duplicates only" and a hash match, which only basic search has.
    Returns paged envelope."""
    conditions = [
        {"field": "name", "op": "include", "value": name or "", "match": name_match},
        {"field": "type", "op": "include", "value": file_type or ""},
        {"field": "description", "op": "include", "value": description or ""},
        {"field": "tags", "op": "include", "value": tags or ""},
        {"field": "size", "op": "include", "min": size_min or "", "max": size_max or ""},
        {"field": "date", "op": "include", "from": date_from or "", "to": date_to or ""},
        {"field": "duplicates", "op": "include", "from": min_dups or "", "to": max_dups or ""},
        # A file count that isn't an integer is ignored
        {"field": "files", "op": "include", "from": int_text(min_files), "to": int_text(max_files)},
    ]

    extra_frags = []
    extra_params = []
    if dupes_only:
        extra_frags.append("f.dup_count > 0")
    if hash_strong:
        # Hashes live in hashes.db, not catalog — look up file IDs there
        async with read_hashes() as hdb:
            hash_rows = await hdb.execute_fetchall(
                "SELECT file_id FROM active_hashes "
                "WHERE hash_strong = ? OR hash_fast = ?",
                (hash_strong, hash_strong),
            )
        if hash_rows:
            hash_file_ids = [r["file_id"] for r in hash_rows]
            ph = ",".join("?" for _ in hash_file_ids)
            extra_frags.append(f"f.id IN ({ph})")
            extra_params.extend(hash_file_ids)
        else:
            extra_frags.append("0")

    return await run_search(
        db,
        conditions=conditions,
        extra_frags=extra_frags,
        extra_params=extra_params,
        include_files=include_files,
        include_folders=include_folders,
        location_id=location_id,
        folder_id=folder_id,
        page=page,
        sort=sort,
        sort_dir=sort_dir,
        search_id=search_id,
        focus_file_id=focus_file_id,
        ctx=ctx,
    )


async def run_search(
    db,
    *,
    conditions,
    extra_frags,
    extra_params,
    include_files,
    include_folders,
    location_id,
    folder_id,
    page,
    sort,
    sort_dir,
    search_id,
    focus_file_id,
    ctx,
):
    """Run a search into the caller's search DB, or page through the cached
    one when search_id still matches it. Returns the paged envelope."""
    show_hidden = await get_setting(db, "showHiddenFiles") == "1"
    scope_file_frag, scope_folder_frag, scope_params = await build_scope_sql(
        db, location_id=location_id, folder_id=folder_id
    )

    where_parts = []
    where_params = list(scope_params)

    if scope_file_frag:
        where_parts.append(scope_file_frag)

    if not show_hidden:
        where_parts.append("f.hidden = 0")

    for cond in conditions:
        frag, params = build_condition_sql(cond)
        if frag is None:
            continue
        if cond["op"] == "exclude":
            where_parts.append(f"NOT ({frag})")
        else:
            where_parts.append(f"({frag})")
        where_params.extend(params)

    where_parts.extend(extra_frags)
    where_params.extend(extra_params)

    where = " AND ".join(where_parts) if where_parts else "1=1"

    if ctx is None:
        ctx = ui_context

    total = 0
    folder_total = 0
    items = []

    # Cache check — files + folders are both in the cached DB
    if (
        search_id
        and ctx.search_id == search_id
        and ctx.search_db_path
        and os.path.exists(ctx.search_db_path)
    ):
        items, total, folder_total, page = await asyncio.to_thread(
            read_search_page, ctx.search_db_path, sort, sort_dir, page, focus_file_id
        )
    elif include_files or include_folders:
        ctx.cancel()

        search_dir = search_db_dir()
        search_dir.mkdir(parents=True, exist_ok=True)
        new_id = secrets.token_hex(8)
        search_path = search_dir / f"search-{new_id}.db"

        if ctx.search_db_path and os.path.exists(ctx.search_db_path):
            try:
                os.unlink(ctx.search_db_path)
            except OSError:
                pass

        # Populate with file results
        if include_files:
            await populate_search_db(db, where, where_params, search_path, ctx=ctx)
        else:
            await asyncio.to_thread(create_empty_search_db, str(search_path))

        # Append folder results to the same search DB
        if include_folders:
            folder_insert = await build_folder_insert_data(
                conditions=conditions,
                show_hidden=show_hidden,
                scope_frag=scope_folder_frag,
                scope_params=scope_params,
            )
            if folder_insert:
                await asyncio.to_thread(
                    append_folder_results, str(search_path), folder_insert
                )

        ctx.search_id = new_id
        ctx.search_db_path = search_path
        search_id = new_id

        items, total, folder_total, page = await asyncio.to_thread(
            read_search_page, search_path, sort, sort_dir, page, focus_file_id
        )

    result = {
        "items": items,
        "folders": [],
        "total": total,
        "folderTotal": folder_total,
        "page": page,
        "pageSize": PAGE_SIZE,
        "searchId": search_id,
    }
    if focus_file_id:
        result["focusFileId"] = focus_file_id
    return result


# ── Advanced search helpers ──


def parse_conditions_from_params(params) -> list[dict]:
    """Parse indexed condition params (c0_field, c0_op, c0_value, etc.)."""
    conditions = []
    i = 0
    while True:
        field = params.get(f"c{i}_field")
        if field is None:
            break
        cond = {
            "field": field,
            "op": params.get(f"c{i}_op", "include"),
        }
        if field in ("size",):
            cond["min"] = params.get(f"c{i}_min", "")
            cond["max"] = params.get(f"c{i}_max", "")
        elif field in ("date", "duplicates", "files"):
            cond["from"] = params.get(f"c{i}_from", "")
            cond["to"] = params.get(f"c{i}_to", "")
        else:
            cond["value"] = params.get(f"c{i}_value", "")
            cond["match"] = params.get(f"c{i}_match", "")
        conditions.append(cond)
        i += 1
    return conditions


def build_name_like(value, match_mode, column="f.filename"):
    """Build SQL fragment + params for a name/folder LIKE condition."""
    if match_mode == "wildcard":
        escaped = escape_like(value)
        pattern = escaped.replace("*", "%").replace("?", "_")
        return f"{column} LIKE ? ESCAPE '\\'", [pattern]
    elif match_mode == "exact":
        return f"{column} = ?", [value]
    else:
        escaped = escape_like(value)
        match_patterns = {
            "starts": f"{escaped}%",
            "ends": f"%{escaped}",
        }
        pattern = match_patterns.get(match_mode, f"%{escaped}%")
        return f"{column} LIKE ? ESCAPE '\\'", [pattern]


def whole_number(text, minimum=None):
    """int(text), or None if it's blank, not a whole number, or below
    minimum."""
    if not text:
        return None
    try:
        value = int(text)
    except (ValueError, TypeError):
        return None
    if minimum is not None and value < minimum:
        return None
    return value


def range_sql(column, low, high):
    """(fragment, params) for low <= column <= high; a None bound is left
    out, and with neither it's (None, [])."""
    frags, params = [], []
    if low is not None:
        frags.append(f"{column} >= ?")
        params.append(low)
    if high is not None:
        frags.append(f"{column} <= ?")
        params.append(high)
    if not frags:
        return None, []
    return "(" + " AND ".join(frags) + ")", params


def size_bounds(cond):
    """(min bytes, max bytes) of a size condition; None where not given."""
    min_val = cond.get("min", "")
    max_val = cond.get("max", "")
    return (
        parse_size(min_val) if min_val else None,
        parse_size(max_val) if max_val else None,
    )


def build_condition_sql(cond):
    """Build (sql_fragment, params_list) for a single advanced condition.

    Returns (None, []) if the condition is empty/no-op.
    """
    field = cond["field"]
    value = cond.get("value", "")
    match_mode = cond.get("match", "wildcard")

    if field == "name":
        if not value:
            return None, []
        return build_name_like(value, match_mode, "f.filename")

    elif field == "type":
        if not value:
            return None, []
        if value == "other":
            return (
                "f.file_type_high NOT IN ('image','video','audio','document','text','compressed','font')",
                [],
            )
        return "f.file_type_high = ?", [value]

    elif field == "description":
        if not value:
            return None, []
        return "f.description LIKE ? ESCAPE '\\'", [f"%{escape_like(value)}%"]

    elif field == "tags":
        if not value:
            return None, []
        return tag_filter_sql(parse_tags(value))

    elif field == "size":
        return range_sql("f.file_size", *size_bounds(cond))

    elif field == "date":
        date_from = cond.get("from", "")
        date_to = cond.get("to", "")
        return range_sql(
            "f.modified_date",
            date_from or None,
            date_to + "T23:59:59" if date_to else None,
        )

    elif field == "folder":
        if not value:
            return None, []
        frag, params = build_name_like(value, match_mode, "fld.name")
        return (
            f"EXISTS (SELECT 1 FROM folders fld WHERE fld.id = f.folder_id AND {frag})",
            params,
        )

    elif field == "path":
        if not value:
            return None, []
        v = value.strip("/")
        e = escape_like(v)
        return (
            "EXISTS (SELECT 1 FROM folders fld WHERE fld.id = f.folder_id "
            "AND (fld.rel_path = ? COLLATE NOCASE "
            "OR fld.rel_path LIKE ? ESCAPE '\\' COLLATE NOCASE "
            "OR fld.rel_path LIKE ? ESCAPE '\\' COLLATE NOCASE "
            "OR fld.rel_path LIKE ? ESCAPE '\\' COLLATE NOCASE))",
            [v, f"%/{e}", f"{e}/%", f"%/{e}/%"],
        )

    elif field == "location":
        if not value:
            return None, []
        try:
            loc_id = int(value)
        except (ValueError, TypeError):
            return None, []
        return "f.location_id = ?", [loc_id]

    elif field == "duplicates":
        return range_sql(
            "f.dup_count",
            whole_number(cond.get("from"), 0),
            whole_number(cond.get("to"), 0),
        )

    elif field == "files":
        # File count — applies to folder queries only, not file queries
        return None, []

    return None, []


async def build_folder_insert_data(
    *,
    conditions,
    show_hidden,
    scope_frag,
    scope_params,
):
    """Folders matching the conditions that apply to folders (location, name,
    size, duplicates, file count), as insert tuples for the search DB.

    Uses a dedicated connection with stats.db attached, so folder stats come
    from the stats database, not the catalog's unpopulated columns.
    """
    folder_where_parts = []
    folder_params = list(scope_params) if scope_frag else []
    if scope_frag:
        folder_where_parts.append(scope_frag)
    if not show_hidden:
        folder_where_parts.append("fld.hidden = 0")
    has_folder_cond = False

    def add(op, frag):
        folder_where_parts.append(f"NOT {frag}" if op == "exclude" else frag)

    for cond in conditions:
        field = cond["field"]
        op = cond["op"]

        if field == "location":
            value = cond.get("value", "")
            if not value:
                continue
            try:
                loc_id = int(value)
            except (ValueError, TypeError):
                continue
            has_folder_cond = True
            if op == "exclude":
                folder_where_parts.append("fld.location_id != ?")
            else:
                folder_where_parts.append("fld.location_id = ?")
            folder_params.append(loc_id)

        elif field == "name":
            value = cond.get("value", "")
            if not value:
                continue
            has_folder_cond = True
            match_mode = cond.get("match", "wildcard")
            frag, cparams = build_name_like(value, match_mode, "fld.name")
            add(op, f"({frag})")
            folder_params.extend(cparams)

        elif field in ("size", "duplicates", "files"):
            if field == "size":
                frag, cparams = range_sql(
                    "COALESCE(fs.total_size, 0)", *size_bounds(cond)
                )
            elif field == "duplicates":
                frag, cparams = range_sql(
                    "COALESCE(fs.duplicate_count, 0)",
                    whole_number(cond.get("from"), 0),
                    whole_number(cond.get("to"), 0),
                )
            else:
                frag, cparams = range_sql(
                    "COALESCE(fs.file_count, 0)",
                    whole_number(cond.get("from")),
                    whole_number(cond.get("to")),
                )
            if frag is None:
                continue
            has_folder_cond = True
            add(op, frag)
            folder_params.extend(cparams)

    if not has_folder_cond:
        return []

    folder_where = " AND ".join(folder_where_parts)
    conn = await open_connection()
    try:
        await conn.execute("ATTACH DATABASE ? AS stats", (str(stats_db_path()),))
        folder_rows = await conn.execute_fetchall(
            f"""SELECT fld.id, fld.name, fld.location_id, l.name as location_name,
                       COALESCE(fs.total_size, 0) as total_size,
                       COALESCE(fs.file_count, 0) as file_count,
                       COALESCE(fs.duplicate_count, 0) as duplicate_count,
                       fld.hidden
               FROM folders fld
               JOIN locations l ON l.id = fld.location_id
               LEFT JOIN stats.folder_stats fs ON fs.folder_id = fld.id
               WHERE {folder_where}""",
            folder_params,
        )
    finally:
        await conn.close()

    return [
        (
            -r["id"],
            r["name"],
            "folder",
            None,
            r["total_size"],
            None,
            0,
            r["hidden"],
            r["location_id"],
            r["location_name"],
            None,
            None,
            r["duplicate_count"],
            r["file_count"],
        )
        for r in folder_rows
    ]


async def search_files_advanced(
    db,
    *,
    conditions,
    include_files=True,
    include_folders=False,
    location_id=None,
    folder_id=None,
    page=0,
    sort="name",
    sort_dir="asc",
    cached_total=None,
    search_id=None,
    focus_file_id=None,
    ctx=None,
):
    """Search files with advanced include/exclude conditions."""
    return await run_search(
        db,
        conditions=conditions,
        extra_frags=[],
        extra_params=[],
        include_files=include_files,
        include_folders=include_folders,
        location_id=location_id,
        folder_id=folder_id,
        page=page,
        sort=sort,
        sort_dir=sort_dir,
        search_id=search_id,
        focus_file_id=focus_file_id,
        ctx=ctx,
    )
