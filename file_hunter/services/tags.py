"""Tag vocabulary and file/tag associations.

Tags live in two tables: `tags` holds the vocabulary (one row per distinct
tag, name already normalised) and `file_tags` associates files with tags.
The legacy `files.tags` column is retained but is no longer read or written
by any code path — the join table is the single source of truth.

Normalisation is applied on the way in, never on the way out. A stored tag
name is already canonical, so reads never need to transform it and callers
can compare stored names directly.
"""

import re
import unicodedata

_WHITESPACE = re.compile(r"\s+")


def normalise_tag(raw: str) -> str:
    """Reduce a single tag to its canonical stored form.

    Applies NFKC unicode normalisation, removes commas (the delimiter used
    by callers that pass tags as a single string), collapses whitespace runs
    to a single space, trims, and lowercases.

    Parameters:
        raw: A single tag as typed by a user or read from legacy data.

    Returns:
        The canonical form, or '' if nothing survives normalisation.
    """
    if not raw:
        return ""
    text = unicodedata.normalize("NFKC", str(raw))
    text = text.replace(",", " ")
    text = _WHITESPACE.sub(" ", text).strip()
    return text.lower()


def parse_tags(value) -> list[str]:
    """Normalise arbitrary caller input into a canonical tag list.

    Accepts either a comma-separated string or a list of strings, and
    tolerates commas inside list elements. Order of first appearance is
    preserved; duplicates and empties are dropped.

    Parameters:
        value: A comma-separated string, a list of strings, or None.

    Returns:
        Normalised tag names, deduplicated, in first-seen order.
    """
    if not value:
        return []
    parts = value if isinstance(value, (list, tuple)) else [value]

    result = []
    seen = set()
    for part in parts:
        for piece in str(part).split(","):
            name = normalise_tag(piece)
            if name and name not in seen:
                seen.add(name)
                result.append(name)
    return result


async def get_file_tags(db, file_id: int) -> list[str]:
    """Return a file's tag names, alphabetically.

    Parameters:
        db: Any database connection.
        file_id: Numeric file ID.

    Returns:
        Canonical tag names. Empty list if the file has none.
    """
    rows = await db.execute_fetchall(
        "SELECT t.name FROM file_tags ft "
        "JOIN tags t ON t.id = ft.tag_id "
        "WHERE ft.file_id = ? ORDER BY t.name",
        (file_id,),
    )
    return [row["name"] for row in rows]


async def get_merged_tags(db, file_ids: list[int]) -> list[str]:
    """Return the union of tags across several files, alphabetically.

    Used when collapsing duplicate copies into one canonical file — the
    survivor inherits every tag any copy carried.

    Parameters:
        db: Any database connection.
        file_ids: Numeric file IDs.

    Returns:
        Distinct canonical tag names across all the given files.
    """
    if not file_ids:
        return []
    placeholders = ",".join("?" for _ in file_ids)
    rows = await db.execute_fetchall(
        f"SELECT DISTINCT t.name FROM file_tags ft "
        f"JOIN tags t ON t.id = ft.tag_id "
        f"WHERE ft.file_id IN ({placeholders}) ORDER BY t.name",
        list(file_ids),
    )
    return [row["name"] for row in rows]


async def list_all_tags(db) -> list[str]:
    """Return the whole tag vocabulary, alphabetically.

    Reads the `tags` table directly — no scan of `files` and no
    application-side deduplication.

    Parameters:
        db: Any database connection.

    Returns:
        Every tag name known to the catalog.
    """
    rows = await db.execute_fetchall("SELECT name FROM tags ORDER BY name")
    return [row["name"] for row in rows]


def tag_filter_sql(names: list[str], file_col: str = "f.id"):
    """Build a SQL fragment matching files carrying *all* the given tags.

    Resolves to a single uncorrelated subquery so the (small) set of matching
    file IDs is computed once rather than re-evaluated per candidate row —
    which matters on a large catalog, where a tag is highly selective.

    Parameters:
        names: Already-normalised tag names.
        file_col: Qualified files.id column to constrain.

    Returns:
        (fragment, params) or (None, []) if `names` is empty.
    """
    if not names:
        return None, []
    placeholders = ",".join("?" for _ in names)
    fragment = (
        f"{file_col} IN ("
        f"SELECT ft.file_id FROM file_tags ft "
        f"JOIN tags t ON t.id = ft.tag_id "
        f"WHERE t.name IN ({placeholders}) "
        f"GROUP BY ft.file_id HAVING COUNT(DISTINCT ft.tag_id) = ?"
        f")"
    )
    return fragment, list(names) + [len(names)]


async def copy_file_tags(db, source_file_id: int, dest_file_id: int):
    """Give the destination file the same tags as the source.

    Used when a file record is created as a copy of another. Additive —
    any tags the destination already has are kept.

    Parameters:
        db: Writable database connection (called inside a write context).
        source_file_id: File to copy tags from.
        dest_file_id: File to copy tags to.

    Side effects:
        Inserts into `file_tags`. Does not commit.
    """
    await db.execute(
        "INSERT OR IGNORE INTO file_tags (file_id, tag_id) "
        "SELECT ?, tag_id FROM file_tags WHERE file_id = ?",
        (dest_file_id, source_file_id),
    )


async def set_file_tags(db, file_id: int, names: list[str], cache: dict = None):
    """Replace a file's tags with exactly `names`.

    Parameters:
        db: Writable database connection (called inside a write context).
        file_id: Numeric file ID.
        names: Already-normalised tag names. Empty clears all tags.
        cache: Optional name -> id dict shared across calls.

    Side effects:
        Deletes and inserts rows in `file_tags`, inserting into `tags` as
        needed. Does not commit — the caller owns the transaction.
    """
    tag_ids = await get_or_create_tag_ids(db, names, cache)
    if tag_ids:
        placeholders = ",".join("?" for _ in tag_ids)
        await db.execute(
            f"DELETE FROM file_tags WHERE file_id = ? AND tag_id NOT IN ({placeholders})",
            [file_id] + tag_ids,
        )
        await db.executemany(
            "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
            [(file_id, tid) for tid in tag_ids],
        )
    else:
        await db.execute("DELETE FROM file_tags WHERE file_id = ?", (file_id,))


async def add_file_tags(db, file_id: int, names: list[str], cache: dict = None):
    """Add tags to a file, leaving existing ones in place.

    Parameters:
        db: Writable database connection (called inside a write context).
        file_id: Numeric file ID.
        names: Already-normalised tag names.
        cache: Optional name -> id dict shared across calls.

    Side effects:
        Inserts into `file_tags` (and `tags` as needed). Does not commit.
    """
    if not names:
        return
    tag_ids = await get_or_create_tag_ids(db, names, cache)
    await db.executemany(
        "INSERT OR IGNORE INTO file_tags (file_id, tag_id) VALUES (?, ?)",
        [(file_id, tid) for tid in tag_ids],
    )


async def remove_file_tags(db, file_id: int, names: list[str]):
    """Remove tags from a file.

    Names that the file doesn't carry, or that aren't in the vocabulary at
    all, are silently ignored. The `tags` row itself is left in place even
    if no file references it any more.

    Parameters:
        db: Writable database connection (called inside a write context).
        file_id: Numeric file ID.
        names: Already-normalised tag names.

    Side effects:
        Deletes rows from `file_tags`. Does not commit.
    """
    if not names:
        return
    placeholders = ",".join("?" for _ in names)
    await db.execute(
        f"DELETE FROM file_tags WHERE file_id = ? AND tag_id IN ("
        f"SELECT id FROM tags WHERE name IN ({placeholders}))",
        [file_id] + list(names),
    )


async def get_or_create_tag_ids(db, names: list[str], cache: dict = None) -> list[int]:
    """Resolve tag names to IDs, inserting any that don't exist yet.

    Parameters:
        db: Writable database connection (called inside a write context).
        names: Already-normalised tag names.
        cache: Optional name -> id dict reused across calls to avoid
            re-querying. Populated as tags are resolved.

    Returns:
        Tag IDs in the same order as `names`.

    Side effects:
        Inserts rows into `tags` for names not already present. Does not
        commit — the caller owns the transaction.
    """
    cache = cache if cache is not None else {}
    ids = []
    for name in names:
        tag_id = cache.get(name)
        if tag_id is None:
            await db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (name,))
            cursor = await db.execute("SELECT id FROM tags WHERE name = ?", (name,))
            row = await cursor.fetchone()
            tag_id = row["id"]
            cache[name] = tag_id
        ids.append(tag_id)
    return ids
