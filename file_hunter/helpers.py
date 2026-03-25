"""Shared helper functions — eliminates repeated patterns across services."""

import asyncio


# ---------------------------------------------------------------------------
# 1. Prefixed ID parsing  (loc-N, fld-N)
# ---------------------------------------------------------------------------

def parse_prefixed_id(prefixed: str) -> tuple[str, int]:
    """Parse 'loc-49' -> ('loc', 49), 'fld-123' -> ('fld', 123).

    Raises ValueError on unrecognised prefix.
    """
    s = str(prefixed)
    if s.startswith("loc-"):
        return "loc", int(s[4:])
    if s.startswith("fld-"):
        return "fld", int(s[4:])
    raise ValueError(f"Invalid prefixed ID: {prefixed!r}")


def parse_location_id(prefixed: str) -> int:
    """Parse 'loc-49' -> 49.  Also accepts plain int strings."""
    return int(str(prefixed).replace("loc-", ""))


def parse_folder_id(prefixed: str) -> int:
    """Parse 'fld-123' -> 123.  Also accepts plain int strings."""
    return int(str(prefixed).replace("fld-", ""))


# ---------------------------------------------------------------------------
# 4. Post-operation stats update
# ---------------------------------------------------------------------------

async def post_op_stats(
    *,
    location_ids: set[int] | None = None,
    strong_hashes: set[str] | None = None,
    fast_hashes: set[str] | None = None,
    source: str = "",
):
    """Invalidate stats cache, schedule size recalc, submit hashes for dup recount.

    Call after any mutation that affects file counts, sizes, or dup groups.
    Only invokes each step when relevant parameters are provided.
    """
    from file_hunter.services.stats import invalidate_stats_cache

    invalidate_stats_cache()

    if location_ids:
        from file_hunter.services.sizes import schedule_size_recalc

        schedule_size_recalc(*location_ids)

    if strong_hashes or fast_hashes:
        from file_hunter.services.dup_counts import submit_hashes_for_recalc

        submit_hashes_for_recalc(
            strong_hashes=strong_hashes or None,
            fast_hashes=fast_hashes or None,
            source=source,
        )


# ---------------------------------------------------------------------------
# 5. Effective hash lookup
# ---------------------------------------------------------------------------

async def get_effective_hash(file_id: int) -> tuple[str | None, str | None]:
    """Return (effective_hash, hash_column) for a file.

    Prefers hash_strong over hash_fast.
    Returns (None, None) when no hash exists.
    """
    from file_hunter.hashes_db import get_file_hashes

    h_map = await get_file_hashes([file_id])
    h = h_map.get(file_id, {})
    hs = h.get("hash_strong")
    if hs:
        return hs, "hash_strong"
    hf = h.get("hash_fast")
    if hf:
        return hf, "hash_fast"
    return None, None


async def get_effective_hashes(file_ids: list[int]) -> dict[int, tuple[str | None, str | None]]:
    """Batch version of get_effective_hash.

    Returns {file_id: (effective_hash, hash_column), ...}.
    """
    from file_hunter.hashes_db import get_file_hashes

    h_map = await get_file_hashes(file_ids)
    result = {}
    for fid in file_ids:
        h = h_map.get(fid, {})
        hs = h.get("hash_strong")
        if hs:
            result[fid] = (hs, "hash_strong")
        else:
            hf = h.get("hash_fast")
            result[fid] = (hf, "hash_fast") if hf else (None, None)
    return result


# ---------------------------------------------------------------------------
# 7. Target resolution  (loc-N / fld-N -> location + folder metadata)
# ---------------------------------------------------------------------------

async def resolve_target(db, prefixed_id: str) -> dict | None:
    """Resolve a prefixed ID to location/folder metadata.

    Returns dict with keys:
        kind:        'loc' | 'fld'
        location_id: int
        folder_id:   int | None   (None for location root)
        root_path:   str          (location root_path)
        rel_path:    str          (folder rel_path, '' for location root)
        abs_path:    str          (full filesystem path)
        name:        str          (location or folder name)

    Returns None if the target doesn't exist.
    """
    import os

    kind, num_id = parse_prefixed_id(prefixed_id)

    if kind == "loc":
        rows = await db.execute_fetchall(
            "SELECT id, name, root_path FROM locations WHERE id = ?",
            (num_id,),
        )
        if not rows:
            return None
        loc = rows[0]
        return {
            "kind": "loc",
            "location_id": loc["id"],
            "folder_id": None,
            "root_path": loc["root_path"],
            "rel_path": "",
            "abs_path": loc["root_path"],
            "name": loc["name"],
        }

    # fld
    rows = await db.execute_fetchall(
        "SELECT f.id, f.name, f.rel_path, f.location_id, "
        "l.name AS location_name, l.root_path "
        "FROM folders f JOIN locations l ON l.id = f.location_id "
        "WHERE f.id = ?",
        (num_id,),
    )
    if not rows:
        return None
    fld = rows[0]
    return {
        "kind": "fld",
        "location_id": fld["location_id"],
        "folder_id": fld["id"],
        "root_path": fld["root_path"],
        "rel_path": fld["rel_path"],
        "abs_path": os.path.join(fld["root_path"], fld["rel_path"]),
        "name": fld["name"],
        "location_name": fld["location_name"],
    }
