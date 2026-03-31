"""Helpers for location deletion — shared by housekeeping."""

import logging

from file_hunter.hashes_db import read_hashes

logger = logging.getLogger("file_hunter")


async def _collect_affected_hashes(location_id: int) -> tuple[set[str], set[str]]:
    """Collect hash_fast and hash_strong values from files with dups.

    Only collects hashes where dup_count > 0 — unique files don't affect
    anything else when deleted.
    """
    affected_fast: set[str] = set()
    affected_strong: set[str] = set()

    async with read_hashes() as hdb:
        fast_rows = await hdb.execute_fetchall(
            "SELECT DISTINCT hash_fast FROM active_hashes "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_fast IS NOT NULL AND hash_fast != ''",
            (location_id,),
        )
        for r in fast_rows:
            affected_fast.add(r["hash_fast"])

        strong_rows = await hdb.execute_fetchall(
            "SELECT DISTINCT hash_strong FROM active_hashes "
            "WHERE location_id = ? AND dup_count > 0 "
            "AND hash_strong IS NOT NULL AND hash_strong != ''",
            (location_id,),
        )
        for r in strong_rows:
            affected_strong.add(r["hash_strong"])

    return affected_fast, affected_strong
