"""Operation result CSV log — written to destination during merge/consolidate.

Creates a CSV at the destination at the start of the operation, appends
one row per file as it is processed. The CSV is added to the catalog
so the user can find it.
"""

import csv
import io
import logging
import os
from datetime import datetime, timezone

from file_hunter.db import db_writer, read_db
from file_hunter.services import fs
from file_hunter.stats_db import update_stats_for_files

logger = logging.getLogger("file_hunter")

CSV_HEADERS = [
    "source_location",
    "source_path",
    "destination_location",
    "destination_path",
    "result",
    "detail",
]


def _make_filename(op_type: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"{op_type}-result-{ts}.csv"


def _row_to_csv(row: list[str]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(row)
    return buf.getvalue()


async def create_log(dest_dir: str, dest_loc_id: int, op_type: str) -> str:
    """Create the CSV file with headers at the destination. Returns the full path."""
    filename = _make_filename(op_type)
    csv_path = os.path.join(dest_dir, filename)
    header_line = _row_to_csv(CSV_HEADERS)
    await fs.file_write_text(csv_path, header_line, dest_loc_id)
    logger.info("Operation result log created: %s", csv_path)
    return csv_path


async def append_row(
    csv_path: str,
    dest_loc_id: int,
    source_location: str,
    source_path: str,
    dest_location: str,
    dest_path: str,
    result: str,
    detail: str = "",
):
    """Append a single result row to the CSV."""
    line = _row_to_csv(
        [source_location, source_path, dest_location, dest_path, result, detail]
    )
    await fs.file_write_text(csv_path, line, dest_loc_id, append=True)


async def add_to_catalog(csv_path: str, location_id: int, folder_id: int | None):
    """Insert the CSV file as a record in the catalog."""
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    filename = os.path.basename(csv_path)
    st = await fs.file_stat(csv_path, location_id)
    file_size = st["size"] if st else 0

    # Build rel_path from location root
    async with read_db() as db:
        loc_rows = await db.execute_fetchall(
            "SELECT root_path FROM locations WHERE id = ?", (location_id,)
        )
    if not loc_rows:
        return
    root_path = loc_rows[0]["root_path"]
    rel_path = os.path.relpath(csv_path, root_path)

    async with db_writer() as wdb:
        await wdb.execute(
            """INSERT OR IGNORE INTO files
               (filename, full_path, rel_path, location_id, folder_id,
                file_type_high, file_type_low, file_size,
                description, tags,
                created_date, modified_date, date_cataloged, date_last_seen)
               VALUES (?, ?, ?, ?, ?, 'text', 'csv', ?, '', '', ?, ?, ?, ?)""",
            (
                filename,
                csv_path,
                rel_path,
                location_id,
                folder_id,
                file_size,
                now_iso,
                now_iso,
                now_iso,
                now_iso,
            ),
        )

    await update_stats_for_files(location_id, added=[(folder_id, file_size, "text", 0)])
