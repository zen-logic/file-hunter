"""Directory walker — synchronous breadth-first scan of a single directory.

Returns raw file metadata and subdirectory paths. No database, no async.
The server's scanner calls this in a thread and handles DB upsert separately.
"""

import os
import stat
from datetime import datetime, timezone

from file_hunter_core.classify import classify_file


def scan_directory(dirpath: str, root_path: str, parent_hidden: bool = False):
    """Scan a single directory. Returns (subdirs, file_infos).

    subdirs: list of full paths to non-symlink subdirectories
    file_infos: list of dicts with file metadata for non-symlink files

    Dotfiles/dotfolders are included with hidden=1. Files inside a hidden
    parent directory inherit hidden status.
    """
    subdirs = []
    file_infos = []

    try:
        entries = os.listdir(dirpath)
    except (PermissionError, OSError):
        return subdirs, file_infos

    rel_dir = os.path.relpath(dirpath, root_path)
    if rel_dir == ".":
        rel_dir = ""

    for name in entries:
        hidden = parent_hidden or name.startswith(".")

        full_path = os.path.join(dirpath, name)

        try:
            is_link = os.path.islink(full_path)
        except OSError:
            continue

        if is_link:
            continue

        try:
            is_dir = os.path.isdir(full_path)
        except OSError:
            continue

        if is_dir:
            subdirs.append(full_path)
            continue

        # It's a file — stat it
        try:
            st = os.stat(full_path)
        except OSError:
            continue

        # Skip non-regular files (sockets, FIFOs, device nodes)
        if not stat.S_ISREG(st.st_mode):
            continue

        rel_path = os.path.join(rel_dir, name) if rel_dir else name
        type_high, type_low = classify_file(name)

        file_infos.append(
            {
                "filename": name,
                "full_path": full_path,
                "rel_path": rel_path,
                "rel_dir": rel_dir,
                "file_size": st.st_size,
                "created_date": datetime.fromtimestamp(
                    st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                    tz=timezone.utc,
                ).isoformat(timespec="seconds"),
                "modified_date": datetime.fromtimestamp(
                    st.st_mtime, tz=timezone.utc
                ).isoformat(timespec="seconds"),
                "file_type_high": type_high,
                "file_type_low": type_low,
                "hidden": 1 if hidden else 0,
            }
        )

    return subdirs, file_infos
