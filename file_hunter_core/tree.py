"""Two-phase tree walk — metadata first, then hashes in inode order.

Synchronous BFS generator streaming TSV over a single HTTP response.

Phase 1 (metadata): streams D and F records as directories are discovered.
  Fast — stat only, no file reads. Server can ingest immediately.

Phase 2 (hashing): streams H records for all files sorted by inode.
  Reads first+last 64KB per file. Inode order minimises disk seeks.

TSV format (tabs in filenames converted to spaces):
    D\trel_dir\n
    F\trel_path\tsize\tmtime\tctime\tinode\n
    P\thashing\ttotal_files\n
    H\trel_path\thash_partial\n
    E\ttotal_dirs\ttotal_files\n

Memory: O(total files) — all file paths retained for hash phase.
"""

import logging
import os
import stat
import time
from collections import deque
from datetime import datetime, timezone

from file_hunter_core.hasher import hash_file_partial_sync

logger = logging.getLogger("file_hunter_agent")


def walk_tree(root: str, prefix: str | None = None, fmt: str = "tsv",
              metadata_only: bool = False):
    """BFS generator: metadata phase then hash phase.

    Args:
        root: absolute path to location root
        prefix: optional subdirectory prefix (relative to root) to scope the walk
        fmt: "tsv" (default). Only supported format.
        metadata_only: if True, skip the hash phase (for rescan diff).
    """
    if fmt != "tsv":
        raise ValueError(f"Unsupported format: {fmt!r} (only 'tsv' is supported)")

    scope = prefix or root
    logger.info("Tree walk starting: %s", scope)
    t0 = time.monotonic()
    last_log = t0
    start = os.path.join(root, prefix) if prefix else root
    queue = deque([start])
    total_dirs = 0
    total_files = 0

    # Collect all files for hash phase
    all_files: list[tuple[int, str, str, int]] = []  # (inode, full_path, rel_path, size)

    # --- Phase 1: metadata ---
    while queue:
        dirpath = queue.popleft()

        rel_dir = os.path.relpath(dirpath, root)
        if rel_dir == ".":
            rel_dir = ""

        now = time.monotonic()
        if now - last_log >= 5.0:
            logger.info(
                "Tree walk metadata: %s — %d dirs, %d files so far (%.1fs)",
                scope,
                total_dirs,
                total_files,
                now - t0,
            )
            last_log = now

        try:
            entries = list(os.scandir(dirpath))
        except (PermissionError, OSError):
            continue

        subdirs = []
        dir_files: list[tuple[int, str, str, int, str, str]] = []

        for entry in entries:
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    subdirs.append(entry.path)
                    continue
                st = entry.stat(follow_symlinks=False)
            except OSError:
                continue

            if not stat.S_ISREG(st.st_mode):
                continue

            rel_path = os.path.join(rel_dir, entry.name) if rel_dir else entry.name
            mtime = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat(
                timespec="seconds"
            )
            ctime = datetime.fromtimestamp(
                st.st_birthtime if hasattr(st, "st_birthtime") else st.st_ctime,
                tz=timezone.utc,
            ).isoformat(timespec="seconds")

            # Reinterpret as signed 64-bit for SQLite compatibility —
            # lossless, preserves sort order within a filesystem
            ino = st.st_ino
            if ino >= 2**63:
                ino -= 2**64
            dir_files.append(
                (ino, entry.path, rel_path, st.st_size, mtime, ctime)
            )

        # Build chunk: D record + F records (no hashes)
        lines = [f"D\t{rel_dir.replace(chr(9), ' ')}\n"]

        for ino, full_path, rel_path, size, mtime, ctime in dir_files:
            safe_rel = rel_path.replace(chr(9), " ")
            lines.append(f"F\t{safe_rel}\t{size}\t{mtime}\t{ctime}\t{ino}\n")
            # Collect for hash phase (skip zero-byte)
            if size > 0:
                all_files.append((ino, full_path, rel_path, size))

        total_dirs += 1
        total_files += len(dir_files)

        yield "".join(lines)

        for sd in sorted(subdirs):
            queue.append(sd)

    walk_elapsed = time.monotonic() - t0
    logger.info(
        "Tree walk metadata complete: %s — %d dirs, %d files in %.1fs",
        scope,
        total_dirs,
        total_files,
        walk_elapsed,
    )

    if metadata_only:
        logger.info("Tree walk: metadata_only — skipping hash phase")
        yield f"E\t{total_dirs}\t{total_files}\n"
        return

    # --- Phase 2: hash partials in inode order ---
    all_files.sort(key=lambda e: e[0])
    hashable = len(all_files)
    logger.info("Tree hash phase: %d files to hash (inode-sorted)", hashable)

    yield f"P\thashing\t{hashable}\n"

    hash_t0 = time.monotonic()
    last_flush = hash_t0
    last_log = hash_t0
    hashed = 0
    buf: list[str] = []

    for ino, full_path, rel_path, size in all_files:
        try:
            hp = hash_file_partial_sync(full_path)
        except (OSError, PermissionError):
            continue

        safe_rel = rel_path.replace(chr(9), " ")
        buf.append(f"H\t{safe_rel}\t{hp}\n")
        hashed += 1

        now = time.monotonic()

        # Flush on time interval — gives accurate counts to server
        if now - last_flush >= 5.0:
            yield "".join(buf)
            buf.clear()
            last_flush = now

        if now - last_log >= 5.0:
            rate = hashed / (now - hash_t0) if (now - hash_t0) > 0 else 0
            logger.info(
                "Tree hash progress: %s — %d / %d hashed (%.0f/sec, %.1fs)",
                scope,
                hashed,
                hashable,
                rate,
                now - hash_t0,
            )
            last_log = now

    if buf:
        yield "".join(buf)

    hash_elapsed = time.monotonic() - hash_t0
    total_elapsed = time.monotonic() - t0
    logger.info(
        "Tree walk complete: %s — %d dirs, %d files, %d hashed in %.1fs (walk %.1fs, hash %.1fs)",
        scope,
        total_dirs,
        total_files,
        hashed,
        total_elapsed,
        walk_elapsed,
        hash_elapsed,
    )
    yield f"E\t{total_dirs}\t{total_files}\n"
