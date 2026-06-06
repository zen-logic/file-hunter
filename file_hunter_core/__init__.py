"""file_hunter_core — pure filesystem operations shared by File Hunter server and agents.

No database, no web framework, no async dependencies. Everything here is
synchronous and portable.
"""

from file_hunter_core.paths import safe_str
from file_hunter_core.classify import classify_file, format_size
from file_hunter_core.hasher import hash_file_sync, hash_file_partial_sync
from file_hunter_core.browse import get_root_entries, get_children
from file_hunter_core.walker import scan_directory
from file_hunter_core.fileops import (
    write_moved_stub,
    write_sources_file,
    write_or_append_sources,
    unique_dest_path,
)

__all__ = [
    "safe_str",
    "classify_file",
    "format_size",
    "hash_file_sync",
    "hash_file_partial_sync",
    "get_root_entries",
    "get_children",
    "scan_directory",
    "write_moved_stub",
    "write_sources_file",
    "write_or_append_sources",
    "unique_dest_path",
]
