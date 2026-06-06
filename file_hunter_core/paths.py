"""Path utilities for filesystem string handling.

On Linux, filenames are raw bytes. Python's os.scandir() decodes them
using the surrogateescape error handler — bytes that aren't valid UTF-8
become surrogate code points (U+DC80..U+DCFF). These look like normal
strings but cannot be encoded to UTF-8, which breaks JSON serialisation,
HTTP response encoding, and SQLite TEXT storage.

safe_str() replaces surrogates with U+FFFD (replacement character) so
the string is safe for any output boundary. Use it on every path or
filename that leaves the process. Keep the raw path for actual file
operations (open, stat, hash).
"""

import os


def safe_str(s: str) -> str:
    """Return a UTF-8 safe copy of a filesystem string."""
    try:
        s.encode("utf-8")
        return s
    except UnicodeEncodeError:
        return s.encode("utf-8", errors="surrogateescape").decode(
            "utf-8", errors="replace"
        )


def safe_path(path: str) -> bytes:
    """Encode a filesystem path to bytes for safe SQLite BLOB storage.

    The raw bytes survive the round-trip through SQLite and can be
    decoded back with os.fsdecode() for file operations.
    """
    return os.fsencode(path)
