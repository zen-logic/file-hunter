"""Dual-hash file computation (xxHash64 + SHA-256) in a single pass.

All functions are synchronous. The server wraps these in asyncio.to_thread().
"""

import hashlib
import os

import xxhash

CHUNK_SIZE = 1024 * 1024  # 1 MB
PARTIAL_SIZE = 64 * 1024  # 64 KB


def hash_file_sync(path: str) -> tuple[str, str]:
    """Read file once, feed both hashers, return (xxhash64_hex, sha256_hex)."""
    xx = xxhash.xxh64()
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx.update(chunk)
            sha.update(chunk)
    return xx.hexdigest(), sha.hexdigest()


def hash_fast_only_sync(path: str) -> str:
    """Read file once, return xxHash64 hex only. ~10x faster than dual hash."""
    xx = xxhash.xxh64()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            xx.update(chunk)
    return xx.hexdigest()


def hash_file_partial_sync(path: str) -> str:
    """xxHash64 of first 64KB + last 64KB. For files <= 128KB, reads everything."""
    xx = xxhash.xxh64()
    file_size = os.path.getsize(path)
    with open(path, "rb") as f:
        if file_size <= PARTIAL_SIZE * 2:
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
        else:
            # First 64KB
            read = 0
            while read < PARTIAL_SIZE:
                chunk = f.read(min(CHUNK_SIZE, PARTIAL_SIZE - read))
                if not chunk:
                    break
                xx.update(chunk)
                read += len(chunk)
            # Last 64KB
            f.seek(-PARTIAL_SIZE, 2)
            while chunk := f.read(CHUNK_SIZE):
                xx.update(chunk)
    return xx.hexdigest()
