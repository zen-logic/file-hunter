#!/usr/bin/env python3
"""Walk a directory tree and report filenames containing invalid UTF-8 bytes.

Usage: python find_invalid_filenames.py /path/to/scan
"""

import os
import sys


def main():
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <path>")
        sys.exit(1)

    root = sys.argv[1]
    if not os.path.isdir(root):
        print(f"Not a directory: {root}")
        sys.exit(1)

    count = 0
    for dirpath, dirnames, filenames in os.walk(root):
        for name in list(dirnames) + filenames:
            try:
                name.encode("utf-8")
            except UnicodeEncodeError:
                count += 1
                full = os.path.join(dirpath, name)
                raw = os.fsencode(full)
                print(f"{count}: {raw}")

    if count:
        print(f"\n{count} invalid filename(s) found.")
    else:
        print("No invalid filenames found.")


if __name__ == "__main__":
    main()
