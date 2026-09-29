"""Create test data directories with real files for scanning.

Builds a directory tree under testdata/ at the project root with three
locations containing real files of known sizes.  Files that should be
detected as duplicates share byte-identical content.

To simulate taking a location offline, rename its directory:
    mv "testdata/Old Archive (2019)" "testdata/Old Archive (2019).offline"
To bring it back online, rename it back.
"""

import random
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
TESTDATA_DIR = PROJECT_ROOT / "testdata"
LOCATION_A = TESTDATA_DIR / "Archive Disk A"
LOCATION_B = TESTDATA_DIR / "Backup Drive B"
LOCATION_C = TESTDATA_DIR / "Old Archive (2019)"
LOCATION_CONSOLIDATED = TESTDATA_DIR / "Consolidated"

KB = 1024

# (size_in_bytes, header_label) — label appears at the start of each blob
# for debuggability.  The rest is deterministic random padding.
BLOB_DEFS = {
    "panorama": (150 * KB, "panorama.jpg"),
    "sunset": (80 * KB, "sunset.heic"),
    "img0421": (40 * KB, "IMG_0421.jpg"),
    "img0422": (39 * KB, "IMG_0422.jpg"),
    "img0423": (50 * KB, "IMG_0423.jpg"),
    "img0424": (120 * KB, "IMG_0424.png"),
    "videoclip": (500 * KB, "video_clip.mp4"),
    "notes": (1 * KB, "notes.txt"),
    "family01": (36 * KB, "family_photo_01.jpg"),
    "family02": (40 * KB, "family_photo_02.jpg"),
    "track01": (85 * KB, "album_track_01.mp3"),
    "track02": (350 * KB, "album_track_02.flac"),
    "podcast": (450 * KB, "podcast_ep12.mp3"),
    "taxreturn": (20 * KB, "tax_return_2024.pdf"),
    "projplan": (5 * KB, "project_plan.docx"),
    "budget": (int(1.3 * KB), "budget.xlsx"),
    "readme": (2 * KB, "readme.txt"),
}

# rel_path -> blob name for each location
FILE_MAP_A = {
    "readme.txt": "readme",
    "Photos/panorama.jpg": "panorama",
    "Photos/sunset.heic": "sunset",
    "Photos/2024 Holiday/IMG_0421.jpg": "img0421",
    "Photos/2024 Holiday/IMG_0422.jpg": "img0422",
    "Photos/2024 Holiday/IMG_0423.jpg": "img0423",
    "Photos/2024 Holiday/IMG_0424.png": "img0424",
    "Photos/2024 Holiday/video_clip.mp4": "videoclip",
    "Photos/2024 Holiday/notes.txt": "notes",
    "Photos/Family/family_photo_01.jpg": "family01",
    "Photos/Family/family_photo_02.jpg": "family02",
    "Photos/Family/IMG_0423_copy.jpg": "img0423",
    "Music/album_track_01.mp3": "track01",
    "Music/album_track_02.flac": "track02",
    "Music/podcast_ep12.mp3": "podcast",
    "Documents/tax_return_2024.pdf": "taxreturn",
    "Documents/project_plan.docx": "projplan",
    "Documents/budget.xlsx": "budget",
}

FILE_MAP_B = {
    "Projects/website/hero.jpg": "panorama",
    "Projects/website/family_02.jpg": "family02",
    "Projects/album/IMG_0421.jpg": "img0421",
    "Videos/holiday_clip.mp4": "videoclip",
    "Videos/thumbs/IMG_0423.jpg": "img0423",
    "Documents/tax_return_2024.pdf": "taxreturn",
}

FILE_MAP_C = {
    "Misc/photos/panorama.jpg": "panorama",
    "Misc/photos/IMG_0423.jpg": "img0423",
    "Misc/photos/family_photo_02.jpg": "family02",
    "Music/album_track_02.flac": "track02",
}

# Empty directories that should exist for folder tree consistency
EMPTY_DIRS_C = [
    "Work Files",
]


def make_blob(size: int, label: str, rng: random.Random) -> bytes:
    header = f"FILE:{label}\n".encode()
    padding_size = size - len(header)
    return header + rng.randbytes(padding_size)


def create_test_locations() -> tuple[Path, Path, Path, Path]:
    """Build testdata/ with real files.

    Returns (path_a, path_b, path_c, path_consolidated).

    If all three source location directories already exist, skips file
    creation and returns the paths immediately.  The consolidated
    directory is always created (empty) if it doesn't exist.
    """
    need_create = not (
        LOCATION_A.exists() and LOCATION_B.exists() and LOCATION_C.exists()
    )

    if need_create:
        rng = random.Random(42)
        blobs = {
            name: make_blob(size, label, rng)
            for name, (size, label) in BLOB_DEFS.items()
        }

        for file_map, root in [
            (FILE_MAP_A, LOCATION_A),
            (FILE_MAP_B, LOCATION_B),
            (FILE_MAP_C, LOCATION_C),
        ]:
            for rel_path, blob_name in file_map.items():
                dest = root / rel_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(blobs[blob_name])

        for dir_name in EMPTY_DIRS_C:
            (LOCATION_C / dir_name).mkdir(parents=True, exist_ok=True)

    LOCATION_CONSOLIDATED.mkdir(parents=True, exist_ok=True)

    return LOCATION_A, LOCATION_B, LOCATION_C, LOCATION_CONSOLIDATED
