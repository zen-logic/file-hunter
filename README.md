# File Hunter

A self-hosted, web-based file manager that remembers everything - even drives you disconnect.

Catalog files across USB drives, backup disks, DVDs, network mounts, and local folders. Browse and search them when the media is offline. Find duplicates across terabytes of archives, consolidate them with a full audit trail, and manage it all from any browser - including on headless servers with no desktop at all.

Built for real-world scale. The UI stays responsive during scans.

![File Hunter](docs/img/app-screenshot.png)

This application was developed with the assistance of AI tools (Claude Opus, Kimi K2.5 and Qwen3-Coder-Next). These tools were used for code linting, syntax checking, testing, performance optimisation (particularly the SQLite WAL optimisation and DB locking operations), the theme engine and code refactoring. All generated code is manually reviewed and heavily tested in real use.

## Features

| Feature | Description |
|---|---|
| **Offline catalog** | Scan any folder and the full file tree persists in a local SQLite database. Unplug the drive, unmount the share - keep browsing, searching, and planning as if it were still there. |
| **Duplicate detection** | Three-gate hashing (size pre-filter, xxHash64 partial on first and last 64KB, xxHash64 fast hash over the full file) identifies byte-identical files across all locations with minimal I/O. Optional SHA-256 strong hash for absolute certainty. |
| **Consolidation** | Keep one copy, stub the rest. Copy or move mode with a preview step before execution. Hash-verified copies, `.moved` stubs, and `.sources` metadata for full provenance. |
| **Merge folders** | Merge entire locations or folder trees. Move or copy mode with preview step. Unique files transfer preserving structure, duplicates are handled at the source. Full CSV result log. |
| **Storage treemap** | Interactive squarified treemap per location. Drill into folders, see individual large files, click to navigate. Theme-aware colour palette. |
| **Full file management** | Move, copy, rename, delete files and folders. Create new folders. Upload via drag-and-drop (duplicates detected on arrival). Download files or entire folders as ZIP. Cross-location operations. |
| **Batch operations** | Multi-select with checkboxes, Shift+click, Ctrl+click. Bulk delete, move, tag, or download as ZIP. |
| **Powerful search** | Basic mode filters by filename (with match modes), file type, tags, description, size range, and date range. Advanced mode builds complex queries with multiple include/exclude conditions. Scope to a location or folder. Save searches for reuse. Results span all locations, online and offline. "Show in Folder" jumps to any result. |
| **Inline previews** | Images, video, audio, PDFs, CSV tables, hex viewer, and text files render inline in the detail panel. Full-screen slideshow for images, playlist for video. |
| **Scheduled scans** | Per-location schedules: pick days of the week and a time. Scans enqueue automatically. Offline locations are silently skipped. |
| **Background scanning** | Scans run as server-side tasks. Close the browser, come back later. Incremental rescans skip unchanged files. |
| **Multi-user auth** | Token-based authentication with PBKDF2 password hashing. First-run setup wizard. All users share full access. |
| **Tags & descriptions** | Add metadata to any file. Searchable and persistent, even when drives are offline. |
| **Themes** | Built-in themes from retro CRT terminals to clean corporate light modes. Create and edit your own in the theme editor. Every theme controls colours, fonts, glows, and treemap palette. |
| **Keyboard navigation** | Full keyboard support across all panels. Arrow keys, Tab cycling, Enter to activate, shortcuts for search and filters. |
| **Scan queue** | Queue up multiple location scans and they run one after another. Close the browser, come back - the queue kept going. |
| **Stale file detection** | Rescans detect files that have disappeared and mark them stale. Your catalog stays honest about what's actually on disk. |
| **Real-time updates** | WebSocket connection streams scan progress, upload status, and mutations to every connected browser. |

## Who is it for

| | |
|---|---|
| **Home lab operators** | Manage files on a headless server from any browser. No desktop environment needed. |
| **Photographers & videographers** | Catalog shoots across dozens of external drives. Find that one file even when the drive is in a drawer. |
| **Data hoarders** | See what you have, where it is, and how much of it is duplicated. Reclaim terabytes. |
| **Sysadmins** | Audit storage usage across backup drives and archive media. Schedule scans, review from anywhere. |

## Requirements

- Python 3.10+

## Installation

```bash
curl -fsSL https://filehunter.zenlogic.uk/install | bash
```

Downloads the latest release, extracts it, and you're ready to go. Works on macOS, Linux, and WSL.

Or download the latest release manually from the [releases page](https://github.com/zen-logic/file-hunter/releases/latest), extract it wherever you like, and run `./filehunter`.

### Install via Docker

The following project tracks this repo's releases and updates the images accordingly.  Instructions are in the project repo for setup:

https://github.com/ikidd/file-hunter-dockerized

### Install from source

```bash
git clone https://github.com/zen-logic/file-hunter.git
git clone https://github.com/zen-logic/file-hunter-agent.git
cd file-hunter
./filehunter
```

## Usage

```bash
cd filehunter-x.x.x
./filehunter
```

On first run, the launcher prompts for host and port, creates a virtual environment, and installs dependencies. Then open the URL shown in your browser.

### Getting started

1. On first launch, create your user account in the setup screen
2. Click **+ Add Location** and browse to a folder (a USB drive, a subfolder on a disk, a network mount, etc.)
3. A scan starts automatically - file metadata and hashes are computed and stored in the catalog
4. Browse the location tree, search files, review duplicates, and consolidate when ready

Everything is self-contained in the install directory - database, config, and virtual environment. Move the folder and it still works. Delete it and it's completely gone.

## Tech stack

- **Backend** - Python, Starlette, uvicorn, aiosqlite
- **Frontend** - vanilla HTML/CSS/JavaScript (no frameworks, no build step)
- **Database** - SQLite in WAL mode
- **Hashing** - xxHash64 + SHA-256

No cloud services. No telemetry. No framework dependencies. Your files never leave your machine.

## File Hunter Pro

File Hunter is free, open source, and always will be. File Hunter Pro is an optional extension that adds remote agent support - install a lightweight agent on any machine on your network and its drives appear as locations in your catalog. Browse, search, scan, preview, and stream content from remote machines as if the files were local. Duplicates are detected across all machines using the same three-gate hashing strategy.

See the [landing page](https://zen-logic.github.io/file-hunter/) for details.

## Links

- [Landing page](https://zen-logic.github.io/file-hunter/)

## License

Copyright 2026 [Zen Logic Ltd.](https://zenlogic.co.uk)
