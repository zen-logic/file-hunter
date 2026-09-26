# File Hunter Features

## Offline catalogue

Register any folder as a named location: USB drives, DVD-ROMs, network mounts, local directories. The full catalogue persists locally so you can browse, search, and review files even when the drive is disconnected. Reconnect and rescan picks up where it left off.

## Duplicate detection

Files are hashed and compared across all locations. Duplicates are identified with minimal I/O using a multi-stage hashing strategy, with optional SHA-256 verification for absolute certainty. Duplicate badges in the file list show how many copies exist and where they are.

## Consolidation

Keep one copy, stub the rest. Copy mode leaves originals in place. Move mode replaces duplicates with small stub files that record where the canonical copy lives. A `.sources` file alongside the kept copy records every original path. Offline duplicates are queued and stubbed automatically when the drive comes back. Every operation produces a CSV result log viewable in the UI.

## Merge folders

Merge entire locations or folder trees into a destination. Preview step shows exactly what will happen. Unique files transfer preserving structure, duplicates are handled at the source. Copy or move mode. Full result log.

## Search

Basic search filters by filename (with wildcard, starts with, ends with, exact match modes), file type, tags, description, size range, date range, and duplicate count. Scope any search to a location or folder.

Advanced search builds complex queries with multiple include and exclude conditions across all fields, including folder path and location. Tag wildcards: `*` finds files with any tag, `!` finds untagged files.

Save searches for one-click reuse. Results span all locations, online and offline, and are sortable by name, type, size, date, or duplicate count. "Show in Folder" jumps to any result.

## Storage treemap

Interactive visualisation of disk usage for any location or folder. Drill into subfolders, see large files, click to navigate to them.

## Previews

Images, video, audio, PDFs, CSV tables, text files, and a hex viewer all render inline in the detail panel. Full-screen zoom on any preview. Image dimensions shown below the preview.

## Similarity search

Optional image similarity search powered by CLIP embeddings. Connect to an [embedding service](https://github.com/zen-logic/file-hunter-embedding) in Settings to enable.

Scan any location or folder to index images. Image and document scans are separate options in the scan dialogue. Search by text features (e.g. "red socks", "blue jacket"), by similarity to a selected file, or by uploading a reference image from your desktop. Text and image queries can be combined. Adjustable similarity threshold. Results are listed in similarity order, closest match first.

Text queries support composite syntax for vector arithmetic: `(red socks) - shoes` embeds each phrase separately and combines the vectors before searching.

Requires a separate [embedding service](https://github.com/zen-logic/file-hunter-embedding). ChromaDB is installed automatically when the feature is first enabled. No impact on installations that do not use this feature.

## Document semantic search

Optional document search powered by [Docling](https://github.com/docling-project/docling). Connect to an [embedding service](https://github.com/zen-logic/file-hunter-embedding) in Settings to enable.

Search within document content using natural language queries. Supported formats: PDF, DOCX, PPTX, XLSX, ODT, HTML, and plain text (TXT, MD, CSV, JSON, etc.). Hybrid scoring combines vector similarity with keyword boosting for precision. Composite query syntax supported. Embed individual files from the detail panel. Dedicated content search panel with threshold control in the toolbar.

## Location filtering

Both image similarity and document content search panels include a multi-select location dropdown. Scope searches to specific locations or search across all.

## Embedding management

Delete image or document embeddings per location or folder from the detail panel. Deletion runs as a background task with progress in the status bar. Embeddings are automatically cleaned up on file delete, folder delete, batch delete, reset stale, and cross-location moves.

## Video transcoding

Convert video files to browser-playable MP4 (H.264/AAC) from the detail panel. Auto-detects hardware encoders (VideoToolbox, NVENC, VAAPI, Quick Sync) with software fallback. Three quality presets: low, medium, high. Progress shown in the status bar. Converted file appears alongside the original and is automatically catalogued. Requires ffmpeg on the host machine.

## Camera raw conversion

Convert camera raw files (NEF, CR2, CR3, ARW, DNG, ORF, RAF, RW2, PEF, SRW, NRW, and others) to full-resolution JPEG from the detail panel. Converted file appears alongside the original and is automatically catalogued. Requires dcraw on the host machine.

## Gallery view

Toggle between list and gallery views in the file panel. Gallery shows image thumbnails in a responsive grid. Folders and non-image files display as icons. Selection, triage marks, duplicate pills, and keyboard navigation work in both views.

## Drag to move

Drag files or folders from the file list or gallery to any folder or location in the tree to move them. Works with multi-selection.

## Slideshow and playlist

Full-screen slideshow for images with crossfade transitions, auto-advance, and configurable speed. Full-screen playlist for video with auto-advance on completion. Launch from any folder, location, search result, or a selected image. Click an image to close and navigate to it.

## Triage

Mark files for action while browsing the file list, previewing, or in a slideshow using keyboard shortcuts (D delete, C consolidate, T tag, M move, Z download). Marks accumulate across navigation and multiple slideshows. A triage bar above the file list shows counts and action buttons. Execute when ready.

## File management

Move, copy, rename, and delete files and folders. Create new folders. Upload via drag-and-drop with automatic duplicate detection. Download individual files or entire folders as ZIP. Cross-location operations.

## Batch operations

Multi-select with checkboxes, Shift-click, or Ctrl/Cmd-click. Bulk delete, move, tag, consolidate, or download as ZIP.

## Tags and descriptions

Add tags and free-text descriptions to any file. Tags propagate to all duplicates when added. Searchable and persistent, even when drives are offline.

## Scanning

Scans run in the background as server-side tasks. Close the browser and come back later. Incremental rescans skip unchanged files. Queue multiple scans. Set per-location schedules with day and time. Stale file detection marks files that have disappeared since the last scan.

## Ignore rules

Exclude files from scans by filename and size, globally or per-location.

## Duplicate exclusion

Mark entire folder trees to exclude from duplicate detection. Useful for system folders, caches, and build artefacts.

## Favourites

Pin locations and folders to the top of the tree for quick access.

## Themes

Built-in themes from retro CRT terminals to clean corporate light modes. Create and edit custom themes in the theme editor.

## Keyboard navigation

Full keyboard support across all panels. Arrow keys, Tab to cycle panels, shortcuts for search, triage, scan, and file operations. Detail panel keys forward to the file list when input fields aren't active.

## Multi-user authentication

Token-based authentication. Multiple user accounts. First-run setup wizard. Application tokens for programmatic API access.

## REST API

Token-based API for connecting File Hunter to external applications. File lookup by hash, tag listing, file content streaming with range requests, location and folder browsing, and search.

## Real-time updates

All connected browsers see scan progress, uploads, and changes as they happen.

## Settings

Server name. Theme selection with built-in themes and a full theme editor for creating custom themes. Show/hide hidden files. Enable similarity search and configure the embedding service URL. User management with multiple accounts. Application tokens for API access. Maintenance: repair catalogue (incomplete scans, missing hashes, duplicate counts, folder totals) and reset queues (cancel all operations, clear temp files and pending queues).

## Self-hosted

Runs on your hardware. No cloud services, no telemetry, no external dependencies. Works on macOS, Linux, WSL, Raspberry Pi. One command to install, one command to run. Docker support available. Everything self-contained in one directory.

## File Hunter Pro

Optional extension for multi-machine support. The free core is open source and always will be.

- **Remote agents**: install a lightweight agent on any machine, its drives appear as locations in your catalogue
- **Content streaming**: preview and play files from remote machines, streamed on demand with seek support
- **Cross-machine deduplication**: duplicates detected across all machines
- **Live connection status**: agent locations show online/offline in real time, reconnection is automatic
- **Read-only WebDAV server**: mount your catalogue as a network drive from Finder, Windows Explorer, or iOS Files

One-time purchase, no subscription, no tracking.
