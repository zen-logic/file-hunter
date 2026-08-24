# File Hunter API

All responses are JSON:

```json
{"ok": true, "data": ...}
{"ok": false, "error": "message"}
```

All endpoints require session auth (cookie or `Authorization: Bearer <token>`) unless noted. Application tokens (from `/api/auth/apps`) also work as Bearer tokens.

---

## Auth

### GET /api/auth/status

No auth required.

```json
{"needsSetup": false, "serverName": "My Server"}
```

### POST /api/auth/setup

No auth required. First-time setup only.

```json
// Request
{"username": "admin", "password": "secret", "displayName": "Admin"}

// Response
{"token": "abc123...", "user": {"id": 1, "username": "admin", "displayName": "Admin"}}
```

### POST /api/auth/login

No auth required.

```json
// Request
{"username": "admin", "password": "secret"}

// Response
{"token": "abc123...", "user": {"id": 1, "username": "admin", "displayName": "Admin"}}
```

### POST /api/auth/logout

```json
{"loggedOut": true}
```

### GET /api/auth/me

```json
{"id": 1, "username": "admin", "displayName": "Admin"}
```

### GET /api/auth/users

```json
[{"id": 1, "username": "admin", "displayName": "Admin"}, ...]
```

### POST /api/auth/users

```json
// Request
{"username": "newuser", "password": "secret", "displayName": "New User"}

// Response
{"id": 2, "username": "newuser", "displayName": "New User"}
```

### PATCH /api/auth/users/{id}

```json
// Request (all optional)
{"username": "renamed", "password": "newpass", "displayName": "New Name"}

// Response
{"updated": true}
```

### DELETE /api/auth/users/{id}

Cannot delete your own account.

```json
{"deleted": true}
```

---

## Applications (API tokens)

### GET /api/auth/apps

```json
[{"id": 1, "name": "My App", "token": "tok_...", "created_at": "2025-01-01T00:00:00"}]
```

### POST /api/auth/apps

```json
// Request
{"name": "My App"}

// Response
{"id": 1, "name": "My App", "token": "tok_..."}
```

### POST /api/auth/apps/{id}/regenerate

```json
{"token": "tok_new..."}
```

### DELETE /api/auth/apps/{id}

```json
{"deleted": true}
```

---

## System

### GET /api/version

```json
{"version": "1.3.5", "pro": false}
```

### GET /api/settings

```json
{"serverName": "My Server", "showHiddenFiles": "0", ...}
```

### PATCH /api/settings

```json
// Request (all optional)
{"serverName": "New Name", "showHiddenFiles": true, "license_key": "..."}

// Response — full settings object
{"serverName": "New Name", "showHiddenFiles": "1", ...}
```

### GET /api/pro/status

```json
{"active": true, "features": ["webdav", "agents"], "version": "1.0.0"}
// or
{"active": false, "features": []}
```

### POST /api/restart

```json
{"message": "Server restarting..."}
```

### POST /api/maintenance/reset-queues

Cancels all running operations, clears temp DBs and pending queues.

```json
{"opsCancelled": 2, "tempFilesRemoved": 1, "message": "All operations stopped and queues reset."}
```

---

## Themes

### GET /api/themes

```json
[{"name": "default", "builtIn": true}, {"name": "dark-blue", "builtIn": false}]
```

### POST /api/themes

```json
// Request
{"name": "my-theme", "css": ":root { ... }", "overwrite": false}

// Response
{"saved": "my-theme"}
```

### DELETE /api/themes/{name}

Only user themes can be deleted.

```json
{"deleted": "my-theme"}
```

---

## Locations

### GET /api/locations

Returns the shallow location tree (locations with root-level folders).

```json
[
  {
    "id": "loc-1", "name": "Photos", "root_path": "/mnt/photos",
    "file_count": 1234, "total_size": 5678901234,
    "agent_id": 1, "agent_name": "Local Agent", "agent_online": true,
    "is_favourite": false,
    "scheduleEnabled": true, "scheduleDays": [1,3,5], "scheduleTime": "03:00",
    "children": [
      {"id": "fld-10", "name": "2024", "file_count": 500, "has_children": true}
    ]
  }
]
```

### POST /api/locations

```json
// Request
{"name": "New Location", "path": "/mnt/new"}

// Response (201)
{"id": 5, "name": "New Location", "root_path": "/mnt/new", ...}
```

### PATCH /api/locations/{id}

`{id}` can be `5` or `loc-5`.

```json
// Rename
{"name": "Renamed"}

// Schedule
{"scheduleEnabled": true, "scheduleDays": [0,6], "scheduleTime": "02:00"}

// Response
{"updated": true}
// or the renamed location object
```

### DELETE /api/locations/{id}

Queues a background purge.

```json
{"message": "Location 'Photos' deleted."}
```

### GET /api/locations/{id}/stats

```json
{"file_count": 1234, "total_size": 5678901234, "type_breakdown": {...}, ...}
```

### POST /api/locations/{id}/download

Starts async ZIP build of the entire location.

```json
{"jobId": "abc-123", "total": 1234}
```

### POST /api/locations/{id}/reset-stale

Queues removal of stale entries.

```json
{"started": true, "op_id": 42}
```

---

## Tree

### GET /api/tree/children?ids=1,2,3

Batch fetch immediate children for folder IDs (comma-separated).

```json
{"1": [{"id": "fld-10", "name": "Sub", ...}], "2": [...]}
```

### GET /api/tree/expand?target=42

Ancestor chain + children at each level for a target folder.

```json
{"ancestors": [...], "children": {...}}
```

### GET /api/treemap/{id}?parent_id=42

Treemap children with cumulative sizes for a location. `parent_id` is optional (omit for root).

```json
[{"id": "fld-10", "name": "Folder", "size": 123456, ...}]
```

---

## Favourites

### POST /api/favourite/toggle

```json
// Request
{"id": "loc-1"}  // or "fld-42"

// Response
{"id": "loc-1", "favourite": true}
```

### GET /api/favourites

```json
[
  {"id": "loc-1", "name": "Photos", "type": "location", "path": "Photos", "locationId": "loc-1"},
  {"id": "fld-42", "name": "2024", "type": "folder", "path": "Local Agent / Photos / 2024", "locationId": "loc-1"}
]
```

---

## Files

### GET /api/files

Query params: `folder_id` (required, e.g. `loc-1` or `fld-42`), `page` (0), `sort` (name|type|size|date|dups), `sortDir` (asc|desc), `filter`, `focusFile` (file ID), `fresh` (skip freshness check).

```json
{
  "items": [
    {
      "id": 123, "name": "photo.jpg", "rel_path": "2024/photo.jpg",
      "typeHigh": "image", "typeLow": "jpg", "size": 4567890,
      "date": "2024-06-15T10:30:00", "stale": false, "hidden": false,
      "hashFast": "a1b2c3d4e5f67890", "hashStrong": null,
      "dupCount": 2, "pendingOp": null
    }
  ],
  "folders": [
    {"id": "fld-10", "name": "Subfolder", "file_count": 50, "has_children": true}
  ],
  "total": 500,
  "page": 0,
  "pageSize": 120,
  "breadcrumb": [
    {"nodeId": "loc-1", "name": "Photos"},
    {"nodeId": "fld-5", "name": "2024"}
  ]
}
```

### GET /api/files/{id}

Full file detail.

```json
{
  "id": 123,
  "name": "photo.jpg",
  "folderId": "fld-5",
  "locationId": "loc-1",
  "locationOnline": true,
  "locationName": "Photos",
  "path": "/Photos/2024/photo.jpg",
  "online": true,
  "typeHigh": "image",
  "typeLow": "jpg",
  "size": 4567890,
  "date": "2024-06-15T10:30:00",
  "created": "2024-06-15T10:30:00",
  "cataloged": "2024-07-01T12:00:00",
  "lastSeen": "2024-08-20T09:00:00",
  "hashPartial": "abc123...",
  "hashFast": "a1b2c3d4e5f67890",
  "hashStrong": null,
  "verified": false,
  "stale": false,
  "pendingOp": null,
  "description": "",
  "tags": ["holiday", "beach"],
  "duplicates": [
    {"fileId": 456, "name": "photo.jpg", "location": "Backup", "agent": "Remote", "locationId": 2, "path": "/backup/photo.jpg"}
  ],
  "dupTotal": 1,
  "breadcrumb": [
    {"nodeId": "loc-1", "name": "Photos"},
    {"nodeId": "fld-5", "name": "2024"}
  ],
  "canTranscode": false,
  "transcodeStatus": null
}
```

### PATCH /api/files/{id}

```json
// Request (all optional)
{"description": "Sunset at the beach", "tags": "holiday, beach"}

// Response — full file detail (as above) plus:
{"tagsPropagated": 3, ...}
```

### DELETE /api/files/{id}?all_duplicates=true

`all_duplicates` is optional (default false).

```json
{
  "filename": "photo.jpg",
  "deleted_from_disk": true,
  // with all_duplicates:
  "deleted_count": 3,
  "deleted_from_disk_count": 2,
  "deferred_count": 1
}
```

### POST /api/files/{id}/move

```json
// Request
{"name": "renamed.jpg", "destination_folder_id": "fld-10", "copy": false}

// Response
{
  "old_name": "photo.jpg", "new_name": "renamed.jpg",
  "renamed": true, "moved": true
}
```

### POST /api/files/{id}/cancel-pending

Cancel a deferred operation on a file.

```json
{"cancelled": true, "filename": "photo.jpg"}
```

### GET /api/files/{id}/content

Streams the file content through the agent. Add `?download=true` for attachment disposition. Supports range requests.

### GET /api/files/{id}/bytes?offset=0&limit=4096

Raw byte slice for hex viewer. Max limit 65536.

Response: `application/octet-stream` with headers `X-File-Size`, `X-Offset`.

### GET /api/files/{id}/base64

```json
{"media_type": "image/jpeg", "data": "/9j/4AAQ..."}
```

### POST /api/files/{id}/verify

Starts background SHA-256 verification for the file's entire dup group.

```json
{"verifying": true, "filename": "photo.jpg", "groupSize": 3}
```

### POST /api/files/{id}/rehash

Recomputes hash_partial + hash_fast for a single file.

```json
// Response — full file detail
```

### POST /api/files/rehash

Batch rehash via queue.

```json
// Request
{"fileIds": [1, 2, 3]}

// Response
{"queued": 3}
```

### POST /api/files/dup-counts

Live duplicate counts for a list of hashes.

```json
// Request
{"hashes": ["a1b2c3d4e5f67890", "..."]}

// Response
{"counts": {"a1b2c3d4e5f67890": 3, ...}}
```

### POST /api/files/{id}/transcode

Queue video transcode on the agent.

```json
// Request (optional)
{"quality": "medium"}  // low|medium|high

// Response
{"started": true, "op_id": 42}
```

---

## Tags

### GET /api/tags

Returns the full tag vocabulary.

```json
["beach", "holiday", "work"]
```

### GET /api/tags/{tags}

Find files matching **all** specified tags (AND). Tags are comma-separated in the URL path and normalised (case-insensitive, whitespace-trimmed).

```
GET /api/tags/people,outdoors,sunny
```

```json
[
  {
    "id": 123,
    "name": "beach.jpg",
    "path": "holiday/beach.jpg",
    "typeHigh": "image",
    "typeLow": "jpg",
    "size": 4567890,
    "date": "2024-06-15T10:30:00",
    "locationId": 1,
    "location": "Photos"
  }
]
```

---

## Folders

### POST /api/folders

```json
// Request
{"parent_id": "loc-1", "name": "New Folder"}  // or "fld-42"

// Response (201)
{"id": 50, "name": "New Folder", ...}
```

### POST /api/folders/{id}/move

```json
// Request
{"destination_parent_id": "fld-10", "name": "Renamed", "copy": false}

// Response
{"name": "Renamed", "old_name": "Original", "renamed": true, "moved": true}
```

### POST /api/folders/{id}/download

Start async ZIP build for a folder.

```json
{"jobId": "abc-123", "total": 50}
```

### POST /api/folders/{id}/dup-exclude

Toggle duplicate exclusion on a folder tree. First call returns counts for confirmation; second call with `confirmed: true` starts the operation.

```json
// Request (confirmation)
{"exclude": true}
// Response
{"confirm": true, "folderName": "Archive", "folderCount": 10, "fileCount": 500, "direction": "exclude"}

// Request (execute)
{"exclude": true, "confirmed": true}
// Response
{"started": true}
```

### GET /api/dup-exclude/progress

```json
{"status": "running", "processed": 100, "total": 500, ...}
```

### POST /api/folders/{id}/reset-stale

```json
{"started": true, "op_id": 42}
```

### DELETE /api/folders/{id}

```json
{"name": "Old Folder", "file_count": 25, "deleted_from_disk": true}
```

### GET /api/folders/{id}/stats

```json
{"file_count": 100, "total_size": 12345678, ...}
```

---

## Search

### GET /api/search

**Standard mode** query params: `name`, `nameMatch` (anywhere|starts|exact), `type` (image|video|audio|document|archive|other), `description`, `tags` (comma-separated), `sizeMin`, `sizeMax` (human: "1MB"), `dateFrom`, `dateTo` (YYYY-MM-DD), `dupes` (true), `minDups`, `maxDups`, `hash` (find by hash), `files` (false to exclude), `folders` (true to include), `scopeType` (location|folder), `scopeId` (loc-N or fld-N), `page`, `sort`, `sortDir`, `searchId` (reuse cached results), `cachedTotal`.

**Advanced mode**: `mode=advanced`, plus `c[0].field`, `c[0].op`, `c[0].value`, `c[0].exclude` pattern for conditions.

```json
{
  "items": [
    {"id": 123, "name": "photo.jpg", "rel_path": "2024/photo.jpg", ...}
  ],
  "folders": [],
  "total": 42,
  "page": 0,
  "pageSize": 120,
  "searchId": "s_abc123"
}
```

### GET /api/searches

Saved searches.

```json
[{"id": 1, "name": "Holiday photos", "params": "{...}", "created_at": "2025-01-01T00:00:00"}]
```

### POST /api/searches

```json
// Request
{"name": "My Search", "params": {"name": "photo", "type": "image"}}

// Response
{"id": 1}
```

### DELETE /api/searches/{id}

```json
{}
```

### GET /api/slideshow-ids

Query params: `folder_id` or `searchId`, `mediaType` (image|video|audio), `sort`, `sortDir`.

```json
{"ids": [1, 2, 3, 4, 5], "total": 5}
```

---

## Batch Operations

### POST /api/batch/delete

```json
// Request
{"file_ids": [1, 2], "folder_ids": [10], "all_duplicates": false}

// Response
{"started": true, "op_id": 42, "total": 3}
```

### POST /api/batch/move

```json
// Request
{"file_ids": [1, 2], "folder_ids": [10], "destination_folder_id": "fld-20", "copy": false}

// Response
{"moved_files": 2, "moved_folders": 1}
```

### POST /api/batch/tag

```json
// Request
{"file_ids": [1, 2, 3], "add_tags": "holiday, beach", "remove_tags": "work"}

// Response
{"started": true, "op_id": 42, "total": 3}
```

### POST /api/batch/download

Start async ZIP build for selected items.

```json
// Request
{"file_ids": [1, 2], "folder_ids": [10]}

// Response
{"jobId": "abc-123", "total": 25}
```

### GET /api/zip/{job_id}/download

Streams the built ZIP file. Returns `application/zip` with `Content-Disposition` and `Content-Length` headers.

---

## Consolidate

### POST /api/consolidate

Start a single-file consolidation (move or copy duplicates).

```json
// Request
{
  "file_id": 123,
  "mode": "keep_here",          // keep_here|move_to
  "consolidateMode": "move",    // move|copy
  "destination_folder_id": "fld-10",  // required for move_to and copy
  "filename_match_only": false,
  "stub_file_ids": [456, 789]   // optional: specific copies to process
}

// Response
{"message": "Consolidation started for 'photo.jpg'"}
```

### POST /api/consolidate/preview

Preview duplicate counts and locations for a set of files.

```json
// Request
{"file_ids": [123, 456]}

// Response
{
  "total_dups": 5,
  "filename_matched_dups": 3,
  "duplicates": [
    {"fileId": 789, "name": "photo.jpg", "location": "Backup", "agent": "Remote", "locationId": 2, "path": "/backup/photo.jpg"}
  ]
}
```

### POST /api/batch/consolidate

Batch consolidation in background.

```json
// Request
{
  "file_ids": [123, 456],
  "mode": "keep_here",
  "consolidateMode": "move",
  "destination_folder_id": "fld-10",
  "filename_match_only": false
}

// Response
{"message": "Batch consolidation started for 2 files"}
```

---

## Merge

### POST /api/merge

Merge one folder/location into another.

```json
// Request
{"source_id": "fld-10", "destination_id": "fld-20", "mode": "move"}  // move|copy

// Response
{"message": "Move started: Source Folder -> Dest Folder"}
```

### POST /api/merge/cancel

```json
{"message": "Merge cancellation requested."}
```

---

## Scan

### POST /api/scan

Start a full scan.

```json
// Request
{"location_id": "loc-1", "folder_id": "fld-10"}  // folder_id optional

// Response
{"message": "Scan queued for 'Photos / Subfolder'", "queue_id": 42}
```

### POST /api/scan/quick

Start a quick scan (requires agent support).

```json
// Request
{"location_id": "loc-1", "folder_id": "fld-10"}

// Response
{"message": "Quick scan started"}
```

### GET /api/scan/capabilities?location_id=1

```json
{"quick_scan": true}
```

### POST /api/scan/cancel

```json
// Cancel by queue ID
{"queue_id": 42}
// or by location
{"location_id": "loc-1", "type": "scan"}  // scan|backfill

// Response
{"message": "Operation cancelled."}
```

### GET /api/scan/queue

```json
[{"queue_id": 42, "type": "scan_dir", "location_id": 1, "name": "Photos", "status": "running"}]
```

---

## Upload

### POST /api/upload

Multipart form data. Fields: `target_id` (loc-N or fld-N), `files` (file uploads), `mtimes` (JSON array of ms timestamps).

```json
{"message": "Uploading 3 file(s) to Photos", "fileCount": 3}
```

---

## Browse (Filesystem)

### GET /api/browse?path=/mnt

Browse the local agent's filesystem (for location setup).

```json
{"path": "/mnt", "entries": [{"name": "photos", "type": "directory"}, ...]}
```

---

## Browse (Catalog)

Hierarchical catalog discovery. Path segments: `{agent}/{location}/{path}`.

### GET /api/browse/

```json
{"kind": "root", "children": [{"kind": "agent", "name": "Local Agent"}]}
```

### GET /api/browse/{agent}

```json
{"kind": "agent", "name": "Local Agent", "children": [{"kind": "location", "name": "Photos"}]}
```

### GET /api/browse/{agent}/{location}/{path}

```json
{
  "kind": "folder",
  "agent": "Local Agent",
  "location": "Photos",
  "children": [
    {"kind": "folder", "name": "2024", "path": "2024"},
    {"kind": "file", "id": 123, "name": "photo.jpg", "path": "photo.jpg", "size": 4567890, "type": "image", "subtype": "jpg", "modified": "2024-06-15T10:30:00"}
  ]
}
```

---

## Ignore Rules

### GET /api/ignore

```json
[{"id": 1, "filename": "Thumbs.db", "file_size": 0, "location_id": null, "created_at": "..."}]
```

### POST /api/ignore

```json
// Request
{"filename": "Thumbs.db", "file_size": 0, "location_id": null}

// Response
{"id": 1, "filename": "Thumbs.db", ...}
```

### GET /api/ignore/check?filename=Thumbs.db&file_size=0&location_id=1

```json
{"ignored": true, "rule": {"id": 1, ...}}
```

### GET /api/ignore/count?filename=Thumbs.db&file_size=0

```json
{"count": 42}
```

### DELETE /api/ignore/{id}

```json
{}
```

---

## Stats

### GET /api/stats

```json
{
  "total_files": 16000000, "total_size": 12345678901234,
  "locations": 20, "agents": 6,
  "type_breakdown": {"image": 8000000, "video": 2000000, ...},
  ...
}
```

### POST /api/stats/repair

Start catalog repair with selectable phases.

```json
// Request (optional — default runs all)
{"phases": ["partials", "hashes", "duplicates", "sizes"]}

// Response
{"status": "started", "phases": ["partials", "hashes", "duplicates", "sizes"]}
```

### GET /api/stats/repair-progress

```json
{
  "status": "running",
  "phase": "hashing",
  "hashed": 500, "total": 1000, "skipped": 50, "errors": 2, "stale": 3,
  ...
}
```

### POST /api/admin/rehash-partial

Enqueue hash_partial computation for all agent-backed locations.

```json
{"queued": 6, "operation_ids": [1, 2, 3, 4, 5, 6]}
```

---

## Update

### POST /api/update/check

```json
// Request
{"key": "license-key"}

// Response — update server data
```

### POST /api/update/install

```json
// Request
{"key": "license-key"}

// Response — install result
```

### POST /api/update/upload

Multipart form: `file` (update package).

### GET /api/update/check-release

Check GitHub for latest version.

```json
{"current": "1.3.5", "latest": "1.3.6", "update_available": true, "download_url": "..."}
```

### POST /api/update/apply-release

Download and install latest from GitHub, then restart.

```json
{"message": "Updated to v1.3.6. Restarting..."}
```

---

## WebSocket

### WS /ws

Client WebSocket for real-time events (scan progress, file changes, queue updates).

### WS /ws/agent

Agent WebSocket for agent registration and bidirectional communication.
