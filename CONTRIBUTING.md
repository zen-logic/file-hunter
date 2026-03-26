# Contributing to File Hunter

Thanks for your interest in contributing.

## Setting up

File Hunter bundles the server, agent, and shared core into a single package. The `filehunter` launcher manages its own virtual environment.

### From a release archive

Download the latest release from [GitHub Releases](https://github.com/zen-logic/file-hunter/releases), extract it, and run:

```bash
./filehunter
```

The launcher creates a venv, installs dependencies, and starts the server. First run prompts for basic configuration.

### From source (development)

```bash
git clone https://github.com/zen-logic/file-hunter.git
cd file-hunter
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m file_hunter
```

To populate with sample data on first run:

```bash
python -m file_hunter --demo
```

The agent is a separate repo ([file-hunter-agent](https://github.com/zen-logic/file-hunter-agent)). For local development with both, clone it alongside `file-hunter/` and run the agent separately.

## Code style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
pip install ruff
ruff check .
ruff format .
```

Please run both before submitting a PR.

## Architecture

- **Backend** — Python, Starlette, uvicorn, aiosqlite. No frameworks beyond Starlette.
- **Frontend** — vanilla HTML/CSS/JavaScript. No build step, no bundler, no npm.
- **Databases** — three SQLite databases in WAL mode:
  - `file_hunter.db` — catalog (files, folders, locations, operations, config)
  - `hashes.db` — all hash data (partial, fast, strong) and dup counts
  - `stats.db` — folder and location size/count statistics
- **Agent** — separate process per machine, connects to the server via WebSocket, serves file operations via HTTP.

Key directories:

| Directory | Contents |
|-----------|----------|
| `file_hunter/routes/` | HTTP route handlers |
| `file_hunter/services/` | Business logic |
| `file_hunter/ws/` | WebSocket handlers (agent connections, browser broadcasts) |
| `file_hunter/helpers.py` | Shared helpers (ID parsing, stats updates, hash lookups, target resolution) |
| `file_hunter/db.py` | Catalog database connection and schema |
| `file_hunter/hashes_db.py` | Hashes database connection and schema |
| `file_hunter/stats_db.py` | Stats database connection and schema |
| `static/js/` | Frontend JavaScript |
| `static/css/` | Stylesheets and themes |

## Guidelines

- Keep PRs focused — one feature or fix per PR.
- The UI must stay responsive during scans. Don't run expensive queries on the shared reader connection (`read_db()`). Use `open_connection()` for heavy reads.
- Target scale is 10M+ files. Avoid per-row loops — use batch operations or single SQL statements.
- Schema changes are additive only (`ALTER TABLE ADD COLUMN` in try/except).
- No new runtime dependencies without discussion first. The dependency footprint is intentionally small.
- Frontend changes: no frameworks, no build tools, no npm. Vanilla JS only.

## Reporting bugs

Use the [bug report template](https://github.com/zen-logic/file-hunter/issues/new?template=bug_report.yml). Include server logs if possible — most UI issues trace back to a 500 error.

## License

By contributing you agree that your contributions will be licensed under the MIT License.
