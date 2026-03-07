#!/usr/bin/env bash
set -euo pipefail

# ── Package File Hunter for release ──────────────────────────────────
#
# Creates filehunter-VERSION.tar.gz and filehunter-VERSION.zip
# containing everything needed to run: server, agent, core, static.
#
# Usage: ./package.sh [output_dir]
#   output_dir defaults to ./dist

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENT_DIR="$(cd "$SCRIPT_DIR/../file-hunter-agent" && pwd)"

VERSION=$(cat "$SCRIPT_DIR/VERSION" | tr -d '[:space:]')
RELEASE_NAME="filehunter-${VERSION}"
OUTPUT_DIR="${1:-$SCRIPT_DIR/dist}"

# ── Preflight checks ────────────────────────────────────────────────

if [ ! -d "$AGENT_DIR/file_hunter_agent" ]; then
    echo "Error: Agent repo not found at $AGENT_DIR" >&2
    exit 1
fi

if [ -d "$OUTPUT_DIR/$RELEASE_NAME" ]; then
    echo "Error: $OUTPUT_DIR/$RELEASE_NAME already exists. Remove it first." >&2
    exit 1
fi

for f in "$OUTPUT_DIR/${RELEASE_NAME}.tar.gz" "$OUTPUT_DIR/${RELEASE_NAME}.zip"; do
    if [ -f "$f" ]; then
        echo "Error: $f already exists. Remove it first." >&2
        exit 1
    fi
done

echo "Packaging File Hunter v${VERSION}"

# ── Build staging directory ──────────────────────────────────────────

mkdir -p "$OUTPUT_DIR"
STAGING="$OUTPUT_DIR/$RELEASE_NAME"
mkdir -p "$STAGING"

# Server package
cp -R "$SCRIPT_DIR/file_hunter" "$STAGING/"

# Shared core
cp -R "$SCRIPT_DIR/file_hunter_core" "$STAGING/"

# Agent package (from sibling repo)
cp -R "$AGENT_DIR/file_hunter_agent" "$STAGING/"

# Static assets
cp -R "$SCRIPT_DIR/static" "$STAGING/"

# Top-level files
cp "$SCRIPT_DIR/filehunter" "$STAGING/"
cp "$SCRIPT_DIR/requirements.txt" "$STAGING/"
cp "$SCRIPT_DIR/VERSION" "$STAGING/"
cp "$SCRIPT_DIR/LICENSE" "$STAGING/"
cp "$SCRIPT_DIR/README.md" "$STAGING/"
cp "$SCRIPT_DIR/update_url" "$STAGING/"

# Merge agent requirements (add any agent-only deps not in server requirements)
while IFS= read -r line; do
    dep=$(echo "$line" | sed 's/[>=<].*//' | tr -d '[:space:]')
    [ -z "$dep" ] && continue
    if ! grep -qi "^${dep}" "$STAGING/requirements.txt"; then
        echo "$line" >> "$STAGING/requirements.txt"
    fi
done < "$AGENT_DIR/requirements.txt"

# ── Clean unwanted files ────────────────────────────────────────────

find "$STAGING" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$STAGING" -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
find "$STAGING" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
find "$STAGING" -name "*.pyc" -delete 2>/dev/null || true
find "$STAGING" -name "*.pyo" -delete 2>/dev/null || true
find "$STAGING" -name ".DS_Store" -delete 2>/dev/null || true

# ── Create archives ─────────────────────────────────────────────────

cd "$OUTPUT_DIR"

tar czf "${RELEASE_NAME}.tar.gz" "$RELEASE_NAME"
echo "  Created ${RELEASE_NAME}.tar.gz"

zip -qr "${RELEASE_NAME}.zip" "$RELEASE_NAME"
echo "  Created ${RELEASE_NAME}.zip"

# ── Clean up staging ────────────────────────────────────────────────

rm -rf "$STAGING"

echo "Done. Archives in $OUTPUT_DIR/"
