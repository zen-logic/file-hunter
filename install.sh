#!/usr/bin/env bash
set -euo pipefail

# ── Install File Hunter ──────────────────────────────────────────────
#
# curl -fsSL https://filehunter.zenlogic.uk/install | bash
#   or
# curl -fsSL https://raw.githubusercontent.com/zen-logic/file-hunter/main/install.sh | bash

REPO="zen-logic/file-hunter"

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
error() { printf '\033[1;31mError:\033[0m %s\n' "$*" >&2; exit 1; }

# ── Get latest release tag ───────────────────────────────────────────

info "Fetching latest release..."
TAG=$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')

if [ -z "$TAG" ]; then
    error "Could not determine latest release."
fi

VERSION="${TAG#v}"
ARCHIVE="filehunter-${VERSION}.tar.gz"
URL="https://github.com/${REPO}/releases/download/${TAG}/${ARCHIVE}"

info "Downloading File Hunter ${VERSION}..."
curl -fSL "$URL" -o "/tmp/${ARCHIVE}" || error "Download failed."

# ── Extract ──────────────────────────────────────────────────────────

info "Extracting to filehunter-${VERSION}/..."
tar xzf "/tmp/${ARCHIVE}"
rm -f "/tmp/${ARCHIVE}"

# ── Done ─────────────────────────────────────────────────────────────

echo ""
info "File Hunter ${VERSION} installed."
echo ""
echo "  cd filehunter-${VERSION}"
echo "  ./filehunter"
echo ""
