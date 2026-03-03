#!/usr/bin/env bash
set -euo pipefail

REPO="https://github.com/zen-logic/file-hunter.git"
INSTALL_DIR="$HOME/.filehunter"
BIN_DIR="$HOME/.local/bin"

# ── Helpers ──────────────────────────────────────────────────────────

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn()  { printf '\033[1;33mWarning:\033[0m %s\n' "$*"; }
error() { printf '\033[1;31mError:\033[0m %s\n' "$*" >&2; exit 1; }

# ── Find Python 3.10+ ───────────────────────────────────────────────

find_python() {
    for cmd in python3 python; do
        if command -v "$cmd" >/dev/null 2>&1; then
            version=$("$cmd" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")' 2>/dev/null) || continue
            major=${version%%.*}
            minor=${version#*.}
            if [ "$major" -eq 3 ] && [ "$minor" -ge 10 ]; then
                echo "$cmd"
                return
            fi
        fi
    done
    return 1
}

PYTHON=$(find_python) || error "Python 3.10+ is required but not found."
info "Using $PYTHON ($($PYTHON --version 2>&1))"

# ── Install or update ───────────────────────────────────────────────

if [ -d "$INSTALL_DIR/.git" ]; then
    info "Existing installation found — updating..."
    cd "$INSTALL_DIR"
    git pull --ff-only || error "git pull failed. Resolve manually in $INSTALL_DIR"
else
    if [ -e "$INSTALL_DIR" ]; then
        error "$INSTALL_DIR already exists but is not a git repo. Remove it first."
    fi
    info "Cloning File Hunter into $INSTALL_DIR..."
    git clone "$REPO" "$INSTALL_DIR"
    cd "$INSTALL_DIR"
fi

# ── Virtual environment ─────────────────────────────────────────────

if [ ! -d "$INSTALL_DIR/venv" ]; then
    info "Creating virtual environment..."
    "$PYTHON" -m venv "$INSTALL_DIR/venv"
fi

info "Installing dependencies..."
"$INSTALL_DIR/venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/venv/bin/pip" install --quiet -r "$INSTALL_DIR/requirements.txt"

# ── Launcher script ─────────────────────────────────────────────────

mkdir -p "$BIN_DIR"

cat > "$BIN_DIR/filehunter" <<'LAUNCHER'
#!/usr/bin/env bash
set -euo pipefail
INSTALL_DIR="$HOME/.filehunter"
exec "$INSTALL_DIR/venv/bin/python" "$INSTALL_DIR/filehunter" "$@"
LAUNCHER
chmod +x "$BIN_DIR/filehunter"

# ── PATH check ──────────────────────────────────────────────────────

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *)
        warn "$BIN_DIR is not on your PATH."
        echo "  Add this to your shell profile (~/.bashrc, ~/.zshrc, etc.):"
        echo "    export PATH=\"\$HOME/.local/bin:\$PATH\""
        echo ""
        ;;
esac

# ── Done ─────────────────────────────────────────────────────────────

info "File Hunter installed successfully!"
echo ""
echo "  Run:  filehunter"
echo "  Then open http://localhost:8000 in your browser."
echo "  To change port, edit ~/$HOME/.filehunter/config.json"
echo ""
