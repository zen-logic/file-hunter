import asyncio
import logging
import shutil
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

import httpx

from file_hunter.config import load_config

log = logging.getLogger(__name__)

_GITHUB_REPO = "zen-logic/file-hunter"

# Directories replaced during self-update (code only, never user data)
_CODE_DIRS = ("file_hunter", "file_hunter_core", "file_hunter_agent", "static")

# Items that must never be overwritten by an update
_PRESERVE = {"data", "config.json", "venv", "file-hunter-pro"}


_UPDATE_URL_FILE = Path(__file__).resolve().parent.parent.parent / "update_url"


def _get_update_url():
    config = load_config()
    url = config.get("update_url")
    if not url and _UPDATE_URL_FILE.exists():
        url = _UPDATE_URL_FILE.read_text().strip()
    if not url:
        raise ValueError("update_url not configured")
    return url.rstrip("/")


async def check_update(key: str) -> dict:
    """Call the reg service to check what version is available for this key."""
    url = _get_update_url()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(f"{url}/api/validate", params={"key": key})
    return resp.json()


async def install_from_zip(zip_path: Path) -> dict:
    """Validate a pro zip, extract to existing pro install dir, pip install -e. Cleans up zip_path."""
    tmp_dir = zip_path.parent

    # Validate zip
    try:
        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()
    except zipfile.BadZipFile:
        log.error("File is not a valid package")
        zip_path.unlink(missing_ok=True)
        tmp_dir.rmdir()
        return {"ok": False, "error": "File is not a valid Pro package"}

    # Detect common top-level prefix (e.g. "file-hunter-pro-1.0.0/")
    prefix = ""
    top_dirs = {n.split("/")[0] for n in names if "/" in n}
    if len(top_dirs) == 1:
        candidate = top_dirs.pop() + "/"
        if all(n.startswith(candidate) or n == candidate.rstrip("/") for n in names):
            prefix = candidate

    # Check required files
    stripped = {n.removeprefix(prefix) for n in names}
    required = {"pyproject.toml", "file_hunter_pro/__init__.py"}
    missing = required - stripped
    if missing:
        log.error("Invalid pro package: missing %s", missing)
        zip_path.unlink(missing_ok=True)
        tmp_dir.rmdir()
        return {
            "ok": False,
            "error": f"Invalid pro package: missing {', '.join(sorted(missing))}",
        }

    # Extract to existing pro install location, or fall back to ./file-hunter-pro/
    repo_root = (
        Path(__file__).resolve().parent.parent.parent
    )  # services/ -> file_hunter/ -> file-hunter/
    pro_dir = None
    try:
        import file_hunter_pro

        existing = Path(file_hunter_pro.__file__).resolve().parent.parent
        if (existing / "pyproject.toml").exists():
            pro_dir = existing
    except ImportError:
        pass
    if pro_dir is None:
        pro_dir = repo_root / "file-hunter-pro"

    if pro_dir.exists():
        await asyncio.to_thread(shutil.rmtree, pro_dir)

    with zipfile.ZipFile(zip_path) as zf:
        for member in zf.infolist():
            target = member.filename.removeprefix(prefix) if prefix else member.filename
            if not target:
                continue
            dest = pro_dir / target
            if member.is_dir():
                dest.mkdir(parents=True, exist_ok=True)
            else:
                dest.parent.mkdir(parents=True, exist_ok=True)
                with zf.open(member) as src, open(dest, "wb") as dst:
                    dst.write(src.read())

    # Editable pip install
    result = await asyncio.to_thread(
        subprocess.run,
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--break-system-packages",
            "-e",
            str(pro_dir),
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    # Clean up temp
    zip_path.unlink(missing_ok=True)
    tmp_dir.rmdir()

    if result.returncode != 0:
        log.error("pip install failed: %s", result.stderr.strip())
        return {"ok": False, "error": f"pip install failed: {result.stderr.strip()}"}

    log.info("Pro installed — restart required to activate")
    return {
        "ok": True,
        "data": {
            "message": "Pro installed — restart to activate",
            "restart_required": True,
        },
    }


async def install_update(key: str) -> dict:
    """Download the pro zip from reg service and install."""
    url = _get_update_url()

    # Download the zip
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(f"{url}/api/download", params={"key": key})

    if resp.status_code != 200:
        log.error("Download failed: HTTP %s", resp.status_code)
        try:
            return resp.json()
        except Exception:
            return {"ok": False, "error": f"Download failed (HTTP {resp.status_code})"}

    # Save to temp file
    filename = "file-hunter-pro.filehunter"
    disposition = resp.headers.get("content-disposition", "")
    if "filename=" in disposition:
        filename = disposition.split("filename=")[-1].strip('" ')

    tmp_dir = Path(tempfile.mkdtemp())
    zip_path = tmp_dir / filename
    zip_path.write_bytes(resp.content)

    return await install_from_zip(zip_path)


def _get_current_version() -> str:
    version_file = Path(__file__).resolve().parent.parent.parent / "VERSION"
    if version_file.exists():
        return version_file.read_text().strip()
    return "0.0.0"


async def check_github_release() -> dict:
    """Check GitHub releases for the latest version."""
    current = _get_current_version()
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            f"https://api.github.com/repos/{_GITHUB_REPO}/releases/latest",
            headers={"Accept": "application/vnd.github.v3+json"},
        )
    if resp.status_code != 200:
        raise RuntimeError(f"GitHub API returned {resp.status_code}")
    data = resp.json()
    tag = data.get("tag_name", "")
    latest = tag.lstrip("v")
    return {
        "current": current,
        "latest": latest,
        "tag": tag,
        "update_available": latest != current,
    }


async def apply_github_update() -> dict:
    """Download latest release from GitHub and replace code in-place.

    Replaces file_hunter/, file_hunter_core/, file_hunter_agent/, static/.
    Never touches data/, config.json, venv/, file-hunter-pro/.
    Returns after file replacement; caller should trigger restart.
    """
    install_dir = Path(__file__).resolve().parent.parent.parent

    # Get latest release info
    release = await check_github_release()
    tag = release["tag"]
    latest = release["latest"]

    # Download tar.gz
    archive_name = f"filehunter-{latest}.tar.gz"
    url = f"https://github.com/{_GITHUB_REPO}/releases/download/{tag}/{archive_name}"

    log.info("Downloading %s", url)
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        resp = await client.get(url)
    if resp.status_code != 200:
        raise RuntimeError(f"Download failed: HTTP {resp.status_code}")

    # Save to temp
    tmp_dir = Path(tempfile.mkdtemp())
    archive_path = tmp_dir / archive_name
    archive_path.write_bytes(resp.content)

    # Extract
    try:
        with tarfile.open(archive_path, "r:gz") as tf:
            tf.extractall(tmp_dir)
    except tarfile.TarError as e:
        shutil.rmtree(tmp_dir)
        raise RuntimeError(f"Invalid archive: {e}") from e

    extracted = tmp_dir / f"filehunter-{latest}"
    if not extracted.is_dir():
        shutil.rmtree(tmp_dir)
        raise RuntimeError("Unexpected archive structure")

    # Validate: extracted must contain file_hunter/ and VERSION
    if (
        not (extracted / "file_hunter").is_dir()
        or not (extracted / "VERSION").is_file()
    ):
        shutil.rmtree(tmp_dir)
        raise RuntimeError("Archive missing required files")

    # Remove old code directories
    for dirname in _CODE_DIRS:
        target = install_dir / dirname
        if target.is_dir():
            shutil.rmtree(target)
            log.info("Removed %s", target)

    # Copy new files, preserving user data
    for item in extracted.iterdir():
        if item.name in _PRESERVE:
            continue
        dest = install_dir / item.name
        if item.is_dir():
            shutil.copytree(item, dest)
        else:
            shutil.copy2(item, dest)
    log.info("Updated to v%s", latest)

    # Make launcher executable
    launcher = install_dir / "filehunter"
    if launcher.exists():
        launcher.chmod(launcher.stat().st_mode | 0o111)

    # Clean up temp
    shutil.rmtree(tmp_dir)

    return {"version": latest}
