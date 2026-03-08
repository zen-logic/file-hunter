import asyncio
import logging
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

import httpx

from file_hunter.config import load_config

log = logging.getLogger(__name__)


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
