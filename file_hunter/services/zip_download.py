"""Async ZIP download — build in background, serve when ready."""

import asyncio
import logging
import os
import tempfile
import time
import zipfile

from file_hunter.services.content_proxy import stream_agent_file
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

# Active jobs: job_id -> {status, progress, total, tmp_path, filename, file_size, task, created}
_jobs: dict[str, dict] = {}
_job_counter = 0
_CLEANUP_TIMEOUT = 600  # 10 minutes — auto-delete unclaimed ZIPs


def _next_job_id() -> str:
    global _job_counter
    _job_counter += 1
    return f"zip-{_job_counter}"


async def start_build(files: list[tuple[str, str, int]], zip_name: str) -> str:
    """Kick off an async ZIP build. Returns job_id immediately."""
    job_id = _next_job_id()
    _jobs[job_id] = {
        "status": "building",
        "progress": 0,
        "total": len(files),
        "tmp_path": None,
        "filename": zip_name,
        "file_size": 0,
        "task": None,
        "created": time.monotonic(),
    }
    task = asyncio.create_task(_build(job_id, files, zip_name))
    _jobs[job_id]["task"] = task
    return job_id


async def _build(job_id: str, files: list[tuple[str, str, int]], zip_name: str):
    """Build the ZIP in a temp file, broadcasting progress via WS."""
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_fd)
    job = _jobs[job_id]
    job["tmp_path"] = tmp_path

    try:
        total = len(files)
        done = 0

        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_STORED) as zf:
            for full_path, arc_name, loc_id in files:
                async with stream_agent_file(full_path, loc_id) as chunks:
                    if chunks is None:
                        done += 1
                        continue
                    with zf.open(arc_name, "w", force_zip64=True) as entry:
                        async for chunk in chunks:
                            entry.write(chunk)
                done += 1
                job["progress"] = done
                if done % 10 == 0 or done == total:
                    await broadcast(
                        {
                            "type": "zip_progress",
                            "jobId": job_id,
                            "done": done,
                            "total": total,
                            "filename": zip_name,
                        }
                    )

        file_size = os.path.getsize(tmp_path)
        job["file_size"] = file_size
        job["status"] = "ready"

        log.info("ZIP ready: %s (%d files, %d bytes)", zip_name, total, file_size)
        await broadcast(
            {
                "type": "zip_ready",
                "jobId": job_id,
                "filename": zip_name,
                "fileSize": file_size,
            }
        )

        # Schedule cleanup if nobody downloads within timeout
        asyncio.create_task(_cleanup_after_timeout(job_id))

    except asyncio.CancelledError:
        _cleanup_job(job_id)
        log.info("ZIP build cancelled: %s", zip_name)
    except Exception:
        log.error("ZIP build failed: %s", zip_name, exc_info=True)
        _cleanup_job(job_id)
        await broadcast(
            {
                "type": "zip_error",
                "jobId": job_id,
                "filename": zip_name,
            }
        )


def get_job(job_id: str) -> dict | None:
    return _jobs.get(job_id)


def cleanup_job(job_id: str):
    """Public cleanup — called after download stream completes."""
    _cleanup_job(job_id)


def _cleanup_job(job_id: str):
    job = _jobs.pop(job_id, None)
    if job and job.get("tmp_path"):
        try:
            os.unlink(job["tmp_path"])
        except OSError:
            pass


def cancel_job(job_id: str):
    job = _jobs.get(job_id)
    if job and job.get("task") and not job["task"].done():
        job["task"].cancel()


async def _cleanup_after_timeout(job_id: str):
    await asyncio.sleep(_CLEANUP_TIMEOUT)
    if job_id in _jobs:
        log.info("ZIP download expired, cleaning up: %s", job_id)
        _cleanup_job(job_id)
