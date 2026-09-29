"""Async ZIP download — build in background, serve when ready."""

import asyncio
import logging
import os
import tempfile
import time
import zipfile

from file_hunter.services.activity import (
    register as activity_register,
    update as activity_update,
    unregister as activity_unregister,
)
from file_hunter.services.content_proxy import stream_agent_file
from file_hunter.ws.scan import broadcast

log = logging.getLogger(__name__)

# Active jobs: job_id -> {status, progress, total, tmp_path, filename, file_size, task, created}
jobs: dict[str, dict] = {}
job_counter = 0
CLEANUP_TIMEOUT = 600  # 10 minutes — auto-delete unclaimed ZIPs


def next_job_id() -> str:
    global job_counter
    job_counter += 1
    return f"zip-{job_counter}"


async def start_build(files: list[tuple[str, str, int]], zip_name: str) -> str:
    """Kick off an async ZIP build. Returns job_id immediately."""
    job_id = next_job_id()
    jobs[job_id] = {
        "status": "building",
        "progress": 0,
        "total": len(files),
        "tmp_path": None,
        "filename": zip_name,
        "file_size": 0,
        "task": None,
        "created": time.monotonic(),
    }
    activity_register(job_id, f"Building ZIP: {zip_name}", progress=f"0/{len(files)}")
    task = asyncio.create_task(build(job_id, files, zip_name))
    jobs[job_id]["task"] = task
    return job_id


async def build(job_id: str, files: list[tuple[str, str, int]], zip_name: str):
    """Build the ZIP in a temp file, broadcasting progress via WS."""
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_fd)
    job = jobs[job_id]
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
                    activity_update(job_id, progress=f"{done}/{total}")
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

        activity_unregister(job_id)
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
        asyncio.create_task(cleanup_after_timeout(job_id))

    except asyncio.CancelledError:
        activity_unregister(job_id)
        cleanup_job(job_id)
        log.info("ZIP build cancelled: %s", zip_name)
    except Exception:
        activity_unregister(job_id)
        log.error("ZIP build failed: %s", zip_name, exc_info=True)
        cleanup_job(job_id)
        await broadcast(
            {
                "type": "zip_error",
                "jobId": job_id,
                "filename": zip_name,
            }
        )


def get_job(job_id: str) -> dict | None:
    return jobs.get(job_id)


def cleanup_job(job_id: str):
    """Drop the job and delete its temporary ZIP."""
    job = jobs.pop(job_id, None)
    if job and job.get("tmp_path"):
        try:
            os.unlink(job["tmp_path"])
        except OSError:
            pass


def cancel_job(job_id: str):
    job = jobs.get(job_id)
    if job and job.get("task") and not job["task"].done():
        job["task"].cancel()


async def cleanup_after_timeout(job_id: str):
    await asyncio.sleep(CLEANUP_TIMEOUT)
    if job_id in jobs:
        log.info("ZIP download expired, cleaning up: %s", job_id)
        cleanup_job(job_id)
