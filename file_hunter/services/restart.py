import asyncio
import os


# Exit code that tells the launcher to restart both server and agent
RESTART_EXIT_CODE = 75


def schedule_restart(delay: float = 1.0):
    """Schedule a server restart after a short delay (so the HTTP response sends first)."""
    loop = asyncio.get_event_loop()
    loop.call_later(delay, _restart)


def _restart():
    """Exit with a special code so the launcher restarts server + agent."""
    os._exit(RESTART_EXIT_CODE)
