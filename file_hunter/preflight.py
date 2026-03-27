"""Pre-flight setup — runs before server and agent start.

Called by the launcher script to ensure the database exists and
the local agent config is written. This avoids any timing issues
between server and agent startup.
"""

import asyncio
import json
import sys
from pathlib import Path

from file_hunter.config import load_config
from file_hunter.db import close_db, get_db
from file_hunter.services.agents import ensure_local_agent


async def _run(base_dir: Path):
    # Ensure data directory exists
    data_dir = base_dir / "data"
    data_dir.mkdir(exist_ok=True)

    # Initialize the database (creates tables, runs migrations)
    db = await get_db()

    # Create the local agent if it doesn't exist
    token = await ensure_local_agent(db)

    # Write agent config if this is first run (token returned)
    agent_config_path = data_dir / "agent_config.json"
    if token:
        config = load_config()
        host = config.get("host", "127.0.0.1")
        port = config.get("port", 8000)
        agent_port = config.get("agent_port", 8001)

        agent_config = {
            "server_url": f"http://{host}:{port}",
            "token": token,
            "http_host": host,
            "http_port": agent_port,
        }
        agent_config_path.write_text(json.dumps(agent_config, indent=2) + "\n")
        print(f"  Local agent configured (token prefix: {token[:8]})")

    await close_db()

    return agent_config_path.exists()


def main():
    base_dir = Path(__file__).resolve().parent.parent
    has_agent = asyncio.run(_run(base_dir))
    sys.exit(0 if has_agent else 1)


if __name__ == "__main__":
    main()
