"""Agent registry — CRUD and token management."""

import secrets
from datetime import datetime, timezone

from file_hunter.services.auth import hash_password


async def create_agent(db, name: str) -> dict:
    """Create a new agent with a generated token. Returns the token once."""
    token = secrets.token_hex(32)
    token_hash = hash_password(token)
    token_prefix = token[:8]
    now = datetime.now(timezone.utc).isoformat()

    cursor = await db.execute(
        "INSERT INTO agents (name, token_hash, token_prefix, date_created) VALUES (?, ?, ?, ?)",
        (name, token_hash, token_prefix, now),
    )
    await db.commit()
    return {"id": cursor.lastrowid, "name": name, "token": token}


async def ensure_local_agent(db) -> str | None:
    """Ensure a local agent exists. Returns the token if newly created, else None.

    On first run, creates a 'Local Agent' entry and returns the pairing token.
    On subsequent runs, returns None (agent already exists).
    """
    cursor = await db.execute("SELECT id FROM agents WHERE name = 'Local Agent'")
    if await cursor.fetchone():
        return None

    result = await create_agent(db, "Local Agent")
    return result["token"]


async def list_agents(db) -> list:
    """Return all agents (without token hashes)."""
    cursor = await db.execute(
        "SELECT id, name, token_prefix, hostname, os, status, date_created, date_last_seen "
        "FROM agents ORDER BY id"
    )
    rows = await cursor.fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "tokenPrefix": row["token_prefix"],
            "hostname": row["hostname"] or "",
            "os": row["os"] or "",
            "status": row["status"],
            "dateCreated": row["date_created"],
            "dateLastSeen": row["date_last_seen"],
        }
        for row in rows
    ]


async def update_agent(db, agent_id: int, name: str):
    """Update agent name."""
    await db.execute("UPDATE agents SET name = ? WHERE id = ?", (name, agent_id))
    await db.commit()


async def delete_agent(db, agent_id: int):
    """Delete an agent."""
    await db.execute("DELETE FROM agents WHERE id = ?", (agent_id,))
    await db.commit()


async def regenerate_token(db, agent_id: int) -> dict | None:
    """Generate a new token for an agent. Returns the token once, or None."""
    cursor = await db.execute("SELECT id FROM agents WHERE id = ?", (agent_id,))
    if not await cursor.fetchone():
        return None

    token = secrets.token_hex(32)
    token_hash = hash_password(token)
    token_prefix = token[:8]

    await db.execute(
        "UPDATE agents SET token_hash = ?, token_prefix = ? WHERE id = ?",
        (token_hash, token_prefix, agent_id),
    )
    await db.commit()
    return {"id": agent_id, "token": token}
