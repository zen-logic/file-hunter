import secrets
from datetime import datetime, timezone


async def get_applications(db) -> list:
    cursor = await db.execute(
        "SELECT id, name, token, date_created FROM applications ORDER BY id"
    )
    rows = await cursor.fetchall()
    return [
        {
            "id": row["id"],
            "name": row["name"],
            "token": row["token"],
            "dateCreated": row["date_created"],
        }
        for row in rows
    ]


async def create_application(db, name: str) -> dict:
    token = secrets.token_hex(32)
    now = datetime.now(timezone.utc).isoformat()
    cursor = await db.execute(
        "INSERT INTO applications (name, token, date_created) VALUES (?, ?, ?)",
        (name, token, now),
    )
    await db.commit()
    return {
        "id": cursor.lastrowid,
        "name": name,
        "token": token,
        "dateCreated": now,
    }


async def regenerate_token(db, app_id: int) -> str:
    token = secrets.token_hex(32)
    await db.execute(
        "UPDATE applications SET token = ? WHERE id = ?",
        (token, app_id),
    )
    await db.commit()
    return token


async def delete_application(db, app_id: int):
    await db.execute("DELETE FROM applications WHERE id = ?", (app_id,))
    await db.commit()


async def validate_app_token(db, token: str):
    cursor = await db.execute(
        "SELECT id, name FROM applications WHERE token = ?",
        (token,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "isApp": True,
    }
