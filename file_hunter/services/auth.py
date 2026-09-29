import hashlib
import hmac
import os
import secrets
from file_hunter.helpers import utc_now


def hash_password(password: str) -> str:
    """Return 'salt_hex:hash_hex' using PBKDF2-HMAC-SHA256."""
    salt = os.urandom(32)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260_000)
    return salt.hex() + ":" + h.hex()


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored 'salt_hex:hash_hex' string."""
    try:
        salt_hex, hash_hex = stored_hash.split(":", 1)
    except ValueError:
        return False
    salt = bytes.fromhex(salt_hex)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 260_000)
    return hmac.compare_digest(h.hex(), hash_hex)


async def user_count(db) -> int:
    cursor = await db.execute("SELECT COUNT(*) FROM users")
    row = await cursor.fetchone()
    return row[0]


async def create_user(db, username: str, password: str, display_name: str = "") -> dict:
    pw_hash = hash_password(password)
    now = utc_now("auto")
    cursor = await db.execute(
        "INSERT INTO users (username, display_name, password_hash, date_created) VALUES (?, ?, ?, ?)",
        (username, display_name, pw_hash, now),
    )
    await db.commit()
    return {
        "id": cursor.lastrowid,
        "username": username,
        "displayName": display_name,
        "dateCreated": now,
    }


async def authenticate(db, username: str, password: str):
    cursor = await db.execute(
        "SELECT id, username, display_name, password_hash FROM users WHERE username = ?",
        (username,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    if not verify_password(password, row["password_hash"]):
        return None
    return {
        "id": row["id"],
        "username": row["username"],
        "displayName": row["display_name"],
    }


async def create_session(db, user_id: int) -> str:
    token = secrets.token_hex(32)
    now = utc_now("auto")
    await db.execute(
        "INSERT INTO sessions (user_id, token, date_created) VALUES (?, ?, ?)",
        (user_id, token, now),
    )
    await db.commit()
    return token


async def validate_session(db, token: str):
    cursor = await db.execute(
        "SELECT s.id, s.user_id, u.username, u.display_name "
        "FROM sessions s JOIN users u ON s.user_id = u.id "
        "WHERE s.token = ?",
        (token,),
    )
    row = await cursor.fetchone()
    if not row:
        return None
    return {
        "id": row["user_id"],
        "username": row["username"],
        "displayName": row["display_name"],
    }


async def delete_session(db, token: str):
    await db.execute("DELETE FROM sessions WHERE token = ?", (token,))
    await db.commit()


async def get_users(db) -> list:
    cursor = await db.execute(
        "SELECT id, username, display_name, date_created FROM users ORDER BY id"
    )
    rows = await cursor.fetchall()
    return [
        {
            "id": row["id"],
            "username": row["username"],
            "displayName": row["display_name"],
            "dateCreated": row["date_created"],
        }
        for row in rows
    ]


async def update_user(
    db,
    user_id: int,
    username: str = None,
    password: str = None,
    display_name: str = None,
):
    fields = []
    values = []
    if username is not None:
        fields.append("username = ?")
        values.append(username)
    if display_name is not None:
        fields.append("display_name = ?")
        values.append(display_name)
    if password is not None:
        fields.append("password_hash = ?")
        values.append(hash_password(password))
    if not fields:
        return
    values.append(user_id)
    await db.execute(f"UPDATE users SET {', '.join(fields)} WHERE id = ?", values)
    await db.commit()


async def delete_user(db, user_id: int):
    await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
    await db.commit()
