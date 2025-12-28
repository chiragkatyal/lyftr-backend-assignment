from datetime import datetime
import sqlite3
from app.models import get_db

def insert_message(msg):
    db = get_db()
    try:
        db.execute(
            """INSERT INTO messages VALUES (?,?,?,?,?,?)""",
            (
                msg["message_id"],
                msg["from"],
                msg["to"],
                msg["ts"],
                msg.get("text"),
                datetime.utcnow().isoformat() + "Z",
            ),
        )
        db.commit()
        return "created"
    except sqlite3.IntegrityError:
        return "duplicate"

def query_messages(limit, offset, from_msisdn, since, q):
    db = get_db()
    where = []
    params = []

    if from_msisdn:
        where.append("from_msisdn=?")
        params.append(from_msisdn)
    if since:
        where.append("ts>=?")
        params.append(since)
    if q:
        where.append("LOWER(text) LIKE ?")
        params.append(f"%{q.lower()}%")

    where_sql = "WHERE " + " AND ".join(where) if where else ""

    total = db.execute(
        f"SELECT COUNT(*) FROM messages {where_sql}", params
    ).fetchone()[0]

    rows = db.execute(
        f"""
        SELECT message_id, from_msisdn as "from", to_msisdn as "to", ts, text
        FROM messages
        {where_sql}
        ORDER BY ts ASC, message_id ASC
        LIMIT ? OFFSET ?
        """,
        params + [limit, offset],
    ).fetchall()

    return total, rows

def stats():
    db = get_db()
    total = db.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
    senders = db.execute(
        """SELECT from_msisdn as "from", COUNT(*) as count
           FROM messages GROUP BY from_msisdn
           ORDER BY count DESC LIMIT 10"""
    ).fetchall()
    first = db.execute("SELECT MIN(ts) FROM messages").fetchone()[0]
    last = db.execute("SELECT MAX(ts) FROM messages").fetchone()[0]

    return total, senders, first, last
