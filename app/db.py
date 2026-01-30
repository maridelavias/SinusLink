import json
import os
import aiosqlite
from typing import Tuple, List, Dict, Any, Optional

# Use /data for persistent volume on Northflank, fallback to current directory
_data_dir = os.getenv("DATA_DIR", ".")
DB_PATH = os.getenv("DB_PATH", os.path.join(_data_dir, "bot.db"))

DRAFT_PK_COL = "dentist_tg_id"
CONS_PK_COL  = "dentist_tg_id"


async def init_db():
    global DRAFT_PK_COL, CONS_PK_COL

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS dentists (
                tg_id       INTEGER PRIMARY KEY,
                full_name   TEXT,
                phone       TEXT,
                workplace   TEXT,
                tg_username TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS consultations (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                dentist_tg_id INTEGER,
                status        TEXT,
                created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS consultations_draft (
                dentist_tg_id INTEGER PRIMARY KEY,
                complaints    TEXT,
                history       TEXT,
                plan          TEXT,
                attachments   TEXT
            )
        """)
        await db.commit()

        cons_cols  = await _table_columns(db, "consultations")
        draft_cols = await _table_columns(db, "consultations_draft")

        CONS_PK_COL  = "dentist_tg_id" if "dentist_tg_id" in cons_cols else (
                       "dentist_id"    if "dentist_id"    in cons_cols else "dentist_tg_id")
        DRAFT_PK_COL = "dentist_tg_id" if "dentist_tg_id" in draft_cols else (
                       "dentist_id"    if "dentist_id"    in draft_cols else "dentist_tg_id")

        await _ensure_columns(db, "consultations_draft", {
            "complaints":  "TEXT",
            "history":     "TEXT",
            "plan":        "TEXT",
            "attachments": "TEXT",
        })
        await _ensure_columns(db, "dentists", {
            "full_name":   "TEXT",
            "phone":       "TEXT",
            "workplace":   "TEXT",
            "tg_username": "TEXT",
        })

        await db.commit()


async def _table_columns(db: aiosqlite.Connection, table: str) -> List[str]:
    cur = await db.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    await cur.close()
    return [r[1] for r in rows]

async def _ensure_columns(db: aiosqlite.Connection, table: str, expected: Dict[str, str]):
    have = set(await _table_columns(db, table))
    for name, typ in expected.items():
        if name not in have:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {typ}")


async def upsert_dentist(
    tg_id: int,
    full_name: Optional[str] = None,
    phone: Optional[str] = None,
    workplace: Optional[str] = None,
    tg_username: Optional[str] = None,
):
    row = await _fetchone("SELECT * FROM dentists WHERE tg_id = ?", (tg_id,))
    row = dict(row) if row else {}
    if full_name   is not None: row["full_name"]   = full_name
    if phone       is not None: row["phone"]       = phone
    if workplace   is not None: row["workplace"]   = workplace
    if tg_username is not None: row["tg_username"] = tg_username

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO dentists (tg_id, full_name, phone, workplace, tg_username)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tg_id) DO UPDATE SET
                full_name  = excluded.full_name,
                phone      = excluded.phone,
                workplace  = excluded.workplace,
                tg_username= excluded.tg_username
            """,
            (tg_id, row.get("full_name"), row.get("phone"),
                   row.get("workplace"), row.get("tg_username")),
        )
        await db.commit()

async def get_dentist_by_tg_id(tg_id: int) -> Dict[str, Any]:
    row = await _fetchone("SELECT * FROM dentists WHERE tg_id = ?", (tg_id,))
    return dict(row) if row else {
        "tg_id": tg_id, "full_name": None, "phone": None, "workplace": None, "tg_username": None
    }


async def save_draft(dentist_tg_id: int, consult: Dict[str, Any], attachments: List[Dict[str, Any]]):
    complaints = consult.get("patient_complaints")
    history    = consult.get("patient_history")
    plan       = consult.get("planned_work")
    attachments_json = json.dumps(attachments, ensure_ascii=False)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"""
            INSERT INTO consultations_draft ({DRAFT_PK_COL}, complaints, history, plan, attachments)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT({DRAFT_PK_COL}) DO UPDATE SET
                complaints = excluded.complaints,
                history    = excluded.history,
                plan       = excluded.plan,
                attachments= excluded.attachments
            """,
            (dentist_tg_id, complaints, history, plan, attachments_json),
        )
        await db.commit()

async def load_draft(dentist_tg_id: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    row = await _fetchone(
        f"SELECT complaints, history, plan, attachments FROM consultations_draft WHERE {DRAFT_PK_COL} = ?",
        (dentist_tg_id,),
    )
    if not row:
        return {}, []
    consult = {
        "patient_complaints": row["complaints"],
        "patient_history":    row["history"],
        "planned_work":       row["plan"],
    }
    atts = json.loads(row["attachments"] or "[]")
    return consult, atts

async def clear_draft(dentist_tg_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"DELETE FROM consultations_draft WHERE {DRAFT_PK_COL} = ?", (dentist_tg_id,))
        await db.commit()


async def insert_consultation_log(dentist_tg_id: int, status: str = "sent"):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT INTO consultations ({CONS_PK_COL}, status) VALUES (?, ?)",
            (dentist_tg_id, status),
        )
        await db.commit()

async def list_consultations_by_dentist(dentist_tg_id: int) -> List[Dict[str, Any]]:
    rows = await _fetchall(
        f"SELECT id, status, created_at FROM consultations WHERE {CONS_PK_COL} = ? ORDER BY id DESC",
        (dentist_tg_id,),
    )
    return [dict(r) for r in rows]

async def get_consultation_by_id(consult_id: int) -> Optional[Dict[str, Any]]:
    row = await _fetchone(
        f"SELECT id, {CONS_PK_COL} AS dentist_tg_id, status, created_at FROM consultations WHERE id = ?",
        (consult_id,),
    )
    return dict(row) if row else None

get_consultation = get_consultation_by_id


async def _fetchone(query: str, params: tuple = ()) -> Optional[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row

async def _fetchall(query: str, params: tuple = ()) -> List[aiosqlite.Row]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows
