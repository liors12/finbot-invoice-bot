# -*- coding: utf-8 -*-
"""
demands.py — מעקב אחרי דרישות תשלום פתוחות בפינבוט וקישורן לחשבוניות.

רקע: ל-Finbot אין API לקריאה. השדה linkedDocument מקבל את המספר הסידורי
הפנימי של המסמך (מ.ס.), לא את מספר המסמך. קישור חשבונית מס קבלה לדרישת
תשלום סוגר את הדרישה אוטומטית בפינבוט.

המספר הסידורי מופיע בעמודת "מ.ס." במסך הכנסות → מסמכי הכנסות,
וגם בתחילת טוקן ה-auth שחוזר בתשובת ה-API (כל הספרות פרט ל-14 האחרונות).
"""

import sqlite3
import re
from datetime import datetime
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS payment_demands (
    serial        INTEGER PRIMARY KEY,
    doc_number    TEXT NOT NULL,
    customer_name TEXT NOT NULL,
    finbot_id     INTEGER,
    amount        REAL NOT NULL,
    doc_date      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open',
    linked_doc    TEXT,
    linked_at     TEXT,
    note          TEXT
);
CREATE INDEX IF NOT EXISTS idx_demands_open ON payment_demands(status, finbot_id);
"""

_db_path = None


def init(db_path):
    """נקרא פעם אחת בעליית הבוט."""
    global _db_path
    _db_path = str(db_path)
    with _conn() as c:
        c.executescript(SCHEMA)


def _conn():
    conn = sqlite3.connect(_db_path, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def serial_from_doc_link(link: str):
    """מחלץ את המספר הסידורי מקישור המסמך שחוזר מ-Finbot.
    הטוקן בנוי: [מספר סידורי][10 ספרות חותמת זמן][4 ספרות] = 14 ספרות אחרונות.
    דוגמה: .../show_doc_app.php?auth/6887017894841970455 -> 68870
    """
    if not link:
        return None
    m = re.search(r'auth/(\d{15,})', link)
    if not m:
        return None
    token = m.group(1)
    if len(token) <= 14:
        return None
    try:
        return int(token[:-14])
    except ValueError:
        return None


def doc_number_from_message(msg: str):
    """מחלץ את מספר המסמך מהודעת ההצלחה של Finbot."""
    if not msg:
        return ""
    m = re.search(r"מספר המסמך:\s*(\d+)", msg)
    return m.group(1) if m else ""


def add(serial, doc_number, customer_name, finbot_id, amount, doc_date, note=None):
    """הוספה/עדכון של דרישה פתוחה. בטוח להרצה חוזרת."""
    with _conn() as c:
        c.execute("""
            INSERT INTO payment_demands
                (serial, doc_number, customer_name, finbot_id, amount, doc_date, status, note)
            VALUES (?, ?, ?, ?, ?, ?, 'open', ?)
            ON CONFLICT(serial) DO UPDATE SET
                doc_number=excluded.doc_number,
                customer_name=excluded.customer_name,
                finbot_id=COALESCE(excluded.finbot_id, payment_demands.finbot_id),
                amount=excluded.amount,
                doc_date=excluded.doc_date,
                note=excluded.note
        """, (serial, doc_number, customer_name, finbot_id, amount, doc_date, note))


def find_open(finbot_id, customer_name):
    """הדרישה הפתוחה הישנה ביותר ללקוח. מתאים לפי מזהה, ואם אין — לפי שם."""
    if not _db_path:
        return None
    with _conn() as c:
        row = None
        if finbot_id:
            row = c.execute("""
                SELECT * FROM payment_demands
                WHERE status='open' AND finbot_id=?
                ORDER BY substr(doc_date,7,4) || substr(doc_date,4,2) || substr(doc_date,1,2)
                LIMIT 1
            """, (finbot_id,)).fetchone()
        if not row and customer_name:
            row = c.execute("""
                SELECT * FROM payment_demands
                WHERE status='open' AND customer_name=?
                ORDER BY substr(doc_date,7,4) || substr(doc_date,4,2) || substr(doc_date,1,2)
                LIMIT 1
            """, (customer_name,)).fetchone()
        return dict(row) if row else None


def mark_linked(serial, linked_doc):
    with _conn() as c:
        c.execute("""
            UPDATE payment_demands
            SET status='linked', linked_doc=?, linked_at=?
            WHERE serial=?
        """, (linked_doc, datetime.now().isoformat(timespec='seconds'), serial))


def list_open():
    if not _db_path:
        return []
    with _conn() as c:
        rows = c.execute("""
            SELECT * FROM payment_demands WHERE status='open'
            ORDER BY substr(doc_date,7,4) || substr(doc_date,4,2) || substr(doc_date,1,2)
        """).fetchall()
        return [dict(r) for r in rows]
