"""
database.py — SQLite persistence for Finbot Invoice Bot
========================================================
Tables:
  customers        — Finbot customer records + active status
  name_aliases     — Maps bank transfer names → customer IDs
  processed_txns   — Deduplication + audit trail
  ignore_patterns  — Transaction descriptions to skip
  special_sources  — Check/PayMe patterns requiring special handling
  config           — Key-value bot configuration
"""

import sqlite3, hashlib, json, re, logging
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager
from difflib import SequenceMatcher
from typing import Optional

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
    finbot_id   INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    email       TEXT DEFAULT '',
    tax         TEXT DEFAULT '',
    doc_type    TEXT DEFAULT '2',        -- 0=חמ, 1=קבלה, 2=חמק
    payment_type TEXT DEFAULT '1',       -- 1=העברה
    active      INTEGER DEFAULT 0,       -- 1=expects monthly payment
    ask_check   INTEGER DEFAULT 0,       -- 1=prompt for check details
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS name_aliases (
    alias       TEXT PRIMARY KEY COLLATE NOCASE,
    finbot_id   INTEGER NOT NULL REFERENCES customers(finbot_id),
    confidence  REAL DEFAULT 1.0,
    added_by    TEXT DEFAULT 'manual',   -- manual | auto | import
    created_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS processed_txns (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint     TEXT NOT NULL,           -- sha256(date|amount|payer_norm)
    screenshot_hash TEXT,                    -- sha256 of image bytes
    bank_date       TEXT,
    amount          REAL,
    amount_before_vat REAL,
    payer_name      TEXT,
    finbot_id       INTEGER REFERENCES customers(finbot_id),
    customer_name   TEXT,
    doc_type        TEXT,
    finbot_doc_link TEXT,
    finbot_status   TEXT DEFAULT 'pending',  -- pending | success | error
    month_key       TEXT,                    -- YYYY-MM
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ignore_patterns (
    pattern TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS special_sources (
    pattern     TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,        -- check_ask | payme_ask
    likely_finbot_id INTEGER,
    payment_type TEXT DEFAULT '1',
    message     TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE INDEX IF NOT EXISTS idx_txns_fingerprint ON processed_txns(fingerprint);
CREATE INDEX IF NOT EXISTS idx_txns_month ON processed_txns(month_key);
CREATE INDEX IF NOT EXISTS idx_txns_screenshot ON processed_txns(screenshot_hash);
CREATE INDEX IF NOT EXISTS idx_aliases_finbot ON name_aliases(finbot_id);
"""

DEFAULT_IGNORE = [
    "חיוב מ", "העברה ל", "דמי מנוי", "הפקדה לחיסכון",
    "משיכה מחיסכון", "זיכוי ממע\"מ", "זיכוי ממס הכנסה",
    "חיוב ממס הכנסה", "חיוב ממע\"מ", "מכירת ני\"ע",
    "קניית ני\"ע", "עמלה/הוצאה", "ריבית על", "ישראכרט-דיירקט",
]

DEFAULT_SPECIALS = [
    ("הפקדת צ'ק", "check_ask", 105103, "3", "הפקדת צ'ק זוהתה. האם זה מגרומן ושות'?"),
    ("פאיימי בע\"מ", "payme_ask", 107670, "1", "תשלום payme זוהה. האם זה מאלמקייס?"),
    ("פאיימי", "payme_ask", 107670, "1", "תשלום payme זוהה. האם זה מאלמקייס?"),
]

DEFAULT_CUSTOMERS = [
    (107263, "בוריס ריבקין", "Boris.tikunim@gmail.com", "", "2", "1", 1, 0),
    (105131, "אשכול נגב מערבי", "einav@westnegev.org.il", "501400634", "2", "1", 1, 0),
    (105106, "הורייזן אר אס אר אמ בע\"מ", "cfo@mossadcapital.com", "", "2", "1", 1, 0),
    (105105, "קריאייטיבטי פי אר", "shoham@creativity-value.com", "", "2", "1", 1, 0),
    (105104, "רינג מובייל", "ringmobile77@gmail.com", "", "2", "1", 1, 0),
    (105103, "גרומן ושות'", "sec@groman.co.il", "", "2", "3", 1, 1),
    (105102, "ריפרש מטבחים", "shauli@krs-c.com", "", "2", "1", 1, 0),
    (104596, "צביקה ברגמן", "zvika30@inter.net.il", "", "2", "1", 1, 0),
    (104418, "ימית אפריאט", "top.yamit@gmail.com", "", "2", "1", 1, 0),
    (100512, "גלזר הדרכה בע''מ", "office@mercur-e.com", "514924091", "2", "1", 1, 0),
    (100511, "ל.א מעבדות בע״מ", "noa@fusion-vc.com", "515692440", "2", "1", 1, 0),
    (100508, "קפטן אינווסט בע''מ", "sbashan4@gmail.com", "", "2", "1", 1, 0),
    (99827, "אינווסט 360 בע״מ", "reout@investor360.co.il", "", "2", "1", 1, 0),
]

DEFAULT_ALIASES = [
    ("ריבקין אוקסנה", 107263), ("ריבקין אוקסנה וב", 107263), ("בוריס ריבקין", 107263),
    ("אשכול נגב מערבי", 105131),
    ("הורייזון אר אס א", 105106), ("הורייזן אר אס אר", 105106),
    ("קריאייטיביטי פי", 105105), ("קריאייטיבטי פי אר", 105105),
    ("ענבי שי", 105104), ("רינג מובייל", 105104),
    ("ששון נתנאל", 105102), ("ריפרש מטבחים", 105102),
    ("ברגמן צבי", 104596), ("צביקה ברגמן", 104596),
    ("ימית אפריאט", 104418), ("ימית אפריאט בע", 104418),
    ("גלזר הדרכה", 100512), ("גלזר הדרכה בע\"מ", 100512),
    ("ל.א. מעבדות חדשנ", 100511), ("ל.א מעבדות", 100511),
    ("קפטן אינווסט בע", 100508), ("קפטן אינווסט", 100508),
    ("360 אינווסט", 99827), ("בע\"מ360 אינווסט", 99827), ("אינווסט 360", 99827),
]


class Database:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript(SCHEMA)
            # Seed defaults if empty
            count = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
            if count == 0:
                self._seed_defaults(conn)

    def _seed_defaults(self, conn):
        log.info("Seeding default data...")
        conn.executemany(
            "INSERT OR IGNORE INTO customers (finbot_id,name,email,tax,doc_type,payment_type,active,ask_check) VALUES (?,?,?,?,?,?,?,?)",
            DEFAULT_CUSTOMERS)
        conn.executemany(
            "INSERT OR IGNORE INTO name_aliases (alias,finbot_id,added_by) VALUES (?,?,'seed')",
            DEFAULT_ALIASES)
        conn.executemany(
            "INSERT OR IGNORE INTO ignore_patterns (pattern) VALUES (?)",
            [(p,) for p in DEFAULT_IGNORE])
        conn.executemany(
            "INSERT OR IGNORE INTO special_sources (pattern,source_type,likely_finbot_id,payment_type,message) VALUES (?,?,?,?,?)",
            DEFAULT_SPECIALS)

    # ── Config ──────────────────────────────────────────────────────────
    def get_config(self, key: str, default: str = "") -> str:
        with self._conn() as conn:
            row = conn.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default

    def set_config(self, key: str, value: str):
        with self._conn() as conn:
            conn.execute("INSERT OR REPLACE INTO config (key,value) VALUES (?,?)", (key, value))

    def get_all_config(self) -> dict:
        defaults = {
            "finbot_token": "", "currency": "ILS", "language": "HE",
            "vat_type": "true", "rounding": "true",
        }
        with self._conn() as conn:
            rows = conn.execute("SELECT key,value FROM config").fetchall()
            for r in rows:
                defaults[r["key"]] = r["value"]
        return defaults

    # ── Customers ───────────────────────────────────────────────────────
    def get_customer(self, finbot_id: int) -> Optional[dict]:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM customers WHERE finbot_id=?", (finbot_id,)).fetchone()
            return dict(row) if row else None

    def upsert_customer(self, finbot_id: int, name: str, email: str = "",
                        tax: str = "", doc_type: str = "2", payment_type: str = "1",
                        active: bool = False, ask_check: bool = False):
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO customers (finbot_id,name,email,tax,doc_type,payment_type,active,ask_check)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(finbot_id) DO UPDATE SET
                    name=excluded.name, email=excluded.email, tax=excluded.tax,
                    doc_type=excluded.doc_type, payment_type=excluded.payment_type,
                    active=excluded.active, ask_check=excluded.ask_check
            """, (finbot_id, name, email, tax, doc_type, payment_type, int(active), int(ask_check)))

    def set_active(self, finbot_id: int, active: bool):
        with self._conn() as conn:
            conn.execute("UPDATE customers SET active=? WHERE finbot_id=?", (int(active), finbot_id))

    def set_doc_type(self, finbot_id: int, doc_type: str):
        with self._conn() as conn:
            conn.execute("UPDATE customers SET doc_type=? WHERE finbot_id=?", (doc_type, finbot_id))

    def list_customers(self) -> list[dict]:
        with self._conn() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM customers ORDER BY name").fetchall()]

    def list_active_customers(self) -> list[dict]:
        with self._conn() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM customers WHERE active=1 ORDER BY name").fetchall()]

    # ── Aliases ─────────────────────────────────────────────────────────
    def add_alias(self, alias: str, finbot_id: int, added_by: str = "manual"):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO name_aliases (alias,finbot_id,added_by) VALUES (?,?,?)",
                (alias.strip(), finbot_id, added_by))

    def get_aliases_for(self, finbot_id: int) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute("SELECT alias FROM name_aliases WHERE finbot_id=?", (finbot_id,)).fetchall()
            return [r["alias"] for r in rows]

    # ── Matching ────────────────────────────────────────────────────────
    @staticmethod
    def _norm(s: str) -> str:
        s = s.strip()
        for sfx in ['בע"מ', "בע''מ", "בע״מ", "Ltd", "ltd", "LTD"]:
            s = s.replace(sfx, "")
        return " ".join(s.split()).strip()

    def match_customer(self, bank_name: str) -> Optional[dict]:
        """Find best matching customer for a bank transfer name."""
        norm = self._norm(bank_name)
        with self._conn() as conn:
            # 1) Exact alias match
            row = conn.execute("""
                SELECT c.* FROM name_aliases a
                JOIN customers c ON c.finbot_id = a.finbot_id
                WHERE ? LIKE a.alias || '%' OR a.alias LIKE ? || '%'
                ORDER BY LENGTH(a.alias) DESC LIMIT 1
            """, (norm, norm)).fetchone()
            if row:
                return dict(row)

            # 2) Fuzzy match against all aliases
            aliases = conn.execute("SELECT alias, finbot_id FROM name_aliases").fetchall()

        best, best_score = None, 0
        for a in aliases:
            a_norm = self._norm(a["alias"])
            score = SequenceMatcher(None, norm, a_norm).ratio()
            # Try reversed word order
            words = norm.split()
            if len(words) >= 2:
                score = max(score, SequenceMatcher(None, " ".join(reversed(words)), a_norm).ratio())
            if a_norm in norm or norm in a_norm:
                score = max(score, 0.9)
            if score > best_score:
                best_score = score
                best = a["finbot_id"]

        if best_score >= 0.6 and best:
            return self.get_customer(best)
        return None

    def check_special(self, bank_desc: str) -> Optional[dict]:
        norm = self._norm(bank_desc)
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM special_sources").fetchall()
            for r in rows:
                pat = self._norm(r["pattern"])
                if pat in norm or norm in pat:
                    return dict(r)
        return None

    def should_ignore(self, bank_desc: str) -> bool:
        with self._conn() as conn:
            rows = conn.execute("SELECT pattern FROM ignore_patterns").fetchall()
            for r in rows:
                if r["pattern"] in bank_desc:
                    return True
        return False

    # ── Deduplication ───────────────────────────────────────────────────
    @staticmethod
    def make_fingerprint(date: str, amount: float, payer: str) -> str:
        """Create a unique fingerprint for a transaction."""
        norm_payer = Database._norm(payer).lower()
        raw = f"{date}|{amount:.2f}|{norm_payer}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    @staticmethod
    def make_screenshot_hash(image_bytes: bytes) -> str:
        return hashlib.sha256(image_bytes).hexdigest()[:16]

    def is_screenshot_processed(self, screenshot_hash: str) -> bool:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id FROM processed_txns WHERE screenshot_hash=? LIMIT 1",
                (screenshot_hash,)).fetchone()
            return row is not None

    def is_txn_duplicate(self, fingerprint: str) -> Optional[dict]:
        """Check if this transaction was already processed. Returns the existing record or None."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM processed_txns WHERE fingerprint=? AND finbot_status='success'",
                (fingerprint,)).fetchone()
            return dict(row) if row else None

    def find_similar_txn(self, date: str, amount: float, finbot_id: int) -> Optional[dict]:
        """Check if a similar transaction exists this month (same customer, same amount)."""
        parts = date.split("/")
        if len(parts) == 3:
            month_key = f"{parts[2]}-{parts[1]}"
        else:
            return None
        with self._conn() as conn:
            row = conn.execute("""
                SELECT * FROM processed_txns
                WHERE month_key=? AND finbot_id=? AND ABS(amount - ?) < 1.0
                AND finbot_status='success'
                ORDER BY created_at DESC LIMIT 1
            """, (month_key, finbot_id, amount)).fetchone()
            return dict(row) if row else None

    def record_transaction(self, fingerprint: str, screenshot_hash: str,
                           bank_date: str, amount: float, amount_before_vat: float,
                           payer_name: str, finbot_id: int, customer_name: str,
                           doc_type: str, finbot_doc_link: str, finbot_status: str):
        parts = bank_date.split("/")
        month_key = f"{parts[2]}-{parts[1]}" if len(parts) == 3 else datetime.now().strftime("%Y-%m")
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO processed_txns
                (fingerprint, screenshot_hash, bank_date, amount, amount_before_vat,
                 payer_name, finbot_id, customer_name, doc_type, finbot_doc_link,
                 finbot_status, month_key)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (fingerprint, screenshot_hash, bank_date, amount, amount_before_vat,
                  payer_name, finbot_id, customer_name, doc_type, finbot_doc_link,
                  finbot_status, month_key))

    # ── Payment tracking ────────────────────────────────────────────────
    def get_unpaid_active(self, month_key: str) -> list[dict]:
        """Active customers with no successful transaction this month."""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT c.* FROM customers c
                WHERE c.active = 1
                AND c.finbot_id NOT IN (
                    SELECT DISTINCT finbot_id FROM processed_txns
                    WHERE month_key = ? AND finbot_status = 'success'
                )
                ORDER BY c.name
            """, (month_key,)).fetchall()
            return [dict(r) for r in rows]

    def get_month_payments(self, month_key: str) -> list[dict]:
        """All successful transactions for a month."""
        with self._conn() as conn:
            rows = conn.execute("""
                SELECT pt.*, c.name as cust_name FROM processed_txns pt
                LEFT JOIN customers c ON c.finbot_id = pt.finbot_id
                WHERE pt.month_key = ? AND pt.finbot_status = 'success'
                ORDER BY pt.bank_date
            """, (month_key,)).fetchall()
            return [dict(r) for r in rows]

    # ── Excel import ────────────────────────────────────────────────────
    def import_from_excel(self, file_bytes: bytes) -> tuple[int, int]:
        """Import customers from Finbot Excel export. Returns (added, updated)."""
        import zipfile, xml.etree.ElementTree as ET
        from io import BytesIO

        ns = {'ns': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
        added = updated = 0

        with zipfile.ZipFile(BytesIO(file_bytes)) as z:
            tree = ET.parse(z.open('xl/worksheets/sheet1.xml'))
            root = tree.getroot()
            for row in root.findall('.//ns:row', ns)[1:]:
                cells = {}
                for cell in row.findall('ns:c', ns):
                    ref = cell.get('r', '')
                    col = re.match(r'([A-Z]+)', ref)
                    if not col:
                        continue
                    col = col.group(1)
                    val = None
                    ve = cell.find('ns:v', ns)
                    if ve is not None:
                        val = ve.text
                    ie = cell.find('ns:is', ns)
                    if ie is not None:
                        te = ie.find('ns:t', ns)
                        if te is not None:
                            val = te.text
                    if val and val != 'None':
                        cells[col] = val

                name = cells.get('C', '').strip()
                serial = cells.get('B', '')
                if not name or name == 'לקוחות מזדמנים':
                    continue
                try:
                    fid = int(float(serial))
                except (ValueError, TypeError):
                    continue

                email = cells.get('F', '')
                if email == 'None':
                    email = ''
                tax = cells.get('L', '')
                if tax == 'None':
                    tax = ''

                existing = self.get_customer(fid)
                if existing:
                    updated += 1
                else:
                    added += 1

                self.upsert_customer(fid, name, email, tax)
                self.add_alias(name, fid, "import")
                self.add_alias(self._norm(name), fid, "import")

        return added, updated
