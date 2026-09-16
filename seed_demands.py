#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
טעינת דרישות התשלום הפתוחות של ספטמבר 2026 לטבלת payment_demands.
הרצה חד-פעמית:  cd /home/finbot-invoice-bot && venv/bin/python seed_demands.py
בטוח להרצה חוזרת.

המספר הסידורי (מ.ס.) נלקח ממסך: הכנסות → מסמכי הכנסות → עמודת מ.ס.
"""
import sys, sqlite3
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import demands as dem

DB = Path("./data/finbot.db")

# מ.ס. | מספר מסמך | שם הלקוח בפינבוט | סכום | תאריך
DEMANDS = [
    (67624, "40092", "אינווסט 360 בע״מ",                             7080.00, "01/09/2026"),
    (67626, "40093", "קפטן אינווסט בע''מ",                            3540.00, "01/09/2026"),
    (67628, "40094", "גרומן ושות'",                                   5900.00, "01/09/2026"),
    (67629, "40095", "רינג מובייל",                                   5900.00, "01/09/2026"),
    (67639, "40096", "אור פוריה סוכנות לביטוח פנסיוני (2022) בע׳׳מ", 4720.00, "01/09/2026"),
    (67758, "40097", "השראה ופיתוח נדל׳׳ן בע׳׳מ",                    2950.00, "02/09/2026"),
    (67759, "40098", "שלוש לאבס בע׳׳מ",                               2950.00, "02/09/2026"),
    (68100, "40100", "יד לבנים",                                      3245.00, "06/09/2026"),
    (68112, "40101", "ששון אלומיניום",                                3540.00, "06/09/2026"),
    (68586, "40103", "גלזר הדרכה בע''מ",                              9912.00, "10/09/2026"),
    (68631, "40104", "Sreedhar Peddineni - GTM BUDDY, INC.,",         2720.70, "11/09/2026"),
    (68852, "40105", "אלמקייס פתרונות מיסוי",                         7670.00, "15/09/2026"),
]

dem.init(DB)

conn = sqlite3.connect(str(DB))
conn.row_factory = sqlite3.Row
customers = conn.execute("SELECT finbot_id, name FROM customers").fetchall()

def norm(s):
    for ch in ['״', '׳', '"', "'", '`', '.', ',', '-', '(', ')']:
        s = s.replace(ch, '')
    s = ''.join(s.split())
    return s.replace('בעמ', '').replace('בע', '').lower()

cust_map = {norm(c['name']): c['finbot_id'] for c in customers}

# שמות שנכתבים אחרת בפינבוט מאשר אצלנו
ALIASES = {
    norm("אור פוריה סוכנות לביטוח פנסיוני (2022) בע׳׳מ"): norm("אור פוריה סוכנות"),
    norm("השראה ופיתוח נדל׳׳ן בע׳׳מ"):                     norm("השראה פיתוח נדל"),
    norm("שלוש לאבס בע׳׳מ"):                               norm('שלוש לאבס בע"מ'),
    norm("Sreedhar Peddineni - GTM BUDDY, INC.,"):          norm("Sreedhar Peddineni - GTM BUDDY, INC.,"),
}

matched = unmatched = 0
for serial, doc, name, amount, date in DEMANDS:
    k = ALIASES.get(norm(name), norm(name))
    fid = cust_map.get(k)
    if fid:
        matched += 1
    else:
        unmatched += 1
    dem.add(serial, doc, name, fid, amount, date)

print(f"נטענו {len(DEMANDS)} דרישות — {matched} שויכו ללקוח במסד, {unmatched} לפי שם בלבד\n")
for d in dem.list_open():
    fid = d['finbot_id'] or '—'
    print(f"  {d['serial']} | {d['doc_number']} | {d['customer_name'][:34]:<34} | "
          f"{d['amount']:>9.2f} | לקוח: {fid}")
conn.close()
