# -*- coding: utf-8 -*-
"""
Finbot Invoice Automation — Telegram Bot v2
=============================================
Rebuilt with SQLite, deduplication, validation layer.

Flow:
  1. Daily reminder (2nd–21st) asks for bank screenshot
  2. Send screenshot → Gemini Vision extracts + confidence scores
  3. Dedup check → Skip already-processed transactions
  4. Auto-match to Finbot customers
  5. Validation warnings (suspicious amounts, low confidence)
  6. Review + approve → Issue via Finbot API
  7. Missing email warnings + unpaid tracking (from 15th)
  8. Screenshot deleted after approval

Commands:
  /start, /token, /settings, /customers, /active
  /activate <id>, /deactivate <id>, /alias <name> <id>
  /unpaid [YYYY-MM], /cancel
"""

import os, json, re, base64, logging, hashlib, functools
from pathlib import Path
from datetime import datetime, time as dtime
from typing import Optional
from zoneinfo import ZoneInfo

import httpx
import google.generativeai as genai
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, ContextTypes, filters,
)

from database import Database

# ── Config ──────────────────────────────────────────────────────────────────

TELEGRAM_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
GEMINI_KEYS    = [k.strip() for k in os.environ.get("GEMINI_API_KEY", "").split(",") if k.strip()]
OWNER_CHAT_ID  = int(os.environ.get("OWNER_CHAT_ID", "0"))
FINBOT_TOKEN_ENV = os.environ.get("FINBOT_TOKEN", "")  # fallback from .env
DB_PATH        = Path(os.environ.get("DATA_DIR", "./data")) / "finbot.db"

_gemini_key_index = 0
def get_next_gemini_key() -> str:
    """Rotate through available Gemini API keys."""
    global _gemini_key_index
    if not GEMINI_KEYS:
        raise ValueError("No Gemini API keys configured")
    key = GEMINI_KEYS[_gemini_key_index % len(GEMINI_KEYS)]
    _gemini_key_index += 1
    return key

FINBOT_URL     = "https://api.finbotai.co.il/income"
VAT_RATE       = 1.18

def get_finbot_token() -> str:
    """Get Finbot token from .env (primary) or DB config (fallback)."""
    if FINBOT_TOKEN_ENV:
        return FINBOT_TOKEN_ENV
    return db.get_config("finbot_token", "")

# ── Access Control ──────────────────────────────────────────────────────────

def owner_only(func):
    """Decorator: reject all interactions from non-owner users."""
    @functools.wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if OWNER_CHAT_ID and user and user.id != OWNER_CHAT_ID:
            log.warning(f"Unauthorized access attempt from user {user.id}")
            if update.message:
                await update.message.reply_text("⛔ גישה מוגבלת — בוט פרטי.")
            elif update.callback_query:
                await update.callback_query.answer("⛔ גישה מוגבלת", show_alert=True)
            return
        return await func(update, ctx)
    return wrapper

# מספר הקצאה thresholds (amount BEFORE VAT)
# Jan 2026: 10,000₪  |  Jun 2026: 5,000₪
ALLOCATION_THRESHOLD = 10_000
ALLOCATION_THRESHOLD_JUN = 5_000

TZ             = ZoneInfo("Asia/Jerusalem")
REMINDER_HOUR  = 9
REMINDER_DAYS  = range(2, 22)

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

# Ensure data dir exists
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
db = Database(DB_PATH)

# ── Gemini Vision ───────────────────────────────────────────────────────────

VISION_PROMPT = """You are analyzing a screenshot from the One Zero (וואן זירו) Israeli bank app.

Extract ALL visible transactions. For each, provide:
- date: in DD/MM/YYYY format
- description: the full text shown (sender name or transaction description)
- amount: the number shown (positive for credit, negative for debit)
- is_incoming: true if money coming IN (shown in green/positive), false if outgoing
- confidence: your confidence (0-100) that you read the amount and name correctly

Respond ONLY with a JSON array. No markdown, no backticks, no explanation.
Example: [{"date":"15/03/2026","description":"העברה מישראל כהן","amount":5000,"is_incoming":true,"confidence":95}]
If no transactions visible: []
If not a bank screen: {"error":"not a bank statement"}"""


async def parse_screenshot(img_bytes: bytes) -> list[dict]:
    import asyncio

    image_part = {
        "inline_data": {
            "mime_type": "image/jpeg",
            "data": base64.b64encode(img_bytes).decode(),
        }
    }

    GEMINI_TIMEOUT = 60  # seconds

    # Try each key until one works (handles rate limits and timeouts)
    last_error = None
    for attempt in range(len(GEMINI_KEYS)):
        key = get_next_gemini_key()
        genai.configure(api_key=key)
        model = genai.GenerativeModel("gemini-2.5-flash")

        def _call():
            response = model.generate_content(
                [image_part, VISION_PROMPT],
                generation_config=genai.types.GenerationConfig(
                    temperature=0.1,
                    max_output_tokens=4000,
                ),
            )
            return response.text

        try:
            txt = await asyncio.wait_for(asyncio.to_thread(_call), timeout=GEMINI_TIMEOUT)
            break
        except asyncio.TimeoutError:
            last_error = TimeoutError(f"Gemini did not respond within {GEMINI_TIMEOUT}s")
            log.warning(f"Gemini timeout with key #{attempt+1}, trying next key...")
            continue
        except Exception as e:
            last_error = e
            if "429" in str(e) or "Resource" in str(e):
                log.warning(f"Gemini key rate limited, trying next key...")
                continue
            raise
    else:
        raise ValueError(f"All {len(GEMINI_KEYS)} Gemini keys failed. Last error: {last_error}")
    txt = txt.strip()
    txt = re.sub(r'^```\w*\n?', '', txt)
    txt = re.sub(r'\n?```$', '', txt)
    parsed = json.loads(txt.strip())
    if isinstance(parsed, dict) and "error" in parsed:
        raise ValueError(parsed["error"])
    return parsed if isinstance(parsed, list) else []


# ── Finbot API ──────────────────────────────────────────────────────────────

async def issue_document(finbot_token: str, customer_id: int, customer_name: str,
                         customer_email: str, customer_tax: str,
                         amount: float, date: str, doc_type: str, payment_type: str,
                         cfg: dict, check_details: dict = None) -> dict:
    if not finbot_token:
        return {"status": 0, "message": "טוקן פינבוט חסר — שלח /token או הוסף FINBOT_TOKEN ל-.env"}

    pre_vat = round(amount / VAT_RATE, 2)
    vat_amount = round(pre_vat * 0.18, 2)
    payment_sum = pre_vat + vat_amount  # Guaranteed to match items + VAT
    lang = cfg.get("language", "HE").upper()  # Finbot requires uppercase HE/EN
    cust = {"name": customer_name, "save": False}
    if customer_email:
        cust["email"] = customer_email
    if customer_tax:
        cust["tax"] = customer_tax
    body = {
        "type": doc_type, "date": date,
        "language": lang,
        "currency": cfg.get("currency", "ILS"),
        "vatType": cfg.get("vat_type", "true") == "true",
        "rounding": cfg.get("rounding", "true") == "true",
        "customer": cust,
        "items": [{"name": "תשלום", "amount": 1, "price": pre_vat}],
    }
    if doc_type in ("1", "2"):
        payment = {"type": payment_type, "date": date, "sum": payment_sum}
        if payment_type == "3" and check_details:
            payment.update(check_details)
        body["payments"] = [payment]

    log.info(f"Finbot API call: customer={customer_name}, email={'yes' if customer_email else 'no'}, amount={amount}, doc_type={doc_type}, lang={lang}")
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(FINBOT_URL, json=body,
                         headers={"Content-Type": "application/json", "secret": finbot_token})

    # Handle non-JSON and error responses
    if r.status_code == 401:
        log.error(f"Finbot 401 — invalid token")
        return {"status": 0, "message": "טוקן פינבוט לא תקין (401). עדכן עם /token"}
    try:
        result = r.json()
        # Finbot returns list of errors on failure, dict on success
        if isinstance(result, list):
            errors = ", ".join(e.get("message", str(e)) for e in result)
            log.warning(f"Finbot errors: {errors}")
            return {"status": 0, "message": errors}
        if result.get("status") != 1:
            log.warning(f"Finbot error: {result}")
        return result
    except Exception:
        log.error(f"Finbot response not JSON. Status: {r.status_code}, Body: {r.text[:200]}")
        return {"status": 0, "message": f"שגיאת פינבוט (HTTP {r.status_code})"}


# ── Session state ───────────────────────────────────────────────────────────

sessions = {}

def get_session(chat_id: int) -> dict:
    if chat_id not in sessions:
        sessions[chat_id] = {
            "phase": "idle",          # idle | collecting | reviewing | check_details | unknown_customer | issuing
            "transactions": [],
            "screenshot_msg_ids": [],  # list of message IDs to delete later
            "screenshot_hashes": [],   # list of hashes for dedup
            "status_msg_ids": [],      # collecting-phase result messages to clean up
            "pending_idx": None,
        }
    return sessions[chat_id]

def clear_session(chat_id: int):
    sessions.pop(chat_id, None)

async def cleanup_old_status_msgs(chat, sess: dict, keep_msg_id: int = None):
    """Delete old collecting-phase status messages, optionally keeping one."""
    for mid in sess.get("status_msg_ids", []):
        if mid != keep_msg_id:
            try:
                await chat.delete_message(mid)
            except:
                pass
    sess["status_msg_ids"] = []

# ── Formatting ──────────────────────────────────────────────────────────────

DOC_LABELS = {"0": "חשבונית מס", "1": "קבלה", "2": "חשבונית מס קבלה"}
PAY_LABELS = {"0": "מזומן", "1": "העברה", "2": "אשראי", "3": "צ'ק", "7": "אחר", "8": "ביט", "9": "פייבוקס"}

def fmt(amount: float) -> str:
    return f"₪{amount:,.2f}"

def esc(text: str) -> str:
    """Escape Markdown special chars in user-generated text."""
    for ch in ('*', '_', '`', '['):
        text = text.replace(ch, '')
    return text

def review_text(txns: list[dict]) -> str:
    if not txns:
        return "לא נמצאו העברות."
    lines = ["📋 *כל ההעברות שזוהו מכל הצילומים:*\n"]
    total = 0
    for i, t in enumerate(txns):
        total += t["amount"]
        flags = []

        if t.get("match") == "matched":
            flags.append(f"✅ {esc(t['customer_name'])}")
        elif t.get("match") == "special_check":
            flags.append("⚠️ צ'ק — צריך פרטים")
        elif t.get("match") == "special_ask":
            flags.append(f"❓ {esc(t.get('special_msg', ''))}")
        elif t.get("match") == "unknown":
            flags.append("🆕 לקוח לא מוכר")
        elif t.get("match") == "duplicate":
            flags.append("🔁 *כפילות*")
        elif t.get("match") == "similar":
            flags.append("⚠️ *עסקה דומה קיימת*")

        if t.get("confidence", 100) < 85:
            flags.append(f"🔍 {t.get('confidence', '?')}%")

        pre_vat = t["amount"] / VAT_RATE
        now = datetime.now(TZ)
        threshold = ALLOCATION_THRESHOLD_JUN if now.month >= 6 and now.year >= 2026 else ALLOCATION_THRESHOLD
        if pre_vat >= threshold:
            flags.append("📋 הקצאה")

        doc_label = DOC_LABELS.get(t.get("doc_type", "2"), "")
        status = " ".join(flags)
        lines.append(
            f"*{i+1}.* {esc(t['bank_desc'])}\n"
            f"💰 {fmt(t['amount'])} 📅 {t['date']} 📄 {doc_label}\n"
            f"{status}"
        )
        lines.append("")

    lines.append(f"*סה\"כ: {fmt(total)}* ({len(txns)} העברות)")
    return "\n".join(lines)


def review_keyboard(txns: list[dict]) -> InlineKeyboardMarkup:
    """Build inline keyboard for the review flow."""
    rows = []
    # Per-transaction delete buttons — rows of 5
    del_btns = [InlineKeyboardButton(f"🗑{i+1}", callback_data=f"del_{i}") for i in range(len(txns))]
    for j in range(0, len(del_btns), 5):
        rows.append(del_btns[j:j+5])
    # Per-transaction doc type buttons — show current type, rows of 4
    SHORT_DOC = {"0": "חמ", "1": "קבלה", "2": "חמק"}
    type_btns = [InlineKeyboardButton(
        f"📄{i+1}:{SHORT_DOC.get(txns[i].get('doc_type','2'),'חמק')}",
        callback_data=f"dtype_{i}") for i in range(len(txns))]
    for j in range(0, len(type_btns), 4):
        rows.append(type_btns[j:j+4])
    # 🆕 buttons for unknown/unresolved customers only
    new_btns = [InlineKeyboardButton(f"🆕{i+1}", callback_data=f"newcust_{i}")
                for i in range(len(txns))
                if txns[i].get("match") in ("unknown", "special_ask", "similar")]
    if new_btns:
        for j in range(0, len(new_btns), 5):
            rows.append(new_btns[j:j+5])
    # Main actions
    rows.append([
        InlineKeyboardButton("📋 פרטים מלאים", callback_data="action_check"),
        InlineKeyboardButton("✅ שלח חשבוניות", callback_data="action_approve"),
    ])
    rows.append([
        InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
    ])
    return InlineKeyboardMarkup(rows)

# ── Command handlers ────────────────────────────────────────────────────────

@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    uid = update.effective_user.id
    await update.message.reply_text(
        "🤖 *Finbot Invoice Bot v2*\n\n"
        "בוט אוטומטי להפקת חשבוניות וקבלות מצילומי מסך של One Zero\\.\n\n"
        "*הגדרה:*\n"
        "1\\. `/token YOUR_TOKEN`\n"
        "2\\. שלח צילום מסך של העברות\n"
        "3\\. בדוק, תקן, `אישור`\n\n"
        f"🆔 Chat ID: `{cid}`\n👤 User ID: `{uid}`",
        parse_mode="MarkdownV2")

@owner_only
async def cmd_token(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("שימוש: `/token YOUR_FINBOT_TOKEN`", parse_mode="Markdown")
        return
    db.set_config("finbot_token", ctx.args[0])
    try:
        await update.message.delete()
    except:
        pass
    await update.effective_chat.send_message("✅ טוקן נשמר! (ההודעה נמחקה)\n\nשלח צילום מסך של העברות.")

@owner_only
async def cmd_settings(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = db.get_all_config()
    tok = "✅" if get_finbot_token() else "❌"
    vat = "כולל" if cfg.get("vat_type", "true") == "true" else "לא כולל"
    kb = [[
        InlineKeyboardButton(f"מטבע: {cfg.get('currency','ILS')}", callback_data="tog_currency"),
        InlineKeyboardButton(f"מע\"מ: {vat}", callback_data="tog_vat"),
    ], [
        InlineKeyboardButton(f"שפה: {'עב' if cfg.get('language','he')=='he' else 'EN'}", callback_data="tog_lang"),
        InlineKeyboardButton(f"עיגול: {'כן' if cfg.get('rounding','true')=='true' else 'לא'}", callback_data="tog_round"),
    ]]
    await update.message.reply_text(
        f"⚙️ *הגדרות*\n\n🔑 טוקן: {tok}\n💱 {cfg.get('currency','ILS')}  🏷 {vat} מע\"מ\n"
        f"🌐 {'עברית' if cfg.get('language','he')=='he' else 'English'}  🔄 עיגול: {'כן' if cfg.get('rounding','true')=='true' else 'לא'}\n\n"
        f"📋 סף מספר הקצאה: {fmt(ALLOCATION_THRESHOLD)} לפני מע\"מ",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

@owner_only
async def cmd_customers(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    custs = db.list_customers()
    if not custs:
        await update.message.reply_text("אין לקוחות. שלח קובץ Excel מפינבוט.")
        return
    lines = [f"👥 *{len(custs)} לקוחות:*\n"]
    for c in custs:
        icon = "🟢" if c["active"] else "⚪"
        email_icon = "📧" if c["email"] else "📭"
        lines.append(f"{icon}{email_icon} {c['name']} (ID: {c['finbot_id']})")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

@owner_only
async def cmd_active(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    active = [c for c in db.list_customers() if c["active"]]
    inactive = [c for c in db.list_customers() if not c["active"]]
    lines = [f"🟢 *פעילים ({len(active)}):*\n"]
    for c in active:
        e = "📧" if c["email"] else "📭"
        lines.append(f"  {e} {c['name']} (ID: {c['finbot_id']})")
    if inactive:
        lines.append(f"\n⚪ *לא פעילים ({len(inactive)}):*")
        for c in inactive:
            lines.append(f"  • {c['name']} (ID: {c['finbot_id']})")
    lines.append(f"\n`/activate <id>` · `/deactivate <id>`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

@owner_only
async def cmd_activate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("שימוש: `/activate <מספר_סידורי>`", parse_mode="Markdown")
        return
    try:
        fid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ שלח מספר סידורי בלבד.")
        return
    cust = db.get_customer(fid)
    if not cust:
        await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return
    db.set_active(fid, True)
    await update.message.reply_text(f"✅ {cust['name']} — *פעיל*", parse_mode="Markdown")

@owner_only
async def cmd_deactivate(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("שימוש: `/deactivate <מספר_סידורי>`", parse_mode="Markdown")
        return
    try:
        fid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ שלח מספר סידורי בלבד.")
        return
    cust = db.get_customer(fid)
    if not cust:
        await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return
    db.set_active(fid, False)
    await update.message.reply_text(f"🔕 {cust['name']} — *לא פעיל*", parse_mode="Markdown")

@owner_only
async def cmd_alias(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if len(ctx.args) < 2:
        await update.message.reply_text("שימוש: `/alias שם_בבנק מספר_סידורי`", parse_mode="Markdown")
        return
    try:
        fid = int(ctx.args[-1])
    except ValueError:
        await update.message.reply_text("⚠️ המספר הסידורי חייב להיות מספר.")
        return
    alias = " ".join(ctx.args[:-1])
    cust = db.get_customer(fid)
    if not cust:
        await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return
    db.add_alias(alias, fid)
    await update.message.reply_text(f"✅ '{alias}' → {cust['name']} (ID {fid})")

@owner_only
async def cmd_unpaid(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(TZ)
    month_key = ctx.args[0] if ctx.args else now.strftime("%Y-%m")
    unpaid = db.get_unpaid_active(month_key)
    if not unpaid:
        await update.message.reply_text(f"✅ כל הלקוחות הפעילים שילמו ב-{month_key}!")
        return
    lines = [f"⏳ *לא שילמו — {month_key}:*\n"]
    for c in unpaid:
        lines.append(f"  • {c['name']} (ID: {c['finbot_id']})")
    paid = db.get_month_payments(month_key)
    if paid:
        lines.append(f"\n✅ *שילמו ({len(paid)}):*")
        for p in paid:
            lines.append(f"  • {p.get('cust_name', '?')} — {fmt(p['amount'])}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

@owner_only
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *פקודות:*\n\n"
        "📸 שלח צילום מסך — זיהוי העברות\n"
        "`/cancel` — ביטול תהליך\n"
        "`/receipt` — הצגת סוגי מסמכים\n"
        "`/receipt <ID>` — קבלה בלבד\n"
        "`/invoice <ID>` — חשבונית מס קבלה\n"
        "`/customers` — רשימת לקוחות\n"
        "`/active` — לקוחות פעילים\n"
        "`/unpaid` — מי לא שילם\n"
        "`/settings` — הגדרות\n"
        "`/token <TOKEN>` — עדכון טוקן פינבוט\n"
        "`/alias <שם> <ID>` — שיוך שם בנקאי\n\n"
        "*בזמן עריכת עסקאות (טקסט):*\n"
        "`מחק 2` — מחיקת שורה\n"
        "`3 לקוח 105103` — שיוך ללקוח\n"
        "`3 סכום 5000` — תיקון סכום\n"
        "`3 סוג קבלה` — שינוי סוג מסמך\n"
        "`3 צק` — תשלום בצ'ק\n"
        "`3 חדש` — לקוח חדש",
        parse_mode="Markdown")

@owner_only
async def cmd_cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    clear_session(update.effective_chat.id)
    await update.message.reply_text("❌ בוטל.")

@owner_only
async def cmd_receipt(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Set customer to receipt-only mode (already has חשבונית מס)."""
    if not ctx.args:
        # Show all receipt-only customers
        custs = db.list_customers()
        receipt_only = [c for c in custs if c["doc_type"] == "1"]
        invoice_receipt = [c for c in custs if c["doc_type"] == "2"]
        lines = ["📄 *מצב מסמכים לפי לקוח:*\n"]
        if receipt_only:
            lines.append("🧾 *קבלה בלבד* (כבר יש חשבונית מס):")
            for c in receipt_only:
                lines.append(f"  • {c['name']} (ID: {c['finbot_id']})")
            lines.append("")
        if invoice_receipt:
            lines.append("📋 *חשבונית מס קבלה* (ברירת מחדל):")
            for c in invoice_receipt:
                lines.append(f"  • {c['name']} (ID: {c['finbot_id']})")
        lines.append("")
        lines.append("לשנות לקבלה בלבד: `/receipt <ID>`")
        lines.append("להחזיר לחשבונית מס קבלה: `/invoice <ID>`")
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return
    try:
        fid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ שלח מספר סידורי בלבד.")
        return
    cust = db.get_customer(fid)
    if not cust:
        await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return
    db.set_doc_type(fid, "1")
    await update.message.reply_text(
        f"🧾 {cust['name']} — *קבלה בלבד*\n"
        f"(כי כבר הוצאת חשבונית מס בנפרד)",
        parse_mode="Markdown")

@owner_only
async def cmd_invoice(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Set customer back to invoice+receipt mode."""
    if not ctx.args:
        await update.message.reply_text("שימוש: `/invoice <ID>`", parse_mode="Markdown")
        return
    try:
        fid = int(ctx.args[0])
    except ValueError:
        await update.message.reply_text("⚠️ שלח מספר סידורי בלבד.")
        return
    cust = db.get_customer(fid)
    if not cust:
        await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return
    db.set_doc_type(fid, "2")
    await update.message.reply_text(
        f"📋 {cust['name']} — *חשבונית מס קבלה*",
        parse_mode="Markdown")

# ── Callback handler ────────────────────────────────────────────────────────

@owner_only
async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    chat_id = update.effective_chat.id
    action = q.data

    # ── Action buttons (review flow) ──
    if action == "action_check":
        await q.answer()
        sess = get_session(chat_id)
        await cleanup_old_status_msgs(update.effective_chat, sess, keep_msg_id=q.message.message_id)
        if sess["phase"] == "collecting":
            # Move to reviewing first, then show check
            sess["phase"] = "reviewing"
        if sess["transactions"]:
            cfg = db.get_all_config()
            txns = [t for t in sess["transactions"] if t["match"] not in ("duplicate",)]
            if not txns:
                await q.edit_message_text("אין העברות להפקה.")
                return
            lines = ["🔍 *מצב בדיקה — לא נשלח כלום!*\n"]
            for i, txn in enumerate(txns):
                pre_vat = round(txn["amount"] / VAT_RATE, 2)
                cust = db.get_customer(txn["customer_id"]) if txn["customer_id"] else None
                email_status = f"📧 {cust['email']}" if cust and cust.get("email") else "📭 *אין מייל — לא יישלח!*"
                doc_label = DOC_LABELS.get(txn["doc_type"], txn["doc_type"])
                pay_label = PAY_LABELS.get(txn["payment_type"], txn["payment_type"])
                lines.append(f"*── עסקה {i+1} ──*")
                lines.append(f"🏦 {esc(txn.get('bank_desc', '?'))}")
                cust_display = txn.get('customer_name', '')
                if cust_display:
                    lines.append(f"👤 לקוח: {esc(cust_display)} (ID: {txn.get('customer_id', '?')})")
                else:
                    lines.append(f"👤 לקוח: *לא מוכר*")
                lines.append(f"💰 סכום כולל מע\"מ: {fmt(txn['amount'])}")
                lines.append(f"💰 סכום לפני מע\"מ: {fmt(pre_vat)}")
                lines.append(f"📄 סוג מסמך: {doc_label}")
                lines.append(f"💳 אמצעי תשלום: {pay_label}")
                lines.append(f"📅 תאריך: {txn['date']}")
                lines.append(f"{email_status}")
                now = datetime.now(TZ)
                threshold = ALLOCATION_THRESHOLD_JUN if now.month >= 6 and now.year >= 2026 else ALLOCATION_THRESHOLD
                if pre_vat >= threshold:
                    lines.append(f"📋 *מספר הקצאה יידרש* (מעל {fmt(threshold)})")
                lines.append("")
            lines.append(f"*סה\"כ: {len(txns)} מסמכים*")
            kb = [[
                InlineKeyboardButton("✅ שלח חשבוניות", callback_data="action_approve"),
            ], [
                InlineKeyboardButton("✏️ עריכה", callback_data="action_summary"),
                InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
            ]]
            await q.edit_message_text("\n".join(lines), parse_mode="Markdown",
                                      reply_markup=InlineKeyboardMarkup(kb))
        return

    if action == "action_summary":
        await q.answer()
        sess = get_session(chat_id)
        await cleanup_old_status_msgs(update.effective_chat, sess, keep_msg_id=q.message.message_id)
        if sess["phase"] in ("collecting", "reviewing") and sess["transactions"]:
            sess["phase"] = "reviewing"
            await q.edit_message_text(review_text(sess["transactions"]), parse_mode="Markdown",
                                      reply_markup=review_keyboard(sess["transactions"]))
        return

    if action == "action_approve":
        await q.answer()
        sess = get_session(chat_id)
        log.info(f"action_approve: phase={sess['phase']}, txns={len(sess.get('transactions', []))}")
        await cleanup_old_status_msgs(update.effective_chat, sess, keep_msg_id=q.message.message_id)
        if sess["phase"] in ("reviewing", "collecting"):
            if sess["phase"] == "collecting":
                sess["phase"] = "reviewing"
            txns = sess["transactions"]
            to_issue = [t for t in txns if t["match"] not in ("duplicate",)]
            unresolved = [i for i, t in enumerate(to_issue)
                          if t["match"] in ("unknown", "special_check", "special_ask", "similar")]
            if unresolved:
                # Build descriptive list + action buttons for each unresolved item
                lines = ["⚠️ *פריטים לא מוכנים:*\n"]
                kb = []
                for i in unresolved:
                    t = to_issue[i]
                    match_type = t.get("match", "")
                    desc = esc(t.get("bank_desc", ""))
                    if match_type == "special_check":
                        lines.append(f"*{i+1}.* {desc} — צ'ק, צריך פרטים")
                        kb.append([
                            InlineKeyboardButton(f"✅ אשר {i+1}", callback_data=f"confirm_{i}"),
                            InlineKeyboardButton(f"🗑 מחק {i+1}", callback_data=f"del_{i}"),
                        ])
                    elif match_type == "unknown":
                        lines.append(f"*{i+1}.* {desc} — לקוח לא מוכר")
                        kb.append([
                            InlineKeyboardButton(f"🆕 שייך {i+1}", callback_data=f"newcust_{i}"),
                            InlineKeyboardButton(f"🗑 מחק {i+1}", callback_data=f"del_{i}"),
                        ])
                    elif match_type in ("special_ask", "similar"):
                        lines.append(f"*{i+1}.* {desc} — {esc(t.get('special_msg', 'צריך אישור'))}")
                        kb.append([
                            InlineKeyboardButton(f"✅ אשר {i+1}", callback_data=f"confirm_{i}"),
                            InlineKeyboardButton(f"🗑 מחק {i+1}", callback_data=f"del_{i}"),
                        ])
                kb.append([
                    InlineKeyboardButton("✏️ עריכה", callback_data="action_summary"),
                    InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
                ])
                try:
                    await q.edit_message_text(
                        "\n".join(lines),
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup(kb))
                except Exception as e:
                    log.error(f"Failed to edit message: {e}")
                    await update.effective_chat.send_message(
                        "\n".join(lines))
                return
            try:
                await _do_issue(update, sess)
            except Exception as e:
                log.exception(f"_do_issue failed: {e}")
                await update.effective_chat.send_message(f"❌ שגיאה בהפקה: {e}")
                clear_session(chat_id)
        else:
            log.warning(f"action_approve: unexpected phase '{sess['phase']}'")
            await update.effective_chat.send_message("⚠️ אין עסקאות פעילות. שלח צילום מסך חדש.")
        return

    if action == "action_cancel":
        await q.answer()
        clear_session(chat_id)
        await q.edit_message_text("❌ בוטל.")
        return

    if action == "action_noop":
        await q.answer("💡 השתמש בפקודות טקסט לעריכה", show_alert=False)
        return

    # ── Per-transaction delete buttons ──
    if action.startswith("del_"):
        await q.answer()
        idx = int(action.split("_")[1])
        sess = get_session(chat_id)
        txns = sess.get("transactions", [])
        if 0 <= idx < len(txns):
            removed = txns.pop(idx)
            if not txns:
                clear_session(chat_id)
                await q.edit_message_text("🗑 הכל נמחק. שלח צילום חדש.")
            else:
                await q.edit_message_text(
                    review_text(txns), parse_mode="Markdown",
                    reply_markup=review_keyboard(txns))
        else:
            await q.answer("⚠️ שורה לא קיימת", show_alert=True)
        return

    # ── Per-transaction doc type: show options ──
    if action.startswith("dtype_") and "_" not in action[6:]:
        await q.answer()
        idx = int(action.split("_")[1])
        sess = get_session(chat_id)
        txns = sess.get("transactions", [])
        if 0 <= idx < len(txns):
            kb = [[
                InlineKeyboardButton("🧾 קבלה", callback_data=f"settype_{idx}_1"),
                InlineKeyboardButton("📋 חמק", callback_data=f"settype_{idx}_2"),
                InlineKeyboardButton("📄 חמ", callback_data=f"settype_{idx}_0"),
            ], [
                InlineKeyboardButton("↩️ חזור", callback_data="action_summary"),
            ]]
            t = txns[idx]
            await q.edit_message_text(
                f"📄 *שינוי סוג מסמך לשורה {idx+1}:*\n"
                f"{t['bank_desc']} — {fmt(t['amount'])}",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(kb))
        return

    # ── Per-transaction doc type: set ──
    if action.startswith("settype_"):
        await q.answer()
        parts = action.split("_")
        idx = int(parts[1])
        doc_type = parts[2]
        sess = get_session(chat_id)
        txns = sess.get("transactions", [])
        if 0 <= idx < len(txns):
            txns[idx]["doc_type"] = doc_type
            sess["phase"] = "reviewing"
            await q.edit_message_text(
                review_text(txns), parse_mode="Markdown",
                reply_markup=review_keyboard(txns))
        return

    # ── Per-transaction: assign new customer ──
    if action.startswith("newcust_"):
        await q.answer()
        idx = int(action.split("_")[1])
        sess = get_session(chat_id)
        txns = sess.get("transactions", [])
        if 0 <= idx < len(txns):
            txn = txns[idx]
            sess["phase"] = "unknown_customer"
            sess["pending_idx"] = idx
            await q.edit_message_text(
                f"🆕 *שיוך לקוח לשורה {idx+1}:*\n"
                f"{esc(txn['bank_desc'])} — {fmt(txn['amount'])}\n\n"
                f"שלח מספר סידורי של הלקוח מפינבוט.\n"
                f"לחזרה שלח `דלג`",
                parse_mode="Markdown")
        else:
            await q.answer("⚠️ שורה לא קיימת", show_alert=True)
        return

    # ── Per-transaction: confirm (special_check, special_ask, similar) ──
    if action.startswith("confirm_"):
        await q.answer()
        idx = int(action.split("_")[1])
        sess = get_session(chat_id)
        txns = sess.get("transactions", [])
        if 0 <= idx < len(txns):
            txn = txns[idx]
            if txn["match"] == "special_check" and txn.get("customer_id"):
                # Ask for check details
                sess["phase"] = "check_details"
                sess["pending_idx"] = idx
                await q.edit_message_text(
                    f"📝 *פרטי צ'ק עבור {esc(txn['customer_name'])}:*\n"
                    f"`בנק,סניף,חשבון,מספר_צק`\n\n"
                    f"לא חובה — לדלג שלח `דלג`",
                    parse_mode="Markdown")
            elif txn["match"] in ("special_ask", "similar") and txn.get("customer_id"):
                txn["match"] = "matched"
                sess["phase"] = "reviewing"
                await q.edit_message_text(
                    review_text(txns), parse_mode="Markdown",
                    reply_markup=review_keyboard(txns))
            else:
                await q.answer("⚠️ לא ניתן לאשר — חסר לקוח", show_alert=True)
        else:
            await q.answer("⚠️ שורה לא קיימת", show_alert=True)
        return

    # ── Settings toggle buttons ──
    cfg = db.get_all_config()
    if action == "tog_currency":
        opts = ["ILS", "USD", "EUR"]
        cur = cfg.get("currency", "ILS")
        db.set_config("currency", opts[(opts.index(cur) + 1) % len(opts)])
    elif action == "tog_vat":
        db.set_config("vat_type", "false" if cfg.get("vat_type", "true") == "true" else "true")
    elif action == "tog_lang":
        db.set_config("language", "en" if cfg.get("language", "he") == "he" else "he")
    elif action == "tog_round":
        db.set_config("rounding", "false" if cfg.get("rounding", "true") == "true" else "true")
    else:
        await q.answer()
        return
    await q.answer("✅")
    # Refresh settings display
    cfg = db.get_all_config()
    vat = "כולל" if cfg.get("vat_type") == "true" else "לא כולל"
    tok = "✅" if get_finbot_token() else "❌"
    kb = [[
        InlineKeyboardButton(f"מטבע: {cfg['currency']}", callback_data="tog_currency"),
        InlineKeyboardButton(f"מע\"מ: {vat}", callback_data="tog_vat"),
    ], [
        InlineKeyboardButton(f"שפה: {'עב' if cfg['language']=='he' else 'EN'}", callback_data="tog_lang"),
        InlineKeyboardButton(f"עיגול: {'כן' if cfg['rounding']=='true' else 'לא'}", callback_data="tog_round"),
    ]]
    await q.edit_message_text(
        f"⚙️ *הגדרות*\n\n🔑 טוקן: {tok}\n💱 {cfg['currency']}  🏷 {vat} מע\"מ\n"
        f"🌐 {'עברית' if cfg['language']=='he' else 'English'}  🔄 עיגול: {'כן' if cfg['rounding']=='true' else 'לא'}",
        parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(kb))

# ── Document (Excel) handler ────────────────────────────────────────────────

@owner_only
async def handle_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    doc = update.message.document
    if not doc.file_name.endswith(('.xlsx', '.xls')):
        return
    status = await update.message.reply_text("⏳ מייבא לקוחות...")
    try:
        f = await doc.get_file()
        data = await f.download_as_bytearray()
        added, updated = db.import_from_excel(bytes(data))
        total = len(db.list_customers())
        await status.edit_text(f"✅ ייבוא הושלם!\n🆕 {added} חדשים  🔄 {updated} עודכנו\n👥 סה\"כ: {total}")
    except Exception as e:
        log.exception("Excel import failed")
        await status.edit_text(f"❌ שגיאה: {e}")

# ── Photo handler — bank screenshot ────────────────────────────────────────

@owner_only
async def handle_photo(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cfg = db.get_all_config()
    chat_id = update.effective_chat.id

    if not get_finbot_token():
        await update.message.reply_text("⚠️ `/token YOUR_TOKEN` קודם", parse_mode="Markdown")
        return

    sess = get_session(chat_id)

    # Only accept photos in idle or collecting phase
    if sess["phase"] not in ("idle", "collecting"):
        kb = [[
            InlineKeyboardButton("📋 פרטים מלאים", callback_data="action_check"),
            InlineKeyboardButton("✅ שלח חשבוניות", callback_data="action_approve"),
        ], [
            InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
        ]]
        await update.message.reply_text(
            "📸 *קיבלתי את צילומי המסך שלך ויש עסקאות שמחכות לטיפול.*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(kb))
        return

    sess["screenshot_msg_ids"].append(update.message.message_id)
    status = await update.message.reply_text("⏳ מנתח צילום מסך...")

    try:
        photo = update.message.photo[-1]
        pf = await photo.get_file()
        img = bytes(await pf.download_as_bytearray())

        # ── Screenshot-level dedup ──
        ss_hash = db.make_screenshot_hash(img)
        is_reupload = db.is_screenshot_processed(ss_hash)
        sess["screenshot_hashes"].append(ss_hash)

        await status.edit_text("🔍 מזהה העברות...")
        raw = await parse_screenshot(img)

        if not raw:
            await status.edit_text("😕 לא זוהו העברות בצילום הזה.")
            return

        new_txns = []
        for r in raw:
            desc = r.get("description", "")
            amount = float(r.get("amount", 0))
            is_in = r.get("is_incoming", amount > 0)
            confidence = r.get("confidence", 90)

            if not is_in or amount <= 0:
                continue
            if db.should_ignore(desc):
                continue

            clean = re.sub(r'^העברה מ', '', desc).strip()

            txn = {
                "date": r.get("date", ""),
                "bank_desc": desc,
                "clean_name": clean,
                "amount": abs(amount),
                "confidence": confidence,
                "customer_id": None,
                "customer_name": "",
                "doc_type": "2",
                "payment_type": "1",
                "check_details": None,
                "match": "unknown",
                "special_msg": "",
                "fingerprint": db.make_fingerprint(r.get("date", ""), abs(amount), clean),
            }

            # ── Transaction-level dedup (against DB) ──
            dup = db.is_txn_duplicate(txn["fingerprint"])
            if dup:
                txn["match"] = "duplicate"
                txn["customer_name"] = dup.get("customer_name", "")
                txn["customer_id"] = dup.get("finbot_id")
                new_txns.append(txn)
                continue

            # ── Dedup against already-collected transactions in this session ──
            existing_fps = [t["fingerprint"] for t in sess["transactions"]]
            if txn["fingerprint"] in existing_fps:
                continue  # skip silently, already in list

            # ── Special sources ──
            special = db.check_special(desc)
            if special:
                if special["source_type"] == "check_ask":
                    txn["match"] = "special_check"
                    txn["special_msg"] = special["message"]
                    if special.get("likely_finbot_id"):
                        cust = db.get_customer(special["likely_finbot_id"])
                        if cust:
                            txn["customer_id"] = cust["finbot_id"]
                            txn["customer_name"] = cust["name"]
                            txn["payment_type"] = "3"
                            txn["doc_type"] = cust["doc_type"]
                            # Already issued this month? Auto-filter
                            similar = db.find_similar_txn(txn["date"], txn["amount"], cust["finbot_id"])
                            if similar:
                                txn["match"] = "duplicate"
                elif special["source_type"] == "payme_ask":
                    txn["match"] = "special_ask"
                    txn["special_msg"] = special["message"]
                    txn["payment_type"] = special.get("payment_type", "1")
                    if special.get("likely_finbot_id"):
                        cust = db.get_customer(special["likely_finbot_id"])
                        if cust:
                            txn["customer_id"] = cust["finbot_id"]
                            txn["customer_name"] = cust["name"]
                            txn["doc_type"] = cust["doc_type"]
                            # Already issued this month? Auto-filter
                            similar = db.find_similar_txn(txn["date"], txn["amount"], cust["finbot_id"])
                            if similar:
                                txn["match"] = "duplicate"
            else:
                # ── Normal match ──
                cust = db.match_customer(clean)
                if cust:
                    txn["match"] = "matched"
                    txn["customer_id"] = cust["finbot_id"]
                    txn["customer_name"] = cust["name"]
                    txn["doc_type"] = cust["doc_type"]
                    txn["payment_type"] = cust["payment_type"]

                    # ── Already issued this month? Auto-filter ──
                    similar = db.find_similar_txn(txn["date"], txn["amount"], cust["finbot_id"])
                    if similar:
                        txn["match"] = "duplicate"

            new_txns.append(txn)

        # Filter: count non-duplicate new transactions
        new_non_dup = [t for t in new_txns if t["match"] != "duplicate"]
        if not new_txns:
            await status.edit_text("😕 לא נמצאו העברות חדשות בצילום הזה.")
            return
        if not new_non_dup and is_reupload:
            await status.edit_text(
                "⚠️ כל ההעברות בצילום הזה כבר הונפקו בהצלחה.\n"
                "שלח צילום מסך חדש עם העברות שלא טופלו.")
            return

        sess["transactions"].extend(new_txns)
        sess["phase"] = "collecting"

        total_txns = len([t for t in sess["transactions"] if t["match"] != "duplicate"])
        total_screenshots = len(sess["screenshot_hashes"])
        dup_count = len(new_txns) - len(new_non_dup)

        # Build full detail view directly
        txns_display = [t for t in sess["transactions"] if t["match"] != "duplicate"]
        cfg = db.get_all_config()
        dup_note = f"🔁 {dup_count} כפולות סוננו\n" if dup_count else ""
        lines = [f"📋 *כל ההעברות שזוהו מכל הצילומים:*\n{dup_note}"]
        for i, txn in enumerate(txns_display):
            pre_vat = round(txn["amount"] / VAT_RATE, 2)
            cust = db.get_customer(txn["customer_id"]) if txn["customer_id"] else None
            email_status = f"📧 {cust['email']}" if cust and cust.get("email") else "📭 *אין מייל — לא יישלח!*"
            doc_label = DOC_LABELS.get(txn["doc_type"], txn["doc_type"])
            pay_label = PAY_LABELS.get(txn["payment_type"], txn["payment_type"])
            lines.append(f"*── עסקה {i+1} ──*")
            lines.append(f"🏦 {esc(txn.get('bank_desc', '?'))}")
            cust_display = txn.get('customer_name', '')
            if cust_display:
                lines.append(f"👤 לקוח: {esc(cust_display)} (ID: {txn.get('customer_id', '?')})")
            else:
                lines.append(f"👤 לקוח: *לא מוכר*")
            lines.append(f"💰 סכום כולל מע\"מ: {fmt(txn['amount'])}")
            lines.append(f"💰 סכום לפני מע\"מ: {fmt(pre_vat)}")
            lines.append(f"📄 סוג מסמך: {doc_label}")
            lines.append(f"💳 אמצעי תשלום: {pay_label}")
            lines.append(f"📅 תאריך: {txn['date']}")
            lines.append(f"{email_status}")
            now_check = datetime.now(TZ)
            threshold = ALLOCATION_THRESHOLD_JUN if now_check.month >= 6 and now_check.year >= 2026 else ALLOCATION_THRESHOLD
            if pre_vat >= threshold:
                lines.append(f"📋 *מספר הקצאה יידרש* (מעל {fmt(threshold)})")
            lines.append("")
        lines.append(f"*סה\"כ: {len(txns_display)} מסמכים*")

        kb = [[
            InlineKeyboardButton("✅ שלח חשבוניות", callback_data="action_approve"),
        ], [
            InlineKeyboardButton("✏️ עריכה", callback_data="action_summary"),
            InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
        ]]
        await status.edit_text("\n".join(lines), parse_mode="Markdown",
                               reply_markup=InlineKeyboardMarkup(kb))
        sess["status_msg_ids"].append(status.message_id)

    except ValueError as e:
        await status.edit_text(f"❌ {e}")
    except json.JSONDecodeError:
        await status.edit_text("❌ שגיאת פרסור. נסה צילום ברור יותר.")
    except Exception as e:
        log.exception("Screenshot error")
        await status.edit_text(f"❌ שגיאה: {e}")

# ── Text handler — corrections & approval ───────────────────────────────────

@owner_only
async def handle_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    sess = get_session(chat_id)
    text = update.message.text.strip()

    # ── Check details input ──
    if sess["phase"] == "check_details":
        idx = sess["pending_idx"]
        txn = sess["transactions"][idx]

        # Allow skipping
        if text in ("דלג", "חזור", "ביטול"):
            txn["match"] = "matched"
            txn["check_details"] = None
            sess["phase"] = "reviewing"
            sess["pending_idx"] = None
            await update.message.reply_text(
                f"✅ דילגת על פרטי צ'ק.\n\n" + review_text(sess["transactions"]),
                parse_mode="Markdown", reply_markup=review_keyboard(sess["transactions"]))
            return

        parts = re.split(r'[,\s]+', text)
        if len(parts) >= 4:
            try:
                txn["check_details"] = {
                    "bankName": int(parts[0]), "bankBranch": int(parts[1]),
                    "bankAccount": int(parts[2]), "checkNumber": int(parts[3]),
                }
            except ValueError:
                await update.message.reply_text(
                    "⚠️ כל הערכים חייבים להיות מספרים.\n"
                    "פורמט: `בנק,סניף,חשבון,מספר_צק`\n\n"
                    "לדלג? שלח `דלג`",
                    parse_mode="Markdown")
                return
            txn["match"] = "matched"
            sess["phase"] = "reviewing"
            sess["pending_idx"] = None
            await update.message.reply_text(
                f"✅ פרטי צ'ק נשמרו.\n\n" + review_text(sess["transactions"]),
                parse_mode="Markdown", reply_markup=review_keyboard(sess["transactions"]))
        else:
            await update.message.reply_text(
                "⚠️ פורמט: `בנק,סניף,חשבון,מספר_צק`\n"
                "לדוגמה: `12,345,678901,1234`\n\n"
                "לדלג על פרטי צ'ק? שלח `דלג`",
                parse_mode="Markdown")
        return

    # ── Unknown customer — waiting for Finbot ID ──
    if sess["phase"] == "unknown_customer":
        idx = sess["pending_idx"]
        txn = sess["transactions"][idx]

        # Allow going back
        if text in ("דלג", "חזור", "ביטול"):
            sess["phase"] = "reviewing"
            sess["pending_idx"] = None
            await update.message.reply_text(
                f"↩️ חזרה לבדיקה.\n\n" + review_text(sess["transactions"]),
                parse_mode="Markdown", reply_markup=review_keyboard(sess["transactions"]))
            return

        try:
            fid = int(text.strip())
            # Save alias + create customer if needed
            clean = txn["clean_name"]
            existing = db.get_customer(fid)
            if not existing:
                db.upsert_customer(fid, clean)
            db.add_alias(clean, fid, "auto")
            cust = db.get_customer(fid)
            txn["customer_id"] = fid
            txn["customer_name"] = cust["name"]
            txn["match"] = "matched"
            sess["phase"] = "reviewing"
            sess["pending_idx"] = None
            await update.message.reply_text(
                f"✅ '{clean}' → {cust['name']} (ID {fid}) — נשמר!\n\n" +
                review_text(sess["transactions"]), parse_mode="Markdown", reply_markup=review_keyboard(sess["transactions"]))
        except ValueError:
            await update.message.reply_text("⚠️ שלח מספר סידורי בלבד, או `דלג` לחזרה.", parse_mode="Markdown")
        return

    if sess["phase"] != "reviewing":
        # ── סיכום — move from collecting to reviewing ──
        if sess["phase"] == "collecting" and text == "סיכום":
            if not sess["transactions"]:
                await update.message.reply_text("אין העברות. שלח צילום מסך קודם.")
                return
            sess["phase"] = "reviewing"
            await update.message.reply_text(review_text(sess["transactions"]), parse_mode="Markdown", reply_markup=review_keyboard(sess["transactions"]))
            return
        return

    txns = sess["transactions"]

    # ── אישור ──
    if text == "אישור":
        # Filter out duplicates (don't issue them)
        to_issue = [t for t in txns if t["match"] not in ("duplicate",)]
        unresolved = [i for i, t in enumerate(to_issue)
                      if t["match"] in ("unknown", "special_check", "special_ask", "similar")]
        if unresolved:
            nums = ", ".join(str(i + 1) for i in unresolved)
            await update.message.reply_text(
                f"⚠️ פריטים {nums} לא מוכנים. טפל בהם, מחק (`מחק <#>`), או אשר שאלות (`<#> כן`).")
            return
        await _do_issue(update, sess)
        return

    # ── בדיקה (dry-run) ──
    if text == "בדיקה":
        to_issue = [t for t in txns if t["match"] not in ("duplicate",)]
        if not to_issue:
            await update.message.reply_text("אין העברות להפקה.")
            return

        cfg = db.get_all_config()
        lines = ["🔍 *מצב בדיקה — לא נשלח כלום!*\n"]
        for i, txn in enumerate(to_issue):
            pre_vat = round(txn["amount"] / VAT_RATE, 2)
            cust = db.get_customer(txn["customer_id"]) if txn["customer_id"] else None
            email_status = f"📧 {cust['email']}" if cust and cust.get("email") else "📭 *אין מייל — לא יישלח!*"
            doc_label = DOC_LABELS.get(txn["doc_type"], txn["doc_type"])
            pay_label = PAY_LABELS.get(txn["payment_type"], txn["payment_type"])

            lines.append(f"*── עסקה {i+1} ──*")
            lines.append(f"🏦 {esc(txn.get('bank_desc', '?'))}")
            cust_display = txn.get('customer_name', '')
            if cust_display:
                lines.append(f"👤 לקוח: {esc(cust_display)} (ID: {txn.get('customer_id', '?')})")
            else:
                lines.append(f"👤 לקוח: *לא מוכר*")
            lines.append(f"💰 סכום כולל מע\"מ: {fmt(txn['amount'])}")
            lines.append(f"💰 סכום לפני מע\"מ: {fmt(pre_vat)}")
            lines.append(f"📄 סוג מסמך: {doc_label}")
            lines.append(f"💳 אמצעי תשלום: {pay_label}")
            lines.append(f"📅 תאריך: {txn['date']}")
            lines.append(f"{email_status}")

            # Check allocation threshold
            now = datetime.now(TZ)
            threshold = ALLOCATION_THRESHOLD_JUN if now.month >= 6 and now.year >= 2026 else ALLOCATION_THRESHOLD
            if pre_vat >= threshold:
                lines.append(f"📋 *מספר הקצאה יידרש* (מעל {fmt(threshold)})")
            lines.append("")

        lines.append(f"*סה\"כ: {len(to_issue)} מסמכים*")

        kb = [[
            InlineKeyboardButton("✅ שלח חשבוניות", callback_data="action_approve"),
        ], [
            InlineKeyboardButton("✏️ עריכה", callback_data="action_summary"),
            InlineKeyboardButton("❌ ביטול", callback_data="action_cancel"),
        ]]

        await update.message.reply_text("\n".join(lines), parse_mode="Markdown",
                                        reply_markup=InlineKeyboardMarkup(kb))
        return

    # ── מחק # ──
    m = re.match(r'מחק\s+(\d+)', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            removed = txns.pop(idx)
            # Update fingerprints display
            if not txns:
                clear_session(chat_id)
                await update.message.reply_text("🗑 הכל נמחק. שלח צילום חדש.")
                return
            await update.message.reply_text(f"🗑 נמחק.\n\n" + review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
        return

    # ── # כן (confirm special/similar) ──
    m = re.match(r'(\d+)\s+כן', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            txn = txns[idx]
            if txn["match"] in ("special_ask", "similar") and txn.get("customer_id"):
                txn["match"] = "matched"
            elif txn["match"] == "special_check" and txn.get("customer_id"):
                # Confirmed it's the likely customer, now ask for check details
                sess["phase"] = "check_details"
                sess["pending_idx"] = idx
                await update.message.reply_text(
                    f"📝 פרטי צ'ק עבור {txn['customer_name']}:\n"
                    "`בנק,סניף,חשבון,מספר_צק`\n\n"
                    "לא חובה — לדלג שלח `דלג`", parse_mode="Markdown")
                return
            await update.message.reply_text(review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
        return

    # ── # לקוח <id> ──
    m = re.match(r'(\d+)\s+לקוח\s+(\d+)', text)
    if m:
        idx = int(m.group(1)) - 1
        fid = int(m.group(2))
        if 0 <= idx < len(txns):
            cust = db.get_customer(fid)
            if cust:
                txn = txns[idx]
                txn["customer_id"] = fid
                txn["customer_name"] = cust["name"]
                txn["match"] = "matched"
                db.add_alias(txn["clean_name"], fid, "auto")
                await update.message.reply_text(
                    f"✅ נשמר!\n\n" + review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
            else:
                await update.message.reply_text(f"⚠️ לקוח {fid} לא נמצא.")
        return

    # ── # צק ──
    m = re.match(r'(\d+)\s+צק', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            sess["phase"] = "check_details"
            sess["pending_idx"] = idx
            await update.message.reply_text(
                f"📝 פרטי צ'ק:\n`בנק,סניף,חשבון,מספר_צק`\n\n"
                "לא חובה — לדלג שלח `דלג`", parse_mode="Markdown")
        return

    # ── # חדש ──
    m = re.match(r'(\d+)\s+חדש', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            sess["phase"] = "unknown_customer"
            sess["pending_idx"] = idx
            await update.message.reply_text(
                f"🆕 צור לקוח בפינבוט עבור: *{txns[idx]['bank_desc']}*\nשלח מספר סידורי.",
                parse_mode="Markdown")
        return

    # ── # שם <name> ──
    m = re.match(r'(\d+)\s+שם\s+(.+)', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            txns[idx]["customer_name"] = m.group(2).strip()
            await update.message.reply_text(review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
        return

    # ── # סכום <amount> ──
    m = re.match(r'(\d+)\s+סכום\s+([\d,.]+)', text)
    if m:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(txns):
            txns[idx]["amount"] = float(m.group(2).replace(",", ""))
            # Recalculate fingerprint
            txns[idx]["fingerprint"] = db.make_fingerprint(
                txns[idx]["date"], txns[idx]["amount"], txns[idx]["clean_name"])
            await update.message.reply_text(review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
        return

    # ── # סוג <type> ──
    m = re.match(r'(\d+)\s+סוג\s+(\S+)', text)
    if m:
        idx = int(m.group(1)) - 1
        dtype = m.group(2)
        type_map = {"חמק": "2", "קבלה": "1", "חמ": "0"}
        if dtype in type_map and 0 <= idx < len(txns):
            txns[idx]["doc_type"] = type_map[dtype]
            await update.message.reply_text(review_text(txns), parse_mode="Markdown", reply_markup=review_keyboard(txns))
        return

# ── Issue documents ─────────────────────────────────────────────────────────

async def _do_issue(update: Update, sess: dict):
    cfg = db.get_all_config()
    txns = [t for t in sess["transactions"] if t["match"] not in ("duplicate",)]
    chat = update.effective_chat
    chat_id = chat.id

    if not txns:
        await chat.send_message("אין העברות להפקה (הכל כפילויות).")
        clear_session(chat_id)
        return

    sess["phase"] = "issuing"
    status = await chat.send_message(f"⏳ מפיק {len(txns)} מסמכים...")

    no_email = []
    lines = ["📊 *תוצאות:*\n"]
    ok = err = 0

    for txn in txns:
        # Look up customer email and tax from DB
        cust = db.get_customer(txn["customer_id"]) if txn.get("customer_id") else None
        cust_email = (cust or {}).get("email", "")
        cust_tax = (cust or {}).get("tax", "")
        try:
            res = await issue_document(
                get_finbot_token(), txn["customer_id"], txn["customer_name"],
                cust_email, cust_tax,
                txn["amount"], txn["date"], txn["doc_type"], txn["payment_type"],
                cfg, txn.get("check_details"))
        except Exception as e:
            res = {"status": 0, "message": str(e)}

        pre_vat = round(txn["amount"] / VAT_RATE, 2)
        doc_link = res.get("data", "")
        finbot_status = "success" if res.get("status") == 1 else "error"

        # Record in DB regardless of success/failure
        db.record_transaction(
            fingerprint=txn["fingerprint"],
            screenshot_hash=",".join(sess.get("screenshot_hashes", [])),
            bank_date=txn["date"],
            amount=txn["amount"],
            amount_before_vat=pre_vat,
            payer_name=txn["bank_desc"],
            finbot_id=txn["customer_id"],
            customer_name=txn["customer_name"],
            doc_type=txn["doc_type"],
            finbot_doc_link=doc_link,
            finbot_status=finbot_status,
        )

        if res.get("status") == 1:
            ok += 1
            lines.append(f"✅ {txn['customer_name']} — {fmt(txn['amount'])}")
            if doc_link:
                lines.append(f"   [צפה במסמך]({doc_link})")
            # Check missing email
            if not cust_email:
                no_email.append(txn["customer_name"])
        else:
            err += 1
            msg = res.get("message", "שגיאה לא ידועה")
            lines.append(f"❌ {txn['customer_name']} — {fmt(txn['amount'])}")
            lines.append(f"   {msg}")
        lines.append("")

    lines.append(f"*סיכום:* {ok} הצליחו, {err} נכשלו")

    # ── Missing email warning ──
    if no_email:
        lines.append("")
        lines.append("📭 *ללקוחות הבאים אין מייל — המסמך לא נשלח:*")
        for name in no_email:
            lines.append(f"  • {name}")

    await status.edit_text("\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)

    # ── Delete all screenshots ──
    for msg_id in sess.get("screenshot_msg_ids", []):
        try:
            await update.effective_chat.delete_message(msg_id)
        except:
            pass

    # ── Overdue check (show who's late based on their due date) ──
    now = datetime.now(TZ)
    if now.day >= 10:
        month_key = now.strftime("%Y-%m")
        overdue = db.get_overdue_customers(month_key, now.day)
        if overdue:
            ulines = [f"\n🚨 *לא שילמו — {month_key}:*\n"]
            for c in overdue:
                due = c.get("payment_due_day", 10)
                days_late = now.day - due
                ulines.append(f"  ⏳ {c['name']} — איחור {days_late} ימים (עד ה-{due})")
            await update.effective_chat.send_message(
                "\n".join(ulines), parse_mode="Markdown")

    clear_session(chat_id)

# ── Daily reminder ──────────────────────────────────────────────────────────

async def daily_reminder(ctx: ContextTypes.DEFAULT_TYPE):
    now = datetime.now(TZ)
    if now.day not in REMINDER_DAYS:
        return
    if not OWNER_CHAT_ID:
        return
    try:
        await ctx.bot.send_message(
            OWNER_CHAT_ID,
            "📸 *תזכורת*\nשלח צילום מסך של One Zero.",
            parse_mode="Markdown")
    except Exception as e:
        log.error(f"Reminder failed: {e}")

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("token", cmd_token))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("customers", cmd_customers))
    app.add_handler(CommandHandler("active", cmd_active))
    app.add_handler(CommandHandler("activate", cmd_activate))
    app.add_handler(CommandHandler("deactivate", cmd_deactivate))
    app.add_handler(CommandHandler("alias", cmd_alias))
    app.add_handler(CommandHandler("unpaid", cmd_unpaid))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("receipt", cmd_receipt))
    app.add_handler(CommandHandler("invoice", cmd_invoice))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    if OWNER_CHAT_ID:
        app.job_queue.run_daily(
            daily_reminder,
            time=dtime(hour=REMINDER_HOUR, minute=0, tzinfo=TZ),
            name="daily_reminder")
        log.info(f"Reminder at {REMINDER_HOUR}:00 for chat {OWNER_CHAT_ID}")

    log.info(f"Bot v2 starting... ({len(GEMINI_KEYS)} Gemini key(s) configured)")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
