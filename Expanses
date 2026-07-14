"""
Expenses tracking module.
Parses credit card Excel exports (Max format) and bank screenshots,
filters to a calendar month, and generates income-vs-expenses reports.
"""

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime
from pathlib import Path
import logging

log = logging.getLogger(__name__)

# ── Parse credit card Excel ────────────────────────────────────────────────

def parse_credit_card_excel(file_path: str) -> list[dict]:
    """
    Parse a Max/Isracard credit card export xlsx.
    Returns list of transactions with: date, name, category, amount, charge_date, notes, tx_type
    """
    wb = openpyxl.load_workbook(file_path, data_only=True)
    transactions = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        is_foreign = "חו\"ל" in sheet_name or "מט\"ח" in sheet_name

        # Find header row (contains "תאריך עסקה")
        header_row = None
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if row and row[0] == "תאריך עסקה":
                header_row = i
                break

        if not header_row:
            continue

        # Parse data rows
        for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
            if not row[0]:  # skip empty rows
                continue

            date_str = str(row[0]).strip()
            name = str(row[1] or "").strip()
            category = str(row[2] or "שונות").strip()
            amount = row[5] or 0  # סכום חיוב
            charge_date_str = str(row[9] or "").strip()
            notes = str(row[10] or "").strip()
            tx_type = str(row[4] or "רגילה").strip()

            # Parse dates
            tx_date = _parse_date(date_str)
            charge_date = _parse_date(charge_date_str)

            if tx_date and amount:
                transactions.append({
                    "date": tx_date,
                    "name": name,
                    "category": category,
                    "amount": float(amount),
                    "charge_date": charge_date,
                    "notes": notes,
                    "tx_type": tx_type,
                    "is_foreign": is_foreign,
                    "source": "credit_card",
                })

    wb.close()
    return transactions


def _parse_date(date_str: str) -> datetime | None:
    """Parse date from various formats."""
    if not date_str or date_str == "None":
        return None
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(date_str.strip(), fmt)
        except ValueError:
            continue
    return None


# ── Filter to calendar month ──────────────────────────────────────────────

def filter_to_month(transactions: list[dict], year: int, month: int) -> list[dict]:
    """
    Filter transactions to a specific calendar month.
    - Regular transactions: by transaction date
    - Installments (תשלומים): by charge date (when money actually left)
    """
    filtered = []
    for t in transactions:
        if t["tx_type"] == "תשלומים":
            # Installments: use charge date
            if t.get("charge_date") and t["charge_date"].year == year and t["charge_date"].month == month:
                filtered.append(t)
        else:
            # Regular: use transaction date
            if t["date"].year == year and t["date"].month == month:
                filtered.append(t)
    return filtered


# ── Categorize and summarize ──────────────────────────────────────────────

def summarize_by_category(transactions: list[dict]) -> dict:
    """Group transactions by category and return totals."""
    categories = {}
    for t in transactions:
        cat = t["category"]
        if cat not in categories:
            categories[cat] = {"total": 0, "count": 0, "items": []}
        categories[cat]["total"] += t["amount"]
        categories[cat]["count"] += 1
        categories[cat]["items"].append(t)
    # Sort by total descending
    return dict(sorted(categories.items(), key=lambda x: -x[1]["total"]))


# ── Generate Excel report ─────────────────────────────────────────────────

def generate_report(
    month_key: str,
    expenses: list[dict],
    income_total: float,
    income_count: int,
    income_details: list[dict],
    output_path: str
) -> str:
    """
    Generate an xlsx report with:
    - Summary: income vs expenses
    - Expenses by category
    - Detailed expense list
    - Income details
    """
    wb = openpyxl.Workbook()

    # Styles
    header_font = Font(name="Arial", bold=True, size=12, color="FFFFFF")
    header_fill = PatternFill(start_color="2B5797", end_color="2B5797", fill_type="solid")
    cat_font = Font(name="Arial", bold=True, size=11)
    cat_fill = PatternFill(start_color="D6E4F0", end_color="D6E4F0", fill_type="solid")
    money_fmt = '#,##0.00 ₪'
    pct_fmt = '0.0%'
    normal_font = Font(name="Arial", size=10)
    green_font = Font(name="Arial", size=12, bold=True, color="006600")
    red_font = Font(name="Arial", size=12, bold=True, color="CC0000")
    border = Border(
        bottom=Side(style="thin", color="CCCCCC"),
    )

    year, mon = month_key.split("-")
    title = f"דוח הכנסות והוצאות — {month_key}"

    # ── Sheet 1: Summary ──
    ws = wb.active
    ws.title = "סיכום"
    ws.sheet_properties.sheetView = openpyxl.worksheet.views.SheetView(rightToLeft=True)

    expenses_total = sum(t["amount"] for t in expenses)
    balance = income_total - expenses_total
    categories = summarize_by_category(expenses)

    # Title
    ws.merge_cells("A1:D1")
    ws["A1"] = title
    ws["A1"].font = Font(name="Arial", bold=True, size=16)
    ws["A1"].alignment = Alignment(horizontal="center")

    # Income vs Expenses summary
    ws["A3"] = "הכנסות"
    ws["B3"] = income_total
    ws["A3"].font = green_font
    ws["B3"].font = green_font
    ws["B3"].number_format = money_fmt

    ws["A4"] = "הוצאות"
    ws["B4"] = expenses_total
    ws["A4"].font = red_font
    ws["B4"].font = red_font
    ws["B4"].number_format = money_fmt

    ws["A5"] = "יתרה"
    ws["B5"] = balance
    ws["A5"].font = Font(name="Arial", bold=True, size=12)
    bal_color = "006600" if balance >= 0 else "CC0000"
    ws["B5"].font = Font(name="Arial", bold=True, size=12, color=bal_color)
    ws["B5"].number_format = money_fmt

    # Category breakdown
    ws["A7"] = "פירוט הוצאות לפי קטגוריה"
    ws["A7"].font = Font(name="Arial", bold=True, size=13)

    headers = ["קטגוריה", "סכום", "מספר עסקאות", "אחוז מסך ההוצאות"]
    for j, h in enumerate(headers, 1):
        cell = ws.cell(row=8, column=j, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    row = 9
    for cat, data in categories.items():
        ws.cell(row=row, column=1, value=cat).font = normal_font
        c = ws.cell(row=row, column=2, value=data["total"])
        c.number_format = money_fmt
        c.font = normal_font
        ws.cell(row=row, column=3, value=data["count"]).font = normal_font
        pct_cell = ws.cell(row=row, column=4, value=data["total"] / expenses_total if expenses_total else 0)
        pct_cell.number_format = pct_fmt
        pct_cell.font = normal_font
        for j in range(1, 5):
            ws.cell(row=row, column=j).border = border
        row += 1

    # Total row
    ws.cell(row=row, column=1, value="סה\"כ").font = cat_font
    c = ws.cell(row=row, column=2, value=expenses_total)
    c.number_format = money_fmt
    c.font = cat_font
    ws.cell(row=row, column=3, value=len(expenses)).font = cat_font

    # Column widths
    ws.column_dimensions["A"].width = 25
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 15
    ws.column_dimensions["D"].width = 20

    # ── Sheet 2: Expense details ──
    ws2 = wb.create_sheet("פירוט הוצאות")
    ws2.sheet_properties.sheetView = openpyxl.worksheet.views.SheetView(rightToLeft=True)

    detail_headers = ["תאריך", "שם בית עסק", "קטגוריה", "סכום", "סוג עסקה", "הערות"]
    for j, h in enumerate(detail_headers, 1):
        cell = ws2.cell(row=1, column=j, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    for i, t in enumerate(sorted(expenses, key=lambda x: x["date"]), 2):
        ws2.cell(row=i, column=1, value=t["date"].strftime("%d/%m/%Y")).font = normal_font
        ws2.cell(row=i, column=2, value=t["name"]).font = normal_font
        ws2.cell(row=i, column=3, value=t["category"]).font = normal_font
        c = ws2.cell(row=i, column=4, value=t["amount"])
        c.number_format = money_fmt
        c.font = normal_font
        ws2.cell(row=i, column=5, value=t["tx_type"]).font = normal_font
        ws2.cell(row=i, column=6, value=t["notes"]).font = normal_font
        for j in range(1, 7):
            ws2.cell(row=i, column=j).border = border

    ws2.column_dimensions["A"].width = 14
    ws2.column_dimensions["B"].width = 30
    ws2.column_dimensions["C"].width = 20
    ws2.column_dimensions["D"].width = 15
    ws2.column_dimensions["E"].width = 15
    ws2.column_dimensions["F"].width = 30

    # ── Sheet 3: Income details ──
    ws3 = wb.create_sheet("פירוט הכנסות")
    ws3.sheet_properties.sheetView = openpyxl.worksheet.views.SheetView(rightToLeft=True)

    inc_headers = ["תאריך", "לקוח", "סכום"]
    for j, h in enumerate(inc_headers, 1):
        cell = ws3.cell(row=1, column=j, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    for i, inc in enumerate(income_details, 2):
        ws3.cell(row=i, column=1, value=inc.get("bank_date", "")).font = normal_font
        ws3.cell(row=i, column=2, value=inc.get("customer_name", "")).font = normal_font
        c = ws3.cell(row=i, column=3, value=inc.get("amount", 0))
        c.number_format = money_fmt
        c.font = normal_font
        for j in range(1, 4):
            ws3.cell(row=i, column=j).border = border

    ws3.column_dimensions["A"].width = 14
    ws3.column_dimensions["B"].width = 30
    ws3.column_dimensions["C"].width = 15

    # Save
    wb.save(output_path)
    log.info(f"Report saved to {output_path}")
    return output_path
