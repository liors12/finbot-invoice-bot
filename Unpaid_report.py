import sqlite3,re,datetime,urllib.request,urllib.parse
from pathlib import Path
D=Path('/home/finbot-invoice-bot')
env={}
for l in (D/'.env').read_text().splitlines():
    if '=' in l and not l.strip().startswith('#'):
        k,v=l.split('=',1); env[k.strip()]=v.strip().strip('"').strip("'")
token=next((v for v in env.values() if re.fullmatch(r'\d{6,}:[\w-]{30,}',v)),None)
chat=next((v.split(',')[0].strip() for k,v in env.items() if re.search('CHAT|OWNER|ADMIN|USER',k.upper()) and re.fullmatch(r'-?\d+(,\s*-?\d+)*',v)),None)
if not token or not chat:
    print("לא מצאתי טוקן או צ'אט. שמות המשתנים:",list(env)); raise SystemExit
t=datetime.date.today(); mk=t.strftime('%Y-%m')
c=sqlite3.connect(str(D/'data/finbot.db'))
rows=c.execute("""select name,payment_due_day from customers c where active=1 and not exists
 (select 1 from processed_txns p where p.finbot_id=c.finbot_id and p.month_key=? and p.finbot_status='success')
 order by payment_due_day,name""",(mk,)).fetchall()
late=[r for r in rows if r[1]<=t.day]; soon=[r for r in rows if r[1]>t.day]
msg=f"💰 מי עוד לא שילם · {t.strftime('%d/%m/%Y')}\n\n"
msg+="🔴 עבר התאריך:\n"+("\n".join(f"• {n} (עד ה־{d})" for n,d in late) or "אין 🎉")
msg+="\n\n⏳ עוד לא הגיע הזמן:\n"+("\n".join(f"• {n} (עד ה־{d})" for n,d in soon) or "אין")
msg+="\n\n(לפי חשבוניות שהבוט הוציא החודש)"
urllib.request.urlopen(f"https://api.telegram.org/bot{token}/sendMessage",
    urllib.parse.urlencode({'chat_id':chat,'text':msg}).encode())
print("נשלח לטלגרם ✅")
