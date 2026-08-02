import requests
import json

bot_token = "7718954879:AAEv34T0_y7mJYV4WNW-QfSGqqK6gv9XgDw"
chat_id = "823456450"

message = """<b>🟢 ALPHA NEXUS ONLINE 🟢</b>
━━━━━━━━━━━━━━━━━━━━━━━━
<b>Bot State:</b> Active & Listening
<b>Wallet:</b> <code>TestWalletKey</code>
<b>Time:</b> 2026-08-01 12:00:00 UTC
━━━━━━━━━━━━━━━━━━━━━━━━
ʜᴛꜰ © ᴀʟᴘʜᴀ ᴀʟᴇʀᴛꜱ | v1.01"""

payload = {
    "chat_id": chat_id,
    "text": message,
    "parse_mode": "HTML",
    "disable_web_page_preview": True
}

r = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload)
print(r.status_code)
print(r.text)
