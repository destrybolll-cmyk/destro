import json, base64

script = (
    "import asyncio, sys\n"
    "sys.path.insert(0, '/app')\n"
    "from config import BOT_TOKEN\n"
    "from database import Database\n"
    "from aiogram import Bot\n"
    "from aiogram.client.default import DefaultBotProperties\n"
    "from aiogram.enums import ParseMode\n"
    "db = Database('/app/data/bot.db')\n"
    "async def go():\n"
    "    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))\n"
    "    try:\n"
    "        c = await bot.get_chat('@xancurse1')\n"
    "        uid = c.id\n"
    "        existing = db.get_user(uid)\n"
    "        if existing:\n"
    "            print(f'EXISTS: anon=#{existing[\"id\"]} uid={uid} name={c.first_name} user=@{c.username}')\n"
    "        else:\n"
    "            anon_id, _ = db.add_user(uid, c.first_name or '', c.username or '', c.language_code or '')\n"
    "            print(f'ADDED: anon=#{anon_id} uid={uid} name={c.first_name} user=@{c.username}')\n"
    "    except Exception as e:\n"
    "        print(f'ERROR: {e}')\n"
    "    await bot.session.close()\n"
    "asyncio.run(go())\n"
)

b64 = base64.b64encode(script.encode()).decode()
body = {
    "jsonrpc": "2.0", "id": 40, "method": "tools/call",
    "params": {
        "name": "jrnm_execute_command_in_app",
        "arguments": {
            "appId": 26452,
            "commandAndArguments": ["/bin/sh", "-c",
                "base64 -d > /app/data/find.py && timeout 30 python /app/data/find.py"],
            "stdin": b64
        }
    }
}
with open(r"C:\Users\Victus\AppData\Local\Temp\body.json", "w") as f:
    json.dump(body, f, ensure_ascii=False)
print("OK")
