import json, base64

script = (
    "import asyncio, sys\n"
    "sys.path.insert(0, '/app')\n"
    "from config import BOT_TOKEN\n"
    "from aiogram import Bot\n"
    "from aiogram.client.default import DefaultBotProperties\n"
    "from aiogram.enums import ParseMode\n"
    "async def go():\n"
    "    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))\n"
    "    try:\n"
    "        c = await bot.get_chat(7572235876)\n"
    "        print(f'OK: id={c.id} name={c.first_name} user=@{c.username}')\n"
    "    except Exception as e:\n"
    "        print(f'ERROR: {e}')\n"
    "    await bot.session.close()\n"
    "asyncio.run(go())\n"
)

b64 = base64.b64encode(script.encode()).decode()
body = {
    "jsonrpc": "2.0", "id": 41, "method": "tools/call",
    "params": {
        "name": "jrnm_execute_command_in_app",
        "arguments": {
            "appId": 26452,
            "commandAndArguments": ["/bin/sh", "-c",
                "base64 -d > /tmp/test_bot.py && timeout 15 python /tmp/test_bot.py"],
            "stdin": b64
        }
    }
}
with open(r"C:\Users\Victus\AppData\Local\Temp\body.json", "w") as f:
    json.dump(body, f, ensure_ascii=False)
print("OK")
