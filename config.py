import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))

if not BOT_TOKEN or not ADMIN_ID:
    raise ValueError("BOT_TOKEN and ADMIN_ID must be set via env vars or .env")
