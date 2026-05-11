import asyncio
import logging
import html
import os

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import BOT_TOKEN, ADMIN_ID
from database import Database

# Track which user the admin is currently replying to (stores anon_id)
admin_pending_reply: int | None = None

# Track "write to user" two-step flow
write_flow_step: str | None = None  # "await_id" or "await_text"
write_flow_anon_id: int | None = None

# Track waiting messages: user_id -> message_id of "подождите" message
waiting_messages: dict[int, int] = {}


async def delete_waiting(target_user_id: int):
    msg_id = waiting_messages.pop(target_user_id, None)
    if msg_id:
        try:
            await bot.delete_message(target_user_id, msg_id)
        except Exception:
            pass


def get_message_text(message: Message) -> str:
    return message.text or message.caption or ""


def get_content_type_label(message: Message) -> str | None:
    if message.sticker:
        return "🃏 <b>Стикер</b>"
    if message.photo:
        return "📷 <b>Фото</b>"
    if message.video:
        return "🎬 <b>Видео</b>"
    if message.animation:
        return "🎞️ <b>GIF</b>"
    if message.voice:
        return "🎤 <b>Голосовое</b>"
    if message.video_note:
        return "📹 <b>Видеосообщение</b>"
    if message.audio:
        return "🎵 <b>Аудио</b>"
    if message.document:
        return "📎 <b>Документ</b>"
    return None


async def forward_media(chat_id: int, message: Message, caption: str = "", reply_markup=None):
    caption_trunc = caption[:1024] if caption else ""
    if message.photo:
        return await bot.send_photo(chat_id, message.photo[-1].file_id, caption=caption_trunc, reply_markup=reply_markup)
    if message.video:
        return await bot.send_video(chat_id, message.video.file_id, caption=caption_trunc, reply_markup=reply_markup)
    if message.animation:
        return await bot.send_animation(chat_id, message.animation.file_id, caption=caption_trunc, reply_markup=reply_markup)
    if message.sticker:
        await bot.send_sticker(chat_id, message.sticker.file_id)
        if caption:
            await bot.send_message(chat_id, caption, reply_markup=reply_markup)
        return
    if message.voice:
        return await bot.send_voice(chat_id, message.voice.file_id, caption=caption_trunc, reply_markup=reply_markup)
    if message.video_note:
        await bot.send_video_note(chat_id, message.video_note.file_id)
        if caption:
            await bot.send_message(chat_id, caption, reply_markup=reply_markup)
        return
    if message.audio:
        return await bot.send_audio(chat_id, message.audio.file_id, caption=caption_trunc, reply_markup=reply_markup)
    if message.document:
        return await bot.send_document(chat_id, message.document.file_id, caption=caption_trunc, reply_markup=reply_markup)
    if caption:
        return await bot.send_message(chat_id, caption, reply_markup=reply_markup)


ADMIN_NAME = "Cookie"
USERS_PER_PAGE = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()
db_path = os.getenv("DB_PATH", "bot.db")
db = Database(db_path)


def esc(text: str) -> str:
    return html.escape(text or "")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def user_actions_keyboard(anon_id: int, is_banned: bool = False) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text="✍️ Ответить", callback_data=f"reply:{anon_id}")
    builder.button(text="🆔 Инфо", callback_data=f"info:{anon_id}")
    if is_banned:
        builder.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
    else:
        builder.button(text="🚫 Заблокировать", callback_data=f"ban:{anon_id}")
    builder.adjust(1)
    return builder


# ────────────────────────────── Users ──────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if is_admin(message.from_user.id):
        await message.answer(
            f"🍪 <b>Добро пожаловать, {ADMIN_NAME}!</b>\n\n"
            "Бот для анонимных сообщений запущен.\n"
            "Пользователи пишут боту — ты видишь их сообщения "
            "с анонимным ID и можешь отвечать.\n\n"
            "Нажимай <b>✍️ Ответить</b> под сообщением — "
            "и просто пиши текст, без команд!\n\n"
            "📜 <b>История</b> — все сообщения за последний час.\n\n"
            "📌 Кнопки внизу — быстрый доступ к командам.",
            reply_markup=admin_cmds_keyboard(),
        )
        return

    anon_id, _ = db.add_user(
        message.from_user.id,
        message.from_user.first_name or "",
        message.from_user.username or "",
        message.from_user.language_code or "",
    )

    await message.answer(
        "👋 <b>Привет! Я анонимный бот.</b>\n\n"
        "Ты можешь написать мне любое сообщение, "
        f"и я передам его <b>{ADMIN_NAME}</b> <b>анонимно</b>.\n"
        "Никто не узнает твой Telegram ID или личные данные.\n\n"
        f"Твой анонимный номер: <b>#{anon_id}</b>\n\n"
        "Просто напиши что-нибудь ниже ✉️"
    )


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if is_admin(message.from_user.id):
        await message.answer(
            f"🔧 <b>Команды {ADMIN_NAME}:</b>\n\n"
            "✍️ <b>Написать</b> — выбрать пользователя и написать\n\n"
            "📜 <b>История</b> — все сообщения за последний час\n"
            "   <code>/history 30</code> — за 30 минут\n\n"
            "📊 <b>Статистика</b> — статистика бота\n\n"
            "👥 <b>Список</b> — список пользователей\n\n"
            "📢 <b>Рассылка</b> — написать всем\n\n"
            "❌ <b>Отмена</b> — отменить текущее действие",
            reply_markup=admin_cmds_keyboard(),
        )
    else:
        await message.answer(
            f"🤖 <b>Как это работает</b>\n\n"
            "1. Ты отправляешь мне сообщение\n"
            f"2. Я анонимно передаю его <b>{ADMIN_NAME}</b>\n"
            f"3. <b>{ADMIN_NAME}</b> может ответить\n"
            "4. Я передаю ответ тебе\n\n"
            "Всё полностью анонимно 🔒"
        )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    if not is_admin(message.from_user.id):
        return
    global admin_pending_reply, write_flow_step, write_flow_anon_id
    admin_pending_reply = None
    write_flow_step = None
    write_flow_anon_id = None
    await cmd_help(message)


# ───────────────────────────── Admin commands ─────────────────────────────

@dp.message(Command("reply"))
async def cmd_reply(message: Message):
    global admin_pending_reply, write_flow_step, write_flow_anon_id
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer(
            "❌ <b>Неверный формат.</b>\n"
            "Используй: <code>/reply &lt;id&gt; &lt;текст&gt;</code>"
        )
        return

    try:
        anon_id = int(parts[1])
    except ValueError:
        await message.answer("❌ ID должен быть числом.")
        return

    target_user_id = db.get_user_id_by_anon(anon_id)
    if target_user_id is None:
        await message.answer(f"❌ Пользователь #{anon_id} не найден.")
        return

    reply_text = parts[2]

    admin_pending_reply = None
    write_flow_step = None
    write_flow_anon_id = None
    await delete_waiting(target_user_id)
    try:
        await bot.send_message(
            target_user_id,
            f"✉️ <b>Ответ от {ADMIN_NAME}:</b>\n\n{esc(reply_text)}",
        )
        db.save_message(target_user_id, anon_id, reply_text, direction="admin_to_user")
        await message.answer(f"✅ Ответ отправлен пользователю #{anon_id}!")
    except Exception as e:
        await message.answer(f"❌ Не удалось отправить: {e}")


@dp.message(Command("ban"))
async def cmd_ban(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        anon_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Используй: <code>/ban &lt;id&gt;</code>")
        return

    target_user_id = db.get_user_id_by_anon(anon_id)
    if target_user_id is None:
        await message.answer(f"❌ Пользователь #{anon_id} не найден.")
        return

    db.ban_user(target_user_id)
    await message.answer(f"🚫 Пользователь #{anon_id} заблокирован.")


@dp.message(Command("unban"))
async def cmd_unban(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        anon_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Используй: <code>/unban &lt;id&gt;</code>")
        return

    target_user_id = db.get_user_id_by_anon(anon_id)
    if target_user_id is None:
        await message.answer(f"❌ Пользователь #{anon_id} не найден.")
        return

    db.unban_user(target_user_id)
    await message.answer(f"✅ Пользователь #{anon_id} разблокирован.")


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if not is_admin(message.from_user.id):
        return

    stats = db.get_stats()
    await message.answer(
        f"📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего: <b>{stats['total']}</b>\n"
        f"🚫 Заблокировано: <b>{stats['banned']}</b>\n"
        f"✅ Активных: <b>{stats['active']}</b>"
    )


def paginated_users_list(page: int = 1):
    users = db.get_all_users()
    if not users:
        return "📭 Нет пользователей.", None

    total_pages = max(1, (len(users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = users[start:end]

    lines = [f"👥 <b>Пользователи</b> (стр. {page}/{total_pages}):\n"]
    for u in page_users:
        ban_icon = "🚫" if u["is_banned"] else "✅"
        name = esc(u["first_name"] or "—")
        username = f" @{esc(u['username'])}" if u["username"] else ""
        lines.append(f"{ban_icon} #<b>{u['id']}</b> — {name}{username}")

    text = "\n".join(lines)

    rows = []
    row = []
    for u in page_users:
        label = (u["first_name"] or f"#{u['id']}")[:14]
        row.append(InlineKeyboardButton(text=f"✍️ {label}", callback_data=f"wrt:{u['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"pgn:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="none"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"pgn:{page + 1}"))
        rows.append(nav)

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("list"))
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = paginated_users_list(1)
    await message.answer(text, reply_markup=markup)


@dp.message(Command("user"))
async def cmd_user(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        anon_id = int(message.text.split()[1])
    except (IndexError, ValueError):
        await message.answer("❌ Используй: <code>/user &lt;id&gt;</code>")
        return

    u = db.get_user_by_anon(anon_id)
    if u is None:
        await message.answer(f"❌ Пользователь #{anon_id} не найден.")
        return

    name = esc(u["first_name"] or "—")
    username = f"@{esc(u['username'])}" if u["username"] else "—"
    lang = u["language_code"] or "—"
    created = u["created_at"][:16] if u["created_at"] else "—"
    last = u["last_active"][:16] if u["last_active"] else "—"
    status = "🚫 Заблокирован" if u["is_banned"] else "✅ Активен"

    text = (
        f"👤 <b>Пользователь #{anon_id}</b>\n\n"
        f"👤 Имя: {name}\n"
        f"🔗 Юзернейм: {username}\n"
        f"🌐 Язык: {lang}\n"
        f"📅 Создан: {created}\n"
        f"🕐 Активен: {last}\n"
        f"📌 Статус: {status}"
    )

    kb = InlineKeyboardBuilder()
    kb.button(text="✍️ Написать", callback_data=f"reply:{anon_id}")
    if u["is_banned"]:
        kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
    else:
        kb.button(text="🚫 Заблокировать", callback_data=f"ban:{anon_id}")
    kb.adjust(1)

    await message.answer(text, reply_markup=kb.as_markup())


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not is_admin(message.from_user.id):
        return

    try:
        text = message.text.split(maxsplit=1)[1]
    except IndexError:
        await message.answer("❌ Используй: <code>/broadcast &lt;текст&gt;</code>")
        return

    users = db.get_all_users()
    sent = 0
    failed = 0

    status = await message.answer("📢 Рассылка началась...")

    for u in users:
        if u["is_banned"]:
            continue
        try:
            await bot.send_message(
                u["user_id"],
                f"📢 <b>Сообщение от {ADMIN_NAME}:</b>\n\n{esc(text)}",
            )
            sent += 1
            await asyncio.sleep(0.03)
        except Exception:
            failed += 1

    await status.edit_text(
        f"📢 <b>Рассылка завершена</b>\n\n"
        f"✅ Доставлено: <b>{sent}</b>\n"
        f"❌ Ошибок: <b>{failed}</b>"
    )


@dp.message(Command("history"))
async def cmd_history(message: Message):
    if not is_admin(message.from_user.id):
        return

    parts = message.text.split()
    minutes = 60
    if len(parts) > 1:
        try:
            minutes = int(parts[1])
        except ValueError:
            pass

    rows = db.get_messages_since(minutes)
    if not rows:
        await message.answer(f"📭 За последние <b>{minutes}</b> мин сообщений нет.")
        return

    lines = [f"📜 <b>Сообщения за последние {minutes} мин:</b>\n"]
    current_id = None
    for r in rows:
        if r["anon_id"] != current_id:
            current_id = r["anon_id"]
            name = esc(r["first_name"] or f"#{current_id}")
            username = f" @{esc(r['username'])}" if r["username"] else ""
            time = r["timestamp"][11:16] if r["timestamp"] else ""
            lines.append(f"\n👤 #{current_id} — {name}{username} ({time}):")
        lines.append(f'  "{esc(r["text"] or "")}"')

    # Split if too long
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n<i>...сообщение обрезано</i>"

    await message.answer(text)


# ─────────────────────────── Callback queries ──────────────────────────

@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    global admin_pending_reply, write_flow_step, write_flow_anon_id
    if not is_admin(callback.from_user.id):
        await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
        return

    action, anon_id_str = callback.data.split(":")
    anon_id = int(anon_id_str)
    target_user_id = db.get_user_id_by_anon(anon_id)

    if target_user_id is None:
        await callback.answer("❌ Пользователь не найден.", show_alert=True)
        return

    if action == "reply":
        admin_pending_reply = anon_id
        write_flow_step = None
        write_flow_anon_id = None
        await callback.answer()
        await delete_waiting(target_user_id)
        await callback.message.answer(
            f"✏️ <b>Введите текст</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Отправь сообщение — оно будет доставлено.\n"
            "/cancel — отменить",
            reply_markup=admin_cmds_keyboard(),
        )

    elif action == "info":
        await callback.answer()
        await delete_waiting(target_user_id)
        u = db.get_user_by_anon(anon_id)
        if u is None:
            await callback.message.answer("❌ Пользователь не найден.")
            return
        name = esc(u["first_name"] or "—")
        username = f"@{esc(u['username'])}" if u["username"] else "—"
        lang = u["language_code"] or "—"
        created = u["created_at"][:16] if u["created_at"] else "—"
        last = u["last_active"][:16] if u["last_active"] else "—"
        status = "🚫 Заблокирован" if u["is_banned"] else "✅ Активен"
        text = (
            f"👤 <b>Пользователь #{anon_id}</b>\n\n"
            f"👤 Имя: {name}\n"
            f"🔗 Юзернейм: {username}\n"
            f"🌐 Язык: {lang}\n"
            f"📅 Создан: {created}\n"
            f"🕐 Активен: {last}\n"
            f"📌 Статус: {status}"
        )
        kb = InlineKeyboardBuilder()
        kb.button(text="✍️ Написать", callback_data=f"reply:{anon_id}")
        if u["is_banned"]:
            kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
        else:
            kb.button(text="🚫 Заблокировать", callback_data=f"ban:{anon_id}")
        kb.adjust(1)
        await callback.message.answer(text, reply_markup=kb.as_markup())

    elif action == "ban":
        await callback.answer("🚫 Пользователь заблокирован.")
        db.ban_user(target_user_id)
        await delete_waiting(target_user_id)
        # Update the keyboard
        new_kb = user_actions_keyboard(anon_id, is_banned=True).as_markup()
        await callback.message.edit_reply_markup(reply_markup=new_kb)

    elif action == "unban":
        await callback.answer("✅ Пользователь разблокирован.")
        db.unban_user(target_user_id)
        await delete_waiting(target_user_id)
        new_kb = user_actions_keyboard(anon_id, is_banned=False).as_markup()
        await callback.message.edit_reply_markup(reply_markup=new_kb)

    elif action == "wrt":
        admin_pending_reply = anon_id
        write_flow_step = None
        write_flow_anon_id = None
        await callback.answer()
        await delete_waiting(target_user_id)
        await callback.message.answer(
            f"✏️ <b>Введите текст</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Отправь сообщение — оно будет доставлено.\n"
            "/cancel — отменить",
            reply_markup=admin_cmds_keyboard(),
        )

    elif action == "pgn":
        page = anon_id  # anon_id_str was parsed as int, we use it as page
        await callback.answer()
        text, markup = paginated_users_list(page)
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)

    elif action == "none":
        await callback.answer()


BTN_WRITE = "✍️ Написать"
BTN_HISTORY = "📜 История"
BTN_STATS = "📊 Статистика"
BTN_LIST = "👥 Список"
BTN_BCAST = "📢 Рассылка"
BTN_HELP = "❓ Помощь"
BTN_CANCEL = "❌ Отмена"


def admin_cmds_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WRITE)],
            [KeyboardButton(text=BTN_HISTORY), KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_LIST), KeyboardButton(text=BTN_BCAST)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


BTN_CMDS = {BTN_WRITE, BTN_HISTORY, BTN_STATS, BTN_LIST, BTN_BCAST, BTN_HELP, BTN_CANCEL}


# ────────────────────────────── Messages ──────────────────────────────

@dp.message()
async def handle_user_message(message: Message):
    global admin_pending_reply, write_flow_step, write_flow_anon_id
    user_id = message.from_user.id

    if is_admin(user_id):
        if message.text and message.text.startswith("/"):
            return
        if message.text is None and admin_pending_reply is None and write_flow_step is None:
            return

        # ── Handle write-to-user flow ──
        if write_flow_step == "await_id":
            if message.text is None:
                await message.answer("❌ Пожалуйста, введи ID числом.")
                return
            try:
                anon_id = int(message.text)
                if not db.user_exists_by_anon(anon_id):
                    await message.answer("❌ Пользователь с таким ID не найден.")
                    write_flow_step = None
                    return
                write_flow_anon_id = anon_id
                write_flow_step = "await_text"
                await message.answer(
                    f"✅ ID #<b>{anon_id}</b> принят. Теперь введи текст сообщения:"
                )
            except ValueError:
                await message.answer(
                    "❌ ID должен быть числом. Попробуй ещё раз или /cancel"
                )
            return

        if write_flow_step == "await_text":
            target_user_id = db.get_user_id_by_anon(write_flow_anon_id)
            if target_user_id is None:
                await message.answer("❌ Пользователь больше не существует.")
                write_flow_step = None
                write_flow_anon_id = None
                return
            await delete_waiting(target_user_id)
            try:
                admin_text = get_message_text(message)
                prefix = f"✉️ <b>Сообщение от {ADMIN_NAME}:</b>"
                caption = f"{prefix}\n\n{esc(admin_text)}" if admin_text else prefix
                await forward_media(target_user_id, message, caption)
                db.save_message(target_user_id, write_flow_anon_id, admin_text, direction="admin_to_user")
                kb = admin_cmds_keyboard()
                await message.answer(
                    f"✅ Сообщение отправлено пользователю #<b>{write_flow_anon_id}</b>!",
                    reply_markup=kb,
                )
            except Exception as e:
                await message.answer(f"❌ Ошибка: {e}")
            write_flow_step = None
            write_flow_anon_id = None
            return

        # ── Handle keyboard buttons ──
        if message.text == BTN_WRITE:
            text, markup = paginated_users_list(1)
            await message.answer(
                "👇 <b>Выбери пользователя</b> — нажми ✍️ рядом с именем:",
                reply_markup=markup,
            )
            return
        if message.text == BTN_HISTORY:
            return await cmd_history(message)
        if message.text == BTN_STATS:
            return await cmd_stats(message)
        if message.text == BTN_LIST:
            return await cmd_list(message)
        if message.text == BTN_BCAST:
            await message.answer(
                "📢 <b>Введите текст для рассылки</b>\n\n"
                "После команды напиши сообщение:\n"
                "<code>/broadcast Ваш текст</code>"
            )
            return
        if message.text == BTN_HELP:
            return await cmd_help(message)
        if message.text == BTN_CANCEL:
            return await cmd_cancel(message)
        # If admin is in reply mode, send as reply to user
        if admin_pending_reply is not None:
            anon_id = admin_pending_reply
            target_user_id = db.get_user_id_by_anon(anon_id)
            if target_user_id is None:
                admin_pending_reply = None
                await message.answer(f"❌ Пользователь #{anon_id} не найден.")
                return
            await delete_waiting(target_user_id)
            try:
                admin_text = get_message_text(message)
                prefix = f"✉️ <b>Ответ от {ADMIN_NAME}:</b>"
                caption = f"{prefix}\n\n{esc(admin_text)}" if admin_text else prefix
                await forward_media(target_user_id, message, caption)
                db.save_message(target_user_id, anon_id, admin_text, direction="admin_to_user")
                kb = admin_cmds_keyboard()
                await message.answer(f"✅ Ответ отправлен пользователю #<b>{anon_id}</b>!", reply_markup=kb)
            except Exception as e:
                if "chat not found" in str(e).lower():
                    await message.answer("❌ Пользователь больше недоступен (удалил чат или заблокировал бота).")
                else:
                    await message.answer(f"❌ Не удалось отправить: {e}")
            admin_pending_reply = None
            return
        await message.answer(
            "💡 Используй кнопки внизу или нажми\n"
            "<b>✍️ Ответить</b> под сообщением пользователя.\n"
            "<code>/help</code> — список команд",
            reply_markup=admin_cmds_keyboard(),
        )
        return

    if db.is_banned(user_id):
        await message.answer("🚫 Вы заблокированы и не можете отправлять сообщения.")
        return

    user = message.from_user
    anon_id, is_banned = db.add_user(
        user_id,
        user.first_name or "",
        user.username or "",
        user.language_code or "",
    )

    last_admin_msg = db.get_last_admin_message(user_id)
    if last_admin_msg:
        reply_context = f"📎 <b>Ответ на ваше сообщение:</b>\n└ {esc(last_admin_msg[:100])}"
    else:
        reply_context = None

    info_lines = [
        f"📩 <b>Новое сообщение</b>",
        f"🆔 Анонимный ID: <b>#{anon_id}</b>",
        f"👤 {esc(user.first_name or '—')}",
    ]
    if user.username:
        info_lines.append(f"🔗 @{esc(user.username)}")
    if user.language_code:
        info_lines.append(f"🌐 {user.language_code}")

    if reply_context:
        info_lines.append("")
        info_lines.append(reply_context)

    content_type = get_content_type_label(message)
    if content_type:
        info_lines.append("")
        info_lines.append(content_type)
        user_text = message.caption or ""
    else:
        info_lines.append("")
        info_lines.append("<b>Текст:</b>")
        user_text = message.text or ""

    if user_text:
        info_lines.append(esc(user_text))

    kb = user_actions_keyboard(anon_id, is_banned=bool(is_banned)).as_markup()
    admin_text = "\n".join(info_lines)
    await forward_media(ADMIN_ID, message, admin_text, reply_markup=kb)

    msg_text = get_message_text(message)
    db.save_message(user_id, anon_id, msg_text)

    wait_msg = await message.answer(
        "🍪 <b>Подождите!</b>\n\n"
        "Ваше сообщение доставлено. "
        "Печенька в скором времени ответит вам. ✉️"
    )
    waiting_messages[user_id] = wait_msg.message_id


# ────────────────────────────── Entry ──────────────────────────────

async def main():
    while True:
        try:
            logging.info("🚀 Бот запущен!")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            logging.error(f"Критическая ошибка: {e}", exc_info=True)
            logging.info("Перезапуск через 5 секунд...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
