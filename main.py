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

# Track "add user by ID" flow
add_user_step: bool = False

# Track "rename user" flow
rename_anon_id: int | None = None

# Tic-Tac-Toe games: key = user_id, value = game state
games: dict[int, dict] = {}

# Pending TTT challenges: key = challenged_user_id, value = {"challenger_id", "challenger_anon_id"}
pending_ttt: dict[int, dict] = {}


def make_ttt_board(board: list[list[str]], anon_id: int, finished: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for r in range(3):
        row_btns = []
        for c in range(3):
            cell = board[r][c]
            if cell == "X":
                row_btns.append(InlineKeyboardButton(text="❌", callback_data="none"))
            elif cell == "O":
                row_btns.append(InlineKeyboardButton(text="⭕", callback_data="none"))
            else:
                row_btns.append(InlineKeyboardButton(text="⬜", callback_data=f"ttt:move:{anon_id}:{r}:{c}"))
        kb.row(*row_btns)
    if finished:
        kb.row(InlineKeyboardButton(text="🔄 Играть ещё", callback_data=f"ttt:new:{anon_id}"))
    return kb.as_markup()


def check_ttt_winner(board: list[list[str]]) -> str | None:
    for i in range(3):
        if board[i][0] == board[i][1] == board[i][2] != " ":
            return board[i][0]
        if board[0][i] == board[1][i] == board[2][i] != " ":
            return board[0][i]
    if board[0][0] == board[1][1] == board[2][2] != " ":
        return board[0][0]
    if board[0][2] == board[1][1] == board[2][0] != " ":
        return board[0][2]
    return None


async def send_ttt_game(target_user_id: int, anon_id: int, admin_id: int, board: list[list[str]], current: str, game_over: bool = False):
    board_markup = make_ttt_board(board, anon_id, finished=game_over)
    admin_label = f"🎮 <b>Крестики-нолики</b> с пользователем #<b>{anon_id}</b>\n"
    user_label = f"🎮 <b>Крестики-нолики</b> с {ADMIN_NAME}\n"

    if game_over:
        winner = check_ttt_winner(board)
        if winner == "X":
            admin_label += "\n🏆 <b>Вы выиграли!</b>"
            user_label += "\n😞 <b>Вы проиграли.</b>"
        elif winner == "O":
            admin_label += "\n😞 <b>Вы проиграли.</b>"
            user_label += "\n🏆 <b>Вы выиграли!</b>"
        else:
            admin_label += "\n🤝 <b>Ничья!</b>"
            user_label += "\n🤝 <b>Ничья!</b>"
    else:
        turn = "Ваш ход" if (current == "X" and admin_id == ADMIN_ID) or (current == "O" and admin_id != ADMIN_ID) else "Ход соперника"
        admin_label += f"\n{('❌' if current == 'X' else '⭕')} {turn}"
        user_label += f"\n{('❌' if current == 'X' else '⭕')} {'Ваш ход' if current == 'O' else 'Ход соперника'}"

    game = games.get(target_user_id)
    admin_msg_id = game["admin_msg_id"] if game else None
    user_msg_id = game["user_msg_id"] if game else None

    if admin_msg_id:
        try:
            await Bot.get_current().edit_message_text(admin_label, chat_id=admin_id, message_id=admin_msg_id, reply_markup=board_markup)
        except Exception:
            msg = await Bot.get_current().send_message(admin_id, admin_label, reply_markup=board_markup)
            if game:
                game["admin_msg_id"] = msg.message_id
    else:
        msg = await Bot.get_current().send_message(admin_id, admin_label, reply_markup=board_markup)
        if game:
            game["admin_msg_id"] = msg.message_id

    if user_msg_id:
        try:
            await Bot.get_current().edit_message_text(user_label, chat_id=target_user_id, message_id=user_msg_id, reply_markup=board_markup)
        except Exception:
            msg = await Bot.get_current().send_message(target_user_id, user_label, reply_markup=board_markup)
            if game:
                game["user_msg_id"] = msg.message_id
    else:
        msg = await Bot.get_current().send_message(target_user_id, user_label, reply_markup=board_markup)
        if game:
            game["user_msg_id"] = msg.message_id

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
        "Просто напиши что-нибудь ниже ✉️\n\n"
        f"Также ты можешь вызвать <b>{ADMIN_NAME}</b> на 🎮 Крестики-нолики!",
        reply_markup=user_cmds_keyboard(),
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
            "➕ <b>Добавить ID</b> — добавить пользователя по Telegram ID\n\n"
            "🔍 <code>/find @user</code> — найти/добавить по юзернейму\n\n"
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
    global admin_pending_reply, write_flow_step, write_flow_anon_id, add_user_step, rename_anon_id
    admin_pending_reply = None
    write_flow_step = None
    write_flow_anon_id = None
    add_user_step = False
    rename_anon_id = None
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


def paginated_users_list(page: int = 1, action: str = "wrt", nav_prefix: str = "pgn"):
    users = db.get_all_users()
    if not users:
        return "📭 Нет пользователей.", None

    total_pages = max(1, (len(users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = users[start:end]

    action_icon = "🎮" if action == "ttt:new" else "✍️"
    title = "🎮 <b>Крестики-нолики</b> — выбери соперника" if action == "ttt:new" else "👥 <b>Пользователи</b>"
    lines = [f"{title} (стр. {page}/{total_pages}):\n"]
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
        row.append(InlineKeyboardButton(text=f"{action_icon} {label}", callback_data=f"{action}:{u['id']}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{nav_prefix}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"📄 {page}/{total_pages}", callback_data="none"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{nav_prefix}:{page + 1}"))
        rows.append(nav)

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("list"))
async def cmd_list(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = paginated_users_list(1)
    await message.answer(text, reply_markup=markup)


def banned_users_list():
    users = db.get_banned_users()
    if not users:
        return "🚫 <b>Нет заблокированных пользователей.</b>", None
    kb = InlineKeyboardBuilder()
    for u in users:
        name = esc(u["first_name"] or "—")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        kb.button(
            text=f"#{u['id']} — {name}{uname}",
            callback_data=f"info:{u['id']}",
        )
    kb.adjust(1)
    return (
        f"🚫 <b>Заблокированные пользователи</b> ({len(users)}):\n\n"
        "Нажми на пользователя, чтобы управлять им.",
        kb.as_markup(),
    )


async def cmd_banned(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = banned_users_list()
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
    kb.button(text="✏️ Изменить имя", callback_data=f"rename:{anon_id}")
    if u["is_banned"]:
        kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
    else:
        kb.button(text="🚫 Заблокировать", callback_data=f"ban:{anon_id}")
    kb.button(text="🎮 Крестики-нолики", callback_data=f"ttt:new:{anon_id}")
    kb.button(text="🗑 Удалить", callback_data=f"del_ask:{anon_id}")
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
    global admin_pending_reply, write_flow_step, write_flow_anon_id, rename_anon_id

    parts = callback.data.split(":")
    action = parts[0]

    # Allow both admin and user to play TTT
    if action == "ttt":
        sub = parts[1]

        # ── Accept / Decline challenge ──
        if sub in ("accept", "decline"):
            challenged_id = callback.from_user.id
            challenger_id = int(parts[2])
            challenge_anon_id = int(parts[3])
            challenge = pending_ttt.pop(challenged_id, None)

            if challenge is None:
                await callback.answer("❌ Вызов устарел.", show_alert=True)
                return

            if sub == "decline":
                try:
                    await callback.message.edit_text(
                        callback.message.html_text + "\n\n❌ Вызов отклонён",
                        reply_markup=None,
                    )
                except Exception:
                    pass
                if challenger_id == ADMIN_ID:
                    await callback.answer("✅ Вызов отклонён.")
                    await bot.send_message(ADMIN_ID, f"❌ Пользователь #<b>{challenge_anon_id}</b> отклонил вызов.")
                else:
                    await callback.answer("✅ Вызов отклонён.")
                    try:
                        await bot.send_message(challenger_id, "❌ Cookie отклонил ваш вызов на Крестики-нолики.")
                    except Exception:
                        pass
                return

            # ── Accept challenge ──
            if games:
                await callback.answer("❌ Cookie уже в другой игре.", show_alert=True)
                return
            non_admin_id = challenger_id if challenger_id != ADMIN_ID else challenged_id
            if non_admin_id in games:
                await callback.answer("❌ Игрок уже в игре.", show_alert=True)
                return

            board = [[" ", " ", " "] for _ in range(3)]
            games[non_admin_id] = {
                "board": board,
                "current": "X",
                "anon_id": challenge_anon_id,
                "admin_msg_id": None,
                "user_msg_id": None,
            }
            # Clean stale pending challenges for both participants
            pending_ttt.pop(ADMIN_ID, None)
            pending_ttt.pop(non_admin_id, None)

            # Notify admin when user accepts
            if challenger_id == ADMIN_ID:
                # User accepted admin's challenge
                await bot.send_message(ADMIN_ID, f"✅ Пользователь #<b>{challenge_anon_id}</b> принял вызов! Игра начинается...")
            else:
                # Admin accepted user's challenge
                await bot.send_message(challenger_id, "✅ Cookie принял ваш вызов! Игра начинается...")

            try:
                await callback.message.edit_text(
                    callback.message.html_text + "\n\n✅ Вызов принят!",
                    reply_markup=None,
                )
            except Exception:
                pass
            await callback.answer("✅ Игра началась!")
            await send_ttt_game(non_admin_id, challenge_anon_id, ADMIN_ID, board, "X")
            return

        # ── New challenge ──
        anon_id = int(parts[2])
        target_user_id = db.get_user_id_by_anon(anon_id)
        if target_user_id is None:
            await callback.answer("❌ Ошибка.", show_alert=True)
            return

        if sub == "new":
            challenger_id = callback.from_user.id
            challenged_id = ADMIN_ID if challenger_id != ADMIN_ID else target_user_id

            if challenger_id in games:
                await callback.answer("❌ Вы уже в игре.", show_alert=True)
                return
            if games:
                await callback.answer("🍪 Cookie сейчас занят игрой. Попробуйте позже.", show_alert=True)
                return
            if challenged_id in pending_ttt:
                await callback.answer("❌ Этому игроку уже отправлен вызов.", show_alert=True)
                return
            if challenged_id in games:
                await callback.answer("❌ Пользователь уже в игре.", show_alert=True)
                return

            kb = InlineKeyboardBuilder()
            kb.button(text="✅ Принять", callback_data=f"ttt:accept:{challenger_id}:{anon_id}")
            kb.button(text="❌ Отклонить", callback_data=f"ttt:decline:{challenger_id}:{anon_id}")
            kb.adjust(2)

            if challenger_id == ADMIN_ID:
                try:
                    await bot.send_message(
                        challenged_id,
                        f"🎮 <b>Крестики-нолики</b>\n\n{ADMIN_NAME} вызывает вас на игру!",
                        reply_markup=kb.as_markup(),
                    )
                except Exception:
                    await callback.answer("❌ Пользователь недоступен.", show_alert=True)
                    return
                await callback.message.answer(f"🎮 Вызов отправлен пользователю #<b>{anon_id}</b>. Ожидайте ответа...")
            else:
                try:
                    await bot.send_message(
                        ADMIN_ID,
                        f"🎮 <b>Крестики-нолики</b>\n\nПользователь #<b>{anon_id}</b> вызывает вас на игру!",
                        reply_markup=kb.as_markup(),
                    )
                except Exception:
                    await callback.answer("❌ Ошибка отправки.", show_alert=True)
                    return

            pending_ttt[challenged_id] = {"challenger_id": challenger_id, "challenger_anon_id": anon_id}
            await callback.answer("🎮 Вызов отправлен!")
            return

        # ── Make a move ──
        if sub == "move":
            r, c = int(parts[3]), int(parts[4])
            game = games.get(target_user_id)
            if game is None:
                await callback.answer("❌ Игра не найдена.", show_alert=True)
                return
            board = game["board"]
            player = callback.from_user.id
            expected = "X" if player == ADMIN_ID else "O"
            if game["current"] != expected:
                await callback.answer("⛔ Сейчас не твой ход!", show_alert=True)
                return
            if board[r][c] != " ":
                await callback.answer("❌ Клетка занята!", show_alert=True)
                return
            board[r][c] = expected
            winner = check_ttt_winner(board)
            if winner or all(board[i][j] != " " for i in range(3) for j in range(3)):
                await send_ttt_game(target_user_id, anon_id, ADMIN_ID, board, game["current"], game_over=True)
                if target_user_id in games:
                    del games[target_user_id]
            else:
                game["current"] = "O" if expected == "X" else "X"
                await send_ttt_game(target_user_id, anon_id, ADMIN_ID, board, game["current"])
            await callback.answer()
        return

    if not is_admin(callback.from_user.id):
        await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
        return

    # Handle TTT user-list pagination (page number, not anon_id)
    if action == "ttpgn":
        page = int(parts[1])
        await callback.answer()
        text, markup = paginated_users_list(page, action="ttt:new", nav_prefix="ttpgn")
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)
        return

    anon_id = int(parts[1])
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
        kb.button(text="✏️ Изменить имя", callback_data=f"rename:{anon_id}")
        if u["is_banned"]:
            kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
        else:
            kb.button(text="🚫 Заблокировать", callback_data=f"ban:{anon_id}")
        kb.button(text="🎮 Крестики-нолики", callback_data=f"ttt:new:{anon_id}")
        kb.button(text="🗑 Удалить", callback_data=f"del_ask:{anon_id}")
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

    elif action == "rename":
        rename_anon_id = anon_id
        await callback.answer()
        await callback.message.answer(
            f"✏️ <b>Введите новое имя</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Просто напиши новое имя.\n"
            "/cancel — отменить"
        )

    elif action == "del_ask":
        await callback.answer()
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Да, удалить", callback_data=f"del_yes:{anon_id}")
        kb.button(text="❌ Нет", callback_data=f"del_no:{anon_id}")
        kb.adjust(2)
        await callback.message.answer(
            f"🗑 <b>Точно удалить</b> пользователя #<b>{anon_id}</b>?\n"
            "Все его сообщения и данные будут безвозвратно удалены.",
            reply_markup=kb.as_markup(),
        )

    elif action == "del_yes":
        await callback.answer("🗑 Пользователь удалён.")
        db.delete_user(anon_id)
        try:
            await callback.message.delete()
        except Exception:
            pass

    elif action == "del_no":
        await callback.answer()
        try:
            await callback.message.delete()
        except Exception:
            pass



    elif action == "none":
        await callback.answer()


BTN_WRITE = "✍️ Написать"
BTN_HISTORY = "📜 История"
BTN_STATS = "📊 Статистика"
BTN_LIST = "👥 Список"
BTN_BANNED = "🚫 Блокировки"
BTN_ADD_ID = "➕ Добавить ID"
BTN_BCAST = "📢 Рассылка"
BTN_TTT = "🎮 Крестики-нолики"
BTN_HELP = "❓ Помощь"
BTN_CANCEL = "❌ Отмена"


def admin_cmds_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WRITE)],
            [KeyboardButton(text=BTN_HISTORY), KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_LIST), KeyboardButton(text=BTN_BANNED)],
            [KeyboardButton(text=BTN_TTT), KeyboardButton(text=BTN_ADD_ID)],
            [KeyboardButton(text=BTN_BCAST)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def user_cmds_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_TTT)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


BTN_CMDS = {BTN_WRITE, BTN_HISTORY, BTN_STATS, BTN_LIST, BTN_BANNED, BTN_ADD_ID, BTN_BCAST, BTN_TTT, BTN_HELP, BTN_CANCEL}


# ────────────────────────────── Messages ──────────────────────────────

@dp.message()
async def handle_user_message(message: Message):
    global admin_pending_reply, write_flow_step, write_flow_anon_id, add_user_step, rename_anon_id
    user_id = message.from_user.id

    if is_admin(user_id):
        if message.text and message.text.startswith("/"):
            return
        if message.text is None and admin_pending_reply is None and write_flow_step is None and not add_user_step and rename_anon_id is None:
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

        # ── Handle add-user-by-ID flow ──
        if message.text == BTN_ADD_ID:
            add_user_step = True
            await message.answer(
                "🔢 <b>Введи Telegram ID пользователя</b> (число).\n\n"
                "Чтобы узнать ID: перешли любое сообщение от пользователя "
                "в @userinfobot.\n\n"
                "Или отправь /cancel чтобы отменить."
            )
            return

        if add_user_step:
            if message.text is None:
                await message.answer("❌ ID должен быть числом.")
                return
            try:
                uid = int(message.text.strip())
            except ValueError:
                await message.answer("❌ ID должен быть числом.")
                return
            add_user_step = False
            existing = db.get_user(uid)
            if existing:
                await message.answer(
                    f"ℹ️ Пользователь с ID <code>{uid}</code> уже есть в БД.\n"
                    f"🆔 Анонимный ID: <b>#{existing['id']}</b>\n"
                    f"👤 {esc(existing['first_name'] or '—')}\n"
                    f"🔗 @{esc(existing['username']) if existing['username'] else '—'}",
                    reply_markup=admin_cmds_keyboard(),
                )
                return
            try:
                chat = await message.bot.get_chat(uid)
                anon_id, _ = db.add_user(uid, chat.first_name or "", chat.username or "", chat.language_code or "")
                await message.answer(
                    f"✅ Пользователь <code>{uid}</code> добавлен!\n"
                    f"🆔 Анонимный ID: <b>#{anon_id}</b>\n"
                    f"👤 {esc(chat.first_name or '—')}\n"
                    f"🔗 @{esc(chat.username) if chat.username else '—'}",
                    reply_markup=admin_cmds_keyboard(),
                )
            except Exception as e:
                anon_id, _ = db.add_user(uid, "", "", "")
                await message.answer(
                    f"⚠️ Пользователь <code>{uid}</code> добавлен в БД, "
                    f"но бот не может получить о нём информацию.\n"
                    f"🆔 Анонимный ID: <b>#{anon_id}</b>\n\n"
                    f"Возможно, этот пользователь ещё не писал боту.\n"
                    f"<code>{esc(str(e)[:200])}</code>",
                    reply_markup=admin_cmds_keyboard(),
                )
            return

        # ── Handle rename flow ──
        if rename_anon_id is not None:
            text = message.text or ""
            if not text.strip():
                await message.answer("❌ Имя не может быть пустым.")
                return
            new_name = text.strip()[:64]
            db.rename_user(rename_anon_id, new_name)
            anon_id = rename_anon_id
            rename_anon_id = None
            await message.answer(
                f"✅ Имя пользователя #<b>{anon_id}</b> изменено на <b>{esc(new_name)}</b>",
                reply_markup=admin_cmds_keyboard(),
            )
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
        if message.text == BTN_BANNED:
            return await cmd_banned(message)
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
        if message.text == BTN_TTT:
            text, markup = paginated_users_list(1, action="ttt:new", nav_prefix="ttpgn")
            await message.answer(
                "👇 <b>Выбери соперника</b> — нажми 🎮 рядом с именем:",
                reply_markup=markup,
            )
            return
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

    if message.text == BTN_TTT:
        if user_id in games:
            await message.answer("🎮 Вы уже в игре.")
            return
        if games:
            await message.answer("🍪 Cookie сейчас занят игрой. Попробуйте позже.")
            return
        if ADMIN_ID in pending_ttt:
            await message.answer("🎮 Вызов уже отправлен. Ожидайте ответа.")
            return

        anon_id, is_banned = db.add_user(
            user_id,
            message.from_user.first_name or "",
            message.from_user.username or "",
            message.from_user.language_code or "",
        )
        if is_banned:
            await message.answer("🚫 Вы заблокированы.")
            return

        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Принять", callback_data=f"ttt:accept:{user_id}:{anon_id}")
        kb.button(text="❌ Отклонить", callback_data=f"ttt:decline:{user_id}:{anon_id}")
        kb.adjust(2)
        try:
            await bot.send_message(
                ADMIN_ID,
                f"🎮 <b>Крестики-нолики</b>\n\nПользователь #<b>{anon_id}</b> вызывает вас на игру!",
                reply_markup=kb.as_markup(),
            )
        except Exception:
            await message.answer("❌ Ошибка отправки вызова.")
            return

        pending_ttt[ADMIN_ID] = {"challenger_id": user_id, "challenger_anon_id": anon_id}
        await message.answer("🎮 Вызов отправлен Cookie! Ожидайте ответа.")
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


# ────────── Healthcheck HTTP server (for platform keep-alive) ─────

async def handle_healthcheck(reader, writer):
    request = await reader.read(2048)
    if b"GET /health" in request or b"GET / " in request:
        resp = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 2\r\n"
            "Connection: close\r\n"
            "\r\n"
            "OK"
        )
    else:
        resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    writer.write(resp.encode())
    await writer.drain()
    writer.close()


async def run_healthcheck():
    server = await asyncio.start_server(
        handle_healthcheck, host="0.0.0.0", port=8080
    )
    logging.info("🩺 Healthcheck server on :8080")
    async with server:
        await server.serve_forever()


# ────────────────────────────── Entry ──────────────────────────────

async def main():
    healthcheck_task = asyncio.create_task(run_healthcheck())
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
