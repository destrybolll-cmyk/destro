import asyncio
import logging
import html
import os
import random
import time
from datetime import datetime

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

from config import BOT_TOKEN, ADMIN_ID
from database import Database

admin_pending_reply: int | None = None
write_flow_step: str | None = None
write_flow_anon_id: int | None = None
add_user_step: bool = False
rename_anon_id: int | None = None
waiting_messages: dict[int, int] = {}

TTT_CELL_EMPTY = "\u2b1c"
TTT_CELL_X = "\u274c"
TTT_CELL_O = "\u2b55"

GAME_KEYWORDS = {"игра", "ttt", "крестики", "нолики", "хочу играть", "поиграем", "давай поиграем", "сыграем"}

# Anti-spam tracking
user_last_msg: dict[int, float] = {}
user_spam_warnings: dict[int, int] = {}


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
        return "\U0001f383 <b>Стикер</b>"
    if message.photo:
        return "\U0001f4f7 <b>Фото</b>"
    if message.video:
        return "\U0001f3ac <b>Видео</b>"
    if message.animation:
        return "\U0001f99e <b>GIF</b>"
    if message.voice:
        return "\U0001f3a4 <b>Голосовое</b>"
    if message.video_note:
        return "\U0001f4f9 <b>Видеосообщение</b>"
    if message.audio:
        return "\U0001f3b5 <b>Аудио</b>"
    if message.document:
        return "\U0001f4ce <b>Документ</b>"
    if message.location:
        return "\U0001f4cd <b>Геопозиция</b>"
    if message.contact:
        return "\U0001f464 <b>Контакт</b>"
    if message.poll:
        return "\U0001f4ca <b>Опрос</b>"
    if message.dice:
        return "\U0001f3b2 <b>Dice</b>"
    if message.venue:
        return "\U0001f3db <b>Место</b>"
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
    if message.location:
        lat = message.location.latitude
        lon = message.location.longitude
        msg = await bot.send_location(chat_id, lat, lon, reply_markup=reply_markup)
        if caption:
            await bot.send_message(chat_id, caption_trunc)
        return msg
    if message.contact:
        c = message.contact
        await bot.send_contact(chat_id, c.phone_number, c.first_name, last_name=c.last_name or "", reply_markup=reply_markup)
        if caption:
            await bot.send_message(chat_id, caption_trunc)
        return
    if message.poll:
        p = message.poll
        await bot.send_message(chat_id, f"\U0001f4ca <b>Опрос:</b> {esc(p.question)}", reply_markup=reply_markup)
        return
    if message.dice:
        return await bot.send_dice(chat_id, emoji=message.dice.emoji, reply_markup=reply_markup)
    if message.venue:
        v = message.venue
        msg = await bot.send_venue(chat_id, v.location.latitude, v.location.longitude, v.title, v.address, reply_markup=reply_markup)
        if caption:
            await bot.send_message(chat_id, caption_trunc)
        return msg
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
db.add_user(ADMIN_ID, ADMIN_NAME, "", "")
ADMIN_ANON_ID: int = db.get_anon_id_by_user_id(ADMIN_ID) or 1


def esc(text: str) -> str:
    return html.escape(text or "")


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def row_get(row, key: str, default=0):
    try:
        val = row[key]
        return val if val is not None else default
    except (KeyError, IndexError, AttributeError):
        return default


def get_opponent_anon(game, my_anon: int) -> int:
    return game["player2_anon_id"] if game["player1_anon_id"] == my_anon else game["player1_anon_id"]


def get_opponent_user_id(game, my_anon: int) -> int:
    return game["player2_user_id"] if game["player1_anon_id"] == my_anon else game["player1_user_id"]


def user_actions_keyboard(anon_id: int, is_banned: bool = False, show_ttt: bool = False) -> InlineKeyboardBuilder:
    builder = InlineKeyboardBuilder()
    builder.button(text="\u270d\ufe0f Ответить", callback_data=f"reply:{anon_id}")
    builder.button(text="\U0001f194 Инфо", callback_data=f"info:{anon_id}")
    if is_banned:
        builder.button(text="\u2705 Разблокировать", callback_data=f"unban:{anon_id}")
    else:
        builder.button(text="\U0001f6ab Заблокировать", callback_data=f"ban:{anon_id}")
    builder.button(text="\U0001f5d1 Удалить", callback_data=f"del_ask:{anon_id}")
    if show_ttt:
        builder.button(text="\U0001f3ae Играть", callback_data=f"ttt_challenge:{anon_id}")
    builder.adjust(1)
    return builder


# ═══════════════════════ Tic-Tac-Toe ═══════════════════════

def ttt_render_cell(board: str, pos: int) -> str:
    ch = board[pos]
    if ch == "X":
        return TTT_CELL_X
    if ch == "O":
        return TTT_CELL_O
    return TTT_CELL_EMPTY


def ttt_build_keyboard(game, anon_id: int, can_move: bool) -> InlineKeyboardMarkup:
    board = game["board"]
    rows = []
    for r in range(3):
        row = []
        for c in range(3):
            pos = r * 3 + c
            cell = ttt_render_cell(board, pos)
            empty = board[pos] == "_"
            if empty and can_move:
                row.append(InlineKeyboardButton(text=cell, callback_data=f"ttt_move:{game['id']}:{pos}"))
            else:
                row.append(InlineKeyboardButton(text=cell, callback_data="none"))
        rows.append(row)
    if game["status"] == "active":
        rows.append([InlineKeyboardButton(text="\u26d4 Сдаюсь", callback_data=f"ttt_surrender:{game['id']}")])
    elif game["status"] == "finished":
        rematch_sent = row_get(game, "rematch_sent", 0)
        if rematch_sent:
            rows.append([InlineKeyboardButton(text="✅ Реванш запрошен", callback_data="none")])
        else:
            rows.append([InlineKeyboardButton(text="\U0001f504 Реванш", callback_data=f"ttt_rematch:{game['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def ttt_board_text(board: str) -> str:
    lines = []
    for r in range(3):
        cells = [ttt_render_cell(board, r * 3 + c) for c in range(3)]
        lines.append(f"  {cells[0]}  │  {cells[1]}  │  {cells[2]}")
        if r < 2:
            lines.append(" ─────┼─────┼─────")
    return "\n".join(lines)


def ttt_check_winner(board: str) -> str:
    wins = [
        (0,1,2), (3,4,5), (6,7,8),
        (0,3,6), (1,4,7), (2,5,8),
        (0,4,8), (2,4,6),
    ]
    for a, b, c in wins:
        if board[a] != "_" and board[a] == board[b] == board[c]:
            return board[a]
    if "_" not in board:
        return "draw"
    return ""


def ttt_game_message(game) -> str:
    board = game["board"]
    x_aid = game["x_player"]
    o_aid = game["player2_anon_id"] if game["player1_anon_id"] == x_aid else game["player1_anon_id"]
    x_name = f"#{x_aid}" if x_aid != ADMIN_ANON_ID else ADMIN_NAME
    o_name = f"#{o_aid}" if o_aid != ADMIN_ANON_ID else ADMIN_NAME
    turn_aid = game["current_turn"]
    turn_name = ADMIN_NAME if turn_aid == ADMIN_ANON_ID else f"#{turn_aid}"
    turn_mark = "X" if turn_aid == game["x_player"] else "O"

    lines = [
        f"\U0001f3ae <b>Крестики-нолики</b>\n"
        f"<b>X</b> {TTT_CELL_X} {x_name}    <b>O</b> {TTT_CELL_O} {o_name}",
        "",
        ttt_board_text(board),
    ]
    if game["status"] == "active":
        turn_emoji = TTT_CELL_X if turn_mark == "X" else TTT_CELL_O
        lines.extend(["", f"<b>\U0001f449 Ход: {turn_name}</b> ({turn_emoji})"])
    elif game["status"] == "finished":
        w = game["winner"]
        if w == "draw":
            lines.extend(["", "<b>\U0001f91d Ничья!</b>"])
        else:
            w_aid = game["x_player"] if w == "X" else o_aid
            w_name = ADMIN_NAME if w_aid == ADMIN_ANON_ID else f"#{w_aid}"
            w_emoji = TTT_CELL_X if w == "X" else TTT_CELL_O
            lines.extend(["", f"<b>\U0001f3c6 Победил: {w_name}</b> {w_emoji}"])
    return "\n".join(lines)


def ttt_game_list(page: int = 1):
    users = [u for u in db.get_all_users() if u["id"] != ADMIN_ANON_ID]
    if not users:
        return "\U0001f4ad Нет пользователей для игры.", None
    total_pages = max(1, (len(users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = users[start:end]

    lines = [f"\U0001f3ae <b>Выбери соперника</b> (стр. {page}/{total_pages}):\n"]
    for u in page_users:
        name = esc(u["first_name"] or "—")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        lines.append(f"\U0001f539 #<b>{u['id']}</b> — {name}{uname}")
    text = "\n".join(lines)

    rows = []
    for u in page_users:
        label = (u["first_name"] or f"#{u['id']}")[:14]
        rows.append([InlineKeyboardButton(text=f"\U0001f3ae {label}", callback_data=f"ttt_challenge:{u['id']}")])

    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"ttt_pgn:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"\U0001f4c4 {page}/{total_pages}", callback_data="none"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"ttt_pgn:{page + 1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="\U0001f4ca Моя статистика", callback_data="ttt_my_stats")])

    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def ttt_send_board(game):
    p1_uid = game["player1_user_id"]
    p2_uid = game["player2_user_id"]
    p1_aid = game["player1_anon_id"]
    p2_aid = game["player2_anon_id"]
    is_admin_p1 = p1_aid == ADMIN_ANON_ID
    admin_uid = p1_uid if is_admin_p1 else p2_uid
    user_uid = p2_uid if is_admin_p1 else p1_uid
    admin_aid = p1_aid if is_admin_p1 else p2_aid
    user_aid = p2_aid if is_admin_p1 else p1_aid
    admin_can_move = game["current_turn"] == admin_aid and game["status"] == "active"
    user_can_move = game["current_turn"] == user_aid and game["status"] == "active"
    text = ttt_game_message(game)
    admin_kb = ttt_build_keyboard(game, admin_aid, admin_can_move)
    user_kb = ttt_build_keyboard(game, user_aid, user_can_move)
    gid = game["id"]
    admin_last = row_get(game, "admin_msg_id", 0)
    user_last = row_get(game, "user_msg_id", 0)

    async def send_or_edit(uid, last_id, kb, col):
        if last_id:
            try:
                await bot.edit_message_text(text, uid, last_id, reply_markup=kb)
                return
            except Exception:
                try:
                    await bot.delete_message(uid, last_id)
                except Exception:
                    pass
        msg = await bot.send_message(uid, text, reply_markup=kb)
        db.update_game(gid, **{col: msg.message_id})

    await send_or_edit(admin_uid, admin_last, admin_kb, "admin_msg_id")
    await send_or_edit(user_uid, user_last, user_kb, "user_msg_id")


async def ttt_start_game(game_id: int):
    game = db.get_game(game_id)
    if not game or game["status"] != "pending":
        return
    if game["x_player"] is None:
        x_player = random.choice([game["player1_anon_id"], game["player2_anon_id"]])
    else:
        x_player = game["x_player"]
    db.update_game(game_id, status="active", x_player=x_player, current_turn=x_player, board="_________")
    game = db.get_game(game_id)
    await ttt_send_board(game)


async def ttt_process_move(game_id: int, anon_id: int, position: int):
    game = db.get_game(game_id)
    if not game or game["status"] != "active":
        return False, "Игра уже завершена."
    if game["current_turn"] != anon_id:
        return False, "Сейчас не ваш ход."
    board = list(game["board"])
    if board[position] != "_":
        return False, "Ячейка уже занята."
    mark = "X" if anon_id == game["x_player"] else "O"
    board[position] = mark
    new_board = "".join(board)
    winner = ttt_check_winner(new_board)
    if winner:
        result = "finished"
        db.update_game(game_id, board=new_board, status=result, winner=winner, finished_at=datetime.now().isoformat())
        game = db.get_game(game_id)
        await ttt_send_board(game)
        return True, None
    next_turn = game["player2_anon_id"] if game["current_turn"] == game["player1_anon_id"] else game["player1_anon_id"]
    db.update_game(game_id, board=new_board, current_turn=next_turn)
    game = db.get_game(game_id)
    await ttt_send_board(game)
    return True, None


# ═══════════════════════ Dice Game (Везение) ═══════════════════════

def dice_game_list(page: int = 1):
    users = [u for u in db.get_all_users() if u["id"] != ADMIN_ANON_ID]
    if not users:
        return "\U0001f4ad Нет пользователей для игры.", None
    total_pages = max(1, (len(users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = users[start:end]
    lines = [f"\U0001f3b2 <b>Выбери соперника</b> (стр. {page}/{total_pages}):\n"]
    for u in page_users:
        name = esc(u["first_name"] or "\u2014")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        lines.append(f"\U0001f539 #<b>{u['id']}</b> \u2014 {name}{uname}")
    text = "\n".join(lines)
    rows = []
    for u in page_users:
        label = (u["first_name"] or f"#{u['id']}")[:14]
        rows.append([InlineKeyboardButton(text=f"\U0001f3b2 {label}", callback_data=f"dice_challenge:{u['id']}")])
    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"dice_pgn:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"\U0001f4c4 {page}/{total_pages}", callback_data="none"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"dice_pgn:{page + 1}"))
        rows.append(nav)
    rows.append([InlineKeyboardButton(text="\U0001f4ca Моя статистика", callback_data="dice_my_stats")])
    return text, InlineKeyboardMarkup(inline_keyboard=rows)


async def dice_play_game(game_id: int, anon_id: int, user_uid: int):
    p1_score = random.randint(1, 6) + random.randint(1, 6)
    p2_score = random.randint(1, 6) + random.randint(1, 6)
    db.finish_dice_game(game_id, p1_score, p2_score)
    game_info = db.get_dice_stats(anon_id)
    p1_name = ADMIN_NAME if ADMIN_ANON_ID == anon_id else f"#{anon_id}"
    p2_name = ADMIN_NAME if ADMIN_ANON_ID != anon_id else f"#{anon_id}"
    lines = [
        f"\U0001f3b2 <b>Везение!</b>\n",
        f"<b>{p1_name}</b> \U0001f3b2: {p1_score // 2} + {p1_score - p1_score // 2} = <b>{p1_score}</b>",
        f"<b>{p2_name}</b> \U0001f3b2: {p2_score // 2} + {p2_score - p2_score // 2} = <b>{p2_score}</b>",
        "",
    ]
    if p1_score > p2_score:
        lines.append(f"\U0001f3c6 <b>Победил: {p1_name}</b>")
    elif p2_score > p1_score:
        lines.append(f"\U0001f3c6 <b>Победил: {p2_name}</b>")
    else:
        lines.append("\U0001f91d <b>Ничья!</b>")
    text = "\n".join(lines)
    admin_uid = ADMIN_ID
    try:
        await bot.send_message(admin_uid, text)
        await bot.send_message(user_uid, text)
    except Exception:
        pass


# ────────────────────────────── Users ──────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: Message):
    if is_admin(message.from_user.id):
        db.add_user(
            message.from_user.id,
            message.from_user.first_name or ADMIN_NAME,
            message.from_user.username or "",
            message.from_user.language_code or "",
        )
        await message.answer(
            f"\U0001f36a <b>Добро пожаловать, {ADMIN_NAME}!</b>\n\n"
            "Бот для анонимных сообщений запущен.\n"
            "Пользователи пишут боту \u2014 ты видишь их сообщения "
            "с анонимным ID и можешь отвечать.\n\n"
            "Нажимай <b>\u270d\ufe0f Ответить</b> под сообщением \u2014 "
            "и просто пиши текст, без команд!\n\n"
            "\U0001f4dc <b>История</b> \u2014 все сообщения за последний час.\n\n"
            "\U0001f3ae <b>Крестики-нолики</b> \u2014 играй с пользователями!\n\n"
            "\U0001f4cc Кнопки внизу \u2014 быстрый доступ ко всем функциям.",
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
        "\U0001f44b <b>Привет! Я анонимный бот.</b>\n\n"
        "Ты можешь написать мне любое сообщение, "
        f"и я передам его <b>{ADMIN_NAME}</b> <b>анонимно</b>.\n"
        "Никто не узнает твой Telegram ID или личные данные.\n\n"
        f"Твой анонимный номер: <b>#{anon_id}</b>\n\n"
        "Просто напиши что-нибудь ниже \u2709\ufe0f\n\n"
        "\U0001f3ae <b>Хочешь сыграть в крестики-нолики?</b>\n"
        "Напиши \u00abигра\u00bb или \u00abttt\u00bb и я передам вызов!",
    )


@dp.message(Command("banned"))
async def cmd_banned_cmd(message: Message):
    await cmd_banned(message)


@dp.message(Command("find"))
async def cmd_find(message: Message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("❌ Используй: <code>/find @username</code>")
        return
    target = parts[1].strip().lstrip("@")
    if not target:
        await message.answer("❌ Укажи юзернейм.")
        return
    try:
        chat = await message.bot.get_chat(f"@{target}")
        uid = chat.id
        existing = db.get_user(uid)
        if existing:
            anon_id = existing["id"]
            await message.answer(
                f"ℹ️ Пользователь @{esc(target)} уже в БД.\n"
                f"🆔 Анонимный ID: <b>#{anon_id}</b>\n"
                f"👤 {esc(chat.first_name or '\u2014')}"
            )
        else:
            anon_id, _ = db.add_user(uid, chat.first_name or "", chat.username or "", chat.language_code or "")
            await message.answer(
                f"✅ Пользователь @{esc(target)} добавлен!\n"
                f"🆔 Анонимный ID: <b>#{anon_id}</b>\n"
                f"👤 {esc(chat.first_name or '\u2014')}"
            )
    except Exception as e:
        await message.answer(f"❌ Не удалось найти @{esc(target)}: {esc(str(e)[:200])}")


@dp.message(Command("help"))
async def cmd_help(message: Message):
    if is_admin(message.from_user.id):
        await message.answer(
            f"\U0001f527 <b>Команды {ADMIN_NAME}:</b>\n\n"
            "\u270d\ufe0f <b>Написать</b> \u2014 выбрать пользователя и написать\n\n"
            "\U0001f4dc <b>История</b> \u2014 все сообщения за последний час\n"
            "   <code>/history 30</code> \u2014 за 30 минут\n\n"
            "\U0001f4ca <b>Статистика</b> \u2014 статистика бота\n\n"
            "\U0001f464 <b>Список</b> \u2014 список пользователей\n\n"
            "\u2795 <b>Добавить ID</b> \u2014 добавить пользователя по Telegram ID\n\n"
            "\U0001f50d <code>/find @user</code> \u2014 найти/добавить по юзернейму\n\n"
            "\U0001f4e2 <b>Рассылка</b> \u2014 написать всем\n\n"
            "\U0001f3ae <b>Крестики-нолики</b> \u2014 игра с пользователями\n\n"
            "\u274c <b>Отмена</b> \u2014 отменить текущее действие",
            reply_markup=admin_cmds_keyboard(),
        )
    else:
        await message.answer(
            f"\U0001f916 <b>Как это работает</b>\n\n"
            "1. Ты отправляешь мне сообщение\n"
            f"2. Я анонимно передаю его <b>{ADMIN_NAME}</b>\n"
            f"3. <b>{ADMIN_NAME}</b> может ответить\n"
            "4. Я передаю ответ тебе\n\n"
            "Всё полностью анонимно \U0001f512\n\n"
            "\U0001f3ae Напиши <b>игра</b> или <b>ttt</b> чтобы сыграть в крестики-нолики!"
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
    # Cancel any pending TTT challenge
    pending = db.get_player_game(ADMIN_ANON_ID, statuses=("pending",))
    if pending:
        db.update_game(pending["id"], status="cancelled")
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
            f"\u2709\ufe0f <b>Ответ от {ADMIN_NAME}:</b>\n\n{esc(reply_text)}",
        )
        db.save_message(target_user_id, anon_id, reply_text, direction="admin_to_user")
        await message.answer(f"✅ Ответ отправлен пользователю #{anon_id}!")
    except Exception as e:
        err = str(e).lower()
        if "forbidden" in err:
            await message.answer("❌ Пользователь ограничил получение этого типа сообщений.")
        else:
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
    await message.answer(f"\U0001f6ab Пользователь #{anon_id} заблокирован.")


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
    ttt_stats = db.get_player_stats(ADMIN_ANON_ID)
    dice_stats = db.get_dice_stats(ADMIN_ANON_ID)
    await message.answer(
        f"\U0001f4ca <b>Статистика бота</b>\n\n"
        f"👥 Всего: <b>{stats['total']}</b>\n"
        f"\U0001f6ab Заблокировано: <b>{stats['banned']}</b>\n"
        f"✅ Активных: <b>{stats['active']}</b>\n"
        f"\U0001f5d1 Удалённых: <b>{stats['deleted']}</b>\n\n"
        f"\U0001f3ae <b>Крестики-нолики</b>\n"
        f"🏆 Побед: <b>{ttt_stats['wins']}</b>\n"
        f"\U0001f4a5 Поражений: <b>{ttt_stats['losses']}</b>\n"
        f"\U0001f91d Ничьих: <b>{ttt_stats['draws']}</b>\n"
        f"👥 Соперников: <b>{ttt_stats['opponents']}</b>\n\n"
        f"\U0001f3b2 <b>Везение</b>\n"
        f"🏆 Побед: <b>{dice_stats['wins']}</b>\n"
        f"\U0001f4a5 Поражений: <b>{dice_stats['losses']}</b>\n"
        f"\U0001f91d Ничьих: <b>{dice_stats['draws']}</b>\n"
        f"👥 Соперников: <b>{dice_stats['opponents']}</b>"
    )


def paginated_users_list(page: int = 1, action: str = "wrt", nav_prefix: str = "pgn"):
    users = db.get_all_users()
    if not users:
        return "\U0001f4ad Нет пользователей.", None
    total_pages = max(1, (len(users) + USERS_PER_PAGE - 1) // USERS_PER_PAGE)
    page = max(1, min(page, total_pages))
    start = (page - 1) * USERS_PER_PAGE
    end = start + USERS_PER_PAGE
    page_users = users[start:end]
    is_del_mode = action == "del_ask"
    action_icon = "\U0001f5d1" if is_del_mode else "\u270d\ufe0f"
    title = "\U0001f5d1 <b>Удаление пользователей</b>" if is_del_mode else "\U0001f464 <b>Пользователи</b>"
    lines = [f"{title} (стр. {page}/{total_pages}):\n"]
    for u in page_users:
        ban_icon = "\U0001f6ab" if u["is_banned"] else "✅"
        name = esc(u["first_name"] or "\u2014")
        username = f" @{esc(u['username'])}" if u["username"] else ""
        lines.append(f"{ban_icon} #<b>{u['id']}</b> \u2014 {name}{username}")
    text = "\n".join(lines)
    rows = []
    for u in page_users:
        label = (u["first_name"] or f"#{u['id']}")[:14]
        row = [InlineKeyboardButton(text=f"{action_icon} {label}", callback_data=f"{action}:{u['id']}")]
        if not is_del_mode:
            row.append(InlineKeyboardButton(text="\U0001f5d1", callback_data=f"del_ask:{u['id']}"))
        rows.append(row)
    if total_pages > 1:
        nav = []
        if page > 1:
            nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"{nav_prefix}:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"\U0001f4c4 {page}/{total_pages}", callback_data="none"))
        if page < total_pages:
            nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"{nav_prefix}:{page + 1}"))
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
        return "\U0001f6ab <b>Нет заблокированных пользователей.</b>", None
    kb = InlineKeyboardBuilder()
    for u in users:
        name = esc(u["first_name"] or "\u2014")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        kb.button(text=f"#{u['id']} \u2014 {name}{uname}", callback_data=f"info:{u['id']}")
    kb.adjust(1)
    return (
        f"\U0001f6ab <b>Заблокированные пользователи</b> ({len(users)}):\n\n"
        "Нажми на пользователя, чтобы управлять им.",
        kb.as_markup(),
    )


async def cmd_banned(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = banned_users_list()
    await message.answer(text, reply_markup=markup)


def blocked_users_list():
    users = db.get_blocked_users()
    if not users:
        return "\U0001f6ab <b>Нет пользователей, которые заблокировали бота.</b>", None
    kb = InlineKeyboardBuilder()
    for u in users:
        name = esc(u["first_name"] or "\u2014")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        kb.button(text=f"#{u['id']} \u2014 {name}{uname}", callback_data=f"unblock:{u['id']}")
    kb.adjust(1)
    return (
        f"\U0001f6ab <b>Пользователи, которые заблокировали бота</b> ({len(users)}):\n\n"
        "Нажми на пользователя, чтобы снять пометку.",
        kb.as_markup(),
    )


async def cmd_blocked(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = blocked_users_list()
    await message.answer(text, reply_markup=markup)


def deleted_users_list():
    users = db.get_deleted_users()
    if not users:
        return "\U0001f5d1 <b>Нет удалённых пользователей.</b>", None
    kb = InlineKeyboardBuilder()
    for u in users:
        name = esc(u["first_name"] or "\u2014")
        uname = f" @{esc(u['username'])}" if u["username"] else ""
        kb.button(text=f"#{u['id']} \u2014 {name}{uname}", callback_data=f"info:{u['id']}")
    kb.adjust(1)
    return (
        f"\U0001f5d1 <b>Удалённые пользователи</b> ({len(users)}):\n\n"
        "Нажми на пользователя, чтобы управлять им.",
        kb.as_markup(),
    )


async def cmd_deleted(message: Message):
    if not is_admin(message.from_user.id):
        return
    text, markup = deleted_users_list()
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
    name = esc(u["first_name"] or "\u2014")
    username = f"@{esc(u['username'])}" if u["username"] else "\u2014"
    lang = u["language_code"] or "\u2014"
    created = u["created_at"][:16] if u["created_at"] else "\u2014"
    last = u["last_active"][:16] if u["last_active"] else "\u2014"
    is_del = row_get(u, "is_deleted")
    if is_del:
        status = "\U0001f5d1 Удалён"
    elif u["is_banned"]:
        status = "\U0001f6ab Заблокирован"
    else:
        status = "✅ Активен"
    text = (
        f"👤 <b>Пользователь #{anon_id}</b>\n\n"
        f"👤 Имя: {name}\n"
        f"\U0001f517 Юзернейм: {username}\n"
        f"\U0001f310 Язык: {lang}\n"
        f"\U0001f4c5 Создан: {created}\n"
        f"\U0001f550 Активен: {last}\n"
        f"\U0001f4cc Статус: {status}"
    )
    kb = InlineKeyboardBuilder()
    if is_del:
        kb.button(text="✅ Восстановить", callback_data=f"restore:{anon_id}")
        kb.button(text="\U0001f5d1 Удалить навсегда", callback_data=f"hard_del_ask:{anon_id}")
    else:
        kb.button(text="\u270d\ufe0f Написать", callback_data=f"reply:{anon_id}")
        kb.button(text="\u270f\ufe0f Изменить имя", callback_data=f"rename:{anon_id}")
        if u["is_banned"]:
            kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
        else:
            kb.button(text="\U0001f6ab Заблокировать", callback_data=f"ban:{anon_id}")
        kb.button(text="\U0001f5d1 Удалить", callback_data=f"del_ask:{anon_id}")
        kb.button(text="\U0001f3ae Играть", callback_data=f"ttt_challenge:{anon_id}")
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
    status = await message.answer("\U0001f4e2 Рассылка началась...")
    for u in users:
        if u["is_banned"]:
            continue
        try:
            await bot.send_message(
                u["user_id"],
                f"\U0001f4e2 <b>Сообщение от {ADMIN_NAME}:</b>\n\n{esc(text)}",
            )
            sent += 1
            await asyncio.sleep(0.03)
        except Exception as be:
            failed += 1
            err_lower = str(be).lower()
            if "chat not found" in err_lower or "bot was blocked" in err_lower:
                db.mark_blocked(u["user_id"])
    await status.edit_text(
        f"\U0001f4e2 <b>Рассылка завершена</b>\n\n"
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
        await message.answer(f"\U0001f4ad За последние <b>{minutes}</b> мин сообщений нет.")
        return
    lines = [f"\U0001f4dc <b>Сообщения за последние {minutes} мин:</b>\n"]
    current_id = None
    for r in rows:
        if r["anon_id"] != current_id:
            current_id = r["anon_id"]
            name = esc(r["first_name"] or f"#{current_id}")
            username = f" @{esc(r['username'])}" if r["username"] else ""
            time = r["timestamp"][11:16] if r["timestamp"] else ""
            lines.append(f"\n👤 #{current_id} \u2014 {name}{username} ({time}):")
        lines.append(f'  "{esc(r["text"] or "")}"')
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n<i>...сообщение обрезано</i>"
    await message.answer(text)


# ─────────────────────────── Callback queries ──────────────────────────

@dp.callback_query()
async def handle_callback(callback: CallbackQuery):
    global admin_pending_reply, write_flow_step, write_flow_anon_id, rename_anon_id

    try:
        await _handle_callback(callback)
    except Exception as e:
        logging.error(f"Callback error: {e}", exc_info=True)
        try:
            await callback.answer("❌ Произошла ошибка.", show_alert=True)
        except Exception:
            pass
        if is_admin(callback.from_user.id):
            try:
                await bot.send_message(ADMIN_ID, f"❌ Ошибка в обработчике: {esc(str(e)[:200])}")
            except Exception:
                pass


async def _handle_callback(callback: CallbackQuery):
    global admin_pending_reply, write_flow_step, write_flow_anon_id, rename_anon_id

    parts = callback.data.split(":")
    action = parts[0]

    if not is_admin(callback.from_user.id):
        if action in ("ttt_accept", "ttt_decline", "ttt_move", "ttt_surrender", "ttt_rematch",
                       "appeal", "appeal_accept", "appeal_decline",
                       "dice_accept", "dice_decline", "dice_my_stats", "dice_pgn"):
            pass
        else:
            await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
            return

    # ── TTT callbacks (work for both admin and user) ──

    if action == "ttt_challenge":
        if not is_admin(callback.from_user.id):
            await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
            return
        anon_id = int(parts[1])
        if anon_id == ADMIN_ANON_ID:
            await callback.answer("❌ Нельзя играть с самим собой.", show_alert=True)
            return
        admin_game = db.get_player_game(ADMIN_ANON_ID)
        if admin_game:
            db.update_game(admin_game["id"], status="cancelled")
            await bot.send_message(ADMIN_ID, f"♻️ Предыдущая игра отменена.")
        user_game = db.get_player_game(anon_id)
        if user_game:
            await callback.answer("❌ Пользователь уже в игре с другим человеком.", show_alert=True)
            return
        user = db.get_user_by_anon(anon_id)
        if not user or row_get(user, "is_deleted"):
            await callback.answer("❌ Пользователь не найден.", show_alert=True)
            return
        target_user_id = user["user_id"]
        game_id = db.create_game(ADMIN_ANON_ID, anon_id, ADMIN_ID, target_user_id, None)
        await callback.answer()
        await bot.send_message(
            ADMIN_ID,
            "Ожидайте ответа от пользователя..."
        )
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"ttt_accept:{game_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ttt_decline:{game_id}"),
            ]
        ])
        try:
            await bot.send_message(
                target_user_id,
                f"\U0001f3ae <b>{ADMIN_NAME} бросил вам вызов в крестики нолики!</b>\n\n"
                f"Нажми <b>✅ Принять</b> чтобы сыграть.\n"
                f"Нажми <b>❌ Отклонить</b> чтобы отказаться.",
                reply_markup=accept_kb,
            )
        except Exception as ce:
            db.mark_blocked(target_user_id)
            db.update_game(game_id, status="cancelled")
            err_msg = str(ce).lower()
            if "chat not found" in err_msg or "bot was blocked" in err_msg:
                await bot.send_message(ADMIN_ID, f"❌ Пользователь #{anon_id} заблокировал бота. Вызов отменён.")
            else:
                await bot.send_message(ADMIN_ID, f"❌ Не удалось отправить вызов пользователю #{anon_id}: {esc(str(ce)[:100])}")
        return

    elif action == "ttt_accept":
        game_id = int(parts[1])
        game = db.get_game(game_id)
        if not game or game["status"] != "pending":
            await callback.answer("❌ Вызов уже недействителен.", show_alert=True)
            return
        await callback.answer("✅ Вызов принят!")
        try:
            await callback.message.delete()
        except Exception:
            pass
        admin_game = db.get_player_game(ADMIN_ANON_ID, statuses=("pending", "active"))
        if admin_game and admin_game["id"] != game_id:
            db.update_game(game_id, status="cancelled")
            await callback.message.answer(
                "❌ Cookie уже в игре с другим человеком.\nВызов отклонён."
            )
            return
        await bot.send_message(ADMIN_ID, f"✅ Пользователь #<b>{game['player2_anon_id']}</b> принял вызов!\nИгра начинается...")
        await ttt_start_game(game_id)
        return

    elif action == "ttt_decline":
        game_id = int(parts[1])
        game = db.get_game(game_id)
        if game and game["status"] == "pending":
            db.update_game(game_id, status="declined")
        await callback.answer("❌ Вызов отклонён.")
        try:
            await callback.message.delete()
        except Exception:
            pass
        if is_admin(callback.from_user.id):
            return
        await bot.send_message(
            ADMIN_ID,
            f"❌ Пользователь #<b>{game['player2_anon_id']}</b> отклонил вызов.\n"
            "Можешь попробовать снова или выбрать другого соперника."
        )
        return

    elif action == "ttt_move":
        game_id = int(parts[1])
        pos = int(parts[2])
        anon_id = db.get_anon_id_by_user_id(callback.from_user.id)
        if anon_id is None:
            await callback.answer("❌ Вы не зарегистрированы.", show_alert=True)
            return
        success, err = await ttt_process_move(game_id, anon_id, pos)
        if success:
            await callback.answer()
        else:
            await callback.answer()
            try:
                await callback.message.answer(f"❌ {err}")
            except Exception:
                pass
        return

    elif action == "ttt_surrender":
        game_id = int(parts[1])
        game = db.get_game(game_id)
        if not game or game["status"] != "active":
            await callback.answer("❌ Игра уже завершена.", show_alert=True)
            return
        anon_id = db.get_anon_id_by_user_id(callback.from_user.id)
        if anon_id is None:
            await callback.answer("❌ Ошибка.", show_alert=True)
            return
        opp_anon = get_opponent_anon(game, anon_id)
        winner_mark = "O" if anon_id == game["x_player"] else "X"
        db.update_game(game_id, status="finished", winner=winner_mark, finished_at=datetime.now().isoformat())
        game = db.get_game(game_id)
        await callback.answer("Вы сдались.")
        await ttt_send_board(game)
        return

    elif action == "ttt_rematch":
        game_id = int(parts[1])
        old_game = db.get_game(game_id)
        if not old_game:
            await callback.answer("❌ Игра не найдена.", show_alert=True)
            return
        if row_get(old_game, "rematch_sent", 0):
            await callback.answer("❌ Реванш можно кинуть только один раз за игру.", show_alert=True)
            return
        await callback.answer()
        db.update_game(game_id, rematch_sent=1)
        old_game = db.get_game(game_id)
        await ttt_send_board(old_game)
        p1_aid = old_game["player1_anon_id"]
        p2_aid = old_game["player2_anon_id"]
        p1_uid = old_game["player1_user_id"]
        p2_uid = old_game["player2_user_id"]
        # Alternate X/O: opposite of who was X last game
        old_x = old_game["x_player"]
        next_x = p2_aid if old_x == p1_aid else p1_aid
        new_id = db.create_game(p1_aid, p2_aid, p1_uid, p2_uid, next_x)
        admin_game = db.get_player_game(ADMIN_ANON_ID, statuses=("pending", "active"))
        if admin_game and admin_game["id"] != new_id:
            db.update_game(new_id, status="cancelled")
            await callback.message.answer("❌ Вы уже в другой игре.")
            return
        player_anon = db.get_anon_id_by_user_id(callback.from_user.id)
        if player_anon is None:
            return
        opp_uid = get_opponent_user_id(old_game, player_anon)
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"ttt_accept:{new_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ttt_decline:{new_id}"),
            ]
        ])
        try:
            await bot.send_message(
                opp_uid,
                f"\U0001f3ae <b>Реванш!</b>\n\n"
                f"Противник хочет сыграть снова!",
                reply_markup=accept_kb,
            )
        except Exception:
            pass
        await callback.message.answer("✅ Запрос на реванш отправлен.")
        return

    elif action == "ttt_pgn":
        page = int(parts[1])
        await callback.answer()
        text, markup = ttt_game_list(page)
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)
        return

    elif action == "ttt_my_stats":
        if not is_admin(callback.from_user.id):
            await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
            return
        await callback.answer()
        stats = db.get_player_stats(ADMIN_ANON_ID)
        await callback.message.answer(
            f"\U0001f4ca <b>Моя статистика (Крестики-нолики)</b>\n\n"
            f"\U0001f3c6 Побед: <b>{stats['wins']}</b>\n"
            f"\U0001f4a5 Поражений: <b>{stats['losses']}</b>\n"
            f"\U0001f91d Ничьих: <b>{stats['draws']}</b>\n"
            f"📊 Всего игр: <b>{stats['total']}</b>\n"
            f"👥 Соперников: <b>{stats['opponents']}</b>"
        )
        return

    elif action == "dice_challenge":
        if not is_admin(callback.from_user.id):
            await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
            return
        anon_id = int(parts[1])
        if anon_id == ADMIN_ANON_ID:
            await callback.answer("❌ Нельзя играть с самим собой.", show_alert=True)
            return
        user = db.get_user_by_anon(anon_id)
        if not user or row_get(user, "is_deleted"):
            await callback.answer("❌ Пользователь не найден.", show_alert=True)
            return
        target_user_id = user["user_id"]
        game_id = db.create_dice_game(ADMIN_ANON_ID, anon_id)
        await callback.answer()
        await bot.send_message(ADMIN_ID, "Ожидайте ответа от пользователя...")
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Кинуть кубик!", callback_data=f"dice_accept:{game_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"dice_decline:{game_id}"),
            ]
        ])
        try:
            await bot.send_message(
                target_user_id,
                f"\U0001f3b2 <b>{ADMIN_NAME} бросил вам вызов в Везение!</b>\n\n"
                f"Нажми <b>✅ Кинуть кубик!</b> чтобы сыграть.",
                reply_markup=accept_kb,
            )
        except Exception as ce:
            err_msg = str(ce).lower()
            if "chat not found" in err_msg or "bot was blocked" in err_msg:
                await bot.send_message(ADMIN_ID, f"❌ Пользователь #{anon_id} заблокировал бота.")
            else:
                await bot.send_message(ADMIN_ID, f"❌ Не удалось отправить вызов: {esc(str(ce)[:100])}")
        return

    elif action == "dice_accept":
        game_id = int(parts[1])
        await callback.answer("\U0001f3b2 Кидаем кубики!")
        try:
            await callback.message.delete()
        except Exception:
            pass
        if is_admin(callback.from_user.id):
            anon_id = ADMIN_ANON_ID
        else:
            anon_id = db.get_anon_id_by_user_id(callback.from_user.id)
        if anon_id is None:
            return
        user_id = callback.from_user.id
        opp_uid = ADMIN_ID if user_id == ADMIN_ID else db.get_user_id_by_anon(anon_id)
        if opp_uid is None:
            return
        await dice_play_game(game_id, anon_id, opp_uid)

    elif action == "dice_decline":
        game_id = int(parts[1])
        await callback.answer("❌ Вызов отклонён.")
        try:
            await callback.message.delete()
        except Exception:
            pass
        if not is_admin(callback.from_user.id):
            await bot.send_message(ADMIN_ID, "❌ Пользователь отклонил вызов в Везение.")

    elif action == "dice_pgn":
        page = int(parts[1])
        await callback.answer()
        text, markup = dice_game_list(page)
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)
        return

    elif action == "dice_my_stats":
        if not is_admin(callback.from_user.id):
            await callback.answer(f"❌ Только для {ADMIN_NAME}.", show_alert=True)
            return
        await callback.answer()
        stats = db.get_dice_stats(ADMIN_ANON_ID)
        await callback.message.answer(
            f"\U0001f4ca <b>Моя статистика (Везение)</b>\n\n"
            f"\U0001f3c6 Побед: <b>{stats['wins']}</b>\n"
            f"\U0001f4a5 Поражений: <b>{stats['losses']}</b>\n"
            f"\U0001f91d Ничьих: <b>{stats['draws']}</b>\n"
            f"📊 Всего игр: <b>{stats['total']}</b>\n"
            f"👥 Соперников: <b>{stats['opponents']}</b>"
        )
        return

    elif action == "appeal":
        if is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        allowed, remaining = db.check_appeal_limit(anon_id)
        if not allowed:
            await callback.answer("❌ Лимит апелляций: 3 в час. Попробуйте позже.", show_alert=True)
            return
        await callback.answer()
        db.increment_appeal(anon_id)
        u = db.get_user_by_anon(anon_id)
        if not u:
            return
        name = esc(u["first_name"] or "—")
        username = f" @{esc(u['username'])}" if u["username"] else ""
        appeal_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Разблокировать", callback_data=f"appeal_accept:{anon_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"appeal_decline:{anon_id}"),
            ]
        ])
        await bot.send_message(
            ADMIN_ID,
            f"👤 <b>Апелляция на разблокировку</b>\n\n"
            f"🆔 Анонимный ID: <b>#{anon_id}</b>\n"
            f"👤 {name}{username}\n\n"
            "Пользователь просит разблокировать его.",
            reply_markup=appeal_kb,
        )
        try:
            await bot.send_message(
                u["user_id"],
                "\U0001f36a <b>Cookie рассматривает вашу апелляцию.</b>\n\n"
                "Ожидайте ответа."
            )
        except Exception:
            pass
        return

    elif action == "appeal_accept":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        uid = db.get_user_id_by_anon(anon_id)
        if uid:
            db.unban_user(uid)
        await callback.answer("✅ Пользователь разблокирован.")
        await callback.message.edit_text(
            f"✅ Пользователь #<b>{anon_id}</b> разблокирован."
        )
        try:
            await bot.send_message(
                uid,
                "\u2705 <b>Cookie одобрил вашу апелляцию!</b>\n\n"
                "Вы разблокированы и снова можете писать."
            )
        except Exception:
            pass
        return

    elif action == "appeal_decline":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        await callback.answer("❌ Апелляция отклонена.")
        await callback.message.edit_text(
            f"❌ Апелляция пользователя #<b>{anon_id}</b> отклонена."
        )
        uid = db.get_user_id_by_anon(anon_id)
        if uid:
            try:
                await bot.send_message(
                    uid,
                    "\u274c <b>Cookie отклонил вашу апелляцию.</b>\n\n"
                    "Вы остаётесь заблокированным."
                )
            except Exception:
                pass
        return

    elif action == "none":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await callback.answer()
        return

    # ── Admin-only callbacks ──

    if not is_admin(callback.from_user.id):
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
            f"\u270f\ufe0f <b>Введите текст</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Отправь сообщение \u2014 оно будет доставлено.\n"
            "/cancel \u2014 отменить",
            reply_markup=admin_cmds_keyboard(),
        )

    elif action == "info":
        await callback.answer()
        await delete_waiting(target_user_id)
        u = db.get_user_by_anon(anon_id)
        if u is None:
            await callback.message.answer("❌ Пользователь не найден.")
            return
        name = esc(u["first_name"] or "\u2014")
        username = f"@{esc(u['username'])}" if u["username"] else "\u2014"
        lang = u["language_code"] or "\u2014"
        created = u["created_at"][:16] if u["created_at"] else "\u2014"
        last = u["last_active"][:16] if u["last_active"] else "\u2014"
        is_del = row_get(u, "is_deleted")
        if is_del:
            status = "\U0001f5d1 Удалён"
        elif u["is_banned"]:
            status = "\U0001f6ab Заблокирован"
        else:
            status = "✅ Активен"
        text = (
            f"👤 <b>Пользователь #{anon_id}</b>\n\n"
            f"👤 Имя: {name}\n"
            f"\U0001f517 Юзернейм: {username}\n"
            f"\U0001f310 Язык: {lang}\n"
            f"\U0001f4c5 Создан: {created}\n"
            f"\U0001f550 Активен: {last}\n"
            f"\U0001f4cc Статус: {status}"
        )
        kb = InlineKeyboardBuilder()
        if is_del:
            kb.button(text="✅ Восстановить", callback_data=f"restore:{anon_id}")
            kb.button(text="\U0001f5d1 Удалить навсегда", callback_data=f"hard_del_ask:{anon_id}")
        else:
            kb.button(text="\u270d\ufe0f Написать", callback_data=f"reply:{anon_id}")
            kb.button(text="\u270f\ufe0f Изменить имя", callback_data=f"rename:{anon_id}")
            if u["is_banned"]:
                kb.button(text="✅ Разблокировать", callback_data=f"unban:{anon_id}")
            else:
                kb.button(text="\U0001f6ab Заблокировать", callback_data=f"ban:{anon_id}")
            kb.button(text="\U0001f5d1 Удалить", callback_data=f"del_ask:{anon_id}")
            kb.button(text="\U0001f3ae Играть", callback_data=f"ttt_challenge:{anon_id}")
        kb.adjust(1)
        await callback.message.answer(text, reply_markup=kb.as_markup())

    elif action == "ban":
        await callback.answer("\U0001f6ab Пользователь заблокирован.")
        db.ban_user(target_user_id)
        await delete_waiting(target_user_id)
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
            f"\u270f\ufe0f <b>Введите текст</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Отправь сообщение \u2014 оно будет доставлено.\n"
            "/cancel \u2014 отменить",
            reply_markup=admin_cmds_keyboard(),
        )

    elif action == "pgn":
        page = anon_id
        await callback.answer()
        text, markup = paginated_users_list(page)
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)

    elif action == "pgn_del":
        page = anon_id
        await callback.answer()
        text, markup = paginated_users_list(page, action="del_ask", nav_prefix="pgn_del")
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)

    elif action == "rename":
        rename_anon_id = anon_id
        await callback.answer()
        await callback.message.answer(
            f"\u270f\ufe0f <b>Введите новое имя</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Просто напиши новое имя.\n"
            "/cancel \u2014 отменить"
        )

    elif action == "del_ask":
        await callback.answer()
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Да, удалить", callback_data=f"del_yes:{anon_id}")
        kb.button(text="❌ Нет", callback_data=f"del_no:{anon_id}")
        kb.adjust(2)
        await callback.message.answer(
            f"\U0001f5d1 <b>Точно удалить</b> пользователя #<b>{anon_id}</b>?\n"
            "Пользователь будет перемещён в \u00abУдалённые\u00bb.\n"
            "Его можно будет восстановить.",
            reply_markup=kb.as_markup(),
        )

    elif action == "del_yes":
        await callback.answer()
        db.delete_user(anon_id)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer(
            f"\U0001f5d1 Пользователь #<b>{anon_id}</b> перемещён в \u00ab\U0001f5d1 Удаленные\u00bb.\n"
            "Можешь нажать кнопку <b>\U0001f5d1 Удаленные</b> внизу, чтобы посмотреть."
        )

    elif action == "del_no":
        await callback.answer()
        try:
            await callback.message.delete()
        except Exception:
            pass

    elif action == "restore":
        await callback.answer("✅ Пользователь восстановлен.")
        db.restore_user(anon_id)
        await delete_waiting(target_user_id)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer(
            f"✅ Пользователь #<b>{anon_id}</b> восстановлен и снова в списке."
        )

    elif action == "hard_del_ask":
        await callback.answer()
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Да, удалить навсегда", callback_data=f"hard_del_yes:{anon_id}")
        kb.button(text="❌ Нет", callback_data=f"del_no:{anon_id}")
        kb.adjust(2)
        await callback.message.answer(
            f"⚠️ <b>Точно удалить навсегда</b> пользователя #<b>{anon_id}</b>?\n"
            "Все сообщения и данные будут безвозвратно удалены.",
            reply_markup=kb.as_markup(),
        )

    elif action == "hard_del_yes":
        await callback.answer("\U0001f5d1 Пользователь удалён навсегда.")
        db.hard_delete_user(anon_id)
        try:
            await callback.message.delete()
        except Exception:
            pass
        await callback.message.answer(
            f"\U0001f5d1 Пользователь #<b>{anon_id}</b> и все его сообщения удалены навсегда."
        )

    elif action == "unblock":
        await callback.answer("✅ Пометка снята.")
        db.unmark_blocked(anon_id)
        try:
            await callback.message.delete()
        except Exception:
            pass


BTN_WRITE = "\u270d\ufe0f Написать"
BTN_HISTORY = "\U0001f4dc История"
BTN_STATS = "\U0001f4ca Статистика"
BTN_LIST = "\U0001f464 Список"
BTN_BANNED = "\U0001f6ab Блокировки"
BTN_BLOCKED = "\U0001f6ab Заблокировали бота"
BTN_DELETED = "\U0001f5d1 Удаленные"
BTN_DEL = "\U0001f5d1 Удалить"
BTN_ADD_ID = "\u2795 Добавить ID"
BTN_TTT = "\U0001f3ae Крестики-нолики"
BTN_DICE = "\U0001f3b2 Везение"
BTN_BCAST = "\U0001f4e2 Рассылка"
BTN_HELP = "❓ Помощь"
BTN_CANCEL = "❌ Отмена"


def admin_cmds_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WRITE)],
            [KeyboardButton(text=BTN_HISTORY), KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_LIST), KeyboardButton(text=BTN_BANNED)],
            [KeyboardButton(text=BTN_DELETED), KeyboardButton(text=BTN_BLOCKED)],
            [KeyboardButton(text=BTN_TTT), KeyboardButton(text=BTN_DICE)],
            [KeyboardButton(text=BTN_DEL)],
            [KeyboardButton(text=BTN_ADD_ID)],
            [KeyboardButton(text=BTN_BCAST)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


BTN_CMDS = {BTN_WRITE, BTN_HISTORY, BTN_STATS, BTN_LIST, BTN_BANNED,
            BTN_DELETED, BTN_DEL, BTN_BLOCKED, BTN_TTT, BTN_DICE, BTN_ADD_ID, BTN_BCAST, BTN_HELP, BTN_CANCEL}


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
                await message.answer(f"✅ ID #<b>{anon_id}</b> принят. Теперь введи текст сообщения:")
            except ValueError:
                await message.answer("❌ ID должен быть числом. Попробуй ещё раз или /cancel")
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
                prefix = f"\u2709\ufe0f <b>Сообщение от {ADMIN_NAME}:</b>"
                caption = f"{prefix}\n\n{esc(admin_text)}" if admin_text else prefix
                await forward_media(target_user_id, message, caption)
                db.save_message(target_user_id, write_flow_anon_id, admin_text, direction="admin_to_user")
                kb = admin_cmds_keyboard()
                await message.answer(f"✅ Сообщение отправлено пользователю #<b>{write_flow_anon_id}</b>!", reply_markup=kb)
            except Exception as e:
                err = str(e).lower()
                if "forbidden" in err:
                    await message.answer("❌ Пользователь ограничил получение этого типа сообщений.")
                else:
                    await message.answer(f"❌ Ошибка: {e}")
            write_flow_step = None
            write_flow_anon_id = None
            return

        if message.text == BTN_ADD_ID:
            add_user_step = True
            await message.answer(
                "\U0001f522 <b>Введи Telegram ID пользователя</b> (число).\n\n"
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
                    f"👤 {esc(existing['first_name'] or '\u2014')}\n"
                    f"\U0001f517 @{esc(existing['username']) if existing['username'] else '\u2014'}",
                    reply_markup=admin_cmds_keyboard(),
                )
                return
            try:
                chat = await message.bot.get_chat(uid)
                anon_id, _ = db.add_user(uid, chat.first_name or "", chat.username or "", chat.language_code or "")
                await message.answer(
                    f"✅ Пользователь <code>{uid}</code> добавлен!\n"
                    f"🆔 Анонимный ID: <b>#{anon_id}</b>\n"
                    f"👤 {esc(chat.first_name or '\u2014')}\n"
                    f"\U0001f517 @{esc(chat.username) if chat.username else '\u2014'}",
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

        if message.text == BTN_WRITE:
            text, markup = paginated_users_list(1)
            await message.answer("\U0001f447 <b>Выбери пользователя</b> \u2014 нажми \u270d\ufe0f рядом с именем:", reply_markup=markup)
            return
        if message.text == BTN_HISTORY:
            return await cmd_history(message)
        if message.text == BTN_STATS:
            return await cmd_stats(message)
        if message.text == BTN_LIST:
            return await cmd_list(message)
        if message.text == BTN_BANNED:
            return await cmd_banned(message)
        if message.text == BTN_BLOCKED:
            return await cmd_blocked(message)
        if message.text == BTN_DELETED:
            return await cmd_deleted(message)
        if message.text == BTN_DEL:
            text, markup = paginated_users_list(1, action="del_ask", nav_prefix="pgn_del")
            await message.answer("\U0001f447 <b>Выбери пользователя для удаления:</b>", reply_markup=markup)
            return
        if message.text == BTN_TTT:
            text, markup = ttt_game_list(1)
            await message.answer(text, reply_markup=markup)
            return
        if message.text == BTN_DICE:
            text, markup = dice_game_list(1)
            await message.answer(text, reply_markup=markup)
            return
        if message.text == BTN_BCAST:
            await message.answer(
                "\U0001f4e2 <b>Введите текст для рассылки</b>\n\n"
                "После команды напиши сообщение:\n"
                "<code>/broadcast Ваш текст</code>"
            )
            return
        if message.text == BTN_HELP:
            return await cmd_help(message)
        if message.text == BTN_CANCEL:
            return await cmd_cancel(message)

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
                prefix = f"\u2709\ufe0f <b>Ответ от {ADMIN_NAME}:</b>"
                caption = f"{prefix}\n\n{esc(admin_text)}" if admin_text else prefix
                await forward_media(target_user_id, message, caption)
                db.save_message(target_user_id, anon_id, admin_text, direction="admin_to_user")
                kb = admin_cmds_keyboard()
                await message.answer(f"✅ Ответ отправлен пользователю #<b>{anon_id}</b>!", reply_markup=kb)
            except Exception as e:
                err = str(e).lower()
                if "chat not found" in err or "bot was blocked" in err:
                    await message.answer("❌ Пользователь больше недоступен (удалил чат или заблокировал бота).")
                    db.mark_blocked(target_user_id)
                elif "forbidden" in err:
                    await message.answer(f"❌ Пользователь ограничил получение этого типа сообщений.")
                else:
                    await message.answer(f"❌ Не удалось отправить: {e}")
            admin_pending_reply = None
            return

        await message.answer(
            "\U0001f4a1 Используй кнопки внизу или нажми\n"
            "<b>\u270d\ufe0f Ответить</b> под сообщением пользователя.\n"
            "<code>/help</code> \u2014 список команд",
            reply_markup=admin_cmds_keyboard(),
        )
        return

    if db.is_banned(user_id):
        ban_anon = db.get_anon_id_by_user_id(user_id)
        appeal_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подать апелляцию", callback_data=f"appeal:{ban_anon}"),
             InlineKeyboardButton(text="❌ Нет", callback_data="none")]
        ])
        await message.answer(
            "\U0001f6ab <b>Вы заблокированы и не можете писать.</b>\n\n"
            "Хотите подать апелляцию?",
            reply_markup=appeal_kb,
        )
        return

    # Anti-spam: max 1 message per 3 seconds
    now = time.time()
    last = user_last_msg.get(user_id, 0)
    if now - last < 2 and message.from_user.id != ADMIN_ID:
        warnings = user_spam_warnings.get(user_id, 0) + 1
        user_spam_warnings[user_id] = warnings
        if warnings >= 5:
            db.ban_user(user_id)
            await message.answer("\U0001f6ab Вы заблокированы за спам.")
            ban_info = db.get_anon_id_by_user_id(user_id)
            await bot.send_message(ADMIN_ID, f"\U0001f6ab Пользователь <code>{user_id}</code> {'(#' + str(ban_info) + ')' if ban_info else ''} автоматически заблокирован за спам.")
            return
        await message.answer("⚠️ <b>Не спамьте!</b> Подождите 2 секунды между сообщениями.")
        return
    user_last_msg[user_id] = now

    user = message.from_user
    anon_id, is_banned = db.add_user(
        user_id,
        user.first_name or "",
        user.username or "",
        user.language_code or "",
    )

    last_admin_msg = db.get_last_admin_message(user_id)
    if last_admin_msg:
        reply_context = f"\U0001f4ce <b>Ответ на ваше сообщение:</b>\n\u2514 {esc(last_admin_msg[:100])}"
    else:
        reply_context = None

    info_lines = [
        f"\U0001f4e9 <b>Новое сообщение</b>",
        f"\U0001f194 Анонимный ID: <b>#{anon_id}</b>",
        f"👤 {esc(user.first_name or '\u2014')}",
    ]
    if user.username:
        info_lines.append(f"\U0001f517 @{esc(user.username)}")
    if user.language_code:
        info_lines.append(f"\U0001f310 {user.language_code}")

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

    show_ttt = False
    if user_text:
        user_text_lower = user_text.lower()
        show_ttt = any(kw in user_text_lower for kw in GAME_KEYWORDS)

    admin_text = "\n".join(info_lines)
    kb = user_actions_keyboard(anon_id, is_banned=bool(is_banned), show_ttt=show_ttt).as_markup()
    await forward_media(ADMIN_ID, message, admin_text, reply_markup=kb)

    msg_text = get_message_text(message)
    db.save_message(user_id, anon_id, msg_text)

    show_ttt = False
    if msg_text:
        msg_lower = msg_text.lower()
        show_ttt = any(kw in msg_lower for kw in GAME_KEYWORDS)

    wait_text = (
        "\U0001f36a <b>Подождите!</b>\n\n"
        "Ваше сообщение доставлено. "
        "Печенька в скором времени ответит вам. \u2709\ufe0f"
    )
    if show_ttt:
        wait_text += (
            "\n\n\U0001f3ae <b>Хочешь сыграть в крестики-нолики?</b>\n"
            f"{ADMIN_NAME} уже получил твой вызов!"
        )

    wait_msg = await message.answer(wait_text)
    waiting_messages[user_id] = wait_msg.message_id


# ────────── Healthcheck HTTP server ────────────────────────────

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
    server = await asyncio.start_server(handle_healthcheck, host="0.0.0.0", port=8080)
    logging.info("\U0001fa7a Healthcheck server on :8080")
    async with server:
        await server.serve_forever()


# ────────────────────────────── Entry ──────────────────────────

async def main():
    healthcheck_task = asyncio.create_task(run_healthcheck())
    while True:
        try:
            logging.info("\U0001f680 Бот запущен!")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            logging.error(f"Критическая ошибка: {e}", exc_info=True)
            logging.info("Перезапуск через 5 секунд...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
