import asyncio
import json
import logging
import html
import math
import os
import random
import secrets
import time
from datetime import datetime

import aiohttp
from aiohttp import web

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo
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

# Ideas tracking
user_telling_idea: set[int] = set()
admin_commenting_idea: int | None = None
ban_user_step: int | None = None
admin_writing_diary: bool = False
diary_editing: int | None = None
dialog_date_entry: int | None = None


async def delete_waiting(target_user_id: int):
    msg_id = waiting_messages.pop(target_user_id, None)
    if msg_id:
        try:
            await bot.delete_message(target_user_id, msg_id)
        except Exception:
            pass


def get_message_text(message: Message) -> str:
    text = message.text or message.caption or ""
    if text:
        return text
    return get_content_type_plain(message) or ""

def get_content_type_plain(message: Message) -> str | None:
    if message.sticker:
        return "[Стикер]"
    if message.photo:
        return "[Фото]"
    if message.video:
        return "[Видео]"
    if message.animation:
        return "[GIF]"
    if message.voice:
        return "[Голосовое]"
    if message.video_note:
        return "[Видеосообщение]"
    if message.audio:
        return "[Аудио]"
    if message.document:
        return "[Документ]"
    if message.location:
        return "[Геопозиция]"
    if message.contact:
        return "[Контакт]"
    if message.poll:
        return "[Опрос]"
    if message.dice:
        return "[Dice]"
    if message.venue:
        return "[Место]"
    return None


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
TIMEZONE_OFFSET = 5  # UTC+5 hours

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


def local_time(ts: str) -> str:
    """Convert UTC timestamp to local time (+5h)"""
    if not ts:
        return "???"
    from datetime import timedelta
    try:
        dt = datetime.fromisoformat(ts) + timedelta(hours=TIMEZONE_OFFSET)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts[:19]


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
        builder.button(text="\U0001f3ae Крестики-нолики", callback_data=f"ttt_challenge:{anon_id}")
    builder.button(text="\U0001f3d3 Пинг-Понг", callback_data=f"pong_challenge_admin:{anon_id}")
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


# ═══════════════════════ Wisdom of the Day ═══════════════════════

WISDOM_QUOTES = [
    "Единственный способ делать великие дела — любить то, что ты делаешь. © Стив Джобс",
    "Будь тем изменением, которое хочешь увидеть в мире. © Махатма Ганди",
    "Жизнь — это то, что с тобой происходит, пока ты строишь планы. © Джон Леннон",
    "Не суди о прошлом по настоящему, а о настоящем — по прошлому. © Пифагор",
    "Сложнее всего начать действовать, все остальное зависит только от упорства. © Амелия Эрхарт",
    "Успех — это способность идти от неудачи к неудаче, не теряя энтузиазма. © Уинстон Черчилль",
    "Лучшая месть — огромный успех. © Фрэнк Синатра",
    "Секрет перемен в том, чтобы сосредоточить всю свою энергию не на борьбе со старым, а на создании нового. © Сократ",
    "Знание — сила. © Фрэнсис Бэкон",
    "Всё, что нас не убивает, делает нас сильнее. © Фридрих Ницше",
    "Путешествие в тысячу миль начинается с одного шага. © Лао-Цзы",
    "Счастье — это не пункт назначения, а способ путешествия. © Маргарет Ли Ранбек",
    "Тот, кто не знает иностранных языков, ничего не знает о своём родном. © Иоганн Вольфганг Гёте",
    "Воображение важнее знания. © Альберт Эйнштейн",
    "Порядок освобождает мысль. © Антуан де Сент-Экзюпери",
    "Глаза бояться, а руки делают. © Русская пословица",
    "Под лежачий камень вода не течёт. © Русская пословица",
    "Тише едешь — дальше будешь. © Русская пословица",
    "Не имей сто рублей, а имей сто друзей. © Русская пословица",
    "Семь раз отмерь — один раз отрежь. © Русская пословица",
    "Век живи — век учись. © Русская пословица",
    "Делу время — потехе час. © Русская пословица",
    "Лучше синица в руках, чем журавль в небе. © Русская пословица",
    "Без труда не выловишь и рыбку из пруда. © Русская пословица",
    "Утро вечера мудренее. © Русская пословица",
    "Не всё то золото, что блестит. © Русская пословица",
    "Доверяй, но проверяй. © Русская пословица",
    "Всяк кулик своё болото хвалит. © Русская пословица",
    "С кем поведёшься — от того и наберёшься. © Русская пословица",
    "Поспешишь — людей насмешишь. © Русская пословица",
    "Всё приходит вовремя для того, кто умеет ждать. © Оноре де Бальзак",
    "Невозможно — это всего лишь громкое слово, за которым прячутся маленькие люди. © Мухаммед Али",
    "То, что мы знаем, — ограничено, а то, чего мы не знаем, — бесконечно. © Пьер-Симон Лаплас",
    "Самая трудная вещь — это решение действовать, остальное — только упорство. © Амелия Эрхарт",
    "Логика может привести вас от пункта А к пункту Б, а воображение — куда угодно. © Альберт Эйнштейн",
    "Лучше зажечь одну маленькую свечу, чем всю жизнь проклинать темноту. © Конфуций",
    "Никогда не ошибается тот, кто ничего не делает. © Теодор Рузвельт",
    "Жизнь измеряется не количеством сделанных вдохов, а количеством моментов, от которых захватывает дух. © Майя Энджелоу",
    "Великие умы обсуждают идеи. Средние умы обсуждают события. Мелкие умы обсуждают людей. © Элеонора Рузвельт",
    "Если хочешь изменить мир — начни с себя. © Лев Толстой",
    "Человек есть то, что он ест. © Людвиг Фейербах",
    "Красота спасёт мир. © Фёдор Достоевский",
    "Мы в ответе за тех, кого приручили. © Антуан де Сент-Экзюпери",
    "Свобода — это осознанная необходимость. © Бенедикт Спиноза",
    "Всё течёт, всё меняется. © Гераклит",
    "Я мыслю, следовательно, я существую. © Рене Декарт",
    "Познай самого себя. © Сократ",
    "Терпение и труд всё перетрут. © Русская пословица",
    "Не в деньгах счастье. © Русская пословица",
    "Авось и как-нибудь до добра не доведут. © Русская пословица",
    "Делай добро и бросай его в воду. © Русская пословица",
    "Москва не сразу строилась. © Русская пословица",
    "Повторение — мать учения. © Русская пословица",
    "Что написано пером, не вырубишь топором. © Русская пословица",
    "Не рой яму другому — сам в неё попадёшь. © Русская пословица",
    "Яблоко от яблони недалеко падает. © Русская пословица",
    "Цыплят по осени считают. © Русская пословица",
]


def wisdom_of_the_day(anon_id: int = 0) -> str:
    idx = (datetime.now().toordinal() * 100 + anon_id) % len(WISDOM_QUOTES)
    return WISDOM_QUOTES[idx]

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


async def dice_play_game(game_id: int):
    game = db.get_dice_game(game_id)
    if not game:
        return
    p2_anon = game["player2_anon_id"]
    p2_uid = db.get_user_id_by_anon(p2_anon)
    if p2_uid is None:
        return
    p1_score = secrets.randbelow(6) + 1 + secrets.randbelow(6) + 1
    p2_score = secrets.randbelow(6) + 1 + secrets.randbelow(6) + 1
    db.finish_dice_game(game_id, p1_score, p2_score)
    p1_name = ADMIN_NAME
    p2_name = f"#{p2_anon}"
    lines = [
        f"\U0001f3b2 <b>Везение!</b>\n",
        f"<b>{p1_name}</b> \U0001f3b2: {p1_score} очков",
        f"<b>{p2_name}</b> \U0001f3b2: {p2_score} очков",
        "",
    ]
    if p1_score > p2_score:
        lines.append(f"\U0001f3c6 <b>Победил: {p1_name}</b>")
    elif p2_score > p1_score:
        lines.append(f"\U0001f3c6 <b>Победил: {p2_name}</b>")
    else:
        lines.append("\U0001f91d <b>Ничья!</b>")
    text = "\n".join(lines)
    rematch_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f504 Реванш", callback_data=f"dice_rematch:{game_id}")]
    ])
    try:
        await bot.send_message(ADMIN_ID, text, reply_markup=rematch_kb)
        await bot.send_message(p2_uid, text, reply_markup=rematch_kb)
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

    # Notify admin about new user
    user_info = (
        f"\U0001f4e5 <b>Новый пользователь</b>\n\n"
        f"\U0001f194 ID: <b>#{anon_id}</b>\n"
        f"\U0001f464 {esc(message.from_user.first_name or '—')}"
    )
    if message.from_user.username:
        user_info += f"\n\U0001f517 @{esc(message.from_user.username)}"
    user_info += f"\n\U0001f511 <a href=\"tg://user?id={message.from_user.id}\">Telegram ID: {message.from_user.id}</a>"
    await bot.send_message(ADMIN_ID, user_info)

    start_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f3ae Бросить вызов Cookie", callback_data=f"user_ttt:{anon_id}"),
         InlineKeyboardButton(text="\U0001f4a1 Мудрость дня", callback_data=f"wisdom:{anon_id}")],
        [InlineKeyboardButton(text="\U0001f4a1 Предложить идею", callback_data=f"user_idea:{anon_id}")],
    ])
    await message.answer(
        "\U0001f44b <b>Привет! Я анонимный бот.</b>\n\n"
        "Ты можешь написать мне любое сообщение, "
        f"и я передам его <b>{ADMIN_NAME}</b> <b>анонимно</b>.\n"
        "Никто не узнает твой Telegram ID или личные данные.\n\n"
        f"Твой анонимный номер: <b>#{anon_id}</b>\n\n"
        "Просто напиши что-нибудь ниже \u2709\ufe0f\n\n"
        "\U0001f3ae <b>Хочешь сыграть в крестики-нолики?</b>\n"
        "Напиши \u00abигра\u00bb или \u00abttt\u00bb и я передам вызов!\n\n"
        f"\U0001f4a1 <b>Мудрость дня:</b>\n{wisdom_of_the_day(anon_id)}\n\n"
        "\U0001f4d6 <b>Дневник Cookie</b>\n"
        "Напиши <b>«дневник»</b> чтобы читать мысли Cookie!\n\n"
        "Напиши <b>«мудрость»</b> чтобы увидеть новую каждый день!",
        reply_markup=start_kb,
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
            "\U0001f4e9 <b>Сообщения:</b>\n"
            "\u270d\ufe0f Написать \u2014 выбрать пользователя и написать\n"
            "\U0001f4dc История \u2014 сообщения за последний час\n"
            "\U0001f4e2 Рассылка \u2014 написать всем\n\n"
            "\U0001f464 <b>Пользователи:</b>\n"
            "\U0001f464 Список \u2014 список пользователей\n"
            "\U0001f6ab Блокировки \u2014 заблокированные\n"
            "\U0001f6ab Заблокировали бота \u2014 кто заблокировал бота\n"
            "\U0001f5d1 Удаленные \u2014 просмотр/восстановление\n"
            "\U0001f5d1 Удалить \u2014 удалить из списка\n"
            "\u2795 Добавить ID \u2014 добавить по Telegram ID\n"
            "\U0001f50d /find @user \u2014 найти/добавить по юзернейму\n\n"
            "\U0001f3ae <b>Игры:</b>\n"
            "\U0001f3ae Крестики-нолики \u2014 TTT с пользователем\n"
            "\U0001f3b2 Везение \u2014 кинь кубики с пользователем\n\n"
            "\U0001f4a1 <b>Прочее:</b>\n"
            "\U0001f4a1 Мудрость дня \u2014 цитата дня\n"
            "\U0001f4a1 Идеи пользователей \u2014 просмотр идей\n"
            "\U0001f4ca Статистика \u2014 полная статистика\n"
            "\u274c Отмена \u2014 отменить действие\n\n"
            "\U0001f4cc <b>Клавиатура внизу \u2014 все кнопки под рукой!</b>",
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
    global admin_pending_reply, write_flow_step, write_flow_anon_id, add_user_step, rename_anon_id, admin_commenting_idea, ban_user_step, admin_writing_diary, diary_editing, dialog_date_entry
    admin_pending_reply = None
    write_flow_step = None
    write_flow_anon_id = None
    add_user_step = False
    rename_anon_id = None
    admin_commenting_idea = None
    ban_user_step = None
    admin_writing_diary = False
    diary_editing = None
    dialog_date_entry = None
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
        ban_reason = row_get(u, "ban_reason", "")
        status = f"\U0001f6ab Заблокирован"
        if ban_reason:
            status += f" ({esc(str(ban_reason)[:100])})"
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
            lines.append(f"\n👤 #{current_id} \u2014 {name}{username}:")
        time = local_time(r["timestamp"])
        direction = r.get("direction", "user_to_admin")
        icon = "\u2709\ufe0f" if direction == "admin_to_user" else "\U0001f4e9"
        label = f"({time})"  # e.g., (2026-05-14 12:30:45)
        lines.append(f'  {icon} {label} "{esc(r["text"] or "")}"')
    text = "\n".join(lines)
    if len(text) > 4000:
        text = text[:4000] + "\n\n<i>...</i>"
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
    global admin_pending_reply, write_flow_step, write_flow_anon_id, rename_anon_id, admin_commenting_idea, ban_user_step, admin_writing_diary, diary_editing, dialog_date_entry

    parts = callback.data.split(":")
    action = parts[0]

    if not is_admin(callback.from_user.id):
        if action in ("ttt_accept", "ttt_decline", "ttt_move", "ttt_surrender", "ttt_rematch",
                       "appeal", "appeal_accept", "appeal_decline",
                       "dice_accept", "dice_decline", "dice_rematch", "dice_my_stats", "dice_pgn",
                       "wisdom", "user_ttt", "user_idea", "idea_accept", "idea_reject", "idea_comment",
                       "diary_read", "diary_pgn", "diary_edit",
                       "diary_notify", "diary_notify_off",
                        "pong_challenge", "pong_user_accept", "pong_user_decline", "none"):
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

    elif action == "pong_challenge_admin":
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
        await callback.answer()
        await bot.send_message(ADMIN_ID, f"⏳ Вызов отправлен пользователю #{anon_id}...")
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Принять", callback_data=f"pong_user_accept:{ADMIN_ANON_ID}:{anon_id}"),
             InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pong_user_decline:{ADMIN_ANON_ID}:{anon_id}")]
        ])
        try:
            await bot.send_message(
                target_user_id,
                f"\U0001f3d3 <b>{ADMIN_NAME} бросает тебе вызов в Пинг-Понг!</b>\n\n"
                f"Нажми <b>✅ Принять</b> чтобы сыграть.\n"
                f"Нажми <b>❌ Отклонить</b> чтобы отказаться.",
                reply_markup=accept_kb,
            )
        except Exception as ce:
            err_msg = str(ce).lower()
            if "chat not found" in err_msg or "bot was blocked" in err_msg:
                await bot.send_message(ADMIN_ID, f"❌ Пользователь #{anon_id} заблокировал бота. Вызов отменён.")
            else:
                await bot.send_message(ADMIN_ID, f"❌ Не удалось отправить вызов: {esc(str(ce)[:100])}")
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
        clicker_anon = db.get_anon_id_by_user_id(callback.from_user.id)
        dg = db.get_dice_game(game_id)
        if dg and clicker_anon and clicker_anon != dg["player2_anon_id"]:
            await callback.answer("❌ Это не ваш вызов.", show_alert=True)
            return
        await callback.answer("\U0001f3b2 Кидаем кубики!")
        try:
            await callback.message.delete()
        except Exception:
            pass
        await dice_play_game(game_id)
        return

    elif action == "dice_decline":
        game_id = int(parts[1])
        await callback.answer("❌ Вызов отклонён.")
        try:
            await callback.message.delete()
        except Exception:
            pass
        if not is_admin(callback.from_user.id):
            await bot.send_message(ADMIN_ID, "❌ Пользователь отклонил вызов в Везение.")
        return

    elif action == "dice_rematch":
        old_id = int(parts[1])
        old = db.get_dice_game(old_id)
        if not old:
            await callback.answer("❌ Игра не найдена.", show_alert=True)
            return
        p2_anon = old["player2_anon_id"]
        p2_uid = db.get_user_id_by_anon(p2_anon)
        if not p2_uid:
            await callback.answer("❌ Пользователь не найден.", show_alert=True)
            return
        await callback.answer()
        new_id = db.create_dice_game(ADMIN_ANON_ID, p2_anon)
        await bot.send_message(ADMIN_ID, "Ожидайте ответа от пользователя...")
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Кинуть кубик!", callback_data=f"dice_accept:{new_id}"),
                InlineKeyboardButton(text="❌ Нет", callback_data=f"dice_decline:{new_id}"),
            ]
        ])
        try:
            await bot.send_message(
                p2_uid,
                f"\U0001f3b2 <b>Реванш в Везение!</b>\n\n"
                f"Противник хочет сыграть снова!",
                reply_markup=accept_kb,
            )
        except Exception:
            await bot.send_message(ADMIN_ID, f"❌ Не удалось отправить реванш.")
        return

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
            await callback.answer("❌ Лимит апелляций: 3 в день. Попробуйте завтра.", show_alert=True)
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

    elif action == "user_ttt":
        if is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        uid = db.get_user_id_by_anon(anon_id)
        if uid and db.is_banned(uid):
            await callback.answer("❌ Вы заблокированы и не можете кидать вызовы.", show_alert=True)
            return
        user_game = db.get_player_game(anon_id)
        if user_game:
            await callback.answer("❌ Вы уже в игре.", show_alert=True)
            return
        admin_game = db.get_player_game(ADMIN_ANON_ID)
        if admin_game:
            await callback.answer("❌ Cookie уже в игре с другим человеком.", show_alert=True)
            return
        target_user_id = db.get_user_id_by_anon(anon_id)
        if target_user_id is None:
            await callback.answer("❌ Ошибка.", show_alert=True)
            return
        game_id = db.create_game(ADMIN_ANON_ID, anon_id, ADMIN_ID, target_user_id, None)
        await callback.answer()
        await callback.message.answer("✅ Вызов отправлен Cookie! Ожидайте ответа.")
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"ttt_accept:{game_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"ttt_decline:{game_id}"),
            ]
        ])
        await bot.send_message(
            ADMIN_ID,
            f"\U0001f3ae Пользователь #<b>{anon_id}</b> бросил вам вызов в крестики-нолики!",
            reply_markup=accept_kb,
        )
        try:
            uid = db.get_user_id_by_anon(anon_id)
            if uid:
                await bot.send_message(uid, "Ожидайте ответа от Cookie...")
        except Exception:
            pass
        return

    elif action == "user_idea":
        if is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        user_telling_idea.add(callback.from_user.id)
        await callback.answer()
        await callback.message.answer(
            "\U0001f4a1 <b>Расскажите вашу идею</b>\n\n"
            "Напишите, что бы вы хотели улучшить в боте.\n"
            "Cookie рассмотрит ваше предложение!"
        )
        return

    elif action == "idea_accept":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        idea_id = int(parts[1])
        db.update_idea(idea_id, "accepted")
        await callback.answer("✅ Идея принята!")
        await callback.message.edit_text(f"✅ Идея #{idea_id} принята.")
        idea = db.get_idea(idea_id)
        if idea:
            try:
                await bot.send_message(idea["user_id"], "\u2705 <b>Cookie принял вашу идею!</b>\n\nСпасибо за предложение!")
            except Exception:
                pass
        return

    elif action == "idea_reject":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        idea_id = int(parts[1])
        db.update_idea(idea_id, "rejected")
        await callback.answer("❌ Идея отклонена.")
        await callback.message.edit_text(f"❌ Идея #{idea_id} отклонена.")
        idea = db.get_idea(idea_id)
        if idea:
            try:
                await bot.send_message(idea["user_id"], "\u274c <b>Cookie отклонил вашу идею.</b>\n\nПопробуйте предложить другую!")
            except Exception:
                pass
        return

    elif action == "idea_comment":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        idea_id = int(parts[1])
        admin_commenting_idea = idea_id
        await callback.answer("✏️ Напишите комментарий.")
        await callback.message.answer(
            f"✏️ <b>Напишите комментарий</b> к идее #{idea_id}.\n"
            "/cancel — отменить"
        )
        return

    elif action == "diary_notify":
        if is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        uid = db.get_user_id_by_anon(anon_id)
        if uid:
            db._exec("UPDATE users SET diary_notify = 1 WHERE user_id = ?", [uid])
        await callback.answer("✅ Уведомления включены!")
        try: await callback.message.delete()
        except: pass
        await callback.message.answer("\U0001f4d6 <b>Уведомления о новых записях в дневнике включены.</b>\n\nНапиши «дневник» чтобы читать дневник.")
        return

    elif action == "diary_notify_off":
        if is_admin(callback.from_user.id):
            await callback.answer()
            return
        anon_id = int(parts[1])
        uid = db.get_user_id_by_anon(anon_id)
        if uid:
            db._exec("UPDATE users SET diary_notify = 0 WHERE user_id = ?", [uid])
        await callback.answer("✅ Уведомления выключены!")
        try: await callback.message.delete()
        except: pass
        await callback.message.answer("\U0001f4d6 <b>Уведомления о новых записях в дневнике выключены.</b>\n\nНапиши «дневник» чтобы читать дневник.")
        return

    elif action == "diary_write":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        admin_writing_diary = True
        await callback.answer()
        await callback.message.answer(
            "\U0001f4d6 <b>Напишите запись в дневник</b>\n\n"
            "Пользователи смогут прочитать это.\n"
            "/cancel — отменить"
        )
        return

    elif action == "diary_edit":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        entry_id = int(parts[1])
        entry = db.get_diary_entry(entry_id)
        if not entry:
            await callback.answer("❌ Запись не найдена.", show_alert=True)
            return
        diary_editing = entry_id
        await callback.answer()
        await callback.message.answer(
            f"\U0001f4d6 <b>Редактирование записи #{entry_id}</b>\n\n"
            f"Текущий текст:\n{esc(entry['text'][:200])}\n\n"
            "Напишите новый текст.\n"
            "/cancel — отменить"
        )
        return

    elif action == "diary_del_ask":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        entry_id = int(parts[1])
        await callback.answer()
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"diary_del_yes:{entry_id}"),
             InlineKeyboardButton(text="❌ Нет", callback_data="none")]
        ])
        await callback.message.answer(f"\U0001f5d1 <b>Точно удалить запись #{entry_id}?</b>", reply_markup=kb)
        return

    elif action == "diary_del_yes":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        entry_id = int(parts[1])
        db.delete_diary_entry(entry_id)
        await callback.answer("🗑 Запись удалена.")
        try: await callback.message.delete()
        except: pass
        return

    elif action == "diary_read":
        page = int(parts[1])
        await callback.answer()
        entries, total_pages = db.get_diary_entries(page)
        if not entries:
            await callback.message.answer("\U0001f4d6 <b>Дневник пока пуст.</b>")
            return
        lines = [f"\U0001f4d6 <b>Дневник Cookie</b> (стр. {page}/{total_pages}):\n"]
        for e in entries:
            ts = local_time(e["created_at"])
            lines.append(f"<b>{ts}</b>")
            lines.append(f"{esc(e['text'][:500])}")
            lines.append("")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n\n..."
        kb_rows = []
        if total_pages > 1:
            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"diary_pgn:{page - 1}"))
            nav.append(InlineKeyboardButton(text=f"\U0001f4c5 {page}/{total_pages}", callback_data="none"))
            if page < total_pages:
                nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"diary_pgn:{page + 1}"))
            kb_rows.append(nav)
        if is_admin(callback.from_user.id) and entries:
            eid = entries[0]["id"]
            kb_rows.append([
                InlineKeyboardButton(text="\u270f\ufe0f Редактировать", callback_data=f"diary_edit:{eid}"),
                InlineKeyboardButton(text="\U0001f5d1 Удалить", callback_data=f"diary_del_ask:{eid}"),
            ])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
        try:
            await callback.message.edit_text(text, reply_markup=kb)
        except Exception:
            await callback.message.answer(text, reply_markup=kb)
        return

    elif action == "diary_pgn":
        page = int(parts[1])
        await callback.answer()
        entries, total_pages = db.get_diary_entries(page)
        if not entries:
            await callback.message.edit_text("\U0001f4d6 <b>Дневник пока пуст.</b>")
            return
        lines = [f"\U0001f4d6 <b>Дневник Cookie</b> (стр. {page}/{total_pages}):\n"]
        for e in entries:
            ts = local_time(e["created_at"])
            lines.append(f"<b>{ts}</b>")
            lines.append(f"{esc(e['text'][:500])}")
            lines.append("")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n\n..."
        kb_rows = []
        if total_pages > 1:
            nav = []
            if page > 1:
                nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"diary_pgn:{page - 1}"))
            nav.append(InlineKeyboardButton(text=f"\U0001f4c5 {page}/{total_pages}", callback_data="none"))
            if page < total_pages:
                nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"diary_pgn:{page + 1}"))
            kb_rows.append(nav)
        if is_admin(callback.from_user.id) and entries:
            eid = entries[0]["id"]
            kb_rows.append([
                InlineKeyboardButton(text="\u270f\ufe0f Редактировать", callback_data=f"diary_edit:{eid}"),
                InlineKeyboardButton(text="\U0001f5d1 Удалить", callback_data=f"diary_del_ask:{eid}"),
            ])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
        await callback.message.edit_text(text, reply_markup=kb)
        return

    elif action == "wisdom":
        if is_admin(callback.from_user.id):
            anon_id = ADMIN_ANON_ID
        else:
            anon_id = int(parts[1])
        await callback.answer()
        await callback.message.answer(f"\U0001f4a1 <b>Мудрость дня</b>\n\n{wisdom_of_the_day(anon_id)}")
        return

    elif action == "none":
        if not is_admin(callback.from_user.id):
            await callback.answer()
            return
        await callback.answer()
        return

    elif action == "pong_user_accept":
        await callback.answer("✅ Принято! Создаю игру...")
        admin_anon_id = int(parts[1])
        user_anon_id = int(parts[2])
        user = db.get_user_by_anon(user_anon_id)
        if not user:
            await callback.message.answer("❌ Ошибка: пользователь не найден.")
            return
        target_user_id = user["user_id"]
        admin_user_id = ADMIN_ID
        room_id = f"pong_{user_anon_id}_{int(time.time())}"
        PONG_ROOMS[room_id] = {
            "id": room_id,
            "p1_ws": None, "p2_ws": None,
            "p1_y": 310, "p2_y": 310,
            "ball_x": 250, "ball_y": 350,
            "ball_vx": 0, "ball_vy": 0,
            "p1_score": 0, "p2_score": 0,
            "running": False,
            "loop_task": None,
        }
        user_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=right"
        admin_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=left"
        play_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=admin_link))]
        ])
        await bot.send_message(ADMIN_ID, f"\U0001f3d3 <b>Пользователь #{user_anon_id} принял вызов!</b>\n\nНажимай!", reply_markup=play_kb)
        try:
            await bot.send_message(target_user_id, f"\U0001f3d3 <b>Ты принял вызов!</b>\n\nИгра начинается!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=user_link))]
            ]))
        except Exception:
            pass
        return

    elif action == "pong_user_decline":
        await callback.answer()
        admin_anon_id = int(parts[1])
        user_anon_id = int(parts[2])
        await bot.send_message(ADMIN_ID, f"\U0001f3d3 Пользователь #{user_anon_id} отклонил вызов в пинг-понг.")
        await callback.message.answer("😔 Ты отклонил вызов.")
        return

    elif action == "pong_challenge":
        await callback.answer()
        user_id = callback.from_user.id
        anon_id = db.get_anon_id_by_user_id(user_id)
        if not anon_id:
            await callback.message.answer("❌ Ты не найден в базе. Напиши боту любое сообщение.")
            return
        accept_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\u2705 Принять", callback_data=f"pong_accept:{anon_id}"),
             InlineKeyboardButton(text="\u274c Отклонить", callback_data=f"pong_decline:{anon_id}")]
        ])
        await bot.send_message(ADMIN_ID, f"\U0001f3d3 <b>Пинг-Понг!</b>\n\nПользователь #{anon_id} хочет сыграть с тобой!", reply_markup=accept_kb)
        await callback.message.answer("\u23f3 Ожидаем ответа от Cookie... Как только он примет, игра начнётся!")
        return

    # ── Admin-only callbacks ──

    if not is_admin(callback.from_user.id):
        return

    # pgn, pgn_del use page number, not anon_id
    if action in ("pgn", "pgn_del", "history_all", "history_date_prompt"):
        anon_id = None
        target_user_id = None
    else:
        anon_id = int(parts[1])
        target_user_id = db.get_user_id_by_anon(anon_id)
        if target_user_id is None:
            detail = f"callback_data: {callback.data}, parts: {parts}, action: {action}"
            await callback.answer("❌ Пользователь не найден.", show_alert=True)
            await bot.send_message(ADMIN_ID, f"❌ Пользователь не найден.\n<code>{esc(detail)}</code>")
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
        return

    elif action == "pong_accept":
        await callback.answer()
        user_anon_id = anon_id
        target_user_id = target_user_id
        room_id = f"pong_{user_anon_id}_{int(time.time())}"
        PONG_ROOMS[room_id] = {
            "id": room_id,
            "p1_ws": None, "p2_ws": None,
            "p1_y": 310, "p2_y": 310,
            "ball_x": 250, "ball_y": 350,
            "ball_vx": 0, "ball_vy": 0,
            "p1_score": 0, "p2_score": 0,
            "running": False,
            "loop_task": None,
        }
        user_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=right"
        admin_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=left"
        play_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=admin_link))]
        ])
        try:
            await bot.send_message(target_user_id, f"\U0001f3d3 <b>Cookie принял вызов!</b>\n\nНажимай кнопку, чтобы начать!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=user_link))]
            ]))
        except Exception:
            await callback.message.answer("❌ Не удалось отправить приглашение пользователю.")
            return
        await callback.message.answer(f"\U0001f3d3 <b>Игра началась!</b>\n\nНажимай кнопку!", reply_markup=play_kb)
        return

    elif action == "pong_decline":
        await callback.answer()
        try:
            await bot.send_message(target_user_id, "😔 Cookie отклонил вызов.")
        except Exception:
            pass
        await callback.message.answer("❌ Вызов отклонён.")
        return

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
            ban_reason = row_get(u, "ban_reason", "")
            status = f"\U0001f6ab Заблокирован"
            if ban_reason:
                status += f" ({esc(str(ban_reason)[:100])})"
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
            kb.button(text="\U0001f4dc Диалог", callback_data=f"dialog:{anon_id}:1")
        kb.adjust(1)
        await callback.message.answer(text, reply_markup=kb.as_markup())
        return

    elif action == "ban":
        ban_user_step = anon_id
        await callback.answer()
        await delete_waiting(target_user_id)
        await callback.message.answer(
            f"✏️ <b>Напишите причину блокировки</b> для пользователя #<b>{anon_id}</b>.\n"
            "Пользователь увидит эту причину.\n"
            "/cancel — отменить"
        )
        return

    elif action == "unban":
        await callback.answer("✅ Пользователь разблокирован.")
        db.unban_user(target_user_id)
        await delete_waiting(target_user_id)
        new_kb = user_actions_keyboard(anon_id, is_banned=False).as_markup()
        await callback.message.edit_reply_markup(reply_markup=new_kb)
        return

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
        return

    elif action == "pgn":
        page = int(parts[1])
        await callback.answer()
        text, markup = paginated_users_list(page)
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)
        return

    elif action == "pgn_del":
        page = int(parts[1])
        await callback.answer()
        text, markup = paginated_users_list(page, action="del_ask", nav_prefix="pgn_del")
        if markup:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.edit_text(text)
        return

    elif action == "rename":
        rename_anon_id = anon_id
        await callback.answer()
        await callback.message.answer(
            f"\u270f\ufe0f <b>Введите новое имя</b> для пользователя #<b>{anon_id}</b>\n\n"
            "Просто напиши новое имя.\n"
            "/cancel \u2014 отменить"
        )
        return

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
        return

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
        return

    elif action == "del_no":
        await callback.answer()
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

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
        return

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
        return

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
        return

    elif action == "unblock":
        await callback.answer("✅ Пометка снята.")
        db.unmark_blocked(anon_id)
        try:
            await callback.message.delete()
        except Exception:
            pass
        return

    elif action == "dialog":
        page = int(parts[2]) if len(parts) > 2 else 1
        date_filter = parts[3] if len(parts) > 3 else None
        msgs, total_pages, _ = db.get_user_messages(anon_id, page, date_filter=date_filter)
        name = esc(db.get_user_by_anon(anon_id)["first_name"] or f"#{anon_id}")
        chat_id = callback.message.chat.id
        try: await callback.message.delete()
        except: pass
        if not msgs:
            label = "за сегодня" if date_filter == "today" else "за вчера" if date_filter == "yesterday" else f"за {date_filter}" if date_filter else ""
            empty_text = f"\U0001f4dc <b>Диалог с {name}</b>\n\nНет сообщений {label}." if label else f"\U0001f4dc <b>Диалог с {name}</b>\n\nНет сообщений."
            await bot.send_message(chat_id, empty_text)
            return
        filter_label = " (сегодня)" if date_filter == "today" else " (вчера)" if date_filter == "yesterday" else f" ({date_filter})" if date_filter else ""
        lines = [f"\U0001f4dc <b>Диалог с {name}</b>{filter_label} (стр. {page}/{total_pages}):\n"]
        for i, m in enumerate(msgs):
            num = (page - 1) * 20 + i + 1
            ts = local_time(m["timestamp"])
            icon = "\u2709\ufe0f" if m["direction"] == "admin_to_user" else "\U0001f4e9"
            who = f"{ADMIN_NAME}" if m["direction"] == "admin_to_user" else name
            lines.append(f"<b>{num}.</b> {icon} <b>{who}</b> ({ts})")
            lines.append(f"  {esc(m['text'][:200])}")
            lines.append("")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n\n..."
        kb_rows = []
        nav = []
        if total_pages > 1:
            if page > 1:
                nav.append(InlineKeyboardButton(text="\u2b05\ufe0f", callback_data=f"dialog:{anon_id}:{page - 1}:{date_filter}" if date_filter else f"dialog:{anon_id}:{page - 1}"))
            nav.append(InlineKeyboardButton(text=f"\U0001f4c5 {page}/{total_pages}", callback_data="none"))
            if page < total_pages:
                nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"dialog:{anon_id}:{page + 1}:{date_filter}" if date_filter else f"dialog:{anon_id}:{page + 1}"))
        if nav:
            kb_rows.append(nav)
        kb_rows.append([
            InlineKeyboardButton(text="\U0001f4c5 Сегодня", callback_data=f"dialog:{anon_id}:1:today"),
            InlineKeyboardButton(text="\U0001f4c5 Вчера", callback_data=f"dialog:{anon_id}:1:yesterday"),
        ])
        kb_rows.append([InlineKeyboardButton(text="\U0001f4c5 Дата", callback_data=f"dialog_date_prompt:{anon_id}")])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        await bot.send_message(chat_id, text, reply_markup=kb)
        return

    elif action == "dialog_date_prompt":
        dialog_date_entry = anon_id
        await callback.answer()
        try: await callback.message.delete()
        except: pass
        await bot.send_message(
            callback.message.chat.id,
            "\U0001f4c5 <b>Введите дату</b> в формате ГГГГ-ММ-ДД\n\n"
            "Например: 2026-05-14\n"
            "/cancel — отменить"
        )
        return

    # ── Stale/removed callbacks (history_all was removed) ──
    if action in ("history_all", "history_date_prompt"):
        await callback.answer()
        try: await callback.message.delete()
        except: pass
        # Run cmd_history logic directly
        chat_id = callback.message.chat.id
        minutes = 60
        rows = db.get_messages_since(minutes)
        if not rows:
            await bot.send_message(chat_id, f"\U0001f4ad За последние <b>{minutes}</b> мин сообщений нет.")
            return
        lines = [f"\U0001f4dc <b>Сообщения за последние {minutes} мин:</b>\n"]
        current_id = None
        for r in rows:
            if r["anon_id"] != current_id:
                current_id = r["anon_id"]
                name = esc(r["first_name"] or f"#{current_id}")
                username = f" @{esc(r['username'])}" if r["username"] else ""
                lines.append(f"\n👤 #{current_id} \u2014 {name}{username}:")
            time = local_time(r["timestamp"])
            direction = r.get("direction", "user_to_admin")
            icon = "\u2709\ufe0f" if direction == "admin_to_user" else "\U0001f4e9"
            label = f"({time})"
            lines.append(f'  {icon} {label} "{esc(r["text"] or "")}"')
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n\n<i>...</i>"
        await bot.send_message(chat_id, text)
        return


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
BTN_WISDOM = "\U0001f4a1 Мудрость дня"
BTN_IDEAS = "\U0001f4a1 Идеи пользователей"
BTN_DIARY = "\U0001f4d6 Дневник Cookie"
BTN_BCAST = "\U0001f4e2 Рассылка"
BTN_GAME = "\U0001f3d3 Пинг-Понг"
BTN_HELP = "❓ Помощь"
BTN_CANCEL = "❌ Отмена"

GAME_URL = "https://cookie-anon-bot.onrender.com/game"


def admin_cmds_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_WRITE)],
            [KeyboardButton(text=BTN_HISTORY), KeyboardButton(text=BTN_STATS)],
            [KeyboardButton(text=BTN_LIST), KeyboardButton(text=BTN_BANNED)],
            [KeyboardButton(text=BTN_DELETED), KeyboardButton(text=BTN_BLOCKED)],
            [KeyboardButton(text=BTN_TTT), KeyboardButton(text=BTN_DICE)],
            [KeyboardButton(text=BTN_DEL), KeyboardButton(text=BTN_IDEAS)],
            [KeyboardButton(text=BTN_WISDOM), KeyboardButton(text=BTN_ADD_ID)],
            [KeyboardButton(text=BTN_BCAST)],
            [KeyboardButton(text=BTN_DIARY)],
            [KeyboardButton(text=BTN_GAME)],
            [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_CANCEL)],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


BTN_CMDS = {BTN_WRITE, BTN_HISTORY, BTN_STATS, BTN_LIST, BTN_BANNED,
            BTN_DELETED, BTN_DEL, BTN_BLOCKED, BTN_TTT, BTN_DICE, BTN_WISDOM, BTN_IDEAS, BTN_DIARY, BTN_ADD_ID, BTN_BCAST, BTN_GAME, BTN_HELP, BTN_CANCEL}


# ────────────────────────────── Messages ──────────────────────────────

@dp.message(lambda msg: msg.web_app_data is not None)
async def handle_web_app_data(message: Message):
    try:
        data = json.loads(message.web_app_data.data)
        action = data.get("action")

        # Challenge from within the game
        if action == "challenge":
            user_anon_id = data.get("user_anon_id")
            challenger_id = message.from_user.id
            if not user_anon_id:
                # No target specified → challenge Cookie (admin)
                anon_id = db.get_anon_id_by_user_id(challenger_id)
                if not anon_id:
                    await message.answer("❌ Ты не найден в базе. Напиши боту любое сообщение сначала.")
                    return
                accept_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Принять", callback_data=f"pong_accept:{anon_id}"),
                     InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pong_decline:{anon_id}")]
                ])
                await bot.send_message(ADMIN_ID, f"\U0001f3d3 <b>Пинг-Понг!</b>\n\nПользователь #{anon_id} хочет сыграть с тобой!", reply_markup=accept_kb)
                await message.answer("⏳ Ожидаем ответа от Cookie...")
                return
                # Admin challenges user directly
                user = db.get_user_by_anon(user_anon_id)
                if not user or row_get(user, "is_deleted"):
                    await message.answer(f"❌ Пользователь #{user_anon_id} не найден.")
                    return
                target_user_id = user["user_id"]
                room_id = f"pong_{user_anon_id}_{int(time.time())}"
                PONG_ROOMS[room_id] = {
                    "id": room_id, "p1_ws": None, "p2_ws": None,
                    "p1_y": 310, "p2_y": 310,
                    "ball_x": 250, "ball_y": 350,
                    "ball_vx": 0, "ball_vy": 0,
                    "p1_score": 0, "p2_score": 0,
                    "running": False, "loop_task": None,
                }
                admin_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=left"
                user_link = f"https://cookie-anon-bot.onrender.com/game?room={room_id}&side=right"
                await message.answer(f"\U0001f3d3 Вызов отправлен пользователю #{user_anon_id}!")
                try:
                    await bot.send_message(target_user_id, f"\U0001f3d3 <b>{ADMIN_NAME} вызвал тебя на Пинг-Понг!</b>", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=user_link))]
                    ]))
                    await bot.send_message(ADMIN_ID, f"\U0001f3d3 Игра с пользователем #{user_anon_id} создана!", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="\U0001f3d3 Открыть игру", web_app=WebAppInfo(url=admin_link))]
                    ]))
                except Exception as e:
                    await message.answer(f"❌ Не удалось отправить приглашение: {e}")
                return
            else:
                # Regular user challenges admin
                anon_id = db.get_anon_id_by_user_id(challenger_id)
                if not anon_id:
                    await message.answer("❌ Ты не найден в базе.")
                    return
                accept_kb = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="✅ Принять", callback_data=f"pong_accept:{anon_id}"),
                     InlineKeyboardButton(text="❌ Отклонить", callback_data=f"pong_decline:{anon_id}")]
                ])
                await bot.send_message(ADMIN_ID, f"\U0001f3d3 <b>Пинг-Понг!</b>\n\nПользователь #{anon_id} хочет сыграть с тобой!", reply_markup=accept_kb)
                await message.answer("⏳ Ожидаем ответа от Cookie...")
            return

        # Game result
        result = data.get("result")
        player_score = data.get("playerScore", 0)
        ai_score = data.get("aiScore", 0)
        user_id = message.from_user.id
        anon_id = db.get_anon_id_by_user_id(user_id)
        if anon_id:
            db.save_ping_pong(user_id, anon_id, result, player_score, ai_score)
            stats = db.get_ping_pong_stats(anon_id)
            if result == "win":
                msg = f"\U0001f3d3 <b>Пинг-Понг — Победа!</b> \U0001f389\n\n<b>Счёт:</b> {player_score}:{ai_score}\n<b>Всего игр:</b> {stats['total']}\n<b>Побед:</b> {stats['wins']}\n<b>Рекорд:</b> {stats['best']}"
            elif result == "lose":
                msg = f"\U0001f3d3 <b>Пинг-Понг — Поражение</b> \U0001f608\n\n<b>Счёт:</b> {player_score}:{ai_score}\n<b>Всего игр:</b> {stats['total']}\n<b>Побед:</b> {stats['wins']}\n<b>Рекорд:</b> {stats['best']}"
            else:
                msg = f"\U0001f3d3 <b>Пинг-Понг — Ничья</b> \U0001f91d\n\n<b>Счёт:</b> {player_score}:{ai_score}\n<b>Всего игр:</b> {stats['total']}\n<b>Побед:</b> {stats['wins']}\n<b>Рекорд:</b> {stats['best']}"
        else:
            msg = f"\U0001f3d3 <b>Пинг-Понг</b>\n\n<b>Счёт:</b> {player_score}:{ai_score}"
        play_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001f3d3 Играть снова", web_app=WebAppInfo(url=GAME_URL))]
        ])
        await message.answer(msg, reply_markup=play_kb)
    except Exception as e:
        logging.error(f"WebApp data error: {e}")
        await message.answer("❌ Ошибка обработки результата.")

@dp.message(Command("pingpong"))
async def cmd_pingpong(message: Message):
    game_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f916 Против AI", web_app=WebAppInfo(url=GAME_URL))],
        [InlineKeyboardButton(text="\U0001f451 Против Cookie", callback_data="pong_challenge")],
    ])
    stats = None
    anon_id = db.get_anon_id_by_user_id(message.from_user.id)
    if anon_id:
        stats = db.get_ping_pong_stats(anon_id)
    text = "\U0001f3d3 <b>Пинг-Понг</b>\n\nВыбери режим:"
    if stats and stats['total'] > 0:
        text += f"\n\n\U0001f4ca <b>Твоя статистика:</b>\n\U0001f3c6 Побед: {stats['wins']} | \U0001f4a2 Поражений: {stats['losses']}\n\U0001f3af Рекорд: {stats['best']} очков"
    await message.answer(text, reply_markup=game_kb)

@dp.message()
async def handle_user_message(message: Message):
    global admin_pending_reply, write_flow_step, write_flow_anon_id, add_user_step, rename_anon_id, admin_commenting_idea
    try:
        await _handle_user_message(message)
    except Exception as e:
        logging.error(f"Message handler error: {e}", exc_info=True)
        if is_admin(message.from_user.id):
            await message.answer(f"❌ Ошибка: {esc(str(e)[:200])}")
        else:
            try:
                await message.answer("🍪 Произошла ошибка. Попробуйте ещё раз.")
            except Exception:
                pass
        try:
            await bot.send_message(ADMIN_ID, f"❌ <b>Ошибка обработки сообщения</b>\nUID: <code>{message.from_user.id}</code>\n{esc(str(e)[:300])}")
        except Exception:
            pass


async def _handle_user_message(message: Message):
    global admin_pending_reply, write_flow_step, write_flow_anon_id, add_user_step, rename_anon_id, admin_commenting_idea, ban_user_step, admin_writing_diary, diary_editing, dialog_date_entry
    user_id = message.from_user.id

    if is_admin(user_id):
        if message.text and message.text.startswith("/"):
            return
        if message.text is None and admin_pending_reply is None and write_flow_step is None and not add_user_step and rename_anon_id is None:
            if message.photo or message.video or message.sticker or message.voice or message.document or message.animation:
                await message.answer(
                    "\U0001f4a1 Нажми <b>\u270d\ufe0f Ответить</b> под сообщением пользователя, чтобы отправить медиа.\n"
                    "Или используй <b>\u270d\ufe0f Написать</b> в меню."
                )
            return

        if write_flow_step == "await_id":
            if message.text is None:
                await message.answer("❌ Пожалуйста, введи ID числом.")
                return
            if message.text == BTN_CANCEL or message.text == "/cancel":
                write_flow_step = None
                await cmd_cancel(message)
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
            if message.text == BTN_CANCEL or message.text == "/cancel":
                write_flow_step = None
                write_flow_anon_id = None
                await cmd_cancel(message)
                return
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
            if message.text == BTN_CANCEL or message.text == "/cancel":
                add_user_step = False
                await cmd_cancel(message)
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
            if message.text == BTN_CANCEL or message.text == "/cancel":
                rename_anon_id = None
                await cmd_cancel(message)
                return
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

        # ── Admin ban reason ──
        if ban_user_step is not None:
            if message.text == BTN_CANCEL or message.text == "/cancel":
                ban_user_step = None
                await cmd_cancel(message)
                return
            reason = (message.text or "").strip()
            if reason:
                anon_id = ban_user_step
                ban_user_step = None
                uid = db.get_user_id_by_anon(anon_id)
                if uid:
                    db.ban_user_with_reason(uid, reason)
                    await delete_waiting(uid)
                    await message.answer(f"✅ Пользователь #<b>{anon_id}</b> заблокирован. Причина: {esc(reason[:200])}")
                    try:
                        await bot.send_message(uid, f"\U0001f6ab <b>Вы заблокированы.</b>\n\nПричина: {esc(reason[:500])}")
                    except Exception:
                        pass
                else:
                    await message.answer("❌ Пользователь не найден.")
            else:
                await message.answer("❌ Причина не может быть пустой.")
            return

        # ── Admin commenting on idea ──
        if admin_commenting_idea is not None:
            if message.text == BTN_CANCEL or message.text == "/cancel":
                admin_commenting_idea = None
                await cmd_cancel(message)
                return
            comment = (message.text or "").strip()
            if comment:
                idea_id = admin_commenting_idea
                admin_commenting_idea = None
                db.update_idea(idea_id, "accepted", comment)
                # Try to send comment to user
                ideas = db.get_ideas()
                uid = None
                for s in ideas:
                    if s["id"] == idea_id:
                        uid = s["user_id"]
                        break
                if uid:
                    try:
                        await bot.send_message(
                            uid,
                            f"\U0001f4ac <b>Cookie оставил комментарий к вашей идее:</b>\n\n{esc(comment[:500])}"
                        )
                        await message.answer(f"✅ Комментарий к идее #{idea_id} отправлен пользователю.")
                    except Exception:
                        await message.answer(f"✅ Комментарий сохранён, но не удалось доставить пользователю.")
                else:
                    await message.answer(f"✅ Комментарий добавлен к идее #{idea_id}.")
            else:
                await message.answer("❌ Комментарий не может быть пустым.")
            return

        # ── Diary editing ──
        if diary_editing is not None:
            if message.text == BTN_CANCEL or message.text == "/cancel":
                diary_editing = None
                await cmd_cancel(message)
                return
            new_text = (message.text or "").strip()
            if new_text:
                entry_id = diary_editing
                diary_editing = None
                db.update_diary_entry(entry_id, new_text)
                await message.answer(f"✅ Запись #{entry_id} обновлена!")
            else:
                await message.answer("❌ Текст не может быть пустым.")
            return

        # ── Dialog date entry ──
        if dialog_date_entry is not None:
            if message.text == BTN_CANCEL or message.text == "/cancel":
                dialog_date_entry = None
                await cmd_cancel(message)
                return
            date_str = (message.text or "").strip()
            import re
            if re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
                anon_id = dialog_date_entry
                dialog_date_entry = None
                msgs, total_pages, _ = db.get_user_messages(anon_id, 1, date_filter=date_str)
                name = esc(db.get_user_by_anon(anon_id)["first_name"] or f"#{anon_id}")
                try: await message.delete()
                except: pass
                chat_id = message.chat.id
                if not msgs:
                    await bot.send_message(chat_id, f"\U0001f4dc <b>Диалог с {name}</b>\n\nНет сообщений за {date_str}.")
                    return
                lines = [f"\U0001f4dc <b>Диалог с {name}</b> ({date_str}) стр. 1/{total_pages}:\n"]
                for i, m in enumerate(msgs):
                    num = i + 1
                    ts = local_time(m["timestamp"])
                    icon = "\u2709\ufe0f" if m["direction"] == "admin_to_user" else "\U0001f4e9"
                    who = f"{ADMIN_NAME}" if m["direction"] == "admin_to_user" else name
                    lines.append(f"<b>{num}.</b> {icon} <b>{who}</b> ({ts})")
                    lines.append(f"  {esc(m['text'][:200])}")
                    lines.append("")
                text = "\n".join(lines)
                if len(text) > 4000: text = text[:4000] + "\n\n..."
                kb = None
                if total_pages > 1:
                    nav = []
                    if 1 < total_pages:
                        nav.append(InlineKeyboardButton(text="\u27a1\ufe0f", callback_data=f"dialog:{anon_id}:2:{date_str}"))
                    kb = InlineKeyboardMarkup(inline_keyboard=[nav])
                await bot.send_message(chat_id, text, reply_markup=kb)
            else:
                await message.answer("❌ Неверный формат. Используйте ГГГГ-ММ-ДД (например 2026-05-14)")
            return

        if admin_writing_diary:
            if message.text == BTN_CANCEL or message.text == "/cancel":
                admin_writing_diary = False
                await cmd_cancel(message)
                return
            entry_text = (message.text or "").strip()
            if entry_text:
                admin_writing_diary = False
                entry_id = db.add_diary_entry(entry_text)
                await message.answer(f"✅ Запись #{entry_id} добавлена в дневник!")
                # Notify all users about new entry
                users = db.get_all_users()
                sent = 0
                for u in users:
                    notify = row_get(u, "diary_notify", 1)
                    if not notify:
                        continue
                    try:
                        await bot.send_message(
                            u["user_id"],
                            f"\U0001f4d6 <b>Новая запись в дневнике Cookie!</b>\n\n"
                            f"{esc(entry_text[:100])}{'...' if len(entry_text) > 100 else ''}\n\n"
                            f"Напиши «дневник» чтобы прочитать полностью."
                        )
                        sent += 1
                        await asyncio.sleep(0.05)
                    except Exception:
                        pass
                await message.answer(f"\U0001f4e2 Уведомление отправлено {sent} пользователям.")
            else:
                await message.answer("❌ Запись не может быть пустой.")
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
        if message.text == BTN_WISDOM:
            await message.answer(f"\U0001f4a1 <b>Мудрость дня</b>\n\n{wisdom_of_the_day(ADMIN_ANON_ID)}")
            return
        if message.text == BTN_IDEAS:
            ideas = db.get_ideas()
            if not ideas:
                await message.answer("\U0001f4a1 <b>Идей пока нет.</b>")
                return
            lines = ["\U0001f4a1 <b>Идеи пользователей</b>\n"]
            for s in ideas:
                sid = s["id"]
                status_emoji = "✅" if s["status"] == "accepted" else "\u23f3" if s["status"] == "pending" else "\u274c"
                name = esc(s["first_name"] or f"#{s['anon_id']}")
                text = esc(s["text"][:80])
                lines.append(f"{status_emoji} #{sid} — {name}: {text}")
            text = "\n".join(lines)
            if len(text) > 4000:
                text = text[:4000] + "\n\n..."
            await message.answer(text)
            return
        if message.text == BTN_DIARY:
            diary_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="\u270f\ufe0f Написать запись", callback_data="diary_write"),
                 InlineKeyboardButton(text="\U0001f4d6 Читать дневник", callback_data="diary_read:1")],
            ])
            await message.answer("\U0001f4d6 <b>Дневник Cookie</b>\n\nЗаписывай свои мысли — пользователи смогут их читать.", reply_markup=diary_kb)
            return
        if message.text == BTN_GAME:
            game_kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="\U0001f916 Против AI", web_app=WebAppInfo(url=GAME_URL))],
                [InlineKeyboardButton(text="\U0001f451 Против Cookie", callback_data="pong_challenge")],
            ])
            await message.answer("\U0001f3d3 <b>Пинг-Понг</b>\n\nВыбери режим:", reply_markup=game_kb)
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
        ban_user_data = db.get_user(user_id)
        ban_reason = ""
        if ban_user_data:
            try:
                br = ban_user_data["ban_reason"]
                if br:
                    ban_reason = f"\n\n\u2139\ufe0f Причина: {esc(str(br)[:200])}"
            except:
                pass
        appeal_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подать апелляцию", callback_data=f"appeal:{ban_anon}"),
             InlineKeyboardButton(text="❌ Нет", callback_data="none")]
        ])
        await message.answer(
            f"\U0001f6ab <b>Вы заблокированы и не можете писать.</b>{ban_reason}\n\n"
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

    # Register user first so we have anon_id
    user = message.from_user
    anon_id, is_banned = db.add_user(
        user_id,
        user.first_name or "",
        user.username or "",
        user.language_code or "",
    )

    # ── User TTT challenge initiation ──
    user_msg_text = (message.text or message.caption or "").lower()
    if any(kw in user_msg_text for kw in GAME_KEYWORDS):
        challenge_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001f3ae Бросить вызов", callback_data=f"user_ttt:{anon_id}")]
        ])
        await message.answer("\U0001f3ae Хотите бросить вызов Cookie?", reply_markup=challenge_kb)
        return

    # Wisdom keyword detection
    user_msg_lower = user_msg_text
    if any(kw in user_msg_lower for kw in {"мудрость", "цитата", "мудрости"}):
        await message.answer(f"\U0001f4a1 <b>Мудрость дня</b>\n\n{wisdom_of_the_day(anon_id)}")
        return

    # ── Diary notify toggle ──
    if any(kw in user_msg_lower for kw in {"уведомления", "уведомление", "notify"}):
        dn = db._fetchone("SELECT diary_notify FROM users WHERE user_id = ?", [user_id])
        now_on = not (dn and dn["diary_notify"])
        db._exec("UPDATE users SET diary_notify = ? WHERE user_id = ?", [1 if now_on else 0, user_id])
        status = "\u2705 включены" if now_on else "\u274c выключены"
        await message.answer(f"\U0001f4d6 <b>Уведомления о новых записях в дневнике {status}.</b>\n\nНапиши «дневник» чтобы читать дневник.")
        return

    # ── Diary keyword detection ──
    if any(kw in user_msg_lower for kw in {"дневник", "дневник cookie", "дневник cookies"}):
        entries, total_pages = db.get_diary_entries(1)
        if not entries:
            await message.answer("\U0001f4d6 <b>Дневник Cookie пока пуст.</b>")
            return
        lines = [f"\U0001f4d6 <b>Дневник Cookie</b> (стр. 1/{total_pages}):\n"]
        for e in entries:
            ts = local_time(e["created_at"])
            lines.append(f"<b>{ts}</b>")
            lines.append(f"{esc(e['text'][:500])}")
            lines.append("")
        text = "\n".join(lines)
        if len(text) > 4000:
            text = text[:4000] + "\n\n..."
        kb_rows = []
        if total_pages > 1:
            kb_rows.append([
                InlineKeyboardButton(text="\u27a1\ufe0f Далее", callback_data="diary_pgn:2")
            ])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
        await message.answer(text, reply_markup=kb)
        return

    # ── User telling an idea ──
    if user_id in user_telling_idea:
        today_count = db.count_today_ideas(user_id)
        if today_count >= 3:
            user_telling_idea.discard(user_id)
            await message.answer("\u26a0\ufe0f <b>Лимит идей на сегодня исчерпан.</b> (3/3)\n\nПопробуйте завтра!")
            return
        idea_text = (message.text or "").strip()
        if idea_text:
            user_telling_idea.discard(user_id)
            idea_id = db.save_idea(anon_id, user_id, idea_text)
            await message.answer("✅ Ваша идея отправлена!\n\n\U0001f36a Cookie рассматривает вашу идею. Ожидайте ответа.")
            idea_kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Принять", callback_data=f"idea_accept:{idea_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"idea_reject:{idea_id}"),
                ],
                [InlineKeyboardButton(text="\U0001f4ac Комментарий", callback_data=f"idea_comment:{idea_id}")],
            ])
            await bot.send_message(
                ADMIN_ID,
                f"\U0001f4a1 <b>Новая идея</b>\n\n"
                f"🆔 #{anon_id} {esc(user.first_name or '')}\n"
                f"Идея: {esc(idea_text[:500])}",
                reply_markup=idea_kb,
            )
        else:
            await message.answer("❌ Идея не может быть пустой. Напишите вашу идею.")
        return

    # ── Idea keyword detection ──
    if any(kw in user_msg_lower for kw in {"идея", "предложение", "улучшение"}):
        today_count = db.count_today_ideas(user_id)
        if today_count >= 3:
            await message.answer("\u26a0\ufe0f <b>Лимит идей на сегодня исчерпан.</b>\n\nВы можете предложить не более 3 идей в день. Попробуйте завтра!")
            return
        user_telling_idea.add(user_id)
        await message.answer(
            f"\U0001f4a1 <b>Расскажите вашу идею</b> ({today_count + 1}/3)\n\n"
            "Напишите, что бы вы хотели улучшить в боте.\n"
            "Cookie рассмотрит ваше предложение!"
        )
        return

    last_admin_msg = db.get_last_admin_message(user_id)
    if last_admin_msg:
        reply_context = f"\U0001f4ce <b>Ответ на ваше сообщение:</b>\n\u2514 {esc(last_admin_msg[:500])}"
    else:
        reply_context = None

    info_lines = [
        f"\U0001f4e9 <b>Новое сообщение</b>",
        f"\U0001f194 Анонимный ID: <b>#{anon_id}</b>",
    ]
    # Use stored name if renamed by admin
    stored_user = db.get_user_by_anon(anon_id)
    display_name = esc(stored_user["first_name"]) if stored_user and stored_user["first_name"] else esc(user.first_name or "\u2014")
    info_lines.append(f"👤 {display_name}")
    if user.username:
        info_lines.append(f"\U0001f517 @{esc(user.username)}")
    info_lines.append(f"\U0001f511 ID: <code>{user_id}</code>")
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

    wait_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="\U0001f4a1 Мудрость дня", callback_data=f"wisdom:{anon_id}")]
    ])
    wait_msg = await message.answer(wait_text, reply_markup=wait_kb)
    waiting_messages[user_id] = wait_msg.message_id


# ────────── WebSocket game rooms ────────────────────────────

PONG_ROOMS: dict[str, dict] = {}

def create_pong_room(room_id: str) -> dict:
    room = {
        "id": room_id,
        "p1_ws": None, "p2_ws": None,
        "p1_y": 310, "p2_y": 310,
        "ball_x": 250, "ball_y": 350,
        "ball_vx": 0, "ball_vy": 0,
        "p1_score": 0, "p2_score": 0,
        "running": False,
        "loop_task": None,
    }
    PONG_ROOMS[room_id] = room
    return room

async def pong_game_loop(room_id: str):
    room = PONG_ROOMS.get(room_id)
    if not room:
        return
    W, H = 500, 700
    PADDLE_H = 80
    BALL_R = 8
    WIN_SCORE = 7
    speed = 4.5
    angle = (random.random() * 0.8 - 0.4)
    room["ball_vx"] = (1 if random.random() > 0.5 else -1) * math.cos(angle) * speed
    room["ball_vy"] = math.sin(angle) * speed
    room["ball_x"] = W / 2
    room["ball_y"] = H / 2
    room["running"] = True
    while room["running"]:
        room["ball_x"] += room["ball_vx"]
        room["ball_y"] += room["ball_vy"]
        # Wall bounce
        if room["ball_y"] - BALL_R < 0:
            room["ball_y"] = BALL_R
            room["ball_vy"] = -room["ball_vy"]
        if room["ball_y"] + BALL_R > H:
            room["ball_y"] = H - BALL_R
            room["ball_vy"] = -room["ball_vy"]
        # Paddle collisions
        px = 20
        if room["ball_x"] - BALL_R < px + 10 and room["ball_x"] - BALL_R > px and room["ball_y"] > room["p1_y"] and room["ball_y"] < room["p1_y"] + PADDLE_H:
            room["ball_x"] = px + 10 + BALL_R
            room["ball_vx"] = -room["ball_vx"]
            speed = min(speed + 0.1, 7)
            room["ball_vx"] = (1 if room["ball_vx"] > 0 else -1) * speed
            rel = (room["ball_y"] - room["p1_y"]) / PADDLE_H - 0.5
            room["ball_vy"] += rel * 1.2
        px2 = W - 20 - 10
        if room["ball_x"] + BALL_R > px2 and room["ball_x"] + BALL_R < px2 + 10 and room["ball_y"] > room["p2_y"] and room["ball_y"] < room["p2_y"] + PADDLE_H:
            room["ball_x"] = px2 - BALL_R
            room["ball_vx"] = -room["ball_vx"]
            speed = min(speed + 0.1, 7)
            room["ball_vx"] = (1 if room["ball_vx"] > 0 else -1) * speed
            rel = (room["ball_y"] - room["p2_y"]) / PADDLE_H - 0.5
            room["ball_vy"] += rel * 1.2
        # Scoring
        if room["ball_x"] < 0:
            room["p2_score"] += 1
            if room["p2_score"] >= WIN_SCORE:
                room["running"] = False
                await broadcast_room(room_id, {"type": "game_over", "winner": "p2", "scores": [room["p1_score"], room["p2_score"]]})
                break
            reset_pong_ball(room, 1)
        if room["ball_x"] > W:
            room["p1_score"] += 1
            if room["p1_score"] >= WIN_SCORE:
                room["running"] = False
                await broadcast_room(room_id, {"type": "game_over", "winner": "p1", "scores": [room["p1_score"], room["p2_score"]]})
                break
            reset_pong_ball(room, -1)
        # Broadcast state
        state = {
            "type": "state",
            "ball_x": room["ball_x"], "ball_y": room["ball_y"],
            "p1_y": room["p1_y"], "p2_y": room["p2_y"],
            "p1_score": room["p1_score"], "p2_score": room["p2_score"],
        }
        await broadcast_room(room_id, state)
        await asyncio.sleep(0.016)  # ~60 FPS

def reset_pong_ball(room, dir_sign):
    import math
    room["ball_x"] = 250
    room["ball_y"] = 350
    speed = 4.5
    angle = (random.random() * 0.8 - 0.4)
    room["ball_vx"] = dir_sign * math.cos(angle) * speed
    room["ball_vy"] = math.sin(angle) * speed

async def broadcast_room(room_id: str, data: dict):
    msg = json.dumps(data)
    room = PONG_ROOMS.get(room_id)
    if not room:
        return
    for ws in (room.get("p1_ws"), room.get("p2_ws")):
        if ws and not ws.closed:
            try:
                await ws.send_str(msg)
            except Exception:
                pass

# ────────── Web server (aiohttp) ─────────────────────────────

GAME_HTML_PATH = os.path.join(os.path.dirname(__file__), "public", "game.html")

async def handle_index(request):
    return web.Response(text="OK")

async def handle_game(request):
    try:
        with open(GAME_HTML_PATH, "rb") as f:
            data = f.read()
        return web.Response(body=data, content_type="text/html; charset=utf-8")
    except FileNotFoundError:
        return web.Response(status=404)

async def handle_api_users(request):
    users = db.get_all_users()
    data = [{"id": u["id"], "name": u["first_name"] or f"#{u['id']}", "username": u.get("username") or ""} for u in users if not u.get("is_deleted")]
    return web.json_response(data)

async def handle_websocket(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    room_id = request.query.get("room")
    side = request.query.get("side", "left")
    if not room_id or room_id not in PONG_ROOMS:
        await ws.send_json({"type": "error", "msg": "Room not found"})
        await ws.close()
        return ws
    room = PONG_ROOMS[room_id]
    if side == "left":
        if room["p1_ws"] and not room["p1_ws"].closed:
            await ws.send_json({"type": "error", "msg": "Player 1 already connected"})
            await ws.close()
            return ws
        room["p1_ws"] = ws
    else:
        if room["p2_ws"] and not room["p2_ws"].closed:
            await ws.send_json({"type": "error", "msg": "Player 2 already connected"})
            await ws.close()
            return ws
        room["p2_ws"] = ws
    await ws.send_json({"type": "joined", "side": side})
    # Start game if both connected
    if room["p1_ws"] and not room["p1_ws"].closed and room["p2_ws"] and not room["p2_ws"].closed:
        await broadcast_room(room_id, {"type": "countdown", "value": 3})
        await asyncio.sleep(1)
        await broadcast_room(room_id, {"type": "countdown", "value": 2})
        await asyncio.sleep(1)
        await broadcast_room(room_id, {"type": "countdown", "value": 1})
        await asyncio.sleep(1)
        await broadcast_room(room_id, {"type": "start"})
        room["loop_task"] = asyncio.create_task(pong_game_loop(room_id))
    # Listen for paddle moves
    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "move":
                    y = data["y"]
                    if side == "left":
                        room["p1_y"] = y
                    else:
                        room["p2_y"] = y
            elif msg.type == aiohttp.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        # Clean up
        if room_id in PONG_ROOMS:
            room = PONG_ROOMS[room_id]
            if side == "left":
                room["p1_ws"] = None
            else:
                room["p2_ws"] = None
            room["running"] = False
            if room["loop_task"]:
                room["loop_task"].cancel()
            # Remove room if both disconnected
            if not room["p1_ws"] and not room["p2_ws"]:
                del PONG_ROOMS[room_id]
    return ws

async def run_web_server():
    app = web.Application()
    app.router.add_get("/health", handle_index)
    app.router.add_get("/", handle_index)
    app.router.add_get("/game", handle_game)
    app.router.add_get("/game.html", handle_game)
    app.router.add_get("/api/users", handle_api_users)
    app.router.add_get("/ws", handle_websocket)
    web_port = int(os.getenv("PORT", 8080))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", web_port)
    await site.start()
    logging.info(f"\U0001fa7a Web server on :{web_port}")
    await asyncio.Event().wait()


async def daily_wisdom_task():
    while True:
        now = datetime.now()
        target = now.replace(hour=10, minute=0, second=0, microsecond=0)
        if now >= target:
            target = target.replace(day=target.day + 1)
        wait_sec = (target - now).total_seconds()
        logging.info(f"\U0001f4a1 Daily wisdom scheduled in {wait_sec:.0f}s")
        await asyncio.sleep(wait_sec)

        users = db.get_all_users()
        sent = 0
        for u in users:
            wid = wisdom_of_the_day(u["id"])
            try:
                await bot.send_message(u["user_id"], f"\U0001f4a1 <b>Мудрость дня</b>\n\n{wid}")
                sent += 1
                await asyncio.sleep(0.05)
            except Exception:
                pass
        logging.info(f"\U0001f4a1 Daily wisdom sent to {sent} users")


async def keep_awake_task():
    """Ping Render health endpoint every 10 minutes to prevent sleeping."""
    self_url = "https://cookie-anon-bot.onrender.com/health"
    while True:
        await asyncio.sleep(600)  # 10 minutes
        try:
            import urllib.request
            urllib.request.urlopen(self_url, timeout=10)
        except Exception:
            pass


# ────────────────────────────── Entry ──────────────────────────

async def main():
    http_task = asyncio.create_task(run_web_server())
    wisdom_task = asyncio.create_task(daily_wisdom_task())
    keep_awake = asyncio.create_task(keep_awake_task())
    while True:
        try:
            logging.info("\U0001f680 Бот запущен!")
            await dp.start_polling(bot, skip_updates=True)
        except Exception as e:
            err_str = str(e).lower()
            if "conflict" in err_str:
                logging.info("Conflict — другой экземпляр, переподключение через 1с...")
                await asyncio.sleep(1)
            else:
                logging.error(f"Критическая ошибка: {e}", exc_info=True)
                logging.info("Перезапуск через 5 секунд...")
                await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
