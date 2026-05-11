# Деплой Cookie Anon Bot на Alwaysdata (Free)

## 1. Регистрация

1. Перейти на https://www.alwaysdata.com/en/register/
2. Заполнить: Email, Пароль, Имя пользователя
3. Подтвердить email (придёт письмо)
4. Войти в админ-панель: https://admin.alwaysdata.com

## 2. Загрузка файлов

Через **Remote Access > FTP** (или через файловый менеджер **Web > Files**):

1. Создать директорию: `/home/{username}/www/cookie_anon_bot/`
2. Загрузить файлы из `cookie_anon_bot.zip`:
   - `main.py`
   - `database.py`
   - `config.py`
   - `requirements.txt`
   - `app.py`
3. **ВНИМАНИЕ**: .env и bot.db НЕ загружать (секреты задаются через админку, БД создатся автоматически)

## 3. Установка зависимостей

Через **Environment > Python**:
1. Выбрать Python 3.12
2. Указать `requirements.txt` в `$HOME/www/cookie_anon_bot/requirements.txt`

Либо через SSH (если доступен) выполнить:
```
cd ~/www/cookie_anon_bot
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 4. Настройка переменных окружения

Через **Environment > Environment variables** добавить:

| Имя | Значение |
|-----|----------|
| `BOT_TOKEN` | `8657629778:AAGy1llDj3BF6xVmpTBwT3txImx3nKhpBIY` |
| `ADMIN_ID` | `1227929365` |

## 5. Создание задачи (Daemon)

Через **Advanced > Scheduled tasks > Add a task**:

- **Type**: Daemon (или Command)
- **Command**: `cd $HOME/www/cookie_anon_bot && python main.py`
- **Frequency**: выбрать первую опцию (или "Always running" если есть)
- Если нет "Daemon" типа — выбрать "Command" с интервалом "Every minute" — но это не идеально. Лучше написать в поддержку.

Если тип "Daemon" недоступен на бесплатном тарифе:

### Альтернатива A: Python Site
Создать **Web > Sites > Add a site**:
- Type: "Python"
- Command: `python $HOME/www/cookie_anon_bot/main.py`
- Адрес: любой (бот не HTTP)

### Альтернатива B: JustRunMy.App (запасной вариант)
Если Alwaysdata не подошёл — деплой на https://justrunmy.app:
1. Зарегистрироваться (без карты)
2. Создать приложение через "Zip Upload"
3. Загрузить `cookie_anon_bot.zip`
4. Установить BOT_TOKEN и ADMIN_ID в Environment Variables
5. Запустить

## 6. Проверка

1. Написать боту в Telegram: @cookei_anon_bot
2. Должен ответить приветствием и показать клавиатуру
3. Написать любое сообщение — должно прийти админу (Cookie)
4. Админ может ответить через кнопку "Ответить" под сообщением

## 7. Файлы для деплоя

Готовый архив: `C:\Users\Victus\AppData\Local\Temp\cookie_anon_bot.zip`

### Состав:
- `main.py` — вся логика бота
- `database.py` — SQLite база
- `config.py` — конфиг (читает BOT_TOKEN, ADMIN_ID из env)
- `requirements.txt` — зависимости (aiogram, python-dotenv)
- `app.py` — точка входа (import + asyncio.run(main()))
