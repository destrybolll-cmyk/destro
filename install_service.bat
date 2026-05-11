@echo off
chcp 65001 >nul
title Установка бота 24/7

echo ============================================
echo   Установка анонимного бота 24/7
echo ============================================
echo.
echo Проверка прав администратора...

net session >nul 2>&1
if %errorLevel% neq 0 (
    echo.
    echo [!] Этот скрипт нужно запускать от имени Администратора!
    echo [!] Нажмите правой кнопкой мыши и выберите "Запуск от имени администратора"
    echo.
    pause
    exit /b 1
)

echo [OK] Права администратора получены.
echo.

set "BOT_DIR=C:\Users\Victus\Downloads\Тг бот анон"
set "TASK_NAME=AnonBot24-7"

echo [1/3] Удаление старой задачи (если есть)...
schtasks /DELETE /TN "%TASK_NAME%" /F >nul 2>&1

echo [2/3] Создание новой задачи...
schtasks /CREATE ^
    /SC ONSTART ^
    /TN "%TASK_NAME%" ^
    /TR "wscript.exe \"%BOT_DIR%\run_bot.vbs\"" ^
    /RU SYSTEM ^
    /RL HIGHEST ^
    /F

if %errorLevel% neq 0 (
    echo [ERR] Не удалось создать задачу!
    pause
    exit /b 1
)

echo [3/3] Запуск задачи...
schtasks /RUN /TN "%TASK_NAME%"

echo.
echo ============================================
echo   [OK] Бот установлен и запущен!
echo ============================================
echo.
echo   - Бот будет запускаться при загрузке Windows
echo   - Работает 24/7 в фоновом режиме
echo   - Если упадёт — перезапустится сам
echo.
echo   Для проверки: открой Telegram и напиши боту
echo.
pause
