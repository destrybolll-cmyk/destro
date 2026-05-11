$taskName = "AnonBot24-7"
$botDir = "C:\Users\Victus\Downloads\Тг бот анон"
$vbsPath = "$botDir\run_bot.vbs"

# Create scheduled task
$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument "`"$vbsPath`"" -WorkingDirectory $botDir
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit 0

$principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest

Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force

Start-ScheduledTask -TaskName $taskName

Write-Host "✅ Задача '$taskName' установлена и запущена!"
Write-Host "📌 Бот будет автоматически запускаться при загрузке Windows."
Write-Host "📌 Если бот упадёт, он перезапустится через 1 минуту (до 3 раз)."
