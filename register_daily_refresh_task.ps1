[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [string]$TaskName = "StockAnalyzerDailyRefresh",
    [string]$Description = "Refresh stock-analyzer daily risk cache at 08:30 and 12:30.",
    [string]$PythonArgs = "",
    [switch]$Force
)

$ErrorActionPreference = "Stop"

$rootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$batchPath = Join-Path $rootDir "run_daily_refresh.bat"
$hiddenLauncherPath = Join-Path $rootDir "run_daily_refresh_hidden.vbs"

if (-not (Test-Path -LiteralPath $batchPath)) {
    throw "Batch file not found: $batchPath"
}

if (-not (Test-Path -LiteralPath $hiddenLauncherPath)) {
    throw "Hidden launcher not found: $hiddenLauncherPath"
}

$actionArgs = @("//B", "//NoLogo", "`"$hiddenLauncherPath`"")
$descriptionSuffix = ""
if ($PythonArgs) {
    $descriptionSuffix = " Extra args are not supported in hidden mode."
}

$action = New-ScheduledTaskAction -Execute "wscript.exe" -Argument ($actionArgs -join " ") -WorkingDirectory $rootDir
$triggerMorning = New-ScheduledTaskTrigger -Daily -At "08:30"
$triggerNoon = New-ScheduledTaskTrigger -Daily -At "12:30"
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -Hidden

$existingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
if ($existingTask -and -not $Force) {
    throw "Scheduled task '$TaskName' already exists. Re-run with -Force to replace it."
}

if ($existingTask -and $Force) {
    if ($PSCmdlet.ShouldProcess($TaskName, "Unregister existing scheduled task")) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
}

if ($PSCmdlet.ShouldProcess($TaskName, "Register scheduled task")) {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $Description `
        -Action $action `
        -Trigger @($triggerMorning, $triggerNoon) `
        -Settings $settings | Out-Null
}

Write-Host "Task name: $TaskName"
Write-Host "Command : wscript.exe $($actionArgs -join ' ')"
Write-Host "Triggers: daily at 08:30 and 12:30"
if ($descriptionSuffix) {
    Write-Host $descriptionSuffix
}
