@echo off
setlocal

set "ROOT_DIR=%~dp0"
cd /d "%ROOT_DIR%"

python skills\stock-analyzer\scripts\refresh_daily_risks.py --bucket positions %*

endlocal
