@echo off
REM setup_daily_fetch_win.bat — Install a Windows Task Scheduler job
REM to auto-fetch daily MLB data and push to GitHub.
REM
REM Usage (run as Administrator):
REM   cd C:\path\to\mlb-nsfi-model
REM   setup_daily_fetch_win.bat
REM
REM To uninstall:
REM   schtasks /delete /tn "MLB-NSFI-Fetch" /f

setlocal

set "REPO_DIR=%~dp0"
set "VENV_DIR=%REPO_DIR%.venv"
set "SCRIPT_PATH=%REPO_DIR%fetch_and_push.bat"
set "PYTHON=%VENV_DIR%\Scripts\python.exe"

echo Setting up MLB NSFI daily fetch...
echo   Repo: %REPO_DIR%

REM ── 1. Create venv if needed ──────────────────────────────────────────────

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo   Creating Python virtual environment...
    python -m venv "%VENV_DIR%"
)
echo   Installing/updating dependencies...
"%PYTHON%" -m pip install --quiet --upgrade requests selenium
echo   Installed dependencies.

REM ── 2. Create the fetch-and-push script ───────────────────────────────────

(
echo @echo off
echo cd /d "%REPO_DIR%"
echo.
echo REM Fetch today's data
echo "%PYTHON%" fetch_daily.py
echo.
echo REM Commit and push if changed
echo for /f "tokens=1-3 delims=-" %%%%a in ('powershell -command "Get-Date -Format yyyy-MM-dd"'^) do set "DDATE=%%%%a-%%%%b-%%%%c"
echo for /f "tokens=1-3 delims=-" %%%%a in ('powershell -command "Get-Date -Format yyyyMMdd"'^) do set "DFILE=%%%%a%%%%b%%%%c"
echo.
echo if exist "daily_%%DFILE%%.json" (
echo     git add "daily_%%DFILE%%.json"
echo     git diff --cached --quiet
echo     if errorlevel 1 (
echo         git commit -m "Auto-fetch daily data for %%DDATE%%"
echo         git push
echo     ^)
echo ^)
) > "%SCRIPT_PATH%"

echo   Created fetch script: %SCRIPT_PATH%

REM ── 3. Create scheduled tasks (1 PM and 5:30 PM) ─────────────────────────

schtasks /create /tn "MLB-NSFI-Fetch-1PM" /tr "\"%SCRIPT_PATH%\"" /sc daily /st 13:00 /f >nul 2>&1
schtasks /create /tn "MLB-NSFI-Fetch-530PM" /tr "\"%SCRIPT_PATH%\"" /sc daily /st 17:30 /f >nul 2>&1

echo   Created scheduled tasks.
echo.
echo Done! Daily fetch will run at 1:00 PM and 5:30 PM.
echo.
echo Useful commands:
echo   Run manually:  "%SCRIPT_PATH%"
echo   View tasks:    schtasks /query /tn "MLB-NSFI-Fetch*"
echo   Uninstall:     schtasks /delete /tn "MLB-NSFI-Fetch-1PM" /f ^& schtasks /delete /tn "MLB-NSFI-Fetch-530PM" /f

endlocal
