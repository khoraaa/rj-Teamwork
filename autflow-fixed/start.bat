@echo off
cd /d "%~dp0"
setlocal enabledelayedexpansion

set "PORT=8010"
set "URL=http://127.0.0.1:%PORT%"

echo ============================================================
echo  AutoFlow workflow orchestrator prototype
echo  %URL%
echo  Keep this window OPEN while using the page.
echo ============================================================
echo.

netstat -ano | findstr /r /c:":%PORT% .*LISTENING" >nul 2>nul
if not errorlevel 1 (
    echo Server already running - opening browser.
    if not "%AUTOFLOW_NO_BROWSER%"=="1" start "" "%URL%"
    timeout /t 2 >nul
    exit /b 0
)

set "PYCMD="
py -3 -c "import sys" >nul 2>nul
if not errorlevel 1 set "PYCMD=py -3"

if not defined PYCMD (
    if exist "%USERPROFILE%\.dsh\dsh-runtimes\dsh-primary-runtime\dependencies\python\python.exe" (
        set "PYCMD=%USERPROFILE%\.dsh\dsh-runtimes\dsh-primary-runtime\dependencies\python\python.exe"
    )
)

if not defined PYCMD (
    python -c "import sys" >nul 2>nul
    if not errorlevel 1 set "PYCMD=python"
)

if not defined PYCMD (
    echo [ERROR] No usable Python 3 found.
    echo         Install Python 3.9+ from https://www.python.org/downloads/
    echo         tick "Add python.exe to PATH" during setup, then run this file again.
    echo.
    pause
    exit /b 1
)

echo Using interpreter: %PYCMD%
if not "%AUTOFLOW_NO_BROWSER%"=="1" (
    echo Starting... browser opens in 2 seconds.
    start "" cmd /c "timeout /t 2 >nul & start %URL%"
)
echo.

%PYCMD% server.py --port %PORT%

echo.
echo Server stopped. Press any key to close.
pause >nul
endlocal
