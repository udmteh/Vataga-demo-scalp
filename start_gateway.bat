@echo off
title VATAGA DEMO SCALP by CGTehnik
color 0A
echo ========================================================
echo   VATAGA DEMO SCALP by CGTehnik
echo   Virtual Paper Gateway - Real Market Data, 0 Risk!
echo ========================================================
py -3 "%~dp0gateway.py"
if %errorlevel% neq 0 (
    python "%~dp0gateway.py"
)
pause
