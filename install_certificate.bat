@echo off
cd /d "%~dp0"
echo ==========================================================
echo   Installing Root Certificate for Vataga Demo by CGTehnik...
echo ==========================================================
certutil -addstore -f Root "%~dp0gateway.crt"
certutil -user -addstore -f Root "%~dp0gateway.crt"
echo.
echo ==========================================================
echo [SUCCESS] Certificate installed successfully!
echo You can now close this window and launch the gateway!
echo ==========================================================
pause
