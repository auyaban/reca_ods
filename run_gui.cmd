@echo off
setlocal
set "ROOT=%~dp0"
set "PY=%ROOT%.venv\Scripts\python.exe"
if exist "%PY%" (
  "%PY%" "%ROOT%start_gui.py"
  exit /b %ERRORLEVEL%
)
where python >nul 2>nul
if %ERRORLEVEL%==0 (
  python "%ROOT%start_gui.py"
  exit /b %ERRORLEVEL%
)
echo No se encontro Python ni el entorno virtual.
pause
