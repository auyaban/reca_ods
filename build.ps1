$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvPath = Join-Path $root ".venv"
if (!(Test-Path $venvPath)) {
    python -m venv $venvPath
}

$python = Join-Path $venvPath "Scripts\\python.exe"

& $python -m pip install --upgrade pip
& $python -m pip install -r requirements.txt
& $python -m pip install pyinstaller pillow

& $python tools\\make_icon.py

$envPath = Join-Path $root ".env"
if (!(Test-Path $envPath)) {
    throw ".env no encontrado"
}
$envLines = Get-Content $envPath
$supabaseUrl = ($envLines | Where-Object { $_ -match '^SUPABASE_URL=' }) -replace '^SUPABASE_URL=', ''
$supabaseKey = ($envLines | Where-Object { $_ -match '^SUPABASE_ANON_KEY=' }) -replace '^SUPABASE_ANON_KEY=', ''
$backendUrl = ($envLines | Where-Object { $_ -match '^BACKEND_URL=' }) -replace '^BACKEND_URL=', ''

$installerConfig = @"
#define SupabaseUrl `"$supabaseUrl`"
#define SupabaseKey `"$supabaseKey`"
#define BackendUrl `"$backendUrl`"
"@
Set-Content -Path (Join-Path $root "installer_config.iss") -Value $installerConfig

$iconPath = Join-Path $root "logo\\logo_reca.ico"
$pyiArgs = @(
    "--noconfirm",
    "--clean",
    "--windowed",
    "--add-data", "logo\\logo_reca.png;logo",
    "--add-data", "Excel\\ods_2026.xlsx;Excel",
    "--add-data", "facturas\\clausulada.xlsx;facturas",
    "--add-data", "facturas\\no_clausulada.xlsx;facturas",
    "--add-data", "VERSION;.",
    "--name", "RECA_ODS",
    "--collect-all", "supabase",
    "main_gui.py"
)
if (Test-Path $iconPath) {
    $pyiArgs = @("--icon", $iconPath) + $pyiArgs
}

& $python -m PyInstaller @pyiArgs
