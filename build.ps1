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
$googleDriveSharedFolderId = ($envLines | Where-Object { $_ -match '^GOOGLE_DRIVE_SHARED_FOLDER_ID=' }) -replace '^GOOGLE_DRIVE_SHARED_FOLDER_ID=', ''
$googleDriveTemplateSpreadsheetName = ($envLines | Where-Object { $_ -match '^GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME=' }) -replace '^GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME=', ''
$googleServiceAccountFile = ($envLines | Where-Object { $_ -match '^GOOGLE_SERVICE_ACCOUNT_FILE=' }) -replace '^GOOGLE_SERVICE_ACCOUNT_FILE=', ''
$googleInstalledServiceAccountPath = '%APPDATA%\Sistema de Gestión ODS RECA\secrets\google-service-account.json'
$hasGoogleServiceAccount = 0

function Escape-InnoValue([string]$value) {
    if ($null -eq $value) {
        return ""
    }
    return $value.Replace('"', '""')
}

if ($googleServiceAccountFile) {
    $resolvedGoogleServiceAccountFile = [Environment]::ExpandEnvironmentVariables($googleServiceAccountFile)
    if (Test-Path $resolvedGoogleServiceAccountFile) {
        $hasGoogleServiceAccount = 1
    } else {
        Write-Warning "No se encontro GOOGLE_SERVICE_ACCOUNT_FILE en '$resolvedGoogleServiceAccountFile'. El instalador se generara sin credencial Google empaquetada."
    }
}

$installerConfig = @"
#define SupabaseUrl `"$(Escape-InnoValue $supabaseUrl)`"
#define SupabaseKey `"$(Escape-InnoValue $supabaseKey)`"
#define BackendUrl `"$(Escape-InnoValue $backendUrl)`"
#define GoogleDriveSharedFolderId `"$(Escape-InnoValue $googleDriveSharedFolderId)`"
#define GoogleDriveTemplateSpreadsheetName `"$(Escape-InnoValue $googleDriveTemplateSpreadsheetName)`"
#define GoogleServiceAccountInstalledPath `"$(Escape-InnoValue $googleInstalledServiceAccountPath)`"
#define HasGoogleServiceAccount $hasGoogleServiceAccount
"@
Set-Content -Path (Join-Path $root "installer_config.iss") -Value $installerConfig

$iconPath = Join-Path $root "logo\\logo_reca.ico"
$pyiArgs = @(
    "--noconfirm",
    "--clean",
    "--windowed",
    "--add-data", "logo\\logo_reca.png;logo",
    "--add-data", "VERSION;.",
    "--name", "RECA_ODS",
    "--collect-all", "supabase",
    "start_gui.py"
)
if (Test-Path $iconPath) {
    $pyiArgs = @("--icon", $iconPath) + $pyiArgs
}

& $python -m PyInstaller @pyiArgs

$installerPayloadDir = Join-Path $root "dist\\installer_payload"
$installerPayloadFile = Join-Path $installerPayloadDir "google-service-account.json"
New-Item -ItemType Directory -Force -Path $installerPayloadDir | Out-Null
if ($hasGoogleServiceAccount -eq 1) {
    Copy-Item -Path $resolvedGoogleServiceAccountFile -Destination $installerPayloadFile -Force
} elseif (Test-Path $installerPayloadFile) {
    Remove-Item -Path $installerPayloadFile -Force
}
