$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvPath = Join-Path $root ".venv"
if (!(Test-Path $venvPath)) {
    python -m venv $venvPath
}

$python = Join-Path $venvPath "Scripts\\python.exe"
$pythonVersionRaw = & $python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}')"
$pythonVersionParts = $pythonVersionRaw.Trim().Split(".")
$pythonMajor = [int]$pythonVersionParts[0]
$pythonMinor = [int]$pythonVersionParts[1]

if ($pythonMajor -ne 3 -or $pythonMinor -lt 10 -or $pythonMinor -gt 14) {
    throw "El build/release soporta Python 3.10 a 3.14. La venv actual usa Python $pythonVersionRaw. Recreate .venv con una version soportada antes de empaquetar."
}
if ($pythonMinor -ne 13) {
    Write-Warning "La venv actual usa Python $pythonVersionRaw. Recomendado para build/release: Python 3.13.x."
}

& $python -m pip install --upgrade pip
& $python -m pip install -r requirements.txt
& $python -m pip install pyinstaller pillow

& $python tools\\make_icon.py

$envPath = Join-Path $root ".env"
if (!(Test-Path $envPath)) {
    throw ".env no encontrado"
}

$envLines = Get-Content $envPath

function Get-EnvValue([string]$name) {
    return ($envLines | Where-Object { $_ -match "^$name=" } | Select-Object -First 1) -replace "^$name=", ''
}

$supabaseUrl = Get-EnvValue "SUPABASE_URL"
$supabaseKey = Get-EnvValue "SUPABASE_ANON_KEY"
$supabaseAuthEmail = Get-EnvValue "SUPABASE_AUTH_EMAIL"
$supabaseAuthPassword = Get-EnvValue "SUPABASE_AUTH_PASSWORD"
$backendUrl = Get-EnvValue "BACKEND_URL"
$googleDriveSharedFolderId = Get-EnvValue "GOOGLE_DRIVE_SHARED_FOLDER_ID"
$googleDriveTemplateSpreadsheetName = Get-EnvValue "GOOGLE_DRIVE_TEMPLATE_SPREADSHEET_NAME"
$googleServiceAccountFile = Get-EnvValue "GOOGLE_SERVICE_ACCOUNT_FILE"
$googleServiceAccountBuildSourceFile = Get-EnvValue "GOOGLE_SERVICE_ACCOUNT_BUILD_SOURCE_FILE"
$supabaseEdgeActaExtractionSecret = Get-EnvValue "SUPABASE_EDGE_ACTA_EXTRACTION_SECRET"
$googleInstalledServiceAccountPath = '%APPDATA%\Sistema de Gestion ODS RECA\secrets\google-service-account.json'
$hasGoogleServiceAccount = 0
$resolvedGoogleServiceAccountFile = ""
$googleFeaturesConfigured = [bool]($googleDriveSharedFolderId -or $googleDriveTemplateSpreadsheetName)

function Escape-InnoValue([string]$value) {
    if ($null -eq $value) {
        return ""
    }
    return $value.Replace('"', '""')
}

if ($googleServiceAccountBuildSourceFile) {
    $resolvedGoogleServiceAccountFile = [Environment]::ExpandEnvironmentVariables($googleServiceAccountBuildSourceFile)
    if (Test-Path $resolvedGoogleServiceAccountFile) {
        $hasGoogleServiceAccount = 1
    } else {
        throw "GOOGLE_SERVICE_ACCOUNT_BUILD_SOURCE_FILE apunta a una ruta inexistente: '$resolvedGoogleServiceAccountFile'."
    }
} elseif ($googleServiceAccountFile) {
    $resolvedGoogleServiceAccountFile = [Environment]::ExpandEnvironmentVariables($googleServiceAccountFile)
    if (Test-Path $resolvedGoogleServiceAccountFile) {
        $hasGoogleServiceAccount = 1
    }
}

if ($googleFeaturesConfigured -and !$hasGoogleServiceAccount) {
    throw "Google Drive/Sheets esta configurado, pero no existe una credencial empaquetable. Define GOOGLE_SERVICE_ACCOUNT_BUILD_SOURCE_FILE con el JSON real o asegura que GOOGLE_SERVICE_ACCOUNT_FILE apunte a un archivo existente."
}

$installerConfig = @"
#define SupabaseUrl `"$(Escape-InnoValue $supabaseUrl)`"
#define SupabaseKey `"$(Escape-InnoValue $supabaseKey)`"
#define SupabaseAuthEmail `"$(Escape-InnoValue $supabaseAuthEmail)`"
#define SupabaseAuthPassword `"$(Escape-InnoValue $supabaseAuthPassword)`"
#define BackendUrl `"$(Escape-InnoValue $backendUrl)`"
#define GoogleDriveSharedFolderId `"$(Escape-InnoValue $googleDriveSharedFolderId)`"
#define GoogleDriveTemplateSpreadsheetName `"$(Escape-InnoValue $googleDriveTemplateSpreadsheetName)`"
#define GoogleServiceAccountInstalledPath `"$(Escape-InnoValue $googleInstalledServiceAccountPath)`"
#define SupabaseEdgeActaExtractionSecret `"$(Escape-InnoValue $supabaseEdgeActaExtractionSecret)`"
#define HasGoogleServiceAccount $hasGoogleServiceAccount
"@
Set-Content -Path (Join-Path $root "installer_config.iss") -Value $installerConfig

$pyInstallerPackages = @(
    "supabase",
    "postgrest",
    "realtime",
    "storage3",
    "supabase_auth",
    "supabase_functions"
)

$hiddenImportsScript = @'
from PyInstaller.utils.hooks import collect_submodules

packages = [
    "supabase",
    "postgrest",
    "realtime",
    "storage3",
    "supabase_auth",
    "supabase_functions",
]

mods = []
for package in packages:
    mods.extend(collect_submodules(package))

for mod in sorted(set(mods)):
    print(mod)
'@
$hiddenImports = @($hiddenImportsScript | & $python -)

$iconPath = Join-Path $root "logo\\logo_reca.ico"
$pyiArgs = @(
    "--noconfirm",
    "--clean",
    "--windowed",
    "--add-data", "logo\\logo_reca.png;logo",
    "--add-data", "VERSION;.",
    "--name", "RECA_ODS",
    "start_gui.py"
)
foreach ($package in $pyInstallerPackages) {
    $pyiArgs += @("--collect-all", $package)
}
foreach ($hiddenImport in $hiddenImports) {
    $moduleName = [string]$hiddenImport
    if ($moduleName.Trim()) {
        $pyiArgs += @("--hidden-import", $moduleName.Trim())
    }
}
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
