$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $args[0]) {
    Write-Host "Usage: .\\release.ps1 vX.Y.Z"
    exit 1
}

$version = $args[0].TrimStart("v")
Set-Content -Path (Join-Path $root "VERSION") -Value $version

$gh = "gh"
if (-not (Get-Command $gh -ErrorAction SilentlyContinue)) {
    $ghPath = "C:\\Program Files\\GitHub CLI\\gh.exe"
    if (Test-Path $ghPath) {
        $gh = $ghPath
    } else {
        Write-Host "GitHub CLI not found. Install it with winget or set PATH."
        exit 1
    }
}

& $gh auth status -h github.com | Out-Null

powershell -ExecutionPolicy Bypass -File build.ps1

& "C:\\Program Files (x86)\\Inno Setup 6\\ISCC.exe" /DMyAppVersion=$version installer.iss

$installerPath = Join-Path $root "installer\\RECA_ODS_Setup.exe"
$hash = (Get-FileHash -Algorithm SHA256 $installerPath).Hash.ToLower()
"$hash  RECA_ODS_Setup.exe" | Set-Content (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256")

& $gh release create "v$version" $installerPath (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256") `
  --title "Sistema de Gestión ODS RECA v$version" `
  --notes "Release v$version"
