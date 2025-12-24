$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$versionPath = Join-Path $root "VERSION"
if ($args[0]) {
    $version = $args[0].TrimStart("v")
    Set-Content -Path $versionPath -Value $version
} else {
    if (!(Test-Path $versionPath)) {
        Write-Host "VERSION no encontrado. Proporciona la version: .\\release.ps1 vX.Y.Z"
        exit 1
    }
    $version = (Get-Content $versionPath | Select-Object -First 1).Trim()
    if (-not $version) {
        Write-Host "VERSION esta vacio. Proporciona la version: .\\release.ps1 vX.Y.Z"
        exit 1
    }
}

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

$releaseTag = "v$version"
$exists = $true
& $gh release view $releaseTag 2>$null | Out-Null
if ($LASTEXITCODE -ne 0) {
    $exists = $false
}

if ($exists) {
    Write-Host "Release $releaseTag encontrado. Subiendo assets..."
    & $gh release upload $releaseTag $installerPath (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256") --clobber
} else {
    Write-Host "Release $releaseTag no existe. Creandolo..."
    & $gh release create $releaseTag $installerPath (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256") `
      --title "Sistema de Gestión ODS RECA v$version" `
      --notes "Release v$version"
}
