$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Invoke-ProcessChecked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory = $true)][string]$Description,
        [int]$TimeoutSeconds = 60
    )

    Write-Host "Verificando $Description..."
    $process = Start-Process -FilePath $FilePath -ArgumentList $ArgumentList -PassThru
    if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
        try {
            $process.Kill($true)
        } catch {
            Write-Warning ("No se pudo cerrar el proceso atascado para {0}: {1}" -f $Description, $_)
        }
        throw "$Description excedio $TimeoutSeconds segundos."
    }
    if ($process.ExitCode -ne 0) {
        throw "$Description fallo con codigo $($process.ExitCode)."
    }
}

function Invoke-ExecutableChecked {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [string[]]$ArgumentList = @(),
        [Parameter(Mandatory = $true)][string]$Description
    )

    Write-Host "Verificando $Description..."
    Push-Location (Split-Path -Parent $FilePath)
    try {
        & $FilePath @ArgumentList
        $exitCode = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    if ($exitCode -ne 0) {
        throw "$Description fallo con codigo $exitCode."
    }
}

function Test-InstallerConfig {
    param(
        [Parameter(Mandatory = $true)][string]$ConfigPath
    )

    if (!(Test-Path $ConfigPath)) {
        throw "No se encontro installer_config.iss en $ConfigPath"
    }

    $content = Get-Content $ConfigPath
    $requiredDefines = @(
        "SupabaseUrl",
        "SupabaseKey",
        "SupabaseAuthEmail",
        "SupabaseAuthPassword",
        "BackendUrl",
        "SupabaseEdgeActaExtractionSecret"
    )
    foreach ($defineName in $requiredDefines) {
        $line = $content | Where-Object { $_ -match "^#define $defineName " } | Select-Object -First 1
        if (-not $line) {
            throw "Falta #define $defineName en installer_config.iss"
        }
        if ($line -match '""$') {
            throw "installer_config.iss tiene $defineName vacio."
        }
    }
}

function Test-InstallerSmoke {
    param(
        [Parameter(Mandatory = $true)][string]$InstallerPath,
        [Parameter(Mandatory = $true)][string]$ExpectedVersion
    )

    $tempRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("reca-installer-smoke-" + [Guid]::NewGuid().ToString("N"))
    $installDir = Join-Path $tempRoot "app"
    $logPath = Join-Path $tempRoot "installer.log"
    New-Item -ItemType Directory -Force -Path $installDir | Out-Null

    try {
        Invoke-ProcessChecked `
            -FilePath $InstallerPath `
            -ArgumentList @(
                "/SP-",
                "/VERYSILENT",
                "/CURRENTUSER",
                "/SUPPRESSMSGBOXES",
                "/NORESTART",
                "/DIR=$installDir",
                "/LOG=$logPath"
            ) `
            -Description "la instalacion silenciosa temporal del installer" `
            -TimeoutSeconds 180

        $installedExe = Join-Path $installDir "RECA_ODS.exe"
        if (!(Test-Path $installedExe)) {
            throw "El installer no dejo RECA_ODS.exe en $installDir"
        }

        $versionCandidates = @(
            (Join-Path $installDir "VERSION"),
            (Join-Path $installDir "_internal\\VERSION")
        )
        $versionPath = $versionCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
        if (!(Test-Path $versionPath)) {
            throw "El installer no incluyo el archivo VERSION."
        }

        $installedVersion = (Get-Content $versionPath | Select-Object -First 1).Trim()
        if ($installedVersion -ne $ExpectedVersion) {
            throw "La version instalada es '$installedVersion' y se esperaba '$ExpectedVersion'."
        }

        Invoke-ExecutableChecked `
            -FilePath $installedExe `
            -ArgumentList @("--smoke-test") `
            -Description "el smoke test del ejecutable instalado"
    } finally {
        $uninstaller = Join-Path $installDir "unins000.exe"
        if (Test-Path $uninstaller) {
            try {
                Invoke-ProcessChecked `
                    -FilePath $uninstaller `
                    -ArgumentList @("/VERYSILENT", "/SUPPRESSMSGBOXES", "/NORESTART") `
                    -Description "la desinstalacion temporal del smoke test" `
                    -TimeoutSeconds 180
            } catch {
                Write-Warning "No se pudo desinstalar el smoke test temporal: $_"
            }
        }
        if (Test-Path $tempRoot) {
            Start-Sleep -Seconds 1
            Remove-Item -Path $tempRoot -Recurse -Force -ErrorAction SilentlyContinue
        }
    }
}

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
Test-InstallerConfig -ConfigPath (Join-Path $root "installer_config.iss")

$distExePath = Join-Path $root "dist\\RECA_ODS\\RECA_ODS.exe"
if (!(Test-Path $distExePath)) {
    throw "No se encontro el ejecutable empaquetado en $distExePath"
}
Invoke-ExecutableChecked `
    -FilePath $distExePath `
    -ArgumentList @("--smoke-test") `
    -Description "el smoke test del ejecutable empaquetado"

& "C:\\Program Files (x86)\\Inno Setup 6\\ISCC.exe" /DMyAppVersion=$version installer.iss

$installerPath = Join-Path $root "installer\\RECA_ODS_Setup.exe"
if (!(Test-Path $installerPath)) {
    throw "No se encontro el instalador generado en $installerPath"
}
Test-InstallerSmoke -InstallerPath $installerPath -ExpectedVersion $version

$hash = (Get-FileHash -Algorithm SHA256 $installerPath).Hash.ToLower()
"$hash  RECA_ODS_Setup.exe" | Set-Content (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256")

$releaseTag = "v$version"
$exists = $true
try {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $gh release view $releaseTag 2>$null | Out-Null
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $prev
} catch {
    $ErrorActionPreference = $prev
    $exitCode = 1
}
if ($exitCode -ne 0) {
    $exists = $false
}

if ($exists) {
    Write-Host "Release $releaseTag encontrado. Subiendo assets..."
    & $gh release upload $releaseTag $installerPath (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256") --clobber
} else {
    Write-Host "Release $releaseTag no existe. Creandolo..."
    & $gh release create $releaseTag $installerPath (Join-Path $root "installer\\RECA_ODS_Setup.exe.sha256") `
      --title "Sistema de Gestion ODS RECA v$version" `
      --notes "Release v$version"
}
