param(
    [Parameter(Mandatory = $true)]
    [string]$Version,
    [string]$Notes = "",
    [switch]$AutoConfirm
)

$ErrorActionPreference = "Stop"

function Read-YesNo([string]$Prompt) {
    while ($true) {
        $answer = Read-Host "$Prompt (s/n)"
        if (-not $answer) { continue }
        switch ($answer.Trim().ToLower()) {
            "s" { return $true }
            "n" { return $false }
        }
    }
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$ver = $Version.TrimStart("v")
if (-not $ver) {
    Write-Host "Version invalida."
    exit 1
}

$versionPath = Join-Path $root "VERSION"
Set-Content -Path $versionPath -Value $ver

$changelogPath = Join-Path $root "CHANGELOG.md"
if (Test-Path $changelogPath) {
    if (-not $AutoConfirm) {
        $Notes = Read-Host "Notas para CHANGELOG (deja vacio para omitir)"
    }
    if ($Notes) {
        $current = Get-Content $changelogPath -Raw
        $entry = "## $ver`r`n- $Notes`r`n`r`n"
        if ($current -match "# Changelog") {
            $current = $current -replace "(# Changelog\\r?\\n\\r?\\n)", "`$1$entry"
            Set-Content -Path $changelogPath -Value $current
        }
    }
}

Write-Host "Publicando version $ver..."
if (-not $AutoConfirm) {
    if (-not (Read-YesNo "Continuar con commit, push y release")) {
        Write-Host "Cancelado."
        exit 0
    }
}

git add VERSION CHANGELOG.md
git add -A
git commit -m "Release $ver"
git push

powershell -ExecutionPolicy Bypass -File release.ps1 "v$ver"
