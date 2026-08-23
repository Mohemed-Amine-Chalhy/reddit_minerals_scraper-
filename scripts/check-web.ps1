[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $RepositoryRoot ".uv-cache"
$WebRoot = Join-Path $RepositoryRoot "web"
$WebManifest = Join-Path $WebRoot "package.json"

function Invoke-External {
    param(
        [Parameter(Mandatory)]
        [string]$FilePath,

        [Parameter(ValueFromRemainingArguments)]
        [string[]]$ArgumentList
    )

    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code ${LASTEXITCODE}: $FilePath $ArgumentList"
    }
}

if (-not (Test-Path -LiteralPath $WebManifest -PathType Leaf)) {
    throw "web/package.json is missing; this script requires the web workspace."
}

foreach ($command in @("uv", "node", "pnpm")) {
    if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
        throw "$command is required for the web workspace. Run scripts/bootstrap-web.ps1 first."
    }
}

$nodeVersionText = (& node --version).Trim().TrimStart("v").Split("-")[0]
try {
    $nodeVersion = [version]$nodeVersionText
}
catch {
    throw "Unable to parse Node.js version '$nodeVersionText'."
}
if (
    $nodeVersion.Major -lt 22 -or
    $nodeVersion.Major -ge 27 -or
    ($nodeVersion.Major -eq 22 -and $nodeVersion.Minor -lt 12)
) {
    throw "Node.js >=22.12 and <27 is required; found $nodeVersionText. See .node-version."
}

$manifest = Get-Content -LiteralPath $WebManifest -Raw | ConvertFrom-Json
$expectedPnpm = [string]$manifest.packageManager -replace '^pnpm@', ''
$actualPnpm = (& pnpm --version).Trim()
if ($actualPnpm -ne $expectedPnpm) {
    throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Run scripts/bootstrap-web.ps1 after activating the pinned version."
}
if (-not (Test-Path -LiteralPath (Join-Path $WebRoot "node_modules") -PathType Container)) {
    throw "Web dependencies are not installed. Run scripts/bootstrap-web.ps1 first."
}
$installedLock = Join-Path $WebRoot "node_modules/.pnpm/lock.yaml"
if (
    -not (Test-Path -LiteralPath $installedLock -PathType Leaf) -or
    (Get-FileHash -LiteralPath (Join-Path $WebRoot "pnpm-lock.yaml") -Algorithm SHA256).Hash -ne
    (Get-FileHash -LiteralPath $installedLock -Algorithm SHA256).Hash
) {
    throw "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap-web.ps1 first."
}

Push-Location $RepositoryRoot
try {
    Invoke-External uv lock --check
    Invoke-External uv run --locked --extra web --no-build-isolation mypy src/reddit_minerals
    Invoke-External uv run --locked --extra web --no-build-isolation python scripts/run_tests.py tests/web --no-cov
    Invoke-External pnpm --dir web run check
}
finally {
    Pop-Location
}
