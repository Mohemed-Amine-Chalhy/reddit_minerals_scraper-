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
        throw "$command is required for the web workspace. See docs/web-app.md."
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
    throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Activate the pinned version and rerun this script."
}

Push-Location $RepositoryRoot
try {
    Invoke-External uv python install 3.12
    Invoke-External uv sync --locked --extra web
    Invoke-External pnpm --dir web install --frozen-lockfile
    Invoke-External uv run --locked --extra web --no-build-isolation python -c "from reddit_minerals.web import create_app; assert create_app().title == 'MineralLens API'"
    Write-Host "MineralLens web environment is ready. Run scripts/dev-web.ps1."
}
finally {
    Pop-Location
}
