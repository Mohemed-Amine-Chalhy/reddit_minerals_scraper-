[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $RepositoryRoot ".uv-cache"
$env:PRE_COMMIT_HOME = Join-Path $RepositoryRoot ".cache/pre-commit"
$WebManifest = Join-Path $RepositoryRoot "web/package.json"
$HasWebWorkspace = Test-Path -LiteralPath $WebManifest

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

function Assert-WebToolchain {
    foreach ($command in @("node", "pnpm")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            throw "$command is required because web/package.json is present. See docs/web-app.md."
        }
    }

    $nodeVersionText = (& node --version).Trim().TrimStart("v").Split("-")[0]
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to determine the installed Node.js version."
    }
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
    if ($LASTEXITCODE -ne 0 -or $actualPnpm -ne $expectedPnpm) {
        throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Activate the pinned version and rerun this script."
    }
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv is required. Install it from https://docs.astral.sh/uv/getting-started/installation/ and rerun this script."
}

Push-Location $RepositoryRoot
try {
    Invoke-External uv python install 3.12
    if ($HasWebWorkspace) {
        Assert-WebToolchain
    }
    Invoke-External uv sync --locked
    Invoke-External uv run --locked --no-build-isolation python scripts/validate_env_example.py
    Invoke-External uv run --locked --no-build-isolation pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push

    if (-not (Test-Path -LiteralPath ".env")) {
        Copy-Item -LiteralPath ".env.example" -Destination ".env"
        Write-Host "Created .env from the safe template. Replace its credential placeholders before live commands."
    }

    & (Join-Path $PSScriptRoot "smoke.ps1")

    if ($HasWebWorkspace) {
        Invoke-External uv sync --locked --extra web
        Invoke-External pnpm --dir web install --frozen-lockfile
        Invoke-External uv run --locked --extra web --no-build-isolation python -c "from reddit_minerals.web import create_app; assert create_app().title == 'MineralLens API'"
    }
}
finally {
    Pop-Location
}
