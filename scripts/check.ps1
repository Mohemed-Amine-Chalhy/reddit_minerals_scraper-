[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $RepositoryRoot ".uv-cache"
$env:PRE_COMMIT_HOME = Join-Path $RepositoryRoot ".cache/pre-commit"
$WebManifest = Join-Path $RepositoryRoot "web/package.json"
$WebModules = Join-Path $RepositoryRoot "web/node_modules"
$WebInstalledLock = Join-Path $WebModules ".pnpm/lock.yaml"
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

function Invoke-UvRun {
    param(
        [Parameter(ValueFromRemainingArguments)]
        [string[]]$CommandArguments
    )

    $uvArguments = @("run", "--locked", "--no-build-isolation")
    if ($HasWebWorkspace) {
        $uvArguments += @("--extra", "web")
    }
    $uvArguments += $CommandArguments
    Invoke-External uv @uvArguments
}

function Assert-WebToolchain {
    foreach ($command in @("node", "pnpm")) {
        if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
            throw "$command is required because web/package.json is present. Run scripts/bootstrap.ps1."
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
        throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Run scripts/bootstrap.ps1 after activating the pinned version."
    }
    if (-not (Test-Path -LiteralPath $WebModules -PathType Container)) {
        throw "Web dependencies are not installed. Run scripts/bootstrap.ps1 first."
    }
    if (
        -not (Test-Path -LiteralPath $WebInstalledLock -PathType Leaf) -or
        (Get-FileHash -LiteralPath "web/pnpm-lock.yaml" -Algorithm SHA256).Hash -ne
        (Get-FileHash -LiteralPath $WebInstalledLock -Algorithm SHA256).Hash
    ) {
        throw "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap.ps1 first."
    }
}

function Invoke-PreCommitWithoutDuplicateWebChecks {
    $previousSkip = $env:SKIP
    $skipWasSet = Test-Path Env:SKIP
    if ($HasWebWorkspace) {
        $webHookIds = "web-prettier,web-eslint,web-typecheck"
        $env:SKIP = if ([string]::IsNullOrWhiteSpace($previousSkip)) {
            $webHookIds
        }
        else {
            "$previousSkip,$webHookIds"
        }
    }

    try {
        Invoke-UvRun pre-commit run --all-files --show-diff-on-failure
    }
    finally {
        if ($skipWasSet) {
            $env:SKIP = $previousSkip
        }
        else {
            Remove-Item Env:SKIP -ErrorAction SilentlyContinue
        }
    }
}

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv is required. Run scripts/bootstrap.ps1 first."
}

Push-Location $RepositoryRoot
try {
    if ($HasWebWorkspace) {
        Assert-WebToolchain
    }
    Invoke-External uv lock --check
    Invoke-PreCommitWithoutDuplicateWebChecks
    & (Join-Path $PSScriptRoot "smoke.ps1")
    Invoke-UvRun mypy src/reddit_minerals
    Invoke-UvRun python scripts/run_tests.py
    if ($HasWebWorkspace) {
        Invoke-External pnpm --dir web run check
    }
    Invoke-UvRun pip-audit --progress-spinner=off --cache-dir (Join-Path $RepositoryRoot ".cache/pip-audit")
    Invoke-External uv build --clear --no-build-isolation --out-dir dist
    Invoke-UvRun python scripts/check_artifacts.py
}
finally {
    Pop-Location
}
