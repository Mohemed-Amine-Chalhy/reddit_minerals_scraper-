[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $RepositoryRoot ".uv-cache"
$WebManifest = Join-Path $RepositoryRoot "web/package.json"
$WebModules = Join-Path $RepositoryRoot "web/node_modules"
$HasWebWorkspace = Test-Path -LiteralPath $WebManifest
Push-Location $RepositoryRoot
try {
    if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
        throw "uv is required. Run scripts/bootstrap.ps1 first."
    }
    $uvRunArguments = @("run", "--locked", "--no-build-isolation")
    if ($HasWebWorkspace) {
        $uvRunArguments += @("--extra", "web")
    }

    & uv @uvRunArguments ruff check --fix .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff could not fix every lint violation."
    }
    & uv @uvRunArguments ruff format .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff formatting failed."
    }
    if ($HasWebWorkspace) {
        foreach ($command in @("node", "pnpm")) {
            if (-not (Get-Command $command -ErrorAction SilentlyContinue)) {
                throw "$command is required to format the web workspace. Run scripts/bootstrap.ps1 first."
            }
        }
        if (-not (Test-Path -LiteralPath $WebModules -PathType Container)) {
            throw "Web dependencies are not installed. Run scripts/bootstrap.ps1 first."
        }

        $nodeVersionText = (& node --version).Trim().TrimStart("v").Split("-")[0]
        $nodeVersion = [version]$nodeVersionText
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
            throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Run scripts/bootstrap.ps1 after activating the pinned version."
        }

        $installedLock = Join-Path $WebModules ".pnpm/lock.yaml"
        if (
            -not (Test-Path -LiteralPath $installedLock -PathType Leaf) -or
            (Get-FileHash -LiteralPath "web/pnpm-lock.yaml" -Algorithm SHA256).Hash -ne
            (Get-FileHash -LiteralPath $installedLock -Algorithm SHA256).Hash
        ) {
            throw "Web dependencies do not match web/pnpm-lock.yaml. Run scripts/bootstrap.ps1 first."
        }
        & pnpm --dir web run format
        if ($LASTEXITCODE -ne 0) {
            throw "Prettier formatting failed."
        }
    }
}
finally {
    Pop-Location
}
