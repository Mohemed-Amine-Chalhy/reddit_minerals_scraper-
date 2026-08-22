[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv is required. Run scripts/bootstrap.ps1 first."
}

Push-Location $RepositoryRoot
try {
    & uv run --locked python -c "import reddit_minerals; import reddit_minerals.cli"
    if ($LASTEXITCODE -ne 0) {
        throw "Package import smoke test failed with exit code ${LASTEXITCODE}."
    }

    & uv run --locked reddit-minerals --help
    if ($LASTEXITCODE -ne 0) {
        throw "CLI smoke test failed with exit code ${LASTEXITCODE}."
    }
}
finally {
    Pop-Location
}
