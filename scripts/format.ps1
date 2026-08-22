[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepositoryRoot
try {
    & uv run --locked --no-build-isolation ruff check --fix .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff could not fix every lint violation."
    }
    & uv run --locked --no-build-isolation ruff format .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff formatting failed."
    }
}
finally {
    Pop-Location
}
