[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
Push-Location $RepositoryRoot
try {
    & uv run --locked ruff check --fix .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff could not fix every lint violation."
    }
    & uv run --locked ruff format .
    if ($LASTEXITCODE -ne 0) {
        throw "Ruff formatting failed."
    }
}
finally {
    Pop-Location
}
