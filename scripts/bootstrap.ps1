[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot

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

if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
    throw "uv is required. Install it from https://docs.astral.sh/uv/getting-started/installation/ and rerun this script."
}

Push-Location $RepositoryRoot
try {
    Invoke-External uv python install 3.12
    Invoke-External uv sync --locked
    Invoke-External uv run --locked python scripts/validate_env_example.py
    Invoke-External uv run --locked pre-commit install --install-hooks --hook-type pre-commit --hook-type pre-push

    if (-not (Test-Path -LiteralPath ".env")) {
        Copy-Item -LiteralPath ".env.example" -Destination ".env"
        Write-Host "Created .env from the safe template. Replace its credential placeholders before live commands."
    }

    & (Join-Path $PSScriptRoot "smoke.ps1")
}
finally {
    Pop-Location
}
