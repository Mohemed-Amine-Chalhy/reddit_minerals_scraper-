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
    throw "uv is required. Run scripts/bootstrap.ps1 first."
}

Push-Location $RepositoryRoot
try {
    Invoke-External uv lock --check
    Invoke-External uv run --locked pre-commit run --all-files --show-diff-on-failure
    Invoke-External uv run --locked mypy src/reddit_minerals
    Invoke-External uv run --locked pytest
    Invoke-External uv run --locked pip-audit --progress-spinner=off
    Invoke-External uv build --no-build-isolation --out-dir dist
    Invoke-External uv run --locked python scripts/check_artifacts.py
    & (Join-Path $PSScriptRoot "smoke.ps1")
}
finally {
    Pop-Location
}
