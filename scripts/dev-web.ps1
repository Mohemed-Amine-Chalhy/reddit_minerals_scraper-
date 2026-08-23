[CmdletBinding()]
param(
    [string]$HostName = "127.0.0.1",
    [int]$ApiPort = 8000,
    [int]$WebPort = 5173
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$env:UV_CACHE_DIR = Join-Path $RepositoryRoot ".uv-cache"
$WebRoot = Join-Path $RepositoryRoot "web"
$WebManifest = Join-Path $WebRoot "package.json"
$LogRoot = Join-Path ([System.IO.Path]::GetTempPath()) "reddit-minerals-web"

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
foreach ($port in @($ApiPort, $WebPort)) {
    if ($port -lt 1 -or $port -gt 65535) {
        throw "Ports must be integers between 1 and 65535; received $port."
    }
}
if ($ApiPort -eq $WebPort) {
    throw "ApiPort and WebPort must be different."
}

$manifest = Get-Content -LiteralPath $WebManifest -Raw | ConvertFrom-Json
$expectedPnpm = [string]$manifest.packageManager -replace '^pnpm@', ''
$actualPnpm = (& pnpm --version).Trim()
if ($actualPnpm -ne $expectedPnpm) {
    throw "pnpm $expectedPnpm is required by web/package.json; found '$actualPnpm'. Run scripts/bootstrap-web.ps1 after activating the pinned version."
}

$connectHost = if ($HostName -eq "0.0.0.0") {
    "127.0.0.1"
}
elseif ($HostName -in @("::", "[::]")) {
    "::1"
}
else {
    $HostName.Trim("[", "]")
}
$portProbe = [System.Net.Sockets.TcpClient]::new()
try {
    $connectTask = $portProbe.ConnectAsync($connectHost, $ApiPort)
    if ($connectTask.Wait(300) -and $portProbe.Connected) {
        throw "ApiPort $ApiPort is already accepting connections on $connectHost. Choose a free port."
    }
}
catch [System.AggregateException] {
    # Connection refusal means the requested local port is available.
}
finally {
    $portProbe.Dispose()
}

New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null
$ApiOutLog = Join-Path $LogRoot "api.out.log"
$ApiErrorLog = Join-Path $LogRoot "api.err.log"

$apiArguments = @(
    "run", "--locked", "--extra", "web", "--no-build-isolation", "uvicorn",
    "reddit_minerals.web.app:create_app", "--factory", "--reload",
    "--host", $HostName, "--port", $ApiPort
)

$apiProcess = Start-Process -FilePath "uv" -ArgumentList $apiArguments `
    -WorkingDirectory $RepositoryRoot -PassThru -WindowStyle Hidden `
    -RedirectStandardOutput $ApiOutLog -RedirectStandardError $ApiErrorLog

try {
    $ready = $false
    $probeHost = if ($HostName -eq "0.0.0.0") {
        "127.0.0.1"
    }
    elseif ($HostName -in @("::", "[::]")) {
        "[::1]"
    }
    elseif ($HostName.Contains(":") -and -not $HostName.StartsWith("[")) {
        "[$HostName]"
    }
    else {
        $HostName
    }
    $readinessDeadline = [DateTime]::UtcNow.AddSeconds(10)
    while ([DateTime]::UtcNow -lt $readinessDeadline) {
        if ($apiProcess.HasExited) {
            break
        }
        try {
            $health = Invoke-RestMethod -Uri "http://${probeHost}:${ApiPort}/api/v1/health" -TimeoutSec 1
            Start-Sleep -Milliseconds 100
            if (-not $apiProcess.HasExited -and $health.status -eq "healthy") {
                $ready = $true
                break
            }
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }

    if (-not $ready) {
        $exitContext = if ($apiProcess.HasExited) { " It exited with code $($apiProcess.ExitCode)." } else { "" }
        throw "The API did not become ready within 10 seconds.$exitContext Inspect $ApiErrorLog."
    }

    Push-Location $WebRoot
    try {
        & pnpm run dev -- --host $HostName --port $WebPort --strictPort
        if ($LASTEXITCODE -ne 0) {
            throw "The Vite development server exited with code $LASTEXITCODE."
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    if (-not $apiProcess.HasExited) {
        Stop-Process -Id $apiProcess.Id -Force
    }
}
