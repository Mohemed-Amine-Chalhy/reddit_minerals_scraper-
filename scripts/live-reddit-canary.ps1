<#
.SYNOPSIS
Runs a low-limit Live Reddit job without printing credentials or collected text.

.DESCRIPTION
The FastAPI backend must already be running with live mode enabled. Server
credentials are used by default. For one-run credentials, set the three
RMS_REDDIT_* values in this process environment and select `provided`; values
are sent in the HTTPS request body and never written by this script. Every mode
also requires the matching RMS_LIVE_ACCESS_TOKEN in this process environment.
#>
[CmdletBinding()]
param(
    [Parameter()]
    [ValidateNotNullOrEmpty()]
    [string]$BaseUrl = "http://127.0.0.1:8000",

    [Parameter()]
    [ValidateLength(1, 128)]
    [string]$Mineral = "gold",

    [Parameter()]
    [ValidatePattern("^[A-Za-z0-9_]{2,64}$")]
    [string]$Subreddit = "mining",

    [Parameter()]
    [ValidateSet("hour", "day", "week", "month", "year", "all")]
    [string]$TimeFilter = "week",

    [Parameter()]
    [ValidateRange(1, 100)]
    [int]$MaxPosts = 2,

    [Parameter()]
    [ValidateRange(0, 500)]
    [int]$MaxComments = 5,

    [Parameter()]
    [ValidateSet("server", "provided")]
    [string]$CredentialMode = "server",

    [Parameter()]
    [ValidateRange(5, 3600)]
    [int]$TimeoutSeconds = 180,

    [Parameter()]
    [ValidateRange(1, 60)]
    [int]$PollIntervalSeconds = 1
)

$ErrorActionPreference = "Stop"
$terminalStatuses = @("cancelled", "succeeded", "partial", "failed")
$jobId = $null
$deploymentAccessToken = [Environment]::GetEnvironmentVariable("RMS_LIVE_ACCESS_TOKEN")
$randomBytes = [byte[]]::new(32)
$randomNumberGenerator = [Security.Cryptography.RandomNumberGenerator]::Create()
try {
    $randomNumberGenerator.GetBytes($randomBytes)
}
finally {
    $randomNumberGenerator.Dispose()
}
$jobToken = [Convert]::ToBase64String($randomBytes).TrimEnd("=").Replace("+", "-").Replace("/", "_")
$terminal = $false
$baseUri = [Uri]$BaseUrl

if ([string]::IsNullOrWhiteSpace($deploymentAccessToken) -or $deploymentAccessToken.Length -lt 32) {
    throw "RMS_LIVE_ACCESS_TOKEN must contain at least 32 characters in the canary process environment."
}

if ($baseUri.Scheme -notin @("http", "https")) {
    throw "BaseUrl must use http or https."
}
if ($baseUri.UserInfo -or $baseUri.Query -or $baseUri.Fragment) {
    throw "BaseUrl must not contain credentials, a query, or a fragment."
}
if ($CredentialMode -eq "provided") {
    $isLoopback = $baseUri.IsLoopback -or $baseUri.Host -in @("localhost", "127.0.0.1", "::1")
    if ($baseUri.Scheme -ne "https" -and -not $isLoopback) {
        throw "Provided credentials require HTTPS unless the backend is on loopback."
    }
}

$apiRoot = $BaseUrl.TrimEnd("/") + "/api/v1/live"

try {
    $capabilities = Invoke-RestMethod `
        -Method Get `
        -Uri "$apiRoot/capabilities" `
        -TimeoutSec 30 `
        -Headers @{ Accept = "application/json" }

    if (-not $capabilities.enabled) {
        throw "Live Reddit is disabled on this FastAPI instance."
    }
    if ($CredentialMode -notin @($capabilities.credential_modes)) {
        throw "Credential mode '$CredentialMode' is not available on this FastAPI instance."
    }
    if ($MaxPosts -gt [int]$capabilities.limits.max_posts_per_mineral) {
        throw "MaxPosts exceeds this deployment's advertised live-job limit."
    }
    if ($MaxComments -gt [int]$capabilities.limits.max_comments_per_post) {
        throw "MaxComments exceeds this deployment's advertised live-job limit."
    }

    $payload = @{
        targets = @(
            @{
                mineral = $Mineral
                subreddits = @($Subreddit)
            }
        )
        time_filter = $TimeFilter
        max_posts_per_mineral = $MaxPosts
        max_comments_per_post = $MaxComments
        credential_mode = $CredentialMode
    }

    if ($CredentialMode -eq "provided") {
        $requiredNames = @(
            "RMS_REDDIT_CLIENT_ID",
            "RMS_REDDIT_CLIENT_SECRET",
            "RMS_REDDIT_USER_AGENT"
        )
        foreach ($name in $requiredNames) {
            $value = [Environment]::GetEnvironmentVariable($name)
            if ([string]::IsNullOrWhiteSpace($value)) {
                throw "Provided mode requires $name in the canary process environment."
            }
        }
        $payload.credentials = @{
            client_id = [Environment]::GetEnvironmentVariable("RMS_REDDIT_CLIENT_ID")
            client_secret = [Environment]::GetEnvironmentVariable("RMS_REDDIT_CLIENT_SECRET")
            user_agent = [Environment]::GetEnvironmentVariable("RMS_REDDIT_USER_AGENT")
        }
    }

    $creationHeaders = @{
        Accept = "application/json"
        "X-Live-Access-Token" = $deploymentAccessToken
        "X-Live-Job-Token" = $jobToken
    }
    $created = $null
    foreach ($attempt in 1..2) {
        try {
            $created = Invoke-RestMethod `
                -Method Post `
                -Uri "$apiRoot/jobs" `
                -TimeoutSec 30 `
                -ContentType "application/json" `
                -Headers $creationHeaders `
                -Body ($payload | ConvertTo-Json -Compress -Depth 6)
            break
        }
        catch {
            if ($attempt -eq 2) {
                throw
            }
            Start-Sleep -Milliseconds 500
        }
    }
    $payload.Remove("credentials")

    $jobId = [string]$created.job.id
    $echoedJobToken = [string]$created.access_token
    if ([string]::IsNullOrWhiteSpace($jobId) -or $echoedJobToken -ne $jobToken) {
        throw "The job-creation response did not echo the expected ID and access token."
    }

    $jobUri = "$apiRoot/jobs/$jobId"
    $headers = @{
        Accept = "application/json"
        "X-Live-Job-Token" = $jobToken
    }
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($TimeoutSeconds)
    Write-Host "Created live canary job $jobId; the access token will not be displayed."

    do {
        if ([DateTimeOffset]::UtcNow -ge $deadline) {
            throw "Live canary did not reach a terminal state within $TimeoutSeconds seconds."
        }
        Start-Sleep -Seconds $PollIntervalSeconds
        $job = Invoke-RestMethod `
            -Method Get `
            -Uri $jobUri `
            -TimeoutSec 30 `
            -Headers $headers
        Write-Host (
            "status={0} stage={1} posts={2} comments={3} failures={4}" -f
            $job.status,
            $job.stage,
            $job.progress.posts_stored,
            $job.progress.comments_stored,
            ($job.progress.posts_failed + $job.progress.searches_failed)
        )
        $terminal = [string]$job.status -in $terminalStatuses
    } until ($terminal)

    if ([string]$job.status -in @("succeeded", "partial")) {
        $snapshot = Invoke-RestMethod `
            -Method Get `
            -Uri "$jobUri/snapshot" `
            -TimeoutSec 30 `
            -Headers $headers
        $snapshotCount = @($snapshot.records).Count
        if ($snapshotCount -ne [int]$job.record_count) {
            throw "Snapshot count did not match the terminal job summary."
        }
        Write-Host "Verified snapshot metadata for $snapshotCount record(s); content was not printed."
    }

    if ([string]$job.status -ne "succeeded") {
        $errorCode = if ($null -ne $job.error) { [string]$job.error.code } else { "none" }
        throw "Live canary ended with status '$($job.status)' and safe error code '$errorCode'."
    }

    Write-Host "Live Reddit canary succeeded."
}
finally {
    if ($null -ne $payload) {
        $payload.Remove("credentials")
    }
    $deploymentAccessToken = $null
    if ($jobId -and $jobToken) {
        try {
            Invoke-RestMethod `
                -Method Delete `
                -Uri "$apiRoot/jobs/$jobId" `
                -TimeoutSec 30 `
                -Headers @{ "X-Live-Job-Token" = $jobToken } | Out-Null
            if ($terminal) {
                Write-Host "Requested cleanup for terminal canary job $jobId."
            }
            else {
                Write-Warning "Requested cancellation for non-terminal canary job $jobId."
            }
        }
        catch {
            Write-Warning "Could not cancel or clean up canary job $jobId; server retention must remove it."
        }
    }
}
