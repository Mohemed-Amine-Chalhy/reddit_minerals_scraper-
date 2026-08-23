# Live Reddit collection

MineralLens can run the same bounded collection engine used by the command-line
pipeline from the local web application. The adapter is
[PRAW](https://praw.readthedocs.io/), the Python Reddit API Wrapper, operating
with read-only application authentication. A live job searches the selected
communities, collects bounded posts and comments, and opens the completed
snapshot in the Research Explorer.

Live collection is an operator-enabled capability, not a property of every
MineralLens deployment. Both live feature flags default to `false`. The public
GitHub Pages build is a static, credential-free portfolio and cannot run Python,
protect a client secret, or start a collection job. Use the local or self-hosted
FastAPI application for live work.

FastAPI registers the live routes in every mode so OpenAPI and clients can
discover the contract. When disabled, capabilities report `enabled: false` and a
job start returns the sanitized `503 live_collection_disabled` error. The main
`/api/v1/config` advertises live collection, mutation, and enabled providers only
when live mode is on and at least one credential mode is usable.

## What you need

A Reddit application supplies three values:

| Setting | Meaning |
| --- | --- |
| `RMS_REDDIT_CLIENT_ID` | The application's client identifier. |
| `RMS_REDDIT_CLIENT_SECRET` | The application's secret. |
| `RMS_REDDIT_USER_AGENT` | A descriptive identifier such as `script:minerallens:0.2.0 (by u/account)`. |

MineralLens does not ask for a Reddit account password or a pre-generated bearer
token. PRAW exchanges the application credentials for short-lived access
internally. Create or inspect an application from Reddit's
[application preferences](https://www.reddit.com/prefs/apps), and keep the client
secret out of chat, screenshots, shell history, browser storage, commits, and
frontend build variables.

If an older secret or password was ever committed by a legacy scraper, rotate it
before enabling this surface. Deleting the current file does not remove a value
from Git history. Revoke the old Reddit application secret, change any exposed
account password, review provider activity, and use only fresh credentials.

The deployment also requires a separate `RMS_LIVE_ACCESS_TOKEN` with at least
32 random characters. This is not a Reddit credential: it authorizes creation
of quota-consuming jobs on this FastAPI instance. The operator enters it in the
Live Reddit form or exposes it to the canary process; the browser sends it only
in `X-Live-Access-Token`, clears the field after submission, and never stores it
in a URL or browser storage. Use HTTPS outside loopback and rotate the token if
it may have been exposed.

Generate a value locally (do not paste the result into chat or commit it):

```shell
uv run python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Store the result in the ignored local `.env` file or the deployment secret
manager. The command produces the same 32-byte URL-safe shape used for job
tokens; the two values still serve different purposes and must not be reused.

## Enable the backend

Copy `.env.example` to the ignored `.env` file and select one credential mode.
Live Reddit stays unavailable until `RMS_LIVE_REDDIT_ENABLED=true` is present in
the FastAPI process environment.

### Server-managed credentials

This is the recommended mode for one trusted operator on a local machine or a
private self-hosted instance:

```dotenv
RMS_LIVE_REDDIT_ENABLED=true
RMS_LIVE_REDDIT_ALLOW_BYO_CREDENTIALS=false
RMS_LIVE_ACCESS_TOKEN=generate-a-fresh-random-value-of-at-least-32-characters
RMS_REDDIT_CLIENT_ID=replace-with-fresh-client-id
RMS_REDDIT_CLIENT_SECRET=replace-with-fresh-client-secret
RMS_REDDIT_USER_AGENT=script:minerallens:0.2.0 (by u/replace-with-account)
```

The browser sees only that server credentials are available. It does not receive
their values. FastAPI reads them from server configuration when a job starts.
Use a deployment secret store instead of `.env` outside local development.

### Credentials for one run

Bring-your-own credentials are a separate, more sensitive capability. Enable
them only on an HTTPS deployment whose users and logs you control:

```dotenv
RMS_LIVE_REDDIT_ENABLED=true
RMS_LIVE_REDDIT_ALLOW_BYO_CREDENTIALS=true
RMS_LIVE_ACCESS_TOKEN=generate-a-fresh-random-value-of-at-least-32-characters
```

The browser sends the three values in the job-creation request and keeps them out
of URLs and browser storage. The backend uses them only for that job and does not
serialize them into job metadata, snapshots, logs, or error responses. Closing a
tab is not a cancellation signal; cancel or delete the job explicitly if it is
still active.

Do not expose this mode on an anonymous public service. The deployment token is
a shared job-creation boundary, not an account, role, or per-user quota system.
Put a multi-user FastAPI deployment behind identity-aware access control, rate
limits, and HTTPS before allowing other users to submit credentials.

## Run locally

Bootstrap and start the combined development environment:

```powershell
.\scripts\bootstrap-web.ps1
.\scripts\dev-web.ps1
```

```bash
./scripts/bootstrap-web.sh
./scripts/dev-web.sh
```

Open `http://127.0.0.1:5173/pipeline`, choose **Live Reddit**, and configure:

- one or more minerals and subreddits;
- the Reddit time window;
- maximum posts per mineral and comments per post;
- the progress refresh interval;
- the deployment access token; and
- **Use server credentials** or, when explicitly enabled, **Use credentials for
  this run**.

Start with very small limits. The page reports queued/running progress, supports
cooperative cancellation, downloads a successful or partial snapshot, deletes
the server-side job artifacts, and only then transfers the raw result into the
in-memory Explorer. A failed deletion retains the job token and offers a retry.
Live credentials never belong in query parameters, client-side routes, local
storage, or the imported dataset.

## Job lifecycle and API

The generated OpenAPI page at `/api/v1/docs` is the authoritative request and
response reference. The browser uses these versioned routes:

| Route | Purpose |
| --- | --- |
| `GET /api/v1/live/capabilities` | Discover whether live mode and each credential mode are available without revealing values. |
| `POST /api/v1/live/jobs` | Validate both access headers and bounded inputs, then idempotently create or recover a job. |
| `GET /api/v1/live/jobs/{id}` | Read progress and the terminal summary. |
| `DELETE /api/v1/live/jobs/{id}` | Request cooperative cancellation, or immediately delete a terminal job and its artifacts. |
| `GET /api/v1/live/jobs/{id}/snapshot` | Retrieve Explorer-compatible results after success or partial completion. |

A creation body contains `targets`, `time_filter`,
`max_posts_per_mineral`, `max_comments_per_post`, and `credential_mode`.
`credentials` must be present only for `provided` mode:

```json
{
  "targets": [
    {
      "mineral": "gold",
      "subreddits": ["mining"]
    }
  ],
  "time_filter": "week",
  "max_posts_per_mineral": 2,
  "max_comments_per_post": 5,
  "credential_mode": "server"
}
```

One job accepts 1–10 unique mineral targets, each with 1–20 syntactically valid
subreddits. Time filters are `hour`, `day`, `week`, `month`, `year`, and `all`.
The absolute request-schema ceilings are 100 posts per mineral, 500 comments per
post, and 10,000 estimated records across all targets. A deployment can advertise
lower post/comment bounds from its configured pipeline limits, and clients must
honor `/capabilities`. Defaults are `week`, 10 posts, and 25 comments. These are
safety ceilings, not recommended starting values; the canary deliberately uses 2
and 5.

Job creation requires `X-Live-Access-Token`. The server compares it in constant
time before reading the request body and returns a sanitized `401` when it is
absent or wrong. The creation endpoint also rejects bodies larger than 64 KiB,
including streamed bodies without `Content-Length`, before model validation. The
live manager also caps queued/running work with `RMS_LIVE_JOB_MAX_ACTIVE` (1–16;
default 4), so even an authorized operator cannot build an unbounded in-process
queue.

The browser generates a 32-byte random `X-Live-Job-Token` for each submission;
FastAPI echoes it in the creation response and requires it for every later job
request. Treat this opaque value as a secret: it grants access to that job's
status and collected snapshot. It stays in page memory and must not be logged or
put in a URL. Retrying the same request with the same token recovers the existing
job instead of spending quota twice. Reusing the token with different targets,
bounds, or credential mode returns a sanitized conflict. A missing, invalid, or
cross-job token must not reveal whether another job exists. Idempotency lasts
only while that job is retained; generate a fresh token for every deliberate new
submission after deletion or expiry.

The lifecycle is `queued` → `running` → `succeeded`, `partial`, or `failed`.
Cancellation moves through `cancel_requested` to `cancelled`; it is cooperative,
so an already-running provider request may finish before the worker observes the
request. Do not kill FastAPI merely to cancel one job. During normal application
shutdown, FastAPI requests cooperative cancellation and waits for workers. The
HTTP connect/read timeout bounds network inactivity, but a thread-based PRAW
operation has no safe in-process hard-kill primitive. Configure the external
service supervisor with a reviewed termination grace period and allow it to
force-stop a genuinely stuck process; marked orphan cleanup handles its isolated
job directory on restart.

`expires_at` remains `null` until a job becomes terminal. Snapshots are available
only for `succeeded` and `partial`; every other authenticated state returns the
safe `409 live_snapshot_unavailable` response. Snapshot comments expose their
post link and Reddit parent link as distinct fields, retain no author name, and
remain untrusted collected text even though Reddit made the source public.

Each job receives an isolated SQLite database beneath `RMS_LIVE_JOB_ROOT`.
`RMS_LIVE_JOB_MAX_WORKERS` bounds concurrent workers from 1–4 and defaults to
one. `RMS_LIVE_JOB_MAX_ACTIVE` independently bounds queued and running jobs.
Terminal artifacts are retained for `RMS_LIVE_JOB_RETENTION_SECONDS`
(1–86,400 seconds; default 3,600), while the total tracked-job registry is capped
by `RMS_LIVE_JOB_MAX_RETAINED` (1–1,000; default 100 and never lower than the
active-job cap). Oldest terminal jobs are purged first when capacity is needed. A manager-owned expiry
sweeper removes expired terminal metadata, databases, and SQLite sidecars even
when the API is otherwise idle; request-time cleanup also enforces count bounds.
`DELETE` cancels queued/running work; on a terminal job it returns the
last status with a zero record count/deleted message, immediately removes the
exact job directory, and makes later access return `404`. Every owned directory
contains a non-secret MineralLens format marker. Startup removes only marked,
exact 32-hex job directories that are unreachable after a process restart;
unmarked directories are never treated as MineralLens artifacts.

One live manager owns one job root. It acquires an OS-level exclusive lock for
its lifetime and fails startup closed if another application process or worker
already owns that root. Run a single FastAPI process for a live deployment. If
process isolation is intentionally required, give every process a different job
root; jobs and tokens are not shared across them. Orphan cleanup starts only
after ownership is acquired.
Export or otherwise retain a snapshot deliberately if it is a research artifact,
then manage that copy under the project's data-retention rules.

## Low-limit canary

After enabling live mode, run the repository's canary instead of testing with a
large browser job:

```powershell
.\scripts\live-reddit-canary.ps1 -Mineral gold -Subreddit mining
```

```bash
./scripts/live-reddit-canary.sh --mineral gold --subreddit mining
```

The scripts create a one-mineral job with low post/comment bounds, retain the job
and deployment tokens only in process memory, poll to a terminal state, verify a
snapshot when available, and print counts rather than collected content. An interrupted or
timed-out canary requests cooperative cancellation, while a terminal canary is
immediately deleted after verification. The scripts use server credentials by
default. Run
`Get-Help .\scripts\live-reddit-canary.ps1 -Detailed` on PowerShell or
`./scripts/live-reddit-canary.sh --help` on Bash for the explicit one-run
credential mode and timeout controls.

Set `RMS_LIVE_ACCESS_TOKEN` in the canary process environment before running the
script. The value must match the FastAPI deployment and is never printed.

## Gemini analysis is optional and separate

The live web job collects Reddit content; it does not silently send text to an AI
provider. No Gemini key is needed to use Live Reddit. If the research workflow
also needs relevance, enrichment, or reputation analysis, configure
`RMS_GEMINI_API_KEY` and an explicitly evaluated `RMS_GEMINI_MODEL`, then run the
corresponding bounded CLI stages against an operator-managed pipeline database.
Those commands have independent provider use, cost, methodology, and retention
considerations; see [configuration](configuration.md) and the
[operations runbook](operations.md).

## Troubleshooting

- **Live Reddit is not shown:** verify that the FastAPI process—not a frontend
  `.env`—has `RMS_LIVE_REDDIT_ENABLED=true`, restart it, and inspect
  `/api/v1/live/capabilities`. Static Pages intentionally has no live backend.
- **Job creation is unauthorized:** enter the same `RMS_LIVE_ACCESS_TOKEN`
  configured on FastAPI. Do not substitute a Reddit client secret or job token.
- **Server credentials are unavailable:** configure all three Reddit settings.
  A partial set is rejected; no setting value is returned to the browser.
- **One-run credentials are unavailable:** set
  `RMS_LIVE_REDDIT_ALLOW_BYO_CREDENTIALS=true` and restart FastAPI. Leave this
  disabled on public or HTTP deployments.
- **Authentication fails:** confirm the client ID and secret belong to the same
  active application and the user agent is descriptive. Rotate rather than
  printing a value during diagnosis.
- **A job stays queued:** the bounded worker pool is busy. Wait, cancel another
  job you own, or change the server worker limit after reviewing provider rate
  and host capacity.
- **Cancellation is not immediate:** PRAW or Reddit may still be completing an
  in-flight request. The job stops scheduling new bounded work after cancellation
  is observed.
- **A snapshot is unavailable:** it is exposed only for a `succeeded` or
  `partial` job and only while that job remains inside its retention window.
- **Rate limits or timeouts occur:** lower posts, comments, and subreddit count;
  avoid concurrent jobs; then retry after the provider recovers. Do not expand
  retry loops or rotate identities to evade a provider limit.

See [troubleshooting.md](troubleshooting.md) for database, CLI, provider, and
secret-incident guidance.
