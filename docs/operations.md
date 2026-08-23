# Operations runbook

## Runtime prerequisites

The operator must be able to stop the scheduler, revoke both provider
credentials, inspect provider usage, and restore the database without repository
access. Before the first live run, configure explicit limits, timeouts, logging,
backups, and alerts, then test interruption, deletion, and restore behavior in an
isolated environment.

## Preflight

From the deployed version and working directory:

```shell
reddit-minerals validate-config
reddit-minerals status --json
```

Confirm:

- configuration points to the intended database and mapping;
- the safe summary reports only configured/not-configured states, never values;
- free disk space exceeds expected database, temporary export, and backup size;
- no other process writes the same SQLite file;
- provider quotas, Gemini model, analysis threshold, limits, and log destination
  match the intended deployment;
- the previous run did not leave unexplained partial or failed work.

## First-run canary

Use one mineral and low bounds:

```shell
reddit-minerals scrape --mineral gold --max-posts 2 --max-comments 5 --dry-run
reddit-minerals scrape --mineral gold --max-posts 2 --max-comments 5
reddit-minerals status
reddit-minerals relevance --mineral gold --limit 2
reddit-minerals enrich --mineral gold --limit 2
reddit-minerals reputation --mineral gold --limit 2
reddit-minerals status --json
reddit-minerals export --mineral gold --format jsonl --output exports/canary.jsonl --overwrite
```

Inspect counts, statuses, latency, provider usage, model identifier, token/cost
data, and a small sanitized sample of results. Remove the canary export after the
review. Do not increase limits until failures and costs are understood.

For an operator-enabled FastAPI instance, exercise the live-job boundary with
the repository canary. It uses server credentials unless one-run credentials are
requested explicitly:

```powershell
.\scripts\live-reddit-canary.ps1 -Mineral gold -Subreddit mining
```

```bash
./scripts/live-reddit-canary.sh --mineral gold --subreddit mining
```

Set the matching `RMS_LIVE_ACCESS_TOKEN` in the canary process environment. The
script first reads non-secret capabilities, creates a two-post/five-comment job
with that deployment token, retains both tokens only in process memory, polls to a terminal
state, and verifies the snapshot without printing collected text. A `partial`
terminal state is inspectable but is not a fully successful canary; review its
safe error/count summary before proceeding.

## Live-job operations

Keep `RMS_LIVE_JOB_MAX_WORKERS=1` until provider behavior and host capacity have
been measured. A larger pool increases simultaneous Reddit requests and SQLite
storage. `RMS_LIVE_JOB_MAX_ACTIVE` separately caps all queued/running jobs and
defaults to four. Keep `RMS_LIVE_JOB_ROOT` on a private local writable filesystem
and out of application assets, Git worktrees, and synchronized folders.

The manager holds an OS-level exclusive ownership lock on the job root. A second
process or web worker pointed at the same root fails startup closed before it can
purge or access job data. Operate live mode with one FastAPI process per root;
separate processes require separate roots and have independent job/token state.

Status values are `queued`, `running`, `cancel_requested`, `cancelled`,
`succeeded`, `partial`, and `failed`. Cancellation is cooperative: a running
PRAW request can finish before the worker stops scheduling work. Do not terminate
the entire API or delete database sidecars to cancel one job. `DELETE` changes a
queued job directly to cancelled, asks a running job to cancel, and immediately
removes a terminal job's metadata and exact isolated directory. Later access to
that terminal job returns `404`.

A normal FastAPI shutdown requests cooperative cancellation and waits for active
workers to finish provider calls and cleanup. PRAW HTTP connect/read inactivity
is timeout-configured, but the thread worker cannot safely kill an arbitrary
in-flight library operation. Set an external supervisor termination grace period
from measured canaries, then allow the supervisor to force-stop a genuinely
stuck process. A forced stop leaves marked work for startup orphan cleanup rather
than a complete terminal audit trail.

The manager's expiry sweeper purges terminal artifacts after
`RMS_LIVE_JOB_RETENTION_SECONDS` even when no API request arrives; request-time
cleanup also bounds the total tracked-job registry with
`RMS_LIVE_JOB_MAX_RETAINED`, purging the oldest terminal work first. Treat expiry
as a safety bound, not a backup policy.
If a snapshot is a required research artifact, export it deliberately to managed
storage before expiry and apply the same deletion, backup, and access controls as
other collected content.

Every live job directory carries a non-secret MineralLens ownership marker.
After the root lock is acquired on startup, only marked exact 32-hex directories
with no reachable in-memory job are treated as prior-process orphans and removed.
Unmarked directories are left untouched, but unrelated data still does not
belong in `RMS_LIVE_JOB_ROOT`.

Every creation request requires the configured `X-Live-Access-Token`, and active
jobs are server-bounded. On a multi-user deployment, also require identity-aware
authentication, HTTPS, per-user rate controls, and authorization. The shared
deployment token and opaque job token are bearer capabilities, not accounts or
roles. Never log either token or provider credentials.

## Scheduling

Run commands as separate scheduled jobs so collection, analysis, and export have
independent limits and alerts. Allow only one writer against a database at a time;
tracked CLI commands enforce this and fail rather than overlap. `status` is
read-only and may run while a tracked command owns the lock. A typical order is:

1. `validate-config`;
2. bounded `scrape`;
3. bounded `relevance`;
4. bounded `enrich`;
5. bounded `reputation`;
6. `status --json` and alert evaluation;
7. an export, if required;
8. backup and deletion processing.

Use the scheduler's secret injection, working-directory, timeout, concurrency,
and retry controls. Scheduler retries must not create an unbounded loop around
the application's own bounded provider retries. Prefer the application's resume
semantics over overlapping jobs.

The operation timeout is a cooperative run-wide budget: the application stops
starting provider work and bounds its own retry backoff once that budget expires.
It cannot interrupt an already in-flight provider call. In particular, PRAW may
perform finite internal HTTP retries or a Reddit rate-limit sleep before control
returns to the application; each individual HTTP request is bounded by
`RMS_REDDIT_REQUEST_TIMEOUT_SECONDS`, but one PRAW operation can span more than
one such window. Gemini calls are bounded by `RMS_GEMINI_REQUEST_TIMEOUT_SECONDS`.
Set the scheduler timeout above the operation budget with enough reviewed margin
for those provider-library waits so the application can close its run record and
transaction before the scheduler sends its final termination signal.

## Expected output and exit behavior

Pretty-printed JSON command summaries go to standard output. Structured JSON logs
go to standard error and contain timestamps, levels, logger names, event messages,
identifiers, statuses, timings, and counts—not content or credential values.
`status --json` is the machine-readable status interface.

An exit status of zero means the command completed its control flow; operators
must still review reported per-item failures after a partial success. A batch
that selected work but completed no provider operation exits non-zero instead of
reporting false success. Other non-zero statuses cover command-level
configuration, validation, provider initialization, deadline, storage, or
unexpected failure. Alert on both non-zero exits and failure/blocked counts
exceeding the deployment's baseline.

Analysis summaries also report `stale_discarded`. This is a concurrency safety
signal: the provider returned after its source, configuration, or relevance
dependency changed, so the result was not written. A later run reselects current
work. Investigate repeated values as overlapping writers or an unstable refresh
schedule.

## Monitoring

Capture and trend:

- run status and duration by command;
- discovered/completed/skipped/failed posts and stored comments;
- work items by state and age;
- analyses selected, completed, blocked, retryable, and permanent failures;
- analysis results discarded because their source/configuration/dependency changed;
- run counts by status, including automatically reconciled interrupted runs;
- queued/running live jobs, queue age, terminal outcomes, cancellations, and
  retention cleanup failures;
- active database schema version and retained post/comment tombstone counts;
- schema-invalid responses and provider error categories;
- p50/p95 latency and request/token counts by analysis kind/model;
- estimated cost against daily and monthly budgets;
- database size, free disk, backup age, and restore-test age;
- deletion results and retained tombstone counts.

Example alert conditions must be tuned from a bounded canary: any authentication
failure, database-integrity failure, unexpected model change, secret-scanner
finding, repeated non-zero exit, growing retryable backlog, or cost/disk threshold
breach should stop or page rather than silently expand work.

## Interruption and resumption

On `Ctrl+C`, scheduler timeout, host restart, or provider outage, do not edit the
database or mark rows complete manually. Confirm that no process is still active,
run `status`, investigate any `partial` or failure states, then rerun the same
bounded command. `status` deliberately leaves a potentially active `running` row
unchanged. The next tracked writer first acquires the released cross-process lock,
then changes audit rows left `running` by an uncatchable exit to failed
`InterruptedRun` rows before doing new work. Complete content and analyses are
skipped unless stale or `--force` is explicitly used.

Use `--force` only to reprocess after a documented prompt/model/schema correction.
It increases provider use and replaces the current analysis identity.

## Backups

SQLite backups must be consistent and encrypted at rest. Either stop the writer
and copy the database, or use SQLite's online backup facility. Do not copy an
actively changing database with an ordinary file copy.

After backup, verify on a separate path:

```shell
sqlite3 backup/reddit_minerals.sqlite3 "PRAGMA integrity_check; PRAGMA foreign_key_check; PRAGMA user_version;"
```

Expected output starts with `ok`, no foreign-key rows, and the supported schema
version. Record backup time, source version, checksum, encryption/key reference,
expiry, and restore-test result without storing content in the runbook.

Test restoration periodically in an isolated directory: restore the backup,
point `RMS_DATABASE_PATH` at it, run `status --json`, perform integrity checks,
and run an offline export. Never restore over the live file while a job runs.

## Provider incidents

### Authentication or authorization

Stop networked stages. Confirm the configured/not-configured summary,
application registration, model availability, and account/project scope. Never
print a secret to test it. Rotate it if exposure is possible.

### Rate limiting or outage

Keep limits fixed or lower them. Let only explicitly retryable failures use
bounded backoff. Pause the scheduler when the retryable backlog or cost grows;
resume after provider recovery and quota confirmation.

### Schema or safety failures

Do not coerce invalid output or substitute neutral results. Preserve the distinct
failure/blocked state, stop the affected analysis stage if the rate exceeds its
expected baseline, retain only safe metadata, and evaluate the model/prompt/schema
change offline before retrying.

## Database incident

If integrity checks fail or an unsupported schema is reported:

1. stop all writers and exports;
2. preserve the affected file read-only for investigation;
3. capture application version, file size, integrity output, and last successful
   run—never raw rows in a ticket;
4. restore the most recent verified backup to a new path;
5. point a staging process at the restored copy and verify status/export;
6. promote only after root cause and lost-work window are documented.

Do not use ad hoc `UPDATE`, `.recover`, or schema-version edits on the sole copy.

## Content deletion

Always preview the stable ID and affected counts before deletion. A successful
database transaction does not remove prior exports, notebooks, provider logs, or
backups; remove or regenerate those copies separately. See `data-safety.md` for
the enforced database behavior.

## Credential incident

Follow `SECURITY.md`: revoke/rotate first, stop jobs, inspect scope, purge Git
history and artifacts when applicable, re-clone, review usage/billing, and add a
safe regression detection rule. Editing the current source is not remediation.

## Routine maintenance

- Apply reviewed dependency updates and re-run offline tests/evaluation.
- Rotate credentials and review access lists.
- Re-evaluate the subreddit mapping and sampling rationale.
- Test database restore and content deletion.
- Review failed/blocked backlogs and old exports/backups.
- Compare the deployed image/package digest and model identifier to the release
  record.
