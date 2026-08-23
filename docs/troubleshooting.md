# Troubleshooting

Start with the exact command, its non-secret structured error type, and:

```shell
reddit-minerals validate-config
reddit-minerals status --json
```

Never paste `.env`, provider request bodies, raw Reddit text, full database rows,
or secret values into an issue or support message.

## `uv` is not found

Install `uv` from its official installation instructions, reopen the terminal so
`PATH` is refreshed, and run the platform bootstrap script. Do not replace the
locked workflow with global `pip install` commands; that produces an unsupported
environment.

## Lockfile check or synchronization fails

Use Python 3.12 and a supported `uv` version. Confirm `pyproject.toml` and
`uv.lock` come from the same commit and that no merge conflict changed one. A
dependency update should intentionally run lock resolution, be reviewed, and
pass the full check—not be fixed by deleting the lockfile.

## `.env` was created but provider commands say configuration is incomplete

`validate-config` can inspect local paths without requiring every provider. The
networked command checks its own credentials. Confirm the process working
directory, exact `RMS_` names, placeholder replacement, scheduler/container secret
injection, and that `RMS_GEMINI_MODEL` is explicitly set for AI commands. Do not
print values. Environment variables override `.env` and can accidentally retain
an old value.

## Subreddit mapping is invalid

The file must be UTF-8 JSON, contain a non-empty object, map each non-empty
mineral to a non-empty list, and use subreddit names of 2–64 letters, digits, or
underscores. JSON comments and trailing commas are invalid. Duplicate names are
removed within a mineral case-insensitively. Run `validate-config` after editing.

Validation does not prove a community exists or is accessible. A syntactically
valid but restricted community still fails at the provider boundary.

## Reddit authentication, 403, or 404 errors

Confirm credentials belong to the intended application, authentication is
read-only application mode, and the user
agent is descriptive. A 403/404 can also mean a private, banned, quarantined,
removed, or mistyped subreddit or content item. Do not bypass restrictions or
switch identities to evade a limit.

## Live Reddit is missing from the Pipeline page

The static Pages build never has a provider backend. On a local or self-hosted
FastAPI instance, set `RMS_LIVE_REDDIT_ENABLED=true` in the server process
environment, restart both development processes, and inspect
`GET /api/v1/live/capabilities`. Do not use a `VITE_*` variable: frontend values
are public build inputs and cannot enable the Python worker safely.

Enabling live mode requires a random `RMS_LIVE_ACCESS_TOKEN` with at least 32
characters. Server mode is advertised only when all three `RMS_REDDIT_*` values
are valid. One-run mode additionally requires
`RMS_LIVE_REDDIT_ALLOW_BYO_CREDENTIALS=true`. The shared deployment token is not
a user-account system; keep one-run mode disabled on anonymous, non-HTTPS, or
otherwise untrusted deployments.

## Live job creation returns `401`

Enter the exact `RMS_LIVE_ACCESS_TOKEN` configured in the FastAPI process. It is
not the Reddit client secret and not a previous job token. Restart FastAPI after
rotating it. The UI clears the entry after submission, so a rejected attempt
requires re-entry without exposing the value in a URL or browser store.

## A live job cannot be opened, cancelled, or fetched

Keep the creation response's opaque token in process/page memory and send it in
`X-Live-Job-Token` for every job-specific request. A missing, invalid, expired,
or cross-job token intentionally receives no credential or job detail. Never put
the token in a URL, issue, screenshot, log, or support message.

A terminal job or snapshot may already have passed
`RMS_LIVE_JOB_RETENTION_SECONDS` or been removed as the oldest terminal job while
enforcing the `RMS_LIVE_JOB_MAX_RETAINED` total registry cap. Retention cleanup is irreversible; retain a needed
research snapshot separately under explicit access and deletion controls.

If the UI reports that terminal cleanup failed, keep the page open and use its
retry action. The job token remains in page memory until deletion succeeds. The
idle expiry sweeper remains a fallback, but explicit successful deletion gives
the clearest operator confirmation.

## Live cancellation remains `cancel_requested`

Cancellation is cooperative. PRAW or Reddit can be completing an in-flight
bounded request before the worker observes the request and stops scheduling new
work. Continue polling within the configured operation timeout. Do not kill the
API, delete files from `RMS_LIVE_JOB_ROOT`, or remove SQLite sidecars while a job
may still own them.

## Reddit rate limits, timeouts, or incomplete posts

Lower `--max-posts`, `--max-comments`, and placeholder-expansion limits; avoid
overlapping jobs; keep bounded backoff; and pause on sustained rate limiting.
When comment collection fails, the post remains in a failure state rather than
complete. After recovery, rerun the same bounded command and let resume logic
retry it.

## Gemini command says a key or model is missing

Both `RMS_GEMINI_API_KEY` and `RMS_GEMINI_MODEL` are required. Select a model that
the configured project, tier, and region can access and that has passed the
project evaluation. The application intentionally has no silent default model.

## Gemini output is blocked or schema-invalid

A blocked result or provider/validation failure is not a neutral analysis. Check
the distinct status counts, provider safety policy, input eligibility, current
model, prompt/schema version, and recent failure rate. Do not disable safety
controls or parse malformed output with regular expressions. Reproduce using a
sanitized evaluation case and roll back the analysis configuration if the release
criteria regressed.

## Analysis selects zero items

Check that:

- scraping completed for the mineral;
- the mineral spelling matches normalized configuration;
- the requested analysis already has complete results and `--force` was not used;
- reputation candidates have a complete relevant=true result at or above the
  0–100 relevance threshold;
- the limit is positive and the database path is the intended one.

Use `--force` only when deliberate recomputation is required, not to hide an
eligibility/configuration problem.

## Database is locked or another tracked command owns it

SQLite supports this workload as a single scheduled writer. The CLI's operation
lock rejects overlapping scrape, migration, deletion, analysis, or export
processes; wait for the owner to exit rather than deleting its `.operation.lock`
file. Confirm the database is on a local writable filesystem, not a network share,
synchronization folder, or read-only container layer. Do not delete the operation
lock, `-wal`, `-shm`, or journal files while a process may be active. The operation
lock is advisory, so unsupported direct SQLite writers remain prohibited.

## Database schema is newer than the application

The database was opened by newer code. Stop immediately and deploy the matching
or newer reviewed application. Do not lower `PRAGMA user_version`. To roll back,
restore the verified pre-migration backup to a new path and use the prior code
against that restored copy.

## Integrity check fails

Stop writers and exports. Preserve the affected file under incident controls and
restore a verified backup to a new path. Follow the database-incident section of
`operations.md`; do not experiment on the only copy or post raw recovery output
that includes content.

## Export is empty or cannot be replaced

Confirm `status` has canonical content, the optional `--mineral` matches stored
associations, the parent path is writable, free space is sufficient for a full
temporary file, and no program holds the destination open (common on Windows).
Exports are written atomically through a temporary sibling; a failed attempt
should not replace the previous file.

Use `--overwrite` only after verifying the exact existing destination. Do not set
the export output to the database, a `-wal`/`-shm`/journal/operation-lock sidecar,
or a hard-link alias of the database.

## Legacy migration reports invalid or orphan records

Use the dry-run counts and follow `migration.md`. Confirm each JSON document is an
array, required IDs/subreddit/timestamp fields exist, timestamp formats are valid,
and each comment's post exists in the same mineral directory. Correct a copy of
the source and repeat the dry run; never edit target tables to force acceptance.

## Notebook import or path errors

Notebooks are optional and their data-science packages may not be in the core
runtime. Install the documented notebook environment, create a CLI export, open
the notebook from the repository or `notebooks/`, and set `EXPORT_PATH` relative
to the repository root or as an absolute path. Missing exports are reported
without contacting providers. Clear outputs before committing.

## Secret scanner reports a credential

Assume it is compromised. Revoke/rotate it first and stop affected jobs. Remove
it from the current tree, purge reachable history and artifacts under an incident
plan, require re-clones, and review provider usage. Do not add the live value to a
scanner allowlist or baseline.

## A command exits non-zero but logs seem sparse

Raise `RMS_LOG_LEVEL` only to `DEBUG` in an isolated environment; debug logging is
still required to omit content and secrets. Capture the application commit,
command flags without secret values, error type, run ID, timestamp, and status
snapshot. Re-run offline validation before repeating a provider operation.
