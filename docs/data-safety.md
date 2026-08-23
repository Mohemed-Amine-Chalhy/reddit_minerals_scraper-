# Data-safety guarantees

This page describes behavior enforced by the application. It is intentionally
focused on implementation guarantees rather than organizational process.

## Collection boundaries

- Reddit access is read-only and uses application authentication through PRAW;
  neither the CLI nor live web jobs accept a Reddit username or password.
- Every collection command has explicit post, comment, content-size, request,
  retry, and operation-time limits.
- Provider records are converted into strict Pydantic models before persistence.
- The canonical schema does not store Reddit author names, private messages,
  device data, or inferred demographic attributes.

## Secrets and logs

- Credentials are read from `RMS_*` environment variables or the deployment
  secret mechanism and are represented as Pydantic secret values.
- One-run Reddit credentials are accepted only when the operator enables both
  live flags. They remain in request/worker memory and are not serialized into
  job metadata, SQLite, snapshots, logs, or responses.
- Live job creation requires a separate random deployment access token in
  `X-Live-Access-Token`. It is constant-time compared, never returned by the API,
  and is distinct from Reddit credentials.
- Each live job has a browser-generated 32-byte opaque access token echoed at
  creation and required in `X-Live-Job-Token`; tokens are never accepted in URLs
  or persisted by the browser application. Identical retries reuse the job
  without retaining plaintext tokens on the server.
- Safe configuration summaries expose only configured/not-configured states.
- Structured logs contain identifiers, counts, statuses, timings, and error
  categories—not credential values or collected content bodies.
- `.env`, SQLite databases, exports, logs, notebook outputs, and common provider
  credential shapes are rejected or ignored by repository checks.

The deployment token is a shared creation boundary and the returned token is
job-level capability isolation; neither is an account or role system. A
multi-user deployment must add HTTPS, identity-aware authorization, and
per-user request-rate controls at the deployment boundary. GitHub Pages cannot
protect a secret or run FastAPI and therefore never offers live collection.

## Untrusted input boundaries

Reddit content and provider responses are untrusted. The pipeline bounds and
delimits model input, validates structured output, uses parameterized SQL, and
never executes collected text. Invalid or blocked responses remain explicit
failure states instead of being converted into plausible-looking results.

## Deletion behavior

`delete-content` first reports the affected post or comment and its derived
records. A confirmed deletion removes canonical and derived database rows in one
transaction and writes a tombstone so the same stable Reddit ID is not collected
again. The database cannot revoke copies already published as exports or copied
into notebooks, backups, or external systems; those snapshots must be removed or
regenerated separately.

Live web jobs use isolated databases. Terminal jobs are removed after the
operator-selected time/count retention bound by an idle-safe manager sweeper, or
immediately after an authenticated terminal `DELETE`; active `DELETE` requests
cooperative cancellation first. Cleanup includes SQLite sidecars, but cannot
retract a snapshot copied or exported elsewhere. Every owned directory carries
a non-secret format marker. Startup cleanup removes only marked exact 32-hex
orphan job directories beneath the configured root and leaves unmarked folders
untouched.

## Verification

The offline test suite covers secret-safe configuration output, log
sanitization, bounded provider calls, prompt delimiting, schema rejection,
transaction rollback, deletion cascades, tombstone suppression, and export path
protection. `SECURITY.md` describes private vulnerability reporting and
credential response.
