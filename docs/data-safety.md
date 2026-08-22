# Data-safety guarantees

This page describes behavior enforced by the application. It is intentionally
focused on implementation guarantees rather than organizational process.

## Collection boundaries

- Reddit access is read-only and uses application authentication; the CLI never
  accepts a Reddit username or password.
- Every collection command has explicit post, comment, content-size, request,
  retry, and operation-time limits.
- Provider records are converted into strict Pydantic models before persistence.
- The canonical schema does not store Reddit author names, private messages,
  device data, or inferred demographic attributes.

## Secrets and logs

- Credentials are read from `RMS_*` environment variables or the deployment
  secret mechanism and are represented as Pydantic secret values.
- Safe configuration summaries expose only configured/not-configured states.
- Structured logs contain identifiers, counts, statuses, timings, and error
  categories—not credential values or collected content bodies.
- `.env`, SQLite databases, exports, logs, notebook outputs, and common provider
  credential shapes are rejected or ignored by repository checks.

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

## Verification

The offline test suite covers secret-safe configuration output, log
sanitization, bounded provider calls, prompt delimiting, schema rejection,
transaction rollback, deletion cascades, tombstone suppression, and export path
protection. `SECURITY.md` describes private vulnerability reporting and
credential response.
