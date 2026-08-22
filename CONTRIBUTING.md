# Contributing

Thank you for improving the Reddit Minerals Pipeline. Changes should preserve
bounded provider use, data minimization, resumability, and offline testability.

## Before contributing

- Do not contribute secrets, production data, Reddit usernames, content bodies,
  generated exports, database files, logs, notebook outputs, or personal
  documents.
- Confirm that any fixture is synthetic or irreversibly sanitized.
- Read `SECURITY.md` before reporting a vulnerability.

## Set up

Use Python 3.12 and the checked-in lockfile:

```powershell
.\scripts\bootstrap.ps1
```

```bash
./scripts/bootstrap.sh
```

Create `.env` from `.env.example` only when a manual provider canary is needed.
The normal test suite does not need provider credentials.

## Make a change

1. Create a focused branch.
2. Keep provider clients, pipeline logic, persistence, and CLI presentation
   separated.
3. Add or update tests for behavior and failure paths.
4. Update user and operator documentation in the same change.
5. Run the full local check script before opening a review.

Prefer small, reviewable commits with imperative subjects, for example:
`Validate subreddit names at startup`. Never mix generated data or unrelated
formatting into a functional commit.

## Required checks

The local check script is authoritative and includes formatting, linting, strict
typing, tests with coverage, configuration-file validation, notebook hygiene,
secret scanning, a dependency audit, and isolated install/smoke checks for both
the wheel and source distribution. Pre-commit provides a faster changed-file
subset.

Do not weaken a quality gate or add a broad ignore to make a change pass. Narrow
exceptions require a comment explaining the invariant and a regression test.

## Tests

- Unit tests cover models, configuration, parsing, storage transitions, retry
  classification, summaries, and deletion behavior.
- Integration tests use fake provider clients and temporary databases.
- Contract fixtures contain sanitized provider response shapes only.
- Live tests, when deliberately enabled by a maintainer, must use strict limits
  and a non-production database. They do not run in pull requests.

Tests must be deterministic: freeze time or random jitter when relevant and
never depend on network access, execution order, or an existing local database.

## Database and schema changes

Every schema change needs a forward migration, a documented compatibility
impact, an upgrade test from the previous supported schema, and a rollback or
restore procedure. Do not edit a production SQLite file manually.

## AI prompt or model changes

Treat prompt text, schemas, thresholds, and model identifiers as behavior
changes. Record their versions, update the evaluation dataset or expected
results, compare quality and cost against the current baseline, and document
known regressions. Never disable provider safety controls globally.

## Documentation and notebooks

Commands in documentation must match `reddit-minerals --help`. Notebooks belong
under `notebooks/`, use parameter cells, tolerate missing local exports, and are
committed without outputs or execution counts.

## Review checklist

- No credentials, personal information, raw provider content, or generated data
  is committed.
- Failure states remain observable; errors are not replaced with plausible data.
- Work is bounded, idempotent, resumable, and safe to interrupt.
- Logs contain identifiers and counts, not content bodies or secret values.
- New settings have safe defaults and appear in `.env.example` and the
  configuration reference.
- User-visible behavior, migration steps, and operational impact are documented.
