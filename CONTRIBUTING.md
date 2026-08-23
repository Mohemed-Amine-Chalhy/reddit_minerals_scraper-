# Contributing

Thank you for improving MineralLens. Changes should preserve the separation
between the React presentation layer, read-only FastAPI adapter, and Python
pipeline engine, together with bounded provider use, resumability, and offline
testability.

## Before contributing

- Do not contribute secrets, production data, Reddit usernames, content bodies,
  generated exports, database files, logs, notebook outputs, or personal
  documents.
- Confirm that any fixture is synthetic or approved repository-safe public
  metadata. The only versioned derived-data asset is the documented sample built
  by `scripts/prepare_kaggle_sample.py`; source archives and source IDs remain
  ignored. Repository-local hashes are not described as anonymization.
- Read `SECURITY.md` before reporting a vulnerability.

## Set up

Use Python 3.12, Node 24, and the checked-in Python and pnpm lockfiles. The
repository scripts validate the pinned runtimes before installing dependencies:

```powershell
.\scripts\bootstrap.ps1
```

```bash
./scripts/bootstrap.sh
```

For web-only setup, use `scripts/bootstrap-web.ps1` on Windows or
`scripts/bootstrap-web.sh` on POSIX systems.

Create `.env` from `.env.example` only when a manual provider canary is needed.
The normal test suite does not need provider credentials.

## Make a change

1. Create a focused branch.
2. Keep provider clients, pipeline logic, persistence, HTTP adaptation, and UI
   presentation separated.
3. Add or update tests for behavior and failure paths.
4. Update user and operator documentation in the same change.
5. Run the full local check script before opening a review.

Prefer small, reviewable commits with imperative subjects, for example:
`Validate subreddit names at startup`. Keep the canonical sample in the same
focused commit as its generator/provenance change; never mix unrelated generated
output or formatting into a functional commit.

## Required checks

The local check script is authoritative and includes Python and frontend
formatting, linting, strict typing, tests with coverage, production web builds,
configuration-file validation, notebook hygiene, secret scanning, dependency
audits, and isolated install/smoke checks for both the wheel and source
distribution. Pre-commit provides a faster changed-file subset.

Use `scripts/check-web.ps1` or `scripts/check-web.sh` for the focused FastAPI and
React gate. It must pass before changes to `web/` or
`src/reddit_minerals/web/` are reviewed.

Do not weaken a quality gate or add a broad ignore to make a change pass. Narrow
exceptions require a comment explaining the invariant and a regression test.

## Tests

- Unit tests cover models, configuration, parsing, storage transitions, retry
  classification, summaries, and deletion behavior.
- Integration tests use fake provider clients and temporary databases.
- API contract tests use an injected in-memory repository and assert that web
  startup cannot construct provider clients or access credentials.
- Frontend tests cover runtime schema validation, filtering, imports, replay,
  fallback behavior, accessibility semantics, and failure states.
- Contract fixtures contain sanitized provider response shapes only.
- Live tests, when deliberately enabled by a maintainer, must use strict limits
  and a non-production database. They do not run in pull requests.

Tests must be deterministic: freeze time or random jitter when relevant and
never depend on network access, execution order, or an existing local database.

## Web application changes

- Keep `/api/v1` GET-only unless a separately reviewed product requirement adds
  an authenticated mutation boundary.
- Validate every network or imported-file payload before it reaches components.
- Keep the active data-source label visible whenever charts or records appear.
- Preserve URL-backed explorer filters, keyboard operation, responsive layouts,
  reduced-motion behavior, and useful empty/error states.
- Do not put credentials or private configuration in `VITE_*` variables; they
  are compiled into public browser assets.
- Treat the static Pages build and the API-backed container build as supported
  delivery modes, and test both after changing routing or data loading.

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

- No credentials, personal information, raw provider content, source archive, or
  undocumented generated data is committed.
- Failure states remain observable; errors are not replaced with plausible data.
- Work is bounded, idempotent, resumable, and safe to interrupt.
- Logs contain identifiers and counts, not content bodies or secret values.
- New settings have safe defaults and appear in `.env.example` and the
  configuration reference.
- User-visible behavior, migration steps, and operational impact are documented.
