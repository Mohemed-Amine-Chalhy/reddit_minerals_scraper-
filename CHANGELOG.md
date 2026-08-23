# Changelog

All notable changes will be documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases should use
[Semantic Versioning](https://semver.org/) once a public API contract exists.

## [Unreleased]

### Added

- MineralLens, a responsive React and strict-TypeScript research interface with
  a dashboard, URL-backed explorer, record detail, deterministic pipeline
  replay, and an implementation-linked engineering case study.
- A versioned, read-only FastAPI presentation API with strict DTOs, bounded
  filtering and pagination, sanitized errors, OpenAPI documentation, and an
  injectable repository boundary.
- A deterministic, repository-safe 104-record sample derived from the owner’s
  public Kaggle dataset, with reproducible checksum-verified generation,
  identifier hashing, complete provenance, and explicit missing-content fields.
- Clearly separated public-sample API/static delivery, synthetic pipeline
  replay and regression fixtures, plus atomic browser-local JSON/JSONL import
  that never uploads selected files.
- A locked Node 24 and pnpm 11 frontend toolchain, web bootstrap/development
  scripts, Vitest coverage, ESLint, Prettier, strict type checking, and web-aware
  pre-commit/pre-push gates.
- A production SPA/API container target and a credential-free GitHub Pages
  deployment for the static portfolio demo.
- An opt-in Live Reddit workspace backed by PRAW, with deployment-authenticated
  idempotent job creation, per-job capability tokens, dynamic mineral/community
  targets, bounded workers and queues, isolated marked SQLite state, progress,
  cancellation, idle expiry, cleanup retry, and raw Explorer handoff.
- A reproducible 71-second, captioned 1080p product walkthrough assembled only
  from reviewed application screenshots, with the MP4 published as a release
  asset rather than committed to source control.
- Typed `reddit-minerals` batch CLI with validated settings and domain models.
- Transactional SQLite persistence, resumable work states, exports, legacy-data
  migration, and derived-content deletion.
- Durable deletion tombstones, complete-snapshot comment reconciliation, and
  prompt-relevant change invalidation.
- Bounded Reddit collection and schema-validated Gemini analysis stages.
- Provider request timeouts, run-wide operation deadlines, fair work selection,
  prompt/schema/model provenance, and provider-wide failure handling.
- Reproducible environment scripts, strict quality gates, tests, pre-commit hooks,
  pinned CI actions, non-root container packaging, vulnerability scanning, and
  operational documentation.
- A pinned, audited packaging backend and isolated installation smoke checks for
  both wheel and source-distribution artifacts.
- Immutable digests for the Python and `uv` container bases, with automated
  dependency-update coverage.
- Parameterized, output-free analysis notebooks.
- Public Mines Nancy internship context, maintainer attribution, and citation
  metadata for the portfolio and research-facing repository.
- Concise engineering-focused data-safety guarantees covering collection,
  secrets, untrusted inputs, exports, and deletion.
- A deterministic `reddit-minerals demo` command that exercises the real scrape,
  analysis, SQLite, and export pipeline without credentials or network access.

### Changed

- Presented the project as MineralLens while retaining the
  `reddit-minerals` Python package and CLI as its collection and analysis engine.
- Deduplicated subreddit lists case-insensitively while preserving first-seen
  spelling and order.
- Replaced direct root-script execution with an import-safe package and explicit
  CLI entry point.
- Made exports snapshot-consistent, create-if-absent by default, explicitly
  overwritable, and protected against database, sidecar, and hard-link targets.
- Made status report the database schema and retained tombstone counts, and made
  legacy migration distinguish accepted records from tombstone suppression.
- Added schema-v3 input/configuration/dependency/result revisions (including the
  provider-input character bound), transactional compare-and-save analysis
  persistence, lock-owned interrupted-run reconciliation, a cross-process tracked
  writer lock, and an export/deletion barrier through final publication.
- Made the container builder include packaged configuration before installing the
  project, matching the wheel's force-included default mapping.
- Reduced web transfer cost with gzip responses, a weak ETag for the immutable
  research snapshot, short revalidation caching, and year-long immutable caching
  for content-hashed frontend assets.
- Extended isolated wheel/source-distribution smoke tests to install the `web`
  extra and verify the packaged Kaggle sample and FastAPI factory.

### Security

- Removed credentials from source configuration paths and documented mandatory
  rotation and history-purge steps for previously exposed values.
- Added secret scanning and content-safe structured logging expectations.
- Added weekly npm dependency monitoring for the locked frontend workspace.
- Minimized model requests and isolated all untrusted Reddit-controlled prompt
  values from trusted task instructions.

### Removed

- Unsupported, output-bearing exploratory notebooks from `Data analysis/` after
  replacing them under `notebooks/`.
- Credential-bearing prototype entry-point scripts and checked-in evaluation PDF
  artifacts.

## Release process

Before creating the first versioned section, complete the release checklist in
`docs/deployment.md`, replace this placeholder with the release date, and add
comparison links when a canonical remote is known.
