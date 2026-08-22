# Changelog

All notable changes will be documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases should use
[Semantic Versioning](https://semver.org/) once a public API contract exists.

## [Unreleased]

### Added

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

### Changed

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

### Security

- Removed credentials from source configuration paths and documented mandatory
  rotation and history-purge steps for previously exposed values.
- Added secret scanning and content-safe structured logging expectations.
- Minimized model requests and isolated all untrusted Reddit-controlled prompt
  values from trusted task instructions.

### Removed

- Unsupported, output-bearing exploratory notebooks from `Data analysis/` after
  replacing them under `notebooks/`.
- Credential-bearing prototype entry-point scripts and checked-in evaluation PDF
  artifacts.

## Release process

Before creating the first versioned section, complete the release checklist in
`docs/deployment.md`, select a license explicitly, replace this placeholder with
the release date, and add comparison links when a canonical remote is known.
