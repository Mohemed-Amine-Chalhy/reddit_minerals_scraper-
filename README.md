# Reddit Minerals Pipeline

[![CI](https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/actions/workflows/ci.yml/badge.svg)](https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/actions/workflows/ci.yml)
[![CodeQL](https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/actions/workflows/codeql.yml/badge.svg)](https://github.com/Mohemed-Amine-Chalhy/reddit_minerals_scraper-/actions/workflows/codeql.yml)
![Python 3.12 and 3.13](https://img.shields.io/badge/Python-3.12%20%7C%203.13-3776AB?logo=python&logoColor=white)

A production-oriented, typed, resumable batch pipeline for collecting approved
public Reddit content about minerals and running schema-validated analysis over
it. SQLite is the canonical store; JSON or JSONL exports and optional notebooks
support downstream research.

## Project context

This project originated as part of my internship at
[Mines Nancy](https://mines-nancy.univ-lorraine.fr/en/). I evolved the initial
research prototype into a software-engineering-focused system that demonstrates:

- clean, typed Python architecture with explicit provider boundaries;
- transactional SQLite storage, migrations, deletion propagation, and
  concurrency controls;
- reproducible environments, offline tests, automated quality gates, packaging,
  and container delivery;
- practical security, privacy, observability, and operations documentation.

The repository is maintained by
[Mohamed Amine Chalhy](https://github.com/Mohemed-Amine-Chalhy). The current tree
contains source code, public configuration, documentation, and output-free
notebooks—not internship-confidential datasets or Reddit user profiles.

This software does **not** grant permission to collect or reuse Reddit data.
Before using it, obtain any required Reddit approval, comply with Reddit's terms
and policies, and document a lawful purpose, retention period, and deletion
process. Model-generated labels are estimates and must not be treated as facts
about people or as representative public-opinion measurements.

## What it provides

- Read-only Reddit application authentication; no username or password flow.
- Bounded post and comment collection with explicit mineral selection.
- SQLite transactions, refresh windows, resumable work states, and idempotent
  record updates.
- Separate relevance, enrichment, and reputation-analysis stages.
- Pydantic validation for configuration, collected records, and AI responses.
- UTC timestamps, structured content-safe logs, bounded retries, and non-zero
  exit codes on failures.
- Config validation, dry runs, status reporting, versioned exports, legacy-data
  migration, and content deletion.
- Reproducible setup, strict typing, tests, pre-commit hooks, CI, and a non-root
  container image.

## Requirements

- Python 3.12 or 3.13 (the deployment baseline and bootstrap default are 3.12)
- [`uv`](https://docs.astral.sh/uv/) for the supported local workflow
- A Reddit API application and approval appropriate to the intended use
- A Gemini API key and explicit evaluated model identifier only for analysis commands

Do not paste secrets into source files, notebooks, command history, logs, issue
reports, or committed `.env` files.

## Quick start

From the repository root, prepare the locked environment:

```powershell
.\scripts\bootstrap.ps1
```

```bash
./scripts/bootstrap.sh
```

Copy `.env.example` to `.env` and provide the credentials needed by the commands
you intend to run. Reddit collection needs:

```dotenv
RMS_REDDIT_CLIENT_ID=replace-me
RMS_REDDIT_CLIENT_SECRET=replace-me
RMS_REDDIT_USER_AGENT=script:reddit-minerals-scraper:<version> (by u/<account>)
```

AI analysis additionally needs:

```dotenv
RMS_GEMINI_API_KEY=replace-me
RMS_GEMINI_MODEL=replace-with-an-evaluated-model-id
```

Validate local configuration without contacting either provider:

```shell
uv run reddit-minerals validate-config
```

Preview a bounded scrape, then run it:

```shell
uv run reddit-minerals scrape --mineral gold --max-posts 10 --max-comments 25 --dry-run
uv run reddit-minerals scrape --mineral gold --max-posts 10 --max-comments 25
```

Run analyses and inspect progress:

```shell
uv run reddit-minerals relevance --mineral gold --limit 100
uv run reddit-minerals enrich --mineral gold --limit 100
uv run reddit-minerals reputation --mineral gold --limit 100
uv run reddit-minerals status
```

Export a research dataset:

```shell
uv run reddit-minerals export --mineral gold --format jsonl --output exports/gold.jsonl
```

Exports never replace an existing destination unless `--overwrite` is explicit,
and they always refuse the live database and its SQLite/operation sidecars. Use
`uv run reddit-minerals --version` to record the application version alongside
an exported research artifact.

Every networked command is bounded by configuration and CLI limits. Start with a
small canary, inspect its status and cost, then increase limits deliberately.

## Command overview

| Command | Purpose | Provider access |
| --- | --- | --- |
| `validate-config` | Validate settings and subreddit mapping | None |
| `scrape` | Collect or refresh posts and comments | Reddit |
| `relevance` | Classify mineral relevance | Gemini |
| `enrich` | Extract sentiment, themes, stance, and concerns | Gemini |
| `reputation` | Estimate documented content-level indicators | Gemini |
| `status` | Report record, work-state, and run counts | None |
| `export` | Write a JSON or JSONL snapshot | None |
| `delete-content` | Preview or delete a post/comment and derivatives | None |
| `migrate-legacy` | Preview or import legacy JSON-array files | None |

Use `uv run reddit-minerals COMMAND --help` as the authoritative option
reference. See [configuration](docs/configuration.md) for all environment
variables.

## Repository layout

```text
src/reddit_minerals/   Application package and CLI
tests/                 Unit and offline integration tests
configs/               Validated mineral-to-subreddit mapping
scripts/               Idempotent setup, checks, and smoke tests
notebooks/             Optional output-free exploration notebooks
docs/                  Architecture, governance, and operations guides
data/                  Local SQLite state; ignored by Git
exports/               Generated datasets; ignored by Git
```

The credential-bearing prototype scripts and output-bearing exploratory
notebooks were removed. Only the package CLI is a supported entry point; legacy
JSON data can be imported with the documented migration command.

## Documentation

- [Documentation index](docs/README.md)
- [Architecture and data flow](docs/architecture.md)
- [Configuration reference](docs/configuration.md)
- [Data model and migrations](docs/data-model.md)
- [Methodology and evaluation](docs/methodology.md)
- [Privacy, retention, deletion, and compliance](docs/privacy-compliance.md)
- [Operations runbook](docs/operations.md)
- [Deployment and rollback](docs/deployment.md)
- [Legacy migration](docs/migration.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Contributing](CONTRIBUTING.md) and [security policy](SECURITY.md)
- [Citation metadata](CITATION.cff)

## Development checks

Run the same quality gate used by CI:

```powershell
.\scripts\check.ps1
```

```bash
./scripts/check.sh
```

Tests are offline by default and must not make live Reddit or Gemini calls. The
full check also builds, installs, and exercises both the wheel and source
distribution from outside the repository.

## Licensing status

No project license has been selected yet. Until the repository owner adds an
approved `LICENSE` file, copyright law reserves redistribution and modification
rights; public visibility alone is not permission. See
[the licensing decision record](docs/licensing.md). Contributions should not add
a license without explicit owner approval.
