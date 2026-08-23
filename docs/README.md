# Documentation

Use this index to explore MineralLens and the Reddit Minerals Pipeline beneath
it. Generated OpenAPI, CLI help, and validated models are authoritative when
documentation and code disagree; fix the documentation in the same change.

## For users

- [Web application](web-app.md): product architecture, API contract, local
  development, production build, and verification.
- [Live Reddit collection](live-reddit.md): PRAW credentials, feature flags,
  bounded web jobs, cancellation, retention, and the low-limit canary.
- [Public sample and local imports](demo-data.md): reproducible Kaggle provenance,
  metadata boundaries, synthetic replay, browser-only JSON/JSONL handling, and
  truthful source labels.
- [Configuration](configuration.md): environment variables, credentials, limits,
  paths, and safe defaults.
- [Legacy migration](migration.md): previewing and importing the old per-mineral
  JSON-array layout.
- [Troubleshooting](troubleshooting.md): common setup, provider, database, and
  analysis failures.
- [Notebooks](../notebooks/README.md): optional export exploration.

## For reviewers and researchers

- [Product walkthrough](walkthrough.md): the reviewed 60–75 second engineering
  story and its claim boundaries.
- [Walkthrough media](media/README.md): capture, encoding, privacy, and delivery
  acceptance criteria.
- [Architecture](architecture.md): components, trust boundaries, data flow, and
  failure semantics.
- [Data model](data-model.md): canonical records, work states, analyses, exports,
  migrations, and deletion propagation.
- [Methodology](methodology.md): sampling limits, model-derived fields,
  evaluation protocol, and interpretation constraints.
- [Data-safety guarantees](data-safety.md): implemented collection, secret,
  logging, untrusted-input, export, and deletion boundaries.

## For operators and maintainers

- [Operations runbook](operations.md): preflight, canaries, scheduling,
  monitoring, backups, restoration, deletion, and incidents.
- [Deployment and rollback](deployment.md): local and container rollout,
  verification, releases, and recovery.
- [Contributing](../CONTRIBUTING.md), [security](../SECURITY.md), and
  [changelog](../CHANGELOG.md).

## Documentation maintenance

Review these documents whenever a CLI flag, environment variable, database
schema, provider, prompt/schema version, or deployment mechanism changes.
Examples must use placeholders and synthetic identifiers; never paste production
output into documentation.
