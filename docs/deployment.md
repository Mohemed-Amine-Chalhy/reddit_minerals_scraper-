# Deployment, release, and rollback

## Deployment contract

The supported package runtimes are Python 3.12 and 3.13. Python 3.12 is the
deployment baseline and bootstrap default; CI exercises both versions on Linux
and Windows. Repository deployments resolve application, development, and build
dependencies from `uv.lock`, and the exact build backend is pinned in project
metadata. Deploy a tagged, reviewed commit; an explicitly selected Gemini model;
one writable persistent database volume; read-only application
code/configuration; and secrets supplied by the platform. Production should not
depend on `.env`, editable installs, root-level legacy scripts, notebooks, or
live dependency resolution.

## Release gate

Before tagging or deploying:

- all historical provider credentials are revoked and purged from reachable Git
  history and release artifacts;
- Reddit approval and the compliance record cover the exact deployment;
- the owner has resolved licensing before distribution;
- `uv lock --check` and both platform check scripts pass;
- formatting, lint, strict typing, tests/coverage, secret scan, dependency audit,
  notebook hygiene, package build, isolated wheel/source-distribution install
  tests, smoke test, container build/runtime checks, and the actionable
  high/critical container vulnerability scan are green;
- the [analysis evaluation record](evaluation-template.md) meets its recorded
  quality, safety, latency, and cost gates for the pinned model/prompt/schema;
- forward database migration and restore-based rollback have been rehearsed on a
  production-shaped copy;
- a low-limit provider canary passes in staging;
- retention, deletion, backup, alerts, incident contacts, and cost ceilings are
  configured and tested;
- package/image digests, database schema version, mapping checksum, and release
  approvers are recorded.

The Dockerfile pins its Python and `uv` bases by both readable tag and immutable
registry digest. Review automated digest updates like dependency changes; never
remove the digest while preparing a release.

## Local or virtual-machine deployment

Install `uv`, check out the release tag, and create the locked environment:

```shell
uv python install 3.12
uv sync --locked --no-default-groups --group build --no-install-project
uv sync --locked --no-default-groups --group build --no-editable --no-build-isolation
uv run --locked --no-sync reddit-minerals validate-config
uv run --locked --no-sync reddit-minerals status --json
```

Inject `RMS_*` values through the service manager, use absolute database and
mapping paths, set the working directory explicitly, and run each bounded CLI
stage as a non-administrative service account. Grant that identity read access to
configuration and write access only to the data/export/log destinations it needs.

The bootstrap script is intended for development because it installs hooks,
creates `.env` when missing, and runs developer checks; it is not a production
provisioning mechanism.

## Container deployment

Build from the reviewed commit and record the digest:

```shell
docker build --pull -t reddit-minerals:0.1.0 .
docker image inspect reddit-minerals:0.1.0
```

Run with a read-only root filesystem, a writable mounted data directory, a
read-only mapping, no published ports, and platform-injected secrets. Adapt paths
to the image's documented working directory:

```shell
docker run --rm --read-only \
  --mount type=bind,src=/srv/reddit-minerals/data,dst=/data \
  --mount type=bind,src=/srv/reddit-minerals/configs,dst=/app/configs,readonly \
  --env-file /srv/reddit-minerals/runtime.env \
  reddit-minerals:0.1.0 validate-config
```

Protect the external environment file so only the scheduler identity can read it,
or use individual secret injection instead. Do not bake `.env`, data, exports,
Git history, test caches, or credentials into the image. Run one writer container
per SQLite database.

## Staged rollout

1. Deploy the new package/image in staging with a copied or synthetic database.
2. Run offline status, migration, deletion, and export tests.
3. Run a one-mineral, low-limit Reddit/Gemini canary using staging credentials.
4. Back up production and stop its scheduler.
5. Apply the release to production; allow any schema migration to finish once.
6. Run `validate-config`, `status --json`, and integrity checks.
7. Run a production canary and compare errors, latency, token/cost, and output
   evaluation against staging.
8. Resume bounded schedules gradually and monitor through the rollback window.

Never perform a rolling deployment with old and new versions concurrently writing
the same SQLite file.

## Rollback

### Code-only release

If no schema or irreversible output change occurred, stop jobs, redeploy the
previous immutable package/image digest, validate configuration and status, then
run a low-limit canary. Do not roll back the configured model implicitly; record
model rollback separately.

### Database-schema change

Stop writers. Restore the verified pre-migration backup to a new path, point the
previous application version at that path, run integrity/status checks, then
promote the restored path. Preserve the failed migrated database under the
incident retention policy. Work completed after the backup may need a controlled
recollection; never merge SQLite files manually.

### Prompt/model regression

Stop only the affected analysis stages. Preserve raw canonical content, deploy
the last approved model/prompt/schema, evaluate on the versioned set, then use a
documented bounded `--force` reprocessing run if invalid results must be replaced.
Record the affected analysis identities and downstream exports requiring
regeneration or withdrawal.

### Security or compliance rollback

Stop all networked work immediately, revoke relevant credentials, quarantine
exports, and follow `SECURITY.md` and `privacy-compliance.md`. A normal code
rollback is insufficient when approval or data handling is the issue.

## Release record template

```text
Version/tag and commit:
Package/image digest:
Python and lockfile checksum:
Database schema version and pre-migration backup:
Mapping checksum:
Gemini model, prompt, schema, threshold, evaluation report:
Reddit approval/compliance review reference:
Canary bounds and result:
Alerts, cost ceiling, retention, and deletion test:
Security/dependency scan result:
Rollback version/digest and restore location:
Approvers and timestamp:
```
