# Architecture and data flow

## Scope

The application is a scheduled batch CLI. It collects a bounded set of public
Reddit submissions and comments for configured minerals, stores canonical
records in SQLite, optionally requests structured analysis from Gemini, and
exports snapshots for research. It is not a web service, social-listening stream,
or population-representative survey system.

## Components

```text
CLI / scheduler
      |
      v
validated settings + subreddit mapping
      |
      +-------------------+
      |                   |
      v                   v
Reddit client         Gemini client
      |                   |
      v                   v
scrape pipeline       analysis pipelines
      |                   |
      +---------+---------+
                v
      transactional SQLite store
                |
        +-------+--------+
        |                |
        v                v
  status/deletion   JSON/JSONL export
                             |
                             v
                    optional notebooks
```

### CLI

`reddit-minerals` parses explicit bounds, loads settings, configures structured
logging, opens storage, invokes one pipeline operation, and maps known failures to
non-zero exit codes. Importing a package module has no provider or filesystem
side effects.

### Configuration boundary

`AppSettings` reads `RMS_*` variables and `.env`; `load_subreddit_mapping`
validates mineral names and subreddit syntax and removes duplicate subreddit
names case-insensitively. Secrets are represented as secret values and excluded
from safe summaries.

### Provider adapters

The Reddit adapter uses application-only read access and converts provider
objects into validated `PostRecord` and `CommentRecord` values. The Gemini
adapter sends bounded, delimited content and accepts only schema-valid responses.
Provider-specific exceptions are classified before they reach pipeline logic.

### Pipelines

- Scraping chooses configured mineral/subreddit pairs, applies explicit item and
  comment bounds, collects records, and commits content plus work state in a
  transaction.
- Relevance, enrichment, and reputation select eligible content, build a bounded
  input, persist a validated result and provider metadata, or persist a distinct
  failure state. Optimistic revision checks reject a result or failure if its
  source, configuration, or upstream relevance dependency changed in flight.
- Migration reads legacy files only after a dry-run inventory and converts them
  through the same models and persistence methods.
- Deletion resolves a post or comment ID, previews affected raw and derived rows,
  and removes them transactionally only when confirmed with `--yes`; durable
  tombstones prevent later recollection of the same stable IDs.

### Persistence and exports

SQLite is the only canonical operational state. Generated JSON/JSONL files are
point-in-time derivatives and cannot be used as checkpoints. Foreign keys and
transactions keep posts, comments, mineral associations, work items, analyses,
run metadata, and deletion tombstones consistent. Exports stream from one SQLite
read snapshot while holding the writer reservation through atomic publication;
this serializes export publication with content deletion. Exports include a
schema version so consumers can reject an unsupported shape.

## Processing sequence

1. Validate settings and the subreddit map without network access.
2. Resolve minerals explicitly or use the validated configured set.
3. Acquire the database's cross-process operation lock, reconcile audit rows left
   by a crashed owner, and start a run record with limits and non-secret metadata.
4. Read from a provider through a bounded adapter.
5. Validate and normalize each item before storage.
6. In one transaction, upsert content/associations and advance the work state.
7. Record retryable, permanent, blocked, or partial outcomes rather than
   manufacturing a neutral result.
8. Revalidate analysis provenance before persistence and discard stale in-flight
   results without recreating deleted or replaced derived state.
9. Close the run with counts and status even on interruption. A later tracked
   writer can safely reconcile a run left `running` by an uncatchable process
   exit because it must first acquire the released operation lock.
10. Export only after checking status and failure counts.

## State and idempotency

Work status is explicit:

- `pending`: discovered but not attempted;
- `partial`: some dependent work was stored, but the unit is incomplete;
- `complete`: all required work for that stage committed successfully;
- `retryable_failure`: a transient error may be retried;
- `permanent_failure`: retrying unchanged input is not expected to help;
- `blocked`: provider policy or safety controls did not return a usable result.

Stable Reddit IDs and analysis identity keys make reruns idempotent. A refresh
window permits metrics and comments to be revisited without duplicating content.
`--force` deliberately replaces an existing analysis for the current prompt and
schema version; it should be used only after recording why recomputation is
needed.

## Trust boundaries and threats

Reddit titles, bodies, comments, subreddit names returned by the provider, and
Gemini responses are untrusted. They may contain prompt instructions, terminal
control characters, malformed Unicode, URLs, or private information copied by a
user. The application:

- bounds content length and comment context;
- separates instructions from content sent to the model;
- validates all structured output and numeric ranges;
- does not execute, render, or interpolate collected text;
- logs IDs, statuses, timings, and counts rather than content bodies;
- does not persist author names in the canonical models;
- uses parameterized SQL and controlled export paths.

Secrets cross only the environment-to-provider-client boundary. The safe settings
summary exposes configured/not-configured booleans, never values.

## Failure semantics

Only explicitly transient failures receive exponential backoff with jitter and a
bounded attempt count. Configuration, authentication, validation, and permission
errors fail fast. An interrupted transaction rolls back; previously committed
items remain available for a resumed run. A failed analysis remains a failure and
is excluded from model-derived aggregates unless a consumer explicitly requests
failure rows. Completed reputation is current only while the exact successful
relevance result and configured threshold on which it depended remain current.
Analysis identity also includes the configured provider-input character bound,
so changing truncation behavior requeues affected work.

## Deliberate constraints

- SQLite favors a single scheduled writer. Supported tracked CLI commands enforce
  this with a per-database cross-process lock and reject an overlap; direct SQLite
  writers or path aliases remain outside the supported storage contract.
- Local export files have no revocation mechanism. Deletion therefore includes an
  operator obligation to find and regenerate or remove exports and backups.
- Subreddit selection is researcher-defined and introduces sampling bias.
- Provider output is model-dependent and requires a versioned evaluation before
  use in decisions or publication.
