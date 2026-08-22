# Data model, exports, and migrations

## Principles

- SQLite is canonical; exports and notebooks are derived.
- Provider data is normalized into strict models before persistence.
- All timestamps are timezone-aware UTC ISO 8601 values.
- Stable provider IDs, not titles or text hashes, identify posts and comments.
- A single post or comment can be associated with more than one mineral without
  duplicating its content.
- Raw content, work state, analysis results, and run metadata remain distinct.
- Schema, prompt, and model versions are recorded so results are reproducible and
  invalidated deliberately.

## Domain records

### Post

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Stable Reddit submission ID. |
| `title` | string | Empty when unavailable; bounded before analysis. |
| `selftext` | string | Empty for link/deleted posts; never logged. |
| `subreddit` | string | Provider-returned community name. |
| `created_at` | UTC datetime | Source creation time. |
| `score` | integer | Snapshot metric; may change on refresh. |
| `num_comments` | non-negative integer | Snapshot metric, not stored-comment count. |
| `upvote_ratio` | number 0–1 or null | Snapshot when available. |
| `permalink` | string | Reddit-relative or canonical link. |
| `fetched_at` | UTC datetime | Most recent successful fetch time. |

### Comment

| Field | Type | Notes |
| --- | --- | --- |
| `id` | string | Stable Reddit comment ID. |
| `post_id` | string | Parent submission ID. |
| `parent_id` | string or null | Reddit parent identifier when available. |
| `body` | string | Empty for unavailable/deleted content; never logged. |
| `score` | integer | Snapshot metric. |
| `created_at` | UTC datetime | Source creation time. |
| `depth` | non-negative integer | Comment-tree depth. |
| `subreddit` | string | Denormalized for selection/export. |
| `permalink` | string | Reddit-relative or canonical link. |
| `fetched_at` | UTC datetime | Most recent successful fetch time. |

Author usernames are intentionally absent because the pipeline does not need
them for content-level analysis.

### Mineral association

Associations connect a content ID and content kind (`post` or `comment`) to a
normalized mineral. They permit one canonical content record to satisfy multiple
configured searches and make deletion independent of the search path that first
found the content.

## Work and run records

A work item identifies a stage and content/search unit and uses one of:
`pending`, `partial`, `complete`, `retryable_failure`, `permanent_failure`, or
`blocked`. It records attempt count, timestamps, and a bounded error category or
message. It must not store credentials or full content in an error field.

A run records command/stage, start and finish timestamps, completion status,
non-secret bounds, and aggregate counts. Run records support incident review and
capacity planning; they are not a substitute for metrics or provider billing
data. Provider credential validation and client construction occur after the run
starts, so initialization failures are auditable without persisting a credential.
For scrape, parameters record the normalized effective mineral set and whether it
came from all configured minerals.

## Analysis records

Each analysis identity includes content kind, content ID, mineral, and analysis
kind. Its current row records the prompt/schema version and model identifier;
deliberate `--force` reprocessing replaces that current result while incrementing
attempt metadata. The result is stored only after Pydantic validation. Operational
metadata can include request ID, token counts, and latency, but never the API key
or unbounded prompt/response text.

The row also stores SHA-256 revisions for source material, analysis configuration
(including the provider-input character bound), the completed result, and (for
reputation) the exact relevance result on which eligibility depended. Candidate
selection captures those revisions.
Persistence rechecks them inside the write transaction, so a refresh, deletion,
association change, threshold change, prompt/model change, or replaced relevance
result cannot attach an in-flight result to stale input. Discarded candidates are
counted explicitly and remain eligible for a later run; a batch that persists
nothing exits non-zero.

### Relevance

- `relevant`: boolean classification;
- `confidence`: 0–100 model confidence;
- `rationale`: bounded explanation;
- `matched_topics`: up to ten bounded topic labels.

### Enrichment

- sentiment: `positive`, `negative`, `neutral`, or `mixed`;
- keywords and themes: bounded label lists;
- 20 concern indicators, each 0–1;
- mining stance: `pro-mining`, `anti-mining`, `neutral`, or `mixed`;
- topic classification;
- relevance score from 0–1.

### Reputation

- overall, sentiment, credibility, and market-impact scores from 0–100;
- controlled sentiment, credibility, market-impact, and controversy labels;
- bounded rationale and evidence signals.

These are model estimates about the supplied text, not verified claims about an
author, organization, mineral, market, or community. See `methodology.md`.

## Physical storage

Schema version 3 uses `posts`, `post_minerals`, `comments`, `comment_minerals`,
`work_items`, `analyses`, `runs`, and `content_tombstones`. Version 2 added durable
post/comment suppression records, including cascade provenance for comments
deleted with a post. Version 3 adds analysis input/configuration/dependency/result
revisions and intentionally treats older unversioned analysis rows as stale work.
Foreign-key enforcement is enabled for every connection; write connections use
WAL journaling and full synchronous durability. Initial schema creation and every
forward migration hold a writer reservation and commit atomically.
Writes that advance a work state occur in the same transaction as their content
or analysis write. Deleting a post cascades to its comments, mineral associations,
work items, and derived analyses while retaining tombstones for the post and its
known comments. Deleting a comment affects only that comment and its derivatives.

`status` reports the active SQLite `schema_version`, tombstone counts by content
kind, and run counts by status in addition to canonical, work, analysis, and
recent-run counts. Every tracked writer holds a cross-process operation lock for
its full run. After acquiring that lock, it marks any pre-existing `running` rows
as failed/interrupted before beginning new work. A concurrent `status` remains a
read-only observation and never changes an active run. Operators should alert on
an unexpected schema version, failed/interrupted runs, and tombstone retention so
deletion protections are not discarded while collection remains active.

The application controls schema creation and migration. Operators must not edit
tables, `PRAGMA user_version`, or analysis JSON manually.

## Export contract

`reddit-minerals export` streams one consistent SQLite read snapshot to UTF-8
JSONL or JSON atomically and creates parent directories. It holds SQLite's writer
reservation until the completed temporary file is published, so deletion cannot
finish between snapshotting and publication. The output path must not be the live
database, a SQLite or operation-lock sidecar, or a hard-link alias; an existing
file is replaced only when `--overwrite` is explicit. Every record contains
`export_schema_version`, `record_type`, `mineral`, a `content` object, and an
`analyses` object keyed by analysis kind. Analysis entries contain status,
prompt/schema/model provenance, safe failure metadata, tokens/latency where
available, update time, and either a validated `result` or null.

JSONL uses one self-contained record per line. JSON uses
`{"export_schema_version": 1, "records": [...]}`. The command summary reports the
output path, format, and record count, and run metadata records non-secret export
parameters and success/failure. The export does not currently embed its
creation timestamp or selection parameters. Record those alongside research
artifacts when reproducibility requires them. Consumers must ignore unknown
additive fields but reject a new unsupported major schema version. Exports may
contain Reddit text and remain sensitive even though the source was public; Git
ignores them by default.

## Schema migrations

1. Back up the database and verify the backup can be opened.
2. Stop all writers.
3. Run the new version against a copy or staging database first.
4. Let the application apply ordered, transactional forward migrations.
5. Verify schema version, foreign-key integrity, row counts, and a status query.
6. Retain the pre-migration backup for the rollback window, then remove it
   safely.

Each migration change needs an upgrade test from the previous supported schema.
SQLite migrations that cannot be reversed in place use restore-based rollback.
Never downgrade application code onto a database with a newer schema unless that
downgrade path is explicitly tested.

Upgrading from schema 2 to schema 3 preserves analysis payloads for audit/export
but leaves their revision fields empty. They are not considered current and are
reselected under the active model, prompt, schema, thresholds, and bounded input.

## Deletion propagation

Preview first:

```shell
reddit-minerals delete-content --post-id abc123 --dry-run
reddit-minerals delete-content --comment-id def456 --dry-run
```

Run the same command with `--yes` and without `--dry-run` after confirming the
target and affected counts. A durable local tombstone prevents a later scrape or
legacy migration from re-collecting that Reddit ID; retain tombstones for at
least as long as any collection is active. Then locate and delete or regenerate
every export,
notebook-derived file, cache, log attachment, and backup copy within the
known snapshot set. The database transaction cannot revoke files already copied
elsewhere.
