# Legacy-data migration

`migrate-legacy` imports the prototype's per-mineral post and comment JSON arrays
into the canonical SQLite schema. It does not execute legacy Python scripts,
contact providers, or import old relevance, reputation, enrichment, progress, or
summary files. Re-run analyses through the typed pipeline so every result has a
validated schema and version provenance.

## Accepted layout

```text
legacy-data/
├── gold/
│   ├── posts.json
│   └── comments.json
├── lithium/
│   ├── posts.json
│   └── comments.json
└── unrelated-directory/   # ignored if neither expected file exists
```

Each expected file must contain one JSON array of objects. JSONL is not accepted
by this migration command. Mineral names come from directory names and are
lowercased with internal whitespace collapsed.

The importer refuses symbolic-link/reparse aliases in expected mineral and JSON
paths, as well as paths that escape the resolved source root. It applies hard
resource ceilings: 5,000 inspected source entries, 1,000 mineral directories,
128 characters per directory name, 100,000,000 encoded bytes per expected JSON
file, and 100,000 records per file. Split and review a larger migration instead
of raising these limits ad hoc.

## Field mapping

Posts require `id`, `subreddit`, and one creation time. The migration maps:

- `created_utc` Unix seconds, otherwise ISO `created_date` or `created_at`;
- optional `title`, `selftext`, `score`, `num_comments`, `upvote_ratio`, and
  `permalink`;
- a generated Reddit URL when `permalink` is absent;
- current migration time as `fetched_at`.

Comments require `id`, `post_id`, `subreddit`, and one creation time. Optional
fields are `parent_id`, `body`, `score`, `level` or `depth`, and `permalink`.
Negative depth and post comment-count values are clamped to zero. Comments whose
`post_id` is not present in the same mineral's valid `posts.json` are counted as
orphans and not imported.

Invalid individual records are counted and skipped. A missing optional
`posts.json` or `comments.json` is treated as an empty side. Malformed JSON,
non-array documents, or non-object array entries stop the command so the operator
can correct the source rather than silently misread it.

## Preparation

1. Revoke any credentials historically embedded in legacy scripts before
   handling the directory.
2. Make a read-only backup of legacy files and record a checksum.
3. Remove unrelated personal documents, logs, exports, and notebooks from the
   migration copy.
4. Validate that the source directory contains only the intended mineral folders
   and JSON arrays.
5. Back up the target SQLite database and stop other writers.
6. Set `RMS_DATABASE_PATH` to a staging database for the first rehearsal.

## Dry run

```shell
reddit-minerals migrate-legacy --source legacy-data --dry-run
```

Review `minerals_seen`, `posts_imported`, `comments_imported`,
`posts_suppressed_by_tombstone`, `comments_suppressed_by_tombstone`,
`invalid_posts`, `invalid_comments`, and `orphan_comments`. Here, imported means
"would be accepted" during a dry run; tombstoned stable IDs are reported as
suppressed rather than imported. No content or analysis rows are written. The
command may initialize an empty target database and records a non-secret audit run.
Investigate every invalid or orphan count against the source rather than assuming
data loss is acceptable.

The dry run validates model conversion but is not an exhaustive duplicate or
disk-capacity simulation. Test the real import in staging as well.

## Import and verification

```shell
reddit-minerals migrate-legacy --source legacy-data
reddit-minerals status --json
reddit-minerals export --format jsonl --output exports/migration-check.jsonl
```

Compare reported counts with the approved dry run, inspect foreign-key and
integrity checks, and review a small policy-approved sample. Re-importing the same
valid post/comment IDs updates canonical records and preserves unique mineral
associations rather than creating duplicate content; nevertheless, always use a
backup because corrected legacy values can replace prior snapshots.

Delete the verification export under the project's retention rules after review.

## After migration

- Keep the legacy directory isolated and read-only only for the approved rollback
  period, then delete it according to retention policy.
- Run new relevance, enrichment, and reputation commands with low limits and a
  pinned, evaluated model.
- Do not copy old analysis JSON into SQLite or mark it complete manually.
- Document counts, invalid/orphan decisions, source checksum, target database
  schema, application commit, operator, and date.

## Rollback

If the target was empty, discard only the explicitly named staging database. If
the target contained data, stop all writers and restore the verified pre-import
backup to a new path; do not attempt to distinguish imported rows using ad hoc
SQL. Preserve diagnostic counts without copying raw source content into tickets.
