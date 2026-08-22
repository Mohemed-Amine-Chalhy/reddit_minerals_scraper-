# Configuration reference

## Loading and precedence

Settings use the `RMS_` prefix. Process environment variables override values in
the repository-root `.env` file; unset values use validated defaults. CLI options
override the corresponding setting for that invocation. Unknown `.env` keys are
ignored, but misspelled required credentials surface when a command requests the
provider.

`.env` is for local development only and is ignored by Git. Production jobs
should inject secrets through the scheduler or container secret mechanism.

## Paths and logging

| Variable | Default | Validation and purpose |
| --- | --- | --- |
| `RMS_DATABASE_PATH` | `data/reddit_minerals.sqlite3` | Canonical SQLite file. Use an absolute mounted path in containers. |
| `RMS_SUBREDDIT_MAPPING_PATH` | `configs/subreddit_mapping.json` | Non-empty JSON object mapping mineral names to subreddit lists. Installed wheels fall back to their bundled copy when this is unset. |
| `RMS_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`, or `CRITICAL`. Logs remain content-safe at every level. |

Relative paths resolve from the process working directory, so scheduled and
container deployments should either set the working directory to the repository
root or use absolute paths.

## Reddit

| Variable | Default | Required for |
| --- | --- | --- |
| `RMS_REDDIT_CLIENT_ID` | none | `scrape` |
| `RMS_REDDIT_CLIENT_SECRET` | none | `scrape` |
| `RMS_REDDIT_USER_AGENT` | none | `scrape` |

Use a read-only script/application registration approved for the intended use.
The user agent should identify the application, version, and a reachable Reddit
account, for example `script:reddit-minerals-scraper:0.1.0 (by u/account)`. This project
does not need or accept a Reddit username/password login flow.

## Gemini

| Variable | Default | Required for |
| --- | --- | --- |
| `RMS_GEMINI_API_KEY` | none | `relevance`, `enrich`, `reputation` |
| `RMS_GEMINI_MODEL` | none | Explicit, evaluated model identifier required by every AI command. |

Changing a model can alter quality, safety behavior, latency, and cost. Evaluate
and record it as a methodology change before rollout.

## Work bounds

| Variable | Default | Allowed range | Purpose |
| --- | ---: | ---: | --- |
| `RMS_MAX_POSTS_PER_MINERAL` | 100 | 1–10,000 | Default post bound for one scrape invocation. |
| `RMS_MAX_COMMENTS_PER_POST` | 100 | 0–10,000 | Default stored-comment bound per post. |
| `RMS_REDDIT_REPLACE_MORE_LIMIT` | 8 | 0–100 | Maximum PRAW placeholder-expansion effort per post. |
| `RMS_REFRESH_AFTER_HOURS` | 24 | 0–8,760 | Age after which previously complete content can be refreshed. Zero permits every run to refresh. |
| `RMS_ANALYSIS_BATCH_SIZE` | 100 | 1–10,000 | Default maximum content items selected for one analysis run. |
| `RMS_MAX_CONTENT_CHARS` | 12,000 | 500–100,000 | Maximum content characters sent for one model request. |
| `RMS_MAX_CONTEXT_COMMENTS` | 10 | 0–20 | Maximum comment snippets provided as context. |
| `RMS_RELEVANCE_THRESHOLD` | 70 | 0–100 | Confidence threshold used by downstream eligibility rules. |

CLI `--max-posts`, `--max-comments`, and `--limit` should normally be lower than
or equal to deployment defaults for canaries. Raising a bound affects time,
provider quota, storage, privacy exposure, and model cost.

## Retry policy

| Variable | Default | Allowed range | Purpose |
| --- | ---: | ---: | --- |
| `RMS_MAX_RETRIES` | 3 | 1–10 | Total attempts for explicitly retryable provider operations. |
| `RMS_RETRY_BASE_DELAY_SECONDS` | 1 | 0–60 | Initial exponential-backoff delay. |
| `RMS_RETRY_MAX_DELAY_SECONDS` | 30 | 0–600 | Maximum actual backoff delay after jitter. |
| `RMS_REDDIT_REQUEST_TIMEOUT_SECONDS` | 30 | 1–300 | Timeout for each Reddit HTTP request. |
| `RMS_GEMINI_REQUEST_TIMEOUT_SECONDS` | 120 | 1–600 | Timeout for each Gemini HTTP request. |
| `RMS_OPERATION_TIMEOUT_SECONDS` | 1,800 | 1–86,400 | Wall-clock deadline for one scrape or analysis invocation. |

Authentication, authorization, configuration, schema-validation, and permanent
provider errors are not made retryable by increasing these values.
Both provider request timeouts must be less than or equal to the operation
timeout. Settings are validated as one deployment contract even for offline
commands, so lower the related request timeout at the same time if you lower the
global operation budget.

## Example local `.env`

```dotenv
RMS_DATABASE_PATH=data/reddit_minerals.sqlite3
RMS_SUBREDDIT_MAPPING_PATH=configs/subreddit_mapping.json
RMS_LOG_LEVEL=INFO

RMS_REDDIT_CLIENT_ID=replace-me
RMS_REDDIT_CLIENT_SECRET=replace-me
RMS_REDDIT_USER_AGENT=script:reddit-minerals-scraper:0.1.0 (by u/replace-me)

RMS_GEMINI_API_KEY=replace-me
RMS_GEMINI_MODEL=replace-with-an-evaluated-model-id

RMS_MAX_POSTS_PER_MINERAL=25
RMS_MAX_COMMENTS_PER_POST=50
RMS_ANALYSIS_BATCH_SIZE=25
```

Do not use real values in examples, screenshots, support messages, tests, or CI
configuration committed to the repository.

## Subreddit mapping

`configs/subreddit_mapping.json` has this shape:

```json
{
  "gold": ["gold", "GoldMining", "mining", "geology"],
  "lithium": ["lithium", "LithiumIon", "mining"]
}
```

Mineral names are trimmed, lowercased, and internal whitespace is collapsed.
Subreddit names must be 2–64 ASCII letters, digits, or underscores. Each mineral
must have at least one subreddit. Duplicate subreddit names within a mineral are
removed case-insensitively while preserving the first spelling and order. Exact
duplicate JSON object keys and mineral names that collide after normalization are
rejected rather than silently overwritten.
The mapping is bounded to 1,000,000 encoded bytes, 500 minerals, 128 characters
per mineral name, 100 configured subreddit entries per mineral, and 10,000
configured entries in total. These are parser/resource safety ceilings, not
recommended collection sizes; production mappings should be much smaller and
individually approved.

Validate after every edit:

```shell
uv run reddit-minerals validate-config
```

Validation proves the file shape, not that each subreddit exists, is accessible,
or is relevant. Review the mapping and research rationale separately.

## Secret rotation

Rotate credentials on a schedule and immediately after suspected disclosure.
Deploy the new value, verify a bounded canary, revoke the old value, and confirm
jobs no longer reference it. If a value entered Git history, follow the full
incident procedure in `SECURITY.md`; replacing `.env` is not sufficient.
