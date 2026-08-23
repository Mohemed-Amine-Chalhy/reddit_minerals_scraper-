# Public sample, synthetic replay, and local imports

MineralLens uses a small, deterministic sample of the public dataset collected
with this research tool. It does not ship the 316 MB expanded release, original
Reddit text, author fields, or source identifiers. Synthetic data remains
available only for regression tests and the explicitly labelled pipeline replay.

## Provenance matrix

| Source | Purpose | Records | Network access | Persistence |
| --- | --- | ---: | --- | --- |
| [Public Kaggle release](https://www.kaggle.com/datasets/mohamedaminechalhy/reddit-mining-stance) | Owner-published research dataset, version 2 | 1,042,563 derived post/comment rows across 26 minerals | Required only to download the source ZIP | Outside the application |
| Bundled public sample | Default FastAPI repository, static Pages data, and API fallback | 104 metadata-only records: 52 posts and 52 comments across all 26 minerals | None | Versioned package asset |
| CLI offline demo | Exercise real services, transactions, state transitions, and export code | Small synthetic run | None | Temporary by default; optional isolated directory |
| Browser-local import | Inspect a compatible file explicitly selected by the viewer | At most 5,000 records | None; the file never reaches FastAPI | Browser memory until reset/reload |
| Live CLI dataset | Run bounded collection and analysis with configured adapters | Operator-selected bounds | Explicit Reddit/Gemini access | Operator-selected SQLite database and exports |

The public release contains 15,779 post rows and 1,026,784 comment rows dated
from 2008-02-19 through 2025-08-27. It includes subreddit, mineral, timestamp,
score, model-derived labels, keywords, themes, concern scores, and repository
linkage fields. It does not include original title/body text or authors. The
interface therefore presents a plain availability explanation instead of
inventing source content. It also leaves reputation blank because that signal is
not present in this release.

## Deterministic sample derivation

The committed sample lives at
`src/reddit_minerals/web/data/kaggle_sample.json`. For each mineral, the builder
selects the highest-scoring qualifying post and the most recent distinct
qualifying post, then pairs each with its highest-scoring qualifying direct
reply. Direct-parent checks prevent a nested comment from being represented as a
post reply.
Only the released `positive`, `negative`, `neutral`, `pro-mining`, `anti-mining`,
and `neutral` label domains are accepted. Timestamp and source ID provide stable
tie-breaking before identifiers are removed.

Every source object identifier is replaced by a deterministic SHA-256-derived,
mineral-scoped repository ID. The original identifiers, author fields, and
content text are never written to the sample. The document records its source
URL, dataset version, archive checksum, published/sample counts, date range,
selection method, and content-availability boundary.

Repository-local IDs are not an anonymization claim. Exact released timestamp,
subreddit, mineral, and score combinations can remain linkable to the public
source dataset.

To reproduce it, download dataset version 2 into the ignored cache directory and
run the builder:

```shell
kaggle datasets download -d mohamedaminechalhy/reddit-mining-stance -p .dataset-cache
uv run python scripts/prepare_kaggle_sample.py
```

The expected source archive is `.dataset-cache/reddit-mining-stance.zip` with
SHA-256:

```text
3A299CEC89CB091E9AD9E8F4158FD264A761C92BD9CA5B37B94924D99C3D7407
```

The builder refuses an unexpected checksum, archive member set, file size, row
count, missing generated parent, duplicate generated ID, or mineral/count
invariant. It filters out-of-taxonomy labels, non-direct replies, and invalid
concern scores rather than coercing them. Verify a committed output without
changing it:

```shell
uv run python scripts/prepare_kaggle_sample.py --check
```

The source ZIP is deliberately ignored and is not needed to run, test, build, or
deploy MineralLens. Rebuilding is a maintainer provenance operation.

Each generated record carries an explicit `topic_label` copied from the released
topic-classification field, with the builder's documented fallback only when
that field is empty. The presentation adapter never infers a topic from the
order of the released theme list.

## Interpretation boundary

Sentiment, stance, relevance, topics, themes, keywords, and concern scores in
the public release are model-derived research signals. They are not ground-truth
annotations, prevalence estimates, accuracy claims, or manuscript-wide
findings. The 104-record sample is deliberately balanced by mineral for product
coverage; it is not statistically representative of Reddit discussion.

Dataset version 2 is a later, larger public export than the counts described in
the current manuscript draft. Documentation and UI copy therefore call it a
public dataset collected with the tool, not the exact manuscript analysis set.

## Browser-only import contract

Import begins only after the viewer explicitly selects a `.json` or `.jsonl`
file. The frontend reads and validates that file with browser APIs and adapts
compatible records to its view model.

The import path guarantees that:

- file bytes and parsed records are never posted to `/api`;
- FastAPI does not receive the file name, content, or derived values;
- the import does not open or modify SQLite;
- imported data remains in browser memory and is cleared by reset or reload;
- strings render as text rather than executable markup;
- unsupported schema versions and invalid records are rejected visibly; and
- parsing is atomic, so a failure does not partially replace the active source.

JSONL is processed as one JSON object per non-empty line. JSON may contain one
record, an array, or an object with a `records` array. Records must match export
schema version 1. Imports are bounded to 10 MB and 5,000 records.

## Source labels

The active source label is a product contract:

- `Read-only public research API` means FastAPI returned the bundled sample;
- `Bundled public Kaggle research sample` means static/fallback delivery of the
  same canonical sample;
- `Synthetic pipeline export` means a local file is recognized as output from
  `reddit-minerals demo`; and
- `Local imported dataset` means the viewer selected another compatible file
  whose origin the application cannot independently establish.

The Pipeline page has no fabricated history for the public sample. Its replay is
separately and persistently labelled synthetic because it demonstrates retry,
resume, and stale-result behavior rather than historical research execution.

## Research and internship wording

The software may be described as research tooling developed during a Mines
Nancy internship. According to the project owner, an associated manuscript is
in advanced pre-publication review. Do not describe it as accepted,
peer-reviewed, published, or institutionally endorsed.

The formal supervisor evaluation may be paraphrased as praising the quality of
the results, impressive workload, completeness of the contribution, and strong
written structure. The private evaluation and manuscript files are not product
assets and must not be committed or shown in public walkthrough media.

## Regression expectations

Tests should prove that:

- default API and static modes identify public-sample provenance;
- all 26 minerals and exact 52/52 sample counts load deterministically;
- raw text/authors remain absent and unavailable content is not fabricated;
- source IDs never appear in public DTOs;
- public run history is empty while replay remains explicitly synthetic;
- API construction cannot instantiate providers or access operational storage;
- invalid local imports preserve the previous dataset; and
- screenshots and walkthroughs contain no credentials or private documents.
