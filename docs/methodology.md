# Methodology and evaluation

## Intended use

The pipeline supports exploratory research into discourse about minerals within
a documented set of public subreddits and collection dates. It can organize and
label text for human review. It does not measure general public opinion, determine
truth, diagnose intent, establish individual credibility, predict prices, or
justify decisions about a person or community.

## Sampling process

For each mineral, the configuration lists subreddits chosen by the researcher.
The scraper issues bounded searches and collects at most the configured number of
posts and comments. Availability, Reddit ranking/search behavior, moderation,
deletions, language, time of collection, refresh windows, and inaccessible
comments all affect the sample.

Every published analysis should report:

- exact code version and run date range;
- mapping version and rationale for every included subreddit;
- query/mineral labels and sorting/time-filter behavior;
- post, comment, placeholder-expansion, and refresh bounds;
- discovered, stored, partial, failed, blocked, deleted, and analyzed counts;
- missingness and exclusions;
- model, prompt, schema, threshold, and evaluation versions;
- known API or policy constraints.

Counts from selected subreddits must not be extrapolated to Reddit or a national
population without an independently valid sampling design.

## Analysis stages

### Relevance

The model returns a boolean, 0–100 confidence, short rationale, and matched topic
labels. Downstream eligibility uses one documented threshold
(`RMS_RELEVANCE_THRESHOLD`) consistently. Confidence is model output, not a
calibrated probability unless a calibration study demonstrates that property.

### Enrichment

The model returns controlled sentiment and mining-stance labels, keywords,
themes, a topic classification, a 0–1 relevance score, and concern indicators.
Concern scores mean evidence in the supplied text according to the current
prompt; they do not prove that an event occurred.

### Reputation

The model returns content-level signals on a 0–100 scale and controlled labels.
The name is retained for compatibility, but results are not objective reputation
or author credibility. Reports should use wording such as "model-estimated
content indicators" and show rationales/evidence for human review. Do not rank
users, infer protected traits, or automate consequential decisions from these
fields.

## Prompt and response controls

- Treat collected text as quoted, untrusted data, never as instructions.
- Truncate deterministically to configured character/context limits.
- Request a provider-supported structured schema.
- Validate enums, list sizes, required strings, and numeric ranges before storage.
- Record model and prompt/schema versions with each result.
- Preserve `blocked`, `invalid_response`, `rate_limited`, and provider failures as
  operational outcomes; do not replace them with neutral-looking analyses.
- Retain only bounded rationales and evidence fields required by the research
  design, not unrestricted chain-of-thought or raw responses.

## Evaluation dataset

Before relying on an analysis stage, create a versioned, human-labeled evaluation
set with synthetic or policy-approved sanitized content. It should be stratified
across minerals, post/comment types, languages in scope, short/long text,
ambiguous examples, deleted/unavailable markers, each controlled label, and
prompt-injection attempts.

At least two reviewers should independently label a meaningful subset using a
written rubric. Resolve disagreements separately and report inter-rater agreement
instead of hiding ambiguity. Keep the evaluation set outside public Git if its
content cannot be redistributed.

## Metrics

Record at minimum:

- relevance precision, recall, F1, confusion matrix, and confidence calibration;
- macro and per-class precision/recall/F1 for controlled labels;
- mean absolute error and rank correlation only where numeric human labels have a
  defensible scale;
- JSON/schema-valid response rate;
- provider blocked/error/rate-limit rates;
- prompt-injection resistance on adversarial cases;
- p50/p95 latency, input/output tokens, and cost per successful item;
- subgroup/slice results by mineral, subreddit category, content kind, language,
  and length where legally and statistically appropriate.

Do not publish only an overall average. Small slices should be marked unstable,
not over-interpreted.

## Release gate

This repository does not claim an accuracy level until an evaluation report is
checked in or linked. Before a production rollout, the research owner must define
minimum metrics and maximum regression tolerances for the actual use case. A
model, prompt, schema, truncation, context, or threshold change triggers the same
evaluation. Fail the rollout if schema validity, safety behavior, required quality,
or cost bounds regress beyond the recorded tolerance.

An evaluation report should include:

```text
Evaluation version:
Code commit:
Provider/model:
Prompt and schema versions:
Dataset source, consent/approval, size, and slices:
Reviewer rubric and agreement:
Metrics with confidence intervals:
Blocked/error rates:
Latency/token/cost summary:
Known failures and examples (sanitized):
Approved use and prohibited interpretations:
Reviewer and approval date:
```

## Human review and publication

Inspect sampled inputs and outputs before publication, subject to the approved
data-handling environment. Report uncertainty, missingness, model and sampling
bias, and changes over time. Aggregate counts should suppress or combine very
small groups when they could expose individuals. Never quote or republish Reddit
content merely because the scraper can retrieve it; verify permissions and
quotation requirements independently.
