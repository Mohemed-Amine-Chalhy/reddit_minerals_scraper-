# Analysis evaluation record template

Complete this record outside the public repository for every model, prompt,
schema, truncation, context, or threshold change. Use only approved synthetic or
access-controlled evaluation data; record a dataset checksum rather than raw
Reddit text here.

## Identity and approval

```text
Evaluation ID:
Application version/commit:
Evaluation date and reviewers:
Dataset version, checksum, owner, and access classification:
Gemini model identifier:
Prompt version:
Analysis schema version:
Relevance threshold:
Max content/context bounds:
Provider terms/privacy review reference:
```

## Dataset coverage

Record counts and selection logic for relevant and irrelevant mineral content,
short/long inputs, posts/comments, multiple minerals/subreddits, deleted or empty
content, multilingual text in scope, ambiguous market references, community and
labor concerns, and adversarial prompt-injection/safety cases. Document known
sampling gaps and prohibit population-level claims the sample cannot support.

## Results and gates

Set project-approved targets before running the evaluation.

| Gate | Target | Result | Pass/fail | Evidence |
| --- | ---: | ---: | --- | --- |
| Schema-valid responses | 100% |  |  |  |
| Unsupported/empty response handling | 100% explicit failure |  |  |  |
| Safety-block handling | 100% explicit `blocked` state |  |  |  |
| Relevance precision |  |  |  |  |
| Relevance recall |  |  |  |  |
| Enrichment agreement by field |  |  |  |  |
| Reputation calibration/error |  |  |  |  |
| Prompt-injection resistance | 100% |  |  |  |
| Sensitive unsupported inference rate | 0% |  |  |  |
| p50/p95 request latency |  |  |  |  |
| Mean/p95 input and output tokens |  |  |  |  |
| Estimated canary and scheduled cost | approved ceiling |  |  |  |

Describe annotation guidance, reviewer agreement, confidence intervals where
appropriate, failure categories, regressions against the last approved baseline,
and any manual review performed. Model-derived credibility or reputation fields
must be evaluated as text-perception signals, never as factual truth about a
person or organization.

## Operational canary

```text
Environment and credentials class:
Mineral/subreddit scope:
Post, comment, batch, retry, request-timeout, and operation-timeout bounds:
Start/end time:
Selected/completed/retryable/permanent/blocked counts:
Observed latency, tokens, and cost:
Database/export verification:
Provider or policy anomalies:
```

## Decision

Record `approved`, `approved with controls`, or `rejected`; the accountable
approver; expiry/review date; required monitoring; known limitations; rollback
model/prompt/schema; affected exports; and the location and retention rule for
the access-controlled evidence. A rejected or incomplete record blocks live
analysis deployment.
