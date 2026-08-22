# Privacy, retention, deletion, and compliance

This document is an operational checklist, not legal advice. The deploying
organization is responsible for obtaining legal and policy review for its
jurisdiction, research design, and provider agreements.

## Mandatory go/no-go gate

Do not run `scrape` until the accountable owner has recorded all of the following:

- the exact research purpose and whether it is commercial, academic, or another
  category;
- explicit Reddit approval for that purpose, scope, account, communities, access
  mechanism, rate, retention, and downstream processing;
- whether sending content to Gemini for inference is within that approved use;
- the applicable Reddit, Google, institutional, contractual, privacy, copyright,
  research-ethics, and cross-border-transfer requirements;
- a lawful basis where personal-data law applies;
- allowed fields, users with access, storage location, encryption controls,
  retention periods, deletion SLA, and incident contact;
- approval or exemption from an ethics/IRB process when applicable.

Reddit's current [Responsible Builder Policy](https://support.reddithelp.com/hc/en-us/articles/42728983564564-Responsible-Builder-Policy)
states that explicit approval is required before API access and prohibits privacy
violations and unapproved uses. The [Data API Terms](https://redditinc.com/policies/data-api-terms)
limit use and retention to the approved use case, and Reddit's
[Developer Terms](https://redditinc.com/policies/developer-terms) also apply.
Terms change; review the official pages immediately before approval and each
production release rather than relying on this summary.

No approval record means no production collection.

## Data inventory and flow

| Data | Source | Canonical location | Recipients | Key risk |
| --- | --- | --- | --- | --- |
| Post title/body and public metadata | Reddit API | SQLite `posts` | Approved operators; Gemini when analysis is approved | Text can contain personal or sensitive information. |
| Comment body and public metadata | Reddit API | SQLite `comments` | Approved operators; Gemini when analysis is approved | Context and quotes may identify people even without usernames. |
| Mineral/subreddit association | Research configuration | SQLite association rows | Approved operators | Community selection can enable sensitive profiling. |
| Analysis labels/rationales | Gemini | SQLite analyses | Approved operators and approved export consumers | Inferences can be wrong, biased, or sensitive. |
| Run/work metadata | Application | SQLite and structured logs | Operators | IDs and failure details can still be linkable. |
| JSON/JSONL/CSV derivatives | Operator export/notebook | Operator-selected path | Explicitly approved consumers only | Copies are easy to lose track of and do not auto-delete. |

The canonical models intentionally omit author usernames, profile history,
private messages, IP/device data, and inferred demographic attributes. Do not
join Reddit IDs or text with off-platform identifiers, attempt re-identification,
or infer sensitive characteristics about Redditors.

## Gemini transfer

Analysis sends bounded Reddit content to Google. Before enabling it:

- confirm Reddit approval permits this processor/recipient and purpose;
- select a Gemini service tier and region compatible with the deployment's legal
  and contractual requirements;
- review the current [Gemini API terms](https://ai.google.dev/gemini-api/terms)
  and [data logging/sharing behavior](https://ai.google.dev/gemini-api/docs/logs-policy);
- disable optional sharing/feedback datasets unless separately approved;
- configure the shortest appropriate provider-log retention and do not submit
  sensitive, confidential, or prohibited material;
- record processor terms, transfer mechanism, region, subprocessors, deletion
  behavior, and incident contact in the deployment's data-processing record.

Google's configuration and terms differ by billing status and region. Do not
assume that a local database deletion deletes provider logs or contributed
datasets; follow the provider-side procedure as well.

## Data minimization

- Configure only communities necessary for the approved question.
- Use a bounded date/sample design and the smallest workable post/comment limits.
- Store no author names and avoid collecting deleted/removed text.
- Truncate model inputs and provide only the minimum comment context.
- Keep rationales/evidence bounded; do not persist unrestricted provider
  responses or hidden reasoning.
- Use IDs and aggregate counts in logs, never content bodies.
- Export only fields needed by a named recipient and approved purpose.
- Do not commit databases, exports, logs, notebook outputs, screenshots, or
  unrelated personal documents.

## Retention schedule

The accountable owner must replace every `TBD` below in the deployment record
before production. Periods must be no longer than the approved use requires.

| Category | Active retention | Backup retention | Deletion trigger | Owner |
| --- | --- | --- | --- | --- |
| Raw posts/comments | TBD | TBD | Age limit, source deletion, request, approval end | TBD |
| Derived analyses | TBD | TBD | Raw-source deletion, model invalidation, age limit | TBD |
| Operational work/run rows | TBD | TBD | Audit/operations limit | TBD |
| Structured logs | TBD | TBD/none | Logging limit or incident closure | TBD |
| Exports/notebook derivatives | TBD | TBD/none | Project milestone or upstream deletion | Named recipient |
| Provider-side prompts/responses | Provider setting | Provider policy | Provider deletion/expiry process | Cloud owner |

Automate periodic deletion where the deployment platform allows it and monitor
the job. A backup is not exempt from retention: use short-lived encrypted backups
with an expiry mechanism and document how an urgent deletion is handled before
normal expiry.

## Content deletion workflow

1. Authenticate and authorize the request or receive a reliable source-deletion
   signal; record only the minimum evidence required.
2. Locate the canonical post or comment ID. Do not paste its content into a ticket.
3. Preview affected canonical and derived rows:

   ```shell
   reddit-minerals delete-content --post-id abc123 --dry-run
   ```

4. Have a second operator review a high-impact deletion when policy requires it.
5. Run the same command with `--yes` and without `--dry-run`; capture counts/run
   ID, not text.
6. Remove or regenerate all exports, reports, caches, evaluation samples, and
   notebook-derived files containing the content.
7. Delete provider logs/datasets where available and required.
8. Handle backups according to the approved immediate-delete or expiry process
   and prevent restoration from reintroducing deleted rows.
9. Verify the ID no longer appears in status/export queries and record completion
   within the deletion SLA.

Deleting a post cascades to its comments and all derived analyses in SQLite. A
comment deletion removes that comment and its derivatives. Local files copied by
people or other systems remain an external responsibility.

## Access and security controls

- Grant read/write database and secret access only to the scheduled-job identity
  and named operators.
- Use encrypted disks/volumes and encrypted, access-controlled backups.
- Keep the database outside web roots and shared synchronization folders.
- Separate development, staging, and production credentials and databases.
- Do not expose SQLite over a network filesystem or run uncoordinated writers.
- Review access periodically and immediately after staff or purpose changes.
- Rotate keys and audit provider usage after any suspected exposure.

## Research and reporting rules

- Report sampling and missingness; do not claim representativeness.
- Treat sentiment, credibility, reputation, stance, and concern fields as model
  estimates requiring validation and human review.
- Do not publish user-level profiles, sensitive inferences, or small-cell
  aggregates that can facilitate identification.
- Recheck whether quoting text is necessary and permitted; prefer aggregate or
  paraphrased reporting where appropriate.
- Do not use the dataset to train a model unless explicit rightsholder, Reddit,
  legal, and ethics approvals cover that exact activity.

## Policy-change and termination response

Review provider terms at least before each release and on provider notice. If
approval expires, terms become incompatible, or Reddit/Google terminates access:

1. stop scheduled collection and analysis;
2. preserve no additional content merely for convenience;
3. determine required deletion/return of existing data and derivatives;
4. execute and verify it across storage, exports, backups, and provider systems;
5. revoke credentials and record closure.

## Deployment approval record

Keep the completed record in the organization's controlled compliance system,
not necessarily this public repository:

```text
Purpose and prohibited uses:
Accountable owner:
Reddit approval reference, scope, and expiry:
Gemini processing explicitly covered: yes/no
Applicable agreements and review dates:
Legal basis / ethics review:
Data fields and subreddit scope:
Storage region and access group:
Retention and deletion SLA:
Gemini tier, region, logging, and sharing settings:
Incident and rights-request contacts:
Approval signatures and date:
Next review date:
```
