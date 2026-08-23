# MineralLens product walkthrough

The public walkthrough should communicate the product, provenance, and
engineering depth in 60–75 seconds. It uses the bundled public Kaggle sample for
data scenes and the separately labelled synthetic replay for failure behavior.
It must not expose credentials, local paths, private documents, original Reddit
text, or manuscript text.

## Reviewed storyboard

| Time | Visual | Caption or narration |
| --- | --- | --- |
| 0–5 s | MineralLens title over the Command Center | “MineralLens turns a research collection pipeline into an inspectable full-stack product.” |
| 5–13 s | Hold on the public-source badge and counts | “The interface uses a deterministic 104-record metadata sample from a public Kaggle release collected with the tool: 52 posts, 52 comments, and all 26 mineral topics.” |
| 13–24 s | Scan totals, sentiment/stance distributions, concerns, and mineral cards | “Every chart comes from released metadata and model-derived labels. Original Reddit text and authors are absent, so the product never invents them.” |
| 24–37 s | Open Research Explorer, filter by mineral and sentiment, then open a record | “URL-backed filters and strict runtime contracts keep the view reproducible, while source notes, content availability, and analysis provenance stay attached to each record.” |
| 37–49 s | Open Pipeline and run one retry or stale-result scenario | “The public sample has no fabricated run history. This replay is explicitly synthetic and demonstrates bounded retries, resumability, and stale-result rejection.” |
| 49–62 s | Open Engineering; pause on architecture and quality evidence | “Under the interface are typed FastAPI DTOs, transactional SQLite, provider isolation, strict Python and TypeScript, deterministic tests, locked builds, CI, and container delivery.” |
| 62–72 s | Return to a clean overview/repository card | “The tooling began during a research internship at Mines Nancy. According to the project owner, the associated manuscript is in advanced pre-publication review.” |

If local import or a second replay introduces timing risk, omit it. A shorter,
stable tour is stronger than an unreliable feature list.

## Required truth markers

Keep `Public research sample` and the active source label (`Read-only public
research API` or `Bundled public Kaggle research sample`) visible whenever
default records or charts are interpreted. A detail view must explain that raw
content is unavailable. Keep `Synthetic replay` visible through every pipeline
scenario.

Do not call the sample live data, production traffic, a representative Reddit
survey, ground truth, or manuscript results. Do not describe the manuscript as
accepted, peer-reviewed, or published, and do not imply endorsement by Mines
Nancy, a supervisor, an author, or a venue.

An optional engineering-context line may paraphrase the formal supervisor
evaluation as praising the quality of the results, impressive workload,
completeness of the contribution, and strong written structure. Do not show the
evaluation, its score, signatures, private contact details, or scan.

## Capture preparation

1. Build the exact commit linked by the walkthrough.
2. Start the production-style combined app and verify `/api/v1/health` reports
   `public_sample: true`, `synthetic: false`, and read-only mode.
3. Confirm the dashboard reports 104 sample records across 26 minerals and the
   Runs section does not invent public history.
4. Use a fresh browser context at 1920×1080, a fixed theme, reduced motion, and
   no unrelated tabs, notifications, extensions, or personal state.
5. Rehearse each scene using stable routes and readiness conditions.
6. Capture short scenes so one correction does not require a full retake.

## Presentation guidance

- Use purposeful pointer movement and one action at a time.
- Leave counts and provenance text on screen long enough to read.
- Prefer concise chapter captions over dense narration.
- Keep captions inside title-safe margins and outside navigation controls.
- Do not claim deployment scale, adoption, model accuracy, novelty, or research
  impact that the repository does not verify.
- End on the canonical repository URL and the public-source label.

## Acceptance checklist

- Runtime is 60–75 seconds at 1920×1080 and 30 fps.
- Text remains legible at normal playback and small embedded sizes.
- Public-data provenance precedes interpretation of any chart.
- Missing source content and model-derived label status are explicit.
- Synthetic replay is never confused with historical execution.
- No credential, private document, local path, raw content, or author appears.
- Internship and manuscript wording matches the boundaries above.
- Captions match narration and the tagged commit.
- The MP4 uses streaming-friendly encoding and has an external caption file.

Capture, encoding, and publishing details live in
[media/README.md](media/README.md).
