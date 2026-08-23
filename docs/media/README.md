# Walkthrough media

This directory documents how to produce and review public walkthrough media for
the local web application. Generated video is a delivery artifact, not a source
of product truth; the application, tests, and [storyboard](../walkthrough.md)
remain authoritative.

## Expected deliverables

| Artifact | Purpose |
| --- | --- |
| `walkthrough-master.webm` | Loss-minimized browser capture master |
| `walkthrough-1080p.mp4` | Streamable public delivery file |
| `walkthrough.en.srt` | English captions matching narration |
| `walkthrough-thumbnail.png` | 16:9 README and release poster image |

Do not commit a large capture casually. Prefer a GitHub release asset for the
MP4 and a small, optimized thumbnail in the repository. A README can link the
thumbnail to the release-hosted video. Avoid a large animated GIF: it is less
legible, less accessible, and usually much larger than an equivalent video.

## Local application setup

Prepare the Python environment and start the API:

```shell
uv sync --extra web
uv run uvicorn reddit_minerals.web.app:create_app --factory --reload --host 127.0.0.1 --port 8000
```

Start the frontend in a second terminal:

```shell
cd web
pnpm install --frozen-lockfile
pnpm dev
```

For a production-style local capture, build `web/dist` with `pnpm build` and let
FastAPI serve the SPA from port 8000.

The repository also provides `scripts/bootstrap-web.ps1` / `.sh` and
`scripts/dev-web.ps1` / `.sh` for repeatable setup and development startup.

## Capture tooling status

The repository includes a deterministic still-based walkthrough renderer. It
uses only six allowlisted screenshots in this directory; it never launches a
browser, reads a browser profile, or records the desktop. Pillow composes the
reviewed product frames and burned-in chapter captions, while the locked
`imageio-ffmpeg` binary encodes H.264. These tools live in the optional `media`
dependency group and are not application runtime dependencies.

Prepare and validate the toolchain:

```shell
uv sync --locked --group media
uv run --locked --group media python scripts/render_walkthrough.py --check-only
```

Visually review `minerallens-overview.png`, `minerallens-signals.png`,
`minerallens-explorer.png`, `minerallens-pipeline.png`,
`minerallens-engineering.png`, and `minerallens-quality.png` at full size before
rendering. Every default-data image must show the public research sample
accurately; the pipeline image must remain clearly labelled as a synthetic
replay. Confirm that no local path, browser chrome, notification, credential,
source text, author, or private document is visible. Then render:

```shell
uv run --locked --group media python scripts/render_walkthrough.py --confirm-reviewed
```

Pass `--force` only after reviewing existing deliverables when intentionally
replacing them. A known full FFmpeg build may be supplied with `--ffmpeg`; when
omitted, the script uses the locked cross-platform binary. The renderer emits a
71-second silent MP4, external English SRT, and 1280×720 thumbnail. It verifies
H.264, 1920×1080, 30 fps, duration, absence of audio, and MP4 faststart before
publishing any artifact, then prints SHA-256 hashes for every source and output.
No browser-capture master is synthesized by this workflow; retain a real WebM
master separately if future walkthroughs use motion capture.

## Encoding target

Use a full FFmpeg installation for final assembly and verification. Normalize
all scenes before concatenation and export:

- canvas: 1920×1080;
- frame rate: 30 fps;
- video: H.264 via `libx264`, CRF 18–20, medium or slower preset;
- pixel format: `yuv420p` for broad playback compatibility;
- audio: AAC, 48 kHz, approximately 192 kbit/s when narration is present;
- streaming: enable `+faststart` for MP4 delivery;
- captions: retain an external `.srt` even if key captions are burned in.

Record narration separately in a quiet environment and edit it against the
picture. Normalize speech consistently and avoid music unless its reuse rights
are certain. A silent walkthrough with strong chapter cards and captions is
preferable to poor or distracting audio.

## Visual system

- Use the product's real typography and colors; do not imitate Mines Nancy or a
  manuscript venue's branding.
- Do not use institutional logos without explicit permission.
- Keep overlays within title-safe margins and use high-contrast text.
- Keep the `Public research sample` marker and show `Read-only public research
  API` or `Bundled public Kaggle research sample` whenever default records or
  charts are visible. Keep `Synthetic replay` visible for pipeline scenarios.
- Use the same short repository title and URL on the opening and closing cards.
- Generate the thumbnail from a clean product frame, not a PDF, evaluation, or
  personal document.

## Content boundaries

The video may say that this is research tooling developed for a Mines Nancy
internship. According to the project owner, it may say that an associated
manuscript is in advanced pre-publication review. It must not say accepted,
peer-reviewed, published, institutionally endorsed, or deployed at Mines Nancy.

The formal supervisor evaluation may be paraphrased only as praising quality
results, an impressive workload, a complete contribution, and strong written
structure. Do not show the evaluation itself or disclose scores, signatures,
contact details, or other personal information.

Default records are released metadata and model-derived labels from a
deterministic public-dataset sample. They are not ground truth, representative
findings, or the exact manuscript analysis set. Original Reddit text and authors
must not appear. Pipeline replay records are synthetic engineering fixtures and
must remain labelled as such. See [demo-data.md](../demo-data.md) for source
labels and import behavior.

## Review before publishing

- Watch the complete MP4 at 100% and at a small embedded size.
- Verify audio/video synchronization and caption timing.
- Check that no notification, local path, credential, browser profile, or private
  document appears in any frame.
- Confirm public-sample provenance remains legible in every default-data scene
  and synthetic replay provenance remains legible in every replay scene.
- Confirm all counts, test results, commands, URLs, and manuscript wording still
  match the tagged commit.
- Run `ffprobe` on the final file and verify resolution, frame rate, codecs,
  duration, and audio channels.
- Test playback in a Chromium browser and one independent player.
- Publish the MP4 as a release asset, then test the README thumbnail link from a
  signed-out browser.
