# MineralLens frontend

MineralLens is a React and TypeScript research interface for the Reddit
Minerals Pipeline. Its default public experience is a deterministic,
metadata-only sample from the published Kaggle dataset; an explicitly enabled
backend can additionally offer bounded, user-initiated Reddit collection.

## Data sources

At startup the app requests the read-only `/api/v1` portfolio endpoints. API
responses are validated with Zod and adapted into the same internal snapshot
used by the UI. If the API is unavailable—such as on a static GitHub Pages
deployment—the app transparently uses bundled deterministic fixtures.

When the backend advertises `live_collection`, the Pipeline page adds a **Live
Reddit** source alongside its offline execution replay. The live form maps each
mineral target to its own subreddit list and supports Reddit time filters, bounded post/comment
limits, configurable progress polling, cancellation, and either server-managed
or per-job Reddit application credentials. A completed raw snapshot is held in
the React research context and can be opened directly in Explorer. It is
explicitly labelled as unanalysed: the frontend never invents sentiment,
stance, topic, relevance, or reputation results.

The capability probe fails closed. Live controls are not rendered when the
backend disables them, when no live endpoint exists, or on the static GitHub
Pages deployment. Static hosting therefore preserves the public sample and
offline replay exactly as before.

The Explorer can also inspect a compatible JSON or JSONL export locally. Files
are parsed in the browser, limited to 10 MB and 5,000 records, and are never
uploaded by this application.

## Commands

```shell
pnpm install --frozen-lockfile
pnpm dev
pnpm check
```

Vite proxies `/api` to `http://127.0.0.1:8000` during local development. Set
`VITE_BASE_PATH=/repository-name/` for a project-scoped Pages build; it defaults
to `/` for local and FastAPI-hosted builds. Set `VITE_DATA_MODE=fixture` for a
fully static build that reads bundled data directly instead of probing the API.

Do not place Reddit credentials in `VITE_*` variables: Vite embeds those values
in the browser bundle. Provided credentials exist only in the live form's
component memory, are cleared as soon as submission begins, and are never
written to a URL, browser storage, application logs, job status, or result
payload. The one-time job token is sent only in the
`X-Live-Job-Token` request header. Terminal jobs are deleted best-effort after
handoff, with server retention limits as a fallback.

## Structure

- `src/domain`: strict schemas, repository boundaries, import validation, and selectors.
- `src/data`: deterministic synthetic fallback data.
- `src/features`: overview, explorer, live collection, pipeline replay, and engineering case study.
- `src/components`: accessible shared presentation components.
- `src/styles`: custom token-based responsive design system without external fonts.
