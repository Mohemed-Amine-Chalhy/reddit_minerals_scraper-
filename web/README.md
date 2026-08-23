# MineralLens frontend

MineralLens is a read-only React and TypeScript portfolio interface for the
Reddit Minerals Pipeline. It always identifies itself as a synthetic demo.

## Data sources

At startup the app requests the read-only `/api/v1` portfolio endpoints. API
responses are validated with Zod and adapted into the same internal snapshot
used by the UI. If the API is unavailable—such as on a static GitHub Pages
deployment—the app transparently uses bundled deterministic fixtures.

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

## Structure

- `src/domain`: strict schemas, repository boundaries, import validation, and selectors.
- `src/data`: deterministic synthetic fallback data.
- `src/features`: overview, explorer, pipeline replay, and engineering case study.
- `src/components`: accessible shared presentation components.
- `src/styles`: custom token-based responsive design system without external fonts.
