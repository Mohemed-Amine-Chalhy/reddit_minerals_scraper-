# Notebooks

These notebooks are optional exploration tools. The `reddit-minerals` CLI and
SQLite database are the supported production interfaces; notebooks must not be
used as pipeline state or committed with source data, generated charts, cell
outputs, or execution counts.

1. Install the optional, locked notebook environment from the repository root:

   ```shell
   uv sync --locked --group notebooks
   ```

2. Create an export:

   ```shell
   reddit-minerals export --mineral gold --format jsonl --output exports/gold.jsonl
   ```

3. Open `data_processing.ipynb`, set `MINERAL` and `EXPORT_PATH`, and run it.
4. Open `data_analysis.ipynb` for a schema-tolerant overview of the same export.

Paths may be absolute or relative to the repository root. Both notebooks handle
a missing export without failing, so they can be syntax-checked in CI without
access to production data or provider credentials.

Before committing notebook changes, clear outputs and execution counts, then run
the repository check script. The pre-commit hooks reject dirty or malformed
notebook JSON; they do not silently rewrite notebooks.
