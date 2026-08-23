# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.12.14-slim-bookworm@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded
ARG NODE_IMAGE=node:24.19.0-bookworm-slim@sha256:3638d9a6fe4030bd716be989438248074489337ba3275657f93595428be4fc03

FROM ${UV_IMAGE} AS uv

FROM ${NODE_IMAGE} AS web-builder

WORKDIR /app/web

RUN npm install --global pnpm@11.19.0

COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY web ./
COPY src/reddit_minerals/web/data /app/src/reddit_minerals/web/data
ARG VITE_BASE_PATH=/
ENV VITE_BASE_PATH=${VITE_BASE_PATH}
RUN pnpm build

FROM ${PYTHON_IMAGE} AS builder

COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --group build --no-install-project

COPY src ./src
COPY configs ./configs
RUN uv sync --locked --no-default-groups --group build --no-editable --no-build-isolation

FROM builder AS web-python-builder

RUN uv sync --locked --no-default-groups --group build --extra web \
    --no-editable --no-build-isolation

FROM ${PYTHON_IMAGE} AS runtime-base

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RMS_DATABASE_PATH=/data/reddit_minerals.sqlite3 \
    RMS_LIVE_JOB_ROOT=/data/live_jobs \
    RMS_SUBREDDIT_MAPPING_PATH=/app/configs/subreddit_mapping.json

RUN groupadd --gid 10001 reddit-minerals \
    && useradd --uid 10001 --gid reddit-minerals --create-home \
        --home-dir /home/reddit-minerals --shell /usr/sbin/nologin reddit-minerals \
    && mkdir --parents /data /app/configs \
    && chown --recursive reddit-minerals:reddit-minerals /data /app

WORKDIR /app

COPY --chown=reddit-minerals:reddit-minerals configs/subreddit_mapping.json /app/configs/subreddit_mapping.json

USER reddit-minerals
STOPSIGNAL SIGTERM
VOLUME ["/data"]

FROM runtime-base AS web-runtime

ENV RMS_WEB_ASSET_DIR=/app/web/dist

COPY --from=web-python-builder --chown=reddit-minerals:reddit-minerals /app/.venv /app/.venv
COPY --from=web-builder --chown=reddit-minerals:reddit-minerals /app/web/dist /app/web/dist

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/v1/health', timeout=2).read()"]

ENTRYPOINT ["uvicorn"]
CMD ["reddit_minerals.web.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]

FROM runtime-base AS runtime

COPY --from=builder --chown=reddit-minerals:reddit-minerals /app/.venv /app/.venv
ENTRYPOINT ["reddit-minerals"]
CMD ["--help"]
