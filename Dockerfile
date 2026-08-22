# syntax=docker/dockerfile:1.7

ARG PYTHON_IMAGE=python:3.12.14-slim-bookworm@sha256:a116514e19457bcb7af7efe9c3dd0b9b71e85b317694e7882a1c52aa15a78134
ARG UV_IMAGE=ghcr.io/astral-sh/uv:0.12.1@sha256:cf4eedcaa81655197f625739489effcbe71b61ceb1506f332c3facae5deceded

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE} AS builder

COPY --from=uv /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-default-groups --group build --no-install-project

COPY src ./src
RUN uv sync --locked --no-default-groups --group build --no-editable --no-build-isolation

FROM ${PYTHON_IMAGE} AS runtime

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RMS_DATABASE_PATH=/data/reddit_minerals.sqlite3 \
    RMS_SUBREDDIT_MAPPING_PATH=/app/configs/subreddit_mapping.json

RUN groupadd --gid 10001 reddit-minerals \
    && useradd --uid 10001 --gid reddit-minerals --create-home \
        --home-dir /home/reddit-minerals --shell /usr/sbin/nologin reddit-minerals \
    && mkdir --parents /data /app/configs \
    && chown --recursive reddit-minerals:reddit-minerals /data /app

WORKDIR /app

COPY --from=builder --chown=reddit-minerals:reddit-minerals /app/.venv /app/.venv
COPY --chown=reddit-minerals:reddit-minerals configs/subreddit_mapping.json /app/configs/subreddit_mapping.json

USER reddit-minerals
VOLUME ["/data"]
STOPSIGNAL SIGTERM

ENTRYPOINT ["reddit-minerals"]
CMD ["--help"]
