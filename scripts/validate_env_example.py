"""Validate the committed environment template without exposing secret values."""

from __future__ import annotations

import re
from pathlib import Path

EXPECTED_KEYS = {
    "RMS_ANALYSIS_BATCH_SIZE",
    "RMS_DATABASE_PATH",
    "RMS_GEMINI_API_KEY",
    "RMS_GEMINI_MODEL",
    "RMS_LOG_LEVEL",
    "RMS_MAX_COMMENTS_PER_POST",
    "RMS_MAX_CONTENT_CHARS",
    "RMS_MAX_CONTEXT_COMMENTS",
    "RMS_MAX_POSTS_PER_MINERAL",
    "RMS_MAX_RETRIES",
    "RMS_OPERATION_TIMEOUT_SECONDS",
    "RMS_REDDIT_CLIENT_ID",
    "RMS_REDDIT_CLIENT_SECRET",
    "RMS_REDDIT_REPLACE_MORE_LIMIT",
    "RMS_REDDIT_REQUEST_TIMEOUT_SECONDS",
    "RMS_REDDIT_USER_AGENT",
    "RMS_REFRESH_AFTER_HOURS",
    "RMS_RELEVANCE_THRESHOLD",
    "RMS_RETRY_BASE_DELAY_SECONDS",
    "RMS_RETRY_MAX_DELAY_SECONDS",
    "RMS_GEMINI_REQUEST_TIMEOUT_SECONDS",
    "RMS_SUBREDDIT_MAPPING_PATH",
}
SECRET_KEYS = {
    "RMS_GEMINI_API_KEY",
    "RMS_REDDIT_CLIENT_ID",
    "RMS_REDDIT_CLIENT_SECRET",
}
KEY_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def parse_template(path: Path) -> tuple[dict[str, str], list[str]]:
    """Parse an env template and collect structural errors."""
    values: dict[str, str] = {}
    failures: list[str] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            failures.append(f"line {line_number} is not KEY=VALUE")
            continue
        key, value = line.split("=", maxsplit=1)
        if not KEY_PATTERN.fullmatch(key):
            failures.append(f"line {line_number} has an invalid key")
        if key in values:
            failures.append(f"line {line_number} duplicates {key}")
        if not value:
            failures.append(f"line {line_number} leaves {key} empty")
        values[key] = value
    return values, failures


def main() -> int:
    """Validate the environment template and return a process exit code."""
    template_path = Path(__file__).resolve().parents[1] / ".env.example"
    values, failures = parse_template(template_path)

    missing = sorted(EXPECTED_KEYS - values.keys())
    unexpected = sorted(values.keys() - EXPECTED_KEYS)
    if missing:
        failures.append(f"missing keys: {', '.join(missing)}")
    if unexpected:
        failures.append(f"unexpected keys: {', '.join(unexpected)}")
    for key in sorted(SECRET_KEYS):
        if key in values and not values[key].startswith("replace-"):
            failures.append(f"{key} must contain an obvious placeholder")

    if failures:
        # Do not print parser details derived from a credential-shaped file.
        # The validator needs only a failing exit code in hooks/CI, and keeping
        # all template-derived text out of logs gives secret scanners a simple,
        # auditable guarantee.
        print(f"Environment template validation failed with {len(failures)} issue(s).")
        print("Check key names, placeholders, duplicates, and KEY=VALUE structure.")
        return 1

    print(f"Validated {len(values)} environment variable names; values were not displayed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
