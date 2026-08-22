"""Environment-backed settings and subreddit mapping validation."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from reddit_minerals.errors import ConfigurationError

_SUBREDDIT_RE = re.compile(r"^[A-Za-z0-9_]{2,64}$")
MAX_PROVIDER_ITEMS = 10_000
MAX_REFRESH_HOURS = 8_760
MAX_OPERATION_TIMEOUT_SECONDS = 86_400
MAX_MAPPING_BYTES = 1_000_000
MAX_MAPPING_MINERALS = 500
MAX_MINERAL_NAME_CHARS = 128
MAX_SUBREDDITS_PER_MINERAL = 100
MAX_MAPPING_SUBREDDIT_ENTRIES = 10_000
_PLACEHOLDER_MARKERS = (
    "replace-",
    "replace_",
    "changeme",
    "change-me",
    "your-",
    "your_",
    "placeholder",
    "example",
)


class _DuplicateJsonObjectKeyError(ValueError):
    """A configuration JSON object repeats an exact key."""


def _default_mapping_path() -> Path:
    packaged = Path(__file__).with_name("defaults") / "subreddit_mapping.json"
    return packaged if packaged.is_file() else Path("configs/subreddit_mapping.json")


class AppSettings(BaseSettings):
    """Application configuration loaded from ``RMS_*`` environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="RMS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    database_path: Path = Path("data/reddit_minerals.sqlite3")
    subreddit_mapping_path: Path = Field(default_factory=_default_mapping_path)
    log_level: str = "INFO"

    reddit_client_id: SecretStr | None = None
    reddit_client_secret: SecretStr | None = None
    reddit_user_agent: str | None = None

    gemini_api_key: SecretStr | None = None
    gemini_model: str | None = None

    max_posts_per_mineral: int = Field(default=100, ge=1, le=MAX_PROVIDER_ITEMS)
    max_comments_per_post: int = Field(default=100, ge=0, le=MAX_PROVIDER_ITEMS)
    reddit_replace_more_limit: int = Field(default=8, ge=0, le=100)
    refresh_after_hours: int = Field(default=24, ge=0, le=MAX_REFRESH_HOURS)
    analysis_batch_size: int = Field(default=100, ge=1, le=MAX_PROVIDER_ITEMS)
    max_content_chars: int = Field(default=12_000, ge=500, le=100_000)
    max_context_comments: int = Field(default=10, ge=0, le=20)
    relevance_threshold: float = Field(default=70.0, ge=0, le=100, allow_inf_nan=False)

    max_retries: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=1.0, ge=0, le=60, allow_inf_nan=False)
    retry_max_delay_seconds: float = Field(default=30.0, ge=0, le=600, allow_inf_nan=False)
    reddit_request_timeout_seconds: float = Field(default=30.0, ge=1, le=300, allow_inf_nan=False)
    gemini_request_timeout_seconds: float = Field(default=120.0, ge=1, le=600, allow_inf_nan=False)
    operation_timeout_seconds: float = Field(
        default=1_800.0,
        ge=1,
        le=MAX_OPERATION_TIMEOUT_SECONDS,
        allow_inf_nan=False,
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, value: str) -> str:
        normalized = value.upper()
        if normalized not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
            raise ValueError("must be DEBUG, INFO, WARNING, ERROR, or CRITICAL")
        return normalized

    @model_validator(mode="after")
    def validate_timeout_budget(self) -> Self:
        if self.reddit_request_timeout_seconds > self.operation_timeout_seconds:
            raise ValueError("Reddit request timeout must not exceed the total operation timeout")
        if self.gemini_request_timeout_seconds > self.operation_timeout_seconds:
            raise ValueError("Gemini request timeout must not exceed the total operation timeout")
        return self

    def require_reddit(self) -> tuple[str, str, str]:
        client_id_setting = self.reddit_client_id
        client_secret_setting = self.reddit_client_secret
        user_agent = self.reddit_user_agent
        missing: list[str] = []
        if client_id_setting is None:
            missing.append("RMS_REDDIT_CLIENT_ID")
        if client_secret_setting is None:
            missing.append("RMS_REDDIT_CLIENT_SECRET")
        if not user_agent:
            missing.append("RMS_REDDIT_USER_AGENT")
        if missing:
            raise ConfigurationError(
                "Reddit configuration is incomplete; set " + ", ".join(missing)
            )
        if client_id_setting is None or client_secret_setting is None or not user_agent:
            raise ConfigurationError("Reddit configuration validation failed")
        invalid: list[str] = []
        client_id = client_id_setting.get_secret_value()
        client_secret = client_secret_setting.get_secret_value()
        if _looks_like_placeholder(client_id):
            invalid.append("RMS_REDDIT_CLIENT_ID")
        if _looks_like_placeholder(client_secret):
            invalid.append("RMS_REDDIT_CLIENT_SECRET")
        if _looks_like_placeholder(user_agent) or len(user_agent) < 10:
            invalid.append("RMS_REDDIT_USER_AGENT")
        if invalid:
            raise ConfigurationError(
                "Reddit configuration contains placeholder or non-identifying values: "
                + ", ".join(invalid)
            )
        return (
            client_id,
            client_secret,
            user_agent,
        )

    def require_gemini(self) -> tuple[str, str]:
        api_key_setting = self.gemini_api_key
        model = self.gemini_model
        missing: list[str] = []
        if api_key_setting is None:
            missing.append("RMS_GEMINI_API_KEY")
        if not model:
            missing.append("RMS_GEMINI_MODEL")
        if missing:
            raise ConfigurationError("AI configuration is incomplete; set " + ", ".join(missing))
        if api_key_setting is None or not model:
            raise ConfigurationError("AI configuration validation failed")
        api_key = api_key_setting.get_secret_value()
        invalid: list[str] = []
        if _looks_like_placeholder(api_key):
            invalid.append("RMS_GEMINI_API_KEY")
        if _looks_like_placeholder(model):
            invalid.append("RMS_GEMINI_MODEL")
        if invalid:
            raise ConfigurationError(
                "AI configuration contains placeholder values: " + ", ".join(invalid)
            )
        return api_key, model

    def safe_summary(self) -> dict[str, Any]:
        """Return a report that never exposes credential values."""

        return {
            "database_path": str(self.database_path),
            "subreddit_mapping_path": str(self.subreddit_mapping_path),
            "log_level": self.log_level,
            "reddit_configured": (
                self.reddit_client_id is not None
                and self.reddit_client_secret is not None
                and self.reddit_user_agent is not None
                and not _looks_like_placeholder(self.reddit_client_id.get_secret_value())
                and not _looks_like_placeholder(self.reddit_client_secret.get_secret_value())
                and not _looks_like_placeholder(self.reddit_user_agent)
                and len(self.reddit_user_agent) >= 10
            ),
            "gemini_configured": (
                self.gemini_api_key is not None
                and self.gemini_model is not None
                and not _looks_like_placeholder(self.gemini_api_key.get_secret_value())
                and not _looks_like_placeholder(self.gemini_model)
            ),
            "gemini_model": self.gemini_model,
            "bounds": {
                "max_posts_per_mineral": self.max_posts_per_mineral,
                "max_comments_per_post": self.max_comments_per_post,
                "analysis_batch_size": self.analysis_batch_size,
                "max_content_chars": self.max_content_chars,
                "reddit_request_timeout_seconds": self.reddit_request_timeout_seconds,
                "gemini_request_timeout_seconds": self.gemini_request_timeout_seconds,
                "operation_timeout_seconds": self.operation_timeout_seconds,
            },
        }


class MappingReport(BaseModel):
    """Validated and normalized subreddit configuration."""

    model_config = ConfigDict(extra="forbid")

    mapping: dict[str, tuple[str, ...]]
    duplicate_entries_removed: int
    mineral_count: int
    subreddit_count: int


def load_subreddit_mapping(path: Path) -> MappingReport:
    """Load, validate, and case-insensitively deduplicate the mapping file."""

    try:
        with path.open("rb") as handle:
            encoded = handle.read(MAX_MAPPING_BYTES + 1)
        if len(encoded) > MAX_MAPPING_BYTES:
            raise ConfigurationError(
                f"Subreddit mapping exceeds the {MAX_MAPPING_BYTES}-byte safety limit"
            )
        raw: Any = json.loads(
            encoded.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_object_keys,
        )
    except FileNotFoundError as exc:
        raise ConfigurationError(f"Subreddit mapping does not exist: {path}") from exc
    except ConfigurationError:
        raise
    except _DuplicateJsonObjectKeyError as exc:
        raise ConfigurationError("Subreddit mapping contains duplicate JSON object keys") from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ConfigurationError(f"Cannot read subreddit mapping {path}: {exc}") from exc

    if not isinstance(raw, dict) or not raw:
        raise ConfigurationError("Subreddit mapping must be a non-empty JSON object")
    if len(raw) > MAX_MAPPING_MINERALS:
        raise ConfigurationError(
            f"Subreddit mapping exceeds the {MAX_MAPPING_MINERALS}-mineral safety limit"
        )

    mapping: dict[str, tuple[str, ...]] = {}
    duplicates = 0
    configured_entries = 0
    unique_subreddits: set[str] = set()
    for mineral_index, (untyped_mineral, untyped_subreddits) in enumerate(raw.items(), start=1):
        if (
            not isinstance(untyped_mineral, str)
            or not untyped_mineral.strip()
            or len(untyped_mineral) > MAX_MINERAL_NAME_CHARS
        ):
            raise ConfigurationError(
                f"Mineral entry {mineral_index} must have a non-empty name no longer than "
                f"{MAX_MINERAL_NAME_CHARS} characters"
            )
        mineral = " ".join(untyped_mineral.lower().split())
        if mineral in mapping:
            raise ConfigurationError(
                f"Mineral entry {mineral_index} duplicates an earlier normalized name"
            )
        if not isinstance(untyped_subreddits, list) or not untyped_subreddits:
            raise ConfigurationError(f"Mineral entry {mineral_index} must map to a non-empty list")
        if len(untyped_subreddits) > MAX_SUBREDDITS_PER_MINERAL:
            raise ConfigurationError(
                f"Mineral entry {mineral_index} exceeds the "
                f"{MAX_SUBREDDITS_PER_MINERAL}-subreddit safety limit"
            )
        configured_entries += len(untyped_subreddits)
        if configured_entries > MAX_MAPPING_SUBREDDIT_ENTRIES:
            raise ConfigurationError(
                "Subreddit mapping exceeds the aggregate subreddit-entry safety limit"
            )

        seen: set[str] = set()
        normalized: list[str] = []
        for subreddit_index, value in enumerate(untyped_subreddits, start=1):
            if not isinstance(value, str) or not _SUBREDDIT_RE.fullmatch(value.strip()):
                raise ConfigurationError(
                    f"Subreddit entry {subreddit_index} for mineral entry {mineral_index} is invalid"
                )
            subreddit = value.strip()
            key = subreddit.casefold()
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            unique_subreddits.add(key)
            normalized.append(subreddit)
        mapping[mineral] = tuple(normalized)

    try:
        return MappingReport(
            mapping=mapping,
            duplicate_entries_removed=duplicates,
            mineral_count=len(mapping),
            subreddit_count=len(unique_subreddits),
        )
    except ValidationError as exc:  # defensive; the detailed checks above should catch this
        raise ConfigurationError(f"Invalid subreddit mapping: {exc}") from exc


def _looks_like_placeholder(value: str) -> bool:
    normalized = value.strip().casefold()
    return not normalized or any(marker in normalized for marker in _PLACEHOLDER_MARKERS)


def _reject_duplicate_object_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonObjectKeyError
        result[key] = value
    return result
