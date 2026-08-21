"""Domain-specific exceptions used to classify operational failures."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


class RedditMineralsError(Exception):
    """Base class for expected application failures."""


class ConfigurationError(RedditMineralsError):
    """Configuration is missing or invalid."""


class ConcurrentOperationError(RedditMineralsError):
    """Another supported writer currently owns the database operation lock."""


class ProviderWideError(RedditMineralsError):
    """A provider-wide fault that must abort a batch without poisoning work items."""


class ProviderConfigurationError(ProviderWideError):
    """A provider dependency or request configuration is invalid."""


class ProviderAuthenticationError(ProviderWideError):
    """Provider credentials or account permissions are invalid."""


class ProviderModelError(ProviderWideError):
    """The configured provider model is missing, disabled, or unsupported."""


class ProviderError(RedditMineralsError):
    """An external provider operation failed."""

    retryable: bool = False
    blocked: bool = False


class RetryableProviderError(ProviderError):
    """A provider failure that can safely be retried."""

    retryable = True


class PermanentProviderError(ProviderError):
    """A provider failure that should not be retried without intervention."""


class ContentBlockedError(PermanentProviderError):
    """The provider refused to process content due to its safety policy."""

    blocked = True


class InvalidProviderResponseError(RetryableProviderError):
    """A provider response did not match the requested schema."""


class BatchOperationError(RedditMineralsError):
    """An expected batch-level failure with a safe, persistable partial summary."""

    def __init__(self, message: str, *, summary: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.summary = dict(summary or {})


class BatchProviderFailureError(BatchOperationError):
    """Every selected item in a batch failed at the provider boundary."""


class OperationDeadlineExceededError(BatchOperationError):
    """A bounded batch reached its monotonic operation deadline."""
