"""Bounded retry utilities with exponential backoff and jitter."""

from __future__ import annotations

import random
import time
from collections.abc import Callable

from reddit_minerals.errors import RetryableProviderError

_JITTER = random.SystemRandom()


def with_retries[T](
    operation: Callable[[], T],
    *,
    attempts: int,
    base_delay_seconds: float,
    max_delay_seconds: float,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run an operation, retrying only explicitly retryable provider failures."""

    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    if base_delay_seconds < 0 or max_delay_seconds < 0:
        raise ValueError("retry delays must be non-negative")

    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except RetryableProviderError:
            if attempt == attempts:
                raise
            exponential = base_delay_seconds * (2 ** (attempt - 1))
            sleep(min(max_delay_seconds, exponential * _JITTER.uniform(0.75, 1.25)))
    raise RuntimeError("retry loop exhausted without returning or raising")
