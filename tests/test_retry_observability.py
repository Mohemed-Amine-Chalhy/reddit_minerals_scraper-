from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import pytest

from reddit_minerals.errors import PermanentProviderError, RetryableProviderError
from reddit_minerals.observability import (
    _APPLICATION_LOGGER,
    _DEPENDENCY_LOGGERS,
    JsonFormatter,
    configure_logging,
)
from reddit_minerals.retry import with_retries


def test_retry_uses_bounded_exponential_delay_and_jitter(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    sleeps: list[float] = []
    jitter = iter((0.75, 1.25))
    monkeypatch.setattr("reddit_minerals.retry._JITTER.uniform", lambda _low, _high: next(jitter))

    def operation() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise RetryableProviderError("temporary")
        return "done"

    result = with_retries(
        operation,
        attempts=3,
        base_delay_seconds=2,
        max_delay_seconds=3,
        sleep=sleeps.append,
    )
    assert result == "done"
    assert calls == 3
    # Jitter must never violate the caller's advertised upper bound.
    assert sleeps == [1.5, 3.0]


def test_retry_reraises_final_retryable_error_without_extra_sleep() -> None:
    error = RetryableProviderError("still unavailable")
    sleeps: list[float] = []
    calls = 0

    def operation() -> None:
        nonlocal calls
        calls += 1
        raise error

    with pytest.raises(RetryableProviderError) as raised:
        with_retries(
            operation,
            attempts=2,
            base_delay_seconds=0,
            max_delay_seconds=0,
            sleep=sleeps.append,
        )
    assert raised.value is error
    assert calls == 2
    assert sleeps == [0]


def test_retry_does_not_intercept_permanent_or_unexpected_errors() -> None:
    for error in (PermanentProviderError("no"), RuntimeError("bug")):
        calls = 0

        def operation(error_to_raise: Exception = error) -> None:
            nonlocal calls
            calls += 1
            raise error_to_raise

        with pytest.raises(type(error)):
            with_retries(
                operation,
                attempts=3,
                base_delay_seconds=0,
                max_delay_seconds=0,
                sleep=lambda _delay: pytest.fail("must not sleep"),
            )
        assert calls == 1


def test_retry_returns_first_success_without_sleeping() -> None:
    assert (
        with_retries(
            lambda: 42,
            attempts=1,
            base_delay_seconds=10,
            max_delay_seconds=20,
            sleep=lambda _delay: pytest.fail("must not sleep"),
        )
        == 42
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"attempts": 0, "base_delay_seconds": 0, "max_delay_seconds": 0},
        {"attempts": 1, "base_delay_seconds": -1, "max_delay_seconds": 0},
        {"attempts": 1, "base_delay_seconds": 0, "max_delay_seconds": -1},
    ],
)
def test_retry_rejects_invalid_bounds_before_running_operation(
    kwargs: dict[str, float | int],
) -> None:
    with pytest.raises(ValueError, match="must"):
        with_retries(
            lambda: pytest.fail("operation must not run"),
            sleep=lambda _delay: pytest.fail("must not sleep"),
            **kwargs,
        )


def test_json_formatter_includes_safe_extras_and_exception_type_only() -> None:
    formatter = JsonFormatter()
    try:
        raise ValueError("sensitive response body")
    except ValueError:
        record = logging.getLogger("tests").makeRecord(
            "tests",
            logging.ERROR,
            __file__,
            1,
            "request failed",
            (),
            exc_info=__import__("sys").exc_info(),
            extra={
                "content_id": "p1",
                "attempts": 2,
                "selected": 8,
                "completed": 3,
                "retryable_failures": 2,
                "permanent_failures": 1,
                "blocked": 2,
                "comments_skipped_deleted": 4,
                "comment_associations_removed": 5,
            },
        )
    payload = json.loads(formatter.format(record))
    assert payload["level"] == "ERROR"
    assert payload["message"] == "request failed"
    assert payload["content_id"] == "p1"
    assert payload["selected"] == 8
    assert payload["completed"] == 3
    assert payload["retryable_failures"] == 2
    assert payload["permanent_failures"] == 1
    assert payload["blocked"] == 2
    assert payload["comments_skipped_deleted"] == 4
    assert payload["comment_associations_removed"] == 5
    # Arbitrary extras are intentionally excluded from the logging allowlist.
    assert "attempts" not in payload
    assert payload["exception_type"] == "ValueError"
    assert "sensitive response body" not in json.dumps(payload)


def test_configure_logging_emits_only_application_records_as_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = logging.getLogger()
    application = logging.getLogger(_APPLICATION_LOGGER)
    root.addHandler(logging.NullHandler())
    configure_logging("WARNING")
    logging.getLogger("offline").warning("dependency request with private query data")
    logging.getLogger("reddit_minerals.offline").warning(
        "bounded warning", extra={"run_id": "run-1"}
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.err)
    assert payload["message"] == "bounded warning"
    assert "private query data" not in captured.err
    assert "run_id" not in payload
    assert root.level > logging.CRITICAL
    assert len(root.handlers) == 1
    assert len(application.handlers) == 1


def test_configure_logging_suppresses_preexisting_dependency_child_handlers() -> None:
    child = logging.getLogger("httpx.transport")
    unexpected_messages: list[logging.LogRecord] = []

    class CapturingHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            unexpected_messages.append(record)

    child.handlers = [CapturingHandler()]
    child.propagate = False
    configure_logging("INFO")
    child.warning("request URL with private query data")

    assert unexpected_messages == []
    assert len(child.handlers) == 1


def test_configure_logging_replaces_and_closes_its_handlers_idempotently() -> None:
    application = logging.getLogger(_APPLICATION_LOGGER)
    configure_logging("INFO")
    original_application_handler = application.handlers[0]
    original_dependency_handlers = {
        name: logging.getLogger(name).handlers[0] for name in _DEPENDENCY_LOGGERS
    }

    configure_logging("ERROR")

    assert application.level == logging.ERROR
    assert len(application.handlers) == 1
    assert application.handlers[0] is not original_application_handler
    assert original_application_handler._closed is True
    for name, original_handler in original_dependency_handlers.items():
        dependency = logging.getLogger(name)
        assert len(dependency.handlers) == 1
        assert dependency.handlers[0] is not original_handler
        assert original_handler._closed is True


def test_invalid_environment_settings_are_logged_as_sanitized_json_in_clean_process() -> None:
    environment = os.environ.copy()
    sensitive_invalid_value = "private-invalid-retry-count"
    environment["RMS_MAX_RETRIES"] = sensitive_invalid_value
    completed = subprocess.run(
        [sys.executable, "-m", "reddit_minerals", "validate-config"],
        cwd=Path(__file__).resolve().parents[1],
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert completed.returncode == 2
    assert completed.stdout == ""
    payload = json.loads(completed.stderr)
    assert payload["level"] == "ERROR"
    assert payload["message"] == "configuration validation error"
    assert payload["error_type"] == "ValidationError"
    assert payload["validation_errors"]
    assert sensitive_invalid_value not in completed.stderr
