"""Bounded read-only confirmation, independent of mutation retry policy."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import math
from time import monotonic, sleep
from typing import Any, Callable
from zoneinfo import ZoneInfo

from api import FeishuAPIError, FieldKind, FieldSchema


@dataclass(frozen=True)
class ReadbackWait:
    verified: bool
    attempts: int
    elapsed_seconds: float
    last_error: str | None = None


def wait_for_readback(
    check: Callable[[], bool],
    *,
    timeout: float,
    interval: float,
    on_wait: Callable[[], None] | None = None,
) -> ReadbackWait:
    """Never replay a write. Invalid responses and permanent errors fail immediately.

    The timeout bounds scheduling of further reads, not an in-flight HTTP call.
    Each read retains its own transport timeout and retry budget.
    """
    if (
        isinstance(timeout, bool)
        or not math.isfinite(timeout)
        or not 0 <= timeout <= 300
    ):
        raise ValueError("readback timeout must be between 0 and 300 seconds")
    if (
        isinstance(interval, bool)
        or not math.isfinite(interval)
        or not 0.05 <= interval <= 60
    ):
        raise ValueError("readback interval must be between 0.05 and 60 seconds")
    started = monotonic()
    deadline = started + timeout
    last_error = None
    # Also bounded when an injected clock/sleeper does not advance.
    max_attempts = max(1, math.ceil(timeout / interval) + 1)
    for attempt in range(1, max_attempts + 1):
        retry_after = 0.0
        try:
            if check():
                return ReadbackWait(True, attempt, monotonic() - started)
            last_error = None
        except FeishuAPIError as error:
            if not error.retryable or error.kind == "invalid_response":
                raise
            last_error = str(error)
            retry_after = error.retry_after or 0.0
        remaining = deadline - monotonic()
        if remaining <= 0 or attempt == max_attempts:
            break
        delay = max(
            min(interval * 2 ** min(attempt - 1, 6), max(interval, 2.0)), retry_after
        )
        if not math.isfinite(delay) or retry_after > remaining:
            break
        delay = min(delay, remaining)
        if attempt == 1 and on_wait is not None:
            on_wait()
        sleep(delay)
    return ReadbackWait(False, attempt, monotonic() - started, last_error)


def _empty(value: Any) -> bool:
    return value is None or value == "" or value == [] or value == ()


def _milliseconds(value: Any, tz: str | None) -> Decimal:
    if isinstance(value, bool):
        raise ValueError("boolean is not a timestamp")
    if isinstance(value, (int, float, Decimal)):
        return Decimal(str(value))
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("not a datetime")
    if value.tzinfo is None:
        value = value.replace(tzinfo=ZoneInfo(tz) if tz else timezone.utc)
    delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        Decimal((delta.days * 86400 + delta.seconds) * 1000)
        + Decimal(delta.microseconds) / 1000
    )


def cells_equal(
    expected: Any, actual: Any, schema: FieldSchema | None, tz: str | None = None
) -> bool:
    """Compare written values, NOT matching keys (no day truncation or text stripping)."""
    if _empty(expected) or _empty(actual):
        return _empty(expected) and _empty(actual)
    kind = schema.kind if schema is not None else None
    try:
        if kind is FieldKind.NUMBER:
            if isinstance(expected, bool) or isinstance(actual, bool):
                return False
            left, right = Decimal(str(expected)), Decimal(str(actual))
            return left.is_finite() and right.is_finite() and left == right
        if kind is FieldKind.DATETIME:
            return _milliseconds(expected, tz) == _milliseconds(actual, tz)
        if kind in {
            FieldKind.SELECT,
            FieldKind.USER,
            FieldKind.GROUP_CHAT,
            FieldKind.LINK,
        }:

            def values(value: Any) -> Counter:
                items = value if isinstance(value, (list, tuple)) else [value]
                return Counter(
                    item.get("id") if isinstance(item, dict) else item for item in items
                )

            return values(expected) == values(actual)
        if kind is FieldKind.CHECKBOX:
            return (
                isinstance(expected, bool)
                and isinstance(actual, bool)
                and expected == actual
            )
    except (ValueError, TypeError, InvalidOperation, OverflowError):
        return False
    return type(expected) is type(actual) and expected == actual


def sheet_values_equal(expected: list[list[Any]], actual: Any) -> bool:
    """Sheets can omit trailing empty cells/rows; internal empty rows stay positional."""
    if not isinstance(actual, list) or any(
        not isinstance(row, (list, tuple)) for row in actual
    ):
        return False
    for r in range(max(len(expected), len(actual))):
        left = expected[r] if r < len(expected) else []
        right = actual[r] if r < len(actual) else []
        for c in range(max(len(left), len(right))):
            a = left[c] if c < len(left) else None
            b = right[c] if c < len(right) else None
            if isinstance(a, dict) and a.get("type") == "formula":
                a = a.get("text")
                if not isinstance(a, str) or not a.startswith("="):
                    return False
            if isinstance(b, dict) and b.get("type") == "formula":
                b = b.get("text")
            if _empty(a) and _empty(b):
                continue
            if isinstance(a, bool) != isinstance(b, bool) or a != b:
                return False
    return True
