"""Canonical key normalization and duplicate/empty-key policy."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import math
import numbers
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Generic, Iterable, Mapping, TypeVar
from zoneinfo import ZoneInfo

import pandas as pd

T = TypeVar("T")


@dataclass(frozen=True)
class CanonicalKey:
    """A losslessly normalized key and its stable lookup digest."""

    value: str
    digest: str

    @classmethod
    def from_value(cls, value: str) -> "CanonicalKey":
        return cls(value=value, digest=hashlib.md5(value.encode("utf-8")).hexdigest())


@dataclass(frozen=True)
class KeyIndex(Generic[T]):
    """A duplicate-free key index plus preserved empty-key observations."""

    items: Mapping[str, T]
    empty_count: int = 0


class KeyPolicy:
    """Normalize keys without lossy float coercion and build strict indexes."""

    MIN_TIMESTAMP_SECONDS = Decimal("946684800")
    MAX_TIMESTAMP_SECONDS = Decimal("4102444800")
    MIN_TIMESTAMP_MILLISECONDS = Decimal("946684800000")
    MAX_TIMESTAMP_MILLISECONDS = Decimal("4102444800000")
    MAX_SAFE_FLOAT_INTEGER = 2**53

    def __init__(
        self,
        *,
        datetime_granularity: str = "exact",
        datetime_timezone: str | None = None,
    ) -> None:
        granularity = str(datetime_granularity).strip().lower()
        if granularity not in {"exact", "day"}:
            raise ValueError("datetime index granularity must be 'exact' or 'day'")
        if granularity == "exact":
            if datetime_timezone not in (None, ""):
                raise ValueError(
                    "datetime index timezone is only valid with day granularity"
                )
            timezone = None
        else:
            if not datetime_timezone or not str(datetime_timezone).strip():
                raise ValueError(
                    "day datetime index granularity requires an IANA timezone"
                )
            try:
                timezone = ZoneInfo(str(datetime_timezone).strip())
            except Exception as exc:
                raise ValueError(
                    f"invalid IANA datetime index timezone: {datetime_timezone}"
                ) from exc
        self.datetime_granularity = granularity
        self.datetime_timezone = timezone

    @staticmethod
    def is_empty(value: Any) -> bool:
        if value is None:
            return True
        if isinstance(value, str):
            return value.strip() == ""
        if isinstance(value, dict):
            if "value" in value:
                return KeyPolicy.is_empty(value.get("value"))
            return not value
        if isinstance(value, (list, tuple, set)):
            return not value or all(KeyPolicy.is_empty(item) for item in value)
        if pd.api.types.is_scalar(value):
            try:
                return bool(pd.isna(value))
            except (TypeError, ValueError):
                return False
        return False

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        if not value.is_finite():
            raise ValueError("numeric key must be finite")
        normalized = value.normalize()
        if normalized == normalized.to_integral_value():
            return format(normalized.quantize(Decimal(1)), "f")
        return format(normalized, "f").rstrip("0").rstrip(".")

    def normalize_number(self, value: Any) -> str | None:
        if self.is_empty(value):
            return None
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("numeric key must be finite")
            if value.is_integer() and abs(value) > self.MAX_SAFE_FLOAT_INTEGER:
                raise ValueError(
                    "integer-like float key exceeds 2^53 and may already be lossy; "
                    "provide the key as an integer or text"
                )
            raw = str(value)
        elif isinstance(value, Decimal):
            return self._decimal_text(value)
        elif isinstance(value, numbers.Integral):
            return str(int(value))
        else:
            raw = str(value).strip().replace(",", "")
        try:
            return self._decimal_text(Decimal(raw))
        except (InvalidOperation, ValueError):
            return str(value)

    def _timestamp_decimal_to_milliseconds(self, value: Decimal) -> int:
        if not value.is_finite():
            raise ValueError("numeric DATETIME key must be finite")
        if self.MIN_TIMESTAMP_SECONDS <= value <= self.MAX_TIMESTAMP_SECONDS:
            return int(value * 1000)
        if self.MIN_TIMESTAMP_MILLISECONDS <= value <= self.MAX_TIMESTAMP_MILLISECONDS:
            return int(value)
        raise ValueError(
            "numeric DATETIME index must be epoch seconds or milliseconds "
            "within 2000-01-01..2100-01-01 UTC"
        )

    def _numeric_timestamp(self, value: Any) -> int:
        if isinstance(value, float):
            if not math.isfinite(value):
                raise ValueError("numeric DATETIME key must be finite")
            raw = str(value)
        else:
            raw = str(value).strip()
        try:
            return self._timestamp_decimal_to_milliseconds(Decimal(raw))
        except InvalidOperation as exc:
            raise ValueError(f"invalid numeric DATETIME key: {value}") from exc

    @staticmethod
    def _as_timestamp(value: Any) -> pd.Timestamp | None:
        if isinstance(value, dt.date) and not isinstance(value, dt.datetime):
            value = dt.datetime.combine(value, dt.time.min)
        try:
            parsed = pd.Timestamp(value)
        except (TypeError, ValueError):
            return None
        return None if pd.isna(parsed) else parsed

    def normalize_datetime(self, value: Any) -> str | None:
        if self.is_empty(value) or isinstance(value, bool):
            return None
        if isinstance(value, numbers.Real) or isinstance(value, Decimal):
            milliseconds = self._numeric_timestamp(value)
            parsed = pd.Timestamp(milliseconds, unit="ms", tz="UTC")
        elif isinstance(value, str) and value.strip().lstrip("+-").isdigit():
            milliseconds = self._numeric_timestamp(value.strip())
            parsed = pd.Timestamp(milliseconds, unit="ms", tz="UTC")
        else:
            parsed = self._as_timestamp(value)
            if parsed is None:
                return None

        if self.datetime_granularity == "exact":
            if parsed.tzinfo is None:
                parsed = parsed.tz_localize("UTC")
            return str(int(parsed.tz_convert("UTC").value // 1_000_000))

        assert self.datetime_timezone is not None
        if parsed.tzinfo is None:
            localized = parsed.tz_localize(self.datetime_timezone)
        else:
            localized = parsed.tz_convert(self.datetime_timezone)
        return localized.date().isoformat()

    def normalize(
        self,
        value: Any,
        field_type: int | None = None,
    ) -> CanonicalKey | None:
        if self.is_empty(value):
            return None
        normalized = self._normalize_value(value, field_type)
        if normalized is None:
            return None
        return CanonicalKey.from_value(normalized)

    def _normalize_value(self, value: Any, field_type: int | None) -> str | None:
        if isinstance(value, dict):
            if "value" in value and "type" in value:
                nested_type = value.get("type")
                return self._normalize_value(
                    value.get("value"),
                    nested_type if isinstance(nested_type, int) else field_type,
                )
            if "text" in value:
                text = str(value.get("text", ""))
                return text if text.strip() else None
            if "link_record_ids" in value:
                return self._normalize_value(value.get("link_record_ids"), field_type)
            if "id" in value:
                return str(value.get("id"))
            if "file_token" in value:
                return str(value.get("file_token"))
            normalized_dict = {
                str(key): item
                for key, raw in value.items()
                if (item := self._normalize_value(raw, None)) is not None
            }
            if not normalized_dict:
                return None
            return json.dumps(
                normalized_dict,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )

        if isinstance(value, (list, tuple, set)):
            values = list(value)
            if field_type == 1 or all(
                isinstance(item, dict) and "text" in item for item in values
            ):
                parts = []
                for item in values:
                    if isinstance(item, dict) and "text" in item:
                        text = str(item.get("text", ""))
                        if text.strip():
                            parts.append(text)
                    elif (normalized := self._normalize_value(item, None)) is not None:
                        parts.append(normalized)
                return "".join(parts) or None
            normalized_items = [
                item
                for raw in values
                if (item := self._normalize_value(raw, field_type)) is not None
            ]
            if not normalized_items:
                return None
            if len(normalized_items) == 1:
                return normalized_items[0]
            return json.dumps(
                normalized_items, ensure_ascii=False, separators=(",", ":")
            )

        if field_type == 2:
            return self.normalize_number(value)
        if field_type == 5:
            return self.normalize_datetime(value)
        if field_type == 7:
            if isinstance(value, bool):
                return "true" if value else "false"
            text = str(value).strip().lower()
            if text in {"true", "1", "yes", "y", "是"}:
                return "true"
            if text in {"false", "0", "no", "n", "否"}:
                return "false"
            raise ValueError(f"invalid boolean key: {value}")
        return str(value)

    def build_index(
        self,
        values: Iterable[T],
        *,
        value_getter: Callable[[T], Any],
        field_type: int | None,
        context: str,
        allow_empty: bool,
    ) -> KeyIndex[T]:
        items: dict[str, T] = {}
        empty_count = 0
        for position, item in enumerate(values, start=1):
            try:
                key = self.normalize(value_getter(item), field_type)
            except ValueError as exc:
                raise ValueError(
                    f"{context}第 {position} 条记录的 key 无法安全归一化: {exc}"
                ) from exc
            if key is None:
                if not allow_empty:
                    raise ValueError(f"{context}第 {position} 条记录的 key 为空")
                empty_count += 1
                continue
            if key.digest in items:
                raise ValueError(f"{context}存在重复值 key: {key.value}")
            items[key.digest] = item
        return KeyIndex(items=items, empty_count=empty_count)


__all__ = ["CanonicalKey", "KeyIndex", "KeyPolicy"]
