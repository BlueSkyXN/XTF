"""Immutable source/target snapshots and deterministic local fingerprints."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence, Union

import pandas as pd

from api.bitable_backend import (
    BitableBackendKind,
    CanonicalRecord,
    FieldSchema,
    RecordReadResult,
)


def _stable_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _stable_value(item)
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
        }
    if isinstance(value, (list, tuple)):
        return [_stable_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized = [_stable_value(item) for item in value]
        return sorted(
            normalized,
            key=lambda item: json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
        )
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def content_fingerprint(value: Any) -> str:
    encoded = json.dumps(
        _stable_value(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class SourceTable:
    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]

    @classmethod
    def from_dataframe(cls, frame: pd.DataFrame) -> "SourceTable":
        columns = tuple(str(column) for column in frame.columns)
        rows = tuple(tuple(row) for row in frame.itertuples(index=False, name=None))
        return cls(columns, rows)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.rows, columns=self.columns)


@dataclass(frozen=True)
class BitableSnapshot:
    backend: BitableBackendKind
    records: tuple[CanonicalRecord, ...]
    schema: tuple[FieldSchema, ...]
    complete: bool
    ignored_fields: tuple[Mapping[str, Any], ...]
    record_not_found: tuple[str, ...]
    revision: int | str | None
    timezone: str | None
    inspected_at: datetime
    fingerprint: str

    @classmethod
    def from_result(cls, result: RecordReadResult) -> "BitableSnapshot":
        material = {
            "backend": result.backend.value,
            "revision": result.revision,
            "timezone": result.timezone,
            "schema": [
                {
                    "id": item.id,
                    "name": item.name,
                    "kind": item.kind.value,
                    "multiple": item.multiple,
                    "writable": item.writable,
                    "raw_type": item.raw_type,
                    "raw_properties": item.raw_properties,
                }
                for item in result.fields
            ],
            "records": [
                {"record_id": item.record_id, "fields": item.fields}
                for item in result.records
            ],
        }
        return cls(
            backend=result.backend,
            records=tuple(result.records),
            schema=tuple(result.fields),
            complete=result.complete,
            ignored_fields=tuple(result.ignored_fields),
            record_not_found=tuple(result.record_not_found),
            revision=result.revision,
            timezone=result.timezone,
            inspected_at=datetime.now(timezone.utc),
            fingerprint=content_fingerprint(material),
        )


@dataclass(frozen=True)
class SheetSnapshot:
    actual_ranges: tuple[str, ...]
    grid: tuple[int, int] | None
    header: tuple[str, ...]
    index_mapping: tuple[tuple[str, int], ...]
    formula_columns: tuple[str, ...]
    complete: bool
    inspected_at: datetime
    content_fingerprint: str

    @classmethod
    def from_dataframe(
        cls,
        frame: pd.DataFrame,
        *,
        actual_ranges: Sequence[str],
        grid: tuple[int, int] | None,
        index_mapping: Mapping[str, int],
        formula_columns: Sequence[str] = (),
        complete: bool,
    ) -> "SheetSnapshot":
        return cls(
            actual_ranges=tuple(actual_ranges),
            grid=grid,
            header=tuple(str(column) for column in frame.columns),
            index_mapping=tuple(sorted(index_mapping.items())),
            formula_columns=tuple(sorted(str(item) for item in formula_columns)),
            complete=complete,
            inspected_at=datetime.now(timezone.utc),
            content_fingerprint=content_fingerprint(
                {
                    "columns": [str(column) for column in frame.columns],
                    "rows": frame.where(pd.notna(frame), None).values.tolist(),
                }
            ),
        )


TargetSnapshot = Union[BitableSnapshot, SheetSnapshot]


__all__ = [
    "BitableSnapshot",
    "SheetSnapshot",
    "SourceTable",
    "TargetSnapshot",
    "content_fingerprint",
]
