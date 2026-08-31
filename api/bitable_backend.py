"""Typed, protocol-neutral contracts for Feishu Bitable backends.

The legacy :class:`api.bitable.BitableAPI` facade is intentionally not imported
here.  This module contains only the small canonical model shared by the
versioned wire clients, so Base v3 and Bitable v1 cannot accidentally share
request or response shapes.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Protocol, Sequence


class BitableBackendKind(str, Enum):
    """Supported, explicitly selected Bitable API families."""

    BASE_V3 = "base_v3"
    BITABLE_V1 = "bitable_v1"


class UserIDType(str, Enum):
    """ID namespace used by the v1 user fields."""

    OPEN_ID = "open_id"
    UNION_ID = "union_id"
    USER_ID = "user_id"


class FieldKind(str, Enum):
    """Protocol-neutral field kinds used by the sync engine."""

    TEXT = "text"
    NUMBER = "number"
    SELECT = "select"
    DATETIME = "datetime"
    CHECKBOX = "checkbox"
    USER = "user"
    GROUP_CHAT = "group_chat"
    LINK = "link"
    LOCATION = "location"
    ATTACHMENT = "attachment"
    FORMULA = "formula"
    LOOKUP = "lookup"
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"
    CREATED_BY = "created_by"
    UPDATED_BY = "updated_by"
    AUTO_NUMBER = "auto_number"
    UNSUPPORTED = "unsupported"


class MutationOutcome(str, Enum):
    """Outcome of a remote mutation request."""

    ACCEPTED = "accepted"
    PARTIAL = "partial"
    REJECTED = "rejected"
    UNKNOWN_OUTCOME = "unknown_outcome"


class ReadbackStatus(str, Enum):
    """Optional post-write readback state."""

    NOT_REQUESTED = "not_requested"
    VERIFIED = "verified"
    MISMATCH = "mismatch"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class FieldSchema:
    """Canonical field definition, independent of API version."""

    id: str | None
    name: str
    kind: FieldKind
    multiple: bool = False
    writable: bool = True
    raw_type: int | str | None = None
    raw_properties: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CanonicalRecord:
    """A record represented with ordinary Python values."""

    record_id: str | None
    fields: Mapping[str, Any]


@dataclass(frozen=True)
class RecordReadResult:
    """Complete (or explicitly incomplete) record read result."""

    records: tuple[CanonicalRecord, ...]
    fields: tuple[FieldSchema, ...]
    complete: bool
    backend: BitableBackendKind
    revision: int | str | None = None
    timezone: str | None = None
    ignored_fields: tuple[Mapping[str, Any], ...] = ()
    record_not_found: tuple[str, ...] = ()
    raw_metadata: Mapping[str, Any] = field(default_factory=dict)


class IncompleteReadError(Exception):
    """读取无法证明完整；保留已成功解析的 partial result。"""

    def __init__(self, message: str, partial_result: RecordReadResult):
        self.partial_result = partial_result
        super().__init__(message)


@dataclass(frozen=True)
class MutationReceipt:
    """Typed mutation result while retaining the applied-prefix boundary."""

    operation: str
    backend: BitableBackendKind | str
    requested_count: int
    accepted_count: int = 0
    unit: str | None = None
    verified_count: int = 0
    record_ids: tuple[str, ...] = ()
    revision: int | str | None = None
    actual_ranges: tuple[Any, ...] = ()
    updated_rows: int | None = None
    updated_columns: int | None = None
    updated_cells: int | None = None
    ignored_fields: tuple[Mapping[str, Any], ...] = ()
    record_not_found: tuple[str, ...] = ()
    failed_batch_index: int | None = None
    outcome: MutationOutcome = MutationOutcome.ACCEPTED
    readback: ReadbackStatus = ReadbackStatus.NOT_REQUESTED
    unknown_scope: bool = False
    raw_metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def applied_count(self) -> int:
        """Compatibility spelling used by the implementation plan."""

        return self.accepted_count

    @property
    def is_success(self) -> bool:
        """Whether the request is safe to treat as a complete batch."""

        return self.outcome is MutationOutcome.ACCEPTED


class BitableBackend(Protocol):
    """Minimal interface consumed by a typed sync engine."""

    api_family: BitableBackendKind
    max_batch_create_size: int
    max_batch_update_size: int
    max_batch_delete_size: int
    max_batch_get_size: int

    def list_fields(self, app_token: str, table_id: str) -> tuple[FieldSchema, ...]: ...

    def create_field(
        self,
        app_token: str,
        table_id: str,
        field_name: str,
        field_type: int | str = 1,
    ) -> MutationReceipt: ...

    def list_records(
        self,
        app_token: str,
        table_id: str,
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult: ...

    def batch_get_records(
        self,
        app_token: str,
        table_id: str,
        record_ids: Sequence[str],
        field_names: Sequence[str] | None = None,
    ) -> RecordReadResult: ...

    def batch_create(
        self,
        app_token: str,
        table_id: str,
        records: Sequence[CanonicalRecord],
    ) -> MutationReceipt: ...

    def batch_update(
        self,
        app_token: str,
        table_id: str,
        records: Sequence[CanonicalRecord],
    ) -> MutationReceipt: ...

    def batch_delete(
        self,
        app_token: str,
        table_id: str,
        record_ids: Sequence[str],
    ) -> MutationReceipt: ...


READ_ONLY_KINDS = frozenset(
    {
        FieldKind.ATTACHMENT,
        FieldKind.FORMULA,
        FieldKind.LOOKUP,
        FieldKind.CREATED_AT,
        FieldKind.UPDATED_AT,
        FieldKind.CREATED_BY,
        FieldKind.UPDATED_BY,
        FieldKind.AUTO_NUMBER,
        FieldKind.UNSUPPORTED,
    }
)


def as_backend_kind(value: BitableBackendKind | str) -> BitableBackendKind:
    """Normalize and validate an explicitly selected backend name."""

    if isinstance(value, BitableBackendKind):
        return value
    try:
        return BitableBackendKind(str(value))
    except ValueError as exc:
        raise ValueError("bitable backend must be 'base_v3' or 'bitable_v1'") from exc


def as_user_id_type(value: UserIDType | str) -> UserIDType:
    """Normalize and validate a user ID type without prefix guessing."""

    if isinstance(value, UserIDType):
        return value
    try:
        return UserIDType(str(value))
    except ValueError as exc:
        raise ValueError("user_id_type must be open_id, union_id, or user_id") from exc


def field_kind_from_type(raw_type: int | str | None) -> FieldKind:
    """Map v1 numeric or v3 string types to the canonical kind."""

    v1 = {
        1: FieldKind.TEXT,
        2: FieldKind.NUMBER,
        3: FieldKind.SELECT,
        4: FieldKind.SELECT,
        5: FieldKind.DATETIME,
        7: FieldKind.CHECKBOX,
        11: FieldKind.USER,
        13: FieldKind.TEXT,
        15: FieldKind.TEXT,
        17: FieldKind.ATTACHMENT,
        18: FieldKind.LINK,
        19: FieldKind.LOOKUP,
        20: FieldKind.FORMULA,
        21: FieldKind.LINK,
        22: FieldKind.LOCATION,
        23: FieldKind.GROUP_CHAT,
        24: FieldKind.UNSUPPORTED,
        1001: FieldKind.CREATED_AT,
        1002: FieldKind.UPDATED_AT,
        1003: FieldKind.CREATED_BY,
        1004: FieldKind.UPDATED_BY,
        1005: FieldKind.AUTO_NUMBER,
        3001: FieldKind.UNSUPPORTED,
    }
    if isinstance(raw_type, int) and not isinstance(raw_type, bool):
        return v1.get(raw_type, FieldKind.UNSUPPORTED)
    v3 = {
        "text": FieldKind.TEXT,
        "number": FieldKind.NUMBER,
        "select": FieldKind.SELECT,
        "datetime": FieldKind.DATETIME,
        "checkbox": FieldKind.CHECKBOX,
        "user": FieldKind.USER,
        "group_chat": FieldKind.GROUP_CHAT,
        "link": FieldKind.LINK,
        "location": FieldKind.LOCATION,
        "attachment": FieldKind.ATTACHMENT,
        "formula": FieldKind.FORMULA,
        "lookup": FieldKind.LOOKUP,
        "created_at": FieldKind.CREATED_AT,
        "updated_at": FieldKind.UPDATED_AT,
        "created_by": FieldKind.CREATED_BY,
        "updated_by": FieldKind.UPDATED_BY,
        "auto_number": FieldKind.AUTO_NUMBER,
        "not_support": FieldKind.UNSUPPORTED,
    }
    return v3.get(str(raw_type), FieldKind.UNSUPPORTED)


def field_is_writable(kind: FieldKind) -> bool:
    """Computed, unsupported, and system fields are never write targets."""

    return kind not in READ_ONLY_KINDS and kind is not FieldKind.UNSUPPORTED


__all__ = [
    "BitableBackend",
    "BitableBackendKind",
    "CanonicalRecord",
    "FieldKind",
    "FieldSchema",
    "IncompleteReadError",
    "MutationOutcome",
    "MutationReceipt",
    "READ_ONLY_KINDS",
    "ReadbackStatus",
    "RecordReadResult",
    "UserIDType",
    "as_backend_kind",
    "as_user_id_type",
    "field_is_writable",
    "field_kind_from_type",
]
