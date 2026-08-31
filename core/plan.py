"""Internal typed execution plans and public, non-replayable plan documents."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar, Mapping, Protocol, Sequence, TypeAlias

import pandas as pd

from api.bitable_backend import CanonicalRecord


class ActionUnit(str, Enum):
    FIELD = "field"
    RECORD = "record"
    ROW = "row"
    COLUMN = "column"
    RANGE = "range"


class VerificationPolicy(str, Enum):
    REQUIRED = "required"
    BEST_EFFORT = "best_effort"


class OutcomeStatus(str, Enum):
    SUCCESS = "success"
    NOOP = "noop"
    PARTIAL = "partial"
    FAILED = "failed"
    INDETERMINATE = "indeterminate"


class ErrorKind(str, Enum):
    VALIDATION = "validation"
    AUTH = "auth"
    RESOURCE = "resource"
    READ = "read"
    MUTATION = "mutation"
    VERIFICATION = "verification"
    STALE_SNAPSHOT = "stale_snapshot"
    INTERNAL = "internal"


@dataclass(frozen=True)
class SnapshotPrecondition:
    """Internal expected target state checked immediately before mutation."""

    kind: str
    expected: Mapping[str, Any] = field(default_factory=dict, repr=False)


@dataclass(frozen=True)
class PlanActionDocument:
    """Public description of one action; it contains no mutation payload."""

    kind: str
    count: int
    unit: ActionUnit
    scope: Mapping[str, Any] = field(default_factory=dict)
    destructive: bool = False
    clears_values: bool = False

    def __post_init__(self) -> None:
        if self.count < 0:
            raise ValueError("plan action count cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "count": self.count,
            "unit": self.unit.value,
            "scope": dict(self.scope),
            "destructive": self.destructive,
            "clears_values": self.clears_values,
        }


class TypedAction(Protocol):
    kind: ClassVar[str]
    unit: ClassVar[ActionUnit]
    scope: Mapping[str, Any]
    destructive: bool
    clears_values: bool
    precondition: SnapshotPrecondition | None
    verification_policy: VerificationPolicy

    @property
    def count(self) -> int: ...

    def to_public(self) -> PlanActionDocument: ...


@dataclass(frozen=True, kw_only=True)
class _ActionBase:
    scope: Mapping[str, Any] = field(default_factory=dict)
    destructive: bool = False
    clears_values: bool = False
    precondition: SnapshotPrecondition | None = field(
        default=None, repr=False, compare=False
    )
    verification_policy: VerificationPolicy = field(
        default=VerificationPolicy.REQUIRED, repr=False
    )

    kind: ClassVar[str]
    unit: ClassVar[ActionUnit]

    @property
    def count(self) -> int:
        raise NotImplementedError

    def to_public(self) -> PlanActionDocument:
        return PlanActionDocument(
            kind=self.kind,
            count=self.count,
            unit=self.unit,
            scope=dict(self.scope),
            destructive=self.destructive,
            clears_values=self.clears_values,
        )


@dataclass(frozen=True)
class CreateFieldAction(_ActionBase):
    field_name: str
    suggested_type: int

    kind: ClassVar[str] = "create_fields"
    unit: ClassVar[ActionUnit] = ActionUnit.FIELD

    @property
    def count(self) -> int:
        return 1


@dataclass(frozen=True)
class CreateRecordsAction(_ActionBase):
    records: tuple[CanonicalRecord, ...]

    kind: ClassVar[str] = "create_records"
    unit: ClassVar[ActionUnit] = ActionUnit.RECORD

    @property
    def count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class UpdateRecordsAction(_ActionBase):
    records: tuple[CanonicalRecord, ...]

    kind: ClassVar[str] = "update_records"
    unit: ClassVar[ActionUnit] = ActionUnit.RECORD

    @property
    def count(self) -> int:
        return len(self.records)


@dataclass(frozen=True)
class DeleteRecordsAction(_ActionBase):
    record_ids: tuple[str, ...]

    kind: ClassVar[str] = "delete_records"
    unit: ClassVar[ActionUnit] = ActionUnit.RECORD

    @property
    def count(self) -> int:
        return len(self.record_ids)


@dataclass(frozen=True)
class ClearRangeAction(_ActionBase):
    a1_range: str

    kind: ClassVar[str] = "clear_range"
    unit: ClassVar[ActionUnit] = ActionUnit.RANGE

    @property
    def count(self) -> int:
        return 1


@dataclass(frozen=True)
class WriteRangeAction(_ActionBase):
    values: tuple[tuple[Any, ...], ...] = field(repr=False, compare=False)

    kind: ClassVar[str] = "write_range"
    unit: ClassVar[ActionUnit] = ActionUnit.ROW

    @property
    def count(self) -> int:
        return len(self.values)


@dataclass(frozen=True)
class AppendRowsAction(_ActionBase):
    values: tuple[tuple[Any, ...], ...] = field(repr=False, compare=False)
    header_width: int

    kind: ClassVar[str] = "append_rows"
    unit: ClassVar[ActionUnit] = ActionUnit.ROW

    @property
    def count(self) -> int:
        return len(self.values)


@dataclass(frozen=True)
class WriteColumnsAction(_ActionBase):
    column_data: Mapping[str, tuple[Any, ...]] = field(repr=False, compare=False)
    column_positions: Mapping[str, int]
    start_row: int
    max_gap: int
    header_width: int

    kind: ClassVar[str] = "write_columns"
    unit: ClassVar[ActionUnit] = ActionUnit.COLUMN

    @property
    def count(self) -> int:
        return len(self.column_data)


@dataclass(frozen=True)
class ApplySheetConfigAction(_ActionBase):
    frame: pd.DataFrame = field(repr=False, compare=False)
    verification_policy: VerificationPolicy = field(
        default=VerificationPolicy.BEST_EFFORT, init=False, repr=False
    )

    kind: ClassVar[str] = "apply_sheet_config"
    unit: ClassVar[ActionUnit] = ActionUnit.COLUMN

    @property
    def count(self) -> int:
        return len(self.frame.columns)


ExecutionAction: TypeAlias = (
    CreateFieldAction
    | CreateRecordsAction
    | UpdateRecordsAction
    | DeleteRecordsAction
    | ClearRangeAction
    | WriteRangeAction
    | AppendRowsAction
    | WriteColumnsAction
    | ApplySheetConfigAction
)


@dataclass(frozen=True)
class PlanDocument:
    """Public, reviewable plan. It is deliberately not accepted by executors."""

    requested_mode: str
    effective_mode: str
    source: Mapping[str, Any]
    target: Mapping[str, Any]
    actions: Sequence[PlanActionDocument] = ()
    warnings: Sequence[str] = ()
    destructive: bool = False
    clears_values: bool = False
    config_sources: Mapping[str, str] = field(default_factory=dict)
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "requested_mode": self.requested_mode,
            "effective_mode": self.effective_mode,
            "source": dict(self.source),
            "target": dict(self.target),
            "actions": [action.to_dict() for action in self.actions],
            "warnings": list(self.warnings),
            "destructive": self.destructive,
            "clears_values": self.clears_values,
            "config_sources": dict(self.config_sources),
        }


@dataclass(frozen=True)
class ExecutionPlan:
    """Process-local plan containing typed mutation payloads and preconditions."""

    requested_mode: str
    effective_mode: str
    source: Mapping[str, Any]
    target: Mapping[str, Any]
    actions: Sequence[ExecutionAction] = ()
    warnings: Sequence[str] = ()
    destructive: bool = False
    clears_values: bool = False
    config_sources: Mapping[str, str] = field(default_factory=dict)

    def to_public(self) -> PlanDocument:
        return PlanDocument(
            requested_mode=self.requested_mode,
            effective_mode=self.effective_mode,
            source=dict(self.source),
            target=dict(self.target),
            actions=tuple(action.to_public() for action in self.actions),
            warnings=tuple(self.warnings),
            destructive=self.destructive,
            clears_values=self.clears_values,
            config_sources=dict(self.config_sources),
        )


@dataclass(frozen=True)
class SyncResult:
    """Execution result retaining only confirmed, public applied-prefix evidence."""

    status: OutcomeStatus
    plan: PlanDocument
    applied: Sequence[PlanActionDocument] = ()
    verification: Sequence[Mapping[str, Any]] = ()
    warnings: Sequence[str] = ()
    error: Mapping[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.status in {OutcomeStatus.SUCCESS, OutcomeStatus.NOOP}

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status.value,
            "ok": self.ok,
            "plan": self.plan.to_dict(),
            "applied": [action.to_dict() for action in self.applied],
            "verification": [dict(item) for item in self.verification],
            "warnings": list(self.warnings),
            "error": dict(self.error) if self.error is not None else None,
        }


__all__ = [
    "ActionUnit",
    "AppendRowsAction",
    "ApplySheetConfigAction",
    "ClearRangeAction",
    "CreateFieldAction",
    "CreateRecordsAction",
    "DeleteRecordsAction",
    "ErrorKind",
    "ExecutionAction",
    "ExecutionPlan",
    "OutcomeStatus",
    "PlanActionDocument",
    "PlanDocument",
    "SnapshotPrecondition",
    "SyncResult",
    "UpdateRecordsAction",
    "VerificationPolicy",
    "WriteColumnsAction",
    "WriteRangeAction",
]
