"""Serializable synchronization plans and execution outcomes.

The public dictionaries deliberately describe *what* will happen without
including record values, worksheet matrices, credentials, or other mutation
payloads.  Payloads stay process-local on :class:`PlanAction` and are consumed
only by ``XTFSyncEngine.execute_plan``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence

ACTION_KINDS = frozenset(
    {
        "create_fields",
        "create_records",
        "update_records",
        "delete_records",
        "clear_range",
        "write_range",
        "append_rows",
        "write_columns",
        "apply_sheet_config",
    }
)


class OutcomeStatus(str, Enum):
    """Terminal status for one plan execution."""

    SUCCESS = "success"
    NOOP = "noop"
    PARTIAL = "partial"
    FAILED = "failed"


class ErrorKind(str, Enum):
    """Stable, high-level execution error categories."""

    VALIDATION = "validation"
    AUTH = "auth"
    RESOURCE = "resource"
    READ = "read"
    MUTATION = "mutation"
    VERIFICATION = "verification"
    INTERNAL = "internal"


@dataclass(frozen=True)
class PlanAction:
    """One ordered, reviewable unit of remote work.

    ``payload`` is intentionally excluded from ``to_dict`` and from repr/equality.
    It may contain canonical records, Sheet matrices, or local DataFrames needed
    to execute the action, but it is never part of the serialized plan contract.
    """

    kind: str
    count: int = 0
    scope: Mapping[str, Any] = field(default_factory=dict)
    destructive: bool = False
    clears_values: bool = False
    payload: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.kind not in ACTION_KINDS:
            raise ValueError(f"unsupported plan action kind: {self.kind}")
        if self.count < 0:
            raise ValueError("plan action count cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "count": self.count,
            "scope": dict(self.scope),
            "destructive": self.destructive,
            "clears_values": self.clears_values,
        }


@dataclass(frozen=True)
class SyncPlan:
    """A complete ordered plan produced without remote mutations."""

    requested_mode: str
    effective_mode: str
    source: Mapping[str, Any]
    target: Mapping[str, Any]
    actions: Sequence[PlanAction] = ()
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
class SyncOutcome:
    """Execution result retaining the successfully applied action prefix."""

    status: OutcomeStatus
    plan: SyncPlan
    applied: Sequence[PlanAction] = ()
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
