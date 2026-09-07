"""XTF 2.0 internal typed synchronization components.

The stable product contract is the CLI, YAML v2, JSON output, and exit codes.
These imports are package-internal conveniences, not a public Python SDK.
"""

from .config import FieldTypeStrategy, MatchStrategy, SourceType, SyncMode, TargetType
from .key_policy import CanonicalKey, KeyIndex, KeyPolicy
from .plan import (
    ActionUnit,
    ErrorKind,
    ExecutionPlan,
    OutcomeStatus,
    PlanDocument,
    SyncResult,
)
from .runtime_config import RuntimeConfig
from .service import SyncService

__all__ = [
    "ActionUnit",
    "CanonicalKey",
    "ErrorKind",
    "ExecutionPlan",
    "FieldTypeStrategy",
    "KeyIndex",
    "KeyPolicy",
    "MatchStrategy",
    "OutcomeStatus",
    "PlanDocument",
    "RuntimeConfig",
    "SourceType",
    "SyncMode",
    "SyncResult",
    "SyncService",
    "TargetType",
]
