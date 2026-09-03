#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XTF 2.0 内部 Feishu typed transport、backend 与 Sheet contracts。"""

from .auth import FeishuAuth
from .base import RateLimiter, RetryableAPIClient
from .bitable_backend import (
    BitableBackend,
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    IncompleteReadError,
    MutationOutcome,
    MutationReceipt,
    ReadbackStatus,
    RecordReadResult,
    UserIDType,
)
from .bitable_v1 import BitableV1Backend
from .bitable_v3 import BaseV3Backend
from .sheet import (
    A1Range,
    FormulaVerificationResult,
    RangeChunk,
    RangeChunker,
    SheetAPI,
    SheetMetadata,
)
from .sdk import (
    FeishuAPIError,
    FeishuResponseParser,
    Page,
    PaginationError,
    Paginator,
    PartialBatchError,
    run_batches,
)

__all__ = [
    "FeishuAuth",
    "RateLimiter",
    "RetryableAPIClient",
    "BitableBackend",
    "BitableBackendKind",
    "BitableV1Backend",
    "BaseV3Backend",
    "CanonicalRecord",
    "FieldKind",
    "FieldSchema",
    "IncompleteReadError",
    "MutationOutcome",
    "MutationReceipt",
    "ReadbackStatus",
    "RecordReadResult",
    "UserIDType",
    "A1Range",
    "FormulaVerificationResult",
    "RangeChunk",
    "RangeChunker",
    "SheetAPI",
    "SheetMetadata",
    "FeishuAPIError",
    "FeishuResponseParser",
    "Page",
    "PaginationError",
    "Paginator",
    "PartialBatchError",
    "run_batches",
]
