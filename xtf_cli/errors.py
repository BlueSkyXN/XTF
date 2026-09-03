"""Stable CLI error and exit-code contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

EXIT_OK = 0
EXIT_RUNTIME = 1
EXIT_USAGE = 2
EXIT_CONFIG = 3
EXIT_INPUT = EXIT_CONFIG
EXIT_AUTH = 4
EXIT_REMOTE = 5
EXIT_PARTIAL = 6
EXIT_MUTATION = 6
EXIT_VERIFICATION = 7
EXIT_INDETERMINATE = 8
EXIT_INTERRUPTED = 130


@dataclass
class CLIError(Exception):
    code: str
    message: str
    exit_code: int
    details: Mapping[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


class ParserSignal(Exception):
    """Internal non-error signal used by argparse help/version actions."""

    def __init__(self, status: int, message: str | None = None):
        self.status = status
        self.message = message
        super().__init__(message or "")
