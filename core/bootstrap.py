"""Explicit runtime dependency assembly for XTF 2.0."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
import sys
from typing import Union

from api.auth import FeishuAuth
from api.base import RateLimiter, RetryableAPIClient
from api.bitable_backend import BitableBackend, BitableBackendKind
from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from api.sheet import SheetAPI

from .control import RequestController, build_request_controller
from .runtime_config import RuntimeBitableTarget, RuntimeConfig, RuntimeSheetTarget


@dataclass(frozen=True)
class RuntimeDependencies:
    """One isolated dependency graph; no process-global controller is consulted."""

    logger: logging.Logger
    controller: RequestController | None
    transport: RetryableAPIClient
    auth: FeishuAuth
    target: Union[BitableBackend, SheetAPI]


class _SensitiveValueFilter(logging.Filter):
    def __init__(self, values: tuple[str, ...]):
        super().__init__()
        self._values = tuple(sorted(set(values), key=len, reverse=True))

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        for value in self._values:
            message = message.replace(value, "<redacted>")
        record.msg = message
        record.args = ()
        return True


def _sensitive_values(config: RuntimeConfig) -> tuple[str, ...]:
    values = [config.auth.app_secret]
    if config.source.app_token:
        values.append(config.source.app_token)
    if isinstance(config.target, RuntimeBitableTarget):
        values.append(config.target.app_token)
    else:
        values.append(config.target.spreadsheet_token)
    return tuple(value for value in values if value)


def _configure_runtime_logger(config: RuntimeConfig) -> logging.Logger:
    """Create the process-local XTF handlers used by this runtime."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    target_name = (
        "bitable" if isinstance(config.target, RuntimeBitableTarget) else "sheet"
    )
    log_file = log_dir / f"xtf_{target_name}_{datetime.now():%Y%m%d_%H%M%S}.log"

    root = logging.getLogger("XTF")
    for handler in tuple(root.handlers):
        root.removeHandler(handler)
        handler.close()

    level = getattr(logging, config.output.log_level, logging.INFO)
    root.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    sensitive_filter = _SensitiveValueFilter(_sensitive_values(config))
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.addFilter(sensitive_filter)
    root.addHandler(file_handler)
    # CLI runtime captures stdout, redacts configured secrets, then relays logs to stderr.
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.addFilter(sensitive_filter)
    root.addHandler(console_handler)
    root.propagate = False
    return logging.getLogger("XTF.service")


def _controller_from_config(config: RuntimeConfig) -> RequestController | None:
    control = config.control
    if not control.advanced_enabled:
        return None
    return build_request_controller(
        retry_type=control.retry.strategy,
        retry_config={
            "initial_delay": control.retry.initial_delay,
            "max_retries": control.max_retries,
            "max_wait_time": control.retry.max_wait_time,
            "multiplier": control.retry.multiplier,
            "increment": control.retry.increment,
        },
        rate_limit_type=control.rate_limit.strategy,
        rate_limit_config={
            "delay": control.rate_limit_delay,
            "window_size": control.rate_limit.window_size,
            "max_requests": control.rate_limit.max_requests,
        },
    )


def bootstrap_runtime(config: RuntimeConfig) -> RuntimeDependencies:
    """Assemble logger, controller, transport, auth, and one target client."""
    logger = _configure_runtime_logger(config)
    controller = _controller_from_config(config)
    transport = RetryableAPIClient(
        max_retries=config.control.max_retries,
        rate_limiter=RateLimiter(config.control.rate_limit_delay),
        controller=controller,
    )
    auth = FeishuAuth(config.auth.app_id, config.auth.app_secret, api_client=transport)
    target: Union[BitableBackend, SheetAPI]
    if isinstance(config.target, RuntimeBitableTarget):
        backend = BitableBackendKind(config.target.backend)
        if backend is BitableBackendKind.BASE_V3:
            target = BaseV3Backend(
                auth,
                transport,
                user_id_type=config.target.user_id_type,
            )
        else:
            target = BitableV1Backend(
                auth,
                transport,
                user_id_type=config.target.user_id_type,
            )
    else:
        sheet = config.target
        assert isinstance(sheet, RuntimeSheetTarget)
        target = SheetAPI(
            auth,
            transport,
            start_row=sheet.start_row,
            start_column=sheet.start_column,
            scan_max_rows=sheet.scan_max_rows,
            scan_max_cols=sheet.scan_max_cols,
            write_max_rows=sheet.write_max_rows,
            write_max_cols=sheet.write_max_cols,
            value_render_option=sheet.value_render_option,
            datetime_render_option=sheet.datetime_render_option,
        )
    return RuntimeDependencies(logger, controller, transport, auth, target)


__all__ = ["RuntimeDependencies", "bootstrap_runtime"]
