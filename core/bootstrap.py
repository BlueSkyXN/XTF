"""Explicit runtime dependency assembly for XTF 2.0."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from api.auth import FeishuAuth
from api.base import RateLimiter, RetryableAPIClient
from api.bitable_backend import BitableBackend, BitableBackendKind
from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from api.sheet import SheetAPI

from .config import SyncConfig, TargetType
from .control import RequestController, build_request_controller


@dataclass(frozen=True)
class RuntimeDependencies:
    """One isolated dependency graph; no process-global controller is consulted."""

    controller: RequestController | None
    transport: RetryableAPIClient
    auth: FeishuAuth
    target: Union[BitableBackend, SheetAPI]


def _controller_from_config(config: SyncConfig) -> RequestController | None:
    if not config.enable_advanced_control:
        return None
    return build_request_controller(
        retry_type=config.retry_strategy_type,
        retry_config={
            "initial_delay": config.retry_initial_delay,
            "max_retries": config.max_retries,
            "max_wait_time": config.retry_max_wait_time,
            "multiplier": config.retry_multiplier,
            "increment": config.retry_increment,
        },
        rate_limit_type=config.rate_limit_strategy_type,
        rate_limit_config={
            "delay": config.rate_limit_delay,
            "window_size": config.rate_limit_window_size,
            "max_requests": config.rate_limit_max_requests,
        },
    )


def bootstrap_runtime(config: SyncConfig) -> RuntimeDependencies:
    """Assemble controller, transport, auth, and exactly one target client."""
    controller = _controller_from_config(config)
    transport = RetryableAPIClient(
        max_retries=config.max_retries,
        rate_limiter=RateLimiter(config.rate_limit_delay),
        controller=controller,
    )
    auth = FeishuAuth(config.app_id, config.app_secret, api_client=transport)
    target: Union[BitableBackend, SheetAPI]
    if config.target_type is TargetType.BITABLE:
        backend = BitableBackendKind(config.bitable_api_backend)
        if backend is BitableBackendKind.BASE_V3:
            target = BaseV3Backend(
                auth,
                transport,
                user_id_type=config.bitable_user_id_type,
            )
        else:
            target = BitableV1Backend(
                auth,
                transport,
                user_id_type=config.bitable_user_id_type,
            )
    else:
        target = SheetAPI(
            auth,
            transport,
            start_row=config.start_row,
            start_column=config.start_column,
            scan_max_rows=config.sheet_scan_max_rows,
            scan_max_cols=config.sheet_scan_max_cols,
            write_max_rows=config.sheet_write_max_rows,
            write_max_cols=config.sheet_write_max_cols,
            value_render_option=config.sheet_value_render_option,
            datetime_render_option=config.sheet_datetime_render_option,
        )
    return RuntimeDependencies(controller, transport, auth, target)


__all__ = ["RuntimeDependencies", "bootstrap_runtime"]
