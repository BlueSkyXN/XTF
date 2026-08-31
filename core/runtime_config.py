"""Nested immutable runtime configuration used by the XTF 2.0 service graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Union

from .config import MatchStrategy, SourceType, SyncConfig, SyncMode, TargetType


@dataclass(frozen=True)
class RuntimeIndexConfig:
    column: str | None
    datetime_granularity: str
    timezone: str | None


@dataclass(frozen=True)
class RuntimeSelectiveConfig:
    enabled: bool
    columns: tuple[str, ...]
    auto_include_index: bool
    optimize_ranges: bool
    max_gap_for_merge: int


@dataclass(frozen=True)
class RuntimeSyncConfig:
    mode: SyncMode
    match_strategy: MatchStrategy | None
    index: RuntimeIndexConfig
    selective: RuntimeSelectiveConfig


@dataclass(frozen=True)
class RuntimeSourceConfig:
    type: SourceType
    file_path: str | None = None
    app_token: str | None = field(default=None, repr=False)
    table_id: str | None = None


@dataclass(frozen=True)
class RuntimeBitableTarget:
    type: TargetType = field(default=TargetType.BITABLE, init=False)
    app_token: str = field(default="", repr=False)
    table_id: str = ""
    backend: str = "base_v3"
    user_id_type: str = "open_id"


@dataclass(frozen=True)
class RuntimeSheetTarget:
    type: TargetType = field(default=TargetType.SHEET, init=False)
    spreadsheet_token: str = field(default="", repr=False)
    sheet_id: str = ""
    start_row: int = 1
    start_column: str = "A"
    scan_max_rows: int = 5000
    scan_max_cols: int = 100
    write_max_rows: int = 5000
    write_max_cols: int = 100
    protect_formulas: bool = False
    verify_formulas: bool = False


RuntimeTargetConfig = Union[RuntimeBitableTarget, RuntimeSheetTarget]


@dataclass(frozen=True)
class RuntimeControlConfig:
    batch_size: int
    max_retries: int
    rate_limit_delay: float
    verify_remote_writes: bool
    advanced: bool


@dataclass(frozen=True)
class RuntimeConfig:
    """Read-only configuration graph consumed after CLI/YAML resolution."""

    app_id: str
    app_secret: str = field(repr=False)
    source: RuntimeSourceConfig
    target: RuntimeTargetConfig
    sync: RuntimeSyncConfig
    control: RuntimeControlConfig
    field_type_strategy: str

    @classmethod
    def from_sync_config(cls, config: SyncConfig) -> "RuntimeConfig":
        selective = config.selective_sync
        sync = RuntimeSyncConfig(
            mode=config.sync_mode,
            match_strategy=(
                None if config.sync_mode is SyncMode.CLONE else config.match_strategy
            ),
            index=RuntimeIndexConfig(
                column=config.index_column,
                datetime_granularity=config.datetime_index_granularity,
                timezone=config.datetime_index_timezone,
            ),
            selective=RuntimeSelectiveConfig(
                enabled=selective.enabled,
                columns=tuple(selective.columns or ()),
                auto_include_index=selective.auto_include_index,
                optimize_ranges=selective.optimize_ranges,
                max_gap_for_merge=selective.max_gap_for_merge,
            ),
        )
        source = RuntimeSourceConfig(
            type=config.source_type,
            file_path=config.file_path,
            app_token=config.source_app_token,
            table_id=config.source_table_id,
        )
        target: RuntimeTargetConfig
        if config.target_type is TargetType.BITABLE:
            target = RuntimeBitableTarget(
                app_token=config.app_token or "",
                table_id=config.table_id or "",
                backend=config.bitable_api_backend,
                user_id_type=config.bitable_user_id_type,
            )
        else:
            target = RuntimeSheetTarget(
                spreadsheet_token=config.spreadsheet_token or "",
                sheet_id=config.sheet_id or "",
                start_row=config.start_row,
                start_column=config.start_column,
                scan_max_rows=config.sheet_scan_max_rows,
                scan_max_cols=config.sheet_scan_max_cols,
                write_max_rows=config.sheet_write_max_rows,
                write_max_cols=config.sheet_write_max_cols,
                protect_formulas=config.sheet_protect_formulas,
                verify_formulas=config.sheet_verify_formulas,
            )
        return cls(
            app_id=config.app_id,
            app_secret=config.app_secret,
            source=source,
            target=target,
            sync=sync,
            control=RuntimeControlConfig(
                batch_size=config.batch_size,
                max_retries=config.max_retries,
                rate_limit_delay=config.rate_limit_delay,
                verify_remote_writes=config.verify_remote_writes,
                advanced=config.enable_advanced_control,
            ),
            field_type_strategy=config.field_type_strategy.value,
        )


__all__ = [
    "RuntimeBitableTarget",
    "RuntimeConfig",
    "RuntimeControlConfig",
    "RuntimeIndexConfig",
    "RuntimeSelectiveConfig",
    "RuntimeSheetTarget",
    "RuntimeSourceConfig",
    "RuntimeSyncConfig",
    "RuntimeTargetConfig",
]
