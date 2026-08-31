"""Nested immutable runtime configuration for the XTF 2.0 service graph."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Union
from zoneinfo import ZoneInfo

from .config import FieldTypeStrategy, MatchStrategy, SourceType, SyncMode, TargetType


@dataclass(frozen=True)
class RuntimeAuthConfig:
    app_id: str
    app_secret: str = field(repr=False)


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
    preserve_column_order: bool


@dataclass(frozen=True)
class RuntimeSyncConfig:
    mode: SyncMode
    match_strategy: MatchStrategy | None
    index: RuntimeIndexConfig
    selective: RuntimeSelectiveConfig
    verify_remote_writes: bool


@dataclass(frozen=True)
class RuntimeSourceConfig:
    type: SourceType
    file_path: str | None = None
    excel_sheet_name: str | int | None = None
    app_token: str | None = field(default=None, repr=False)
    table_id: str | None = None


@dataclass(frozen=True)
class RuntimeBitableTarget:
    type: TargetType = field(default=TargetType.BITABLE, init=False)
    app_token: str = field(default="", repr=False)
    table_id: str = ""
    create_missing_fields: bool = True
    backend: str = "base_v3"
    user_id_type: str = "open_id"


@dataclass(frozen=True)
class RuntimeSheetTarget:
    type: TargetType = field(default=TargetType.SHEET, init=False)
    spreadsheet_token: str = field(default="", repr=False)
    sheet_id: str = ""
    start_row: int = 1
    start_column: str = "A"
    value_render_option: str | None = None
    datetime_render_option: str | None = None
    scan_max_rows: int = 5000
    scan_max_cols: int = 100
    write_max_rows: int = 5000
    write_max_cols: int = 100
    validate_results: bool = False
    protect_formulas: bool = False
    verify_formulas: bool = False
    formula_max_locations: int = 20
    report_column_diff: bool = False
    diff_tolerance: float = 0.001


RuntimeTargetConfig = Union[RuntimeBitableTarget, RuntimeSheetTarget]


@dataclass(frozen=True)
class RuntimeConversionConfig:
    strategy: FieldTypeStrategy
    intelligence_date_confidence: float
    intelligence_choice_confidence: float
    intelligence_boolean_confidence: float


@dataclass(frozen=True)
class RuntimeRetryConfig:
    strategy: str
    initial_delay: float
    max_wait_time: float | None
    multiplier: float
    increment: float


@dataclass(frozen=True)
class RuntimeRateLimitConfig:
    strategy: str
    window_size: float
    max_requests: int


@dataclass(frozen=True)
class RuntimeControlConfig:
    batch_size: int
    max_retries: int
    rate_limit_delay: float
    advanced_enabled: bool
    retry: RuntimeRetryConfig
    rate_limit: RuntimeRateLimitConfig


@dataclass(frozen=True)
class RuntimeOutputConfig:
    log_level: str


@dataclass(frozen=True)
class RuntimeConfig:
    """Read-only configuration graph consumed by bootstrap and SyncService."""

    auth: RuntimeAuthConfig
    source: RuntimeSourceConfig
    target: RuntimeTargetConfig
    sync: RuntimeSyncConfig
    conversion: RuntimeConversionConfig
    control: RuntimeControlConfig
    output: RuntimeOutputConfig
    config_sources: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        self._validate_auth_and_targets()
        self._validate_sync()
        self._validate_controls()

    @staticmethod
    def flat_defaults(target_type: str | TargetType) -> dict[str, Any]:
        target = (
            target_type
            if isinstance(target_type, TargetType)
            else TargetType(str(target_type))
        )
        values: dict[str, Any] = {
            "file_path": None,
            "app_id": "",
            "app_secret": "",
            "target_type": target.value,
            "source_type": SourceType.FILE.value,
            "source_app_token": None,
            "source_table_id": None,
            "excel_sheet_name": None,
            "app_token": None,
            "table_id": None,
            "create_missing_fields": True,
            "bitable_api_backend": "base_v3",
            "bitable_user_id_type": "open_id",
            "field_type_strategy": FieldTypeStrategy.BASE.value,
            "intelligence_date_confidence": 0.85,
            "intelligence_choice_confidence": 0.9,
            "intelligence_boolean_confidence": 0.95,
            "spreadsheet_token": None,
            "sheet_id": None,
            "start_row": 1,
            "start_column": "A",
            "sheet_value_render_option": None,
            "sheet_datetime_render_option": None,
            "sheet_scan_max_rows": 5000,
            "sheet_scan_max_cols": 100,
            "sheet_write_max_rows": 5000,
            "sheet_write_max_cols": 100,
            "sheet_validate_results": False,
            "sheet_protect_formulas": False,
            "sheet_verify_formulas": False,
            "sheet_formula_max_locations": 20,
            "sheet_report_column_diff": False,
            "sheet_diff_tolerance": 0.001,
            "sync_mode": SyncMode.FULL.value,
            "match_strategy": MatchStrategy.BY_KEY.value,
            "index_column": None,
            "datetime_index_granularity": "exact",
            "datetime_index_timezone": None,
            "verify_remote_writes": False,
            "batch_size": 500 if target is TargetType.BITABLE else 1000,
            "rate_limit_delay": 0.01 if target is TargetType.BITABLE else 0.1,
            "max_retries": 3,
            "enable_advanced_control": False,
            "retry_strategy_type": "exponential_backoff",
            "retry_initial_delay": 0.5,
            "retry_max_wait_time": None,
            "retry_multiplier": 2.0,
            "retry_increment": 0.5,
            "rate_limit_strategy_type": "fixed_wait",
            "rate_limit_window_size": 1.0,
            "rate_limit_max_requests": 10,
            "log_level": "INFO",
            "selective_sync.enabled": False,
            "selective_sync.columns": [],
            "selective_sync.auto_include_index": True,
            "selective_sync.optimize_ranges": True,
            "selective_sync.max_gap_for_merge": 2,
            "selective_sync.preserve_column_order": True,
        }
        return values

    @classmethod
    def from_flat(
        cls,
        values: Mapping[str, Any],
        *,
        sources: Mapping[str, str] | None = None,
    ) -> "RuntimeConfig":
        target_type = _enum(TargetType, values.get("target_type"), "target_type")
        source_type = _enum(
            SourceType, values.get("source_type", SourceType.FILE.value), "source_type"
        )
        mode = _enum(
            SyncMode, values.get("sync_mode", SyncMode.FULL.value), "sync_mode"
        )
        raw_strategy = values.get("match_strategy")
        if mode is SyncMode.CLONE:
            if raw_strategy not in (None, ""):
                raise ValueError("clone 模式必须省略 match_strategy")
            match_strategy = None
        else:
            match_strategy = _enum(MatchStrategy, raw_strategy, "match_strategy")
        field_strategy = _enum(
            FieldTypeStrategy,
            values.get("field_type_strategy", FieldTypeStrategy.BASE.value),
            "field_type_strategy",
        )

        selective_raw = values.get("selective_sync", {})
        if not isinstance(selective_raw, Mapping):
            raise ValueError("selective_sync 必须是对象")
        raw_columns = selective_raw.get("columns", ())
        if raw_columns is None:
            raw_columns = ()
        if not isinstance(raw_columns, (list, tuple)):
            raise ValueError("selective_sync.columns 必须是列表类型")
        columns: list[str] = []
        for index, column in enumerate(raw_columns):
            if not isinstance(column, str) or not column.strip():
                raise ValueError(f"selective_sync.columns[{index}] 必须是非空字符串")
            columns.append(column.strip())
        if len(columns) != len(set(columns)):
            raise ValueError("selective_sync.columns 包含重复的列名")

        granularity = (
            str(values.get("datetime_index_granularity", "exact")).strip().lower()
        )
        raw_timezone = values.get("datetime_index_timezone")
        timezone = str(raw_timezone).strip() if raw_timezone not in (None, "") else None

        value_render = _normalize_render(
            values.get("sheet_value_render_option"),
            {
                "tostring": "ToString",
                "formula": "Formula",
                "formattedvalue": "FormattedValue",
                "unformattedvalue": "UnformattedValue",
            },
        )
        datetime_render = _normalize_render(
            values.get("sheet_datetime_render_option"),
            {"formattedstring": "FormattedString"},
        )
        protect_formulas = bool(values.get("sheet_protect_formulas", False))

        source = RuntimeSourceConfig(
            type=source_type,
            file_path=_optional_string(values.get("file_path")),
            excel_sheet_name=values.get("excel_sheet_name"),
            app_token=_optional_string(values.get("source_app_token")),
            table_id=_optional_string(values.get("source_table_id")),
        )
        target: RuntimeTargetConfig
        if target_type is TargetType.BITABLE:
            target = RuntimeBitableTarget(
                app_token=_optional_string(values.get("app_token")) or "",
                table_id=_optional_string(values.get("table_id")) or "",
                create_missing_fields=bool(values.get("create_missing_fields", True)),
                backend=str(values.get("bitable_api_backend", "base_v3"))
                .strip()
                .lower(),
                user_id_type=str(values.get("bitable_user_id_type", "open_id"))
                .strip()
                .lower(),
            )
        else:
            target = RuntimeSheetTarget(
                spreadsheet_token=(
                    _optional_string(values.get("spreadsheet_token")) or ""
                ),
                sheet_id=_optional_string(values.get("sheet_id")) or "",
                start_row=int(values.get("start_row", 1)),
                start_column=str(values.get("start_column", "A")).strip().upper(),
                value_render_option=value_render,
                datetime_render_option=datetime_render,
                scan_max_rows=int(values.get("sheet_scan_max_rows", 5000)),
                scan_max_cols=int(values.get("sheet_scan_max_cols", 100)),
                write_max_rows=int(values.get("sheet_write_max_rows", 5000)),
                write_max_cols=int(values.get("sheet_write_max_cols", 100)),
                validate_results=(
                    bool(values.get("sheet_validate_results", False))
                    or protect_formulas
                ),
                protect_formulas=protect_formulas,
                verify_formulas=bool(values.get("sheet_verify_formulas", False)),
                formula_max_locations=int(
                    values.get("sheet_formula_max_locations", 20)
                ),
                report_column_diff=bool(values.get("sheet_report_column_diff", False)),
                diff_tolerance=float(values.get("sheet_diff_tolerance", 0.001)),
            )

        return cls(
            auth=RuntimeAuthConfig(
                app_id=str(values.get("app_id", "")).strip(),
                app_secret=str(values.get("app_secret", "")),
            ),
            source=source,
            target=target,
            sync=RuntimeSyncConfig(
                mode=mode,
                match_strategy=match_strategy,
                index=RuntimeIndexConfig(
                    column=_optional_string(values.get("index_column")),
                    datetime_granularity=granularity,
                    timezone=timezone,
                ),
                selective=RuntimeSelectiveConfig(
                    enabled=bool(selective_raw.get("enabled", False)),
                    columns=tuple(columns),
                    auto_include_index=bool(
                        selective_raw.get("auto_include_index", True)
                    ),
                    optimize_ranges=bool(selective_raw.get("optimize_ranges", True)),
                    max_gap_for_merge=int(selective_raw.get("max_gap_for_merge", 2)),
                    preserve_column_order=bool(
                        selective_raw.get("preserve_column_order", True)
                    ),
                ),
                verify_remote_writes=bool(values.get("verify_remote_writes", False)),
            ),
            conversion=RuntimeConversionConfig(
                strategy=field_strategy,
                intelligence_date_confidence=float(
                    values.get("intelligence_date_confidence", 0.85)
                ),
                intelligence_choice_confidence=float(
                    values.get("intelligence_choice_confidence", 0.9)
                ),
                intelligence_boolean_confidence=float(
                    values.get("intelligence_boolean_confidence", 0.95)
                ),
            ),
            control=RuntimeControlConfig(
                batch_size=int(values.get("batch_size", 500)),
                max_retries=int(values.get("max_retries", 3)),
                rate_limit_delay=float(values.get("rate_limit_delay", 0.01)),
                advanced_enabled=bool(values.get("enable_advanced_control", False)),
                retry=RuntimeRetryConfig(
                    strategy=str(
                        values.get("retry_strategy_type", "exponential_backoff")
                    ),
                    initial_delay=float(values.get("retry_initial_delay", 0.5)),
                    max_wait_time=(
                        None
                        if values.get("retry_max_wait_time") is None
                        else float(values["retry_max_wait_time"])
                    ),
                    multiplier=float(values.get("retry_multiplier", 2.0)),
                    increment=float(values.get("retry_increment", 0.5)),
                ),
                rate_limit=RuntimeRateLimitConfig(
                    strategy=str(values.get("rate_limit_strategy_type", "fixed_wait")),
                    window_size=float(values.get("rate_limit_window_size", 1.0)),
                    max_requests=int(values.get("rate_limit_max_requests", 10)),
                ),
            ),
            output=RuntimeOutputConfig(
                log_level=str(values.get("log_level", "INFO")).strip().upper()
            ),
            config_sources=tuple(sorted((sources or {}).items())),
        )

    def config_source_map(self) -> dict[str, str]:
        return dict(self.config_sources)

    def _validate_auth_and_targets(self) -> None:
        if not self.auth.app_id:
            raise ValueError("auth.app_id or --app-id is required")
        if not self.auth.app_secret:
            raise ValueError(
                "app secret is required via --app-secret, XTF_APP_SECRET, or auth.app_secret"
            )
        if isinstance(self.target, RuntimeBitableTarget):
            if not self.target.app_token or not self.target.table_id:
                raise ValueError("多维表格模式需要app_token和table_id")
            if self.target.backend not in {"base_v3", "bitable_v1"}:
                raise ValueError(
                    "bitable_api_backend 仅支持 base_v3 或 bitable_v1，不支持自动回退"
                )
            if self.target.user_id_type not in {"open_id", "union_id", "user_id"}:
                raise ValueError(
                    "bitable_user_id_type 仅支持 open_id、union_id 或 user_id"
                )
        else:
            if not self.target.spreadsheet_token or not self.target.sheet_id:
                raise ValueError("电子表格模式需要spreadsheet_token和sheet_id")
            if self.target.start_row <= 0:
                raise ValueError("start_row 必须为正整数")
            if not self.target.start_column:
                raise ValueError("start_column 必须为非空列名")
            if (
                min(
                    self.target.scan_max_rows,
                    self.target.scan_max_cols,
                    self.target.write_max_rows,
                    self.target.write_max_cols,
                )
                <= 0
            ):
                raise ValueError("Sheet 读取/写入分块上限必须为正整数")
            if self.target.formula_max_locations <= 0:
                raise ValueError("sheet_formula_max_locations 必须为正整数")
            if self.target.diff_tolerance < 0:
                raise ValueError("sheet_diff_tolerance 不能为负数")

    def _validate_sync(self) -> None:
        index = self.sync.index
        selective = self.sync.selective
        if index.datetime_granularity not in {"exact", "day"}:
            raise ValueError("datetime_index_granularity 仅支持 exact 或 day")
        if index.datetime_granularity == "exact":
            if index.timezone is not None:
                raise ValueError("exact DATETIME key 不允许配置 timezone")
        else:
            if index.timezone is None:
                raise ValueError("day DATETIME key 必须配置 IANA timezone")
            try:
                ZoneInfo(index.timezone)
            except Exception as exc:
                raise ValueError(f"无效的 IANA timezone: {index.timezone}") from exc

        if self.sync.mode is SyncMode.CLONE:
            if self.sync.match_strategy is not None:
                raise ValueError("clone 模式必须省略 match_strategy")
        else:
            if self.sync.match_strategy is MatchStrategy.BY_KEY:
                if not index.column:
                    raise ValueError("by_key 模式必须配置 index_column")
            elif self.sync.match_strategy is MatchStrategy.APPEND_ONLY:
                if self.sync.mode is not SyncMode.INCREMENTAL:
                    raise ValueError("append_only 仅支持 incremental 模式")
                if index.column:
                    raise ValueError("append_only 不允许配置 index_column")
                if selective.enabled:
                    raise ValueError("append_only 不支持 selective 同步")
            else:
                raise ValueError("非 clone 模式必须显式配置 match_strategy")
        if (
            self.sync.mode in {SyncMode.FULL, SyncMode.OVERWRITE}
            and self.sync.match_strategy is not MatchStrategy.BY_KEY
        ):
            raise ValueError(f"{self.sync.mode.value} 只支持 by_key")

        if selective.enabled:
            if self.sync.mode is SyncMode.CLONE:
                raise ValueError("Clone 模式不支持 selective 同步")
            if not selective.columns:
                raise ValueError("启用 selective 同步时必须指定 columns")
            if not 0 <= selective.max_gap_for_merge <= 50:
                raise ValueError("selective_sync.max_gap_for_merge 必须在 0 到 50 之间")
            if (
                index.column
                and not selective.auto_include_index
                and index.column not in selective.columns
            ):
                raise ValueError(
                    "关闭 selective_sync.auto_include_index 时必须显式包含 index_column"
                )

        if self.source.type is SourceType.BITABLE:
            if not isinstance(self.target, RuntimeBitableTarget):
                raise ValueError("bitable 数据源仅支持 target_type=bitable")
            if not self.source.app_token or not self.source.table_id:
                raise ValueError(
                    "bitable 数据源需要 source_app_token 和 source_table_id"
                )
            if self.sync.match_strategy is not MatchStrategy.BY_KEY:
                raise ValueError("bitable 数据源仅支持 by_key")
            if self.sync.mode not in {SyncMode.FULL, SyncMode.INCREMENTAL}:
                raise ValueError("bitable 数据源仅支持 full 或 incremental 模式")
            if (
                self.source.app_token == self.target.app_token
                and self.source.table_id == self.target.table_id
            ):
                raise ValueError("源表和目标表不能是同一张多维表格")

        if isinstance(self.target, RuntimeSheetTarget):
            if self.target.protect_formulas and self.sync.mode is not SyncMode.FULL:
                raise ValueError("sheet_protect_formulas 仅支持 full 同步模式")
            if self.target.protect_formulas and not index.column:
                raise ValueError("sheet_protect_formulas 必须配置有效的 index_column")

    def _validate_controls(self) -> None:
        if self.control.batch_size <= 0:
            raise ValueError("batch_size 必须为正整数")
        if self.control.max_retries < 0:
            raise ValueError("max_retries 不能为负数")
        if self.control.rate_limit_delay < 0:
            raise ValueError("rate_limit_delay 不能为负数")
        retry = self.control.retry
        if retry.strategy not in {
            "exponential_backoff",
            "linear_growth",
            "fixed_wait",
        }:
            raise ValueError("retry_strategy_type 无效")
        if retry.initial_delay < 0:
            raise ValueError("retry_initial_delay 不能为负数")
        if retry.max_wait_time is not None and retry.max_wait_time <= 0:
            raise ValueError("retry_max_wait_time 必须为正数或 null")
        if retry.multiplier <= 0:
            raise ValueError("retry_multiplier 必须为正数")
        if retry.increment < 0:
            raise ValueError("retry_increment 不能为负数")
        rate_limit = self.control.rate_limit
        if rate_limit.strategy not in {
            "fixed_wait",
            "sliding_window",
            "fixed_window",
        }:
            raise ValueError("rate_limit_strategy_type 无效")
        if rate_limit.window_size <= 0:
            raise ValueError("rate_limit_window_size 必须为正数")
        if rate_limit.max_requests <= 0:
            raise ValueError("rate_limit_max_requests 必须为正整数")
        for name, confidence in (
            (
                "intelligence_date_confidence",
                self.conversion.intelligence_date_confidence,
            ),
            (
                "intelligence_choice_confidence",
                self.conversion.intelligence_choice_confidence,
            ),
            (
                "intelligence_boolean_confidence",
                self.conversion.intelligence_boolean_confidence,
            ),
        ):
            if not 0 <= confidence <= 1:
                raise ValueError(f"{name} 必须在 0 到 1 之间")


def _optional_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _enum(enum_type: type[Any], value: Any, name: str) -> Any:
    try:
        return enum_type(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} 无效: {value}") from exc


def _normalize_render(value: Any, mapping: Mapping[str, str]) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized or normalized.lower() == "none":
        return None
    canonical = mapping.get(normalized.lower(), normalized)
    if canonical not in set(mapping.values()):
        raise ValueError(f"无效的 Sheet render option: {value}")
    return canonical


__all__ = [
    "RuntimeAuthConfig",
    "RuntimeBitableTarget",
    "RuntimeConfig",
    "RuntimeControlConfig",
    "RuntimeConversionConfig",
    "RuntimeIndexConfig",
    "RuntimeOutputConfig",
    "RuntimeRateLimitConfig",
    "RuntimeRetryConfig",
    "RuntimeSelectiveConfig",
    "RuntimeSheetTarget",
    "RuntimeSourceConfig",
    "RuntimeSyncConfig",
    "RuntimeTargetConfig",
]
