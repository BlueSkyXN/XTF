#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一同步引擎模块

模块概述：
    此模块实现了 XTF 工具的核心同步逻辑，提供统一的同步引擎类
    XTFSyncEngine，支持多维表格（Bitable）和电子表格（Sheet）
    两种目标类型的数据同步。

主要功能：
    1. 统一的同步入口和流程控制
    2. 多维表格字段管理（自动创建缺失字段）
    3. 四种同步模式的具体实现
    4. 批量数据处理与分块上传
    5. 选择性列同步支持
    6. 日志系统配置和管理
    7. 全局请求控制器集成

核心类：
    XTFSyncEngine:
        统一同步引擎，根据配置的目标类型自动选择对应的 API 客户端
        和同步策略，执行数据同步操作。

同步模式说明：
    - full（全量同步）：
        对比索引列，已存在的记录更新，不存在的新增

    - incremental（增量同步）：
        仅新增本地有而远程没有的记录，跳过已存在记录

    - overwrite（覆盖同步）：
        先删除远程表中与本地数据索引匹配的记录，再新增全部本地数据

    - clone（克隆同步）：
        清空远程表全部数据，然后完整写入本地数据

同步流程：
    1. 初始化日志和 API 客户端
    2. 获取/创建远程表字段（Bitable）
    3. 获取远程现有数据
    4. 根据同步模式执行相应操作
    5. 批量处理数据（分块、重试、错误处理）
    6. 返回同步结果

依赖关系：
    内部模块：
        - core.config: 配置类（SyncConfig, SyncMode, TargetType）
        - core.converter: 数据转换（DataConverter）
        - api: API客户端（FeishuAuth, BitableAPI, SheetAPI等）
    外部依赖：
        - pandas: 数据处理
        - logging: 日志记录

性能优化：
    1. 批量操作减少 API 调用次数
    2. 预分块机制应对大数据量
    3. 智能重试和频控策略
    4. 选择性列同步减少数据传输

错误处理：
    - 三层数据上传保护机制
    - 自动二分重试（针对请求过大错误）
    - 详细的错误日志和状态反馈

使用示例：
    >>> from core.config import SyncConfig, TargetType
    >>> from core.engine import XTFSyncEngine
    >>>
    >>> config = SyncConfig(...)
    >>> engine = XTFSyncEngine(config)
    >>> success = engine.sync(dataframe)

注意事项：
    1. 同步前会自动设置日志，日志文件保存在 logs/ 目录
    2. Bitable 模式支持自动创建缺失字段
    3. 大数据量建议调整 batch_size 参数
    4. clone 模式会清空远程表，请谨慎使用

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import pandas as pd
import logging
import numbers
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Mapping, Union, Tuple, cast

from .config import MatchStrategy, SourceType, SyncConfig, SyncMode, TargetType
from .converter import DataConverter
from .plan import ErrorKind, OutcomeStatus, PlanAction, SyncOutcome, SyncPlan
from api import (
    A1Range,
    BitableBackend,
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    MutationOutcome,
    MutationReceipt,
    ReadbackStatus,
    PartialBatchError,
    SheetAPI,
    XTFFeishuClient,
    run_batches,
)


class XTFSyncEngine:
    """统一同步引擎 - 支持多维表格和电子表格"""

    def __init__(self, config: SyncConfig):
        """
        初始化同步引擎

        Args:
            config: 统一同步配置对象
        """
        self.config = config
        self._last_action_error_kind = ErrorKind.MUTATION
        self._last_action_applied_count = 0
        self._last_action_accepted_units = 0
        self._last_action_applied_rows: set[int] = set()
        self._last_action_mutation_complete = False
        self._last_action_remote_outcome: Optional[str] = None

        # 设置日志（必须先设置，因为其他初始化可能需要日志）
        self.setup_logging()
        self.logger = logging.getLogger("XTF.engine")

        # 初始化全局请求控制器（如果配置了高级重试和频控策略）
        self._init_global_controller()

        # 通过兼容式 SDK client 装配认证和目标 API；原 API 类保持不变。
        self.sdk = XTFFeishuClient(
            config.app_id,
            config.app_secret,
            max_retries=config.max_retries,
            rate_limit_delay=config.rate_limit_delay,
        )
        self.api_client = self.sdk.api_client
        self.auth = self.sdk.auth

        # 根据目标类型选择API客户端
        self.api: Union[BitableBackend, SheetAPI]
        if config.target_type == TargetType.BITABLE:
            self.api = self.sdk.bitable_backend(
                backend=config.bitable_api_backend,
                user_id_type=config.bitable_user_id_type,
            )
        else:  # SHEET
            self.api = self.sdk.sheet(
                start_row=self.config.start_row,
                start_column=self.config.start_column,
                scan_max_rows=self.config.sheet_scan_max_rows,
                scan_max_cols=self.config.sheet_scan_max_cols,
                write_max_rows=self.config.sheet_write_max_rows,
                write_max_cols=self.config.sheet_write_max_cols,
                value_render_option=self.config.sheet_value_render_option,
                datetime_render_option=self.config.sheet_datetime_render_option,
            )
        # 初始化数据转换器
        self.converter = DataConverter(
            config.target_type,
            datetime_index_granularity=config.datetime_index_granularity,
            datetime_index_timezone=config.datetime_index_timezone,
        )
        # 缓存工作表网格属性，避免重复请求
        self._sheet_grid_cache: Optional[Tuple[int, int]] = None
        self._sheet_grid_cache_key: Optional[Tuple[str, str]] = None
        self._sheet_read_complete = True

    def _init_global_controller(self):
        """初始化全局请求控制器"""
        try:
            from .config import ConfigManager

            # 使用配置管理器创建全局控制器
            global_controller = ConfigManager.create_request_controller(self.config)
            if global_controller:
                self.logger.info(
                    f"已初始化全局请求控制器 - 重试策略: {self.config.retry_strategy_type}, "
                    f"频控策略: {self.config.rate_limit_strategy_type}"
                )
            else:
                self.logger.info(
                    f"使用传统控制模式 - 重试次数: {self.config.max_retries}, "
                    f"频控间隔: {self.config.rate_limit_delay}s"
                )
        except Exception as e:
            self.logger.warning(f"初始化全局控制器失败，回退到传统模式: {e}")

    def setup_logging(self):
        """设置日志"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        target_name = (
            "bitable" if self.config.target_type == TargetType.BITABLE else "sheet"
        )
        log_file = (
            log_dir
            / f"xtf_{target_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        # 获取XTF专用的logger，避免全局污染
        xtf_logger = logging.getLogger("XTF")
        xtf_logger.handlers.clear()

        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        xtf_logger.setLevel(level)

        # 设置格式
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        xtf_logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        xtf_logger.addHandler(console_handler)

        # 防止传播到根logger
        xtf_logger.propagate = False

    # ========== 多维表格专用方法 ==========

    def _bitable_backend(self) -> BitableBackend:
        return cast(BitableBackend, self.api)

    def get_field_types(self) -> Dict[str, FieldSchema]:
        """获取多维表格字段类型映射"""
        if self.config.target_type != TargetType.BITABLE:
            return {}

        try:
            if not self.config.app_token or not self.config.table_id:
                self.logger.error("多维表格的 app_token 或 table_id 未配置")
                return {}
            backend = self._bitable_backend()
            existing_fields = backend.list_fields(
                self.config.app_token, self.config.table_id
            )
            field_types = {}
            for field in existing_fields:
                field_types[field.name] = field

            self.logger.debug(f"获取到 {len(field_types)} 个字段类型信息")
            return field_types

        except Exception as e:
            self.logger.warning(f"获取字段类型失败: {e}，将使用智能类型检测")
            return {}

    def plan_fields(
        self, df: pd.DataFrame
    ) -> Tuple[List[PlanAction], Dict[str, FieldSchema]]:
        """Read target schemas and plan missing fields without mutating Feishu."""
        if self.config.target_type is not TargetType.BITABLE:
            return [], {}
        if not self.config.app_token or not self.config.table_id:
            raise ValueError("多维表格的 app_token 或 table_id 未配置")

        from api.bitable_backend import field_is_writable, field_kind_from_type

        existing_fields = self._bitable_backend().list_fields(
            self.config.app_token, self.config.table_id
        )
        field_types = {field.name: field for field in existing_fields}
        missing_fields = [name for name in df.columns if name not in field_types]
        if missing_fields and not self.config.create_missing_fields:
            raise ValueError(
                "目标 Bitable 缺少字段且 create_missing_fields=false: "
                f"{[str(name) for name in missing_fields]}"
            )
        if not missing_fields:
            return [], field_types

        actions = []
        for raw_name in missing_fields:
            field_name = str(raw_name)
            analysis = self.converter.analyze_excel_column_data_enhanced(
                df,
                field_name,
                self.config.field_type_strategy.value,
                self.config,
            )
            suggested_type = int(analysis["suggested_feishu_type"])
            payload = {
                "field_name": field_name,
                "suggested_type": suggested_type,
            }
            actions.append(
                PlanAction(
                    kind="create_fields",
                    count=1,
                    scope={"target": "bitable", "field": field_name},
                    payload=payload,
                )
            )
            kind = field_kind_from_type(suggested_type)
            field_types[field_name] = FieldSchema(
                id=None,
                name=field_name,
                kind=kind,
                multiple=suggested_type == 4,
                writable=field_is_writable(kind),
                raw_type=suggested_type,
            )
        return actions, field_types

    def ensure_fields_exist(
        self, df: pd.DataFrame
    ) -> Tuple[bool, Dict[str, FieldSchema]]:
        """Compatibility helper: plan fields, execute them, then refresh schemas."""
        try:
            actions, field_types = self.plan_fields(df)
            if not actions:
                return True, field_types
            backend = self._bitable_backend()
            for action in actions:
                payload = cast(Mapping[str, Any], action.payload)
                receipt = backend.create_field(
                    cast(str, self.config.app_token),
                    cast(str, self.config.table_id),
                    str(payload["field_name"]),
                    cast(int, payload["suggested_type"]),
                )
                if receipt.outcome is not MutationOutcome.ACCEPTED:
                    return False, field_types
            refreshed = backend.list_fields(
                cast(str, self.config.app_token), cast(str, self.config.table_id)
            )
            return True, {field.name: field for field in refreshed}
        except Exception as error:
            self.logger.error(f"字段检查失败: {error}")
            return False, {}

    def get_all_bitable_records(
        self, field_names: Optional[List[str]] = None
    ) -> List[Dict]:
        """获取所有多维表格记录

        Args:
            field_names: 指定返回的字段名称列表，为None时返回全部字段。
                         用于减少不必要的数据传输，提升查询性能。
        """
        if not self.config.app_token or not self.config.table_id:
            self.logger.error("多维表格的 app_token 或 table_id 未配置")
            return []
        result = self._bitable_backend().list_records(
            self.config.app_token, self.config.table_id, field_names=field_names
        )
        if not result.complete:
            raise RuntimeError("多维表格读取不完整，拒绝继续同步")
        if result.ignored_fields:
            raise RuntimeError("多维表格读取存在 ignored_fields，拒绝继续同步")
        return [
            {"record_id": record.record_id, "fields": dict(record.fields)}
            for record in result.records
        ]

    @staticmethod
    def _bitable_copy_empty_value(schema: FieldSchema) -> Any:
        if schema.multiple or schema.kind in {
            FieldKind.SELECT,
            FieldKind.USER,
            FieldKind.GROUP_CHAT,
        }:
            return []
        return None

    def _bitable_copy_values_equal(self, source_value: Any, target_value: Any) -> bool:
        source_empty = self.converter._is_empty_value(source_value)
        target_empty = self.converter._is_empty_value(target_value)
        if source_empty or target_empty:
            return source_empty and target_empty

        def normalize(value: Any) -> Any:
            if isinstance(value, dict):
                return tuple(
                    sorted((str(key), normalize(item)) for key, item in value.items())
                )
            if isinstance(value, (list, tuple, set)):
                normalized = [normalize(item) for item in value]
                return tuple(sorted(normalized, key=repr))
            return value

        return normalize(source_value) == normalize(target_value)

    def _build_strict_bitable_index(
        self,
        records: tuple[CanonicalRecord, ...],
        schema: FieldSchema,
        *,
        source: bool,
    ) -> Dict[str, CanonicalRecord]:
        index: Dict[str, CanonicalRecord] = {}
        empty_count = 0
        type_code = self.converter._field_schema_type_code(schema)
        table_name = "源表" if source else "目标表"

        for position, record in enumerate(records, start=1):
            try:
                normalized = self.converter._normalize_index_value(
                    record.fields.get(schema.name),
                    type_code,
                    self.config.datetime_index_granularity,
                )
            except ValueError as error:
                raise RuntimeError(
                    f"{table_name}第 {position} 条记录的索引列 '{schema.name}' 无法安全归一化: {error}"
                ) from error
            if normalized is None:
                if source:
                    raise RuntimeError(
                        f"{table_name}第 {position} 条记录的索引列 '{schema.name}' 为空"
                    )
                empty_count += 1
                continue
            if normalized in index:
                raise RuntimeError(
                    f"{table_name}索引列 '{schema.name}' 存在重复值: {normalized}"
                )
            index[normalized] = record

        if empty_count:
            self.logger.warning(
                f"目标表有 {empty_count} 条记录未配置索引值；这些记录保持不变"
            )
        return index

    @staticmethod
    def _base_v3_write_shape(schema: FieldSchema) -> Mapping[str, Any]:
        """Return only Base v3 properties that affect mutation value shape."""
        shape_keys = {
            "multiple",
            "is_multiple",
            "ui_type",
            "value_type",
            "user_id_type",
            "id_type",
            "type",
        }
        return {
            key: schema.raw_properties[key]
            for key in sorted(shape_keys)
            if key in schema.raw_properties
        }

    @classmethod
    def _bitable_schemas_compatible(
        cls,
        source_schema: FieldSchema,
        target_schema: FieldSchema,
        backend_kind: BitableBackendKind,
    ) -> bool:
        forbidden = {FieldKind.LINK, FieldKind.ATTACHMENT}
        if (
            not source_schema.writable
            or not target_schema.writable
            or source_schema.kind in forbidden
            or target_schema.kind in forbidden
        ):
            return False
        if backend_kind is BitableBackendKind.BITABLE_V1:
            return (
                source_schema.raw_type == target_schema.raw_type
                and source_schema.multiple == target_schema.multiple
            )
        return (
            source_schema.kind is target_schema.kind
            and source_schema.multiple == target_schema.multiple
            and cls._base_v3_write_shape(source_schema)
            == cls._base_v3_write_shape(target_schema)
        )

    def _plan_bitable_source(self) -> SyncPlan:
        """Plan source-Bitable differences without mutating the target.

        ``full`` 只更新发生变化的字段并新增缺失记录；``incremental``
        只新增缺失记录。两种模式都不会删除目标表记录或复制 Base 结构。
        """
        if self.config.source_type is not SourceType.BITABLE:
            raise ValueError("sync_bitable_source 仅支持 source_type=bitable")
        if self.config.target_type is not TargetType.BITABLE:
            raise ValueError("远端多维表格数据源只能同步到多维表格")
        if not all(
            (
                self.config.source_app_token,
                self.config.source_table_id,
                self.config.app_token,
                self.config.table_id,
                self.config.index_column,
            )
        ):
            raise ValueError("源表、目标表和 index_column 配置不完整")

        source_app_token = cast(str, self.config.source_app_token)
        source_table_id = cast(str, self.config.source_table_id)
        target_app_token = cast(str, self.config.app_token)
        target_table_id = cast(str, self.config.table_id)
        backend = self._bitable_backend()
        source_fields = backend.list_fields(source_app_token, source_table_id)
        target_fields = backend.list_fields(target_app_token, target_table_id)
        source_by_name = {field.name: field for field in source_fields}
        target_by_name = {field.name: field for field in target_fields}

        requested_names: List[str]
        explicit_selection = bool(
            self.config.selective_sync.enabled and self.config.selective_sync.columns
        )
        if explicit_selection:
            requested_names = list(self.config.selective_sync.columns or [])
            unknown = [name for name in requested_names if name not in source_by_name]
            if unknown:
                raise ValueError(f"源表不存在 selective_sync 字段: {unknown}")
        else:
            requested_names = [field.name for field in source_fields]

        index_column = str(self.config.index_column)
        if index_column not in source_by_name:
            raise ValueError(f"源表不存在索引列 '{index_column}'")
        if index_column not in requested_names:
            requested_names.append(index_column)

        unsafe_kinds = {FieldKind.LINK, FieldKind.ATTACHMENT}
        skipped_fields: List[str] = []
        copy_names: List[str] = []
        for name in requested_names:
            schema = source_by_name[name]
            if not schema.writable or schema.kind in unsafe_kinds:
                if explicit_selection:
                    raise ValueError(f"字段 '{name}' 不支持跨表数据复制")
                skipped_fields.append(name)
                continue
            if name not in copy_names:
                copy_names.append(name)

        if skipped_fields:
            self.logger.info(
                f"跳过 {len(skipped_fields)} 个只读或需 ID 映射的字段: {skipped_fields}"
            )
        if index_column not in copy_names:
            raise ValueError(f"索引列 '{index_column}' 必须是可写的普通数据字段")

        missing_target = [name for name in copy_names if name not in target_by_name]
        if missing_target:
            raise ValueError(
                f"目标表缺少字段，远端表数据同步不会自动复制结构: {missing_target}"
            )

        incompatible: List[str] = []
        for name in copy_names:
            source_schema = source_by_name[name]
            target_schema = target_by_name[name]
            if not self._bitable_schemas_compatible(
                source_schema,
                target_schema,
                BitableBackendKind(self.config.bitable_api_backend),
            ):
                incompatible.append(name)
        if incompatible:
            raise ValueError(f"源表和目标表字段类型不兼容: {incompatible}")

        source_result = backend.list_records(
            source_app_token,
            source_table_id,
            field_names=copy_names,
        )
        target_projection = (
            copy_names if self.config.sync_mode is SyncMode.FULL else [index_column]
        )
        target_result = backend.list_records(
            target_app_token,
            target_table_id,
            field_names=target_projection,
        )
        for table_name, result in (
            ("源表", source_result),
            ("目标表", target_result),
        ):
            if not result.complete or result.ignored_fields or result.record_not_found:
                raise RuntimeError(f"{table_name}读取不完整，拒绝继续写入")

        index_schema = target_by_name[index_column]
        self._build_strict_bitable_index(
            source_result.records, index_schema, source=True
        )
        target_index = self._build_strict_bitable_index(
            target_result.records, index_schema, source=False
        )

        records_to_create: List[CanonicalRecord] = []
        records_to_update: List[CanonicalRecord] = []
        clears_values = False
        unchanged = 0
        type_code = self.converter._field_schema_type_code(index_schema)
        for source_record in source_result.records:
            normalized = self.converter._normalize_index_value(
                source_record.fields.get(index_column),
                type_code,
                self.config.datetime_index_granularity,
            )
            if normalized is None:
                raise ValueError(f"源表索引列 '{index_column}' 存在空值")
            target_record = target_index.get(normalized)
            if target_record is None:
                create_fields = {
                    name: source_record.fields.get(
                        name, self._bitable_copy_empty_value(target_by_name[name])
                    )
                    for name in copy_names
                }
                records_to_create.append(CanonicalRecord(None, create_fields))
                continue

            if self.config.sync_mode is SyncMode.INCREMENTAL:
                unchanged += 1
                continue

            changed_fields: Dict[str, Any] = {}
            for name in copy_names:
                if name == index_column:
                    continue
                schema = target_by_name[name]
                empty_value = self._bitable_copy_empty_value(schema)
                source_value = source_record.fields.get(name, empty_value)
                target_value = target_record.fields.get(name, empty_value)
                if not self._bitable_copy_values_equal(source_value, target_value):
                    changed_fields[name] = source_value
                    if self.converter._is_empty_value(
                        source_value
                    ) and not self.converter._is_empty_value(target_value):
                        clears_values = True
            if changed_fields:
                if not target_record.record_id:
                    raise RuntimeError("目标表记录缺少 record_id，拒绝更新")
                records_to_update.append(
                    CanonicalRecord(target_record.record_id, changed_fields)
                )
            else:
                unchanged += 1

        self.logger.info(
            "远端差异同步计划: "
            f"更新 {len(records_to_update)} 条，新增 {len(records_to_create)} 条，"
            f"跳过未变化/已存在 {unchanged} 条；目标表多余记录保持不变"
        )

        actions: List[PlanAction] = []
        if records_to_update:
            actions.append(
                PlanAction(
                    "update_records",
                    len(records_to_update),
                    {"target": "bitable"},
                    clears_values=clears_values,
                    payload=records_to_update,
                )
            )
        if records_to_create:
            actions.append(
                PlanAction(
                    "create_records",
                    len(records_to_create),
                    {"target": "bitable"},
                    payload=records_to_create,
                )
            )
        warnings = (
            [f"跳过 {len(skipped_fields)} 个不可复制字段"] if skipped_fields else []
        )
        if clears_values:
            warnings.append("full 同步将清空目标记录中的一个或多个字段值")
        return self._make_plan(
            requested_mode=self.config.sync_mode,
            effective_mode=self.config.sync_mode,
            source={"type": "bitable", "records": len(source_result.records)},
            target={"type": "bitable", "records": len(target_result.records)},
            actions=actions,
            warnings=warnings,
        )

    def sync_bitable_source(self) -> bool:
        """Compatibility wrapper around plan/execute for Bitable sources."""
        try:
            return self.execute_plan(self.plan()).ok
        except Exception as error:
            self.logger.error(f"远端多维表格同步计划或执行失败: {error}")
            return False

    def process_in_batches(
        self, items: List[Any], batch_size: int, processor_func, *args, **kwargs
    ) -> bool:
        """分批处理数据（多维表格模式）"""
        if self.config.target_type != TargetType.BITABLE:
            return False

        # 按接口上限自动限制批大小，避免超限请求
        max_batch_size = self._get_operation_max_batch_size(processor_func)
        effective_batch_size = batch_size
        if max_batch_size and batch_size > max_batch_size:
            self.logger.warning(
                f"{self._get_operation_type(processor_func)}批处理大小 {batch_size} 超过接口上限 {max_batch_size}，已自动降至 {max_batch_size}"
            )
            effective_batch_size = max_batch_size

        # 获取操作类型用于日志显示
        operation_type = self._get_operation_type(processor_func)
        total_batches = (len(items) + effective_batch_size - 1) // effective_batch_size

        def process(batch):
            return processor_func(*args, batch, **kwargs)

        try:
            results = run_batches(
                operation_type,
                items,
                effective_batch_size,
                process,
            )
        except PartialBatchError as error:
            self.logger.error(f"❌ {error}")
            return False

        self.logger.info(
            f"🎉 {operation_type}完成: {len(results)}/{total_batches} 个批次成功"
        )
        return True

    def process_typed_bitable_batches(
        self,
        items: List[Any],
        processor_func,
        *,
        receipt_callback=None,
    ) -> Tuple[bool, List[MutationReceipt]]:
        """按 backend 上限分块，保留 receipt 并在 partial/unknown 首错停止。"""
        max_batch_size = self._get_operation_max_batch_size(processor_func)
        effective_batch_size = min(
            self.config.batch_size,
            max_batch_size or self.config.batch_size,
        )
        receipts: List[MutationReceipt] = []
        for batch_index, start in enumerate(
            range(0, len(items), effective_batch_size), start=1
        ):
            batch = items[start : start + effective_batch_size]
            receipt = processor_func(
                self.config.app_token,
                self.config.table_id,
                batch,
            )
            if not isinstance(receipt, MutationReceipt):
                raise TypeError("typed backend mutation 必须返回 MutationReceipt")
            receipts.append(receipt)
            if receipt_callback is not None:
                receipt_callback(receipt)
            if (
                receipt.outcome is not MutationOutcome.ACCEPTED
                or receipt.accepted_count != len(batch)
                or receipt.ignored_fields
                or receipt.record_not_found
            ):
                self.logger.error(
                    f"第 {batch_index} 批结果为 {receipt.outcome.value}，"
                    "停止后续批次；已成功前缀不会回滚"
                )
                return False, receipts
        return True, receipts

    @staticmethod
    def _canonical_records(records: List[Dict[str, Any]]) -> List[CanonicalRecord]:
        return [
            CanonicalRecord(
                record_id=record.get("record_id"),
                fields=dict(record.get("fields", {})),
            )
            for record in records
        ]

    def _verify_bitable_mutation(
        self,
        operation: str,
        requested: List[CanonicalRecord] | List[str],
        receipts: List[MutationReceipt],
    ) -> bool:
        if not self.config.verify_remote_writes:
            return True
        if not receipts:
            return True

        if operation == "delete":
            record_ids = [str(item) for item in requested]
            if not record_ids:
                return True
            if not self.config.app_token or not self.config.table_id:
                return False
            backend = self._bitable_backend()
            result = backend.batch_get_records(
                self.config.app_token,
                self.config.table_id,
                record_ids,
            )
            missing = set(result.record_not_found)
            present = {record.record_id for record in result.records}
            verified = result.complete and not present and missing == set(record_ids)
            self._set_receipt_readback(
                receipts,
                ReadbackStatus.VERIFIED if verified else ReadbackStatus.MISMATCH,
                len(record_ids) if verified else 0,
            )
            return verified

        records = [item for item in requested if isinstance(item, CanonicalRecord)]
        record_ids = [record.record_id for record in records if record.record_id]
        if operation == "create" and not record_ids:
            record_ids = [
                record_id for receipt in receipts for record_id in receipt.record_ids
            ]
            if len(record_ids) != len(records):
                self.logger.error("创建响应没有完整 record IDs，无法证明写后读回范围")
                self._set_receipt_readback(receipts, ReadbackStatus.UNKNOWN, 0)
                return False
            records = [
                CanonicalRecord(record_id, record.fields)
                for record_id, record in zip(record_ids, records)
            ]
        if not record_ids:
            return False
        if not self.config.app_token or not self.config.table_id:
            return False
        backend = self._bitable_backend()
        observed = backend.batch_get_records(
            self.config.app_token,
            self.config.table_id,
            record_ids,
            field_names=tuple(
                dict.fromkeys(name for record in records for name in record.fields)
            ),
        )
        if (
            not observed.complete
            or observed.ignored_fields
            or observed.record_not_found
        ):
            self._set_receipt_readback(receipts, ReadbackStatus.INCOMPLETE, 0)
            return False
        schema_by_name = {field.name: field for field in observed.fields}
        by_id = {record.record_id: record for record in observed.records}
        for expected in records:
            actual = by_id.get(expected.record_id)
            if actual is None:
                self._set_receipt_readback(receipts, ReadbackStatus.MISMATCH, 0)
                return False
            for name, value in expected.fields.items():
                schema = schema_by_name.get(name)
                actual_value = actual.fields.get(name)
                type_code = self.converter._field_schema_type_code(schema)
                expected_normalized = self.converter._normalize_index_value(
                    value, type_code
                )
                actual_normalized = self.converter._normalize_index_value(
                    actual_value, type_code
                )
                if expected_normalized != actual_normalized:
                    self._set_receipt_readback(receipts, ReadbackStatus.MISMATCH, 0)
                    return False
        self._set_receipt_readback(receipts, ReadbackStatus.VERIFIED, len(records))
        return True

    @staticmethod
    def _set_receipt_readback(
        receipts: List[MutationReceipt],
        status: ReadbackStatus,
        verified_count: int,
    ) -> None:
        """Dataclass 保持 frozen；engine 用返回副本记录本次读回状态。"""
        from dataclasses import replace

        remaining = verified_count
        for index, receipt in enumerate(receipts):
            verified = min(receipt.accepted_count, remaining)
            receipts[index] = replace(
                receipt,
                verified_count=verified,
                readback=status,
            )
            remaining -= verified

    @staticmethod
    def _merge_sheet_formula_ranges(
        actual_ranges: List[A1Range], start_col: int, header_width: int
    ) -> List[str]:
        """Collapse successful write rows into formula-verification bands."""
        if start_col <= 0 or header_width <= 0:
            return []
        row_bands = sorted(
            {
                (item.start_row, item.end_row)
                for item in actual_ranges
                if item.start_row > 0 and item.end_row >= item.start_row
            }
        )
        if not row_bands:
            return []
        merged: List[List[int]] = []
        for start_row, end_row in row_bands:
            if merged and start_row <= merged[-1][1] + 1:
                merged[-1][1] = max(merged[-1][1], end_row)
            else:
                merged.append([start_row, end_row])
        left = SheetAPI.column_number_to_letter_static(start_col)
        right = SheetAPI.column_number_to_letter_static(start_col + header_width - 1)
        return [f"{left}{start}:{right}{end}" for start, end in merged]

    def _finalize_sheet_mutation(
        self,
        receipt: MutationReceipt,
        *,
        expected_ranges: Optional[Dict[str, List[List[Any]]]] = None,
        header_width: Optional[int] = None,
        verify_formulas: bool = True,
        skip_header_row: bool = False,
        skip_data_readback: bool = False,
    ) -> bool:
        """Apply receipt gates, optional data readback, and Sheet AI verification."""
        self._record_action_receipt(receipt)
        if receipt.outcome is not MutationOutcome.ACCEPTED:
            self.logger.error(
                f"Sheet {receipt.operation} 结果为 {receipt.outcome.value}；"
                "已成功前缀不会回滚，停止后续阶段"
            )
            return self._mark_action_failure(ErrorKind.MUTATION)
        if receipt.readback is ReadbackStatus.UNKNOWN:
            self.logger.error(
                f"Sheet {receipt.operation} 已接受但实际应用范围未知；"
                "停止后续阶段且不声称完整成功"
            )
            self._last_action_remote_outcome = MutationOutcome.UNKNOWN_OUTCOME.value
            return self._mark_action_failure(ErrorKind.MUTATION)

        actual_ranges = [
            item for item in receipt.actual_ranges if isinstance(item, A1Range)
        ]
        if self.config.verify_remote_writes and not skip_data_readback:
            if not expected_ranges:
                self.logger.error(
                    "Sheet 写后读回范围未知，无法证明 mutation 已完整应用"
                )
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            if not isinstance(self.api, SheetAPI) or not self.config.spreadsheet_token:
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            for range_text, expected in expected_ranges.items():
                try:
                    observed = self.api.get_sheet_data(
                        self.config.spreadsheet_token, range_text
                    )
                except Exception as error:
                    self.logger.error(f"Sheet 写后读回失败: {error}")
                    return self._mark_action_failure(ErrorKind.VERIFICATION)
                if observed != expected:
                    self.logger.error(f"Sheet 写后读回不一致: {range_text}")
                    return self._mark_action_failure(ErrorKind.VERIFICATION)

        if not self.config.sheet_verify_formulas or not verify_formulas:
            return True
        if not actual_ranges:
            self.logger.error("公式验证范围未知：mutation 未返回可证明的实际范围")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        if not isinstance(self.api, SheetAPI) or not self.config.spreadsheet_token:
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        width = header_width if header_width is not None else 0
        formula_ranges = actual_ranges
        if skip_header_row:
            formula_ranges = [
                A1Range(
                    item.sheet_id,
                    max(item.start_row, self.config.start_row + 1),
                    item.end_row,
                    item.start_col,
                    item.end_col,
                )
                for item in actual_ranges
                if item.end_row > self.config.start_row
            ]
        if not formula_ranges:
            self.logger.info("没有成功写入数据行，跳过公式验证")
            return True
        ranges = self._merge_sheet_formula_ranges(
            formula_ranges, self.api.start_col_num, width
        )
        if not ranges:
            self.logger.error("公式验证范围未知：无法证明表头宽度、起始列或实际行区间")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        try:
            result = self.api.verify_formulas(
                self.config.spreadsheet_token,
                [str(self.config.sheet_id)],
                ranges,
                max_locations_per_error=self.config.sheet_formula_max_locations,
            )
        except Exception as error:
            self.logger.error(f"Sheet AI 公式验证失败: {error}")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        if not result.passed:
            self.logger.error(
                f"Sheet AI 公式验证未通过: status={result.status}, "
                f"has_more={result.has_more}"
            )
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        return True

    def _typed_sheet_write(
        self, values: List[List[Any]], *, verify_formulas: bool = True
    ) -> bool:
        if (
            not values
            or not isinstance(self.api, SheetAPI)
            or not self.config.spreadsheet_token
            or not self.config.sheet_id
        ):
            return False
        end_row = self.config.start_row + len(values) - 1
        end_col = self.api.start_col_num + len(values[0]) - 1
        a1 = A1Range(
            str(self.config.sheet_id),
            self.config.start_row,
            end_row,
            self.api.start_col_num,
            end_col,
        )
        receipt = self.api.write_values(self.config.spreadsheet_token, a1.text, values)
        if receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        expected = {
            item.text: [
                row[item.start_col - a1.start_col : item.end_col - a1.start_col + 1]
                for row in values[
                    item.start_row - a1.start_row : item.end_row - a1.start_row + 1
                ]
            ]
            for item in receipt.actual_ranges
            if isinstance(item, A1Range)
        }
        return self._finalize_sheet_mutation(
            receipt,
            expected_ranges=expected,
            header_width=len(values[0]),
            verify_formulas=verify_formulas,
            skip_header_row=True,
        )

    def _typed_sheet_append(
        self, values: List[List[Any]], *, header_width: int
    ) -> bool:
        if (
            not values
            or not isinstance(self.api, SheetAPI)
            or not self.config.spreadsheet_token
            or not self.config.sheet_id
        ):
            return False
        placeholder_end_row = self.config.start_row + len(values) - 1
        end_col = self.api.start_col_num + len(values[0]) - 1
        requested = A1Range(
            str(self.config.sheet_id),
            self.config.start_row,
            placeholder_end_row,
            self.api.start_col_num,
            end_col,
        )
        receipt = self.api.append_values(
            self.config.spreadsheet_token, requested.text, values
        )
        if receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        expected: Dict[str, List[List[Any]]] = {}
        actual_ranges = [
            item for item in receipt.actual_ranges if isinstance(item, A1Range)
        ]
        if actual_ranges and sum(item.row_count for item in actual_ranges) == len(
            values
        ):
            offset = 0
            for item in actual_ranges:
                expected[item.text] = values[offset : offset + item.row_count]
                offset += item.row_count
        return self._finalize_sheet_mutation(
            receipt,
            expected_ranges=expected,
            header_width=header_width,
        )

    def _typed_sheet_batch_update(
        self,
        value_ranges: List[Dict[str, Any]],
        *,
        header_width: int,
        verify_formulas: bool = True,
        complete_action: bool = True,
    ) -> bool:
        if (
            not value_ranges
            or not isinstance(self.api, SheetAPI)
            or not self.config.spreadsheet_token
        ):
            return False
        receipt = self.api.batch_update_values(
            self.config.spreadsheet_token, value_ranges
        )
        if complete_action and receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        expected = (
            {
                str(item["range"]): [list(row) for row in item["values"]]
                for item in value_ranges
            }
            if self.config.verify_remote_writes
            else None
        )
        return self._finalize_sheet_mutation(
            receipt,
            expected_ranges=expected,
            header_width=header_width,
            verify_formulas=verify_formulas,
        )

    def _typed_sheet_selective_write(
        self,
        column_data: Dict[str, List[Any]],
        column_positions: Dict[str, int],
        *,
        start_row: int,
        max_gap: int,
        header_width: int,
    ) -> bool:
        if not isinstance(self.api, SheetAPI) or not self.config.sheet_id:
            return False
        optimized = self.api._optimize_column_ranges(
            column_data, column_positions, start_row, max_gap
        )
        value_ranges: List[Dict[str, Any]] = []
        for item in optimized:
            full_range = f"{self.config.sheet_id}!{item['range']}"
            a1 = A1Range.parse(full_range)
            values = [list(row) for row in item["values"]]
            for row_start in range(
                a1.start_row, a1.end_row + 1, self.api.write_max_rows
            ):
                row_end = min(row_start + self.api.write_max_rows - 1, a1.end_row)
                offset = row_start - a1.start_row
                chunk = values[offset : offset + row_end - row_start + 1]
                value_ranges.append(
                    {
                        "range": A1Range(
                            a1.sheet_id,
                            row_start,
                            row_end,
                            a1.start_col,
                            a1.end_col,
                        ).text,
                        "values": chunk,
                    }
                )
        for value_range in value_ranges:
            if not self._typed_sheet_batch_update(
                [value_range], header_width=header_width, complete_action=False
            ):
                return False
        self._last_action_mutation_complete = True
        return True

    def _typed_sheet_clear(self, range_str: str) -> bool:
        if (
            not isinstance(self.api, SheetAPI)
            or not self.config.spreadsheet_token
            or not self.config.sheet_id
        ):
            return False
        full_range = (
            range_str if "!" in range_str else f"{self.config.sheet_id}!{range_str}"
        )
        a1 = A1Range.parse(full_range)
        receipt = self.api.clear_values(self.config.spreadsheet_token, a1.text)
        if receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        if not self._finalize_sheet_mutation(
            receipt,
            expected_ranges=None,
            header_width=a1.col_count,
            verify_formulas=False,
            skip_data_readback=True,
        ):
            return False
        if not self.config.verify_remote_writes:
            return True
        try:
            observed = self.api.get_sheet_data(self.config.spreadsheet_token, a1.text)
        except Exception as error:
            self.logger.error(f"Sheet clear 写后读回失败: {error}")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        if any(
            cell is not None and str(cell).strip() != ""
            for row in observed
            for cell in row
        ):
            self.logger.error(f"Sheet clear 写后读回不一致: {a1.text}")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        return True

    def _get_operation_type(self, processor_func) -> str:
        """根据处理函数获取操作类型"""
        func_name = getattr(processor_func, "__name__", str(processor_func))
        if "create" in func_name:
            return "批量创建"
        elif "update" in func_name:
            return "批量更新"
        elif "delete" in func_name:
            return "批量删除"
        else:
            return "批量处理"

    def _get_operation_max_batch_size(self, processor_func) -> Optional[int]:
        """根据处理函数获取批量接口上限"""
        func_name = getattr(processor_func, "__name__", str(processor_func))
        if "create" in func_name:
            return getattr(self.api, "max_batch_create_size", None)
        if "update" in func_name:
            return getattr(self.api, "max_batch_update_size", None)
        if "delete" in func_name:
            return getattr(self.api, "max_batch_delete_size", None)
        return None

    # ========== 电子表格专用方法 ==========

    def _get_sheet_grid_properties(self) -> Optional[Tuple[int, int]]:
        """获取工作表网格属性（行数、列数）"""
        if self.config.target_type != TargetType.SHEET:
            return None
        if not isinstance(self.api, SheetAPI):
            return None
        if not self.config.spreadsheet_token or not self.config.sheet_id:
            return None
        cache_key = (self.config.spreadsheet_token, self.config.sheet_id)
        if self._sheet_grid_cache_key == cache_key and self._sheet_grid_cache:
            return self._sheet_grid_cache
        try:
            grid = self.api.get_sheet_grid_properties(
                self.config.spreadsheet_token, self.config.sheet_id
            )
            self._sheet_grid_cache = grid
            self._sheet_grid_cache_key = cache_key
            return grid
        except Exception as e:
            self.logger.warning(f"获取工作表网格属性失败: {e}")
            return None

    def _build_sheet_full_range(self) -> Optional[str]:
        """构建覆盖整个工作表的范围字符串（基于网格属性）"""
        grid = self._get_sheet_grid_properties()
        if not grid:
            return None
        row_count, col_count = grid
        if row_count <= 0 or col_count <= 0:
            return None
        if not isinstance(self.api, SheetAPI):
            return None
        end_col = self.api.column_number_to_letter(col_count)
        return f"A1:{end_col}{row_count}"

    def get_current_sheet_data(self) -> pd.DataFrame:
        """获取当前电子表格数据"""
        self._sheet_read_complete = True
        if self.config.target_type != TargetType.SHEET:
            return pd.DataFrame()

        # 构建从配置起始点开始的读取范围
        start_cell = f"{self.config.start_column}{self.config.start_row}"
        read_range = None
        end_row = None
        end_col = None

        # 优先使用工作表网格属性精确限定范围
        grid = self._get_sheet_grid_properties()
        if grid and isinstance(self.api, SheetAPI):
            row_count, col_count = grid
            start_col_num = self.api.column_letter_to_number(self.config.start_column)
            if row_count < self.config.start_row or col_count < start_col_num:
                self.logger.info(
                    f"工作表网格范围小于起始位置: "
                    f"row_count={row_count}, column_count={col_count}, "
                    f"start={start_cell}"
                )
                return pd.DataFrame()

            end_row = row_count
            end_col = self.api.column_number_to_letter(col_count)
            read_range = (
                f"{self.config.sheet_id}!"
                f"{self.config.start_column}{self.config.start_row}:{end_col}{end_row}"
            )
        else:
            # 元数据不可用时使用配置化读取窗口，避免硬编码超大范围。
            if not isinstance(self.api, SheetAPI):
                self._sheet_read_complete = False
                return pd.DataFrame()
            self._sheet_read_complete = False
            end_row = self.config.start_row + self.config.sheet_scan_max_rows - 1
            start_col_num = self.api.column_letter_to_number(self.config.start_column)
            end_col = self.api.column_number_to_letter(
                start_col_num + self.config.sheet_scan_max_cols - 1
            )
            read_range = f"{self.config.sheet_id}!{start_cell}:{end_col}{end_row}"
            self.logger.warning(
                "无法获取工作表网格属性，使用配置化读取窗口: "
                f"{self.config.sheet_scan_max_rows} 行 × "
                f"{self.config.sheet_scan_max_cols} 列"
            )

        self.logger.info(f"尝试从范围读取数据: {read_range}")

        try:
            if not isinstance(self.api, SheetAPI):
                self._sheet_read_complete = False
                return pd.DataFrame()
            if not self.config.spreadsheet_token:
                self.logger.error("电子表格的 spreadsheet_token 未配置")
                self._sheet_read_complete = False
                return pd.DataFrame()
            if not self.config.sheet_id:
                self.logger.error("电子表格的 sheet_id 未配置")
                self._sheet_read_complete = False
                return pd.DataFrame()

            if not (end_row and end_col):
                self._sheet_read_complete = False
                return pd.DataFrame()

            values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            df = self.converter.values_to_df(values)

            if not df.empty:
                # 检查是否包含有效数据（至少有一行数据包含非空值）
                has_valid_data = False
                for _, row in df.iterrows():
                    if any(pd.notnull(val) and str(val).strip() != "" for val in row):
                        has_valid_data = True
                        break

                if has_valid_data:
                    self.logger.info(
                        f"成功获取电子表格数据: {len(df)} 行 x {len(df.columns)} 列 (从 {start_cell} 开始)"
                    )
                    return df

            # 如果df为空或数据全为空，说明表格在指定范围确实是空的
            self.logger.info(f"在范围 {read_range} 内未找到有效数据，视为空表")
            return pd.DataFrame()

        except Exception as e:
            self._sheet_read_complete = False
            self.logger.warning(f"尝试从范围 {read_range} 读取数据失败: {e}")
            self.logger.warning("无法完整获取电子表格数据；依赖远端现状的同步将停止")
            return pd.DataFrame()

    def _require_complete_sheet_read(self, operation: str) -> bool:
        """阻止基于截断或失败读取继续做行匹配和远端写入。"""
        if getattr(self, "_sheet_read_complete", True):
            return True
        self.logger.error(
            f"{operation}需要完整读取远端电子表格；当前读取窗口不完整或读取失败，已停止写入"
        )
        return False

    def get_sheet_data_with_validation(
        self,
    ) -> tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[set]]:
        """
        获取电子表格数据（支持双读用于结果检测）

        Returns:
            (result_df, formula_df, formula_columns):
            - result_df: 计算结果数据（用于比较）
            - formula_df: 公式数据（仅在启用 validate_results 时返回）
            - formula_columns: 包含公式的列集合（列名）
        """
        if not self.config.sheet_validate_results:
            # 未启用检测，使用原有单次读取逻辑
            return self.get_current_sheet_data(), None, None

        # 启用检测，执行双读
        if not isinstance(self.api, SheetAPI):
            return pd.DataFrame(), None, None

        if not self.config.spreadsheet_token or not self.config.sheet_id:
            return pd.DataFrame(), None, None

        # 获取网格范围
        grid = self._get_sheet_grid_properties()
        if not grid:
            self.logger.warning("无法获取工作表网格属性，无法进行双读")
            return self.get_current_sheet_data(), None, None

        row_count, col_count = grid
        start_col_num = self.api.column_letter_to_number(self.config.start_column)
        if row_count < self.config.start_row or col_count < start_col_num:
            self.logger.info("工作表范围小于起始位置，视为空表")
            return pd.DataFrame(), None, None

        end_row = row_count
        end_col = self.api.column_number_to_letter(col_count)

        self.logger.info("🔍 启用结果检测，开始双读云端数据...")

        # 第一次读取：公式模式
        self.logger.info("  📖 读取公式数据...")
        original_value_option = self.config.sheet_value_render_option
        original_datetime_option = self.config.sheet_datetime_render_option
        original_api_value_option = self.api.value_render_option
        original_api_datetime_option = self.api.datetime_render_option
        try:
            # 强制使用 Formula 模式读取
            self.config.sheet_value_render_option = "Formula"
            self.config.sheet_datetime_render_option = None
            self.api.value_render_option = "Formula"
            self.api.datetime_render_option = None

            formula_values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            formula_df = self.converter.values_to_df(formula_values)

        except Exception as e:
            self.logger.warning(f"读取公式数据失败: {e}")
            return self.get_current_sheet_data(), None, None
        finally:
            self.config.sheet_value_render_option = original_value_option
            self.config.sheet_datetime_render_option = original_datetime_option
            self.api.value_render_option = original_api_value_option
            self.api.datetime_render_option = original_api_datetime_option

        # 第二次读取：结果模式
        self.logger.info("  📊 读取计算结果数据...")
        try:
            # 使用配置的读取选项（或 FormattedValue 作为默认）
            self.config.sheet_value_render_option = (
                original_value_option or "FormattedValue"
            )
            self.config.sheet_datetime_render_option = (
                original_datetime_option or "FormattedString"
            )
            self.api.value_render_option = self.config.sheet_value_render_option
            self.api.datetime_render_option = self.config.sheet_datetime_render_option

            result_values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            result_df = self.converter.values_to_df(result_values)

        except Exception as e:
            self.logger.warning(f"读取结果数据失败: {e}")
            return self.get_current_sheet_data(), None, None
        finally:
            self.config.sheet_value_render_option = original_value_option
            self.config.sheet_datetime_render_option = original_datetime_option
            self.api.value_render_option = original_api_value_option
            self.api.datetime_render_option = original_api_datetime_option

        # 识别公式列
        if formula_df.empty:
            formula_columns = set()
        else:
            # 转换为二维列表用于识别
            formula_data = [formula_df.columns.tolist()] + formula_df.values.tolist()
            formula_columns = set(
                str(col)
                for col in self.api.identify_formula_columns(
                    formula_data, headers=formula_df.columns.tolist()
                )
            )

        if formula_columns:
            self.logger.info(f"  🔒 识别到公式列: {sorted(formula_columns)}")
        else:
            self.logger.info("  ℹ️  未识别到公式列")

        return result_df, formula_df, formula_columns

    def validate_and_report_differences(
        self,
        local_df: pd.DataFrame,
        remote_result_df: pd.DataFrame,
        formula_columns: Optional[set],
    ) -> Dict[str, Any]:
        """
        检测本地数据与云端结果的差异，生成列级差异报告

        Args:
            local_df: 本地数据
            remote_result_df: 云端结果数据
            formula_columns: 公式列集合

        Returns:
            差异统计字典
        """
        if formula_columns is None:
            formula_columns = set()

        formula_diff: Dict[str, int] = {}
        data_diff: Dict[str, int] = {}
        error_diff: Dict[str, str] = {}
        diff_stats: Dict[str, Any] = {
            "formula_columns": formula_diff,  # 公式列差异: {列名: 差异行数}
            "data_columns": data_diff,  # 数据列差异: {列名: 差异行数}
            "error_columns": error_diff,  # 异常列: {列名: 错误信息}
            "total_rows": len(local_df),
        }

        # 遍历所有列
        for col in local_df.columns:
            if col not in remote_result_df.columns:
                error_diff[str(col)] = "云端不存在此列"
                continue

            try:
                diff_count = 0
                local_col = local_df[col]
                remote_col = remote_result_df[col]

                # 逐行比较
                for idx in range(len(local_col)):
                    if idx >= len(remote_col):
                        diff_count += 1
                        continue

                    local_val = local_col.iloc[idx]
                    remote_val = remote_col.iloc[idx]

                    if not self._values_equal(local_val, remote_val):
                        diff_count += 1

                # 记录差异
                if diff_count > 0:
                    if col in formula_columns:
                        formula_diff[str(col)] = diff_count
                    else:
                        data_diff[str(col)] = diff_count

            except Exception as e:
                error_diff[str(col)] = str(e)

        return diff_stats

    def _values_equal(self, val1: Any, val2: Any) -> bool:
        """
        比较两个值是否相等（考虑数值容差）

        Args:
            val1: 第一个值
            val2: 第二个值

        Returns:
            是否相等
        """
        import pandas as pd

        # 都是空值
        if pd.isnull(val1) and pd.isnull(val2):
            return True

        # 一个空一个不空
        if pd.isnull(val1) or pd.isnull(val2):
            return False

        # 都是数值类型
        try:
            num1 = float(val1)
            num2 = float(val2)
            return abs(num1 - num2) <= self.config.sheet_diff_tolerance
        except (ValueError, TypeError):
            pass

        # 字符串比较
        return str(val1).strip() == str(val2).strip()

    def print_column_diff_report(self, diff_stats: Dict[str, Any]):
        """
        打印列级差异报告

        Args:
            diff_stats: 差异统计字典
        """
        if not self.config.sheet_report_column_diff:
            return

        total_rows = diff_stats["total_rows"]
        formula_cols = diff_stats["formula_columns"]
        data_cols = diff_stats["data_columns"]
        error_cols = diff_stats["error_columns"]

        # 统计信息
        total_cols = len(formula_cols) + len(data_cols) + len(error_cols)
        diff_cols = len(formula_cols) + len(data_cols)

        print("\n" + "=" * 60)
        print("📊 列差异检测报告")
        print(f"时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("模式: 逻辑同步+结果检测")
        print("=" * 60)

        if formula_cols:
            print("\n🔒 公式列（已保护，不覆盖）:")
            for col, diff_count in sorted(formula_cols.items()):
                pct = (diff_count / total_rows * 100) if total_rows > 0 else 0
                print(f"  ✓ {col}: {diff_count}/{total_rows} 行结果不一致 ({pct:.2f}%)")
            if self.config.sheet_protect_formulas:
                print("  → 建议: 检查输入数据列是否变化")

        if data_cols:
            print("\n📝 数据列（已同步）:")
            for col, diff_count in sorted(data_cols.items()):
                print(f"  ✓ {col}: {diff_count} 行差异 → 已更新")

        if error_cols:
            print("\n⚠️  异常列（类型不匹配或无法比较）:")
            for col, error in sorted(error_cols.items()):
                print(f"  ✗ {col}: {error}")

        print("\n" + "=" * 60)
        print(f"总计: {diff_cols}/{total_cols} 列有差异")
        if self.config.sheet_protect_formulas:
            print(f"同步完成: {len(data_cols)}/{total_cols} 列")
            print(f"保护跳过: {len(formula_cols)}/{total_cols} 列")
        else:
            print(f"同步完成: {len(data_cols) + len(formula_cols)}/{total_cols} 列")
        print("=" * 60 + "\n")

    # ========== 选择性同步辅助方法 ==========

    def _get_effective_selective_columns(self, df: pd.DataFrame) -> List[str]:
        """获取选择性同步实际生效的列（含索引列）"""
        if (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            return df.columns.tolist()

        target_columns = self.config.selective_sync.columns.copy()

        # 自动包含索引列（用于匹配逻辑）
        if (
            self.config.selective_sync.auto_include_index
            and self.config.index_column
            and self.config.index_column not in target_columns
        ):
            target_columns.append(self.config.index_column)
            self.logger.info(f"自动包含索引列: {self.config.index_column}")

        # 去重，保留顺序
        deduped_columns = []
        seen = set()
        for col in target_columns:
            if col not in seen:
                seen.add(col)
                deduped_columns.append(col)

        # 验证列是否存在
        missing_columns = [col for col in deduped_columns if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"指定的列不存在于数据中: {missing_columns}")
            deduped_columns = [col for col in deduped_columns if col in df.columns]

        # 保持列顺序（如果启用）
        if self.config.selective_sync.preserve_column_order:
            return [col for col in df.columns if col in deduped_columns]

        return deduped_columns

    def _apply_selective_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用选择性列过滤"""
        if (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            return df

        # 获取要处理的列
        target_columns = self._get_effective_selective_columns(df)
        return df[target_columns]

    # ========== Bitable 字段查询优化 ==========

    def _get_bitable_fetch_field_names(
        self, df: pd.DataFrame, mode: str
    ) -> Optional[List[str]]:
        """
        根据同步模式计算获取远程记录时需要的字段列表。

        通过飞书查询记录API的 field_names 参数，只返回必要的字段，
        减少不必要的数据传输，提升查询性能。

        Args:
            df: 本地数据 DataFrame
            mode: 同步模式 ('full', 'incremental', 'overwrite', 'clone')

        Returns:
            field_names 列表，None 表示获取全部字段
        """
        if mode == "clone":
            # clone 模式只需 record_id（API 固定返回），使用空 field_names 返回最小字段集
            return []

        index_col = self.config.index_column
        if not index_col:
            return None  # 无索引列时无法优化

        # full / incremental / overwrite：仅需索引列用于匹配和获取 record_id
        if mode in ("full", "incremental", "overwrite"):
            return [index_col]

        return None

    # ========== 统一同步方法 ==========

    def sync_full(self, df: pd.DataFrame) -> bool:
        """全量同步：已存在的更新，不存在的新增"""
        self.logger.info("开始全量同步...")

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_full_bitable(df)
        else:  # SHEET
            return self._sync_full_sheet(df)

    def _sync_full_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格全量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            field_types = self.get_field_types()
            new_records = self.converter.df_to_records(df, field_types)
            if self.config.app_token and self.config.table_id:
                canonical = self._canonical_records(new_records)
                success, receipts = self.process_typed_bitable_batches(
                    canonical, self._bitable_backend().batch_create
                )
                return success and self._verify_bitable_mutation(
                    "create", canonical, receipts
                )
            return False

        # 获取现有记录并建立索引（使用field_names优化，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "full")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        self.logger.info(f"🔍 获取到现有记录数量: {len(existing_records)}")

        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )
        self.logger.info(f"🔍 构建索引成功，索引数量: {len(existing_index)}")

        # 打印前几个现有记录的索引列值用于调试
        if existing_records and len(existing_records) > 0:
            for i, record in enumerate(existing_records[:3]):
                fields = record.get("fields", {})
                index_value = fields.get(self.config.index_column, "未找到")
                normalized_value = self.converter._normalize_index_value(
                    index_value,
                    self.converter._field_schema_type_code(
                        field_types.get(self.config.index_column)
                    ),
                )
                self.logger.info(
                    f"🔍 现有记录 {i + 1} 索引列 '{self.config.index_column}' 值: '{index_value}' -> 规范化: '{normalized_value}'"
                )

        # 分类本地数据
        records_to_update = []
        records_to_create = []

        for i, (_, row) in enumerate(df.iterrows()):
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )
            index_value = row.get(self.config.index_column, "未找到")

            # 打印前几条记录的匹配信息用于调试
            if i < 3:
                self.logger.info(
                    f"🔍 新数据记录 {i + 1} 索引列 '{self.config.index_column}' 值: '{index_value}' -> 哈希: {index_hash}"
                )
                self.logger.info(
                    f"🔍 哈希是否在现有索引中: {index_hash in existing_index if index_hash else False}"
                )

            # 使用字段类型转换构建记录
            fields = {}
            for k, v in row.to_dict().items():
                if not self.converter._is_empty_value(v):
                    converted_value = self.converter.convert_field_value_safe(
                        str(k), v, field_types
                    )
                    if converted_value is not None:
                        fields[str(k)] = converted_value

            record = {"fields": fields}

            if index_hash and index_hash in existing_index:
                # 需要更新的记录
                existing_record = existing_index[index_hash]
                record["record_id"] = existing_record["record_id"]
                records_to_update.append(record)
            else:
                # 需要新增的记录
                records_to_create.append(record)

        self.logger.info(
            f"全量同步计划: 更新 {len(records_to_update)} 条，新增 {len(records_to_create)} 条"
        )

        # 执行更新
        update_success = True
        if records_to_update and self.config.app_token and self.config.table_id:
            canonical_updates = self._canonical_records(records_to_update)
            update_success, update_receipts = self.process_typed_bitable_batches(
                canonical_updates, self._bitable_backend().batch_update
            )
            update_success = update_success and self._verify_bitable_mutation(
                "update", canonical_updates, update_receipts
            )
            if not update_success:
                self.logger.error("批量更新未完整成功，停止后续新增")
                return False

        # 执行新增
        create_success = True
        if records_to_create and self.config.app_token and self.config.table_id:
            canonical_creates = self._canonical_records(records_to_create)
            create_success, create_receipts = self.process_typed_bitable_batches(
                canonical_creates, self._bitable_backend().batch_create
            )
            create_success = create_success and self._verify_bitable_mutation(
                "create", canonical_creates, create_receipts
            )

        return update_success and create_success

    def _sync_full_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格全量同步"""
        if not self.config.index_column:
            if self.config.sheet_protect_formulas:
                self.logger.error("启用公式保护时全量同步需要索引列，已停止写入")
                return False
            self.logger.warning("未指定索引列，将执行完全覆盖操作")
            return self.sync_clone(df)

        # 获取现有数据（支持双读和差异检测）
        current_df, formula_df, formula_columns = self.get_sheet_data_with_validation()

        if not self._require_complete_sheet_read("全量同步"):
            return False

        if self.config.sheet_protect_formulas and formula_columns is None:
            self.logger.error("无法确认远端公式列，公式保护已停止全量写入")
            return False

        if current_df.empty:
            if self.config.sheet_protect_formulas:
                self.logger.error("启用公式保护时无法确认远端公式状态，已停止克隆写入")
                return False
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)

        # 差异检测与报告
        if self.config.sheet_validate_results and formula_columns is not None:
            diff_stats = self.validate_and_report_differences(
                df, current_df, formula_columns
            )
            self.print_column_diff_report(diff_stats)

        # 选择性同步仍需经过差异检测和公式保护。
        if self.config.selective_sync.enabled and self.config.selective_sync.columns:
            selective_columns = self._get_effective_selective_columns(df)
            if self.config.sheet_protect_formulas and formula_columns:
                if self.config.index_column in formula_columns:
                    self.logger.error(
                        "索引列是公式列，无法在保护公式的同时执行选择性同步"
                    )
                    return False
                selective_columns = [
                    col for col in selective_columns if col not in formula_columns
                ]
            if not selective_columns:
                self.logger.info("选择性同步列均为受保护公式列，无需同步")
                return True
            selective_df = df[selective_columns].copy()
            self.logger.info(f"🎯 启用精确列控制同步: {selective_columns}")
            return self._sync_selective_columns_sheet(selective_df, current_df)

        # 公式保护：过滤掉公式列
        sync_df = df
        if self.config.sheet_protect_formulas and formula_columns:
            if self.config.index_column in formula_columns:
                self.logger.error("索引列是公式列，无法在保护公式时完成行匹配")
                return False
            # 只同步非公式列
            non_formula_cols = [col for col in df.columns if col not in formula_columns]
            if not non_formula_cols:
                self.logger.warning("所有列都是公式列，且启用了公式保护，无需同步")
                return True
            sync_df = df[non_formula_cols].copy()
            self.logger.info(
                f"🔒 公式保护已启用，仅同步 {len(non_formula_cols)} 个数据列"
            )
            # 必须使用精确列写入；整表回写会把 Formula 读取的计算结果覆盖回公式列。
            return self._sync_selective_columns_sheet(sync_df, current_df)

        # 原有的完整表格同步逻辑
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 分类数据
        update_rows = []
        new_rows = []

        for _, row in sync_df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 更新现有行
                current_row_idx = current_index[index_hash]
                update_rows.append((current_row_idx, row))
            else:
                # 新增行
                new_rows.append(row)

        self.logger.info(
            f"全量同步计划: 更新 {len(update_rows)} 行，新增 {len(new_rows)} 行"
        )

        # 执行更新
        success = True
        if update_rows:
            # 更新现有行
            updated_df = current_df.copy()
            for current_row_idx, new_row in update_rows:
                for col in sync_df.columns:
                    if col in updated_df.columns:
                        # 使用 .iloc 双索引避免链式赋值问题 (SettingWithCopyWarning)
                        updated_df.iloc[
                            current_row_idx, updated_df.columns.get_loc(col)
                        ] = new_row[col]

            # 写入更新后的数据
            values = self.converter.df_to_values(updated_df)
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                success = self._typed_sheet_write(values)

        # 追加新行
        if new_rows and success:
            new_df = pd.DataFrame(new_rows)
            new_values = self.converter.df_to_values(new_df, include_headers=False)

            if (
                new_values
                and isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info(f"开始追加 {len(new_values)} 行新数据")
                success = self._typed_sheet_append(
                    new_values, header_width=len(sync_df.columns)
                )

        return success

    def _sync_selective_columns_sheet(
        self, df: pd.DataFrame, current_df: pd.DataFrame
    ) -> bool:
        """电子表格选择性列同步 - 使用精确列控制"""
        columns = self._get_effective_selective_columns(df)
        if not columns:
            self.logger.warning("选择性列同步未配置 columns 或无可用列，已跳过")
            return False
        self.logger.info(f"🎯 启用精确列控制同步: {columns}")

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 准备更新数据映射 {row_idx: {col: value}}
        update_data_map: Dict[int, Dict[str, Any]] = {}
        new_rows: List[pd.Series] = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 更新现有行
                current_row_idx = current_index[index_hash]
                if current_row_idx not in update_data_map:
                    update_data_map[current_row_idx] = {}

                # 只更新指定列
                for col in columns:
                    if col in df.columns:
                        update_data_map[current_row_idx][col] = row[col]
            else:
                # 新增行
                new_rows.append(row)

        self.logger.info(
            f"精确列控制计划: 更新 {len(update_data_map)} 行的指定列，新增 {len(new_rows)} 行"
        )

        success = True

        # 执行选择性列更新
        if update_data_map:
            success = self._update_selective_columns(current_df, update_data_map)

        # 追加新行（如果有）
        if new_rows and success:
            success = self._append_selective_columns(
                pd.DataFrame(new_rows),
                columns=columns,
                current_df=current_df,
            )

        return success

    def _update_selective_columns(
        self, current_df: pd.DataFrame, update_data_map: Dict[int, Dict[str, Any]]
    ) -> bool:
        """使用精确列控制更新数据"""
        if not update_data_map:
            return True

        # 准备按列组织的更新数据
        columns_to_update: set[str] = set()
        for row_updates in update_data_map.values():
            columns_to_update.update(row_updates.keys())

        self.logger.info(f"🔄 准备更新列: {list(columns_to_update)}")

        # 使用 converter 的 df_to_column_data 和 get_column_positions
        # 构建需要更新的列数据
        column_data = {}
        for col in columns_to_update:
            col_data = []
            for row_idx in range(len(current_df)):
                if row_idx in update_data_map and col in update_data_map[row_idx]:
                    # 使用新值
                    converted_value = self.converter.simple_convert_value(
                        update_data_map[row_idx][col]
                    )
                    col_data.append(converted_value)
                else:
                    # 保持原值
                    original_value = (
                        current_df.iloc[row_idx][col]
                        if col in current_df.columns
                        else ""
                    )
                    converted_value = self.converter.simple_convert_value(
                        original_value
                    )
                    col_data.append(converted_value)
            column_data[col] = col_data

        # 获取起始列偏移量
        start_col_offset = 0
        if isinstance(self.api, SheetAPI):
            start_col_offset = self.api.start_col_num - 1

        # 获取列位置映射（考虑起始列偏移）
        column_positions = self.converter.get_column_positions(
            current_df, list(columns_to_update), start_col_offset
        )

        self.logger.info(f"📍 列位置映射: {column_positions}")

        # 使用精确列写入API
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            # start_row 需要考虑配置的起始行 + 表头行
            actual_start_row = self.config.start_row + 1
            # 如果 optimize_ranges 为 False，设置 max_gap=0 禁用合并
            effective_max_gap = (
                self.config.selective_sync.max_gap_for_merge
                if self.config.selective_sync.optimize_ranges
                else 0
            )
            return self._typed_sheet_selective_write(
                column_data,
                column_positions,
                start_row=actual_start_row,
                max_gap=effective_max_gap,
                header_width=len(current_df.columns),
            )

        return False

    def sync_incremental(self, df: pd.DataFrame) -> bool:
        """增量同步：只新增不存在的记录"""
        self.logger.info("开始增量同步...")

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_incremental_bitable(df)
        else:  # SHEET
            return self._sync_incremental_sheet(df)

    def _sync_incremental_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格增量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            field_types = self.get_field_types()
            new_records = self.converter.df_to_records(df, field_types)
            if self.config.app_token and self.config.table_id:
                canonical = self._canonical_records(new_records)
                success, receipts = self.process_typed_bitable_batches(
                    canonical, self._bitable_backend().batch_create
                )
                return success and self._verify_bitable_mutation(
                    "create", canonical, receipts
                )
            return False

        # 获取现有记录并建立索引（仅获取索引列，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "incremental")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )

        # 筛选出需要新增的记录
        records_to_create = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )

            if not index_hash or index_hash not in existing_index:
                # 使用字段类型转换构建记录
                fields = {}
                for k, v in row.to_dict().items():
                    if not self.converter._is_empty_value(v):
                        converted_value = self.converter.convert_field_value_safe(
                            str(k), v, field_types
                        )
                        if converted_value is not None:
                            fields[str(k)] = converted_value

                record = {"fields": fields}
                records_to_create.append(record)

        self.logger.info(f"增量同步计划: 新增 {len(records_to_create)} 条记录")

        if records_to_create and self.config.app_token and self.config.table_id:
            canonical = self._canonical_records(records_to_create)
            success, receipts = self.process_typed_bitable_batches(
                canonical, self._bitable_backend().batch_create
            )
            return success and self._verify_bitable_mutation(
                "create", canonical, receipts
            )
        else:
            self.logger.info("没有新记录需要同步")
            return True

    def _sync_incremental_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格增量同步 - 使用优化API策略"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将新增全部数据")

            # ⭐ 检查选择性同步：如果启用，需要用列级控制追加
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                self.logger.info(
                    f"🎯 增量同步启用精确列控制: {self.config.selective_sync.columns}"
                )
                return self._append_selective_columns(df)

            # 常规增量同步策略
            values = self.converter.df_to_values(
                df, include_headers=False
            )  # 追加不需要表头
            self.logger.info("使用append接口进行增量同步")
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                return self._typed_sheet_append(values, header_width=len(df.columns))
            return False

        # 获取现有数据
        current_df = self.get_current_sheet_data()

        if not self._require_complete_sheet_read("增量同步"):
            return False

        if current_df.empty:
            self.logger.info("电子表格为空，新增全部数据")
            # ⭐ 检查选择性同步
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                return self._append_selective_columns(df)
            # 使用克隆同步（会先写入数据再设置格式）
            return self.sync_clone(df)

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 筛选需要新增的记录
        new_rows = []
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if not index_hash or index_hash not in current_index:
                new_rows.append(row)

        self.logger.info(f"增量同步计划: 新增 {len(new_rows)} 行")

        if new_rows:
            new_df = pd.DataFrame(new_rows)

            # ⭐ 检查选择性同步：如果启用，需要用列级控制追加
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                return self._append_selective_columns(new_df)

            # 常规追加
            new_values = self.converter.df_to_values(new_df, include_headers=False)

            # 追加新数据
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info(f"开始增量追加 {len(new_values)} 行数据")
                return self._typed_sheet_append(
                    new_values, header_width=len(new_df.columns)
                )
            return False
        else:
            self.logger.info("没有新记录需要同步")
            return True

    def _append_selective_columns(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        current_df: Optional[pd.DataFrame] = None,
    ) -> bool:
        """选择性列的追加操作"""
        if columns is None and (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            self.logger.warning("选择性同步未启用或未指定列，使用常规追加")
            values = self.converter.df_to_values(df, include_headers=False)
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                return self._typed_sheet_append(values, header_width=len(df.columns))
            return False

        # 获取当前表格数据以确定正确的列位置。调用方已有完整快照时直接复用，
        # 避免更新后的重复读取，也避免把公式列降级成普通值。
        if current_df is None:
            current_df = self.get_current_sheet_data()

        if current_df is None or not self._require_complete_sheet_read("选择性列追加"):
            return False

        effective_columns = (
            [column for column in columns if column in df.columns]
            if columns is not None
            else self._get_effective_selective_columns(df)
        )
        if not effective_columns:
            self.logger.warning("选择性列追加无可用列，已跳过")
            return False

        if current_df.empty:
            # 如果表格为空，先写入表头，然后追加数据
            self.logger.info("表格为空，先创建表头然后追加选择性列数据")
            header_values = [effective_columns]

            # 写入表头
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                header_success = self._typed_sheet_write(
                    header_values, verify_formulas=False
                )
                if not header_success:
                    return False

                # 更新current_df为包含表头的空数据框
                current_df = pd.DataFrame(columns=effective_columns)

        # 准备选择性列数据
        column_data = self.converter.df_to_column_data(df, effective_columns)

        # 获取起始列偏移量
        start_col_offset = 0
        if isinstance(self.api, SheetAPI):
            start_col_offset = self.api.start_col_num - 1

        column_positions = self.converter.get_column_positions(
            current_df, effective_columns, start_col_offset
        )

        # 计算起始行：配置的起始行 + 当前数据行数 + 1（表头）
        start_row = self.config.start_row + len(current_df) + 1

        self.logger.info(
            f"🎯 选择性列追加: {list(column_data.keys())} 从第{start_row}行开始"
        )

        # 使用精确列追加API
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            effective_max_gap = (
                self.config.selective_sync.max_gap_for_merge
                if self.config.selective_sync.optimize_ranges
                else 0
            )
            return self._typed_sheet_selective_write(
                column_data,
                column_positions,
                start_row=start_row,
                max_gap=effective_max_gap,
                header_width=len(effective_columns),
            )

        return False

    def sync_overwrite(self, df: pd.DataFrame) -> bool:
        """覆盖同步：删除已存在的，然后新增全部"""
        self.logger.info("开始覆盖同步...")

        if not self.config.index_column:
            self.logger.error("覆盖同步模式需要指定索引列")
            return False

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_overwrite_bitable(df)
        else:  # SHEET
            return self._sync_overwrite_sheet(df)

    def _sync_overwrite_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格覆盖同步"""
        # 获取现有记录并建立索引（仅获取索引列，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "overwrite")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )

        # 找出需要删除的记录
        record_ids_to_delete = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )
            if index_hash and index_hash in existing_index:
                existing_record = existing_index[index_hash]
                record_ids_to_delete.append(existing_record["record_id"])

        self.logger.info(
            f"覆盖同步计划: 删除 {len(record_ids_to_delete)} 条已存在记录，然后新增 {len(df)} 条记录"
        )

        # 删除已存在的记录
        delete_success = True
        if record_ids_to_delete and self.config.app_token and self.config.table_id:
            delete_success, delete_receipts = self.process_typed_bitable_batches(
                record_ids_to_delete, self._bitable_backend().batch_delete
            )
            delete_success = delete_success and self._verify_bitable_mutation(
                "delete", record_ids_to_delete, delete_receipts
            )
            if not delete_success:
                self.logger.error("覆盖同步删除未完整成功，停止后续新增")
                return False

        # 新增全部记录
        new_records = self.converter.df_to_records(df, field_types)
        create_success = False
        if self.config.app_token and self.config.table_id:
            canonical = self._canonical_records(new_records)
            create_success, create_receipts = self.process_typed_bitable_batches(
                canonical, self._bitable_backend().batch_create
            )
            create_success = create_success and self._verify_bitable_mutation(
                "create", canonical, create_receipts
            )

        return delete_success and create_success

    def _sync_overwrite_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格覆盖同步"""
        # 获取现有数据
        current_df = self.get_current_sheet_data()

        if not self._require_complete_sheet_read("覆盖同步"):
            return False

        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)

        # ⭐ 检查是否启用选择性同步，使用精确列级控制
        if self.config.selective_sync.enabled and self.config.selective_sync.columns:
            self.logger.info(
                f"🎯 覆盖同步启用精确列控制: {self.config.selective_sync.columns}"
            )
            return self._sync_overwrite_selective_columns_sheet(df, current_df)

        # 原有的完整表格覆盖逻辑。预先构建新数据索引，避免逐行嵌套扫描。
        new_df_rows = []
        deleted_count = 0
        new_index_hashes = {
            index_hash
            for _, new_row in df.iterrows()
            if (
                index_hash := self.converter.get_index_value_hash(
                    new_row, self.config.index_column
                )
            )
        }

        # 保留不在新数据中的现有记录
        for _, row in current_df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash:
                if index_hash not in new_index_hashes:
                    new_df_rows.append(row)
                else:
                    deleted_count += 1

        # 添加新数据
        for _, row in df.iterrows():
            new_df_rows.append(row)

        self.logger.info(f"覆盖同步计划: 删除 {deleted_count} 行，新增 {len(df)} 行")

        # 重写整个表格
        if new_df_rows:
            new_df = pd.DataFrame(new_df_rows)
            values = self.converter.df_to_values(new_df)

            # 使用优化API策略覆盖写入
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info("使用write_sheet_data覆盖写入")
                return self._typed_sheet_write(values)
            return False
        else:
            # 如果没有数据，清空表格
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                clear_range = self._build_sheet_full_range()
                if not clear_range:
                    self.logger.error("无法获取工作表网格范围，清空失败")
                    return False
                return self._typed_sheet_clear(clear_range)
            return False

    def _sync_overwrite_selective_columns_sheet(
        self, df: pd.DataFrame, current_df: pd.DataFrame
    ) -> bool:
        """电子表格选择性列覆盖同步"""
        columns = self._get_effective_selective_columns(df)
        if not columns:
            self.logger.warning("选择性列覆盖同步未配置 columns 或无可用列，已跳过")
            return False
        self.logger.info(f"🎯 选择性列覆盖同步: {columns}")

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 准备数据映射 {row_idx: {col: value}}
        update_data_map: Dict[int, Dict[str, Any]] = {}  # 更新现有行的指定列
        new_rows: List[pd.Series] = []  # 全新的行

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 覆盖现有行的指定列
                current_row_idx = current_index[index_hash]
                if current_row_idx not in update_data_map:
                    update_data_map[current_row_idx] = {}

                # 只覆盖指定列
                for col in columns:
                    if col in df.columns:
                        update_data_map[current_row_idx][col] = row[col]
            else:
                # 全新行
                new_rows.append(row)

        self.logger.info(
            f"选择性列覆盖计划: 覆盖 {len(update_data_map)} 行的指定列，新增 {len(new_rows)} 行"
        )

        success = True

        # 执行选择性列覆盖更新
        if update_data_map:
            success = self._update_selective_columns(current_df, update_data_map)

        # 追加新行（如果有）
        if new_rows and success:
            # 对于新行，也应该只包含选择性列
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                success = self._append_selective_columns(pd.DataFrame(new_rows))
            else:
                # 常规追加
                new_df = pd.DataFrame(new_rows)
                new_values = self.converter.df_to_values(new_df, include_headers=False)

                if (
                    isinstance(self.api, SheetAPI)
                    and self.config.spreadsheet_token
                    and self.config.sheet_id
                ):
                    success = self._typed_sheet_append(
                        new_values, header_width=len(new_df.columns)
                    )

        return success

    def sync_clone(self, df: pd.DataFrame) -> bool:
        """克隆同步：清空全部，然后新增全部"""
        self.logger.info("开始克隆同步...")

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_clone_bitable(df)
        else:  # SHEET
            return self._sync_clone_sheet(df)

    def _sync_clone_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格克隆同步"""
        # 获取所有现有记录（仅获取最小字段集，clone模式只需record_id）
        fetch_fields = self._get_bitable_fetch_field_names(df, "clone")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        existing_record_ids = [record["record_id"] for record in existing_records]

        self.logger.info(
            f"克隆同步计划: 删除 {len(existing_record_ids)} 条已有记录，然后新增 {len(df)} 条记录"
        )

        # 删除所有记录
        delete_success = True
        if existing_record_ids and self.config.app_token and self.config.table_id:
            delete_success, delete_receipts = self.process_typed_bitable_batches(
                existing_record_ids, self._bitable_backend().batch_delete
            )
            delete_success = delete_success and self._verify_bitable_mutation(
                "delete", existing_record_ids, delete_receipts
            )
            if not delete_success:
                self.logger.error("克隆同步删除未完整成功，停止后续新增")
                return False

        # 新增全部记录
        field_types = self.get_field_types()
        new_records = self.converter.df_to_records(df, field_types)
        create_success = False
        if self.config.app_token and self.config.table_id:
            canonical = self._canonical_records(new_records)
            create_success, create_receipts = self.process_typed_bitable_batches(
                canonical, self._bitable_backend().batch_create
            )
            create_success = create_success and self._verify_bitable_mutation(
                "create", canonical, create_receipts
            )

        return delete_success and create_success

    def _sync_clone_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格克隆同步 - 使用优化API策略"""
        # 转换数据格式
        values = self.converter.df_to_values(df)

        self.logger.info(f"克隆同步计划: 清空现有数据，新增 {len(df)} 行")
        self.logger.info("使用write_sheet_data进行克隆写入")

        # 首先清空表格的一个大范围
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            self.logger.info("清空现有数据...")
            clear_range = self._build_sheet_full_range()
            if not clear_range:
                self.logger.error("无法获取工作表网格范围，清空失败")
                return False
            clear_success = self._typed_sheet_clear(clear_range)
            if not clear_success:
                self.logger.error("清空电子表格失败，终止克隆同步")
                return False

            # 使用增强的写入方法
            write_success = self._typed_sheet_write(values)
        else:
            write_success = False

        # 数据写入成功后，再应用智能字段配置
        if write_success:
            if not self._setup_sheet_intelligence(df):
                self.logger.warning("智能字段配置失败，但数据同步已完成")

        return write_success

    def _setup_sheet_intelligence(self, df: pd.DataFrame) -> bool:
        """
        为电子表格设置智能字段配置

        Args:
            df: 数据DataFrame

        Returns:
            是否设置成功
        """
        if self.config.target_type != TargetType.SHEET:
            return True

        if not isinstance(self.api, SheetAPI):
            self.logger.error(
                "内部逻辑错误: _setup_sheet_intelligence 应该只被 SheetAPI 调用"
            )
            return False

        # 不同策略的配置范围不同
        strategy_name = self.config.field_type_strategy.value
        self.logger.info(f"开始电子表格智能字段配置 ({strategy_name}策略)...")

        # raw策略：不应用任何格式化，直接返回成功
        if strategy_name == "raw":
            self.logger.info("raw策略：跳过所有格式化，保持原始数据")
            return True

        # 生成字段配置
        field_config = self.converter.generate_sheet_field_config(
            df, self.config.field_type_strategy.value, self.config
        )

        success = True

        def record_applied(count: int) -> None:
            if hasattr(self, "_last_action_applied_count"):
                self._last_action_applied_count += count
                self._last_action_accepted_units += count

        # 1. 配置下拉列表 (base策略跳过)
        if strategy_name != "base":
            for dropdown_config in field_config["dropdown_configs"]:
                column_name = dropdown_config["column"]

                # 计算列的绝对位置
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                # 计算行的绝对范围 (数据行，不含表头)
                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                # 仅在有数据行时才设置范围
                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                else:
                    self.logger.warning(
                        f"列 '{column_name}' 没有数据行，跳过下拉列表设置"
                    )
                    continue

                # 确保使用SheetAPI并检查token
                if not isinstance(self.api, SheetAPI):
                    self.logger.error("API类型不匹配，需要SheetAPI")
                    success = False
                    continue

                if not self.config.spreadsheet_token:
                    self.logger.error("电子表格Token为空")
                    success = False
                    continue

                # 设置下拉列表
                dropdown_success = self.api.set_dropdown_validation(
                    self.config.spreadsheet_token,
                    range_str,
                    dropdown_config["options"],
                    dropdown_config["multiple"],
                    dropdown_config["colors"],
                )

                if dropdown_success:
                    record_applied(1)
                    self.logger.info(f"成功为列 '{column_name}' 设置下拉列表")
                else:
                    self.logger.error(f"为列 '{column_name}' 设置下拉列表失败")
                    success = False
        else:
            self.logger.info("base策略跳过下拉列表配置")

        # 2. 配置日期格式
        if (
            field_config["date_columns"]
            and isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
        ):
            date_ranges = []
            for column_name in field_config["date_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    date_ranges.append(range_str)

            # 设置日期格式
            date_success = self.api.set_date_format(
                self.config.spreadsheet_token, date_ranges, "yyyy/MM/dd"
            )

            if date_success:
                record_applied(len(date_ranges))
                self.logger.info(f"成功为 {len(date_ranges)} 个日期列设置格式")
            else:
                self.logger.error("设置日期格式失败")
                success = False

        # 3. 配置数字格式
        if (
            field_config["number_columns"]
            and isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
        ):
            number_ranges = []
            for column_name in field_config["number_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    number_ranges.append(range_str)

            # 设置数字格式
            number_success = self.api.set_number_format(
                self.config.spreadsheet_token, number_ranges, "#,##0.00"
            )

            if number_success:
                record_applied(len(number_ranges))
                self.logger.info(f"成功为 {len(number_ranges)} 个数字列设置格式")
            else:
                self.logger.error("设置数字格式失败")
                success = False

        # 输出配置摘要
        dropdown_count = (
            len(field_config["dropdown_configs"]) if strategy_name != "base" else 0
        )
        date_count = len(field_config["date_columns"])
        number_count = len(field_config["number_columns"])
        total_configs = dropdown_count + date_count + number_count

        if total_configs > 0:
            config_summary = []
            if dropdown_count > 0:
                config_summary.append(f"{dropdown_count}个下拉列表")
            if date_count > 0:
                config_summary.append(f"{date_count}个日期格式")
            if number_count > 0:
                config_summary.append(f"{number_count}个数字格式")

            self.logger.info(f"智能字段配置完成: {', '.join(config_summary)}")
        else:
            self.logger.info("未检测到需要智能配置的字段")

        if hasattr(self, "_last_action_mutation_complete"):
            self._last_action_mutation_complete = success
            if not success and self._last_action_applied_count:
                self._last_action_remote_outcome = MutationOutcome.PARTIAL.value

        return success

    def _plan_config_sources(self) -> Mapping[str, str]:
        sources = getattr(self.config, "config_sources", {})
        return dict(sources) if isinstance(sources, Mapping) else {}

    def _make_plan(
        self,
        *,
        requested_mode: SyncMode,
        effective_mode: SyncMode,
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        actions: List[PlanAction],
        warnings: Optional[List[str]] = None,
    ) -> SyncPlan:
        plan_warnings = list(warnings or ())
        plan_warnings.extend(self.converter.consume_key_warnings())
        return SyncPlan(
            requested_mode=requested_mode.value,
            effective_mode=effective_mode.value,
            source=source,
            target=target,
            actions=tuple(actions),
            warnings=tuple(plan_warnings),
            destructive=(
                effective_mode in {SyncMode.OVERWRITE, SyncMode.CLONE}
                or any(action.destructive for action in actions)
            ),
            clears_values=any(action.clears_values for action in actions),
            config_sources=self._plan_config_sources(),
        )

    def _row_to_canonical_fields(
        self, row: pd.Series, field_types: Mapping[str, FieldSchema]
    ) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}
        for raw_name, value in row.to_dict().items():
            name = str(raw_name)
            if self.converter._is_empty_value(value):
                continue
            converted = self.converter.convert_field_value_safe(
                name, value, dict(field_types)
            )
            if converted is not None:
                fields[name] = converted
        return fields

    def _plan_file_bitable(self, df: pd.DataFrame) -> SyncPlan:
        mode = self.config.sync_mode
        match_strategy = self.config.match_strategy
        actions, field_types = self.plan_fields(df)
        source = {"type": "file", "rows": len(df), "columns": len(df.columns)}
        target = {"type": "bitable"}
        index_column = self.config.index_column
        planned_fields = {
            str(cast(Mapping[str, Any], action.payload)["field_name"])
            for action in actions
            if action.kind == "create_fields"
        }

        fetch_fields = self._get_bitable_fetch_field_names(df, mode.value)
        existing_records: List[Dict[str, Any]] = []
        if mode is SyncMode.CLONE or (
            match_strategy is MatchStrategy.BY_KEY
            and index_column
            and index_column not in planned_fields
        ):
            existing_records = self.get_all_bitable_records(fetch_fields)
        try:
            existing_index = (
                self.converter.build_record_index(
                    existing_records, index_column, field_types
                )
                if match_strategy is MatchStrategy.BY_KEY and index_column
                else {}
            )
        except ValueError as error:
            raise RuntimeError(f"目标 Bitable 索引不安全: {error}") from error
        if match_strategy is MatchStrategy.BY_KEY and index_column:
            self.converter.build_data_index(
                df,
                index_column,
                field_types,
                allow_empty=False,
                context=f"本地数据索引列 '{index_column}' ",
            )

        creates: List[CanonicalRecord] = []
        updates: List[CanonicalRecord] = []
        deletes: List[str] = []

        if mode is SyncMode.CLONE:
            deletes = [
                str(record["record_id"])
                for record in existing_records
                if record.get("record_id")
            ]
            creates = self._canonical_records(
                self.converter.df_to_records(df, field_types)
            )
        elif match_strategy is MatchStrategy.APPEND_ONLY:
            creates = self._canonical_records(
                self.converter.df_to_records(df, field_types)
            )
        elif mode is SyncMode.OVERWRITE:
            if not index_column:
                raise ValueError("覆盖同步模式需要指定索引列")
            for _, row in df.iterrows():
                index_hash = self.converter.get_index_value_hash(
                    row, index_column, field_types
                )
                if index_hash and index_hash in existing_index:
                    record_id = existing_index[index_hash].get("record_id")
                    if record_id:
                        deletes.append(str(record_id))
            creates = self._canonical_records(
                self.converter.df_to_records(df, field_types)
            )
        else:
            for _, row in df.iterrows():
                index_hash = self.converter.get_index_value_hash(
                    row, index_column, field_types
                )
                fields = self._row_to_canonical_fields(row, field_types)
                if index_hash and index_hash in existing_index:
                    if mode is SyncMode.FULL:
                        record_id = existing_index[index_hash].get("record_id")
                        if not record_id:
                            raise RuntimeError("目标记录缺少 record_id")
                        updates.append(CanonicalRecord(str(record_id), fields))
                else:
                    creates.append(CanonicalRecord(None, fields))

        if deletes:
            actions.append(
                PlanAction(
                    "delete_records",
                    len(deletes),
                    {"target": "bitable"},
                    destructive=True,
                    payload=deletes,
                )
            )
        if updates:
            actions.append(
                PlanAction(
                    "update_records",
                    len(updates),
                    {"target": "bitable"},
                    payload=updates,
                )
            )
        if creates:
            actions.append(
                PlanAction(
                    "create_records",
                    len(creates),
                    {"target": "bitable"},
                    payload=creates,
                )
            )
        return self._make_plan(
            requested_mode=mode,
            effective_mode=mode,
            source=source,
            target=target,
            actions=actions,
        )

    def _sheet_clear_action(self) -> PlanAction:
        clear_range = self._build_sheet_full_range()
        if not clear_range:
            raise RuntimeError("无法获取工作表网格范围")
        return PlanAction(
            "clear_range",
            1,
            {"target": "sheet", "range": clear_range},
            destructive=True,
            clears_values=True,
            payload=clear_range,
        )

    def _sheet_write_action(self, df: pd.DataFrame) -> PlanAction:
        values = self.converter.df_to_values(df)
        return PlanAction(
            "write_range",
            max(len(values), 0),
            {"target": "sheet", "columns": len(df.columns)},
            clears_values=True,
            payload={"values": values, "header_width": len(df.columns)},
        )

    def _sheet_append_action(self, df: pd.DataFrame) -> PlanAction:
        values = self.converter.df_to_values(df, include_headers=False)
        return PlanAction(
            "append_rows",
            len(values),
            {"target": "sheet", "columns": len(df.columns)},
            payload={"values": values, "header_width": len(df.columns)},
        )

    def _sheet_columns_action(
        self,
        df: pd.DataFrame,
        current_df: pd.DataFrame,
        columns: List[str],
        *,
        start_row: int,
        preserve_rows: bool,
        update_data_map: Optional[Dict[int, Dict[str, Any]]] = None,
    ) -> Optional[PlanAction]:
        if not columns:
            return None
        if preserve_rows:
            updates = update_data_map or {}
            column_data: Dict[str, List[Any]] = {}
            for column in columns:
                values: List[Any] = []
                for row_index in range(len(current_df)):
                    value = (
                        updates[row_index][column]
                        if row_index in updates and column in updates[row_index]
                        else current_df.iloc[row_index].get(column, "")
                    )
                    values.append(self.converter.simple_convert_value(value))
                column_data[column] = values
        else:
            column_data = self.converter.df_to_column_data(df, columns)
        start_col_offset = (
            self.api.start_col_num - 1 if isinstance(self.api, SheetAPI) else 0
        )
        positions = self.converter.get_column_positions(
            current_df, columns, start_col_offset
        )
        max_gap = (
            self.config.selective_sync.max_gap_for_merge
            if self.config.selective_sync.optimize_ranges
            else 0
        )
        return PlanAction(
            "write_columns",
            len(df) if not preserve_rows else len(update_data_map or {}),
            {"target": "sheet", "columns": len(columns)},
            clears_values=any(
                self.converter._is_empty_value(value)
                for values in column_data.values()
                for value in values
            ),
            payload={
                "column_data": column_data,
                "column_positions": positions,
                "start_row": start_row,
                "max_gap": max_gap,
                "header_width": len(current_df.columns) or len(columns),
            },
        )

    def _plan_sheet_selective(
        self,
        df: pd.DataFrame,
        current_df: pd.DataFrame,
        mode: SyncMode,
        columns: List[str],
        current_index: Mapping[str, int],
        index_field_types: Mapping[str, Any],
    ) -> List[PlanAction]:
        update_data_map: Dict[int, Dict[str, Any]] = {}
        new_rows: List[pd.Series] = []
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, dict(index_field_types)
            )
            if index_hash and index_hash in current_index:
                if mode in {SyncMode.FULL, SyncMode.OVERWRITE}:
                    row_index = current_index[index_hash]
                    update_data_map[row_index] = {
                        column: row[column] for column in columns if column in row
                    }
            else:
                new_rows.append(row)

        actions: List[PlanAction] = []
        if update_data_map:
            action = self._sheet_columns_action(
                df,
                current_df,
                columns,
                start_row=self.config.start_row + 1,
                preserve_rows=True,
                update_data_map=update_data_map,
            )
            if action:
                actions.append(action)
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            action = self._sheet_columns_action(
                new_df,
                current_df,
                columns,
                start_row=self.config.start_row + len(current_df) + 1,
                preserve_rows=False,
            )
            if action:
                actions.append(action)
        return actions

    def _sheet_index_field_types(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Infer DATETIME semantics for Sheet indexes before cross-form matching."""
        index_column = self.config.index_column
        if not index_column:
            return {}

        epoch_sets: List[set[str]] = []
        for frame in (source_df, target_df):
            if index_column not in frame.columns:
                continue
            series = frame[index_column]
            if pd.api.types.is_datetime64_any_dtype(series.dtype):
                return {index_column: 5}
            values = [
                value
                for value in series.tolist()
                if not self.converter._is_empty_value(value)
            ]
            if not values:
                continue
            if any(
                isinstance(value, (date, datetime, pd.Timestamp)) for value in values
            ):
                return {index_column: 5}

            formatted_strings = [
                value
                for value in values
                if isinstance(value, str)
                and any(marker in value for marker in ("-", "/", ":", "年", "月"))
            ]
            if len(formatted_strings) == len(values):
                parsed = [pd.to_datetime(value, errors="coerce") for value in values]
                if all(not pd.isna(value) for value in parsed):
                    return {index_column: 5}

            normalized_epochs: set[str] = set()
            all_epoch_like = True
            for value in values:
                if isinstance(value, bool):
                    all_epoch_like = False
                    break
                numeric: Optional[float] = None
                if isinstance(value, numbers.Real):
                    numeric = float(value)
                elif isinstance(value, str) and value.strip().isdigit():
                    numeric = float(value.strip())
                if numeric is None:
                    all_epoch_like = False
                    break
                milliseconds = self.converter._numeric_timestamp_to_milliseconds(
                    numeric, strict=False
                )
                if milliseconds is None:
                    all_epoch_like = False
                    break
                normalized = self.converter._normalize_timestamp_index_value(
                    value, self.config.datetime_index_granularity
                )
                if normalized is None:
                    all_epoch_like = False
                    break
                normalized_epochs.add(normalized)
            if all_epoch_like and normalized_epochs:
                if len(normalized_epochs) < len(values):
                    return {index_column: 5}
                epoch_sets.append(normalized_epochs)

        if len(epoch_sets) >= 2 and set.intersection(*epoch_sets):
            return {index_column: 5}
        return {}

    def _plan_file_sheet(self, df: pd.DataFrame) -> SyncPlan:
        requested_mode = self.config.sync_mode
        source = {"type": "file", "rows": len(df), "columns": len(df.columns)}
        target = {"type": "sheet"}
        warnings: List[str] = []
        if self.config.match_strategy is MatchStrategy.APPEND_ONLY:
            actions = [self._sheet_append_action(df)] if not df.empty else []
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=requested_mode,
                source=source,
                target=target,
                actions=actions,
            )
        formula_columns: Optional[set[Union[str, int]]] = None
        if requested_mode is SyncMode.FULL:
            current_df, _, formula_columns = self.get_sheet_data_with_validation()
        else:
            current_df = self.get_current_sheet_data()
        if not self._require_complete_sheet_read("同步计划"):
            raise RuntimeError("Sheet 读取不完整，拒绝生成写计划")

        if requested_mode is SyncMode.CLONE:
            actions = [self._sheet_clear_action(), self._sheet_write_action(df)]
            actions.append(
                PlanAction(
                    "apply_sheet_config",
                    len(df.columns),
                    {"target": "sheet"},
                    payload=df.copy(),
                )
            )
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=SyncMode.CLONE,
                source=source,
                target=target,
                actions=actions,
                warnings=warnings,
            )

        if requested_mode is SyncMode.OVERWRITE and not self.config.index_column:
            raise ValueError("覆盖同步模式需要指定索引列")

        index_field_types = self._sheet_index_field_types(df, current_df)
        current_index: Mapping[str, int] = {}
        if self.config.index_column:
            self.converter.build_data_index(
                df,
                self.config.index_column,
                index_field_types,
                allow_empty=False,
                context=f"本地数据索引列 '{self.config.index_column}' ",
            )
            try:
                current_index = self.converter.build_data_index(
                    current_df,
                    self.config.index_column,
                    index_field_types,
                    allow_empty=True,
                    context=f"目标 Sheet 索引列 '{self.config.index_column}' ",
                )
            except ValueError as error:
                raise RuntimeError(f"目标 Sheet 索引不安全: {error}") from error

        sync_df = df
        selected_columns: Optional[List[str]] = None
        if requested_mode is SyncMode.FULL and self.config.sheet_protect_formulas:
            if formula_columns is None:
                raise RuntimeError("无法确认远端公式列")
            if self.config.index_column in formula_columns:
                raise ValueError("索引列是公式列，无法安全匹配")
            selected_columns = [
                str(column) for column in df.columns if column not in formula_columns
            ]
            sync_df = df[selected_columns].copy()
        elif self.config.selective_sync.enabled:
            selected_columns = self._get_effective_selective_columns(df)
            sync_df = df[selected_columns].copy()

        if selected_columns is not None:
            actions = self._plan_sheet_selective(
                sync_df,
                current_df,
                requested_mode,
                selected_columns,
                current_index,
                index_field_types,
            )
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=requested_mode,
                source=source,
                target=target,
                actions=actions,
            )

        if requested_mode is SyncMode.OVERWRITE:
            new_hashes = {
                index_hash
                for _, row in sync_df.iterrows()
                if (
                    index_hash := self.converter.get_index_value_hash(
                        row, self.config.index_column, index_field_types
                    )
                )
            }
            rows = [
                row
                for _, row in current_df.iterrows()
                if self.converter.get_index_value_hash(
                    row, self.config.index_column, index_field_types
                )
                not in new_hashes
            ]
            rows.extend(row for _, row in sync_df.iterrows())
            merged = pd.DataFrame(rows)
            actions = (
                [self._sheet_write_action(merged)]
                if not merged.empty
                else [self._sheet_clear_action()]
            )
            actions[0] = PlanAction(
                actions[0].kind,
                actions[0].count,
                actions[0].scope,
                destructive=True,
                clears_values=True,
                payload=actions[0].payload,
            )
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=requested_mode,
                source=source,
                target=target,
                actions=actions,
            )

        updates: List[Tuple[int, pd.Series]] = []
        new_rows: List[pd.Series] = []
        for _, row in sync_df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, index_field_types
            )
            if index_hash and index_hash in current_index:
                if requested_mode is SyncMode.FULL:
                    updates.append((current_index[index_hash], row))
            else:
                new_rows.append(row)

        actions = []
        if updates:
            updated = current_df.copy()
            for row_index, row in updates:
                for column in sync_df.columns:
                    if column in updated.columns:
                        updated.iloc[row_index, updated.columns.get_loc(column)] = row[
                            column
                        ]
            actions.append(self._sheet_write_action(updated))
        if new_rows:
            actions.append(self._sheet_append_action(pd.DataFrame(new_rows)))
        return self._make_plan(
            requested_mode=requested_mode,
            effective_mode=requested_mode,
            source=source,
            target=target,
            actions=actions,
        )

    def plan(self, df: Optional[pd.DataFrame] = None) -> SyncPlan:
        """Build a complete mutation plan using reads and local classification only."""
        if (
            self.config.sync_mode is not SyncMode.CLONE
            and self.config.match_strategy is MatchStrategy.BY_KEY
            and not self.config.index_column
        ):
            raise ValueError("by_key 模式必须配置 index_column")
        if self.config.source_type is SourceType.BITABLE:
            if df is not None:
                raise ValueError("source_type=bitable 不接受本地 DataFrame")
            return self._plan_bitable_source()
        if df is None:
            raise ValueError("source_type=file 必须提供 DataFrame")
        planned_df = (
            self._apply_selective_filter(df)
            if self.config.selective_sync.enabled
            else df.copy()
        )
        if self.config.target_type is TargetType.BITABLE:
            return self._plan_file_bitable(planned_df)
        return self._plan_file_sheet(planned_df)

    def _reset_action_execution_state(self) -> None:
        self._last_action_error_kind = ErrorKind.MUTATION
        self._last_action_applied_count = 0
        self._last_action_accepted_units = 0
        self._last_action_applied_rows = set()
        self._last_action_mutation_complete = False
        self._last_action_remote_outcome = None

    def _record_action_receipt(self, receipt: MutationReceipt) -> None:
        if not hasattr(self, "_last_action_accepted_units"):
            self._reset_action_execution_state()
        accepted = max(0, int(receipt.accepted_count))
        self._last_action_accepted_units += accepted
        for item in receipt.actual_ranges:
            if isinstance(item, A1Range):
                self._last_action_applied_rows.update(
                    range(item.start_row, item.end_row + 1)
                )
        if self._last_action_applied_rows:
            self._last_action_applied_count = len(self._last_action_applied_rows)
        else:
            self._last_action_applied_count += accepted
        self._last_action_remote_outcome = receipt.outcome.value

    def _mark_action_failure(self, kind: ErrorKind) -> bool:
        self._last_action_error_kind = kind
        return False

    @staticmethod
    def _is_auth_error(error: Exception) -> bool:
        from api import FeishuAPIError

        return isinstance(error, FeishuAPIError) and (
            error.code in {99991661, 99991663, 99991664, 99991668}
            or error.http_status in {401, 403}
        )

    @staticmethod
    def _is_resource_error(error: Exception) -> bool:
        from api import FeishuAPIError

        return isinstance(error, FeishuAPIError) and error.http_status == 404

    def _applied_action_prefix(self, action: PlanAction) -> Optional[PlanAction]:
        if self._last_action_mutation_complete:
            return action
        if (
            self._last_action_applied_count <= 0
            and self._last_action_accepted_units <= 0
        ):
            return None
        count = self._last_action_applied_count or self._last_action_accepted_units
        if action.count:
            count = min(action.count, count)
        scope = dict(action.scope)
        scope.update(
            {
                "partial": True,
                "accepted_units": self._last_action_accepted_units,
                "requested_count": action.count,
            }
        )
        if self._last_action_remote_outcome:
            scope["remote_outcome"] = self._last_action_remote_outcome
        return PlanAction(
            action.kind,
            count,
            scope,
            destructive=action.destructive,
            clears_values=action.clears_values,
        )

    def _action_error(self, action: PlanAction, message: str) -> Mapping[str, Any]:
        error: Dict[str, Any] = {
            "kind": self._last_action_error_kind.value,
            "message": message,
            "failed_action": action.kind,
            "accepted_count": self._last_action_applied_count,
            "requested_count": action.count,
        }
        if self._last_action_remote_outcome:
            error["remote_outcome"] = self._last_action_remote_outcome
            error["unknown"] = (
                self._last_action_remote_outcome
                == MutationOutcome.UNKNOWN_OUTCOME.value
            )
        return error

    def _execute_action(self, action: PlanAction) -> bool:
        if action.kind == "create_fields":
            payload = cast(Mapping[str, Any], action.payload)
            receipt = self._bitable_backend().create_field(
                cast(str, self.config.app_token),
                cast(str, self.config.table_id),
                str(payload["field_name"]),
                cast(int, payload["suggested_type"]),
            )
            self._record_action_receipt(receipt)
            if receipt.outcome is not MutationOutcome.ACCEPTED:
                return self._mark_action_failure(ErrorKind.MUTATION)
            self._last_action_mutation_complete = True
            return True
        if action.kind in {"create_records", "update_records"}:
            records = cast(List[CanonicalRecord], action.payload)
            operation = "create" if action.kind == "create_records" else "update"
            processor = (
                self._bitable_backend().batch_create
                if operation == "create"
                else self._bitable_backend().batch_update
            )
            success, receipts = self.process_typed_bitable_batches(
                records, processor, receipt_callback=self._record_action_receipt
            )
            if not success:
                return self._mark_action_failure(ErrorKind.MUTATION)
            self._last_action_mutation_complete = True
            try:
                verified = self._verify_bitable_mutation(operation, records, receipts)
            except Exception:
                self._last_action_error_kind = ErrorKind.VERIFICATION
                raise
            if not verified:
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            return True
        if action.kind == "delete_records":
            record_ids = cast(List[str], action.payload)
            success, receipts = self.process_typed_bitable_batches(
                record_ids,
                self._bitable_backend().batch_delete,
                receipt_callback=self._record_action_receipt,
            )
            if not success:
                return self._mark_action_failure(ErrorKind.MUTATION)
            self._last_action_mutation_complete = True
            try:
                verified = self._verify_bitable_mutation("delete", record_ids, receipts)
            except Exception:
                self._last_action_error_kind = ErrorKind.VERIFICATION
                raise
            if not verified:
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            return True
        if action.kind == "clear_range":
            return self._typed_sheet_clear(cast(str, action.payload))
        if action.kind == "write_range":
            payload = cast(Mapping[str, Any], action.payload)
            return self._typed_sheet_write(cast(List[List[Any]], payload["values"]))
        if action.kind == "append_rows":
            payload = cast(Mapping[str, Any], action.payload)
            return self._typed_sheet_append(
                cast(List[List[Any]], payload["values"]),
                header_width=int(payload["header_width"]),
            )
        if action.kind == "write_columns":
            payload = cast(Mapping[str, Any], action.payload)
            return self._typed_sheet_selective_write(
                cast(Dict[str, List[Any]], payload["column_data"]),
                cast(Dict[str, int], payload["column_positions"]),
                start_row=int(payload["start_row"]),
                max_gap=int(payload["max_gap"]),
                header_width=int(payload["header_width"]),
            )
        if action.kind == "apply_sheet_config":
            success = self._setup_sheet_intelligence(cast(pd.DataFrame, action.payload))
            if success:
                self._last_action_applied_count = action.count
                self._last_action_accepted_units = action.count
                self._last_action_mutation_complete = True
            return success
        raise ValueError(f"未知 plan action: {action.kind}")

    def _refresh_and_verify_created_fields(
        self, actions: List[PlanAction]
    ) -> Tuple[bool, str]:
        """Refresh backend schema cache and validate every planned field."""
        if not self.config.app_token or not self.config.table_id:
            return False, "目标 Bitable 配置不完整"
        from api.bitable_backend import field_kind_from_type

        fields = self._bitable_backend().list_fields(
            self.config.app_token, self.config.table_id
        )
        by_name = {field.name: field for field in fields}
        backend_kind = BitableBackendKind(self.config.bitable_api_backend)
        for action in actions:
            payload = cast(Mapping[str, Any], action.payload)
            name = str(payload["field_name"])
            suggested_type = int(payload["suggested_type"])
            actual = by_name.get(name)
            if actual is None:
                return False, f"字段 '{name}' 创建后未出现在服务端 schema 中"
            expected_kind = field_kind_from_type(suggested_type)
            expected_multiple = suggested_type == 4
            if (
                not actual.writable
                or actual.kind is not expected_kind
                or actual.multiple != expected_multiple
            ):
                return False, f"字段 '{name}' 创建后的写入形状与计划不兼容"
            if (
                backend_kind is BitableBackendKind.BITABLE_V1
                and actual.raw_type != suggested_type
            ):
                return False, f"字段 '{name}' 创建后的 raw_type 与计划不一致"
        return True, ""

    def execute_plan(self, plan: SyncPlan) -> SyncOutcome:
        """Execute ordered actions, stopping at the first failed action."""
        applied: List[PlanAction] = []
        verification: List[Mapping[str, Any]] = []
        created_field_actions: List[PlanAction] = []
        fields_refreshed = False
        if not plan.actions:
            return SyncOutcome(
                OutcomeStatus.NOOP,
                plan,
                warnings=tuple(plan.warnings),
            )
        for action in plan.actions:
            if (
                action.kind != "create_fields"
                and created_field_actions
                and not fields_refreshed
            ):
                refresh_error: Optional[Exception] = None
                try:
                    valid, message = self._refresh_and_verify_created_fields(
                        created_field_actions
                    )
                except Exception as error:
                    refresh_error = error
                    valid, message = False, str(error)
                if not valid:
                    error_kind = ErrorKind.VERIFICATION
                    if refresh_error is not None:
                        if self._is_auth_error(refresh_error):
                            error_kind = ErrorKind.AUTH
                        elif self._is_resource_error(refresh_error):
                            error_kind = ErrorKind.RESOURCE
                    return SyncOutcome(
                        OutcomeStatus.PARTIAL,
                        plan,
                        applied=tuple(applied),
                        verification=tuple(verification),
                        warnings=tuple(plan.warnings),
                        error={
                            "kind": error_kind.value,
                            "message": message,
                            "failed_action": "create_fields",
                        },
                    )
                fields_refreshed = True
                if self.config.verify_remote_writes:
                    created_ids = {id(item) for item in created_field_actions}
                    verification = [
                        (
                            {
                                "kind": applied_action.kind,
                                "status": "verified",
                                "ok": True,
                            }
                            if id(applied_action) in created_ids
                            else verification_item
                        )
                        for verification_item, applied_action in zip(
                            verification, applied
                        )
                    ]
            self._reset_action_execution_state()
            try:
                success = self._execute_action(action)
            except Exception as error:
                if self._is_auth_error(error):
                    self._last_action_error_kind = ErrorKind.AUTH
                elif self._is_resource_error(error):
                    self._last_action_error_kind = ErrorKind.RESOURCE
                prefix = self._applied_action_prefix(action)
                if prefix is not None:
                    applied.append(prefix)
                if self._last_action_error_kind is ErrorKind.VERIFICATION:
                    verification.append(
                        {"kind": action.kind, "status": "failed", "ok": False}
                    )
                uncertain = self._last_action_remote_outcome in {
                    MutationOutcome.PARTIAL.value,
                    MutationOutcome.UNKNOWN_OUTCOME.value,
                }
                status = (
                    OutcomeStatus.PARTIAL
                    if applied or uncertain
                    else OutcomeStatus.FAILED
                )
                return SyncOutcome(
                    status,
                    plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(plan.warnings),
                    error=self._action_error(action, str(error)),
                )
            if not success:
                prefix = self._applied_action_prefix(action)
                if prefix is not None:
                    applied.append(prefix)
                if self._last_action_error_kind is ErrorKind.VERIFICATION:
                    verification.append(
                        {"kind": action.kind, "status": "failed", "ok": False}
                    )
                uncertain = self._last_action_remote_outcome in {
                    MutationOutcome.PARTIAL.value,
                    MutationOutcome.UNKNOWN_OUTCOME.value,
                }
                status = (
                    OutcomeStatus.PARTIAL
                    if applied or uncertain
                    else OutcomeStatus.FAILED
                )
                return SyncOutcome(
                    status,
                    plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(plan.warnings),
                    error=self._action_error(action, f"action failed: {action.kind}"),
                )
            applied.append(action)
            if action.kind == "create_fields":
                created_field_actions.append(action)
            verification.append(
                {
                    "kind": action.kind,
                    "status": (
                        "verified"
                        if self.config.verify_remote_writes
                        and action.kind not in {"create_fields", "apply_sheet_config"}
                        else (
                            "not_supported"
                            if self.config.verify_remote_writes
                            and action.kind == "apply_sheet_config"
                            else "not_requested"
                        )
                    ),
                    "ok": True,
                }
            )
        if created_field_actions and not fields_refreshed:
            refresh_error = None
            try:
                valid, message = self._refresh_and_verify_created_fields(
                    created_field_actions
                )
            except Exception as error:
                refresh_error = error
                valid, message = False, str(error)
            if not valid:
                error_kind = ErrorKind.VERIFICATION
                if refresh_error is not None:
                    if self._is_auth_error(refresh_error):
                        error_kind = ErrorKind.AUTH
                    elif self._is_resource_error(refresh_error):
                        error_kind = ErrorKind.RESOURCE
                return SyncOutcome(
                    OutcomeStatus.PARTIAL,
                    plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(plan.warnings),
                    error={
                        "kind": error_kind.value,
                        "message": message,
                        "failed_action": "create_fields",
                    },
                )
            if self.config.verify_remote_writes:
                created_ids = {id(item) for item in created_field_actions}
                verification = [
                    (
                        {"kind": item.kind, "status": "verified", "ok": True}
                        if id(item) in created_ids
                        else verification_item
                    )
                    for verification_item, item in zip(verification, applied)
                ]
        return SyncOutcome(
            OutcomeStatus.SUCCESS,
            plan,
            applied=tuple(applied),
            verification=tuple(verification),
            warnings=tuple(plan.warnings),
        )

    def sync(self, df: pd.DataFrame) -> bool:
        """执行同步"""
        if self.config.source_type is SourceType.BITABLE:
            self.logger.error("source_type=bitable 不接受本地 DataFrame")
            return False
        try:
            outcome = self.execute_plan(self.plan(df))
        except Exception as error:
            self.logger.error(f"同步计划或执行失败: {error}")
            return False
        if self.config.target_type is TargetType.BITABLE:
            self.converter.report_conversion_stats()
        return outcome.ok

    def _show_field_analysis_summary(
        self, df: pd.DataFrame, field_types: Dict[str, FieldSchema]
    ):
        """显示字段分析摘要"""
        self.logger.info("\n📋 字段类型映射摘要:")
        self.logger.info("-" * 50)

        for col_name in df.columns:
            if col_name in field_types:
                schema = field_types[col_name]
                self.logger.info(
                    f"  {col_name} → {schema.kind.value} "
                    f"(后端类型: {schema.raw_type}, writable={schema.writable})"
                )
            else:
                self.logger.warning(f"  {col_name} → 未知字段类型")

        self.logger.info("-" * 50)
