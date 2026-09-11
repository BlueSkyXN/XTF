#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XTF 2.0 的单一 typed 同步服务。

``SyncService`` 消费不可变 ``RuntimeConfig``，通过只读 planner 生成进程内
``ExecutionPlan``，再按 action 顺序执行 snapshot precondition、mutation 和
verification，最终返回 ``SyncResult``。Bitable 与 Sheet 共用 key/mode/result
语义，但分别编译和执行目标特定 action；本模块不提供旧 bool facade。
"""

import pandas as pd
import numbers
from dataclasses import replace
from datetime import date, datetime
from typing import Optional, Dict, Any, List, Mapping, Union, Tuple, Sequence, cast

from .config import MatchStrategy, SourceType, SyncMode, TargetType
from .converter import DataConverter
from .bootstrap import bootstrap_runtime
from .compiler import BitablePlanCompiler, SheetPlanCompiler
from .key_policy import KeyPolicy
from .mode_policy import ModeDecision, ModePolicy
from .verification import cells_equal, sheet_values_equal, wait_for_readback
from .reconcile import Reconciler
from .plan import (
    AppendRowsAction,
    ApplySheetConfigAction,
    ClearRangeAction,
    CreateFieldAction,
    CreateRecordsAction,
    DeleteRecordsAction,
    ErrorKind,
    ExecutionAction,
    ExecutionPlan,
    OutcomeStatus,
    PlanActionDocument,
    SnapshotPrecondition,
    SyncResult,
    UpdateRecordsAction,
    VerificationPolicy,
    WriteColumnsAction,
    WriteRangeAction,
)
from .runtime_config import RuntimeBitableTarget, RuntimeConfig, RuntimeSheetTarget
from .snapshot import (
    BitableSnapshot,
    SheetSnapshot,
    SheetLayout,
    SourceTable,
    content_fingerprint,
)
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
    RecordReadResult,
    SheetAPI,
)


class SyncService:
    """XTF 2.0 单一 typed 同步服务。"""

    # Client work budget, not an assertion about the server's hidden scan limit.
    MAX_FORMULA_PROBES = 128

    def __init__(self, config: RuntimeConfig):
        """
        初始化同步服务

        Args:
            config: 不可变运行时配置
        """
        self.runtime = config
        self.source = config.source
        self.target = config.target
        self.sync_config = config.sync
        self.control = config.control
        self.conversion = config.conversion
        self.output = config.output
        self._last_action_error_kind = ErrorKind.MUTATION
        self._last_action_applied_count = 0
        self._last_action_accepted_units = 0
        self._last_action_applied_rows: set[int] = set()
        self._last_action_mutation_complete = False
        self._last_action_remote_outcome: Optional[str] = None
        self._last_action_revision: int | str | None = None

        dependencies = bootstrap_runtime(config)
        self.logger = dependencies.logger
        self.api_client = dependencies.transport
        self.auth = dependencies.auth
        self.api: Union[BitableBackend, SheetAPI] = dependencies.target
        # 初始化数据转换器
        self.converter = DataConverter(
            config.target.type,
            datetime_index_granularity=config.sync.index.datetime_granularity,
            datetime_index_timezone=config.sync.index.timezone,
        )
        # 缓存工作表网格属性，避免重复请求
        self._sheet_grid_cache: Optional[Tuple[int, int]] = None
        self._sheet_grid_cache_key: Optional[Tuple[str, str]] = None
        self._sheet_read_complete = True
        self._last_sheet_read_range: Optional[str] = None
        self._last_bitable_read_result: Optional[RecordReadResult] = None
        self._planned_target_snapshot: Optional[
            Union[BitableSnapshot, SheetSnapshot]
        ] = None
        self._planned_sheet_index_field_types: Dict[str, Any] = {}
        self._planned_bitable_schema_fingerprint: Optional[str] = None
        self._expected_bitable_schema_fingerprint: Optional[str] = None
        self._expected_bitable_revision: int | str | None = None
        self._expected_bitable_snapshot: Optional[BitableSnapshot] = None
        self._expected_sheet_snapshot: Optional[SheetSnapshot] = None
        self._last_action_failure_message: Optional[str] = None
        self._mode_decision: Optional[ModeDecision] = None

    # ========== 多维表格专用方法 ==========

    def _bitable_backend(self) -> BitableBackend:
        return cast(BitableBackend, self.api)

    def _sheet_api(self) -> SheetAPI:
        return cast(SheetAPI, self.api)

    def _bitable_target(self) -> RuntimeBitableTarget:
        if not isinstance(self.target, RuntimeBitableTarget):
            raise TypeError("当前 runtime 目标不是 Bitable")
        return self.target

    def _sheet_target(self) -> RuntimeSheetTarget:
        if not isinstance(self.target, RuntimeSheetTarget):
            raise TypeError("当前 runtime 目标不是 Sheet")
        return self.target

    @staticmethod
    def _schema_fingerprint(fields: Tuple[FieldSchema, ...]) -> str:
        return content_fingerprint(
            [
                {
                    "id": field.id,
                    "name": field.name,
                    "kind": field.kind.value,
                    "multiple": field.multiple,
                    "writable": field.writable,
                    "raw_type": field.raw_type,
                    "raw_properties": field.raw_properties,
                }
                for field in fields
            ]
        )

    def plan_fields(
        self, df: pd.DataFrame
    ) -> Tuple[List[ExecutionAction], Dict[str, FieldSchema]]:
        """Read target schemas and plan missing fields without mutating Feishu."""
        if self.target.type is not TargetType.BITABLE:
            return [], {}
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            raise ValueError("多维表格的 app_token 或 table_id 未配置")

        from api.bitable_backend import field_is_writable, field_kind_from_type

        existing_fields = self._bitable_backend().list_fields(
            self._bitable_target().app_token, self._bitable_target().table_id
        )
        self._planned_bitable_schema_fingerprint = self._schema_fingerprint(
            tuple(existing_fields)
        )
        field_types = {field.name: field for field in existing_fields}
        missing_fields = [name for name in df.columns if name not in field_types]
        if missing_fields and not self._bitable_target().create_missing_fields:
            raise ValueError(
                "目标 Bitable 缺少字段且 create_missing_fields=false: "
                f"{[str(name) for name in missing_fields]}"
            )
        self._planned_bitable_fields = tuple(field_types.values())
        if not missing_fields:
            return [], field_types

        actions: List[ExecutionAction] = []
        for raw_name in missing_fields:
            field_name = str(raw_name)
            analysis = self.converter.analyze_excel_column_data_enhanced(
                df,
                field_name,
                self.conversion.strategy.value,
                self.conversion,
            )
            suggested_type = int(analysis["suggested_feishu_type"])
            actions.append(
                BitablePlanCompiler.create_field(
                    field_name,
                    suggested_type,
                    scope={"target": "bitable", "field": field_name},
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
        self._planned_bitable_fields = tuple(field_types.values())
        return actions, field_types

    def get_all_bitable_records(
        self, field_names: Optional[List[str]] = None
    ) -> List[Dict]:
        """获取所有多维表格记录

        Args:
            field_names: 指定返回的字段名称列表，为None时返回全部字段。
                         用于减少不必要的数据传输，提升查询性能。
        """
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            self.logger.error("多维表格的 app_token 或 table_id 未配置")
            return []
        result = self._bitable_backend().list_records(
            self._bitable_target().app_token,
            self._bitable_target().table_id,
            field_names=field_names,
        )
        if not result.complete:
            raise RuntimeError("多维表格读取不完整，拒绝继续同步")
        if result.ignored_fields:
            raise RuntimeError("多维表格读取存在 ignored_fields，拒绝继续同步")
        self._last_bitable_read_result = result
        self._planned_target_snapshot = BitableSnapshot.from_result(result)
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
                    self.sync_config.index.datetime_granularity,
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

    def _plan_bitable_source(self) -> ExecutionPlan:
        """Plan source-Bitable differences without mutating the target.

        ``full`` 只更新发生变化的字段并新增缺失记录；``incremental``
        只新增缺失记录。两种模式都不会删除目标表记录或复制 Base 结构。
        """
        if self.source.type is not SourceType.BITABLE:
            raise ValueError("Bitable source planner 仅支持 source_type=bitable")
        if self.target.type is not TargetType.BITABLE:
            raise ValueError("远端多维表格数据源只能同步到多维表格")
        if not all(
            (
                self.source.app_token,
                self.source.table_id,
                self._bitable_target().app_token,
                self._bitable_target().table_id,
                self.sync_config.index.column,
            )
        ):
            raise ValueError("源表、目标表和 index_column 配置不完整")

        source_app_token = cast(str, self.source.app_token)
        source_table_id = cast(str, self.source.table_id)
        target_app_token = cast(str, self._bitable_target().app_token)
        target_table_id = cast(str, self._bitable_target().table_id)
        backend = self._bitable_backend()
        source_fields = backend.list_fields(source_app_token, source_table_id)
        target_fields = backend.list_fields(target_app_token, target_table_id)
        source_by_name = {field.name: field for field in source_fields}
        target_by_name = {field.name: field for field in target_fields}
        self._planned_bitable_fields = tuple(target_fields)

        requested_names: List[str]
        explicit_selection = bool(
            self.sync_config.selective.enabled and self.sync_config.selective.columns
        )
        if explicit_selection:
            requested_names = list(self.sync_config.selective.columns or [])
            unknown = [name for name in requested_names if name not in source_by_name]
            if unknown:
                raise ValueError(f"源表不存在 selective_sync 字段: {unknown}")
        else:
            requested_names = [field.name for field in source_fields]

        index_column = str(self.sync_config.index.column)
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
                BitableBackendKind(self._bitable_target().backend),
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
            copy_names if self.sync_config.mode is SyncMode.FULL else [index_column]
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
        self._planned_target_snapshot = BitableSnapshot.from_result(target_result)

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
                self.sync_config.index.datetime_granularity,
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

            if self.sync_config.mode is SyncMode.INCREMENTAL:
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

        actions: List[ExecutionAction] = []
        if records_to_update:
            actions.append(
                BitablePlanCompiler.update_records(
                    records_to_update,
                    scope={"target": "bitable"},
                    clears_values=clears_values,
                )
            )
        if records_to_create:
            actions.append(
                BitablePlanCompiler.create_records(
                    records_to_create,
                    scope={"target": "bitable"},
                )
            )
        warnings = (
            [f"跳过 {len(skipped_fields)} 个不可复制字段"] if skipped_fields else []
        )
        if clears_values:
            warnings.append("full 同步将清空目标记录中的一个或多个字段值")
        return self._make_plan(
            requested_mode=self.sync_config.mode,
            effective_mode=self.sync_config.mode,
            source={"type": "bitable", "records": len(source_result.records)},
            target={"type": "bitable", "records": len(target_result.records)},
            actions=actions,
            warnings=warnings,
        )

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
            self.control.batch_size,
            max_batch_size or self.control.batch_size,
        )
        receipts: List[MutationReceipt] = []
        for batch_index, start in enumerate(
            range(0, len(items), effective_batch_size), start=1
        ):
            batch = items[start : start + effective_batch_size]
            receipt = processor_func(
                self._bitable_target().app_token,
                self._bitable_target().table_id,
                batch,
            )
            if not isinstance(receipt, MutationReceipt):
                raise TypeError("typed backend mutation 必须返回 MutationReceipt")
            receipts.append(receipt)
            self.logger.info(
                f"第 {batch_index} 批：服务端接受 {receipt.accepted_count}/{len(batch)} 条；累计 {start + receipt.accepted_count}/{len(items)} 条"
            )
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

    def _read_bitable_verification(
        self, backend, record_ids: List[str], field_names=None
    ) -> RecordReadResult:
        """Read by the GET limit and split Base v3 field projections at 100.

        This aggregates explicit read results; it does not replay a mutation or
        assume that an accepted write is immediately visible at the server.
        """
        limit = getattr(backend, "max_batch_get_size", 100)
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            limit = 100  # Conservative for protocol-compatible test/legacy adapters.
        projections = (
            [None]
            if not field_names
            else [
                tuple(field_names[i : i + 100]) for i in range(0, len(field_names), 100)
            ]
        )
        records: Dict[str, Dict[str, Any]] = {}
        schemas: Dict[str, Any] = {}
        absent = set()
        ignored = []
        complete = True
        timezone = None
        for start in range(0, len(record_ids), limit):
            ids = record_ids[start : start + limit]
            projection_presence = None
            for projection in projections:
                result = backend.batch_get_records(
                    self._bitable_target().app_token,
                    self._bitable_target().table_id,
                    ids,
                    field_names=projection,
                )
                if result.timezone is not None:
                    if timezone is not None and timezone != result.timezone:
                        complete = False
                    timezone = result.timezone
                present_ids = {record.record_id for record in result.records}
                missing_ids = set(result.record_not_found)
                presence = (frozenset(present_ids), frozenset(missing_ids))
                complete = bool(
                    complete
                    and result.complete
                    and len(present_ids) == len(result.records)
                    and not present_ids & missing_ids
                    and present_ids | missing_ids == set(ids)
                    and (projection_presence is None or projection_presence == presence)
                )
                projection_presence = presence
                absent.update(missing_ids)
                ignored.extend(result.ignored_fields)
                for schema in result.fields:
                    if schema.name in schemas and schemas[schema.name] != schema:
                        complete = False
                    schemas[schema.name] = schema
                for record in result.records:
                    if record.record_id is not None:
                        records.setdefault(record.record_id, {}).update(record.fields)
        return RecordReadResult(
            records=tuple(
                CanonicalRecord(rid, values) for rid, values in records.items()
            ),
            fields=tuple(schemas.values()),
            complete=complete,
            backend=BitableBackendKind(self._bitable_target().backend),
            ignored_fields=tuple(ignored),
            record_not_found=tuple(sorted(absent)),
            timezone=timezone,
        )

    def _wait_for_confirmation(self, check, label: str) -> bool:
        try:
            result = wait_for_readback(
                check,
                timeout=self.sync_config.verify_timeout_seconds,
                interval=self.sync_config.verify_interval_seconds,
                on_wait=lambda: self.logger.info(
                    f"{label}暂未可见，正在等待；不会重复写入"
                ),
            )
        except Exception:
            self._last_action_error_kind = ErrorKind.VERIFICATION
            self._last_action_confirmation: Dict[str, Any] = {"status": "read_failed"}
            raise
        self._last_action_confirmation = {
            "attempts": result.attempts,
            "elapsed_seconds": round(result.elapsed_seconds, 3),
            "status": "verified" if result.verified else "visibility_timeout",
        }
        if not result.verified:
            self._last_action_failure_message = (
                f"{label}在等待窗口内仍未确认。写请求已接受，不代表未写入；"
                "已停止后续操作，请先读取目标状态，不要直接重跑追加或清空任务。"
            )
            if result.last_error:
                self._last_action_failure_message += (
                    f" 最后一次读取错误: {result.last_error}"
                )
        return result.verified

    def _verify_bitable_mutation(
        self,
        operation: str,
        requested: List[CanonicalRecord] | List[str],
        receipts: List[MutationReceipt],
    ) -> bool:
        if not self.sync_config.verify_remote_writes or not receipts:
            return True
        backend = self._bitable_backend()
        records = [item for item in requested if isinstance(item, CanonicalRecord)]
        if operation == "delete":
            record_ids = [str(item) for item in requested]
        elif operation == "create":
            record_ids = [rid for receipt in receipts for rid in receipt.record_ids]
            if len(record_ids) != len(records) or len(set(record_ids)) != len(
                record_ids
            ):
                self._set_receipt_readback(receipts, ReadbackStatus.UNKNOWN, 0)
                self._last_action_failure_message = (
                    "创建响应没有完整且唯一的记录 ID，不能确认结果；不要重复创建"
                )
                return False
            records = [
                CanonicalRecord(rid, record.fields)
                for rid, record in zip(record_ids, records)
            ]
        else:
            record_ids = [record.record_id for record in records if record.record_id]
        if not record_ids:
            return not requested
        # Remember successful chunks: later probes need only the unresolved IDs.
        pending = set(record_ids)
        expected = {record.record_id: record for record in records}
        projection = (
            tuple(dict.fromkeys(name for record in records for name in record.fields))
            or None
        )

        def check() -> bool:
            observed = self._read_bitable_verification(
                backend,
                [rid for rid in record_ids if rid in pending],
                field_names=projection,
            )
            if not observed.complete or observed.ignored_fields:
                raise RuntimeError("写后读取不完整或字段被忽略，不能当作可见性延迟继续")
            if operation == "delete":
                pending.difference_update(observed.record_not_found)
            else:
                schemas = {field.name: field for field in observed.fields}
                for record in observed.records:
                    target = expected.get(record.record_id)
                    if target is not None and all(
                        cells_equal(
                            value,
                            record.fields.get(name),
                            schemas.get(name),
                            observed.timezone,
                        )
                        for name, value in target.fields.items()
                    ):
                        pending.discard(record.record_id)
            self._last_action_confirmed_count = len(record_ids) - len(pending)
            return not pending

        verified = self._wait_for_confirmation(check, "Bitable 写入结果")
        # Successful IDs need not form a prefix. Count within each actual batch.
        offset = 0
        for index, receipt in enumerate(receipts):
            ids = record_ids[offset : offset + receipt.requested_count]
            count = sum(rid not in pending for rid in ids)
            receipts[index] = replace(
                receipt,
                verified_count=count,
                readback=(
                    ReadbackStatus.VERIFIED
                    if count == len(ids)
                    else ReadbackStatus.UNKNOWN
                ),
            )
            offset += receipt.requested_count
        return verified

    @staticmethod
    def _set_receipt_readback(
        receipts: List[MutationReceipt],
        status: ReadbackStatus,
        verified_count: int,
    ) -> None:
        """Dataclass 保持 frozen；engine 用返回副本记录本次读回状态。"""

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
            code = receipt.raw_metadata.get("error_code")
            status = receipt.raw_metadata.get("http_status")
            kind = (
                ErrorKind.AUTH
                if code in {10003, 99991661, 99991663, 99991664, 99991668}
                or status in {401, 403}
                else ErrorKind.RESOURCE if status == 404 else ErrorKind.MUTATION
            )
            self._last_action_failure_message = receipt.raw_metadata.get("error")
            return self._mark_action_failure(kind)
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
        if self.sync_config.verify_remote_writes and not skip_data_readback:
            if not expected_ranges:
                self.logger.error(
                    "Sheet 写后读回范围未知，无法证明 mutation 已完整应用"
                )
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            if (
                not isinstance(self.api, SheetAPI)
                or not self._sheet_target().spreadsheet_token
            ):
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            remaining = dict(expected_ranges)

            def check_values() -> bool:
                for range_text, expected in tuple(remaining.items()):
                    observed = self._sheet_api().get_sheet_data(
                        self._sheet_target().spreadsheet_token,
                        range_text,
                        value_render_option=(
                            "Formula"
                            if any(
                                isinstance(value, dict)
                                and value.get("type") == "formula"
                                for row in expected
                                for value in row
                            )
                            else "UnformattedValue"
                        ),
                    )
                    if sheet_values_equal(expected, observed):
                        remaining.pop(range_text)
                return not remaining

            if not self._wait_for_confirmation(check_values, "Sheet 单元格结果"):
                return self._mark_action_failure(ErrorKind.VERIFICATION)
            self._last_action_confirmed_count = self._last_action_applied_count

        if not self._sheet_target().verify_formulas or not verify_formulas:
            return True
        if not actual_ranges:
            self.logger.error("公式验证范围未知：mutation 未返回可证明的实际范围")
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        if (
            not isinstance(self.api, SheetAPI)
            or not self._sheet_target().spreadsheet_token
        ):
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        width = header_width if header_width is not None else 0
        formula_ranges = actual_ranges
        if skip_header_row:
            formula_ranges = [
                A1Range(
                    item.sheet_id,
                    max(item.start_row, self._sheet_target().start_row + 1),
                    item.end_row,
                    item.start_col,
                    item.end_col,
                )
                for item in actual_ranges
                if item.end_row > self._sheet_target().start_row
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
        return self._verify_formula_ranges(ranges)

    def _verify_formula_ranges(self, ranges: List[str]) -> bool:
        """Subdivide truncated scans; count only complete disjoint leaf ranges."""
        if not isinstance(self.api, SheetAPI):
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        target = self._sheet_target()
        if not target.spreadsheet_token or not target.sheet_id:
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        pending = [ranges]
        complete_ranges = 0
        requests = 0
        failure = "公式扫描次数达到本次上限；剩余区域未确认"
        failure_kind = ErrorKind.VERIFICATION
        while pending and requests < self.MAX_FORMULA_PROBES:
            group = pending.pop()
            requests += 1
            try:
                result = self.api.verify_formulas(
                    target.spreadsheet_token,
                    [str(target.sheet_id)],
                    group,
                    max_locations_per_error=target.formula_max_locations,
                )
            except Exception as error:
                failure = f"Sheet AI 公式读取失败: {error}"
                if self._is_auth_error(error):
                    failure_kind = ErrorKind.AUTH
                elif self._is_resource_error(error):
                    failure_kind = ErrorKind.RESOURCE
                break
            if result.passed:
                complete_ranges += len(group)
                continue
            if (
                result.status not in {"partial", "success"}
                or result.has_more is not True
                or result.total_errors
            ):
                failure = f"Sheet AI 公式扫描未通过: status={result.status}, has_more={result.has_more}, ranges={group}"
                break
            if len(group) > 1:
                middle = len(group) // 2
                pending.extend([group[middle:], group[:middle]])
                continue
            region = A1Range.parse(f"{target.sheet_id}!{group[0]}")
            if region.row_count > 1:
                middle = (region.start_row + region.end_row) // 2
                parts = [
                    A1Range(
                        region.sheet_id,
                        region.start_row,
                        middle,
                        region.start_col,
                        region.end_col,
                    ),
                    A1Range(
                        region.sheet_id,
                        middle + 1,
                        region.end_row,
                        region.start_col,
                        region.end_col,
                    ),
                ]
            elif region.col_count > 1:
                middle = (region.start_col + region.end_col) // 2
                parts = [
                    A1Range(
                        region.sheet_id,
                        region.start_row,
                        region.end_row,
                        region.start_col,
                        middle,
                    ),
                    A1Range(
                        region.sheet_id,
                        region.start_row,
                        region.end_row,
                        middle + 1,
                        region.end_col,
                    ),
                ]
            else:
                failure = f"单个单元格 {group[0]} 的公式扫描仍被截断，不能确认完整结果"
                break
            pending.extend([[part.text.split("!", 1)[1]] for part in reversed(parts)])
        else:
            if not pending:
                detail = getattr(self, "_last_action_confirmation", {})
                self._last_action_confirmation = {
                    **detail,
                    "formula_scan": {
                        "status": "verified",
                        "requests": requests,
                        "complete_ranges": complete_ranges,
                    },
                }
                return True
        detail = getattr(self, "_last_action_confirmation", {})
        self._last_action_confirmation = {
            **detail,
            "formula_scan": {
                "status": "failed_or_incomplete",
                "requests": requests,
                "complete_ranges": complete_ranges,
            },
        }
        self._last_action_failure_message = failure + "；写入不会重放。"
        self.logger.error(self._last_action_failure_message)
        return self._mark_action_failure(failure_kind)

    def _typed_sheet_write(
        self, values: List[List[Any]], *, verify_formulas: bool = True
    ) -> bool:
        if (
            not values
            or not isinstance(self.api, SheetAPI)
            or not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return False
        end_row = self._sheet_target().start_row + len(values) - 1
        end_col = self.api.start_col_num + len(values[0]) - 1
        a1 = A1Range(
            str(self._sheet_target().sheet_id),
            self._sheet_target().start_row,
            end_row,
            self.api.start_col_num,
            end_col,
        )
        receipt = self.api.write_values(
            self._sheet_target().spreadsheet_token, a1.text, values
        )
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
        self,
        values: List[List[Any]],
        *,
        header_width: int,
        start_row: Optional[int] = None,
    ) -> bool:
        if (
            not values
            or not isinstance(self.api, SheetAPI)
            or not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return False
        append_start_row = (
            start_row if start_row is not None else self._sheet_target().start_row
        )
        placeholder_end_row = append_start_row + len(values) - 1
        end_col = self.api.start_col_num + len(values[0]) - 1
        requested = A1Range(
            str(self._sheet_target().sheet_id),
            append_start_row,
            placeholder_end_row,
            self.api.start_col_num,
            end_col,
        )
        receipt = self.api.append_values(
            self._sheet_target().spreadsheet_token, requested.text, values
        )
        if receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        expected: Dict[str, List[List[Any]]] = {}
        actual_ranges = [
            item for item in receipt.actual_ranges if isinstance(item, A1Range)
        ]
        source_slices = receipt.raw_metadata.get("source_slices")
        if isinstance(source_slices, (list, tuple)) and len(source_slices) == len(
            actual_ranges
        ):
            actual_text = {item.text for item in actual_ranges}
            for item in source_slices:
                if not isinstance(item, Mapping):
                    expected = {}
                    break
                range_text = item.get("range")
                offsets = (
                    item.get("row_offset"),
                    item.get("col_offset"),
                    item.get("row_count"),
                    item.get("col_count"),
                )
                if (
                    not isinstance(range_text, str)
                    or range_text not in actual_text
                    or any(
                        not isinstance(value, int) or isinstance(value, bool)
                        for value in offsets
                    )
                ):
                    expected = {}
                    break
                row_offset, col_offset, row_count, col_count = cast(
                    Tuple[int, int, int, int], offsets
                )
                if (
                    row_offset < 0
                    or col_offset < 0
                    or row_count <= 0
                    or col_count <= 0
                    or row_offset + row_count > len(values)
                    or col_offset + col_count > len(values[0])
                ):
                    expected = {}
                    break
                expected[range_text] = [
                    row[col_offset : col_offset + col_count]
                    for row in values[row_offset : row_offset + row_count]
                ]
        elif actual_ranges and sum(item.row_count for item in actual_ranges) == len(
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
            or not self._sheet_target().spreadsheet_token
        ):
            return False
        receipt = self.api.batch_update_values(
            self._sheet_target().spreadsheet_token, value_ranges
        )
        if complete_action and receipt.outcome is MutationOutcome.ACCEPTED:
            self._last_action_mutation_complete = True
        expected = (
            {
                str(item["range"]): [list(row) for row in item["values"]]
                for item in value_ranges
            }
            if self.sync_config.verify_remote_writes
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
        if not isinstance(self.api, SheetAPI) or not self._sheet_target().sheet_id:
            return False
        optimized = self.api._optimize_column_ranges(
            column_data, column_positions, start_row, max_gap
        )
        value_ranges: List[Dict[str, Any]] = []
        for item in optimized:
            full_range = f"{self._sheet_target().sheet_id}!{item['range']}"
            a1 = A1Range.parse(full_range)
            values = [list(row) for row in item["values"]]
            value_ranges.append({"range": a1.text, "values": values})
        return self._typed_sheet_batch_update(
            value_ranges, header_width=header_width, complete_action=True
        )

    def _typed_sheet_clear(self, range_str: str) -> bool:
        if (
            not isinstance(self.api, SheetAPI)
            or not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return False
        full_range = (
            range_str
            if "!" in range_str
            else f"{self._sheet_target().sheet_id}!{range_str}"
        )
        a1 = A1Range.parse(full_range)
        receipt = self.api.clear_values(self._sheet_target().spreadsheet_token, a1.text)
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
        if not self.sync_config.verify_remote_writes:
            return True
        # Clear can cover more than one read window. Never allocate an empty
        # matrix the size of the sheet or treat whitespace as an empty cell.
        pending = []
        for row in range(a1.start_row, a1.end_row + 1, self.api.scan_max_rows):
            for col in range(a1.start_col, a1.end_col + 1, self.api.scan_max_cols):
                pending.append(
                    A1Range(
                        a1.sheet_id,
                        row,
                        min(row + self.api.scan_max_rows - 1, a1.end_row),
                        col,
                        min(col + self.api.scan_max_cols - 1, a1.end_col),
                    ).text
                )

        def check_clear() -> bool:
            for text in tuple(pending):
                observed = self._sheet_api().get_sheet_data(
                    self._sheet_target().spreadsheet_token,
                    text,
                    value_render_option="Formula",
                )
                if sheet_values_equal([], observed):
                    pending.remove(text)
            return not pending

        if not self._wait_for_confirmation(check_clear, "Sheet 清空结果"):
            return self._mark_action_failure(ErrorKind.VERIFICATION)
        self._last_action_confirmed_count = self._last_action_applied_count
        return True

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
        if self.target.type != TargetType.SHEET:
            return None
        if not isinstance(self.api, SheetAPI):
            return None
        if (
            not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return None
        cache_key = (
            self._sheet_target().spreadsheet_token,
            self._sheet_target().sheet_id,
        )
        if self._sheet_grid_cache_key == cache_key and self._sheet_grid_cache:
            return self._sheet_grid_cache
        try:
            grid = self.api.get_sheet_grid_properties(
                self._sheet_target().spreadsheet_token, self._sheet_target().sheet_id
            )
            self._sheet_grid_cache = grid
            self._sheet_grid_cache_key = cache_key
            return grid
        except Exception as e:
            if self._is_auth_error(e) or self._is_resource_error(e):
                raise
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
        return f"{self._sheet_target().sheet_id}!A1:{end_col}{row_count}"

    def get_current_sheet_data(self) -> pd.DataFrame:
        """获取当前电子表格数据"""
        self._sheet_read_complete = True
        self._last_sheet_read_range = None
        self._last_sheet_layout: Optional[SheetLayout] = None
        self._last_sheet_values: Optional[tuple[tuple[Any, ...], ...]] = None
        self._last_sheet_formula_values: Optional[tuple[tuple[Any, ...], ...]] = None
        if self.target.type != TargetType.SHEET:
            return pd.DataFrame()

        # 构建从配置起始点开始的读取范围
        start_cell = (
            f"{self._sheet_target().start_column}{self._sheet_target().start_row}"
        )
        read_range = None
        end_row = None
        end_col = None

        # 优先使用工作表网格属性精确限定范围
        grid = self._get_sheet_grid_properties()
        if grid and isinstance(self.api, SheetAPI):
            row_count, col_count = grid
            start_col_num = self.api.column_letter_to_number(
                self._sheet_target().start_column
            )
            if row_count < self._sheet_target().start_row or col_count < start_col_num:
                self.logger.info(
                    f"工作表网格范围小于起始位置: "
                    f"row_count={row_count}, column_count={col_count}, "
                    f"start={start_cell}"
                )
                return pd.DataFrame()

            end_row = row_count
            end_col = self.api.column_number_to_letter(col_count)
            read_range = (
                f"{self._sheet_target().sheet_id}!"
                f"{self._sheet_target().start_column}{self._sheet_target().start_row}:{end_col}{end_row}"
            )
        else:
            # 元数据不可用时使用配置化读取窗口，避免硬编码超大范围。
            if not isinstance(self.api, SheetAPI):
                self._sheet_read_complete = False
                return pd.DataFrame()
            self._sheet_read_complete = False
            end_row = (
                self._sheet_target().start_row + self._sheet_target().scan_max_rows - 1
            )
            start_col_num = self.api.column_letter_to_number(
                self._sheet_target().start_column
            )
            end_col = self.api.column_number_to_letter(
                start_col_num + self._sheet_target().scan_max_cols - 1
            )
            read_range = (
                f"{self._sheet_target().sheet_id}!{start_cell}:{end_col}{end_row}"
            )
            self.logger.warning(
                "无法获取工作表网格属性，使用配置化读取窗口: "
                f"{self._sheet_target().scan_max_rows} 行 × "
                f"{self._sheet_target().scan_max_cols} 列"
            )

        self.logger.info(f"尝试从范围读取数据: {read_range}")
        self._last_sheet_read_range = read_range

        try:
            if not isinstance(self.api, SheetAPI):
                self._sheet_read_complete = False
                return pd.DataFrame()
            if not self._sheet_target().spreadsheet_token:
                self.logger.error("电子表格的 spreadsheet_token 未配置")
                self._sheet_read_complete = False
                return pd.DataFrame()
            if not self._sheet_target().sheet_id:
                self.logger.error("电子表格的 sheet_id 未配置")
                self._sheet_read_complete = False
                return pd.DataFrame()

            if not (end_row and end_col):
                self._sheet_read_complete = False
                return pd.DataFrame()

            values = self.api.get_sheet_data_chunked(
                self._sheet_target().spreadsheet_token,
                self._sheet_target().sheet_id,
                self._sheet_target().start_row,
                end_row,
                self._sheet_target().start_column,
                end_col,
            )
            df = self._store_sheet_read(values)
            self.logger.info(f"读取电子表格: {len(df)} 行 x {len(df.columns)} 列")
            return df

        except Exception as e:
            self._sheet_read_complete = False
            if (
                self._is_auth_error(e)
                or self._is_resource_error(e)
                or isinstance(e, ValueError)
            ):
                raise
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

    def _store_sheet_read(self, values, formula_values=None) -> pd.DataFrame:
        """Keep raw cells and a shared coordinate map for both render modes."""
        self._last_sheet_values = tuple(tuple(row) for row in values)
        self._last_sheet_formula_values = (
            tuple(tuple(row) for row in formula_values)
            if formula_values is not None
            else None
        )
        occupied_values = values
        if formula_values is not None:
            if len(values) != len(formula_values):
                raise RuntimeError(
                    "Sheet Formula/FormattedValue 两次读取的行范围不一致"
                )
            if values and formula_values and values[0] != formula_values[0]:
                raise RuntimeError("Sheet Formula/FormattedValue 两次读取的表头不一致")
            occupied_values = []
            for row, formulas in zip(values, formula_values):
                width = max(len(row), len(formulas))
                occupied_values.append(
                    [
                        (
                            formulas[idx]
                            if idx < len(formulas)
                            and not self.converter._is_empty_value(formulas[idx])
                            else row[idx] if idx < len(row) else None
                        )
                        for idx in range(width)
                    ]
                )
        layout = self.converter.build_sheet_layout(
            occupied_values,
            start_row=self._sheet_target().start_row,
            start_column=self.converter.column_letter_to_number(
                self._sheet_target().start_column
            ),
        )
        self._last_sheet_layout = layout
        return self.converter.values_to_df(values, layout=layout)

    def get_sheet_data_with_validation(
        self,
    ) -> tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[set]]:
        """Read Formula and FormattedValue against the same physical grid."""
        if not self._sheet_target().validate_results:
            return self.get_current_sheet_data(), None, None
        self._sheet_read_complete = False
        self._last_sheet_layout = None
        self._last_sheet_values = None
        self._last_sheet_formula_values = None
        if not isinstance(self.api, SheetAPI):
            raise RuntimeError("Sheet 双读缺少可用客户端")
        target = self._sheet_target()
        grid = self._get_sheet_grid_properties()
        if not grid or not target.spreadsheet_token or not target.sheet_id:
            raise RuntimeError("无法获取 Sheet 双读范围")
        row_count, col_count = grid
        start_col = self.converter.column_letter_to_number(target.start_column)
        if row_count < target.start_row or col_count < start_col:
            raise ValueError("Sheet 起始位置超出工作表网格")
        end_col = self.api.column_number_to_letter(col_count)
        self._last_sheet_read_range = f"{target.sheet_id}!{target.start_column}{target.start_row}:{end_col}{row_count}"
        original_value = self.api.value_render_option
        original_datetime = self.api.datetime_render_option
        try:
            self.api.value_render_option = "Formula"
            self.api.datetime_render_option = None
            formula_values = self.api.get_sheet_data_chunked(
                target.spreadsheet_token,
                target.sheet_id,
                target.start_row,
                row_count,
                target.start_column,
                end_col,
            )
            self.api.value_render_option = "FormattedValue"
            self.api.datetime_render_option = (
                target.datetime_render_option or "FormattedString"
            )
            values = self.api.get_sheet_data_chunked(
                target.spreadsheet_token,
                target.sheet_id,
                target.start_row,
                row_count,
                target.start_column,
                end_col,
            )
        finally:
            self.api.value_render_option = original_value
            self.api.datetime_render_option = original_datetime
        result_df = self._store_sheet_read(values, formula_values)
        formula_df = self.converter.values_to_df(
            formula_values, layout=self._last_sheet_layout
        )
        formula_columns = set(
            self.api.identify_formula_columns(
                [formula_df.columns.tolist()] + formula_df.values.tolist(),
                headers=formula_df.columns.tolist(),
            )
        )
        self._sheet_read_complete = True
        return result_df, formula_df, formula_columns

    def _get_effective_selective_columns(self, df: pd.DataFrame) -> List[str]:
        """获取选择性同步实际生效的列（含索引列）"""
        if (
            not self.sync_config.selective.enabled
            or not self.sync_config.selective.columns
        ):
            return df.columns.tolist()

        target_columns = list(self.sync_config.selective.columns)

        # 自动包含索引列（用于匹配逻辑）
        if (
            self.sync_config.selective.auto_include_index
            and self.sync_config.index.column
            and self.sync_config.index.column not in target_columns
        ):
            target_columns.append(self.sync_config.index.column)
            self.logger.info(f"自动包含索引列: {self.sync_config.index.column}")

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
        if self.sync_config.selective.preserve_column_order:
            return [col for col in df.columns if col in deduped_columns]

        return deduped_columns

    def _apply_selective_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用选择性列过滤"""
        if (
            not self.sync_config.selective.enabled
            or not self.sync_config.selective.columns
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
            # clone 的 freshness 指纹覆盖全部字段，规划和执行须使用相同投影。
            return None

        index_col = self.sync_config.index.column
        if not index_col:
            return None  # 无索引列时无法优化

        # full / incremental / overwrite：仅需索引列用于匹配和获取 record_id
        if mode in ("full", "incremental", "overwrite"):
            return [index_col]

        return None

    # ========== 统一同步方法 ==========

    def _setup_sheet_intelligence(self, df: pd.DataFrame) -> bool:
        """
        为电子表格设置智能字段配置

        Args:
            df: 数据DataFrame

        Returns:
            是否设置成功
        """
        if self.target.type != TargetType.SHEET:
            return True

        if not isinstance(self.api, SheetAPI):
            self.logger.error(
                "内部逻辑错误: _setup_sheet_intelligence 应该只被 SheetAPI 调用"
            )
            return False

        # 不同策略的配置范围不同
        strategy_name = self.conversion.strategy.value
        self.logger.info(f"开始电子表格智能字段配置 ({strategy_name}策略)...")

        # raw策略：不应用任何格式化，直接返回成功
        if strategy_name == "raw":
            self.logger.info("raw策略：跳过所有格式化，保持原始数据")
            return True

        # 生成字段配置
        field_config = self.converter.generate_sheet_field_config(
            df, self.conversion.strategy.value, self.conversion
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
                    self._sheet_target().start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                # 计算行的绝对范围 (数据行，不含表头)
                start_data_row = self._sheet_target().start_row + 1
                end_data_row = self._sheet_target().start_row + len(df)

                # 仅在有数据行时才设置范围
                if end_data_row >= start_data_row:
                    range_str = f"{self._sheet_target().sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
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

                if not self._sheet_target().spreadsheet_token:
                    self.logger.error("电子表格Token为空")
                    success = False
                    continue

                # 设置下拉列表
                dropdown_success = self.api.set_dropdown_validation(
                    self._sheet_target().spreadsheet_token,
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
            and self._sheet_target().spreadsheet_token
        ):
            date_ranges = []
            for column_name in field_config["date_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self._sheet_target().start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self._sheet_target().start_row + 1
                end_data_row = self._sheet_target().start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self._sheet_target().sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    date_ranges.append(range_str)

            # 设置日期格式
            date_success = self.api.set_date_format(
                self._sheet_target().spreadsheet_token, date_ranges, "yyyy/MM/dd"
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
            and self._sheet_target().spreadsheet_token
        ):
            number_ranges = []
            for column_name in field_config["number_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self._sheet_target().start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self._sheet_target().start_row + 1
                end_data_row = self._sheet_target().start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self._sheet_target().sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    number_ranges.append(range_str)

            # 设置数字格式
            number_success = self.api.set_number_format(
                self._sheet_target().spreadsheet_token, number_ranges, "#,##0.00"
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
        return self.runtime.config_source_map()

    def _bitable_snapshot_key(
        self, record: CanonicalRecord, schema: FieldSchema
    ) -> Optional[str]:
        policy = KeyPolicy(
            datetime_granularity=self.sync_config.index.datetime_granularity,
            datetime_timezone=self.sync_config.index.timezone,
        )
        field_type = self.converter._field_schema_type_code(schema)
        key = policy.normalize(record.fields.get(schema.name), field_type)
        return key.digest if key is not None else None

    def _attach_snapshot_preconditions(
        self, actions: List[ExecutionAction], effective_mode: SyncMode
    ) -> List[ExecutionAction]:
        snapshot = getattr(self, "_planned_target_snapshot", None)
        attached: List[ExecutionAction] = []
        for position, action in enumerate(actions):
            precondition: SnapshotPrecondition | None = None
            if isinstance(action, CreateFieldAction):
                fingerprint = getattr(self, "_planned_bitable_schema_fingerprint", None)
                if fingerprint:
                    schema_expected: Dict[str, Any] = {"fingerprint": fingerprint}
                    if effective_mode is SyncMode.CLONE and isinstance(
                        snapshot, BitableSnapshot
                    ):
                        schema_expected["record_snapshot"] = snapshot
                    precondition = SnapshotPrecondition(
                        "bitable_schema", schema_expected
                    )
            elif isinstance(snapshot, BitableSnapshot):
                expected: Dict[str, Any] = {
                    "backend": snapshot.backend.value,
                    "revision": snapshot.revision,
                    "fingerprint": snapshot.fingerprint,
                    "index_column": (
                        None
                        if effective_mode is SyncMode.CLONE
                        else self.sync_config.index.column
                    ),
                    "clone": effective_mode is SyncMode.CLONE,
                }
                index_schema = next(
                    (
                        field
                        for field in snapshot.schema
                        if field.name == self.sync_config.index.column
                    ),
                    None,
                )
                if index_schema is not None and effective_mode is not SyncMode.CLONE:
                    record_keys = {
                        record.record_id: key
                        for record in snapshot.records
                        if record.record_id
                        and (key := self._bitable_snapshot_key(record, index_schema))
                    }
                    if isinstance(action, CreateRecordsAction) and (
                        effective_mode is not SyncMode.CLONE
                    ):
                        expected["absent_keys"] = tuple(
                            key
                            for record in action.records
                            if (key := self._bitable_snapshot_key(record, index_schema))
                        )
                    elif isinstance(action, (UpdateRecordsAction, DeleteRecordsAction)):
                        ids = (
                            tuple(record.record_id for record in action.records)
                            if isinstance(action, UpdateRecordsAction)
                            else action.record_ids
                        )
                        expected["record_keys"] = {
                            record_id: record_keys.get(record_id)
                            for record_id in ids
                            if record_id
                        }
                precondition = SnapshotPrecondition("bitable_records", expected)
            elif (
                isinstance(action, CreateRecordsAction)
                and self.sync_config.match_strategy is MatchStrategy.BY_KEY
                and self.sync_config.index.column
            ):
                precondition = SnapshotPrecondition(
                    "bitable_absent_keys",
                    {
                        "backend": self._bitable_target().backend,
                        "index_column": self.sync_config.index.column,
                        "absent_values": tuple(
                            record.fields.get(self.sync_config.index.column)
                            for record in action.records
                        ),
                    },
                )
            elif isinstance(snapshot, SheetSnapshot) and not isinstance(
                action, ApplySheetConfigAction
            ):
                expected = {
                    "fingerprint": snapshot.content_fingerprint,
                    "header": snapshot.header,
                    "index_mapping": snapshot.index_mapping,
                    "index_field_types": dict(self._planned_sheet_index_field_types),
                    "actual_ranges": snapshot.actual_ranges,
                    "grid": snapshot.grid,
                }
                if (
                    effective_mode is SyncMode.CLONE
                    and isinstance(action, WriteRangeAction)
                    and position > 0
                ):
                    kind = "sheet_empty"
                elif isinstance(action, (WriteColumnsAction, AppendRowsAction)):
                    kind = "sheet_mapping"
                else:
                    kind = "sheet_content"
                precondition = SnapshotPrecondition(kind, expected)
            attached.append(
                replace(action, precondition=precondition)
                if precondition is not None
                else action
            )
        return attached

    def _make_plan(
        self,
        *,
        requested_mode: SyncMode,
        effective_mode: SyncMode,
        source: Mapping[str, Any],
        target: Mapping[str, Any],
        actions: List[ExecutionAction],
        warnings: Optional[List[str]] = None,
    ) -> ExecutionPlan:
        actions = self._attach_snapshot_preconditions(actions, effective_mode)
        plan_warnings = list(warnings or ())
        plan_warnings.extend(self.converter.consume_key_warnings())
        plan = ExecutionPlan(
            bitable_fields=getattr(self, "_planned_bitable_fields", ()),
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

        self._preflight_plan(plan)
        return plan

    def _preflight_plan(self, plan: ExecutionPlan) -> None:
        """Validate the entire payload before field creation, deletion, or data writes."""
        import json

        for action in plan.actions:
            if isinstance(action, CreateFieldAction):
                if action.suggested_type not in {1, 2, 3, 4, 5, 7}:
                    raise ValueError(f"字段 {action.field_name!r} 不能自动创建为此类型")
            elif isinstance(action, (CreateRecordsAction, UpdateRecordsAction)):
                if isinstance(action, UpdateRecordsAction):
                    ids = [record.record_id for record in action.records]
                    if any(
                        not isinstance(rid, str) or not rid.strip() for rid in ids
                    ) or len(ids) != len(set(ids)):
                        raise ValueError("待更新记录 ID 为空或重复；未发送写请求")
                self._bitable_backend().validate_records(
                    action.records, plan.bitable_fields
                )
            elif isinstance(action, DeleteRecordsAction):
                if len(action.record_ids) != len(set(action.record_ids)) or any(
                    not rid for rid in action.record_ids
                ):
                    raise ValueError("待删除记录 ID 为空或重复")
            elif isinstance(action, ClearRangeAction):
                A1Range.parse(action.a1_range)
            elif isinstance(action, (WriteRangeAction, AppendRowsAction)):
                width = len(action.values[0]) if action.values else 0
                if not width or any(len(row) != width for row in action.values):
                    raise ValueError("Sheet 待写数据必须为非空等宽矩阵")
                json.dumps(action.values, allow_nan=False)
            elif isinstance(action, WriteColumnsAction):
                lengths = {len(column) for column in action.column_data.values()}
                if not lengths or 0 in lengths or len(lengths) != 1:
                    raise ValueError("Sheet 待写列的长度必须相同且非空")
                positions = [
                    action.column_positions.get(name) for name in action.column_data
                ]
                if len(set(positions)) != len(positions) or any(
                    not isinstance(pos, int) or pos < 1 for pos in positions
                ):
                    raise ValueError("Sheet 待写列物理位置缺失或重复")
                json.dumps(dict(action.column_data), allow_nan=False)

    def _row_to_canonical_fields(
        self, row: pd.Series, field_types: Mapping[str, FieldSchema]
    ) -> Dict[str, Any]:
        fields: Dict[str, Any] = {}
        index_column = self.sync_config.index.column
        schemas = field_types if isinstance(field_types, dict) else dict(field_types)
        for raw_name, value in row.to_dict().items():
            name = str(raw_name)
            if self.converter._is_empty_value(value):
                continue
            # Use strict canonical key conversion for the index column.
            if index_column and name == index_column:
                converted = self.converter.convert_strict_key_value(
                    value, name, schemas
                )
            else:
                converted = self.converter.convert_write_value(name, value, schemas)
            if converted is not None:
                fields[name] = converted
        return fields

    def _plan_file_bitable(self, df: pd.DataFrame) -> ExecutionPlan:
        mode = self.sync_config.mode
        match_strategy = self.sync_config.match_strategy
        actions, field_types = self.plan_fields(df)
        source = {"type": "file", "rows": len(df), "columns": len(df.columns)}
        target = {"type": "bitable"}
        index_column = self.sync_config.index.column
        planned_fields = {
            action.field_name
            for action in actions
            if isinstance(action, CreateFieldAction)
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
            # Strict canonical key pre-flight: every source row's index value
            # must be losslessly convertible.  Failure here stops the plan
            # before any mutation actions are generated.
            for pos, (_, row) in enumerate(df.iterrows(), start=1):
                if index_column in row:
                    try:
                        self.converter.convert_strict_key_value(
                            row[index_column], index_column, field_types
                        )
                    except ValueError as exc:
                        raise ValueError(
                            f"本地数据第 {pos} 条记录的索引列 '{index_column}' 无效: {exc}"
                        ) from exc

        creates: List[CanonicalRecord] = []
        updates: List[CanonicalRecord] = []
        deletes: List[str] = []
        source_rows = (row for _, row in df.iterrows())
        reconciliation = (
            Reconciler.by_key(
                source_rows,
                existing_index,
                source_key=lambda row: self.converter.get_index_value_hash(
                    row, index_column, field_types
                ),
            )
            if match_strategy is MatchStrategy.BY_KEY
            else None
        )

        if mode is SyncMode.CLONE:
            deletes = [
                str(record["record_id"])
                for record in existing_records
                if record.get("record_id")
            ]
            creates = [
                CanonicalRecord(None, self._row_to_canonical_fields(row, field_types))
                for _, row in df.iterrows()
            ]
        elif match_strategy is MatchStrategy.APPEND_ONLY:
            creates = [
                CanonicalRecord(None, self._row_to_canonical_fields(row, field_types))
                for _, row in df.iterrows()
            ]
        elif mode is SyncMode.OVERWRITE:
            if not index_column:
                raise ValueError("覆盖同步模式需要指定索引列")
            assert reconciliation is not None
            for _, _, target_record in reconciliation.matched:
                record_id = target_record.get("record_id")
                if record_id:
                    deletes.append(str(record_id))
            creates = [
                CanonicalRecord(None, self._row_to_canonical_fields(row, field_types))
                for _, row in df.iterrows()
            ]
        else:
            assert reconciliation is not None
            if mode is SyncMode.FULL:
                for _, row, target_record in reconciliation.matched:
                    fields = self._row_to_canonical_fields(row, field_types)
                    record_id = target_record.get("record_id")
                    if not record_id:
                        raise RuntimeError("目标记录缺少 record_id")
                    updates.append(CanonicalRecord(str(record_id), fields))
            for row in reconciliation.missing:
                fields = self._row_to_canonical_fields(row, field_types)
                creates.append(CanonicalRecord(None, fields))

        if deletes:
            actions.append(
                BitablePlanCompiler.delete_records(
                    deletes,
                    scope={"target": "bitable"},
                )
            )
        if updates:
            actions.append(
                BitablePlanCompiler.update_records(
                    updates,
                    scope={"target": "bitable"},
                )
            )
        if creates:
            actions.append(
                BitablePlanCompiler.create_records(
                    creates,
                    scope={"target": "bitable"},
                )
            )
        return self._make_plan(
            requested_mode=mode,
            effective_mode=mode,
            source=source,
            target=target,
            actions=actions,
        )

    def _sheet_clear_action(self) -> ClearRangeAction:
        clear_range = self._build_sheet_full_range()
        if not clear_range:
            raise RuntimeError("无法获取工作表网格范围")
        return SheetPlanCompiler.clear(
            clear_range,
            scope={"target": "sheet", "range": clear_range},
        )

    def _sheet_write_action(self, df: pd.DataFrame) -> WriteRangeAction:
        values = self.converter.df_to_values(df)
        from api.sheet import RangeChunker

        RangeChunker.copy_matrix(values)
        A1Range(
            str(self._sheet_target().sheet_id),
            self._sheet_target().start_row,
            self._sheet_target().start_row + len(values) - 1,
            self.converter.column_letter_to_number(self._sheet_target().start_column),
            self.converter.column_letter_to_number(self._sheet_target().start_column)
            + len(values[0])
            - 1,
        )
        return SheetPlanCompiler.write(
            values,
            scope={"target": "sheet", "columns": len(df.columns)},
        )

    def _read_sheet_header(self) -> Optional[List[str]]:
        frame = self.get_current_sheet_data()
        if not self._require_complete_sheet_read("Sheet 表头读取"):
            raise RuntimeError("无法完整读取目标 Sheet 表头")
        return [str(column) for column in frame.columns] or None

    def _sheet_column_positions(
        self, frame: pd.DataFrame, columns: List[str]
    ) -> Dict[str, int]:
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise ValueError(f"源数据列 {missing} 不存在于目标 Sheet header 中")
        layout = getattr(self, "_last_sheet_layout", None)
        if layout is not None:
            return {column: layout.header_to_physical_col[column] for column in columns}
        offset = (
            self.converter.column_letter_to_number(self._sheet_target().start_column)
            - 1
        )
        return self.converter.get_column_positions(frame, columns, offset)

    def _build_aligned_append_actions(
        self,
        df: pd.DataFrame,
        target_header: Optional[List[str]],
        source: Mapping[str, Any],
        target: Mapping[str, Any],
    ) -> List[ExecutionAction]:
        """Build append actions aligned to the target header.

        For empty targets, writes header + data.  For non-empty targets,
        projects source values to target header order.
        """
        source_columns = [str(c) for c in df.columns]

        if target_header is None:
            # Empty target: validate source columns are unique, write header + data
            if len(set(source_columns)) != len(source_columns):
                raise ValueError("源数据列名重复，无法作为 Sheet header 写入")
            values = self.converter.df_to_values(df, include_headers=True)
            return [
                SheetPlanCompiler.write(
                    values,
                    scope={"target": "sheet", "columns": len(source_columns)},
                )
            ]

        # Validate target header
        if len(set(target_header)) != len(target_header):
            raise ValueError("目标 Sheet header 包含重复列名")
        index_col = self.sync_config.index.column
        if index_col and index_col not in target_header:
            raise ValueError(f"目标 Sheet header 缺少索引列 '{index_col}'")

        # Check for source columns not in target
        extra = [c for c in source_columns if c not in target_header]
        if extra:
            raise ValueError(f"源数据列 {extra} 不存在于目标 Sheet header 中")

        # Project onto physical positions, including unnamed header gaps.
        positions = self._sheet_column_positions(
            pd.DataFrame(columns=target_header), target_header
        )
        start_col = self.converter.column_letter_to_number(
            self._sheet_target().start_column
        )
        width = max(positions.values()) - start_col + 1
        values = []
        for row in df.to_dict(orient="records"):
            projected = [""] * width
            for column in target_header:
                projected[positions[column] - start_col] = (
                    self.converter.simple_convert_value(row.get(column))
                )
            values.append(projected)
        layout = getattr(self, "_last_sheet_layout", None)
        return [
            SheetPlanCompiler.append(
                values,
                header_width=width,
                start_row=(
                    max(
                        layout.physical_row_numbers,
                        default=self._sheet_target().start_row,
                    )
                    + 1
                    if layout is not None
                    else self._sheet_target().start_row + 1
                ),
                scope={"target": "sheet", "columns": width},
            )
        ]

    def _sheet_physical_width(
        self, current_df: pd.DataFrame, columns: Sequence[str]
    ) -> int:
        layout = getattr(self, "_last_sheet_layout", None)
        return (
            layout.raw_width
            if layout is not None
            else len(current_df.columns) or len(columns)
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
    ) -> Optional[WriteColumnsAction]:
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
        positions = self._sheet_column_positions(current_df, columns)
        max_gap = (
            self.sync_config.selective.max_gap_for_merge
            if self.sync_config.selective.optimize_ranges
            else 0
        )
        return SheetPlanCompiler.columns(
            column_data={name: tuple(values) for name, values in column_data.items()},
            column_positions=dict(positions),
            start_row=start_row,
            max_gap=max_gap,
            header_width=self._sheet_physical_width(current_df, columns),
            scope={
                "target": "sheet",
                "columns": len(columns),
                "affected_rows": (
                    len(df) if not preserve_rows else len(update_data_map or {})
                ),
            },
            clears_values=any(
                self.converter._is_empty_value(value)
                for values in column_data.values()
                for value in values
            ),
        )

    def _plan_sheet_selective(
        self,
        df: pd.DataFrame,
        current_df: pd.DataFrame,
        mode: SyncMode,
        columns: List[str],
        current_index: Mapping[str, int],
        index_field_types: Mapping[str, Any],
    ) -> List[ExecutionAction]:
        if len(current_df.columns) == 0:
            if df.empty:
                return []
            return self._build_aligned_append_actions(df, None, {}, {})
        self._sheet_column_positions(current_df, columns)
        reconciliation = Reconciler.by_key(
            (row for _, row in df.iterrows()),
            current_index,
            source_key=lambda row: self.converter.get_index_value_hash(
                row, self.sync_config.index.column, dict(index_field_types)
            ),
        )

        # --- physical row mapping ---
        layout = getattr(self, "_last_sheet_layout", None)
        physical_rows = layout.physical_row_numbers if layout else None

        actions: List[ExecutionAction] = []

        # --- matched-row patches (FULL / OVERWRITE) ---
        if mode in {SyncMode.FULL, SyncMode.OVERWRITE} and reconciliation.matched:
            # Build (physical_row, update_values) pairs
            patched: List[tuple] = []
            for _, row, logical_idx in reconciliation.matched:
                phys = (
                    physical_rows[logical_idx]
                    if physical_rows and logical_idx < len(physical_rows)
                    else self._sheet_target().start_row + 1 + logical_idx
                )
                patched.append(
                    (
                        phys,
                        {col: row[col] for col in columns if col in row},
                    )
                )

            # Group contiguous physical rows into one WriteColumnsAction
            patched.sort(key=lambda pair: pair[0])
            groups: List[List[tuple]] = []
            cur: List[tuple] = []
            max_gap = (
                self.sync_config.selective.max_gap_for_merge
                if self.sync_config.selective.optimize_ranges
                else 0
            )
            for pair in patched:
                # Row gaps are never merged: the matrix has no rows for them.
                if cur and pair[0] != cur[-1][0] + 1:
                    groups.append(cur)
                    cur = []
                cur.append(pair)
            if cur:
                groups.append(cur)

            positions = self._sheet_column_positions(current_df, columns)
            header_width = self._sheet_physical_width(current_df, columns)

            for group in groups:
                group_rows = [p[0] for p in group]
                col_data: Dict[str, list] = {}
                for col in columns:
                    col_data[col] = [
                        self.converter.simple_convert_value(p[1].get(col, ""))
                        for p in group
                    ]
                action = SheetPlanCompiler.columns(
                    column_data={n: tuple(v) for n, v in col_data.items()},
                    column_positions=dict(positions),
                    start_row=group_rows[0],
                    max_gap=max_gap,
                    header_width=header_width,
                    scope={
                        "target": "sheet",
                        "columns": len(columns),
                        "affected_rows": len(group),
                        "physical_rows": group_rows,
                    },
                    clears_values=any(
                        self.converter._is_empty_value(val)
                        for vals in col_data.values()
                        for val in vals
                    ),
                )
                actions.append(action)

        # --- missing-row appends ---
        if reconciliation.missing:
            new_df = pd.DataFrame(reconciliation.missing)
            if self.sync_config.selective.enabled or (
                mode is SyncMode.FULL and self._sheet_target().protect_formulas
            ):
                # Selective mode uses column writes for new rows.
                append_start = (
                    (physical_rows[-1] + 1)
                    if physical_rows
                    else self._sheet_target().start_row + len(current_df) + 1
                )
                col_action = self._sheet_columns_action(
                    new_df,
                    current_df,
                    columns,
                    start_row=append_start,
                    preserve_rows=False,
                )
                if col_action:
                    actions.append(col_action)
            else:
                actions.extend(
                    self._build_aligned_append_actions(
                        new_df, list(current_df.columns) or None, {}, {}
                    )
                )
        return actions

    def _sheet_index_field_types(
        self, source_df: pd.DataFrame, target_df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Infer DATETIME semantics for Sheet indexes before cross-form matching."""
        index_column = self.sync_config.index.column
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
                    value, self.sync_config.index.datetime_granularity
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

    def _capture_sheet_snapshot(
        self,
        frame: pd.DataFrame,
        *,
        index_mapping: Mapping[str, int],
        formula_columns: Optional[set[Union[str, int]]] = None,
        index_field_types: Optional[Mapping[str, Any]] = None,
    ) -> SheetSnapshot:
        read_range = getattr(self, "_last_sheet_read_range", None)
        snapshot = SheetSnapshot.from_dataframe(
            frame,
            actual_ranges=((read_range,) if read_range else ()),
            grid=getattr(self, "_sheet_grid_cache", None),
            index_mapping=index_mapping,
            formula_columns=tuple(str(item) for item in (formula_columns or ())),
            complete=getattr(self, "_sheet_read_complete", True),
            layout=getattr(self, "_last_sheet_layout", None),
            raw_values=getattr(self, "_last_sheet_values", None),
            formula_values=getattr(self, "_last_sheet_formula_values", None),
        )
        self._planned_target_snapshot = snapshot
        self._planned_sheet_index_field_types = dict(index_field_types or {})
        return snapshot

    def _plan_file_sheet(self, df: pd.DataFrame) -> ExecutionPlan:
        requested_mode = self.sync_config.mode
        source = {"type": "file", "rows": len(df), "columns": len(df.columns)}
        target = {"type": "sheet"}
        warnings: List[str] = []
        actions: List[ExecutionAction]
        if self.sync_config.match_strategy is MatchStrategy.APPEND_ONLY:
            if df.empty:
                return self._make_plan(
                    requested_mode=requested_mode,
                    effective_mode=requested_mode,
                    source=source,
                    target=target,
                    actions=[],
                )
            # Read target header to align append payload.
            target_header = self._read_sheet_header()
            self._capture_sheet_snapshot(
                (
                    self.converter.values_to_df(
                        [
                            list(row)
                            for row in getattr(self, "_last_sheet_values", ()) or ()
                        ],
                        layout=getattr(self, "_last_sheet_layout", None),
                    )
                    if getattr(self, "_last_sheet_values", None) is not None
                    else pd.DataFrame(columns=target_header or ())
                ),
                index_mapping={},
            )
            actions = self._build_aligned_append_actions(
                df, target_header, source, target
            )
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
        self._capture_sheet_snapshot(
            current_df,
            index_mapping={},
            formula_columns=formula_columns,
        )

        if requested_mode is SyncMode.CLONE:
            # DEC-001: empty source clone = clear-only, no degenerate write.
            if df.empty:
                actions = [self._sheet_clear_action()]
            else:
                actions = [
                    self._sheet_clear_action(),
                    self._sheet_write_action(df),
                ]
                actions.append(SheetPlanCompiler.enrichment(df))
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=SyncMode.CLONE,
                source=source,
                target=target,
                actions=actions,
                warnings=warnings,
            )

        if requested_mode is SyncMode.OVERWRITE and not self.sync_config.index.column:
            raise ValueError("覆盖同步模式需要指定索引列")

        index_field_types = self._sheet_index_field_types(df, current_df)
        current_index: Mapping[str, int] = {}
        if self.sync_config.index.column:
            self.converter.build_data_index(
                df,
                self.sync_config.index.column,
                index_field_types,
                allow_empty=False,
                context=f"本地数据索引列 '{self.sync_config.index.column}' ",
            )
            try:
                current_index = self.converter.build_data_index(
                    current_df,
                    self.sync_config.index.column,
                    index_field_types,
                    allow_empty=True,
                    context=f"目标 Sheet 索引列 '{self.sync_config.index.column}' ",
                )
            except ValueError as error:
                raise RuntimeError(f"目标 Sheet 索引不安全: {error}") from error
        self._capture_sheet_snapshot(
            current_df,
            index_mapping=current_index,
            formula_columns=formula_columns,
            index_field_types=index_field_types,
        )

        sync_df = df
        selected_columns: Optional[List[str]] = None
        if requested_mode is SyncMode.FULL and self._sheet_target().protect_formulas:
            if formula_columns is None:
                raise RuntimeError("无法确认远端公式列")
            if self.sync_config.index.column in formula_columns:
                raise ValueError("索引列是公式列，无法安全匹配")
            selected_columns = [
                str(column) for column in df.columns if column not in formula_columns
            ]
            sync_df = df[selected_columns].copy()
        elif self.sync_config.selective.enabled:
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
            reconciliation = Reconciler.by_key(
                (row for _, row in sync_df.iterrows()),
                current_index,
                source_key=lambda row: self.converter.get_index_value_hash(
                    row, self.sync_config.index.column, index_field_types
                ),
            )
            matched_rows = {row_index for _, _, row_index in reconciliation.matched}
            layout = getattr(self, "_last_sheet_layout", None)
            raw_values = getattr(self, "_last_sheet_values", None)
            if layout is not None and raw_values is not None:
                positions = self._sheet_column_positions(
                    current_df, list(sync_df.columns)
                )
                width = layout.raw_width
                matched_physical = {
                    layout.physical_row_for_logical(pos) for pos in matched_rows
                }
                last_row = max(layout.physical_row_numbers, default=layout.start_row)
                values = []
                for offset, raw_row in enumerate(
                    raw_values[: last_row - layout.start_row + 1]
                ):
                    if layout.start_row + offset in matched_physical:
                        continue
                    row_values = list(raw_row[:width])
                    values.append(row_values + [""] * (width - len(row_values)))
                for row in sync_df.to_dict(orient="records"):
                    projected = [""] * width
                    for column, value in row.items():
                        projected[positions[column] - layout.start_column] = (
                            self.converter.simple_convert_value(value)
                        )
                    values.append(projected)
                actions = [
                    SheetPlanCompiler.write(
                        values,
                        scope={"target": "sheet", "columns": width},
                        destructive=True,
                        clears_values=True,
                    )
                ]
            else:
                # Empty target (or a non-network adapter without physical metadata).
                if current_df.empty:
                    actions = (
                        self._build_aligned_append_actions(
                            sync_df,
                            list(current_df.columns) or None,
                            source,
                            target,
                        )
                        if not sync_df.empty
                        else []
                    )
                else:
                    rows = [
                        row
                        for row_index, row in current_df.iterrows()
                        if row_index not in matched_rows
                    ]
                    rows.extend(row for _, row in sync_df.iterrows())
                    actions = [self._sheet_write_action(pd.DataFrame(rows))]
                if actions and isinstance(actions[0], WriteRangeAction):
                    first = actions[0]
                    actions[0] = SheetPlanCompiler.write(
                        first.values,
                        scope=first.scope,
                        destructive=True,
                        clears_values=True,
                    )
            return self._make_plan(
                requested_mode=requested_mode,
                effective_mode=requested_mode,
                source=source,
                target=target,
                actions=actions,
            )

        # For FULL mode without formula protection and without selective,
        # route through _plan_sheet_selective to preserve physical row layout.
        full_columns = list(sync_df.columns)
        actions = self._plan_sheet_selective(
            sync_df,
            current_df,
            requested_mode,
            full_columns,
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

    def plan(self, df: Optional[pd.DataFrame] = None) -> ExecutionPlan:
        """Build a complete mutation plan using reads and local classification only."""
        self._planned_target_snapshot = None
        self._planned_sheet_index_field_types = {}
        self._planned_bitable_fields = ()
        self._planned_bitable_schema_fingerprint = None
        self._last_sheet_layout = None
        self._last_sheet_values = None
        self._last_sheet_formula_values = None
        self._sheet_grid_cache = None
        self._sheet_grid_cache_key = None
        self._mode_decision = ModePolicy.decide(
            mode=self.sync_config.mode,
            strategy=self.sync_config.match_strategy,
            index_column=self.sync_config.index.column,
            source_type=self.source.type,
            selective_enabled=self.sync_config.selective.enabled,
        )
        if self.source.type is SourceType.BITABLE:
            if df is not None:
                raise ValueError("source_type=bitable 不接受本地 DataFrame")
            return self._plan_bitable_source()
        if df is None:
            raise ValueError("source_type=file 必须提供 DataFrame")
        source_table = SourceTable.from_dataframe(df)
        source_frame = source_table.to_dataframe()
        planned_df = (
            self._apply_selective_filter(source_frame)
            if self.sync_config.selective.enabled
            else source_frame
        )
        if self.target.type is TargetType.BITABLE:
            return self._plan_file_bitable(planned_df)
        return self._plan_file_sheet(planned_df)

    def _read_current_bitable_snapshot(self) -> BitableSnapshot:
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            raise RuntimeError("目标 Bitable 配置不完整")
        field_names = (
            [self.sync_config.index.column]
            if self.sync_config.index.column
            and self.sync_config.mode is not SyncMode.CLONE
            else None
        )
        result = self._bitable_backend().list_records(
            self._bitable_target().app_token,
            self._bitable_target().table_id,
            field_names=field_names,
        )
        if not result.complete or result.ignored_fields or result.record_not_found:
            raise RuntimeError("目标 Bitable freshness read 不完整")
        return BitableSnapshot.from_result(result)

    def _snapshot_record_keys(
        self, snapshot: BitableSnapshot, index_column: str
    ) -> Dict[str, str]:
        schema = next(
            (field for field in snapshot.schema if field.name == index_column), None
        )
        if schema is None:
            raise RuntimeError(
                f"目标 Bitable freshness read 缺少索引列 '{index_column}'"
            )
        return {
            record.record_id: key
            for record in snapshot.records
            if record.record_id
            and (key := self._bitable_snapshot_key(record, schema)) is not None
        }

    def _current_sheet_snapshot(
        self, *, index_field_types: Optional[Mapping[str, Any]] = None
    ) -> SheetSnapshot:
        # Re-read grid metadata as rows/columns can have been inserted since plan.
        self._sheet_grid_cache = None
        self._sheet_grid_cache_key = None
        frame, _, formula_columns = self.get_sheet_data_with_validation()
        if not self._require_complete_sheet_read("snapshot freshness"):
            raise RuntimeError("目标 Sheet freshness read 不完整")
        mapping: Mapping[str, int] = {}
        if self.sync_config.index.column:
            if self.sync_config.index.column not in frame.columns and not frame.empty:
                raise RuntimeError("目标 Sheet freshness read 的表头已变化")
            if self.sync_config.index.column in frame.columns:
                # 复用计划中的规则；目标与自身比较会把普通数字编号误推断为时间戳。
                field_types = (
                    dict(index_field_types)
                    if index_field_types is not None
                    else self._sheet_index_field_types(frame, frame)
                )
                mapping = self.converter.build_data_index(
                    frame,
                    self.sync_config.index.column,
                    field_types,
                    allow_empty=True,
                    context=f"目标 Sheet 索引列 '{self.sync_config.index.column}' ",
                )
        read_range = getattr(self, "_last_sheet_read_range", None)
        return SheetSnapshot.from_dataframe(
            frame,
            actual_ranges=((read_range,) if read_range else ()),
            grid=getattr(self, "_sheet_grid_cache", None),
            index_mapping=mapping,
            formula_columns=tuple(str(item) for item in (formula_columns or ())),
            complete=True,
            layout=getattr(self, "_last_sheet_layout", None),
            raw_values=getattr(self, "_last_sheet_values", None),
            formula_values=getattr(self, "_last_sheet_formula_values", None),
        )

    def _check_action_precondition(self, action: ExecutionAction) -> bool:
        precondition = action.precondition
        if precondition is None:
            return True
        try:
            if precondition.kind == "bitable_schema":
                if (
                    not self._bitable_target().app_token
                    or not self._bitable_target().table_id
                ):
                    raise RuntimeError("目标 Bitable 配置不完整")
                fields = self._bitable_backend().list_fields(
                    self._bitable_target().app_token, self._bitable_target().table_id
                )
                current_fingerprint = self._schema_fingerprint(tuple(fields))
                expected_fingerprint = (
                    self._expected_bitable_schema_fingerprint
                    or precondition.expected.get("fingerprint")
                )
                if current_fingerprint != expected_fingerprint:
                    raise RuntimeError("目标 Bitable schema 在计划后发生变化")
                self._expected_bitable_schema_fingerprint = current_fingerprint
                return True
            if precondition.kind in {"bitable_records", "bitable_absent_keys"}:
                current = self._read_current_bitable_snapshot()
                expected_backend = precondition.expected.get("backend")
                if current.backend.value != expected_backend:
                    raise RuntimeError("目标 Bitable backend 与计划不一致")
                expected_revision = (
                    self._expected_bitable_revision
                    if self._expected_bitable_snapshot is not None
                    else precondition.expected.get("revision")
                )
                if (
                    precondition.kind == "bitable_records"
                    and current.backend is BitableBackendKind.BASE_V3
                    and expected_revision != current.revision
                ):
                    raise RuntimeError("目标 Base revision 在计划后发生变化")
                index_column = precondition.expected.get("index_column")
                if isinstance(index_column, str) and index_column:
                    current_keys = self._snapshot_record_keys(current, index_column)
                    expected_record_keys = precondition.expected.get("record_keys")
                    if isinstance(expected_record_keys, Mapping):
                        for record_id, expected_key in expected_record_keys.items():
                            if current_keys.get(str(record_id)) != expected_key:
                                raise RuntimeError(
                                    "目标 Bitable record ID 到 key 的映射已漂移"
                                )
                    absent_keys = precondition.expected.get("absent_keys", ())
                    if set(absent_keys) & set(current_keys.values()):
                        raise RuntimeError("目标 Bitable 已出现计划创建的 key")
                    absent_values = precondition.expected.get("absent_values", ())
                    if absent_values:
                        schema = next(
                            field
                            for field in current.schema
                            if field.name == index_column
                        )
                        policy = KeyPolicy(
                            datetime_granularity=self.sync_config.index.datetime_granularity,
                            datetime_timezone=self.sync_config.index.timezone,
                        )
                        field_type = self.converter._field_schema_type_code(schema)
                        desired = {
                            key.digest
                            for value in absent_values
                            if (key := policy.normalize(value, field_type)) is not None
                        }
                        if desired & set(current_keys.values()):
                            raise RuntimeError("目标 Bitable 已出现计划创建的 key")
                elif current.fingerprint != (
                    self._expected_bitable_snapshot.fingerprint
                    if self._expected_bitable_snapshot is not None
                    else precondition.expected.get("fingerprint")
                ):
                    raise RuntimeError("目标 Bitable 内容在计划后发生变化")
                self._expected_bitable_snapshot = current
                self._expected_bitable_revision = current.revision
                return True
            if precondition.kind.startswith("sheet_"):
                current_sheet = self._current_sheet_snapshot(
                    index_field_types=precondition.expected.get("index_field_types")
                )
                baseline = self._expected_sheet_snapshot
                if precondition.kind == "sheet_empty":
                    if current_sheet.header or current_sheet.index_mapping:
                        raise RuntimeError("目标 Sheet 在 clear 后不再为空")
                elif precondition.kind == "sheet_mapping":
                    expected_header = (
                        baseline.header
                        if baseline is not None
                        else tuple(precondition.expected.get("header", ()))
                    )
                    expected_mapping = (
                        baseline.index_mapping
                        if baseline is not None
                        else tuple(precondition.expected.get("index_mapping", ()))
                    )
                    if (
                        current_sheet.header != expected_header
                        or current_sheet.index_mapping != expected_mapping
                    ):
                        raise RuntimeError(
                            "目标 Sheet header 或 key-row mapping 已漂移"
                        )
                    expected_fingerprint = (
                        baseline.content_fingerprint
                        if baseline is not None
                        else precondition.expected.get("fingerprint")
                    )
                    if (
                        expected_fingerprint is not None
                        and current_sheet.content_fingerprint != expected_fingerprint
                    ):
                        raise RuntimeError("目标 Sheet 待写范围的内容或物理位置已变化")
                else:
                    expected_fingerprint = (
                        baseline.content_fingerprint
                        if baseline is not None
                        else precondition.expected.get("fingerprint")
                    )
                    if current_sheet.content_fingerprint != expected_fingerprint:
                        raise RuntimeError("目标 Sheet 关键范围在计划后发生变化")
                self._expected_sheet_snapshot = current_sheet
                return True
        except Exception as error:
            self._last_action_error_kind = (
                ErrorKind.AUTH
                if self._is_auth_error(error)
                else (
                    ErrorKind.RESOURCE
                    if self._is_resource_error(error)
                    else ErrorKind.STALE_SNAPSHOT
                )
            )
            self._last_action_failure_message = str(error)
            return False
        return True

    def _check_sheet_unmodified_cells(
        self, before: SheetSnapshot, after: SheetSnapshot
    ) -> None:
        """Do not adopt unrelated concurrent changes as the next action's baseline."""
        if (
            before.raw_values is None
            or after.raw_values is None
            or before.layout is None
        ):
            return
        from itertools import zip_longest

        formula_positions = {
            before.layout.header_to_physical_col[name]
            for name in before.formula_columns
            if name in before.layout.header_to_physical_col
        }
        ranges = self._last_action_actual_ranges
        for offset, (old_row, new_row) in enumerate(
            zip_longest(before.raw_values, after.raw_values, fillvalue=())
        ):
            if old_row == new_row:
                continue
            row = before.layout.start_row + offset
            row_ranges = [
                item for item in ranges if item.start_row <= row <= item.end_row
            ]
            for col_offset, (old, new) in enumerate(
                zip_longest(old_row, new_row, fillvalue=None)
            ):
                col = before.layout.start_column + col_offset
                if col in formula_positions or any(
                    item.start_col <= col <= item.end_col for item in row_ranges
                ):
                    continue
                if self.converter._is_empty_value(
                    old
                ) and self.converter._is_empty_value(new):
                    continue
                if old != new:
                    raise RuntimeError(
                        f"Sheet 本次写入范围外的单元格发生变化: row={row}, column={col}"
                    )

    def _advance_snapshot_after_mutation(self, action: ExecutionAction) -> bool:
        precondition = action.precondition
        if precondition is None:
            return True

        def advance() -> bool:
            if precondition.kind == "bitable_schema":
                fields = tuple(
                    self._bitable_backend().list_fields(
                        self._bitable_target().app_token,
                        self._bitable_target().table_id,
                    )
                )
                if isinstance(action, CreateFieldAction) and not any(
                    field.name == action.field_name for field in fields
                ):
                    return False
                if isinstance(action, CreateFieldAction):
                    unchanged_fields = tuple(
                        field for field in fields if field.name != action.field_name
                    )
                    expected_fingerprint = (
                        self._expected_bitable_schema_fingerprint
                        or precondition.expected.get("fingerprint")
                    )
                    if (
                        self._schema_fingerprint(unchanged_fields)
                        != expected_fingerprint
                    ):
                        raise RuntimeError(
                            "创建字段后其他 schema 发生变化；停止后续写入"
                        )
                    previous = (
                        self._expected_bitable_snapshot
                        or precondition.expected.get("record_snapshot")
                    )
                    if isinstance(previous, BitableSnapshot):
                        current = self._read_current_bitable_snapshot()
                        if {
                            (field.id, field.name, field.kind)
                            for field in current.schema
                        } != {(field.id, field.name, field.kind) for field in fields}:
                            return False
                        before = {
                            record.record_id: dict(record.fields)
                            for record in previous.records
                        }
                        after = {
                            record.record_id: dict(record.fields)
                            for record in current.records
                        }
                        if (
                            current.backend != previous.backend
                            or current.timezone != previous.timezone
                            or len(before) != len(previous.records)
                            or len(after) != len(current.records)
                            or before.keys() != after.keys()
                            or any(
                                not cells_equal(
                                    values.get(field.name),
                                    after[record_id].get(field.name),
                                    field,
                                    previous.timezone,
                                )
                                for record_id, values in before.items()
                                for field in previous.schema
                            )
                            or any(
                                not self.converter._is_empty_value(
                                    record.fields.get(action.field_name)
                                )
                                and not (
                                    action.suggested_type == 7
                                    and record.fields.get(action.field_name) is False
                                )
                                for record in current.records
                            )
                        ):
                            raise RuntimeError(
                                "创建字段后出现未预期的记录变化；停止后续写入"
                            )
                        self._expected_bitable_snapshot = current
                        self._expected_bitable_revision = current.revision
                self._expected_bitable_schema_fingerprint = self._schema_fingerprint(
                    fields
                )
            elif precondition.kind in {"bitable_records", "bitable_absent_keys"}:
                current = self._read_current_bitable_snapshot()
                if (
                    current.backend is BitableBackendKind.BASE_V3
                    and self._last_action_revision is not None
                    and current.revision != self._last_action_revision
                ):
                    expected_rev = self._last_action_revision
                    if (
                        isinstance(expected_rev, int)
                        and isinstance(current.revision, int)
                        and current.revision > expected_rev
                    ):
                        raise RuntimeError("写入后目标版本再次变化，请读取最新状态")
                    # Opaque revisions cannot be ordered: an intermediate batch
                    # revision may still be propagating. Never adopt it as current.
                    return False
                previous = self._expected_bitable_snapshot
                if isinstance(action, DeleteRecordsAction) and previous is not None:
                    expected_ids = {
                        record.record_id for record in previous.records
                    } - set(action.record_ids)
                    actual_ids = {record.record_id for record in current.records}
                    if (
                        not expected_ids <= actual_ids
                        or actual_ids - expected_ids - set(action.record_ids)
                    ):
                        raise RuntimeError("删除后出现未预期的记录变化；停止后续写入")
                    if actual_ids != expected_ids:
                        return False
                if isinstance(action, CreateRecordsAction):
                    created_ids = {
                        rid
                        for receipt in self._last_action_receipts
                        for rid in receipt.record_ids
                    }
                    if not created_ids <= {
                        record.record_id for record in current.records
                    }:
                        return False
                self._expected_bitable_snapshot = current
                self._expected_bitable_revision = current.revision
            elif precondition.kind.startswith("sheet_"):
                previous_sheet = self._expected_sheet_snapshot
                current_sheet = self._current_sheet_snapshot(
                    index_field_types=precondition.expected.get("index_field_types")
                )
                if (
                    isinstance(action, (WriteColumnsAction, AppendRowsAction))
                    and previous_sheet is not None
                ):
                    if current_sheet.header != previous_sheet.header:
                        raise RuntimeError("写入后 Sheet 表头发生变化")
                    old_mapping = dict(previous_sheet.index_mapping)
                    new_mapping = dict(current_sheet.index_mapping)
                    if any(
                        new_mapping.get(key) != row for key, row in old_mapping.items()
                    ):
                        raise RuntimeError(
                            "写入后既有 Sheet 记录物理行发生变化；停止后续写入"
                        )
                    self._check_sheet_unmodified_cells(previous_sheet, current_sheet)
                self._expected_sheet_snapshot = current_sheet
            return True

        try:
            prior = getattr(self, "_last_action_confirmation", {})
            success = self._wait_for_confirmation(advance, "后续操作所需的目标状态")
            if success:
                self._last_action_confirmation = prior
            else:
                self._last_action_error_kind = ErrorKind.VERIFICATION
            return success
        except Exception as error:
            self._last_action_error_kind = ErrorKind.VERIFICATION
            self._last_action_failure_message = (
                f"mutation 后无法推进 snapshot freshness: {error}"
            )
            return False

    def _reset_action_execution_state(self) -> None:
        self._last_action_error_kind = ErrorKind.MUTATION
        self._last_action_applied_count = 0
        self._last_action_accepted_units = 0
        self._last_action_applied_rows = set()
        self._last_action_receipts: List[MutationReceipt] = []
        self._last_action_actual_ranges: List[A1Range] = []
        self._last_action_mutation_complete = False
        self._last_action_remote_outcome = None
        self._last_action_revision = None
        self._last_action_failure_message = None
        self._last_action_confirmation = {}
        self._last_action_confirmed_count = 0

    def _record_action_receipt(self, receipt: MutationReceipt) -> None:
        if not hasattr(self, "_last_action_accepted_units"):
            self._reset_action_execution_state()
        self._last_action_receipts.append(receipt)
        accepted = max(0, int(receipt.accepted_count))
        self._last_action_accepted_units += accepted
        for item in receipt.actual_ranges:
            if isinstance(item, A1Range):
                self._last_action_actual_ranges.append(item)
                self._last_action_applied_rows.update(
                    range(item.start_row, item.end_row + 1)
                )
        if self._last_action_applied_rows:
            self._last_action_applied_count = len(self._last_action_applied_rows)
        else:
            self._last_action_applied_count += accepted
        self._last_action_remote_outcome = receipt.outcome.value
        if receipt.revision is not None:
            self._last_action_revision = receipt.revision

    def _mark_action_failure(self, kind: ErrorKind) -> bool:
        self._last_action_error_kind = kind
        if kind is ErrorKind.VERIFICATION:
            detail = getattr(self, "_last_action_confirmation", {})
            if detail.get("status") in {None, "verified"}:
                self._last_action_confirmation = {**detail, "status": "failed"}
        return False

    @staticmethod
    def _is_auth_error(error: Exception) -> bool:
        from api import FeishuAPIError

        return isinstance(error, FeishuAPIError) and (
            error.code in {10003, 99991661, 99991663, 99991664, 99991668}
            or error.http_status in {401, 403}
        )

    @staticmethod
    def _is_resource_error(error: Exception) -> bool:
        from api import FeishuAPIError

        return isinstance(error, FeishuAPIError) and error.http_status == 404

    @staticmethod
    def _covered_interval(
        start: int, end: int, intervals: Sequence[tuple[int, int]]
    ) -> bool:
        cursor = start
        for lo, hi in sorted(intervals):
            if lo > cursor:
                return False
            if hi >= cursor:
                cursor = hi + 1
            if cursor > end:
                return True
        return cursor > end

    def _confirmed_action_units(self, action: ExecutionAction) -> int:
        if self._last_action_mutation_complete:
            return action.count
        ranges = self._last_action_actual_ranges
        if isinstance(action, WriteColumnsAction):
            return sum(
                self._covered_interval(
                    action.start_row,
                    action.start_row + len(values) - 1,
                    [
                        (item.start_row, item.end_row)
                        for item in ranges
                        if item.start_col
                        <= action.column_positions[column]
                        <= item.end_col
                    ],
                )
                for column, values in action.column_data.items()
                if values
            )
        if isinstance(action, (WriteRangeAction, ClearRangeAction)):
            if isinstance(action, ClearRangeAction):
                requested = A1Range.parse(action.a1_range)
            else:
                if not action.values:
                    return 0
                start_row = self._sheet_target().start_row
                start_col = self.converter.column_letter_to_number(
                    self._sheet_target().start_column
                )
                requested = A1Range(
                    str(self._sheet_target().sheet_id),
                    start_row,
                    start_row + len(action.values) - 1,
                    start_col,
                    start_col + len(action.values[0]) - 1,
                )
            cuts = sorted(
                {requested.start_row, requested.end_row + 1}
                | {
                    max(requested.start_row, min(requested.end_row + 1, pos))
                    for item in ranges
                    for pos in (item.start_row, item.end_row + 1)
                }
            )
            rows = sum(
                hi - lo
                for lo, hi in zip(cuts, cuts[1:])
                if self._covered_interval(
                    requested.start_col,
                    requested.end_col,
                    [
                        (item.start_col, item.end_col)
                        for item in ranges
                        if item.sheet_id == requested.sheet_id
                        and item.start_row <= lo
                        and item.end_row >= hi - 1
                    ],
                )
            )
            return (
                int(rows == requested.row_count)
                if isinstance(action, ClearRangeAction)
                else rows
            )
        return min(self._last_action_accepted_units, action.count)

    def _applied_action_prefix(
        self, action: ExecutionAction
    ) -> Optional[PlanActionDocument]:
        if self._last_action_mutation_complete:
            return action.to_public()
        accepted = self._last_action_accepted_units
        applied_rows = len(self._last_action_applied_rows)
        if accepted <= 0 and applied_rows <= 0:
            return None
        count = self._confirmed_action_units(action)
        units = {receipt.unit for receipt in self._last_action_receipts if receipt.unit}
        scope = dict(action.scope)
        scope.update(
            {
                "partial": True,
                "accepted_units": accepted,
                "receipt_unit": next(iter(units)) if len(units) == 1 else "unspecified",
                "requested_count": action.count,
                "confirmed_count": getattr(self, "_last_action_confirmed_count", 0),
                "confirmation": getattr(self, "_last_action_confirmation", {}),
                "next_step": "读取目标状态后再决定如何继续；不要自动重跑整个任务",
                "actual_ranges": [
                    item.text for item in self._last_action_actual_ranges
                ],
            }
        )
        if applied_rows:
            scope["applied_physical_rows"] = applied_rows
        if self._last_action_remote_outcome:
            scope["remote_outcome"] = self._last_action_remote_outcome
        return PlanActionDocument(
            kind=action.kind,
            count=count,
            unit=action.unit,
            scope=scope,
            destructive=action.destructive,
            clears_values=action.clears_values,
        )

    def _action_error(self, action: ExecutionAction, message: str) -> Mapping[str, Any]:
        message = self._last_action_failure_message or message
        error: Dict[str, Any] = {
            "kind": self._last_action_error_kind.value,
            "message": message,
            "failed_action": action.kind,
            "accepted_count": self._confirmed_action_units(action),
            "accepted_units": self._last_action_accepted_units,
            "unit": action.unit.value,
            "requested_count": action.count,
            "confirmed_count": min(
                action.count, getattr(self, "_last_action_confirmed_count", 0)
            ),
            "confirmation": getattr(self, "_last_action_confirmation", {}),
            "next_step": "先读取目标状态；不要自动重跑整个追加、覆盖或清空任务",
        }
        if self._last_action_remote_outcome:
            error["remote_outcome"] = self._last_action_remote_outcome
            error["unknown"] = (
                self._last_action_remote_outcome
                == MutationOutcome.UNKNOWN_OUTCOME.value
            )
        return error

    def _confirmation_document(
        self, action: ExecutionAction, *, failed: bool = False
    ) -> Mapping[str, Any]:
        detail = getattr(self, "_last_action_confirmation", {})
        status = (
            detail.get("status", "failed")
            if failed
            else (
                "verified" if self.sync_config.verify_remote_writes else "not_requested"
            )
        )
        if isinstance(action, CreateFieldAction) and not failed:
            status = "pending_schema"
        count = (
            min(action.count, getattr(self, "_last_action_confirmed_count", 0))
            if failed
            else action.count if status == "verified" else 0
        )
        return {
            "kind": action.kind,
            "unit": action.unit.value,
            "status": status,
            "ok": not failed,
            "confirmed_count": count,
            "attempts": detail.get("attempts", 0),
            "elapsed_seconds": detail.get("elapsed_seconds", 0.0),
            "formula_scan": detail.get("formula_scan"),
        }

    def _execute_action(self, action: ExecutionAction) -> bool:
        if isinstance(action, CreateFieldAction):
            receipt = self._bitable_backend().create_field(
                cast(str, self._bitable_target().app_token),
                cast(str, self._bitable_target().table_id),
                action.field_name,
                action.suggested_type,
            )
            self._record_action_receipt(receipt)
            if receipt.outcome is not MutationOutcome.ACCEPTED:
                return self._mark_action_failure(ErrorKind.MUTATION)
            self._last_action_mutation_complete = True
            return True
        if isinstance(action, (CreateRecordsAction, UpdateRecordsAction)):
            records = list(action.records)
            operation = (
                "create" if isinstance(action, CreateRecordsAction) else "update"
            )
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
        if isinstance(action, DeleteRecordsAction):
            record_ids = list(action.record_ids)
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
        if isinstance(action, ClearRangeAction):
            return self._typed_sheet_clear(action.a1_range)
        if isinstance(action, WriteRangeAction):
            return self._typed_sheet_write([list(row) for row in action.values])
        if isinstance(action, AppendRowsAction):
            return self._typed_sheet_append(
                [list(row) for row in action.values],
                header_width=action.header_width,
                start_row=action.start_row,
            )
        if isinstance(action, WriteColumnsAction):
            return self._typed_sheet_selective_write(
                {name: list(values) for name, values in action.column_data.items()},
                dict(action.column_positions),
                start_row=action.start_row,
                max_gap=action.max_gap,
                header_width=action.header_width,
            )
        if isinstance(action, ApplySheetConfigAction):
            success = self._setup_sheet_intelligence(action.frame)
            if success:
                self._last_action_applied_count = action.count
                self._last_action_accepted_units = action.count
                self._last_action_mutation_complete = True
            return success
        raise ValueError(f"未知 plan action: {action.kind}")

    def _refresh_and_verify_created_fields(
        self, actions: List[CreateFieldAction]
    ) -> Tuple[bool, str]:
        valid = self._wait_for_confirmation(
            lambda: self._refresh_created_fields_once(actions)[0],
            "新建字段",
        )
        return valid, (
            "" if valid else self._last_action_failure_message or "新建字段尚未可见"
        )

    def _refresh_created_fields_once(
        self, actions: List[CreateFieldAction]
    ) -> Tuple[bool, str]:
        """Refresh backend schema cache and validate every planned field."""
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            return False, "目标 Bitable 配置不完整"
        from api.bitable_backend import field_kind_from_type

        fields = self._bitable_backend().list_fields(
            self._bitable_target().app_token, self._bitable_target().table_id
        )
        self._refreshed_bitable_fields = tuple(fields)
        by_name = {field.name: field for field in fields}
        backend_kind = BitableBackendKind(self._bitable_target().backend)
        for action in actions:
            name = action.field_name
            suggested_type = action.suggested_type
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
                raise ValueError(f"字段 '{name}' 创建后的写入形状与计划不兼容")
            if (
                backend_kind is BitableBackendKind.BITABLE_V1
                and actual.raw_type != suggested_type
            ):
                raise ValueError(f"字段 '{name}' 创建后的 raw_type 与计划不一致")
        return True, ""

    def execute_plan(self, plan: ExecutionPlan) -> SyncResult:
        """Execute ordered actions, stopping at the first failed action."""
        if not isinstance(plan, ExecutionPlan):
            raise TypeError("executor only accepts an internal ExecutionPlan")
        public_plan = plan.to_public()
        self._expected_bitable_snapshot = None
        self._expected_bitable_revision = None
        self._expected_bitable_schema_fingerprint = None
        self._expected_sheet_snapshot = None
        applied: List[PlanActionDocument] = []
        verification: List[Mapping[str, Any]] = []
        result_warnings = list(plan.warnings)
        created_field_actions: List[CreateFieldAction] = []
        fields_refreshed = False
        if not plan.actions:
            return SyncResult(
                OutcomeStatus.NOOP,
                public_plan,
                warnings=tuple(result_warnings),
            )
        try:
            self._preflight_plan(plan)
        except (ValueError, TypeError, OverflowError) as error:
            return SyncResult(
                OutcomeStatus.FAILED,
                public_plan,
                warnings=tuple(result_warnings),
                error={
                    "kind": ErrorKind.VALIDATION.value,
                    "message": str(error),
                    "accepted_units": 0,
                },
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
                    verification = [
                        (
                            {
                                **item,
                                **getattr(self, "_last_action_confirmation", {}),
                                "ok": False,
                            }
                            if item.get("kind") == "create_fields"
                            else item
                        )
                        for item in verification
                    ]
                    error_kind = ErrorKind.VERIFICATION
                    if refresh_error is not None:
                        if self._is_auth_error(refresh_error):
                            error_kind = ErrorKind.AUTH
                        elif self._is_resource_error(refresh_error):
                            error_kind = ErrorKind.RESOURCE
                    return SyncResult(
                        OutcomeStatus.PARTIAL,
                        public_plan,
                        applied=tuple(applied),
                        verification=tuple(verification),
                        warnings=tuple(result_warnings),
                        error={
                            "kind": error_kind.value,
                            "message": message,
                            "failed_action": "create_fields",
                            "accepted_units": len(created_field_actions),
                            "unit": "field",
                            "confirmed_count": 0,
                            "confirmation": dict(
                                getattr(self, "_last_action_confirmation", {})
                            ),
                        },
                    )
                try:
                    self._preflight_plan(
                        replace(
                            plan,
                            bitable_fields=getattr(
                                self, "_refreshed_bitable_fields", plan.bitable_fields
                            ),
                        )
                    )
                except (ValueError, TypeError, OverflowError) as error:
                    return SyncResult(
                        OutcomeStatus.PARTIAL,
                        public_plan,
                        applied=tuple(applied),
                        verification=tuple(verification),
                        warnings=tuple(result_warnings),
                        error={
                            "kind": "validation",
                            "message": str(error),
                            "failed_action": "prepare_records",
                        },
                    )
                fields_refreshed = True
                verification = [
                    (
                        {
                            **item,
                            "confirmed_count": 1,
                            "status": "schema_confirmed",
                            "ok": True,
                        }
                        if item.get("kind") == "create_fields"
                        else item
                    )
                    for item in verification
                ]
            self._reset_action_execution_state()
            if action.verification_policy is VerificationPolicy.BEST_EFFORT:
                try:
                    best_effort_success = self._execute_action(action)
                except Exception:
                    best_effort_success = False
                if not best_effort_success:
                    prefix = self._applied_action_prefix(action)
                    if prefix is not None:
                        applied.append(prefix)
                    warning = (
                        "Sheet best-effort 字段配置失败；"
                        "已确认的数据写入状态保持不变"
                    )
                    self.logger.warning(warning)
                    result_warnings.append(warning)
                    verification.append(
                        {
                            "kind": action.kind,
                            "status": "best_effort_failed",
                            "ok": True,
                        }
                    )
                    continue
                applied.append(action.to_public())
                verification.append(
                    {
                        "kind": action.kind,
                        "status": (
                            "not_supported"
                            if self.sync_config.verify_remote_writes
                            else "not_requested"
                        ),
                        "ok": True,
                    }
                )
                continue
            try:
                success = self._check_action_precondition(
                    action
                ) and self._execute_action(action)
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
                        self._confirmation_document(action, failed=True)
                    )
                unknown = (
                    self._last_action_remote_outcome
                    == MutationOutcome.UNKNOWN_OUTCOME.value
                )
                partial = (
                    self._last_action_remote_outcome == MutationOutcome.PARTIAL.value
                )
                status = (
                    OutcomeStatus.INDETERMINATE
                    if unknown
                    else (
                        OutcomeStatus.PARTIAL
                        if applied or partial
                        else OutcomeStatus.FAILED
                    )
                )
                return SyncResult(
                    status,
                    public_plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(result_warnings),
                    error=self._action_error(action, str(error)),
                )
            if not success:
                prefix = self._applied_action_prefix(action)
                if prefix is not None:
                    applied.append(prefix)
                if self._last_action_error_kind is ErrorKind.VERIFICATION:
                    verification.append(
                        self._confirmation_document(action, failed=True)
                    )
                unknown = (
                    self._last_action_remote_outcome
                    == MutationOutcome.UNKNOWN_OUTCOME.value
                )
                partial = (
                    self._last_action_remote_outcome == MutationOutcome.PARTIAL.value
                )
                status = (
                    OutcomeStatus.INDETERMINATE
                    if unknown
                    else (
                        OutcomeStatus.PARTIAL
                        if applied or partial
                        else OutcomeStatus.FAILED
                    )
                )
                return SyncResult(
                    status,
                    public_plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(result_warnings),
                    error=self._action_error(action, f"action failed: {action.kind}"),
                )
            if not self._advance_snapshot_after_mutation(action):
                applied.append(action.to_public())
                verification.append(self._confirmation_document(action, failed=True))
                return SyncResult(
                    OutcomeStatus.PARTIAL,
                    public_plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(result_warnings),
                    error=self._action_error(
                        action, "snapshot verification failed after mutation"
                    ),
                )
            applied.append(action.to_public())
            if isinstance(action, CreateFieldAction):
                created_field_actions.append(action)
            verification.append(self._confirmation_document(action))
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
                verification = [
                    (
                        {
                            **item,
                            **getattr(self, "_last_action_confirmation", {}),
                            "ok": False,
                        }
                        if item.get("kind") == "create_fields"
                        else item
                    )
                    for item in verification
                ]
                error_kind = ErrorKind.VERIFICATION
                if refresh_error is not None:
                    if self._is_auth_error(refresh_error):
                        error_kind = ErrorKind.AUTH
                    elif self._is_resource_error(refresh_error):
                        error_kind = ErrorKind.RESOURCE
                return SyncResult(
                    OutcomeStatus.PARTIAL,
                    public_plan,
                    applied=tuple(applied),
                    verification=tuple(verification),
                    warnings=tuple(result_warnings),
                    error={
                        "kind": error_kind.value,
                        "message": message,
                        "failed_action": "create_fields",
                        "accepted_units": len(created_field_actions),
                        "unit": "field",
                        "confirmed_count": 0,
                        "confirmation": dict(
                            getattr(self, "_last_action_confirmation", {})
                        ),
                    },
                )
            verification = [
                (
                    {
                        **item,
                        "confirmed_count": 1,
                        "status": "schema_confirmed",
                        "ok": True,
                    }
                    if item.get("kind") == "create_fields"
                    else item
                )
                for item in verification
            ]
        return SyncResult(
            OutcomeStatus.SUCCESS,
            public_plan,
            applied=tuple(applied),
            verification=tuple(verification),
            warnings=tuple(result_warnings),
        )
