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
from typing import Optional, Dict, Any, List, Mapping, Union, Tuple, cast

from .config import MatchStrategy, SourceType, SyncMode, TargetType
from .converter import DataConverter
from .bootstrap import bootstrap_runtime
from .compiler import BitablePlanCompiler, SheetPlanCompiler
from .key_policy import KeyPolicy
from .mode_policy import ModeDecision, ModePolicy
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
from .snapshot import BitableSnapshot, SheetSnapshot, SourceTable, content_fingerprint
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
        if not self.sync_config.verify_remote_writes:
            return True
        if not receipts:
            return True

        if operation == "delete":
            record_ids = [str(item) for item in requested]
            if not record_ids:
                return True
            if (
                not self._bitable_target().app_token
                or not self._bitable_target().table_id
            ):
                return False
            backend = self._bitable_backend()
            result = backend.batch_get_records(
                self._bitable_target().app_token,
                self._bitable_target().table_id,
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
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            return False
        backend = self._bitable_backend()
        observed = backend.batch_get_records(
            self._bitable_target().app_token,
            self._bitable_target().table_id,
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
            for range_text, expected in expected_ranges.items():
                try:
                    observed = self.api.get_sheet_data(
                        self._sheet_target().spreadsheet_token, range_text
                    )
                except Exception as error:
                    self.logger.error(f"Sheet 写后读回失败: {error}")
                    return self._mark_action_failure(ErrorKind.VERIFICATION)
                if observed != expected:
                    self.logger.error(f"Sheet 写后读回不一致: {range_text}")
                    return self._mark_action_failure(ErrorKind.VERIFICATION)

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
        try:
            result = self.api.verify_formulas(
                self._sheet_target().spreadsheet_token,
                [str(self._sheet_target().sheet_id)],
                ranges,
                max_locations_per_error=self._sheet_target().formula_max_locations,
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
        self, values: List[List[Any]], *, header_width: int
    ) -> bool:
        if (
            not values
            or not isinstance(self.api, SheetAPI)
            or not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return False
        placeholder_end_row = self._sheet_target().start_row + len(values) - 1
        end_col = self.api.start_col_num + len(values[0]) - 1
        requested = A1Range(
            str(self._sheet_target().sheet_id),
            self._sheet_target().start_row,
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
        try:
            observed = self.api.get_sheet_data(
                self._sheet_target().spreadsheet_token, a1.text
            )
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
        self._last_sheet_read_range = None
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
        if not self._sheet_target().validate_results:
            # 未启用检测，使用原有单次读取逻辑
            return self.get_current_sheet_data(), None, None

        # 启用检测，执行双读
        if not isinstance(self.api, SheetAPI):
            return pd.DataFrame(), None, None

        if (
            not self._sheet_target().spreadsheet_token
            or not self._sheet_target().sheet_id
        ):
            return pd.DataFrame(), None, None

        # 获取网格范围
        grid = self._get_sheet_grid_properties()
        if not grid:
            self.logger.warning("无法获取工作表网格属性，无法进行双读")
            return self.get_current_sheet_data(), None, None

        row_count, col_count = grid
        start_col_num = self.api.column_letter_to_number(
            self._sheet_target().start_column
        )
        if row_count < self._sheet_target().start_row or col_count < start_col_num:
            self.logger.info("工作表范围小于起始位置，视为空表")
            return pd.DataFrame(), None, None

        end_row = row_count
        end_col = self.api.column_number_to_letter(col_count)

        self.logger.info("🔍 启用结果检测，开始双读云端数据...")

        # 第一次读取：公式模式
        self.logger.info("  📖 读取公式数据...")
        original_value_option = self._sheet_target().value_render_option
        original_datetime_option = self._sheet_target().datetime_render_option
        original_api_value_option = self.api.value_render_option
        original_api_datetime_option = self.api.datetime_render_option
        self._last_sheet_read_range = (
            f"{self._sheet_target().sheet_id}!{self._sheet_target().start_column}{self._sheet_target().start_row}:"
            f"{end_col}{end_row}"
        )
        try:
            # 强制使用 Formula 模式读取
            self.api.value_render_option = "Formula"
            self.api.datetime_render_option = None

            formula_values = self.api.get_sheet_data_chunked(
                self._sheet_target().spreadsheet_token,
                self._sheet_target().sheet_id,
                self._sheet_target().start_row,
                end_row,
                self._sheet_target().start_column,
                end_col,
            )
            formula_df = self.converter.values_to_df(formula_values)

        except Exception as e:
            self.logger.warning(f"读取公式数据失败: {e}")
            return self.get_current_sheet_data(), None, None
        finally:
            self.api.value_render_option = original_api_value_option
            self.api.datetime_render_option = original_api_datetime_option

        # 第二次读取：结果模式
        self.logger.info("  📊 读取计算结果数据...")
        try:
            # 使用配置的读取选项（或 FormattedValue 作为默认）
            self.api.value_render_option = original_value_option or "FormattedValue"
            self.api.datetime_render_option = (
                original_datetime_option or "FormattedString"
            )

            result_values = self.api.get_sheet_data_chunked(
                self._sheet_target().spreadsheet_token,
                self._sheet_target().sheet_id,
                self._sheet_target().start_row,
                end_row,
                self._sheet_target().start_column,
                end_col,
            )
            result_df = self.converter.values_to_df(result_values)

        except Exception as e:
            self.logger.warning(f"读取结果数据失败: {e}")
            return self.get_current_sheet_data(), None, None
        finally:
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
            # clone 模式只需 record_id（API 固定返回），使用空 field_names 返回最小字段集
            return []

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
                    precondition = SnapshotPrecondition(
                        "bitable_schema", {"fingerprint": fingerprint}
                    )
            elif isinstance(snapshot, BitableSnapshot):
                expected: Dict[str, Any] = {
                    "backend": snapshot.backend.value,
                    "revision": snapshot.revision,
                    "fingerprint": snapshot.fingerprint,
                    "index_column": self.sync_config.index.column,
                }
                index_schema = next(
                    (
                        field
                        for field in snapshot.schema
                        if field.name == self.sync_config.index.column
                    ),
                    None,
                )
                if index_schema is not None:
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
        return ExecutionPlan(
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

        creates: List[CanonicalRecord] = []
        updates: List[CanonicalRecord] = []
        deletes: List[str] = []
        source_rows = tuple(row for _, row in df.iterrows())
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
            assert reconciliation is not None
            for _, _, target_record in reconciliation.matched:
                record_id = target_record.get("record_id")
                if record_id:
                    deletes.append(str(record_id))
            creates = self._canonical_records(
                self.converter.df_to_records(df, field_types)
            )
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
        return SheetPlanCompiler.write(
            values,
            scope={"target": "sheet", "columns": len(df.columns)},
        )

    def _sheet_append_action(self, df: pd.DataFrame) -> AppendRowsAction:
        values = self.converter.df_to_values(df, include_headers=False)
        return SheetPlanCompiler.append(
            values,
            header_width=len(df.columns),
            scope={"target": "sheet", "columns": len(df.columns)},
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
        start_col_offset = (
            self.api.start_col_num - 1 if isinstance(self.api, SheetAPI) else 0
        )
        positions = self.converter.get_column_positions(
            current_df, columns, start_col_offset
        )
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
            header_width=len(current_df.columns) or len(columns),
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
        update_data_map: Dict[int, Dict[str, Any]] = {}
        reconciliation = Reconciler.by_key(
            (row for _, row in df.iterrows()),
            current_index,
            source_key=lambda row: self.converter.get_index_value_hash(
                row, self.sync_config.index.column, dict(index_field_types)
            ),
        )
        if mode in {SyncMode.FULL, SyncMode.OVERWRITE}:
            for _, row, row_index in reconciliation.matched:
                update_data_map[row_index] = {
                    column: row[column] for column in columns if column in row
                }

        actions: List[ExecutionAction] = []
        if update_data_map:
            action = self._sheet_columns_action(
                df,
                current_df,
                columns,
                start_row=self._sheet_target().start_row + 1,
                preserve_rows=True,
                update_data_map=update_data_map,
            )
            if action:
                actions.append(action)
        if reconciliation.missing:
            new_df = pd.DataFrame(reconciliation.missing)
            action = self._sheet_columns_action(
                new_df,
                current_df,
                columns,
                start_row=self._sheet_target().start_row + len(current_df) + 1,
                preserve_rows=False,
            )
            if action:
                actions.append(action)
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
    ) -> SheetSnapshot:
        read_range = getattr(self, "_last_sheet_read_range", None)
        snapshot = SheetSnapshot.from_dataframe(
            frame,
            actual_ranges=((read_range,) if read_range else ()),
            grid=getattr(self, "_sheet_grid_cache", None),
            index_mapping=index_mapping,
            formula_columns=tuple(str(item) for item in (formula_columns or ())),
            complete=getattr(self, "_sheet_read_complete", True),
        )
        self._planned_target_snapshot = snapshot
        return snapshot

    def _plan_file_sheet(self, df: pd.DataFrame) -> ExecutionPlan:
        requested_mode = self.sync_config.mode
        source = {"type": "file", "rows": len(df), "columns": len(df.columns)}
        target = {"type": "sheet"}
        warnings: List[str] = []
        actions: List[ExecutionAction]
        if self.sync_config.match_strategy is MatchStrategy.APPEND_ONLY:
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
        self._capture_sheet_snapshot(
            current_df,
            index_mapping={},
            formula_columns=formula_columns,
        )

        if requested_mode is SyncMode.CLONE:
            actions = [self._sheet_clear_action(), self._sheet_write_action(df)]
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
            rows = [
                row
                for row_index, row in current_df.iterrows()
                if row_index not in matched_rows
            ]
            rows.extend(row for _, row in sync_df.iterrows())
            merged = pd.DataFrame(rows)
            actions = (
                [self._sheet_write_action(merged)]
                if not merged.empty
                else [self._sheet_clear_action()]
            )
            first = actions[0]
            if isinstance(first, WriteRangeAction):
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

        reconciliation = Reconciler.by_key(
            (row for _, row in sync_df.iterrows()),
            current_index,
            source_key=lambda row: self.converter.get_index_value_hash(
                row, self.sync_config.index.column, index_field_types
            ),
        )

        actions = []
        if requested_mode is SyncMode.FULL and reconciliation.matched:
            updated = current_df.copy()
            for _, row, row_index in reconciliation.matched:
                for column in sync_df.columns:
                    if column in updated.columns:
                        updated.iloc[row_index, updated.columns.get_loc(column)] = row[
                            column
                        ]
            actions.append(self._sheet_write_action(updated))
        if reconciliation.missing:
            actions.append(
                self._sheet_append_action(pd.DataFrame(reconciliation.missing))
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
        self._planned_bitable_schema_fingerprint = None
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
            [self.sync_config.index.column] if self.sync_config.index.column else None
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

    def _current_sheet_snapshot(self) -> SheetSnapshot:
        frame = self.get_current_sheet_data()
        if not self._require_complete_sheet_read("snapshot freshness"):
            raise RuntimeError("目标 Sheet freshness read 不完整")
        mapping: Mapping[str, int] = {}
        if self.sync_config.index.column:
            if self.sync_config.index.column not in frame.columns and not frame.empty:
                raise RuntimeError("目标 Sheet freshness read 的表头已变化")
            if self.sync_config.index.column in frame.columns:
                field_types = self._sheet_index_field_types(frame, frame)
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
            complete=True,
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
                elif self._expected_bitable_snapshot is None and (
                    current.fingerprint != precondition.expected.get("fingerprint")
                ):
                    raise RuntimeError("目标 Bitable 内容在计划后发生变化")
                self._expected_bitable_snapshot = current
                self._expected_bitable_revision = current.revision
                return True
            if precondition.kind.startswith("sheet_"):
                current_sheet = self._current_sheet_snapshot()
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
            self._last_action_error_kind = ErrorKind.STALE_SNAPSHOT
            self._last_action_failure_message = str(error)
            return False
        return True

    def _advance_snapshot_after_mutation(self, action: ExecutionAction) -> bool:
        precondition = action.precondition
        if precondition is None:
            return True
        try:
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
                    raise RuntimeError(
                        f"字段 '{action.field_name}' 创建后未出现在服务端 schema 中"
                    )
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
                    raise RuntimeError(
                        "mutation receipt revision 与后续 Base readback 不一致"
                    )
                self._expected_bitable_snapshot = current
                self._expected_bitable_revision = current.revision
            elif precondition.kind.startswith("sheet_"):
                self._expected_sheet_snapshot = self._current_sheet_snapshot()
            return True
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
        self._last_action_mutation_complete = False
        self._last_action_remote_outcome = None
        self._last_action_revision = None
        self._last_action_failure_message = None

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
        if receipt.revision is not None:
            self._last_action_revision = receipt.revision

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

    def _applied_action_prefix(
        self, action: ExecutionAction
    ) -> Optional[PlanActionDocument]:
        if self._last_action_mutation_complete:
            return action.to_public()
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
        """Refresh backend schema cache and validate every planned field."""
        if not self._bitable_target().app_token or not self._bitable_target().table_id:
            return False, "目标 Bitable 配置不完整"
        from api.bitable_backend import field_kind_from_type

        fields = self._bitable_backend().list_fields(
            self._bitable_target().app_token, self._bitable_target().table_id
        )
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
                return False, f"字段 '{name}' 创建后的写入形状与计划不兼容"
            if (
                backend_kind is BitableBackendKind.BITABLE_V1
                and actual.raw_type != suggested_type
            ):
                return False, f"字段 '{name}' 创建后的 raw_type 与计划不一致"
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
                        },
                    )
                fields_refreshed = True
                if self.sync_config.verify_remote_writes:
                    verification = [
                        (
                            {
                                "kind": verification_item["kind"],
                                "status": "verified",
                                "ok": True,
                            }
                            if verification_item.get("kind") == "create_fields"
                            else verification_item
                        )
                        for verification_item in verification
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
                        {"kind": action.kind, "status": "failed", "ok": False}
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
                        {"kind": action.kind, "status": "failed", "ok": False}
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
                verification.append(
                    {"kind": action.kind, "status": "failed", "ok": False}
                )
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
            verification.append(
                {
                    "kind": action.kind,
                    "status": (
                        "verified"
                        if self.sync_config.verify_remote_writes
                        and action.kind not in {"create_fields", "apply_sheet_config"}
                        else (
                            "not_supported"
                            if self.sync_config.verify_remote_writes
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
                    },
                )
            if self.sync_config.verify_remote_writes:
                verification = [
                    (
                        {
                            "kind": verification_item["kind"],
                            "status": "verified",
                            "ok": True,
                        }
                        if verification_item.get("kind") == "create_fields"
                        else verification_item
                    )
                    for verification_item in verification
                ]
        return SyncResult(
            OutcomeStatus.SUCCESS,
            public_plan,
            applied=tuple(applied),
            verification=tuple(verification),
            warnings=tuple(result_warnings),
        )
