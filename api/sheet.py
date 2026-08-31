#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电子表格 API 模块

模块概述：
    此模块封装了飞书电子表格（Sheets）的 API 操作，提供电子表格的
    读取和写入功能。电子表格是飞书的在线表格产品，类似于 Excel Online
    或 Google Sheets。

主要功能：
    1. 获取电子表格信息
    2. 读取指定范围的数据
    3. 写入数据（支持大数据量自动分块）
    4. 批量单元格操作
    5. 行列操作（清空、删除、追加）
    6. 选择性列同步
    7. 范围优化（合并相邻列）

核心类：
    SheetAPI:
        飞书电子表格 API 客户端，封装所有电子表格相关的 API 调用。
        支持配置起始位置（start_row, start_column）。

写入策略：
    1. 预分块：将大数据量按行和列预先分割
    2. 自动二分重试：遇到"请求过大"错误时自动缩小批次
    3. 频率控制：请求间自动添加延迟避免限流

范围表示法：
    使用 A1 表示法：{sheet_id}!{start_col}{start_row}:{end_col}{end_row}
    示例：Sheet1!A1:C10 表示 Sheet1 工作表的 A1 到 C10 区域

列号转换：
    A=1, B=2, ..., Z=26, AA=27, AB=28, ...
    提供 column_letter_to_number 和 column_number_to_letter 方法

API 端点（基础路径：https://open.feishu.cn/open-apis/sheets）：
    信息：
        GET  /v3/spreadsheets/{token} - 获取电子表格信息
    数据：
        GET  /v2/spreadsheets/{token}/values/{range} - 读取数据
        PUT  /v2/spreadsheets/{token}/values - 写入数据
        POST /v2/spreadsheets/{token}/values_batch_update - 批量更新
        POST /v2/spreadsheets/{token}/values_append - 追加数据
    元数据与公式：
        GET  /sheets/v3/spreadsheets/{token}/sheets/query - 查询工作表元数据
        POST /sheet_ai/v2/spreadsheets/{token}/tools/invoke_read - 校验公式
    行列：
        POST /v2/spreadsheets/{token}/insert_dimension_range - 插入行/列
        DELETE /v2/spreadsheets/{token}/dimension_range - 删除行/列

错误码处理：
    - 90227: 请求过大（自动触发二分重试）
    - 其他错误码按标准流程处理

使用示例：
    >>> from api import FeishuAuth, SheetAPI
    >>>
    >>> auth = FeishuAuth(app_id, app_secret)
    >>> api = SheetAPI(auth, start_row=1, start_column="A")
    >>>
    >>> # 获取表格信息
    >>> info = api.get_sheet_info(spreadsheet_token)
    >>>
    >>> # 读取数据
    >>> data = api.get_sheet_data(spreadsheet_token, "Sheet1!A1:C10")
    >>>
    >>> # 写入数据
    >>> values = [["姓名", "年龄"], ["张三", 25], ["李四", 30]]
    >>> api.write_sheet_data(spreadsheet_token, sheet_id, values)

选择性列同步：
    支持只同步指定的列，而非全部数据：
    1. 配置 selective_sync.columns 指定要同步的列名
    2. 系统自动计算列范围并优化（合并相邻列）
    3. 仅更新指定列的数据，保留其他列不变

性能特性：
    - 自动分块：大数据按 row_batch_size 和 col_batch_size 分块
    - 二分重试：请求过大时自动减半批次大小
    - 范围优化：相邻列合并为连续范围减少 API 调用
    - 串行写入：按成功前缀首错停止，保留范围与错误语义

依赖关系：
    内部模块：
        - api.auth: 认证管理（FeishuAuth）
        - api.base: 网络请求（RetryableAPIClient）
    外部依赖：
        - time: 延迟控制
        - logging: 日志记录

注意事项：
    1. 行号从 1 开始（1-based）
    2. 写入空值会清除单元格内容
    3. 大数据量写入建议调整 batch_size
    4. clone 模式会清空整个工作表

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from .auth import FeishuAuth
from .base import RetryableAPIClient
from .bitable_backend import MutationOutcome, MutationReceipt, ReadbackStatus
from .sdk import FeishuAPIError, FeishuResponseParser
from .url import encode_a1_range, encode_path_segment


@dataclass(frozen=True)
class A1Range:
    """Validated rectangular A1 range used by typed Sheet operations."""

    sheet_id: str
    start_row: int
    end_row: int
    start_col: int
    end_col: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.sheet_id, str)
            or not self.sheet_id
            or "!" in self.sheet_id
        ):
            raise ValueError("A1 sheet_id must be a non-empty path segment")
        bounds = (self.start_row, self.end_row, self.start_col, self.end_col)
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 1
            for value in bounds
        ):
            raise ValueError("A1 row and column bounds must be positive integers")
        if self.end_row < self.start_row or self.end_col < self.start_col:
            raise ValueError("A1 end bounds must not precede start bounds")

    @property
    def row_count(self) -> int:
        return self.end_row - self.start_row + 1

    @property
    def col_count(self) -> int:
        return self.end_col - self.start_col + 1

    @property
    def text(self) -> str:
        return (
            f"{self.sheet_id}!{SheetAPI.column_number_to_letter_static(self.start_col)}"
            f"{self.start_row}:{SheetAPI.column_number_to_letter_static(self.end_col)}"
            f"{self.end_row}"
        )

    @classmethod
    def parse(cls, range_str: str) -> "A1Range":
        if not isinstance(range_str, str):
            raise ValueError("A1 range must be a string")
        match = re.fullmatch(r"([^!]+)!([A-Za-z]+)(\d+):([A-Za-z]+)(\d+)", range_str)
        if not match:
            raise ValueError(f"invalid A1 range: {range_str}")
        sheet_id, start_col, start_row, end_col, end_row = match.groups()
        start_col, end_col = start_col.upper(), end_col.upper()
        start_row_i, end_row_i = int(start_row), int(end_row)
        start_col_i = cls._column_number(start_col)
        end_col_i = cls._column_number(end_col)
        if start_row_i < 1 or end_row_i < start_row_i:
            raise ValueError(f"invalid A1 row bounds: {range_str}")
        if start_col_i < 1 or end_col_i < start_col_i:
            raise ValueError(f"invalid A1 column bounds: {range_str}")
        return cls(sheet_id, start_row_i, end_row_i, start_col_i, end_col_i)

    @staticmethod
    def _column_number(value: str) -> int:
        result = 0
        for char in value:
            result = result * 26 + ord(char) - ord("A") + 1
        return result


@dataclass(frozen=True)
class RangeChunk:
    """One bounded A1 range and its matching rectangular matrix."""

    a1_range: A1Range
    values: Tuple[Tuple[Any, ...], ...]

    def as_lists(self) -> List[List[Any]]:
        return [list(row) for row in self.values]


class RangeChunker:
    """Single source of truth for bounded Sheet range and matrix slicing."""

    def __init__(self, max_rows: int, max_cols: int):
        for name, value in (("max_rows", max_rows), ("max_cols", max_cols)):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        self.max_rows = max_rows
        self.max_cols = max_cols

    @staticmethod
    def copy_matrix(values: Sequence[Sequence[Any]]) -> Tuple[Tuple[Any, ...], ...]:
        if not isinstance(values, (list, tuple)) or not values:
            raise ValueError("typed Sheet values must be a non-empty matrix")
        if any(not isinstance(row, (list, tuple)) or not row for row in values):
            raise ValueError("typed Sheet values must have non-empty rows")
        width = len(values[0])
        if any(len(row) != width for row in values):
            raise ValueError("typed Sheet values must be rectangular")
        return tuple(tuple(row) for row in values)

    @staticmethod
    def require_shape(a1_range: A1Range, values: Sequence[Sequence[Any]]) -> None:
        if len(values) != a1_range.row_count or len(values[0]) != a1_range.col_count:
            raise ValueError(
                "typed Sheet matrix shape does not match A1 range "
                f"{a1_range.text}: expected "
                f"{a1_range.row_count}x{a1_range.col_count}"
            )

    def chunk_count(self, a1_range: A1Range) -> int:
        row_chunks = (a1_range.row_count + self.max_rows - 1) // self.max_rows
        col_chunks = (a1_range.col_count + self.max_cols - 1) // self.max_cols
        return row_chunks * col_chunks

    def split(
        self, a1_range: A1Range, values: Sequence[Sequence[Any]]
    ) -> Iterator[RangeChunk]:
        self.require_shape(a1_range, values)
        for col_start in range(a1_range.start_col, a1_range.end_col + 1, self.max_cols):
            col_end = min(col_start + self.max_cols - 1, a1_range.end_col)
            col_offset = col_start - a1_range.start_col
            width = col_end - col_start + 1
            for row_start in range(
                a1_range.start_row, a1_range.end_row + 1, self.max_rows
            ):
                row_end = min(row_start + self.max_rows - 1, a1_range.end_row)
                row_offset = row_start - a1_range.start_row
                height = row_end - row_start + 1
                chunk_values = tuple(
                    tuple(row[col_offset : col_offset + width])
                    for row in values[row_offset : row_offset + height]
                )
                yield RangeChunk(
                    A1Range(
                        a1_range.sheet_id,
                        row_start,
                        row_end,
                        col_start,
                        col_end,
                    ),
                    chunk_values,
                )

    def empty(self, a1_range: A1Range) -> Iterator[RangeChunk]:
        """Yield bounded empty matrices lazily instead of allocating the full grid."""
        for col_start in range(a1_range.start_col, a1_range.end_col + 1, self.max_cols):
            col_end = min(col_start + self.max_cols - 1, a1_range.end_col)
            width = col_end - col_start + 1
            for row_start in range(
                a1_range.start_row, a1_range.end_row + 1, self.max_rows
            ):
                row_end = min(row_start + self.max_rows - 1, a1_range.end_row)
                height = row_end - row_start + 1
                yield RangeChunk(
                    A1Range(
                        a1_range.sheet_id,
                        row_start,
                        row_end,
                        col_start,
                        col_end,
                    ),
                    tuple(tuple("" for _ in range(width)) for _ in range(height)),
                )

    @staticmethod
    def fixed_band(
        actual_anchor: A1Range, *, column_offset: int, width: int
    ) -> A1Range:
        """Map a source column band onto rows allocated by an append response."""
        if column_offset < 0 or width <= 0:
            raise ValueError("column_offset and width must describe a non-empty band")
        start_col = actual_anchor.start_col + column_offset
        return A1Range(
            actual_anchor.sheet_id,
            actual_anchor.start_row,
            actual_anchor.end_row,
            start_col,
            start_col + width - 1,
        )


@dataclass(frozen=True)
class SheetMetadata:
    """Typed projection of the Sheets v3 ``sheets/query`` response."""

    sheet_id: str
    title: Optional[str] = None
    hidden: Optional[bool] = None
    grid_properties: Mapping[str, Any] = field(default_factory=dict)
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)


@dataclass(frozen=True)
class FormulaVerificationResult:
    """Typed Sheet AI formula verification result.

    ``passed`` is deliberately strict: a result is clean only when the AI
    reports ``success`` and explicitly says that no more pages remain.
    """

    status: str
    has_more: Optional[bool]
    total_errors: Optional[int] = None
    extensions: Mapping[str, Any] = field(default_factory=dict)
    raw: Mapping[str, Any] = field(default_factory=dict, compare=False)
    error: Optional[str] = None

    @property
    def passed(self) -> bool:
        return self.status == "success" and self.has_more is False


_SHEET_BACKEND = "sheet_v2"


class SheetAPI:
    """飞书电子表格API客户端"""

    # 单次扫描范围上限（尽量避免触发 90221: TooLargeResponse）
    MAX_SCAN_ROWS_PER_REQUEST = 5000
    MAX_SCAN_COLS_PER_REQUEST = 100
    # 单次写入/清空范围上限（对齐写入接口限制）
    MAX_WRITE_ROWS_PER_REQUEST = 5000
    MAX_WRITE_COLS_PER_REQUEST = 100

    def __init__(
        self,
        auth: FeishuAuth,
        api_client: Optional[RetryableAPIClient] = None,
        start_row: int = 1,
        start_column: str = "A",
        scan_max_rows: Optional[int] = None,
        scan_max_cols: Optional[int] = None,
        write_max_rows: Optional[int] = None,
        write_max_cols: Optional[int] = None,
        value_render_option: Optional[str] = None,
        datetime_render_option: Optional[str] = None,
    ):
        """
        初始化电子表格API客户端

        Args:
            auth: 飞书认证管理器
            api_client: API客户端实例
            start_row: 起始行号 (1-based)
            start_column: 起始列号
        """
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.logger = logging.getLogger("XTF.sheet")
        self.ERROR_CODE_REQUEST_TOO_LARGE = 90227

        # 存储起始位置配置
        self.start_row = start_row
        self.start_column = start_column
        self.start_col_num = self.column_letter_to_number(start_column)
        # 扫描/写入范围限制（可配置）
        self.scan_max_rows = scan_max_rows or self.MAX_SCAN_ROWS_PER_REQUEST
        self.scan_max_cols = scan_max_cols or self.MAX_SCAN_COLS_PER_REQUEST
        self.write_max_rows = write_max_rows or self.MAX_WRITE_ROWS_PER_REQUEST
        self.write_max_cols = write_max_cols or self.MAX_WRITE_COLS_PER_REQUEST
        # 读取渲染选项（可配置）
        self.value_render_option = value_render_option
        self.datetime_render_option = datetime_render_option

    @staticmethod
    def column_number_to_letter_static(col_num: int) -> str:
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result or "A"

    @staticmethod
    def _typed_matrix(values: Sequence[Sequence[Any]]) -> Tuple[Tuple[Any, ...], ...]:
        """Validate and copy a non-empty rectangular matrix before mutation."""
        return RangeChunker.copy_matrix(values)

    @staticmethod
    def _typed_shape(a1: A1Range, values: Sequence[Sequence[Any]]) -> None:
        RangeChunker.require_shape(a1, values)

    def _range_chunker(self) -> RangeChunker:
        return RangeChunker(self.write_max_rows, self.write_max_cols)

    @staticmethod
    def _typed_failure_receipt(
        operation: str,
        requested: int,
        accepted: int,
        actual_ranges: Sequence[A1Range],
        error: FeishuAPIError,
        *,
        failed_batch_index: int,
        raw_responses: Sequence[Mapping[str, Any]],
        unit: str = "range",
        unknown_scope: bool = False,
    ) -> MutationReceipt:
        unknown = error.kind == "transport"
        return MutationReceipt(
            operation=operation,
            backend=_SHEET_BACKEND,
            requested_count=requested,
            accepted_count=accepted,
            unit=unit,
            actual_ranges=tuple(actual_ranges),
            failed_batch_index=failed_batch_index,
            outcome=(
                MutationOutcome.UNKNOWN_OUTCOME
                if unknown
                else MutationOutcome.PARTIAL if accepted else MutationOutcome.REJECTED
            ),
            readback=(
                ReadbackStatus.UNKNOWN if unknown else ReadbackStatus.NOT_REQUESTED
            ),
            unknown_scope=unknown or unknown_scope,
            raw_metadata={
                "error": str(error),
                "responses": tuple(raw_responses),
                "unknown_scope": unknown or unknown_scope,
            },
        )

    @staticmethod
    def _typed_range_from_value(value: Any) -> Optional[A1Range]:
        if not isinstance(value, str):
            return None
        try:
            return A1Range.parse(value)
        except ValueError:
            return None

    def _typed_actual_ranges(
        self,
        result: Mapping[str, Any],
        fallback: Sequence[str],
        *,
        allow_fallback: bool,
    ) -> Tuple[Tuple[A1Range, ...], bool]:
        """Extract server ranges; fixed writes may fall back to requested ranges."""
        found: List[A1Range] = []
        accepted_keys = {
            "range",
            "updatedRange",
            "updated_range",
            "actualRange",
            "actual_range",
        }

        def visit(value: Any) -> None:
            if isinstance(value, Mapping):
                for key, item in value.items():
                    if key in accepted_keys:
                        parsed = self._typed_range_from_value(item)
                        if parsed is not None and parsed not in found:
                            found.append(parsed)
                    visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        visit(result)
        if found:
            return tuple(found), False
        if allow_fallback:
            ranges = tuple(
                parsed
                for item in fallback
                if (parsed := self._typed_range_from_value(item)) is not None
            )
            return ranges, False
        return (), True

    def _typed_values_call(
        self,
        method: str,
        spreadsheet_token: str,
        endpoint: str,
        body: Mapping[str, Any],
        *,
        retry_transport: bool = True,
    ) -> Mapping[str, Any]:
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/{endpoint}"
        response = self.api_client.call_api(
            method,
            url,
            headers=self.auth.get_auth_headers(),
            json=dict(body),
            retry_transport=retry_transport,
        )
        result = FeishuResponseParser.parse(response)
        if not isinstance(result, dict):
            raise FeishuAPIError(
                -1, "typed Sheet response must be an object", kind="invalid_response"
            )
        return result

    def _typed_sheet_receipt(
        self,
        operation: str,
        requested: int,
        actual_ranges: Sequence[A1Range],
        result: Mapping[str, Any],
        *,
        accepted: Optional[int] = None,
        unit: str = "range",
        unknown_scope: bool = False,
        failed_batch_index: Optional[int] = None,
        outcome: MutationOutcome = MutationOutcome.ACCEPTED,
        extra_metadata: Optional[Mapping[str, Any]] = None,
    ) -> MutationReceipt:
        data = result.get("data", {})
        responses = data.get("responses", []) if isinstance(data, Mapping) else []
        if not isinstance(responses, list):
            responses = []

        def collect_metric(value: Any, key: str) -> int:
            if isinstance(value, Mapping):
                own = value.get(key)
                total = (
                    int(own)
                    if isinstance(own, (int, float)) and not isinstance(own, bool)
                    else 0
                )
                return total + sum(
                    collect_metric(item, key)
                    for name, item in value.items()
                    if name != key
                )
            if isinstance(value, list):
                return sum(collect_metric(item, key) for item in value)
            return 0

        updated_rows = collect_metric(responses, "updatedRows")
        updated_columns = collect_metric(responses, "updatedColumns")
        updated_cells = collect_metric(responses, "updatedCells")
        metadata: Dict[str, Any] = {
            "response": result,
            "unknown_scope": unknown_scope,
        }
        if extra_metadata:
            metadata.update(extra_metadata)
        return MutationReceipt(
            operation=operation,
            backend=_SHEET_BACKEND,
            requested_count=requested,
            accepted_count=(
                requested
                if accepted is None and outcome is MutationOutcome.ACCEPTED
                else accepted or 0
            ),
            unit=unit,
            actual_ranges=tuple(actual_ranges),
            updated_rows=updated_rows or None,
            updated_columns=updated_columns or None,
            updated_cells=updated_cells or None,
            failed_batch_index=failed_batch_index,
            outcome=outcome,
            readback=(
                ReadbackStatus.UNKNOWN
                if unknown_scope
                else ReadbackStatus.NOT_REQUESTED
            ),
            unknown_scope=unknown_scope,
            raw_metadata=metadata,
        )

    def write_values(
        self, spreadsheet_token: str, a1_range: str, values: Sequence[Sequence[Any]]
    ) -> MutationReceipt:
        matrix = self._typed_matrix(values)
        a1 = A1Range.parse(a1_range)
        self._typed_shape(a1, matrix)
        chunker = self._range_chunker()
        applied: List[A1Range] = []
        responses: List[Mapping[str, Any]] = []
        requested_ranges = chunker.chunk_count(a1)
        successful_requests = 0
        failed_request_index = 0
        for chunk in chunker.split(a1, matrix):
            pending = [chunk]
            while pending:
                current = pending.pop(0)
                current_range = current.a1_range
                current_values = current.as_lists()
                failed_request_index += 1
                try:
                    result = self._typed_values_call(
                        "PUT",
                        spreadsheet_token,
                        "values",
                        {
                            "valueRange": {
                                "range": current_range.text,
                                "values": current_values,
                            }
                        },
                    )
                except FeishuAPIError as error:
                    split: List[RangeChunk] = []
                    if error.code == self.ERROR_CODE_REQUEST_TOO_LARGE:
                        if current_range.row_count > 1:
                            first_height = current_range.row_count // 2
                            split = [
                                RangeChunk(
                                    A1Range(
                                        current_range.sheet_id,
                                        current_range.start_row,
                                        current_range.start_row + first_height - 1,
                                        current_range.start_col,
                                        current_range.end_col,
                                    ),
                                    current.values[:first_height],
                                ),
                                RangeChunk(
                                    A1Range(
                                        current_range.sheet_id,
                                        current_range.start_row + first_height,
                                        current_range.end_row,
                                        current_range.start_col,
                                        current_range.end_col,
                                    ),
                                    current.values[first_height:],
                                ),
                            ]
                        elif current_range.col_count > 1:
                            first_width = current_range.col_count // 2
                            split = [
                                RangeChunk(
                                    A1Range(
                                        current_range.sheet_id,
                                        current_range.start_row,
                                        current_range.end_row,
                                        current_range.start_col,
                                        current_range.start_col + first_width - 1,
                                    ),
                                    tuple(row[:first_width] for row in current.values),
                                ),
                                RangeChunk(
                                    A1Range(
                                        current_range.sheet_id,
                                        current_range.start_row,
                                        current_range.end_row,
                                        current_range.start_col + first_width,
                                        current_range.end_col,
                                    ),
                                    tuple(row[first_width:] for row in current.values),
                                ),
                            ]
                    if split:
                        requested_ranges += 1
                        pending[0:0] = split
                        continue
                    return self._typed_failure_receipt(
                        "write",
                        requested_ranges,
                        successful_requests,
                        applied,
                        error,
                        failed_batch_index=failed_request_index,
                        raw_responses=responses,
                        unit="range",
                    )
                ranges, _ = self._typed_actual_ranges(
                    result, [current_range.text], allow_fallback=True
                )
                applied.extend(ranges or (current_range,))
                responses.append(result)
                successful_requests += 1
        return self._typed_sheet_receipt(
            "write",
            requested_ranges,
            applied,
            {"data": {"responses": responses}},
            accepted=successful_requests,
            unit="range",
        )

    def append_values(
        self, spreadsheet_token: str, a1_range: str, values: Sequence[Sequence[Any]]
    ) -> MutationReceipt:
        matrix = self._typed_matrix(values)
        a1 = A1Range.parse(a1_range)
        self._typed_shape(a1, matrix)
        anchor_width = min(a1.col_count, self.write_max_cols)
        anchor_range = A1Range(
            a1.sheet_id,
            a1.start_row,
            a1.end_row,
            a1.start_col,
            a1.start_col + anchor_width - 1,
        )
        anchor_values = tuple(tuple(row[:anchor_width]) for row in matrix)
        anchor_chunker = RangeChunker(self.write_max_rows, anchor_width)
        applied: List[A1Range] = []
        responses: List[Mapping[str, Any]] = []
        source_slices: List[Mapping[str, int | str]] = []
        successful_rows = 0
        request_index = 0
        pending = list(anchor_chunker.split(anchor_range, anchor_values))
        while pending:
            anchor_chunk = pending.pop(0)
            request_index += 1
            try:
                result = self._typed_values_call(
                    "POST",
                    spreadsheet_token,
                    "values_append",
                    {
                        "valueRange": {
                            "range": anchor_chunk.a1_range.text,
                            "values": anchor_chunk.as_lists(),
                        }
                    },
                    retry_transport=False,
                )
            except FeishuAPIError as error:
                if (
                    error.code == self.ERROR_CODE_REQUEST_TOO_LARGE
                    and anchor_chunk.a1_range.row_count > 1
                ):
                    first_height = anchor_chunk.a1_range.row_count // 2
                    pending[0:0] = [
                        RangeChunk(
                            A1Range(
                                anchor_chunk.a1_range.sheet_id,
                                anchor_chunk.a1_range.start_row,
                                anchor_chunk.a1_range.start_row + first_height - 1,
                                anchor_chunk.a1_range.start_col,
                                anchor_chunk.a1_range.end_col,
                            ),
                            anchor_chunk.values[:first_height],
                        ),
                        RangeChunk(
                            A1Range(
                                anchor_chunk.a1_range.sheet_id,
                                anchor_chunk.a1_range.start_row + first_height,
                                anchor_chunk.a1_range.end_row,
                                anchor_chunk.a1_range.start_col,
                                anchor_chunk.a1_range.end_col,
                            ),
                            anchor_chunk.values[first_height:],
                        ),
                    ]
                    continue
                return self._typed_failure_receipt(
                    "append",
                    len(matrix),
                    successful_rows,
                    applied,
                    error,
                    failed_batch_index=request_index,
                    raw_responses=responses,
                    unit="row",
                )
            anchor_ranges, unknown = self._typed_actual_ranges(
                result, (), allow_fallback=False
            )
            responses.append(result)
            if (
                unknown
                or len(anchor_ranges) != 1
                or anchor_ranges[0].row_count != anchor_chunk.a1_range.row_count
                or anchor_ranges[0].col_count != anchor_width
            ):
                return self._typed_sheet_receipt(
                    "append",
                    len(matrix),
                    applied,
                    {"data": {"responses": responses}},
                    accepted=successful_rows,
                    unit="row",
                    unknown_scope=True,
                    failed_batch_index=request_index,
                    outcome=MutationOutcome.UNKNOWN_OUTCOME,
                    extra_metadata={"source_slices": tuple(source_slices)},
                )

            actual_anchor = anchor_ranges[0]
            applied.append(actual_anchor)
            source_slices.append(
                {
                    "range": actual_anchor.text,
                    "row_offset": successful_rows,
                    "col_offset": 0,
                    "row_count": actual_anchor.row_count,
                    "col_count": actual_anchor.col_count,
                }
            )
            for column_offset in range(anchor_width, a1.col_count, self.write_max_cols):
                width = min(self.write_max_cols, a1.col_count - column_offset)
                fixed_range = RangeChunker.fixed_band(
                    actual_anchor, column_offset=column_offset, width=width
                )
                band_values = tuple(
                    tuple(row[column_offset : column_offset + width])
                    for row in matrix[
                        successful_rows : successful_rows
                        + anchor_chunk.a1_range.row_count
                    ]
                )
                request_index += 1
                band_receipt = self.write_values(
                    spreadsheet_token, fixed_range.text, band_values
                )
                for item in band_receipt.actual_ranges:
                    if not isinstance(item, A1Range):
                        continue
                    applied.append(item)
                    source_slices.append(
                        {
                            "range": item.text,
                            "row_offset": successful_rows
                            + item.start_row
                            - fixed_range.start_row,
                            "col_offset": column_offset
                            + item.start_col
                            - fixed_range.start_col,
                            "row_count": item.row_count,
                            "col_count": item.col_count,
                        }
                    )
                if band_receipt.outcome is not MutationOutcome.ACCEPTED:
                    unknown_band = (
                        band_receipt.outcome is MutationOutcome.UNKNOWN_OUTCOME
                    )
                    return MutationReceipt(
                        operation="append",
                        backend=_SHEET_BACKEND,
                        requested_count=len(matrix),
                        accepted_count=successful_rows,
                        unit="row",
                        actual_ranges=tuple(applied),
                        failed_batch_index=request_index,
                        outcome=(
                            MutationOutcome.UNKNOWN_OUTCOME
                            if unknown_band
                            else MutationOutcome.PARTIAL
                        ),
                        readback=(
                            ReadbackStatus.UNKNOWN
                            if unknown_band
                            else ReadbackStatus.NOT_REQUESTED
                        ),
                        unknown_scope=unknown_band,
                        raw_metadata={
                            "responses": tuple(responses),
                            "failed_band": band_receipt.raw_metadata,
                            "unknown_scope": unknown_band,
                            "source_slices": tuple(source_slices),
                        },
                    )
            successful_rows += anchor_chunk.a1_range.row_count
        return self._typed_sheet_receipt(
            "append",
            len(matrix),
            applied,
            {"data": {"responses": responses}},
            accepted=successful_rows,
            unit="row",
            extra_metadata={"source_slices": tuple(source_slices)},
        )

    def batch_update_values(
        self, spreadsheet_token: str, value_ranges: Sequence[Mapping[str, Any]]
    ) -> MutationReceipt:
        if not isinstance(value_ranges, (list, tuple)) or not value_ranges:
            raise ValueError("typed batch update requires non-empty value_ranges")
        normalized: List[Tuple[A1Range, Tuple[Tuple[Any, ...], ...]]] = []
        for item in value_ranges:
            if (
                not isinstance(item, Mapping)
                or "range" not in item
                or "values" not in item
            ):
                raise ValueError("each typed value range requires range and values")
            a1 = A1Range.parse(item["range"])
            matrix = self._typed_matrix(item["values"])
            self._typed_shape(a1, matrix)
            normalized.append((a1, matrix))

        chunker = self._range_chunker()
        requested_ranges = sum(chunker.chunk_count(a1) for a1, _ in normalized)

        def chunks() -> Iterator[RangeChunk]:
            for a1, matrix in normalized:
                yield from chunker.split(a1, matrix)

        return self._submit_batch_chunks(
            spreadsheet_token,
            chunks(),
            requested_ranges=requested_ranges,
            operation="batch_update",
        )

    def _submit_batch_chunks(
        self,
        spreadsheet_token: str,
        chunks: Iterator[RangeChunk],
        *,
        requested_ranges: int,
        operation: str,
    ) -> MutationReceipt:
        applied: List[A1Range] = []
        responses: List[Mapping[str, Any]] = []
        accepted_ranges = 0
        request_index = 0
        for chunk in chunks:
            request_index += 1
            try:
                result = self._typed_values_call(
                    "POST",
                    spreadsheet_token,
                    "values_batch_update",
                    {
                        "valueRanges": [
                            {
                                "range": chunk.a1_range.text,
                                "values": chunk.as_lists(),
                            }
                        ]
                    },
                )
            except FeishuAPIError as error:
                return self._typed_failure_receipt(
                    operation,
                    requested_ranges,
                    accepted_ranges,
                    applied,
                    error,
                    failed_batch_index=request_index,
                    raw_responses=responses,
                    unit="range",
                )
            ranges, unknown = self._typed_actual_ranges(
                result, (chunk.a1_range.text,), allow_fallback=True
            )
            if unknown:
                return self._typed_sheet_receipt(
                    operation,
                    requested_ranges,
                    applied,
                    {"data": {"responses": responses + [result]}},
                    accepted=accepted_ranges,
                    unit="range",
                    unknown_scope=True,
                    failed_batch_index=request_index,
                    outcome=MutationOutcome.UNKNOWN_OUTCOME,
                )
            applied.extend(ranges or (chunk.a1_range,))
            responses.append(result)
            accepted_ranges += 1
        return self._typed_sheet_receipt(
            operation,
            requested_ranges,
            applied,
            {"data": {"responses": responses}},
            accepted=accepted_ranges,
            unit="range",
        )

    def clear_values(self, spreadsheet_token: str, a1_range: str) -> MutationReceipt:
        a1 = A1Range.parse(a1_range)
        chunker = self._range_chunker()
        return self._submit_batch_chunks(
            spreadsheet_token,
            chunker.empty(a1),
            requested_ranges=chunker.chunk_count(a1),
            operation="clear",
        )

    def query_sheets(self, spreadsheet_token: str) -> Tuple[SheetMetadata, ...]:
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{token}/sheets/query"
        response = self.api_client.call_api(
            "GET", url, headers=self.auth.get_auth_headers()
        )
        result = FeishuResponseParser.parse(response)
        data = result.get("data")
        if not isinstance(data, Mapping):
            raise FeishuAPIError(
                -1,
                "sheets/query response missing data object",
                kind="invalid_response",
                response_data=result,
            )
        sheets = data.get("sheets")
        if not isinstance(sheets, list):
            raise FeishuAPIError(
                -1,
                "sheets/query response missing data.sheets",
                kind="invalid_response",
                response_data=result,
            )
        metadata: List[SheetMetadata] = []
        for item in sheets:
            if (
                not isinstance(item, Mapping)
                or not isinstance(item.get("sheet_id"), str)
                or not item["sheet_id"]
            ):
                raise FeishuAPIError(
                    -1,
                    "sheets/query item missing sheet_id",
                    kind="invalid_response",
                    response_data=result,
                )
            grid = item.get("grid_properties")
            title = item.get("title")
            hidden = item.get("hidden")
            if title is not None and not isinstance(title, str):
                raise FeishuAPIError(
                    -1,
                    "sheets/query title must be a string",
                    kind="invalid_response",
                    response_data=result,
                )
            if hidden is not None and not isinstance(hidden, bool):
                raise FeishuAPIError(
                    -1,
                    "sheets/query hidden must be boolean",
                    kind="invalid_response",
                    response_data=result,
                )
            if grid is not None and not isinstance(grid, Mapping):
                raise FeishuAPIError(
                    -1,
                    "sheets/query grid_properties must be an object",
                    kind="invalid_response",
                    response_data=result,
                )
            metadata.append(
                SheetMetadata(
                    item["sheet_id"],
                    title,
                    hidden,
                    grid or {},
                    item,
                )
            )
        return tuple(metadata)

    def verify_formulas(
        self,
        spreadsheet_token: str,
        sheet_ids: Sequence[str],
        ranges: Sequence[str],
        max_locations_per_error: int = 20,
    ) -> FormulaVerificationResult:
        if (
            not isinstance(sheet_ids, (list, tuple))
            or not sheet_ids
            or not all(isinstance(item, str) and item for item in sheet_ids)
        ):
            raise ValueError("sheet_ids must be a non-empty sequence of strings")
        if (
            not isinstance(ranges, (list, tuple))
            or not ranges
            or not all(isinstance(item, str) and item for item in ranges)
        ):
            raise ValueError("ranges must be a non-empty sequence of strings")
        if any("!" in item for item in ranges):
            raise ValueError(
                "formula verification ranges must not include sheet prefix"
            )
        if (
            not isinstance(max_locations_per_error, int)
            or isinstance(max_locations_per_error, bool)
            or max_locations_per_error <= 0
        ):
            raise ValueError("max_locations_per_error must be positive")
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheet_ai/v2/spreadsheets/{token}/tools/invoke_read"
        payload = {
            "tool_name": "verify_formula",
            "input": json.dumps(
                {
                    "excel_id": spreadsheet_token,
                    "sheet_ids": list(sheet_ids),
                    "ranges": list(ranges),
                    "max_locations_per_error": max_locations_per_error,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ),
        }
        response = self.api_client.call_api(
            "POST", url, headers=self.auth.get_auth_headers(), json=payload
        )
        result = FeishuResponseParser.parse(response)
        data = result.get("data")
        output = data.get("output") if isinstance(data, Mapping) else None
        if not isinstance(output, str):
            return FormulaVerificationResult(
                "invalid_response",
                None,
                raw=result,
                error="data.output must be JSON string",
            )
        try:
            decoded = json.loads(output)
        except (TypeError, ValueError):
            return FormulaVerificationResult(
                "invalid_response",
                None,
                raw=result,
                error="data.output is invalid JSON",
            )
        if not isinstance(decoded, Mapping):
            return FormulaVerificationResult(
                "invalid_response",
                None,
                raw=result,
                error="formula output must be object",
            )
        status = decoded.get("status")
        has_more = decoded.get("has_more")
        if status not in {"success", "errors_found", "partial"} or not isinstance(
            has_more, bool
        ):
            return FormulaVerificationResult(
                "invalid_response",
                has_more if isinstance(has_more, bool) else None,
                raw=decoded,
                error="invalid status or has_more",
            )
        total_errors = decoded.get("total_errors")
        if total_errors is not None and (
            isinstance(total_errors, bool) or not isinstance(total_errors, int)
        ):
            return FormulaVerificationResult(
                "invalid_response",
                has_more,
                raw=decoded,
                error="total_errors must be integer",
            )
        extensions = {
            key: decoded[key]
            for key in (
                "error_summary",
                "compile_errors",
                "warning_message",
                "total_formulas",
                "scanned_cells",
            )
            if key in decoded
        }
        return FormulaVerificationResult(
            status, has_more, total_errors, extensions, decoded
        )

    def _parse_boolean_response(
        self, response, operation: str
    ) -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
        """统一解析 bool 写接口，同时保留原有 False/错误码契约。"""
        try:
            return FeishuResponseParser.parse(response), 0
        except FeishuAPIError as error:
            details = f"错误码 {error.code}, 错误信息: {error.message}"
            if error.log_id:
                details += f", log_id: {error.log_id}"
            self.logger.error(f"{operation}失败: {details}")
            # 历史 tuple 契约以 None 表示响应无法解析；HTTP 状态不是飞书业务错误码。
            error_code = None if error.kind == "invalid_response" else error.code
            return None, error_code

    def _call_boolean_api(self, method: str, url: str, operation: str, **kwargs):
        """调用 bool 写接口，并把 transport failure 收敛到旧的失败 tuple。"""
        try:
            response = self.api_client.call_api(method, url, **kwargs)
        except FeishuAPIError as error:
            details = f"错误码 {error.code}, 错误信息: {error.message}"
            self.logger.error(f"{operation}失败: {details}")
            return None, None
        return self._parse_boolean_response(response, operation)

    def get_sheet_info(self, spreadsheet_token: str) -> Dict[str, Any]:
        """
        获取电子表格信息

        Args:
            spreadsheet_token: 电子表格Token

        Returns:
            电子表格信息字典

        Raises:
            Exception: 当API调用失败时
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{token}"
        headers = self.auth.get_auth_headers()

        params = {}
        if self.value_render_option:
            params["valueRenderOption"] = self.value_render_option
        if self.datetime_render_option:
            params["dateTimeRenderOption"] = self.datetime_render_option

        response = self.api_client.call_api("GET", url, headers=headers, params=params)

        result = FeishuResponseParser.parse(response)

        return result.get("data", {})

    def get_sheet_meta(self, spreadsheet_token: str, sheet_id: str) -> Dict[str, Any]:
        """
        获取工作表属性信息（sheet 级别）

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID

        Returns:
            工作表信息字典
        """
        token = encode_path_segment(spreadsheet_token)
        sheet = encode_path_segment(sheet_id)
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{token}/sheets/{sheet}"
        headers = self.auth.get_auth_headers()

        response = self.api_client.call_api("GET", url, headers=headers)

        result = FeishuResponseParser.parse(response)

        return result.get("data", {}).get("sheet", {})

    def get_sheet_grid_properties(
        self, spreadsheet_token: str, sheet_id: str
    ) -> Tuple[int, int]:
        """
        获取工作表网格属性（行数、列数）

        Returns:
            (row_count, column_count)
        """
        sheet = self.get_sheet_meta(spreadsheet_token, sheet_id)
        resource_type = sheet.get("resource_type", "sheet")
        if resource_type != "sheet":
            raise Exception(f"工作表类型异常: {resource_type}")

        grid = sheet.get("grid_properties") or {}
        row_count = int(grid.get("row_count", 0) or 0)
        col_count = int(grid.get("column_count", 0) or 0)

        if row_count <= 0 or col_count <= 0:
            raise Exception(
                f"工作表网格属性无效: row_count={row_count}, column_count={col_count}"
            )

        return row_count, col_count

    def get_sheet_data(self, spreadsheet_token: str, range_str: str) -> List[List[Any]]:
        """
        读取电子表格数据

        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串，如 "Sheet1!A1:C10"

        Returns:
            二维数组表示的表格数据

        Raises:
            Exception: 当API调用失败时
        """
        # 实际读取会由服务端校验网格范围；这里只做纯本地 A1 校验，
        # 避免每个读取块先探测再读取，产生双倍 GET。
        is_valid, error_msg = self._validate_range_format(range_str)
        if not is_valid:
            raise Exception(f"读取数据范围验证失败: {error_msg}")

        token = encode_path_segment(spreadsheet_token)
        encoded_range = encode_a1_range(range_str)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/values/{encoded_range}"
        headers = self.auth.get_auth_headers()
        params = {}
        if self.value_render_option:
            params["valueRenderOption"] = self.value_render_option
        if self.datetime_render_option:
            params["dateTimeRenderOption"] = self.datetime_render_option

        response = self.api_client.call_api("GET", url, headers=headers, params=params)

        result = FeishuResponseParser.parse(response)

        data = result.get("data", {})
        value_range = data.get("valueRange", {})
        return value_range.get("values", [])

    def identify_formula_columns(
        self, formula_data: List[List[Any]], headers: Optional[List[str]] = None
    ) -> set[Union[str, int]]:
        """
        识别包含公式的列

        Args:
            formula_data: 使用 Formula 渲染选项读取的数据
            headers: 列名列表（可选，用于返回列名而不是列索引）

        Returns:
            包含公式的列集合（列名或列索引）
        """
        formula_cols: set[Union[str, int]] = set()

        if not formula_data:
            return formula_cols

        # 遍历所有列
        num_cols = max(len(row) for row in formula_data) if formula_data else 0

        for col_idx in range(num_cols):
            has_formula = False
            # 检查该列是否有单元格以 = 开头（公式标识）
            for row in formula_data:
                if col_idx < len(row):
                    cell_value = str(row[col_idx]) if row[col_idx] is not None else ""
                    if cell_value.startswith("="):
                        has_formula = True
                        break

            if has_formula:
                if headers and col_idx < len(headers):
                    formula_cols.add(headers[col_idx])
                else:
                    formula_cols.add(col_idx)

        return formula_cols

    def get_sheet_data_chunked(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        start_row: int,
        end_row: int,
        start_col: str,
        end_col: str,
        max_rows_per_request: Optional[int] = None,
        max_cols_per_request: Optional[int] = None,
    ) -> List[List[Any]]:
        """
        分块读取工作表数据，避免单次请求过大

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            start_row: 起始行（1-based）
            end_row: 结束行（1-based）
            start_col: 起始列（字母）
            end_col: 结束列（字母）
            max_rows_per_request: 单次请求最大行数
            max_cols_per_request: 单次请求最大列数

        Returns:
            二维数组表示的表格数据
        """
        max_rows = max_rows_per_request or self.scan_max_rows
        max_cols = max_cols_per_request or self.scan_max_cols

        start_col_num = self.column_letter_to_number(start_col)
        end_col_num = self.column_letter_to_number(end_col)

        if start_row > end_row or start_col_num > end_col_num:
            return []

        total_cols = end_col_num - start_col_num + 1
        total_rows = end_row - start_row + 1

        self.logger.info(
            f"📖 分块读取: 总范围 {sheet_id}!{start_col}{start_row}:{end_col}{end_row} "
            f"(总计 {total_rows} 行 × {total_cols} 列), 单次上限 {max_rows} 行 × {max_cols} 列"
        )

        def _is_too_large_response(err: Exception) -> bool:
            if isinstance(err, FeishuAPIError) and err.code in (90221, 90227):
                return True
            msg = str(err)
            return (
                "90221" in msg
                or "90227" in msg
                or "TooLargeResponse" in msg
                or "TooLargeRequest" in msg
                or "data exceeded" in msg
            )

        def _pad_rows(values: List[List[Any]], expected_rows: int) -> List[List[Any]]:
            if len(values) < expected_rows:
                values.extend([[] for _ in range(expected_rows - len(values))])
            return values

        def _read_range(range_str: str) -> List[List[Any]]:
            return self.get_sheet_data(spreadsheet_token, range_str)

        def _build_col_ranges(
            col_start_num: int, col_end_num: int, col_step: int
        ) -> List[Tuple[int, int]]:
            ranges = []
            for c_start in range(col_start_num, col_end_num + 1, col_step):
                c_end = min(c_start + col_step - 1, col_end_num)
                ranges.append((c_start, c_end))
            return ranges

        def _read_row_block(
            row_start: int,
            row_end: int,
            col_ranges: List[Tuple[int, int]],
        ) -> List[List[Any]]:
            chunk_rows = row_end - row_start + 1
            if len(col_ranges) == 1 and col_ranges[0] == (
                start_col_num,
                end_col_num,
            ):
                range_str = f"{sheet_id}!{start_col}{row_start}:{end_col}{row_end}"
                chunk_values = _read_range(range_str)
                return _pad_rows(chunk_values, chunk_rows)

            # 列分块读取后拼接
            row_chunk_values = [[None] * total_cols for _ in range(chunk_rows)]
            for col_start_num, col_end_num in col_ranges:
                col_offset = col_start_num - start_col_num
                col_start_letter = self.column_number_to_letter(col_start_num)
                col_end_letter = self.column_number_to_letter(col_end_num)
                range_str = (
                    f"{sheet_id}!"
                    f"{col_start_letter}{row_start}:{col_end_letter}{row_end}"
                )
                chunk_values = _read_range(range_str)
                for r_idx, row in enumerate(chunk_values):
                    if r_idx >= chunk_rows:
                        break
                    for c_idx, cell in enumerate(row):
                        target_col = col_offset + c_idx
                        if target_col < total_cols:
                            row_chunk_values[r_idx][target_col] = cell
            return row_chunk_values

        def _read_single_row_adaptive_cols(
            row_idx: int, col_start_num: int, col_end_num: int
        ) -> List[Any]:
            col_span = col_end_num - col_start_num + 1
            col_start_letter = self.column_number_to_letter(col_start_num)
            col_end_letter = self.column_number_to_letter(col_end_num)
            range_str = (
                f"{sheet_id}!" f"{col_start_letter}{row_idx}:{col_end_letter}{row_idx}"
            )
            try:
                values = _read_range(range_str)
                row = values[0] if values else []
                if len(row) < col_span:
                    row.extend([None] * (col_span - len(row)))
                return row
            except Exception as e:
                if _is_too_large_response(e) and col_span > 1:
                    mid = col_start_num + (col_span // 2) - 1
                    left = _read_single_row_adaptive_cols(row_idx, col_start_num, mid)
                    right = _read_single_row_adaptive_cols(
                        row_idx, mid + 1, col_end_num
                    )
                    return left + right
                raise

        values_all: List[List[Any]] = []
        col_ranges = _build_col_ranges(start_col_num, end_col_num, max_cols)

        row_start = start_row
        while row_start <= end_row:
            row_chunk_size = min(max_rows, end_row - row_start + 1)
            while True:
                row_end = row_start + row_chunk_size - 1
                try:
                    rows_values = _read_row_block(row_start, row_end, col_ranges)
                    values_all.extend(rows_values)
                    break
                except Exception as e:
                    if _is_too_large_response(e):
                        if row_chunk_size > 1:
                            new_size = max(1, row_chunk_size // 2)
                            self.logger.warning(
                                f"读取范围过大(90221)，行数减半重试: "
                                f"{row_chunk_size} -> {new_size} 行"
                            )
                            row_chunk_size = new_size
                            continue
                        # 单行仍过大，按列二分读取
                        self.logger.warning(
                            f"单行读取仍过大，启用列二分读取: 第 {row_start} 行"
                        )
                        row_values = _read_single_row_adaptive_cols(
                            row_start, start_col_num, end_col_num
                        )
                        if len(row_values) < total_cols:
                            row_values.extend([None] * (total_cols - len(row_values)))
                        values_all.append(row_values)
                        break
                    raise

            row_start = row_end + 1

        return values_all

    def write_sheet_data(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        values: List[List[Any]],
        row_batch_size: int = 500,
        col_batch_size: int = 80,
        rate_limit_delay: float = 0.05,
    ) -> bool:
        """
        写入电子表格数据，具备“自动二分重试”能力。

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            values: 要写入的数据（包含表头）
            row_batch_size: 初始行批次大小
            col_batch_size: 列批次大小
            rate_limit_delay: 接口调用间隔

        Returns:
            是否写入成功
        """
        if not values:
            self.logger.warning("写入数据为空")
            return True

        self.logger.info("🔄 执行写入操作 (具备自动二分重试能力)")

        data_chunks = self._create_data_chunks(values, row_batch_size, col_batch_size)
        total_chunks = len(data_chunks)

        self.logger.info(f"📦 初始数据分块完成: 共 {total_chunks} 个数据块")

        for i, chunk in enumerate(data_chunks, 1):
            self.logger.info(f"--- 开始处理初始数据块 {i}/{total_chunks} ---")
            if not self._upload_chunk_with_auto_split(
                spreadsheet_token, sheet_id, chunk, rate_limit_delay
            ):
                self.logger.error(
                    f"❌ 初始数据块 {i}/{total_chunks} (行 {chunk['start_row']}-{chunk['end_row']}) 最终上传失败"
                )
                return False
            self.logger.info(f"--- ✅ 成功处理初始数据块 {i}/{total_chunks} ---")

        self.logger.info(f"🎉 写入操作全部完成: 成功处理 {total_chunks} 个初始数据块")
        return True

    def _write_single_batch(
        self, spreadsheet_token: str, range_str: str, values: List[List[Any]]
    ) -> Tuple[bool, Optional[int]]:
        """
        写入单个批次数据。

        Returns:
            元组 (是否成功, 错误码)
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/values"
        headers = self.auth.get_auth_headers()

        data = {"valueRange": {"range": range_str, "values": values}}

        result, error_code = self._call_boolean_api(
            "PUT", url, "写入电子表格数据", headers=headers, json=data
        )
        if result is None:
            return False, error_code

        self.logger.debug(f"成功写入 {len(values)} 行数据")
        return True, 0

    def column_number_to_letter(self, col_num: int) -> str:
        """将列号转换为字母（1->A, 2->B, ..., 26->Z, 27->AA）"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result or "A"

    def _build_range_string(
        self, sheet_id: str, start_row: int, start_col: int, end_row: int, end_col: int
    ) -> str:
        """构建范围字符串"""
        start_col_letter = self.column_number_to_letter(start_col)
        end_col_letter = self.column_number_to_letter(end_col)
        return f"{sheet_id}!{start_col_letter}{start_row}:{end_col_letter}{end_row}"

    def append_sheet_data(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        values: List[List[Any]],
        row_batch_size: int = 500,
        rate_limit_delay: float = 0.05,
    ) -> bool:
        """
        追加电子表格数据，同样具备“自动二分重试”能力。
        注意：追加操作不支持按列分块，它总是追加到表格的末尾。

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            values: 要追加的数据
            row_batch_size: 初始行批次大小
            rate_limit_delay: 接口调用间隔

        Returns:
            是否追加成功
        """
        if not values:
            self.logger.warning("追加数据为空")
            return True

        self.logger.info("➕ 执行追加操作 (具备自动二分重试能力)")

        # 对于追加操作，我们只按行分块
        data_chunks = self._create_data_chunks(
            values, row_batch_size, len(values[0]) if values else 0
        )
        total_chunks = len(data_chunks)

        self.logger.info(f"📦 初始数据分块完成: 共 {total_chunks} 个数据块")

        for i, chunk in enumerate(data_chunks, 1):
            self.logger.info(f"--- 开始处理初始追加块 {i}/{total_chunks} ---")
            # 注意：追加操作的range只需要指定工作表ID
            append_range = f"{sheet_id}"
            if not self._append_chunk_with_auto_split(
                spreadsheet_token, append_range, chunk["data"], rate_limit_delay
            ):
                self.logger.error(f"❌ 初始追加块 {i}/{total_chunks} 最终上传失败")
                return False
            self.logger.info(f"--- ✅ 成功处理初始追加块 {i}/{total_chunks} ---")

        self.logger.info(f"🎉 追加操作全部完成: 成功处理 {total_chunks} 个初始数据块")
        return True

    def _append_single_batch(
        self, spreadsheet_token: str, range_str: str, values: List[List[Any]]
    ) -> Tuple[bool, Optional[int]]:
        """
        追加单个批次数据。

        Returns:
            元组 (是否成功, 错误码)
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/values_append"
        headers = self.auth.get_auth_headers()

        data = {"valueRange": {"range": range_str, "values": values}}

        result, error_code = self._call_boolean_api(
            "POST", url, "追加电子表格数据", headers=headers, json=data
        )
        if result is None:
            return False, error_code

        self.logger.debug(f"成功追加 {len(values)} 行数据")
        return True, 0

    def write_selective_columns(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        column_data: Dict[str, List[Any]],
        column_positions: Dict[str, int],
        start_row: int = 1,
        rate_limit_delay: float = 0.05,
        max_gap: int = 2,
    ) -> bool:
        """
        写入选择性列数据，支持不连续列的高效批量操作

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            column_data: 字典，键为列名，值为该列的数据列表
            column_positions: 字典，键为列名，值为列位置（1-based）
            start_row: 开始行号（1-based）
            rate_limit_delay: 接口调用间隔
            max_gap: 最大允许合并的间隔列数

        Returns:
            是否写入成功
        """
        if not column_data:
            self.logger.warning("选择性写入数据为空")
            return True

        self.logger.info(f"🎯 执行选择性列写入: {list(column_data.keys())}")

        # 优化相邻列为连续范围
        ranges_data = self._optimize_column_ranges(
            column_data, column_positions, start_row, max_gap
        )

        # 构建多范围数据
        value_ranges = []
        for range_info in ranges_data:
            range_str = f"{sheet_id}!{range_info['range']}"
            is_valid, error_msg = self._validate_range_format(range_str)
            if not is_valid:
                self.logger.error(f"选择性写入范围无效: {error_msg}")
                return False
            value_ranges.append({"range": range_str, "values": range_info["values"]})

        # 使用批量更新API
        if value_ranges:
            time.sleep(rate_limit_delay)
            success, _ = self._batch_update_ranges(spreadsheet_token, value_ranges)
            if success:
                self.logger.info(f"✅ 选择性列写入成功: {len(value_ranges)} 个范围")
            else:
                self.logger.error("❌ 选择性列写入失败")
            return success

        return True

    def _optimize_column_ranges(
        self,
        column_data: Dict[str, List[Any]],
        column_positions: Dict[str, int],
        start_row: int,
        max_gap: int = 2,
    ) -> List[Dict]:
        """
        优化列范围，将相邻列合并为连续范围以提高API效率

        Args:
            column_data: 列数据
            column_positions: 列位置映射
            start_row: 开始行号
            max_gap: 最大允许合并的间隔列数

        Returns:
            优化后的范围数据列表
        """
        # 按列位置排序
        sorted_columns = sorted(
            column_data.keys(), key=lambda x: column_positions.get(x, 0)
        )

        ranges_data = []
        i = 0

        while i < len(sorted_columns):
            range_start = i
            range_end = i

            # 只合并真正相邻的目标列。跨非目标列合并会要求为中间列提供值，
            # 使用空值占位会清空未选择列，因此这里不做跨列合并。
            while range_end + 1 < len(sorted_columns):
                current_pos = column_positions[sorted_columns[range_end]]
                next_pos = column_positions[sorted_columns[range_end + 1]]

                if max_gap > 0 and next_pos - current_pos == 1:
                    range_end += 1
                else:
                    break

            # 构建范围数据
            start_col = column_positions[sorted_columns[range_start]]
            end_col = column_positions[sorted_columns[range_end]]

            start_col_letter = self.column_number_to_letter(start_col)
            end_col_letter = self.column_number_to_letter(end_col)

            # 计算数据行数
            max_rows = max(
                len(column_data[col])
                for col in sorted_columns[range_start : range_end + 1]
            )
            end_row = start_row + max_rows - 1

            range_str = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"

            # 构建该范围的数据矩阵
            range_values = []
            for row_idx in range(max_rows):
                row_data = []
                for col_idx in range(start_col, end_col + 1):
                    # 查找对应的列名
                    col_name = None
                    for name, pos in column_positions.items():
                        if pos == col_idx:
                            col_name = name
                            break

                    if col_name and col_name in column_data:
                        # 有数据的列
                        if row_idx < len(column_data[col_name]):
                            row_data.append(column_data[col_name][row_idx])
                        else:
                            row_data.append("")
                    else:
                        # 连续范围内不应出现非目标列；防御性失败，避免清空数据。
                        raise ValueError(f"选择性范围包含未提供数据的列位置: {col_idx}")

                range_values.append(row_data)

            ranges_data.append({"range": range_str, "values": range_values})

            i = range_end + 1

        return ranges_data

    def clear_sheet_data(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        range_str: str,
        max_rows_per_batch: Optional[int] = None,
        max_cols_per_batch: Optional[int] = None,
    ) -> bool:
        """
        清空电子表格指定范围的数据

        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            range_str: 范围字符串，如 "A1:Z1000"
            max_rows_per_batch: 单次清空最大行数
            max_cols_per_batch: 单次清空最大列数

        Returns:
            是否清空成功
        """
        # 构建完整范围字符串用于验证
        full_range = f"{sheet_id}!{range_str}"

        # 验证范围有效性
        is_valid, error_msg = self._validate_range(spreadsheet_token, full_range)
        if not is_valid:
            self.logger.error(f"清空数据范围验证失败: {error_msg}")
            return False

        max_rows = max_rows_per_batch or self.write_max_rows
        max_cols = max_cols_per_batch or self.write_max_cols

        self.logger.info(
            f"准备清空范围: {full_range} (单次上限 {max_rows} 行 × {max_cols} 列)"
        )

        def _build_empty_values_for_range(
            range_to_clear: str,
        ) -> Optional[List[List[str]]]:
            import re

            match = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_to_clear)
            if not match:
                return None
            _, start_col, start_row, end_col, end_row = match.groups()
            start_row_i, end_row_i = int(start_row), int(end_row)
            start_col_num = self.column_letter_to_number(start_col)
            end_col_num = self.column_letter_to_number(end_col)

            rows = end_row_i - start_row_i + 1
            cols = end_col_num - start_col_num + 1
            if rows <= 0 or cols <= 0:
                return None

            # 清空通过写入空字符串实现，避免 values 为空导致的 90226 错误
            empty_row = [""] * cols
            return [empty_row] * rows

        def _split_range_half(range_to_split: str) -> Optional[List[str]]:
            import re

            match = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_to_split)
            if not match:
                return None
            sheet_id, start_col, start_row, end_col, end_row = match.groups()
            start_row_i, end_row_i = int(start_row), int(end_row)
            start_col_num = self.column_letter_to_number(start_col)
            end_col_num = self.column_letter_to_number(end_col)

            if start_row_i < end_row_i:
                mid = (start_row_i + end_row_i) // 2
                return [
                    f"{sheet_id}!{start_col}{start_row_i}:{end_col}{mid}",
                    f"{sheet_id}!{start_col}{mid + 1}:{end_col}{end_row_i}",
                ]

            if start_col_num < end_col_num:
                mid_col = (start_col_num + end_col_num) // 2
                left_col = self.column_number_to_letter(mid_col)
                right_col = self.column_number_to_letter(mid_col + 1)
                return [
                    f"{sheet_id}!{start_col}{start_row_i}:{left_col}{end_row_i}",
                    f"{sheet_id}!{right_col}{start_row_i}:{end_col}{end_row_i}",
                ]

            return None

        # 先按行列上限分块，避免单次范围过大
        chunks = self._split_range_into_chunks(full_range, max_rows, max_cols)
        total_chunks = len(chunks)
        self.logger.info(f"📋 清空范围分块: {total_chunks} 个块")

        for i, chunk_ranges in enumerate(chunks, 1):
            # 每个 chunk_ranges 里目前只有一个范围
            for chunk_range in chunk_ranges:
                stack = [chunk_range]
                while stack:
                    current_range = stack.pop()
                    empty_values = _build_empty_values_for_range(current_range)
                    if empty_values is None:
                        self.logger.error(f"无法构建清空数据矩阵: {current_range}")
                        return False
                    value_ranges = [{"range": current_range, "values": empty_values}]
                    success, error_code = self._batch_update_ranges(
                        spreadsheet_token, value_ranges, is_clear=True
                    )
                    if success:
                        self.logger.info(
                            f"✅ 清空成功: {current_range} (块 {i}/{total_chunks})"
                        )
                        continue

                    # 如果请求过大，按行优先二分拆分重试
                    if error_code == self.ERROR_CODE_REQUEST_TOO_LARGE:
                        self.logger.warning(
                            f"清空范围过大(90227)，启用二分拆分: {current_range}"
                        )
                        sub_ranges = _split_range_half(current_range)
                        if not sub_ranges:
                            self.logger.error(
                                f"无法拆分范围，清空失败: {current_range}"
                            )
                            return False
                        # LIFO，先处理左半边
                        stack.extend(reversed(sub_ranges))
                        continue

                    # 其他错误直接失败
                    self.logger.error(
                        f"❌ 范围 {current_range} 清空失败 (错误码 {error_code})"
                    )
                    return False

        return True

    def set_dropdown_validation(
        self,
        spreadsheet_token: str,
        range_str: str,
        options: List[str],
        multiple_values: bool = False,
        colors: Optional[List[str]] = None,
        max_rows_per_batch: int = 4000,
    ) -> bool:
        """
        分块设置电子表格下拉列表数据校验

        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串，如 "Sheet1!A1:A100000" (自动分块)
            options: 下拉列表选项值列表
            multiple_values: 是否支持多选，默认False
            colors: 选项颜色列表，需要与options一一对应
            max_rows_per_batch: 每批次最大行数，保持在API限制内

        Returns:
            是否设置成功
        """
        if not options:
            self.logger.warning("下拉列表选项为空，跳过设置")
            return True

        # 验证范围有效性
        is_valid, error_msg = self._validate_range(spreadsheet_token, range_str)
        if not is_valid:
            self.logger.error(f"下拉列表设置范围验证失败: {error_msg}")
            return False

        # 验证选项数量
        if len(options) > 500:
            self.logger.warning(f"下拉列表选项过多({len(options)})，将截取前500个")
            options = options[:500]

        # 验证选项值
        valid_options = []
        for option in options:
            option_str = str(option)
            if "," in option_str:
                self.logger.warning(f"选项值包含逗号，将被跳过: {option_str}")
                continue
            if len(option_str.encode("utf-8")) > 100:
                self.logger.warning(f"选项值过长，将被截取: {option_str[:20]}...")
                option_str = option_str[:50]  # 保守截取
            valid_options.append(option_str)

        if not valid_options:
            self.logger.warning("没有有效的下拉列表选项")
            return False

        # 处理颜色配置
        if colors and len(colors) != len(valid_options):
            self.logger.warning(
                f"颜色数量({len(colors)})与选项数量({len(valid_options)})不匹配，将自动补齐"
            )
            default_colors = [
                "#1FB6C1",
                "#F006C2",
                "#FB16C3",
                "#FFB6C1",
                "#32CD32",
                "#FF6347",
            ]
            colors = [
                (
                    colors[i % len(colors)]
                    if i < len(colors)
                    else default_colors[i % len(default_colors)]
                )
                for i in range(len(valid_options))
            ]

        # 分块处理下拉列表设置
        self.logger.info(f"📝 开始分块设置下拉列表，批次大小: {max_rows_per_batch} 行")

        # 将大范围分解为小块
        range_chunks = self._split_range_into_chunks(range_str, max_rows_per_batch, 1)
        success_count = 0

        self.logger.info(f"📋 范围 {range_str} 分解为 {len(range_chunks)} 个块")

        for i, chunk in enumerate(range_chunks, 1):
            chunk_range = chunk[0]  # 每个chunk包含一个range列表

            self.logger.info(
                f"🔄 设置下拉列表批次 {i}/{len(range_chunks)}: {chunk_range}"
            )

            if self._set_dropdown_single_batch(
                spreadsheet_token, chunk_range, valid_options, multiple_values, colors
            ):
                success_count += 1
                self.logger.info(f"✅ 下拉列表批次 {i} 设置成功")
            else:
                self.logger.error(f"❌ 下拉列表批次 {i} 设置失败")
                return False

            # 接口频率控制
            time.sleep(0.1)

        self.logger.info(
            f"🎉 下拉列表设置完成: 成功 {success_count}/{len(range_chunks)} 个批次"
        )
        return success_count == len(range_chunks)

    def _set_dropdown_single_batch(
        self,
        spreadsheet_token: str,
        range_str: str,
        options: List[str],
        multiple_values: bool,
        colors: Optional[List[str]],
    ) -> bool:
        """
        设置单个批次的下拉列表
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/dataValidation"
        headers = self.auth.get_auth_headers()

        # 构建请求数据
        options_payload: Dict[str, Any] = {
            "multipleValues": multiple_values,
            "highlightValidData": bool(colors),
        }
        if colors:
            options_payload["colors"] = colors

        data_validation: Dict[str, Any] = {
            "conditionValues": options,
            "options": options_payload,
        }

        request_data = {
            "range": range_str,
            "dataValidationType": "list",
            "dataValidation": data_validation,
        }

        result, _ = self._call_boolean_api(
            "POST", url, "设置下拉列表", headers=headers, json=request_data
        )
        return result is not None

    def _validate_range(
        self, spreadsheet_token: str, range_str: str
    ) -> Tuple[bool, str]:
        """
        完整的范围有效性验证

        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串，如 "Sheet1!A1:A10"

        Returns:
            (是否有效, 错误信息)
        """
        is_valid, error_msg = self._validate_range_format(range_str)
        if not is_valid:
            return is_valid, error_msg

        # 写入、清空、样式等路径保留服务端网格探测。
        if not self._validate_range_size(spreadsheet_token, range_str):
            return False, f"范围超出电子表格网格限制: {range_str}"
        return True, ""

    def _validate_range_format(self, range_str: str) -> Tuple[bool, str]:
        """纯本地验证完整 A1 范围，不发起网络请求。"""
        import re

        if not re.match(r"^[^!]+![A-Z]+\d+:[A-Z]+\d+$", range_str):
            return False, f"范围格式无效: {range_str}，期望格式如 'Sheet1!A1:C10'"

        # 2. 解析范围组件
        try:
            match = re.match(r"^([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)$", range_str)
            if not match:
                return False, f"无法解析范围: {range_str}"

            sheet_id, start_col, start_row, end_col, end_row = match.groups()
            start_row, end_row = int(start_row), int(end_row)

            # 3. 边界检查
            MAX_ROWS = 1048576  # Excel/电子表格通用限制
            MAX_COLS = 16384  # Excel/电子表格通用限制

            if start_row < 1 or end_row < 1:
                return False, f"行号不能小于1: {start_row}-{end_row}"

            if start_row > MAX_ROWS or end_row > MAX_ROWS:
                return False, f"行号超过限制({MAX_ROWS}): {start_row}-{end_row}"

            start_col_num = self.column_letter_to_number(start_col)
            end_col_num = self.column_letter_to_number(end_col)

            if start_col_num > MAX_COLS or end_col_num > MAX_COLS:
                return False, f"列号超过限制({MAX_COLS}): {start_col}-{end_col}"

            # 4. 范围逻辑验证
            if start_row > end_row:
                return False, f"起始行({start_row})不能大于结束行({end_row})"

            if start_col_num > end_col_num:
                return False, f"起始列({start_col})不能大于结束列({end_col})"

            return True, ""

        except Exception as e:
            return False, f"范围验证异常: {e}"

    def _validate_range_size(self, spreadsheet_token: str, range_str: str) -> bool:
        """
        验证范围是否在表格网格限制内

        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串，如 "Sheet1!A1:A10"

        Returns:
            是否在网格限制内
        """
        try:
            # 尝试获取指定范围的数据来测试是否超出网格限制
            # 这是一个轻量级的测试，不会实际获取大量数据
            test_response = self.api_client.call_api(
                "GET",
                f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{encode_path_segment(spreadsheet_token)}/values/{encode_a1_range(range_str)}",
                headers=self.auth.get_auth_headers(),
            )

            FeishuResponseParser.parse(test_response)
            return True

        except FeishuAPIError as error:
            if error.code == 90202:
                self.logger.debug(f"范围 {range_str} 超出网格限制")
            else:
                self.logger.debug(f"范围验证失败: {error}")
            # 验证失败时保守返回False，避免后续API调用失败
            return False

    def set_cell_style(
        self,
        spreadsheet_token: str,
        ranges: List[str],
        style: Dict[str, Any],
        max_rows_per_batch: int = 4000,
        max_cols_per_batch: int = 80,
        adaptive_batch: bool = True,
    ) -> bool:
        """
        分块批量设置单元格样式，支持自适应批次优化

        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表，如 ["Sheet1!A1:A100000"] (自动分块)
            style: 样式配置字典
            max_rows_per_batch: 每批次最大行数，保持在API限制内
            max_cols_per_batch: 每批次最大列数，保持在API限制内
            adaptive_batch: 是否启用自适应批次优化（针对少列场景）

        Returns:
            是否设置成功
        """
        if not ranges:
            self.logger.warning("样式设置范围为空，跳过设置")
            return True

        # 针对列批量设置优化：5000行×1列为最优批次
        if adaptive_batch:
            # 格式设置API的最优策略：垂直批量，每次5000行×1列
            max_rows_per_batch = 5000
            max_cols_per_batch = 1  # 强制单列处理
            self.logger.info(
                f"🚀 启用格式设置专用优化: 垂直批量 {max_rows_per_batch}行×{max_cols_per_batch}列"
            )

        self.logger.info(
            f"🎨 开始分块设置单元格样式，批次大小: {max_rows_per_batch}行 × {max_cols_per_batch}列"
        )

        success_batches = 0
        total_batches = 0

        for range_str in ranges:
            # 解析范围
            chunks = self._split_range_into_chunks(
                range_str, max_rows_per_batch, max_cols_per_batch
            )
            total_batches += len(chunks)

            self.logger.info(f"📋 范围 {range_str} 分解为 {len(chunks)} 个块")

            # 分批处理每个块
            for i, chunk_ranges in enumerate(chunks, 1):
                # 解析范围信息用于详细日志
                range_details = []
                for chunk_range in chunk_ranges:
                    range_details.append(self._parse_range_for_log(chunk_range))

                # 显示详细的处理信息
                if len(range_details) == 1:
                    detail = range_details[0]
                    style_type = self._get_style_type_description(style)
                    self.logger.info(
                        f"🔄 设置{detail['col_name']}列的{detail['start_row']}-{detail['end_row']}行为{style_type} (批次 {i}/{len(chunks)})"
                    )
                else:
                    self.logger.info(
                        f"🔄 处理样式批次 {i}/{len(chunks)}: {len(chunk_ranges)} 个范围"
                    )

                if self._set_style_single_batch(spreadsheet_token, chunk_ranges, style):
                    success_batches += 1
                    if len(range_details) == 1:
                        detail = range_details[0]
                        style_type = self._get_style_type_description(style)
                        range_info = f"{detail['col_name']}{detail['start_row']}:{detail['col_name']}{detail['end_row']}"
                        if isinstance(detail["start_row"], int) and isinstance(
                            detail["end_row"], int
                        ):
                            row_count: Union[int, str] = (
                                detail["end_row"] - detail["start_row"] + 1
                            )
                        else:
                            row_count = "未知"
                        self.logger.info(
                            f"✅ {detail['col_name']}列样式设置成功: 范围 {range_info}, 格式 {style_type}, 共 {row_count} 行"
                        )
                    else:
                        total_ranges = len(chunk_ranges)
                        style_type = self._get_style_type_description(style)
                        self.logger.info(
                            f"✅ 样式批次 {i} 设置成功: {total_ranges} 个范围, 格式 {style_type}"
                        )
                else:
                    self.logger.error(f"❌ 样式批次 {i} 设置失败")
                    return False

                # 接口频率控制
                time.sleep(0.1)

        self.logger.info(
            f"🎉 样式设置完成: 成功 {success_batches}/{total_batches} 个批次"
        )
        return success_batches == total_batches

    def _parse_range_for_log(self, range_str: str) -> Dict[str, Any]:
        """解析范围字符串用于日志显示"""
        import re

        match = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_str)
        if match:
            sheet_id, start_col, start_row, end_col, end_row = match.groups()
            return {
                "sheet_id": sheet_id,
                "col_name": (
                    start_col if start_col == end_col else f"{start_col}-{end_col}"
                ),
                "start_row": int(start_row),
                "end_row": int(end_row),
            }
        return {"col_name": "未知", "start_row": "?", "end_row": "?"}

    def _parse_range_for_detailed_log(self, range_str: str) -> Dict[str, Any]:
        """解析范围字符串用于详细日志显示"""
        import re

        match = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_str)
        if match:
            sheet_id, start_col, start_row, end_col, end_row = match.groups()
            return {
                "sheet_id": sheet_id,
                "start_col": start_col,
                "end_col": end_col,
                "start_row": int(start_row),
                "end_row": int(end_row),
            }
        return {
            "sheet_id": "未知",
            "start_col": "?",
            "end_col": "?",
            "start_row": 0,
            "end_row": 0,
        }

    def _get_style_type_description(self, style: Dict[str, Any]) -> str:
        """获取样式类型的中文描述"""
        if "formatter" in style:
            formatter = style["formatter"]
            if (
                "yyyy" in formatter.lower()
                or "mm" in formatter.lower()
                or "dd" in formatter.lower()
            ):
                return "日期格式"
            elif "#" in formatter or "0" in formatter:
                return "数字格式"
            else:
                return f"自定义格式({formatter})"
        elif "fore_color" in style or "background_color" in style:
            return "颜色样式"
        elif "bold" in style or "italic" in style:
            return "字体样式"
        else:
            return "样式"

    def _split_range_into_chunks(
        self, range_str: str, max_rows: int, max_cols: int
    ) -> List[List[str]]:
        """
        将大范围分解为符合API限制的小块

        Args:
            range_str: 原始范围，如 "Sheet1!A1:AK94277"
            max_rows: 最大行数
            max_cols: 最大列数

        Returns:
            分块后的范围列表的列表
        """
        import re

        # 解析范围字符串
        match = re.match(r"([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)", range_str)
        if not match:
            self.logger.warning(f"无法解析范围字符串: {range_str}")
            return [[range_str]]  # 返回原始范围

        sheet_id, start_col, start_row, end_col, end_row = match.groups()
        start_row, end_row = int(start_row), int(end_row)

        # 转换列字母为数字
        start_col_num = self.column_letter_to_number(start_col)
        end_col_num = self.column_letter_to_number(end_col)

        chunks = []

        # 按列分块
        for col_start in range(start_col_num, end_col_num + 1, max_cols):
            col_end = min(col_start + max_cols - 1, end_col_num)

            # 按行分块
            for row_start in range(start_row, end_row + 1, max_rows):
                row_end = min(row_start + max_rows - 1, end_row)

                # 构建块范围
                chunk_start_col = self.column_number_to_letter(col_start)
                chunk_end_col = self.column_number_to_letter(col_end)
                chunk_range = (
                    f"{sheet_id}!{chunk_start_col}{row_start}:{chunk_end_col}{row_end}"
                )

                chunks.append([chunk_range])

        return chunks

    def column_letter_to_number(self, col_letter: str) -> int:
        """将列字母转换为数字（A->1, B->2, ..., AA->27）"""
        result = 0
        # 转换为大写以处理小写字母
        for char in col_letter.upper():
            result = result * 26 + (ord(char) - ord("A") + 1)
        return result

    def _set_style_single_batch(
        self, spreadsheet_token: str, ranges: List[str], style: Dict[str, Any]
    ) -> bool:
        """
        设置单个批次的样式
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/styles_batch_update"
        headers = self.auth.get_auth_headers()

        # 构建请求数据
        request_data = {"data": [{"ranges": ranges, "style": style}]}

        result, _ = self._call_boolean_api(
            "PUT", url, "设置单元格样式", headers=headers, json=request_data
        )
        return result is not None

    def set_date_format(
        self, spreadsheet_token: str, ranges: List[str], date_format: str = "yyyy/MM/dd"
    ) -> bool:
        """
        为指定范围设置日期格式

        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表
            date_format: 日期格式，默认为 "yyyy/MM/dd"

        Returns:
            是否设置成功
        """
        style = {"formatter": date_format}

        return self.set_cell_style(spreadsheet_token, ranges, style)

    def _create_data_chunks(
        self, values: List[List[Any]], row_batch_size: int, col_batch_size: int
    ) -> List[Dict]:
        """
        创建数据分块

        Returns:
            包含分块信息的字典列表，每个字典包含：
            - data: 数据块
            - start_row, end_row: 行范围
            - start_col, end_col: 列范围
        """
        chunks = []
        total_rows = len(values)
        total_cols = len(values[0]) if values else 0

        # 按列分块（外层循环）
        for col_start in range(0, total_cols, col_batch_size):
            col_end = min(col_start + col_batch_size, total_cols)

            # 按行分块（内层循环）
            for row_start in range(0, total_rows, row_batch_size):
                row_end = min(row_start + row_batch_size, total_rows)

                # 提取数据块
                chunk_data = []
                for row_idx in range(row_start, row_end):
                    if row_idx < len(values):
                        chunk_row = values[row_idx][col_start:col_end]
                        # 确保行长度与列块大小一致
                        while len(chunk_row) < (col_end - col_start):
                            chunk_row.append("")
                        chunk_data.append(chunk_row)

                if chunk_data:  # 只添加非空块
                    # 应用配置的起始行和列偏移量
                    actual_start_row = row_start + self.start_row
                    actual_end_row = actual_start_row + len(chunk_data) - 1
                    actual_start_col = col_start + self.start_col_num
                    actual_end_col = actual_start_col + (col_end - col_start) - 1

                    chunks.append(
                        {
                            "data": chunk_data,
                            "start_row": actual_start_row,
                            "end_row": actual_end_row,
                            "start_col": actual_start_col,
                            "end_col": actual_end_col,
                        }
                    )

        return chunks

    def _upload_chunk_with_auto_split(
        self,
        spreadsheet_token: str,
        sheet_id: str,
        chunk: Dict,
        rate_limit_delay: float,
    ) -> bool:
        """
        上传单个数据块，如果因请求过大失败，则自动二分重试。
        使用迭代实现避免栈溢出风险。
        """
        # 使用栈来模拟递归，避免栈溢出
        chunk_stack = [chunk]

        while chunk_stack:
            current_chunk = chunk_stack.pop()

            # 准备请求数据
            range_str = self._build_range_string(
                sheet_id,
                current_chunk["start_row"],
                current_chunk["start_col"],
                current_chunk["end_row"],
                current_chunk["end_col"],
            )
            value_ranges = [{"range": range_str, "values": current_chunk["data"]}]

            self.logger.info(
                f"📤 尝试上传: {len(current_chunk['data'])} 行 (范围 {range_str})"
            )

            # 发起API调用
            success, error_code = self._batch_update_ranges(
                spreadsheet_token, value_ranges
            )

            if success:
                # 解析范围信息用于日志显示
                range_info = self._parse_range_for_detailed_log(range_str)
                columns_info = (
                    f"{range_info['start_col']}列至{range_info['end_col']}列"
                    if range_info["start_col"] != range_info["end_col"]
                    else f"{range_info['start_col']}列"
                )
                rows_info = (
                    f"第{range_info['start_row']}-{range_info['end_row']}行"
                    if range_info["start_row"] != range_info["end_row"]
                    else f"第{range_info['start_row']}行"
                )

                self.logger.info(
                    f"✅ 上传成功: {len(current_chunk['data'])} 行数据至 {columns_info} {rows_info} (范围: {range_str})"
                )
                # 成功上传后进行频率控制
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)
                continue  # 继续处理栈中的下一个块

            # 如果失败，检查是否是请求过大错误
            if error_code == self.ERROR_CODE_REQUEST_TOO_LARGE:
                num_rows = len(current_chunk["data"])
                self.logger.warning(
                    f"检测到请求过大错误 (错误码 {error_code})，当前块包含 {num_rows} 行，将进行二分。"
                )

                # 如果块已经小到无法再分，则视为最终失败
                if num_rows <= 1:
                    self.logger.error(
                        f"❌ 块大小已为 {num_rows} 行，无法再分割，上传失败。"
                    )
                    return False

                # 将当前块分割成两个子块并压入栈
                mid_point = num_rows // 2

                chunk1_data = current_chunk["data"][:mid_point]
                chunk1 = {
                    "data": chunk1_data,
                    "start_row": current_chunk["start_row"],
                    "end_row": current_chunk["start_row"] + len(chunk1_data) - 1,
                    "start_col": current_chunk["start_col"],
                    "end_col": current_chunk["end_col"],
                }

                chunk2_data = current_chunk["data"][mid_point:]
                chunk2 = {
                    "data": chunk2_data,
                    "start_row": current_chunk["start_row"] + mid_point,
                    "end_row": current_chunk["start_row"]
                    + mid_point
                    + len(chunk2_data)
                    - 1,
                    "start_col": current_chunk["start_col"],
                    "end_col": current_chunk["end_col"],
                }

                # 注意：后进先出，所以先压入chunk2，后压入chunk1
                chunk_stack.append(chunk2)
                chunk_stack.append(chunk1)

                self.logger.info(
                    f" 分割为: 块1 ({len(chunk1_data)}行), 块2 ({len(chunk2_data)}行)"
                )
                continue  # 继续处理分割后的块

            # 其他类型的API错误，直接判为失败
            self.logger.error(f"❌ 上传发生不可恢复的错误 (错误码: {error_code})")
            return False

        return True  # 所有块都成功上传

    def _append_chunk_with_auto_split(
        self,
        spreadsheet_token: str,
        range_str: str,
        values: List[List[Any]],
        rate_limit_delay: float,
    ) -> bool:
        """
        追加单个数据块，如果因请求过大失败，则自动二分重试。
        使用迭代实现避免栈溢出风险。
        """
        # 使用栈来模拟递归，避免栈溢出
        values_stack = [values]

        while values_stack:
            current_values = values_stack.pop()

            self.logger.info(f"📤 尝试追加: {len(current_values)} 行")

            success, error_code = self._append_single_batch(
                spreadsheet_token, range_str, current_values
            )

            if success:
                # 解析范围信息用于日志显示
                range_info = self._parse_range_for_detailed_log(range_str)
                columns_info = (
                    f"{range_info['start_col']}列至{range_info['end_col']}列"
                    if range_info["start_col"] != range_info["end_col"]
                    else f"{range_info['start_col']}列"
                )
                start_row = range_info["start_row"]
                end_row = start_row + len(current_values) - 1
                rows_info = (
                    f"第{start_row}-{end_row}行"
                    if start_row != end_row
                    else f"第{start_row}行"
                )

                self.logger.info(
                    f"✅ 追加成功: {len(current_values)} 行数据至 {columns_info} {rows_info} (范围: {range_str})"
                )
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)
                continue  # 继续处理栈中的下一个块

            if error_code == self.ERROR_CODE_REQUEST_TOO_LARGE:
                num_rows = len(current_values)
                self.logger.warning(
                    f"检测到请求过大错误 (错误码 {error_code})，当前追加块包含 {num_rows} 行，将进行二分。"
                )

                if num_rows <= 1:
                    self.logger.error(
                        f"❌ 追加块大小已为 {num_rows} 行，无法再分割，上传失败。"
                    )
                    return False

                # 将当前块分割成两个子块并压入栈
                mid_point = num_rows // 2
                chunk1 = current_values[:mid_point]
                chunk2 = current_values[mid_point:]

                # 注意：后进先出，所以先压入chunk2，后压入chunk1
                values_stack.append(chunk2)
                values_stack.append(chunk1)

                self.logger.info(
                    f" 分割为: 块1 ({len(chunk1)}行), 块2 ({len(chunk2)}行)"
                )
                continue  # 继续处理分割后的块

            # 其他类型的API错误，直接判为失败
            self.logger.error(f"❌ 追加发生不可恢复的错误 (错误码: {error_code})")
            return False

        return True  # 所有块都成功追加

    def _batch_update_ranges(
        self, spreadsheet_token: str, value_ranges: List[Dict], is_clear: bool = False
    ) -> Tuple[bool, Optional[int]]:
        """
        批量更新多个范围。

        Returns:
            元组 (是否成功, 错误码)
        """
        token = encode_path_segment(spreadsheet_token)
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{token}/values_batch_update"
        headers = self.auth.get_auth_headers()

        data = {"valueRanges": value_ranges}

        result, error_code = self._call_boolean_api(
            "POST", url, "批量写入", headers=headers, json=data
        )
        if result is None:
            # 清空操作时，允许某些“错误”，比如清空一个已经为空的区域
            if is_clear and error_code == 90202:  # The range is invalid
                self.logger.warning(
                    f"清空操作时遇到可忽略的错误 (错误码 {error_code}), 视为成功。"
                )
                return True, 0
            return False, error_code

        # 记录详细的写入结果
        responses = result.get("data", {}).get("responses", [])
        total_cells = sum(resp.get("updatedCells", 0) for resp in responses)
        self.logger.debug(
            f"批量写入成功: {len(responses)} 个范围, 共 {total_cells} 个单元格"
        )

        return True, 0

    def set_number_format(
        self, spreadsheet_token: str, ranges: List[str], number_format: str = "#,##0.00"
    ) -> bool:
        """
        为指定范围设置数字格式

        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表
            number_format: 数字格式，默认为 "#,##0.00"

        Returns:
            是否设置成功
        """
        style = {"formatter": number_format}

        return self.set_cell_style(spreadsheet_token, ranges, style)
