#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XTF 专用飞书 SDK 的通用响应、分页和批处理契约。"""

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import math
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)

import requests  # type: ignore[import-untyped]

if TYPE_CHECKING:
    from .auth import FeishuAuth
    from .base import RetryableAPIClient
    from .bitable import BitableAPI
    from .sheet import SheetAPI

T = TypeVar("T")
R = TypeVar("R")


@dataclass(frozen=True)
class Page(Generic[T]):
    """一个显式分页结果，避免调用方猜测游标状态。"""

    items: List[T]
    next_page_token: Optional[str]
    has_more: bool
    raw: Dict[str, Any]


class FeishuAPIError(Exception):
    """OpenAPI 的结构化错误，保留排障和重试所需的事实。"""

    def __init__(
        self,
        code: int,
        message: str,
        *,
        http_status: Optional[int] = None,
        log_id: Optional[str] = None,
        retryable: bool = False,
        retry_after: Optional[float] = None,
        response_data: Optional[Dict[str, Any]] = None,
        kind: str = "api",
    ):
        self.code = code
        self.message = message
        self.msg = message  # 兼容 SheetAPI 既有属性
        self.http_status = http_status
        self.log_id = log_id
        self.retryable = retryable
        self.retry_after = retry_after
        self.response_data = response_data or {}
        self.kind = kind

        details = [f"code={code}"]
        if http_status is not None:
            details.append(f"http_status={http_status}")
        if log_id:
            details.append(f"log_id={log_id}")
        super().__init__(f"Feishu API error {code}: {message} ({', '.join(details)})")

    @classmethod
    def from_transport(
        cls, message: str, *, cause: Optional[BaseException] = None
    ) -> "FeishuAPIError":
        """将没有 HTTP response 的网络失败包装为统一错误。"""
        error = cls(-1, message, retryable=True, kind="transport")
        if cause is not None:
            error.__cause__ = cause
        return error


class PaginationError(Exception):
    """服务端分页契约不完整或可能形成死循环。"""


class PartialBatchError(Exception):
    """批处理只完成了前缀批次；已成功部分不会被假定回滚。"""

    def __init__(
        self,
        operation: str,
        requested: int,
        processed: int,
        failed_batch_index: int,
        cause: Exception,
    ):
        self.operation = operation
        self.requested = requested
        self.processed = processed
        self.failed_batch_index = failed_batch_index
        self.cause = cause
        super().__init__(
            f"{operation} 部分完成: 已处理 {processed}/{requested}，"
            f"第 {failed_batch_index} 批失败；已成功批次不会自动回滚: {cause}"
        )


class FeishuResponseParser:
    """统一解析 JSON envelope、HTTP 状态、业务码和排障元数据。"""

    RETRYABLE_BIZ_CODES = {
        1254001,
        1254002,
        1254006,
        1254290,
        1254291,
        1254607,
        1255001,
        1255002,
        99991400,
    }

    @classmethod
    def parse(cls, response: requests.Response) -> Dict[str, Any]:
        status_code = response.status_code
        try:
            result = response.json()
        except ValueError as exc:
            raise FeishuAPIError(
                status_code or -1,
                "OpenAPI 返回了无效 JSON",
                http_status=status_code,
                log_id=cls._extract_log_id(response, {}),
                retryable=status_code == 429 or status_code >= 500,
                retry_after=cls._parse_retry_after(response),
                kind="invalid_response",
            ) from exc

        if not isinstance(result, dict):
            raise FeishuAPIError(
                -1,
                "OpenAPI 返回的 JSON 不是对象",
                http_status=status_code,
                log_id=cls._extract_log_id(response, {}),
                retryable=status_code == 429 or status_code >= 500,
                retry_after=cls._parse_retry_after(response),
                kind="invalid_response",
            )

        code = cls._coerce_code(result.get("code", 0))
        if status_code >= 400 or code != 0:
            effective_code = code if code != 0 else status_code
            raise FeishuAPIError(
                effective_code,
                str(
                    result.get("msg")
                    or result.get("message")
                    or f"OpenAPI 请求失败: HTTP {status_code}"
                ),
                http_status=status_code,
                log_id=cls._extract_log_id(response, result),
                retryable=(
                    status_code == 429
                    or status_code >= 500
                    or effective_code in cls.RETRYABLE_BIZ_CODES
                ),
                retry_after=cls._parse_retry_after(response),
                response_data=result,
            )
        return result

    @staticmethod
    def _coerce_code(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return -1

    @staticmethod
    def _extract_log_id(
        response: requests.Response, result: Dict[str, Any]
    ) -> Optional[str]:
        for key in ("log_id", "logid"):
            value = result.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        error = result.get("error")
        if isinstance(error, dict):
            for key in ("log_id", "logid"):
                value = error.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        for key in ("X-Tt-Logid", "X-Request-Id"):
            value = FeishuResponseParser._get_header(response, key)
            if value and value.strip():
                return value.strip()
        return None

    @staticmethod
    def _get_header(response: requests.Response, name: str) -> Optional[str]:
        headers = response.headers
        if not hasattr(headers, "get"):
            return None
        value = headers.get(name)
        if isinstance(value, str):
            return value
        items = getattr(headers, "items", None)
        if not callable(items):
            return None
        lower_name = name.lower()
        header_items = items()
        if not isinstance(header_items, (list, tuple)) and not hasattr(
            header_items, "__iter__"
        ):
            return None
        for key, candidate in header_items:
            if (
                isinstance(key, str)
                and key.lower() == lower_name
                and isinstance(candidate, str)
            ):
                return candidate
        return None

    @staticmethod
    def _parse_retry_after(response: requests.Response) -> Optional[float]:
        for key in ("X-Ogw-Ratelimit-Reset", "Retry-After"):
            raw = FeishuResponseParser._get_header(response, key)
            if not isinstance(raw, str) or not raw.strip():
                continue
            try:
                seconds = float(raw.strip())
                if seconds > 0:
                    return seconds
            except ValueError:
                if key != "Retry-After":
                    continue
                try:
                    retry_at = parsedate_to_datetime(raw)
                    date_header = FeishuResponseParser._get_header(response, "Date")
                    reference_time = (
                        parsedate_to_datetime(date_header)
                        if date_header
                        else datetime.now(timezone.utc)
                    )
                    delay = retry_at.timestamp() - reference_time.timestamp()
                    if delay > 0:
                        return float(math.ceil(delay))
                except (TypeError, ValueError, OverflowError):
                    continue
        return None


class XTFFeishuClient:
    """兼容式统一入口：共享认证和 transport，并按需创建目标 API。"""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        *,
        max_retries: int = 3,
        rate_limit_delay: float = 0.5,
        api_client: Optional["RetryableAPIClient"] = None,
    ):
        """创建统一 client；传入 api_client 时重试和频控参数由该实例负责。"""
        from .auth import FeishuAuth
        from .base import RateLimiter, RetryableAPIClient

        self.api_client = api_client or RetryableAPIClient(
            max_retries=max_retries,
            rate_limiter=RateLimiter(rate_limit_delay),
        )
        self.auth: FeishuAuth = FeishuAuth(
            app_id,
            app_secret,
            api_client=self.api_client,
        )

    def bitable(self) -> "BitableAPI":
        """创建与本 client 共享认证和 transport 的 BitableAPI。"""
        from .bitable import BitableAPI

        return BitableAPI(self.auth, self.api_client)

    def sheet(
        self,
        *,
        start_row: int = 1,
        start_column: str = "A",
        scan_max_rows: Optional[int] = None,
        scan_max_cols: Optional[int] = None,
        write_max_rows: Optional[int] = None,
        write_max_cols: Optional[int] = None,
        value_render_option: Optional[str] = None,
        datetime_render_option: Optional[str] = None,
    ) -> "SheetAPI":
        """创建与本 client 共享认证和 transport 的 SheetAPI。"""
        from .sheet import SheetAPI

        return SheetAPI(
            self.auth,
            self.api_client,
            start_row=start_row,
            start_column=start_column,
            scan_max_rows=scan_max_rows,
            scan_max_cols=scan_max_cols,
            write_max_rows=write_max_rows,
            write_max_cols=write_max_cols,
            value_render_option=value_render_option,
            datetime_render_option=datetime_render_option,
        )


class Paginator(Generic[T]):
    """缺失或重复游标立即失败，不静默返回不完整集合。"""

    def __init__(self, max_pages: Optional[int] = None):
        if max_pages is not None and max_pages <= 0:
            raise ValueError("max_pages 必须为正整数或 None")
        self.max_pages = max_pages

    def iter_pages(
        self, fetch_page: Callable[[Optional[str]], Page[T]]
    ) -> Iterator[Page[T]]:
        page_token: Optional[str] = None
        seen_tokens = set()
        page_number = 0

        while True:
            page = fetch_page(page_token)
            if not isinstance(page.items, list):
                raise PaginationError("分页响应 items 必须是列表")
            page_number += 1
            yield page
            if not page.has_more:
                return
            if self.max_pages is not None and page_number >= self.max_pages:
                raise PaginationError(
                    f"分页达到 max_pages={self.max_pages}，结果尚未完整"
                )
            next_token = page.next_page_token
            if not next_token:
                raise PaginationError("has_more=true 但响应未提供 page_token")
            if next_token == page_token or next_token in seen_tokens:
                raise PaginationError(f"检测到重复 page_token: {next_token}")
            seen_tokens.add(next_token)
            page_token = next_token

    def collect(self, fetch_page: Callable[[Optional[str]], Page[T]]) -> List[T]:
        items: List[T] = []
        for page in self.iter_pages(fetch_page):
            items.extend(page.items)
        return items


def iter_chunks(items: List[T], size: int) -> Iterator[List[T]]:
    if size <= 0:
        raise ValueError("批处理大小必须为正整数")
    for start in range(0, len(items), size):
        yield items[start : start + size]


def run_batches(
    operation: str,
    items: List[T],
    batch_size: int,
    processor: Callable[[List[T]], R],
    success: Optional[Callable[[R], bool]] = None,
) -> List[R]:
    """串行执行批次，首个失败即停止并报告已应用前缀。

    未提供 ``success`` 时处理器必须返回 bool，避免 tuple 或普通对象因非空而
    被误判为成功。需要其他结果类型的调用方应显式提供成功判定函数。
    """

    results: List[R] = []
    processed = 0
    for batch_index, batch in enumerate(iter_chunks(items, batch_size), start=1):
        try:
            result = processor(batch)
            if success is None:
                if not isinstance(result, bool):
                    raise TypeError("处理器必须返回 bool 或显式提供 success 判定函数")
                succeeded: bool = result
            else:
                succeeded = success(result)
            if not succeeded:
                raise RuntimeError("处理器返回失败结果")
        except Exception as exc:
            raise PartialBatchError(
                operation,
                requested=len(items),
                processed=processed,
                failed_batch_index=batch_index,
                cause=exc,
            ) from exc
        results.append(result)
        processed += len(batch)
    return results
