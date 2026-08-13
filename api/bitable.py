#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多维表格 API 模块

模块概述：
    此模块封装了飞书多维表格（Bitable）的 API 操作，提供字段管理
    和记录的增删改查功能。多维表格是飞书的结构化数据存储产品，
    类似于在线数据库。

主要功能：
    1. 字段管理（获取字段列表、创建新字段）
    2. 记录查询（搜索记录、获取全部记录）
    3. 记录创建（批量创建）
    4. 记录更新（批量更新）
    5. 记录删除（批量删除）

核心类：
    BitableAPI:
        飞书多维表格 API 客户端，封装所有多维表格相关的 API 调用。

API 限制常量：
    - MAX_SEARCH_PAGE_SIZE: 100（搜索接口每页最大记录数）
    - MAX_BATCH_CREATE_SIZE: 1000（批量创建每次最大记录数）
    - MAX_BATCH_UPDATE_SIZE: 1000（批量更新每次最大记录数）
    - MAX_BATCH_DELETE_SIZE: 500（批量删除每次最大记录数）

字段类型编码：
    1  - 多行文本
    2  - 数字
    3  - 单选
    4  - 多选
    5  - 日期
    7  - 复选框
    11 - 人员
    15 - 超链接
    17 - 附件
    19 - 单向关联
    21 - 查找引用
    22 - 公式
    23 - 双向关联

API 端点（基础路径：https://open.feishu.cn/open-apis/bitable/v1）：
    字段：
        GET  /apps/{app_token}/tables/{table_id}/fields - 获取字段列表
        POST /apps/{app_token}/tables/{table_id}/fields - 创建字段
    记录：
        POST /apps/{app_token}/tables/{table_id}/records/search - 搜索记录
        POST /apps/{app_token}/tables/{table_id}/records/batch_create - 批量创建
        POST /apps/{app_token}/tables/{table_id}/records/batch_update - 批量更新
        POST /apps/{app_token}/tables/{table_id}/records/batch_delete - 批量删除

使用示例：
    >>> from api import FeishuAuth, BitableAPI
    >>>
    >>> auth = FeishuAuth(app_id, app_secret)
    >>> api = BitableAPI(auth)
    >>>
    >>> # 获取字段列表
    >>> fields = api.list_fields(app_token, table_id)
    >>>
    >>> # 创建字段
    >>> api.create_field(app_token, table_id, "姓名", field_type=1)
    >>>
    >>> # 获取所有记录
    >>> records = api.get_all_records(app_token, table_id)
    >>>
    >>> # 批量创建记录
    >>> new_records = [{"fields": {"姓名": "张三", "年龄": 25}}]
    >>> api.batch_create_records(app_token, table_id, new_records)

分页处理：
    搜索记录接口支持分页，使用 page_token 实现：
    1. 首次请求不传 page_token
    2. 响应中 has_more=true 时，使用返回的 page_token 继续请求
    3. has_more=false 时表示已获取全部数据

    get_all_records 方法已封装完整的分页逻辑。

性能优化参数：
    - ignore_consistency_check: 跳过一致性检查，提高写入性能
    - client_token: 幂等性标识，防止重复创建

依赖关系：
    内部模块：
        - api.auth: 认证管理（FeishuAuth）
        - api.base: 网络请求（RetryableAPIClient）
    外部依赖：
        - uuid: 生成幂等性标识
        - logging: 日志记录

注意事项：
    1. 批量操作数量超过限制会返回失败
    2. 字段名称在表格内必须唯一
    3. 创建记录时字段名必须已存在
    4. 删除记录需要 record_id，不支持按条件删除

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import logging
import uuid
from typing import Dict, Any, List, Optional, Tuple, Union

from .auth import FeishuAuth
from .base import RetryableAPIClient
from .sdk import (
    FeishuAPIError,
    FeishuResponseParser,
    Page,
    PaginationError,
    Paginator,
)
from .url import encode_path_segment


class BitableAPI:
    """飞书多维表格API客户端"""

    # 批量接口上限（避免超出API限制）
    MAX_SEARCH_PAGE_SIZE = 100
    MAX_BATCH_CREATE_SIZE = 1000
    MAX_BATCH_UPDATE_SIZE = 1000
    MAX_BATCH_DELETE_SIZE = 500

    # 飞书官方接口频率限制（次/秒）
    # 数据来源：https://open.feishu.cn/document/ukTMukTMukTM/uUzN04SN3QjL1cDN
    # 作为程序内嵌上限使用，不额外折扣
    OFFICIAL_RATE_LIMITS = {
        "search": 20,  # 查询记录
        "batch_get": 20,  # 批量获取记录
        "batch_create": 50,  # 新增多条记录
        "batch_update": 50,  # 更新多条记录
        "batch_delete": 50,  # 删除多条记录
        "list_fields": 20,  # 列出字段
        "create_field": 10,  # 新增字段
    }

    # 需要重试的飞书业务错误码（瞬态错误，重试可能恢复）
    RETRYABLE_BIZ_CODES = FeishuResponseParser.RETRYABLE_BIZ_CODES

    def __init__(
        self, auth: FeishuAuth, api_client: Optional[RetryableAPIClient] = None
    ):
        """
        初始化多维表格API客户端

        Args:
            auth: 飞书认证管理器
            api_client: API客户端实例
        """
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.logger = logging.getLogger("XTF.bitable")

    def _call_api_with_biz_retry(
        self, method: str, url: str, max_retries: Optional[int] = None, **kwargs
    ):
        """
        调用API并处理飞书业务错误码重试。

        飞书部分限流错误以 HTTP 200 + 业务错误码返回（如 1254290 TooManyRequest），
        HTTP 层面的重试机制无法捕获这类错误，需要在应用层检查并重试。
        网络异常和 HTTP 429/5xx 只由共享 transport 负责，这里不会再次重试，
        避免对非幂等 POST 形成嵌套请求。未显式传入 max_retries 时，复用
        transport 的重试预算；传入 0 可关闭业务码重试。

        Args:
            method: HTTP方法
            url: 请求URL
            max_retries: 业务码最大重试次数；None 表示使用 transport 的预算
            **kwargs: 传递给 call_api 的参数

        Returns:
            (response, result_dict) 元组
        """
        import time as _time

        # transport 独占网络异常和 HTTP 429/5xx 重试；这里仅处理
        # HTTP 200 中的明确飞书业务错误码，避免嵌套循环放大请求次数。
        configured_retries = getattr(self.api_client, "max_retries", 3)
        if not isinstance(configured_retries, int):
            configured_retries = 3
        effective_max_retries = (
            configured_retries if max_retries is None else max_retries
        )
        effective_max_retries = max(0, effective_max_retries)

        for attempt in range(effective_max_retries + 1):
            response = self.api_client.call_api(method, url, **kwargs)
            error: Optional[FeishuAPIError] = None
            try:
                result = FeishuResponseParser.parse(response)
                return response, result
            except FeishuAPIError as caught_error:
                error = caught_error
                is_retryable = (
                    caught_error.http_status is not None
                    and caught_error.http_status < 400
                    and caught_error.code in FeishuResponseParser.RETRYABLE_BIZ_CODES
                )
                if not is_retryable or attempt >= effective_max_retries:
                    raise

            # 可重试的业务错误
            if error is not None and attempt < effective_max_retries:
                wait_time = (
                    error.retry_after
                    if error.retry_after is not None
                    else float(2**attempt)
                )
                self.logger.warning(
                    f"飞书业务错误码 {error.code}（{error.message}），等待 {wait_time}s 后第 {attempt + 1} 次重试..."
                )
                _time.sleep(wait_time)

        raise RuntimeError("飞书业务错误重试循环异常退出")

    @staticmethod
    def _parse_page_data(result: Dict[str, Any]) -> Dict[str, Any]:
        """校验分页 envelope，避免 null/错误类型被误当成完整数据。"""
        data = result.get("data")
        if not isinstance(data, dict):
            raise FeishuAPIError(
                -1,
                "分页响应 data 必须是对象",
                response_data=result,
                kind="invalid_response",
            )
        items = data.get("items", [])
        if not isinstance(items, list):
            raise FeishuAPIError(
                -1,
                "分页响应 items 必须是列表",
                response_data=result,
                kind="invalid_response",
            )
        has_more = data.get("has_more", False)
        if not isinstance(has_more, bool):
            raise FeishuAPIError(
                -1,
                "分页响应 has_more 必须是布尔值",
                response_data=result,
                kind="invalid_response",
            )
        page_token = data.get("page_token")
        if page_token is not None and not isinstance(page_token, str):
            raise FeishuAPIError(
                -1,
                "分页响应 page_token 必须是字符串或 null",
                response_data=result,
                kind="invalid_response",
            )
        return data

    def list_fields(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        """
        列出表格字段

        Args:
            app_token: 应用Token
            table_id: 数据表ID

        Returns:
            字段列表

        Raises:
            Exception: 当API调用失败时
        """
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/fields"
        headers = self.auth.get_auth_headers()

        def fetch_page(page_token: Optional[str]) -> Page[Dict[str, Any]]:
            params: Dict[str, Union[int, str]] = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token

            _, result = self._call_api_with_biz_retry(
                "GET", url, headers=headers, params=params
            )

            data = self._parse_page_data(result)
            return Page(
                items=data.get("items", []),
                next_page_token=data.get("page_token"),
                has_more=bool(data.get("has_more")),
                raw=data,
            )

        return Paginator[Dict[str, Any]]().collect(fetch_page)

    def create_field(
        self, app_token: str, table_id: str, field_name: str, field_type: int = 1
    ) -> bool:
        """
        创建字段

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            field_name: 字段名称
            field_type: 字段类型（1=多行文本）

        Returns:
            是否创建成功
        """
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/fields"
        headers = self.auth.get_auth_headers()
        data = {"field_name": field_name, "type": field_type}

        try:
            self._call_api_with_biz_retry("POST", url, headers=headers, json=data)
        except FeishuAPIError as error:
            self._log_boolean_operation_error(f"创建字段 '{field_name}'", error)
            return False

        # 获取字段类型信息用于日志显示
        field_type_name = self._get_field_type_display_name(field_type)
        field_config_info = {"type": field_type}
        self.logger.info(
            f"✅ 创建字段 '{field_name}' 成功: 类型 {field_type_name}, 配置 {field_config_info}"
        )
        return True

    def search_records(
        self,
        app_token: str,
        table_id: str,
        page_token: Optional[str] = None,
        page_size: int = 100,
        field_names: Optional[List[str]] = None,
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        搜索记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            page_token: 分页标记
            page_size: 页面大小
            field_names: 指定返回的字段名称列表，为None时返回全部字段

        Returns:
            记录列表和下一页标记的元组

        Raises:
            Exception: 当API调用失败时
        """
        page = self._search_records_page(
            app_token,
            table_id,
            page_token=page_token,
            page_size=page_size,
            field_names=field_names,
        )
        if page.has_more and not page.next_page_token:
            raise PaginationError("has_more=true 但响应未提供 page_token")
        return page.items, page.next_page_token if page.has_more else None

    def _search_records_page(
        self,
        app_token: str,
        table_id: str,
        page_token: Optional[str] = None,
        page_size: int = 100,
        field_names: Optional[List[str]] = None,
    ) -> Page[Dict[str, Any]]:
        """读取单页并保留服务端 has_more，供完整分页校验使用。"""
        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/search"
        headers = self.auth.get_auth_headers()

        # 分页参数作为查询参数（限制在接口上限内）
        effective_page_size = page_size
        if page_size > self.MAX_SEARCH_PAGE_SIZE:
            effective_page_size = self.MAX_SEARCH_PAGE_SIZE
            self.logger.warning(
                f"page_size={page_size} 超过接口上限 {self.MAX_SEARCH_PAGE_SIZE}，已自动降至 {effective_page_size}"
            )
        elif page_size <= 0:
            effective_page_size = self.MAX_SEARCH_PAGE_SIZE
            self.logger.warning(
                f"page_size={page_size} 非法，已自动使用 {effective_page_size}"
            )

        params: Dict[str, Union[int, str]] = {"page_size": effective_page_size}
        if page_token:
            params["page_token"] = page_token

        # 请求体：支持 field_names 指定返回字段，减少不必要的数据传输
        data: Dict[str, Any] = {}
        if field_names is not None:
            data["field_names"] = field_names

        _, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, params=params, json=data
        )

        result_data = self._parse_page_data(result)
        records = result_data.get("items", [])
        return Page(
            items=records,
            next_page_token=result_data.get("page_token"),
            has_more=bool(result_data.get("has_more")),
            raw=result_data,
        )

    def get_all_records(
        self, app_token: str, table_id: str, field_names: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        获取所有记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            field_names: 指定返回的字段名称列表，为None时返回全部字段

        Returns:
            所有记录的列表
        """
        page_num = 0

        if field_names is None:
            field_hint = "（全部字段）"
        elif len(field_names) == 0:
            field_hint = "（仅 record_id）"
        else:
            field_hint = f"（指定字段: {field_names}）"
        self.logger.info(f"开始拉取全部记录...{field_hint}")

        def fetch_page(page_token: Optional[str]) -> Page[Dict[str, Any]]:
            nonlocal page_num
            page = self._search_records_page(
                app_token,
                table_id,
                page_token=page_token,
                field_names=field_names,
            )
            page_num += 1
            return page

        all_records = Paginator[Dict[str, Any]]().collect(fetch_page)
        self.logger.info(f"已拉取 {len(all_records)} 条记录（共 {page_num} 页）")
        return all_records

    def batch_create_records(
        self, app_token: str, table_id: str, records: List[Dict]
    ) -> bool:
        """
        批量创建记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            records: 记录列表

        Returns:
            是否创建成功
        """
        if len(records) > self.MAX_BATCH_CREATE_SIZE:
            self.logger.error(
                f"批量创建记录数量 {len(records)} 超过接口上限 {self.MAX_BATCH_CREATE_SIZE}"
            )
            return False

        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_create"
        headers = self.auth.get_auth_headers()

        # 每次逻辑调用生成一个全局唯一 token；业务错误重试复用同一 params，
        # 避免进程内外 token 碰撞或一次重试重复创建。
        client_token = str(uuid.uuid4())
        params = {
            "client_token": client_token,
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id",
        }

        data = {"records": records}

        try:
            self._call_api_with_biz_retry(
                "POST", url, headers=headers, params=params, json=data
            )
        except FeishuAPIError as error:
            self._log_boolean_operation_error("批量创建记录", error)
            return False

        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功创建 {len(records)} 条记录")
        return True

    def batch_update_records(
        self, app_token: str, table_id: str, records: List[Dict]
    ) -> bool:
        """
        批量更新记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            records: 记录列表

        Returns:
            是否更新成功
        """
        if len(records) > self.MAX_BATCH_UPDATE_SIZE:
            self.logger.error(
                f"批量更新记录数量 {len(records)} 超过接口上限 {self.MAX_BATCH_UPDATE_SIZE}"
            )
            return False

        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_update"
        headers = self.auth.get_auth_headers()

        # 添加查询参数提高性能
        params = {
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id",
        }

        data = {"records": records}

        try:
            self._call_api_with_biz_retry(
                "POST", url, headers=headers, params=params, json=data
            )
        except FeishuAPIError as error:
            self._log_boolean_operation_error("批量更新记录", error)
            return False

        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功更新 {len(records)} 条记录")
        return True

    def batch_delete_records(
        self, app_token: str, table_id: str, record_ids: List[str]
    ) -> bool:
        """
        批量删除记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            record_ids: 记录ID列表

        Returns:
            是否删除成功
        """
        if len(record_ids) > self.MAX_BATCH_DELETE_SIZE:
            self.logger.error(
                f"批量删除记录数量 {len(record_ids)} 超过接口上限 {self.MAX_BATCH_DELETE_SIZE}"
            )
            return False

        app = encode_path_segment(app_token)
        table = encode_path_segment(table_id)
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app}/tables/{table}/records/batch_delete"
        headers = self.auth.get_auth_headers()
        data = {"records": record_ids}

        try:
            self._call_api_with_biz_retry("POST", url, headers=headers, json=data)
        except FeishuAPIError as error:
            self._log_boolean_operation_error("批量删除记录", error)
            return False

        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功删除 {len(record_ids)} 条记录")
        return True

    def _log_boolean_operation_error(
        self, operation: str, error: FeishuAPIError
    ) -> None:
        """在 bool 公共方法边界保留旧的失败返回契约。"""
        details = f"错误码 {error.code}, 错误信息: {error.message}"
        if error.log_id:
            details += f", log_id: {error.log_id}"
        self.logger.error(f"{operation}失败: {details}")

    def _get_field_type_display_name(self, field_type: int) -> str:
        """获取字段类型的显示名称"""
        type_mapping = {
            1: "文本",
            2: "数字",
            3: "单选",
            4: "多选",
            5: "日期",
            7: "复选框",
            11: "人员",
            13: "电话号码",
            15: "超链接",
            17: "附件",
            18: "单向关联",
            19: "查找引用",
            20: "公式",
            21: "双向关联",
            22: "地理位置",
            23: "群组",
            1001: "创建时间",
            1002: "最后更新时间",
            1003: "创建人",
            1004: "修改人",
            1005: "自动编号",
        }
        return type_mapping.get(field_type, f"未知类型({field_type})")
