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

import uuid
import logging
from typing import Dict, Any, List, Optional, Tuple, Union

from .auth import FeishuAuth
from .base import RetryableAPIClient


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
        "search": 20,         # 查询记录
        "batch_get": 20,      # 批量获取记录
        "batch_create": 50,   # 新增多条记录
        "batch_update": 50,   # 更新多条记录
        "batch_delete": 50,   # 删除多条记录
        "list_fields": 20,    # 列出字段
        "create_field": 10,   # 新增字段
    }

    # 需要重试的飞书业务错误码（瞬态错误，重试可能恢复）
    RETRYABLE_BIZ_CODES = {
        1254290,  # TooManyRequest: 请求过快
        1254607,  # Data not ready: 前置操作未完成或数据过大
        1254002,  # Fail: 通用失败（并发/超时等）
        1254001,  # InternalError: 服务器内部错误
        1254006,  # Timeout: 超时
    }

    # 明确不重试的飞书业务错误码（永久性错误，重试无意义）
    NON_RETRYABLE_BIZ_CODES = {
        1254000,  # InvalidParameter: 参数错误
        1254003,  # PermissionDenied: 权限不足
        1254004,  # NotFound: 资源不存在
        1254005,  # DuplicateRecord: 记录重复
        1254040,  # FieldNotFound: 字段不存在
    }

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

    def _is_retryable_biz_code(self, code: int) -> bool:
        """判断飞书业务错误码是否可重试"""
        if code in self.RETRYABLE_BIZ_CODES:
            return True
        if code != 0 and code not in self.NON_RETRYABLE_BIZ_CODES:
            self.logger.warning(
                f"未知的飞书业务错误码 {code}，不进行重试。如该错误可恢复，请反馈以更新重试列表。"
            )
        return False

    def _call_api_with_biz_retry(
        self, method: str, url: str, max_retries: int = 3, **kwargs
    ):
        """
        调用API并处理飞书业务错误码重试。

        飞书部分限流错误以 HTTP 200 + 业务错误码返回（如 1254290 TooManyRequest），
        HTTP层面的重试机制无法捕获这类错误，需要在应用层检查并重试。

        Args:
            method: HTTP方法
            url: 请求URL
            max_retries: 最大重试次数
            **kwargs: 传递给 call_api 的参数

        Returns:
            (response, result_dict) 元组
        """
        import time as _time

        for attempt in range(max_retries + 1):
            response = self.api_client.call_api(method, url, **kwargs)
            try:
                result = response.json()
            except ValueError:
                return response, None

            code = result.get("code", 0)
            if code == 0 or not self._is_retryable_biz_code(code):
                return response, result

            # 可重试的业务错误
            if attempt < max_retries:
                wait_time = 2 ** attempt
                error_msg = result.get("msg", "未知错误")
                self.logger.warning(
                    f"飞书业务错误码 {code}（{error_msg}），等待 {wait_time}s 后第 {attempt + 1} 次重试..."
                )
                _time.sleep(wait_time)
            else:
                return response, result

        return response, result

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
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.auth.get_auth_headers()

        all_fields = []
        page_token = None

        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token

            response, result = self._call_api_with_biz_retry(
                "GET", url, headers=headers, params=params
            )

            if result is None:
                raise Exception(
                    f"获取字段列表响应解析失败, HTTP状态码: {response.status_code}"
                )

            if result.get("code") != 0:
                error_msg = result.get("msg", "未知错误")
                raise Exception(
                    f"获取字段列表失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
                )

            data = result.get("data", {})
            all_fields.extend(data.get("items", []))

            if not data.get("has_more"):
                break
            page_token = data.get("page_token")

        return all_fields

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
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.auth.get_auth_headers()
        data = {"field_name": field_name, "type": field_type}

        response, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, json=data
        )

        if result is None:
            self.logger.error(
                f"创建字段 '{field_name}' 响应解析失败, HTTP状态码: {response.status_code}"
            )
            return False

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            self.logger.error(
                f"创建字段 '{field_name}' 失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )
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
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
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

        response, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, params=params, json=data
        )

        if result is None:
            raise Exception(
                f"搜索记录响应解析失败, HTTP状态码: {response.status_code}"
            )

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            raise Exception(
                f"搜索记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )

        result_data = result.get("data", {})
        records = result_data.get("items", [])
        next_page_token = (
            result_data.get("page_token") if result_data.get("has_more") else None
        )

        return records, next_page_token

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
        all_records = []
        page_token = None
        page_num = 0
        seen_page_tokens = set()

        if field_names is None:
            field_hint = "（全部字段）"
        elif len(field_names) == 0:
            field_hint = "（仅 record_id）"
        else:
            field_hint = f"（指定字段: {field_names}）"
        self.logger.info(f"开始拉取全部记录...{field_hint}")

        while True:
            records, next_page_token = self.search_records(
                app_token, table_id, page_token, field_names=field_names
            )
            all_records.extend(records)
            page_num += 1

            if (
                page_num == 1
                or page_num % 5 == 0
                or not next_page_token
            ):
                self.logger.info(
                    f"已拉取 {len(all_records)} 条记录（第 {page_num} 页）"
                )

            if next_page_token:
                if next_page_token in seen_page_tokens:
                    raise Exception(
                        "检测到重复 page_token，可能导致死循环，请检查接口响应"
                    )
                seen_page_tokens.add(next_page_token)

            page_token = next_page_token

            if not page_token:
                break

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

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = self.auth.get_auth_headers()

        # 生成唯一的client_token，并添加性能优化参数
        client_token = str(uuid.uuid4())
        params = {
            "client_token": client_token,
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id",
        }

        data = {"records": records}

        response, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, params=params, json=data
        )

        if result is None:
            self.logger.error(
                f"批量创建记录响应解析失败, HTTP状态码: {response.status_code}"
            )
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            self.logger.error(
                f"批量创建记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )
            self.logger.debug(f"创建失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
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

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = self.auth.get_auth_headers()

        # 添加查询参数提高性能
        params = {
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id",
        }

        data = {"records": records}

        response, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, params=params, json=data
        )

        if result is None:
            self.logger.error(
                f"批量更新记录响应解析失败, HTTP状态码: {response.status_code}"
            )
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            self.logger.error(
                f"批量更新记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )
            self.logger.debug(f"更新失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
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

        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = self.auth.get_auth_headers()
        data = {"records": record_ids}

        response, result = self._call_api_with_biz_retry(
            "POST", url, headers=headers, json=data
        )

        if result is None:
            self.logger.error(
                f"批量删除记录响应解析失败, HTTP状态码: {response.status_code}"
            )
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            self.logger.error(
                f"批量删除记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )
            self.logger.debug(f"删除失败的记录数量: {len(record_ids)}")
            self.logger.debug(f"API响应: {result}")
            return False

        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功删除 {len(record_ids)} 条记录")
        return True

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
            15: "超链接",
            17: "附件",
            19: "单向关联",
            21: "查找引用",
            22: "公式",
            23: "双向关联",
        }
        return type_mapping.get(field_type, f"未知类型({field_type})")
