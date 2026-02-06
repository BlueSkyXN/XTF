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
from utils.validators import validate_feishu_app_token, validate_feishu_table_id


class BitableAPI:
    """飞书多维表格API客户端"""

    # 批量接口上限（避免超出API限制）
    MAX_SEARCH_PAGE_SIZE = 100
    MAX_BATCH_CREATE_SIZE = 1000
    MAX_BATCH_UPDATE_SIZE = 1000
    MAX_BATCH_DELETE_SIZE = 500

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

    def _validate_tokens(self, app_token: str, table_id: str) -> Tuple[str, str]:
        """
        验证令牌格式，防止 SSRF 和路径遍历攻击
        
        此方法验证 app_token 和 table_id 是否包含合法字符，
        拒绝包含路径遍历序列（如 ../、/、\\ 等）的输入。
        
        Args:
            app_token: 多维表格应用令牌
            table_id: 数据表 ID
        
        Returns:
            Tuple[str, str]: 验证通过的 (app_token, table_id)
        
        Raises:
            ValidationError: 当令牌格式无效时
        """
        return validate_feishu_app_token(app_token), validate_feishu_table_id(table_id)

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
        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.auth.get_auth_headers()

        all_fields = []
        page_token = None

        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token

            response = self.api_client.call_api(
                "GET", url, headers=headers, params=params
            )

            try:
                result = response.json()
            except ValueError as e:
                raise Exception(
                    f"获取字段列表响应解析失败: {e}, HTTP状态码: {response.status_code}"
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
        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.auth.get_auth_headers()
        data = {"field_name": field_name, "type": field_type}

        response = self.api_client.call_api("POST", url, headers=headers, json=data)

        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(
                f"创建字段 '{field_name}' 响应解析失败: {e}, HTTP状态码: {response.status_code}"
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
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        搜索记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID
            page_token: 分页标记
            page_size: 页面大小

        Returns:
            记录列表和下一页标记的元组

        Raises:
            Exception: 当API调用失败时
        """
        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
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

        # 请求体可以包含过滤条件、排序等（当前为空）
        data: Dict[str, Any] = {}

        response = self.api_client.call_api(
            "POST", url, headers=headers, params=params, json=data
        )

        try:
            result = response.json()
        except ValueError as e:
            raise Exception(
                f"搜索记录响应解析失败: {e}, HTTP状态码: {response.status_code}"
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

    def get_all_records(self, app_token: str, table_id: str) -> List[Dict]:
        """
        获取所有记录

        Args:
            app_token: 应用Token
            table_id: 数据表ID

        Returns:
            所有记录的列表
        """
        all_records = []
        page_token = None
        page_num = 0
        seen_page_tokens = set()

        self.logger.info("开始拉取全部记录...")

        while True:
            records, next_page_token = self.search_records(
                app_token, table_id, page_token
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

        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
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

        response = self.api_client.call_api(
            "POST", url, headers=headers, params=params, json=data
        )

        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(
                f"批量创建记录响应解析失败: {e}, HTTP状态码: {response.status_code}"
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

        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = self.auth.get_auth_headers()

        # 添加查询参数提高性能
        params = {
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id",
        }

        data = {"records": records}

        response = self.api_client.call_api(
            "POST", url, headers=headers, params=params, json=data
        )

        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(
                f"批量更新记录响应解析失败: {e}, HTTP状态码: {response.status_code}"
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

        # 验证令牌格式，防止 SSRF 和路径遍历
        app_token, table_id = self._validate_tokens(app_token, table_id)
        
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = self.auth.get_auth_headers()
        data = {"records": record_ids}

        response = self.api_client.call_api("POST", url, headers=headers, json=data)

        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(
                f"批量删除记录响应解析失败: {e}, HTTP状态码: {response.status_code}"
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
