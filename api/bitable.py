#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多维表格API模块
提供飞书多维表格的字段和记录操作功能
"""

import uuid
import logging
from typing import Dict, Any, List, Optional, Tuple, Union

from .auth import FeishuAuth
from .base import RetryableAPIClient


class BitableAPI:
    """飞书多维表格API客户端"""
    
    def __init__(self, auth: FeishuAuth, api_client: Optional[RetryableAPIClient] = None):
        """
        初始化多维表格API客户端
        
        Args:
            auth: 飞书认证管理器
            api_client: API客户端实例
        """
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.logger = logging.getLogger('XTF.bitable')
    
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
            
            response = self.api_client.call_api("GET", url, headers=headers, params=params)
            
            try:
                result = response.json()
            except ValueError as e:
                raise Exception(f"获取字段列表响应解析失败: {e}, HTTP状态码: {response.status_code}")
            
            if result.get("code") != 0:
                error_msg = result.get('msg', '未知错误')
                raise Exception(f"获取字段列表失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            
            data = result.get("data", {})
            all_fields.extend(data.get("items", []))
            
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
        
        return all_fields
    
    def create_field(self, app_token: str, table_id: str, field_name: str, field_type: int = 1) -> bool:
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
        data = {
            "field_name": field_name,
            "type": field_type
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"创建字段 '{field_name}' 响应解析失败: {e}, HTTP状态码: {response.status_code}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"创建字段 '{field_name}' 失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            return False
        
        # 获取字段类型信息用于日志显示
        field_type_name = self._get_field_type_display_name(field_type)
        field_config_info = {"type": field_type}
        self.logger.info(f"✅ 创建字段 '{field_name}' 成功: 类型 {field_type_name}, 配置 {field_config_info}")
        return True
    
    def search_records(self, app_token: str, table_id: str, page_token: Optional[str] = None,
                      page_size: int = 500) -> Tuple[List[Dict], Optional[str]]:
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
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        headers = self.auth.get_auth_headers()
        
        # 分页参数作为查询参数
        params: Dict[str, Union[int, str]] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        
        # 请求体可以包含过滤条件、排序等（当前为空）
        data = {}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            raise Exception(f"搜索记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            raise Exception(f"搜索记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
        
        result_data = result.get("data", {})
        records = result_data.get("items", [])
        next_page_token = result_data.get("page_token") if result_data.get("has_more") else None
        
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
        
        while True:
            records, page_token = self.search_records(app_token, table_id, page_token)
            all_records.extend(records)
            
            if not page_token:
                break
        
        return all_records
    
    def batch_create_records(self, app_token: str, table_id: str, records: List[Dict]) -> bool:
        """
        批量创建记录
        
        Args:
            app_token: 应用Token
            table_id: 数据表ID
            records: 记录列表
            
        Returns:
            是否创建成功
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = self.auth.get_auth_headers()
        
        # 生成唯一的client_token，并添加性能优化参数
        client_token = str(uuid.uuid4())
        params = {
            "client_token": client_token,
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id"
        }
        
        data = {"records": records}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量创建记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量创建记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"创建失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功创建 {len(records)} 条记录")
        return True
    
    def batch_update_records(self, app_token: str, table_id: str, records: List[Dict]) -> bool:
        """
        批量更新记录
        
        Args:
            app_token: 应用Token
            table_id: 数据表ID
            records: 记录列表
            
        Returns:
            是否更新成功
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = self.auth.get_auth_headers()
        
        # 添加查询参数提高性能
        params = {
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id"
        }
        
        data = {"records": records}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量更新记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量更新记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"更新失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        # 简化日志，详细信息由process_in_batches显示
        self.logger.debug(f"成功更新 {len(records)} 条记录")
        return True
    
    def batch_delete_records(self, app_token: str, table_id: str, record_ids: List[str]) -> bool:
        """
        批量删除记录
        
        Args:
            app_token: 应用Token
            table_id: 数据表ID
            record_ids: 记录ID列表
            
        Returns:
            是否删除成功
        """
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = self.auth.get_auth_headers()
        data = {"records": record_ids}
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量删除记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量删除记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
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
            23: "双向关联"
        }
        return type_mapping.get(field_type, f"未知类型({field_type})")
