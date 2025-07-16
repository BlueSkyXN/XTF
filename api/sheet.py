#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电子表格API模块
提供飞书电子表格的读写操作功能
"""

import logging
from typing import Dict, Any, List, Optional

from .auth import FeishuAuth
from .base import RetryableAPIClient


class SheetAPI:
    """飞书电子表格API客户端"""
    
    def __init__(self, auth: FeishuAuth, api_client: Optional[RetryableAPIClient] = None):
        """
        初始化电子表格API客户端
        
        Args:
            auth: 飞书认证管理器
            api_client: API客户端实例
        """
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.logger = logging.getLogger(__name__)
    
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
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}"
        headers = self.auth.get_auth_headers()
        
        response = self.api_client.call_api("GET", url, headers=headers)
        
        try:
            result = response.json()
        except ValueError as e:
            raise Exception(f"获取电子表格信息响应解析失败: {e}, HTTP状态码: {response.status_code}")
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            raise Exception(f"获取电子表格信息失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
        
        return result.get("data", {})
    
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
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}"
        headers = self.auth.get_auth_headers()
        
        response = self.api_client.call_api("GET", url, headers=headers)
        
        try:
            result = response.json()
        except ValueError as e:
            raise Exception(f"读取电子表格数据响应解析失败: {e}, HTTP状态码: {response.status_code}")
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            raise Exception(f"读取电子表格数据失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
        
        data = result.get("data", {})
        value_range = data.get("valueRange", {})
        return value_range.get("values", [])
    
    def write_sheet_data(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        写入电子表格数据
        
        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串
            values: 要写入的数据
            
        Returns:
            是否写入成功
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        headers = self.auth.get_auth_headers()
        
        data = {
            "valueRange": {
                "range": range_str,
                "values": values
            }
        }
        
        response = self.api_client.call_api("PUT", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"写入电子表格数据响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"写入电子表格数据失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.debug(f"成功写入 {len(values)} 行数据")
        return True
    
    def append_sheet_data(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        追加电子表格数据
        
        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串
            values: 要追加的数据
            
        Returns:
            是否追加成功
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append"
        headers = self.auth.get_auth_headers()
        
        data = {
            "valueRange": {
                "range": range_str,
                "values": values
            }
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"追加电子表格数据响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"追加电子表格数据失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.debug(f"成功追加 {len(values)} 行数据")
        return True
    
    def clear_sheet_data(self, spreadsheet_token: str, range_str: str) -> bool:
        """
        清空电子表格数据
        
        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串
            
        Returns:
            是否清空成功
        """
        # 通过写入空数据来清空
        return self.write_sheet_data(spreadsheet_token, range_str, [[]])