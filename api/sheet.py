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
    
    def set_dropdown_validation(self, spreadsheet_token: str, range_str: str, 
                               options: List[str], multiple_values: bool = False, 
                               colors: Optional[List[str]] = None) -> bool:
        """
        为电子表格指定区域设置下拉列表数据校验
        
        Args:
            spreadsheet_token: 电子表格Token
            range_str: 范围字符串，如 "Sheet1!A1:A100"
            options: 下拉列表选项值列表
            multiple_values: 是否支持多选，默认False
            colors: 选项颜色列表，需要与options一一对应
            
        Returns:
            是否设置成功
        """
        if not options:
            self.logger.warning("下拉列表选项为空，跳过设置")
            return True
            
        # 验证选项数量
        if len(options) > 500:
            self.logger.warning(f"下拉列表选项过多({len(options)})，将截取前500个")
            options = options[:500]
        
        # 验证选项值
        valid_options = []
        for option in options:
            option_str = str(option)
            if ',' in option_str:
                self.logger.warning(f"选项值包含逗号，将被跳过: {option_str}")
                continue
            if len(option_str.encode('utf-8')) > 100:
                self.logger.warning(f"选项值过长，将被截取: {option_str[:20]}...")
                option_str = option_str[:50]  # 保守截取
            valid_options.append(option_str)
        
        if not valid_options:
            self.logger.warning("没有有效的下拉列表选项")
            return False
        
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dataValidation"
        headers = self.auth.get_auth_headers()
        
        # 构建请求数据
        data_validation = {
            "conditionValues": valid_options,
            "options": {
                "multipleValues": multiple_values,
                "highlightValidData": bool(colors),
            }
        }
        
        # 如果提供了颜色配置
        if colors:
            if len(colors) != len(valid_options):
                self.logger.warning(f"颜色数量({len(colors)})与选项数量({len(valid_options)})不匹配，将自动补齐")
                # 循环使用颜色或使用默认颜色
                default_colors = ["#1FB6C1", "#F006C2", "#FB16C3", "#FFB6C1", "#32CD32", "#FF6347"]
                colors = [colors[i % len(colors)] if i < len(colors) else default_colors[i % len(default_colors)] 
                         for i in range(len(valid_options))]
            data_validation["options"]["colors"] = colors
        
        request_data = {
            "range": range_str,
            "dataValidationType": "list",
            "dataValidation": data_validation
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=request_data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"设置下拉列表响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"设置下拉列表失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"请求数据: {request_data}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.info(f"成功为范围 {range_str} 设置下拉列表，选项数量: {len(valid_options)}")
        return True
    
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
                f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}",
                headers=self.auth.get_auth_headers()
            )
            
            result = test_response.json()
            
            # 如果返回错误码90202，说明范围超出网格限制
            if result.get("code") == 90202:
                self.logger.debug(f"范围 {range_str} 超出网格限制")
                return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"范围验证失败: {e}")
            # 验证失败时保守返回False，避免后续API调用失败
            return False
    
    def set_cell_style(self, spreadsheet_token: str, ranges: List[str], 
                      style: Dict[str, Any]) -> bool:
        """
        批量设置单元格样式
        
        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表，如 ["Sheet1!A1:A10", "Sheet1!B1:B10"]
            style: 样式配置字典
            
        Returns:
            是否设置成功
        """
        if not ranges:
            self.logger.warning("样式设置范围为空，跳过设置")
            return True
        
        # 验证范围是否在网格限制内
        valid_ranges = []
        for range_str in ranges:
            if self._validate_range_size(spreadsheet_token, range_str):
                valid_ranges.append(range_str)
            else:
                self.logger.warning(f"范围 {range_str} 超出网格限制，跳过设置")
        
        if not valid_ranges:
            self.logger.warning("所有范围都超出网格限制，跳过样式设置")
            return True
        
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/styles_batch_update"
        headers = self.auth.get_auth_headers()
        
        # 构建请求数据
        request_data = {
            "data": [
                {
                    "ranges": valid_ranges,
                    "style": style
                }
            ]
        }
        
        response = self.api_client.call_api("PUT", url, headers=headers, json=request_data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"设置单元格样式响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"设置单元格样式失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            self.logger.debug(f"请求数据: {request_data}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.info(f"成功为 {len(valid_ranges)} 个范围设置单元格样式 (原计划 {len(ranges)} 个)")
        return True
    
    def set_date_format(self, spreadsheet_token: str, ranges: List[str], 
                       date_format: str = "yyyy/MM/dd") -> bool:
        """
        为指定范围设置日期格式
        
        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表
            date_format: 日期格式，默认为 "yyyy/MM/dd"
            
        Returns:
            是否设置成功
        """
        style = {
            "formatter": date_format
        }
        
        return self.set_cell_style(spreadsheet_token, ranges, style)
    
    def set_number_format(self, spreadsheet_token: str, ranges: List[str], 
                         number_format: str = "#,##0.00") -> bool:
        """
        为指定范围设置数字格式
        
        Args:
            spreadsheet_token: 电子表格Token
            ranges: 范围列表
            number_format: 数字格式，默认为 "#,##0.00"
            
        Returns:
            是否设置成功
        """
        style = {
            "formatter": number_format
        }
        
        return self.set_cell_style(spreadsheet_token, ranges, style)