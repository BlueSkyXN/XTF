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
    
    def write_sheet_data(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                        row_batch_size: int = 500, col_batch_size: int = 80) -> bool:
        """
        二维分块写入电子表格数据（扫描算法）
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            values: 要写入的数据，第一行为表头
            row_batch_size: 行批次大小，默认500（用户配置的batch_size）
            col_batch_size: 列批次大小，默认80（安全限制，API限制100列）
            
        Returns:
            是否写入成功
        """
        if not values:
            self.logger.warning("写入数据为空")
            return True
        
        if not values[0]:  # 检查第一行是否为空
            self.logger.warning("数据表头为空")
            return True
        
        headers = values[0]
        data_rows = values[1:] if len(values) > 1 else []
        total_rows = len(data_rows)
        total_cols = len(headers)
        
        self.logger.info(f"开始二维分块上传: {total_rows} 行 × {total_cols} 列")
        self.logger.info(f"分块策略: {row_batch_size} 行/批 × {col_batch_size} 列/批")
        
        # 检查是否需要分块
        need_row_chunking = total_rows > row_batch_size
        need_col_chunking = total_cols > col_batch_size
        
        if not need_row_chunking and not need_col_chunking:
            # 数据量小，直接写入
            range_str = self._build_range_string(sheet_id, 1, 1, len(values), total_cols)
            return self._write_single_batch(spreadsheet_token, range_str, values)
        
        # 计算分块数量
        row_chunks = (total_rows + row_batch_size - 1) // row_batch_size if need_row_chunking else 1
        col_chunks = (total_cols + col_batch_size - 1) // col_batch_size if need_col_chunking else 1
        
        self.logger.info(f"总计划块数: {row_chunks} 行块 × {col_chunks} 列块 = {row_chunks * col_chunks} 个数据块")
        
        total_blocks = 0
        success_blocks = 0
        
        # 按列扫描（外层循环）
        for col_chunk_idx in range(col_chunks):
            col_start = col_chunk_idx * col_batch_size
            col_end = min(col_start + col_batch_size, total_cols)
            
            chunk_headers = headers[col_start:col_end]
            
            self.logger.debug(f"处理列块 {col_chunk_idx + 1}/{col_chunks}: 列 {col_start + 1}-{col_end}")
            
            # 写入当前列块的表头
            start_col_letter = self._column_number_to_letter(col_start + 1)
            end_col_letter = self._column_number_to_letter(col_end)
            header_range = f"{sheet_id}!{start_col_letter}1:{end_col_letter}1"
            
            if not self._write_single_batch(spreadsheet_token, header_range, [chunk_headers]):
                self.logger.error(f"列块 {col_chunk_idx + 1} 表头写入失败")
                return False
            
            # 按行扫描当前列块（内层循环）
            for row_chunk_idx in range(row_chunks):
                row_start = row_chunk_idx * row_batch_size
                row_end = min(row_start + row_batch_size, total_rows)
                
                if row_start >= total_rows:
                    break
                
                # 提取当前块的数据
                chunk_data = []
                for row_idx in range(row_start, row_end):
                    if row_idx < len(data_rows):
                        chunk_row = data_rows[row_idx][col_start:col_end]
                        # 确保行长度与表头一致
                        while len(chunk_row) < len(chunk_headers):
                            chunk_row.append("")
                        chunk_data.append(chunk_row[:len(chunk_headers)])
                
                if not chunk_data:
                    continue
                
                # 计算写入范围
                data_start_row = row_start + 2  # +1 for 1-based, +1 for header
                data_end_row = data_start_row + len(chunk_data) - 1
                data_range = f"{sheet_id}!{start_col_letter}{data_start_row}:{end_col_letter}{data_end_row}"
                
                self.logger.debug(f"写入数据块 [{row_chunk_idx + 1},{col_chunk_idx + 1}]: "
                                f"行 {data_start_row}-{data_end_row}, 列 {start_col_letter}-{end_col_letter} "
                                f"({len(chunk_data)} 行 × {len(chunk_headers)} 列)")
                
                total_blocks += 1
                if self._write_single_batch(spreadsheet_token, data_range, chunk_data):
                    success_blocks += 1
                else:
                    self.logger.error(f"数据块 [{row_chunk_idx + 1},{col_chunk_idx + 1}] 写入失败")
                    return False
        
        self.logger.info(f"二维分块上传完成: 成功 {success_blocks}/{total_blocks} 个数据块")
        return success_blocks == total_blocks
    
    def _write_single_batch(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        写入单个批次数据
        
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
    
    def _column_number_to_letter(self, col_num: int) -> str:
        """将列号转换为字母（1->A, 2->B, ..., 26->Z, 27->AA）"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result or "A"
    
    def _build_range_string(self, sheet_id: str, start_row: int, start_col: int, end_row: int, end_col: int) -> str:
        """构建范围字符串"""
        start_col_letter = self._column_number_to_letter(start_col)
        end_col_letter = self._column_number_to_letter(end_col)
        return f"{sheet_id}!{start_col_letter}{start_row}:{end_col_letter}{end_row}"
    
    def _get_end_column_from_range(self, range_str: str) -> str:
        """
        从范围字符串中提取结束列字母
        
        Args:
            range_str: 范围字符串，如 "Sheet1!A1:AK94277"
            
        Returns:
            结束列字母，如 "AK"
        """
        if ':' not in range_str:
            return "A"
        
        end_part = range_str.split(':')[1]
        # 提取字母部分
        import re
        match = re.match(r'([A-Z]+)', end_part)
        return match.group(1) if match else "A"
    
    def append_sheet_data(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                         row_batch_size: int = 500, col_batch_size: int = 80) -> bool:
        """
        分批追加电子表格数据
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID (不再使用range_str，改为动态计算追加位置)
            values: 要追加的数据
            row_batch_size: 行批次大小
            col_batch_size: 列批次大小
            
        Returns:
            是否追加成功
        """
        if not values:
            self.logger.warning("追加数据为空")
            return True
        
        total_rows = len(values)
        total_cols = len(values[0]) if values else 0
        
        self.logger.info(f"开始分批追加数据: {total_rows} 行 × {total_cols} 列")
        self.logger.info(f"追加策略: {row_batch_size} 行/批 × {col_batch_size} 列/批")
        
        # 如果数据量小，直接追加
        if total_rows <= row_batch_size and total_cols <= col_batch_size:
            return self._append_single_batch(spreadsheet_token, f"{sheet_id}!A:A", values)
        
        # 分批追加
        success_count = 0
        total_batches = 0
        
        # 按列分块
        col_chunks = (total_cols + col_batch_size - 1) // col_batch_size if total_cols > col_batch_size else 1
        
        for col_chunk_idx in range(col_chunks):
            col_start = col_chunk_idx * col_batch_size
            col_end = min(col_start + col_batch_size, total_cols)
            
            # 提取当前列块的数据
            chunk_values = []
            for row in values:
                chunk_row = row[col_start:col_end]
                # 确保行长度一致
                while len(chunk_row) < (col_end - col_start):
                    chunk_row.append("")
                chunk_values.append(chunk_row[:col_end - col_start])
            
            self.logger.debug(f"处理列块 {col_chunk_idx + 1}/{col_chunks}: 列 {col_start + 1}-{col_end}")
            
            # 按行分批追加当前列块
            for row_start in range(0, len(chunk_values), row_batch_size):
                row_end = min(row_start + row_batch_size, len(chunk_values))
                batch_data = chunk_values[row_start:row_end]
                
                if not batch_data:
                    continue
                
                # 构建追加范围（让系统自动确定追加位置）
                start_col_letter = self._column_number_to_letter(col_start + 1)
                end_col_letter = self._column_number_to_letter(col_end)
                append_range = f"{sheet_id}!{start_col_letter}:{end_col_letter}"
                
                batch_num = total_batches + 1
                self.logger.info(f"追加批次 {batch_num}: 行 {row_start + 1}-{row_end}, 列 {start_col_letter}-{end_col_letter} ({len(batch_data)} 行)")
                
                total_batches += 1
                if self._append_single_batch(spreadsheet_token, append_range, batch_data):
                    success_count += 1
                else:
                    self.logger.error(f"追加批次 {batch_num} 失败")
                    return False
        
        self.logger.info(f"分批追加完成: 成功 {success_count}/{total_batches} 个批次")
        return success_count == total_batches
    
    def _append_single_batch(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        追加单个批次数据
        
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