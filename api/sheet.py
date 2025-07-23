#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
电子表格API模块
提供飞书电子表格的读写操作功能
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple

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
        self.ERROR_CODE_REQUEST_TOO_LARGE = 90227
    
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
                         row_batch_size: int = 500, col_batch_size: int = 80,
                         rate_limit_delay: float = 0.05,
                         start_row_offset: int = 0, start_col_offset: int = 0) -> bool:
        """
        写入电子表格数据，具备“自动二分重试”能力。
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            values: 要写入的数据（包含表头）
            row_batch_size: 初始行批次大小
            col_batch_size: 列批次大小
            rate_limit_delay: 接口调用间隔
            start_row_offset: 起始行偏移量 (0-based)
            start_col_offset: 起始列偏移量 (0-based)
            
        Returns:
            是否写入成功
        """
        if not values:
            self.logger.warning("写入数据为空")
            return True

        self.logger.info("🔄 执行写入操作 (具备自动二分重试能力)")

        data_chunks = self._create_data_chunks(values, row_batch_size, col_batch_size, start_row_offset, start_col_offset)
        total_chunks = len(data_chunks)
        
        self.logger.info(f"📦 初始数据分块完成: 共 {total_chunks} 个数据块")

        for i, chunk in enumerate(data_chunks, 1):
            self.logger.info(f"--- 开始处理初始数据块 {i}/{total_chunks} ---")
            if not self._upload_chunk_with_auto_split(spreadsheet_token, sheet_id, chunk, rate_limit_delay):
                self.logger.error(f"❌ 初始数据块 {i}/{total_chunks} (行 {chunk['start_row']}-{chunk['end_row']}) 最终上传失败")
                return False
            self.logger.info(f"--- ✅ 成功处理初始数据块 {i}/{total_chunks} ---")
            
        self.logger.info(f"🎉 写入操作全部完成: 成功处理 {total_chunks} 个初始数据块")
        return True
    
    def _write_single_batch(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> Tuple[bool, Optional[int]]:
        """
        写入单个批次数据。

        Returns:
            元组 (是否成功, 错误码)
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
            return False, None
        
        code = result.get("code")
        if code != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"写入电子表格数据失败: 错误码 {code}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False, code
        
        self.logger.debug(f"成功写入 {len(values)} 行数据")
        return True, 0
    
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
                         row_batch_size: int = 500, rate_limit_delay: float = 0.05) -> bool:
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
        data_chunks = self._create_data_chunks(values, row_batch_size, len(values[0]) if values else 0)
        total_chunks = len(data_chunks)
        
        self.logger.info(f"📦 初始数据分块完成: 共 {total_chunks} 个数据块")

        for i, chunk in enumerate(data_chunks, 1):
            self.logger.info(f"--- 开始处理初始追加块 {i}/{total_chunks} ---")
            # 注意：追加操作的range只需要指定工作表ID
            append_range = f"{sheet_id}"
            if not self._append_chunk_with_auto_split(spreadsheet_token, append_range, chunk['data'], rate_limit_delay):
                self.logger.error(f"❌ 初始追加块 {i}/{total_chunks} 最终上传失败")
                return False
            self.logger.info(f"--- ✅ 成功处理初始追加块 {i}/{total_chunks} ---")
            
        self.logger.info(f"🎉 追加操作全部完成: 成功处理 {total_chunks} 个初始数据块")
        return True
    
    def _append_single_batch(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> Tuple[bool, Optional[int]]:
        """
        追加单个批次数据。

        Returns:
            元组 (是否成功, 错误码)
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
            return False, None
        
        code = result.get("code")
        if code != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"追加电子表格数据失败: 错误码 {code}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False, code
        
        self.logger.debug(f"成功追加 {len(values)} 行数据")
        return True, 0
    
    def write_selective_columns(self, spreadsheet_token: str, sheet_id: str, 
                              column_data: Dict[str, List[Any]], 
                              column_positions: Dict[str, int],
                              start_row: int = 1,
                              rate_limit_delay: float = 0.05) -> bool:
        """
        写入选择性列数据，支持不连续列的高效批量操作
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            column_data: 字典，键为列名，值为该列的数据列表
            column_positions: 字典，键为列名，值为列位置（1-based）
            start_row: 开始行号（1-based）
            rate_limit_delay: 接口调用间隔
            
        Returns:
            是否写入成功
        """
        if not column_data:
            self.logger.warning("选择性写入数据为空")
            return True
        
        self.logger.info(f"🎯 执行选择性列写入: {list(column_data.keys())}")
        
        # 优化相邻列为连续范围
        ranges_data = self._optimize_column_ranges(column_data, column_positions, start_row)
        
        # 构建多范围数据
        value_ranges = []
        for range_info in ranges_data:
            range_str = f"{sheet_id}!{range_info['range']}"
            value_ranges.append({
                "range": range_str,
                "values": range_info['values']
            })
        
        # 使用批量更新API
        if value_ranges:
            time.sleep(rate_limit_delay)
            success, _ = self._batch_update_ranges(spreadsheet_token, value_ranges)
            if success:
                self.logger.info(f"✅ 选择性列写入成功: {len(value_ranges)} 个范围")
            else:
                self.logger.error(f"❌ 选择性列写入失败")
            return success
        
        return True
    
    def _optimize_column_ranges(self, column_data: Dict[str, List[Any]], 
                               column_positions: Dict[str, int], 
                               start_row: int,
                               max_gap: int = 2) -> List[Dict]:
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
        sorted_columns = sorted(column_data.keys(), key=lambda x: column_positions.get(x, 0))
        
        ranges_data = []
        i = 0
        
        while i < len(sorted_columns):
            range_start = i
            range_end = i
            
            # 查找可以合并的连续列
            while range_end + 1 < len(sorted_columns):
                current_pos = column_positions[sorted_columns[range_end]]
                next_pos = column_positions[sorted_columns[range_end + 1]]
                
                # 如果间隔小于等于max_gap，则合并
                if next_pos - current_pos <= max_gap:
                    range_end += 1
                else:
                    break
            
            # 构建范围数据
            start_col = column_positions[sorted_columns[range_start]]
            end_col = column_positions[sorted_columns[range_end]]
            
            start_col_letter = self._column_number_to_letter(start_col)
            end_col_letter = self._column_number_to_letter(end_col)
            
            # 计算数据行数
            max_rows = max(len(column_data[col]) for col in sorted_columns[range_start:range_end+1])
            end_row = start_row + max_rows - 1
            
            range_str = f"{start_col_letter}{start_row}:{end_col_letter}{end_row}"
            
            # 构建该范围的数据矩阵
            range_values = []
            for row_idx in range(max_rows):
                row_data = []
                for col_idx in range(start_col, end_col + 1):
                    col_letter = self._column_number_to_letter(col_idx)
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
                        # 空列（用于填充间隔）
                        row_data.append("")
                
                range_values.append(row_data)
            
            ranges_data.append({
                'range': range_str,
                'values': range_values
            })
            
            i = range_end + 1
        
        return ranges_data
    
    def clear_sheet_data(self, spreadsheet_token: str, sheet_id: str, range_str: str) -> bool:
        """
        清空电子表格指定范围的数据
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            range_str: 范围字符串，如 "A1:Z1000"
            
        Returns:
            是否清空成功
        """
        self.logger.info(f"准备清空范围: {sheet_id}!{range_str}")
        # 通过调用batch_update并传递空值数组来清空
        full_range = f"{sheet_id}!{range_str}"
        value_ranges = [{"range": full_range, "values": [[]]}]
        success, _ = self._batch_update_ranges(spreadsheet_token, value_ranges, is_clear=True)
        if success:
            self.logger.info(f"✅ 范围 {full_range} 清空成功")
        else:
            self.logger.error(f"❌ 范围 {full_range} 清空失败")
        return success
    
    def set_dropdown_validation(self, spreadsheet_token: str, range_str: str, 
                               options: List[str], multiple_values: bool = False, 
                               colors: Optional[List[str]] = None, 
                               max_rows_per_batch: int = 4000) -> bool:
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
        
        # 处理颜色配置
        if colors and len(colors) != len(valid_options):
            self.logger.warning(f"颜色数量({len(colors)})与选项数量({len(valid_options)})不匹配，将自动补齐")
            default_colors = ["#1FB6C1", "#F006C2", "#FB16C3", "#FFB6C1", "#32CD32", "#FF6347"]
            colors = [colors[i % len(colors)] if i < len(colors) else default_colors[i % len(default_colors)] 
                     for i in range(len(valid_options))]
        
        # 分块处理下拉列表设置
        self.logger.info(f"📝 开始分块设置下拉列表，批次大小: {max_rows_per_batch} 行")
        
        # 将大范围分解为小块
        range_chunks = self._split_range_into_chunks(range_str, max_rows_per_batch, 1)
        success_count = 0
        
        self.logger.info(f"📋 范围 {range_str} 分解为 {len(range_chunks)} 个块")
        
        for i, chunk in enumerate(range_chunks, 1):
            chunk_range = chunk[0]  # 每个chunk包含一个range列表
            
            self.logger.info(f"🔄 设置下拉列表批次 {i}/{len(range_chunks)}: {chunk_range}")
            
            if self._set_dropdown_single_batch(spreadsheet_token, chunk_range, valid_options, 
                                             multiple_values, colors):
                success_count += 1
                self.logger.info(f"✅ 下拉列表批次 {i} 设置成功")
            else:
                self.logger.error(f"❌ 下拉列表批次 {i} 设置失败")
                return False
            
            # 接口频率控制
            time.sleep(0.1)
        
        self.logger.info(f"🎉 下拉列表设置完成: 成功 {success_count}/{len(range_chunks)} 个批次")
        return success_count == len(range_chunks)
    
    def _set_dropdown_single_batch(self, spreadsheet_token: str, range_str: str, 
                                  options: List[str], multiple_values: bool, 
                                  colors: Optional[List[str]]) -> bool:
        """
        设置单个批次的下拉列表
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dataValidation"
        headers = self.auth.get_auth_headers()
        
        # 构建请求数据
        data_validation = {
            "conditionValues": options,
            "options": {
                "multipleValues": multiple_values,
                "highlightValidData": bool(colors),
            }
        }
        
        # 如果提供了颜色配置
        if colors:
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
                      style: Dict[str, Any], max_rows_per_batch: int = 4000, 
                      max_cols_per_batch: int = 80, adaptive_batch: bool = True) -> bool:
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
            self.logger.info(f"🚀 启用格式设置专用优化: 垂直批量 {max_rows_per_batch}行×{max_cols_per_batch}列")
        
        self.logger.info(f"🎨 开始分块设置单元格样式，批次大小: {max_rows_per_batch}行 × {max_cols_per_batch}列")
        
        success_batches = 0
        total_batches = 0
        
        for range_str in ranges:
            # 解析范围
            chunks = self._split_range_into_chunks(range_str, max_rows_per_batch, max_cols_per_batch)
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
                    self.logger.info(f"🔄 设置{detail['col_name']}列的{detail['start_row']}-{detail['end_row']}行为{style_type} (批次 {i}/{len(chunks)})")
                else:
                    self.logger.info(f"🔄 处理样式批次 {i}/{len(chunks)}: {len(chunk_ranges)} 个范围")
                
                if self._set_style_single_batch(spreadsheet_token, chunk_ranges, style):
                    success_batches += 1
                    if len(range_details) == 1:
                        detail = range_details[0]
                        self.logger.info(f"✅ {detail['col_name']}列样式设置成功")
                    else:
                        self.logger.info(f"✅ 样式批次 {i} 设置成功")
                else:
                    self.logger.error(f"❌ 样式批次 {i} 设置失败")
                    return False
                
                # 接口频率控制
                time.sleep(0.1)
        
        self.logger.info(f"🎉 样式设置完成: 成功 {success_batches}/{total_batches} 个批次")
        return success_batches == total_batches
    
    def _parse_range_for_log(self, range_str: str) -> Dict[str, Any]:
        """解析范围字符串用于日志显示"""
        import re
        match = re.match(r'([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)', range_str)
        if match:
            sheet_id, start_col, start_row, end_col, end_row = match.groups()
            return {
                'sheet_id': sheet_id,
                'col_name': start_col if start_col == end_col else f"{start_col}-{end_col}",
                'start_row': start_row,
                'end_row': end_row
            }
        return {'col_name': '未知', 'start_row': '?', 'end_row': '?'}
    
    def _get_style_type_description(self, style: Dict[str, Any]) -> str:
        """获取样式类型的中文描述"""
        if 'formatter' in style:
            formatter = style['formatter']
            if 'yyyy' in formatter.lower() or 'mm' in formatter.lower() or 'dd' in formatter.lower():
                return "日期格式"
            elif '#' in formatter or '0' in formatter:
                return "数字格式"
            else:
                return f"自定义格式({formatter})"
        elif 'fore_color' in style or 'background_color' in style:
            return "颜色样式"
        elif 'bold' in style or 'italic' in style:
            return "字体样式"
        else:
            return "样式"
    
    def _split_range_into_chunks(self, range_str: str, max_rows: int, max_cols: int) -> List[List[str]]:
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
        match = re.match(r'([^!]+)!([A-Z]+)(\d+):([A-Z]+)(\d+)', range_str)
        if not match:
            self.logger.warning(f"无法解析范围字符串: {range_str}")
            return [[range_str]]  # 返回原始范围
        
        sheet_id, start_col, start_row, end_col, end_row = match.groups()
        start_row, end_row = int(start_row), int(end_row)
        
        # 转换列字母为数字
        start_col_num = self._column_letter_to_number(start_col)
        end_col_num = self._column_letter_to_number(end_col)
        
        chunks = []
        
        # 按列分块
        for col_start in range(start_col_num, end_col_num + 1, max_cols):
            col_end = min(col_start + max_cols - 1, end_col_num)
            
            # 按行分块
            for row_start in range(start_row, end_row + 1, max_rows):
                row_end = min(row_start + max_rows - 1, end_row)
                
                # 构建块范围
                chunk_start_col = self._column_number_to_letter(col_start)
                chunk_end_col = self._column_number_to_letter(col_end)
                chunk_range = f"{sheet_id}!{chunk_start_col}{row_start}:{chunk_end_col}{row_end}"
                
                chunks.append([chunk_range])
        
        return chunks
    
    def _column_letter_to_number(self, col_letter: str) -> int:
        """将列字母转换为数字（A->1, B->2, ..., AA->27）"""
        result = 0
        for char in col_letter:
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result
    
    def _set_style_single_batch(self, spreadsheet_token: str, ranges: List[str], style: Dict[str, Any]) -> bool:
        """
        设置单个批次的样式
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/styles_batch_update"
        headers = self.auth.get_auth_headers()
        
        # 构建请求数据
        request_data = {
            "data": [
                {
                    "ranges": ranges,
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

    def _create_data_chunks(self, values: List[List[Any]], row_batch_size: int, col_batch_size: int) -> List[Dict]:
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
                    chunks.append({
                        'data': chunk_data,
                        'start_row': row_start + 1,  # 1-based indexing
                        'end_row': row_start + len(chunk_data),
                        'start_col': col_start + 1,  # 1-based indexing
                        'end_col': col_end
                    })
        
        return chunks

    def _upload_chunk_with_auto_split(self, spreadsheet_token: str, sheet_id: str, chunk: Dict, rate_limit_delay: float) -> bool:
        """
        上传单个数据块，如果因请求过大失败，则自动二分重试。
        """
        # 准备请求数据
        range_str = self._build_range_string(sheet_id, chunk['start_row'], chunk['start_col'], chunk['end_row'], chunk['end_col'])
        value_ranges = [{"range": range_str, "values": chunk['data']}]
        
        self.logger.info(f"📤 尝试上传: {len(chunk['data'])} 行 (范围 {range_str})")

        # 发起API调用
        success, error_code = self._batch_update_ranges(spreadsheet_token, value_ranges)
        
        if success:
            self.logger.info(f"✅ 上传成功: {len(chunk['data'])} 行")
            # 成功上传后进行频率控制
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)
            return True
            
        # 如果失败，检查是否是请求过大错误
        if error_code == self.ERROR_CODE_REQUEST_TOO_LARGE:
            num_rows = len(chunk['data'])
            self.logger.warning(f"检测到请求过大错误 (错误码 {error_code})，当前块包含 {num_rows} 行，将进行二分。")

            # 如果块已经小到无法再分，则视为最终失败
            if num_rows <= 1:
                self.logger.error(f"❌ 块大小已为 {num_rows} 行，无法再分割，上传失败。")
                return False

            # 将当前块分割成两个子块
            mid_point = num_rows // 2
            
            chunk1_data = chunk['data'][:mid_point]
            chunk1 = {
                'data': chunk1_data,
                'start_row': chunk['start_row'],
                'end_row': chunk['start_row'] + len(chunk1_data) - 1,
                'start_col': chunk['start_col'],
                'end_col': chunk['end_col']
            }

            chunk2_data = chunk['data'][mid_point:]
            chunk2 = {
                'data': chunk2_data,
                'start_row': chunk['start_row'] + mid_point,
                'end_row': chunk['start_row'] + mid_point + len(chunk2_data) - 1,
                'start_col': chunk['start_col'],
                'end_col': chunk['end_col']
            }
            
            # 递归上传两个子块
            self.logger.info(f" 分割为: 块1 ({len(chunk1_data)}行), 块2 ({len(chunk2_data)}行)")
            return (self._upload_chunk_with_auto_split(spreadsheet_token, sheet_id, chunk1, rate_limit_delay) and
                    self._upload_chunk_with_auto_split(spreadsheet_token, sheet_id, chunk2, rate_limit_delay))

        # 其他类型的API错误，直接判为失败
        self.logger.error(f"❌ 上传发生不可恢复的错误 (错误码: {error_code})")
        return False
    
    def _append_chunk_with_auto_split(self, spreadsheet_token: str, range_str: str, values: List[List[Any]], rate_limit_delay: float) -> bool:
        """
        追加单个数据块，如果因请求过大失败，则自动二分重试。
        """
        self.logger.info(f"📤 尝试追加: {len(values)} 行")

        success, error_code = self._append_single_batch(spreadsheet_token, range_str, values)
        
        if success:
            self.logger.info(f"✅ 追加成功: {len(values)} 行")
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)
            return True
            
        if error_code == self.ERROR_CODE_REQUEST_TOO_LARGE:
            num_rows = len(values)
            self.logger.warning(f"检测到请求过大错误 (错误码 {error_code})，当前追加块包含 {num_rows} 行，将进行二分。")

            if num_rows <= 1:
                self.logger.error(f"❌ 追加块大小已为 {num_rows} 行，无法再分割，上传失败。")
                return False

            mid_point = num_rows // 2
            chunk1 = values[:mid_point]
            chunk2 = values[mid_point:]
            
            self.logger.info(f" 分割为: 块1 ({len(chunk1)}行), 块2 ({len(chunk2)}行)")
            return (self._append_chunk_with_auto_split(spreadsheet_token, range_str, chunk1, rate_limit_delay) and
                    self._append_chunk_with_auto_split(spreadsheet_token, range_str, chunk2, rate_limit_delay))

        self.logger.error(f"❌ 追加发生不可恢复的错误 (错误码: {error_code})")
        return False

    def _batch_update_ranges(self, spreadsheet_token: str, value_ranges: List[Dict], is_clear: bool = False) -> Tuple[bool, Optional[int]]:
        """
        批量更新多个范围。

        Returns:
            元组 (是否成功, 错误码)
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update"
        headers = self.auth.get_auth_headers()
        
        data = {"valueRanges": value_ranges}
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量写入响应解析失败: {e}, HTTP状态码: {response.status_code}")
            return False, None
        
        code = result.get("code")
        if code != 0:
            # 清空操作时，允许某些“错误”，比如清空一个已经为空的区域
            if is_clear and code in [90202]: # 90202: The range is invalid
                 self.logger.warning(f"清空操作时遇到可忽略的错误 (错误码 {code}), 视为成功。")
                 return True, 0

            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量写入失败: 错误码 {code}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False, code
        
        # 记录详细的写入结果
        responses = result.get("data", {}).get("responses", [])
        total_cells = sum(resp.get("updatedCells", 0) for resp in responses)
        self.logger.debug(f"批量写入成功: {len(responses)} 个范围, 共 {total_cells} 个单元格")
        
        return True, 0
    
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