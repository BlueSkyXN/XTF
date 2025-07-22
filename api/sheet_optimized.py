#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化的电子表格API模块
针对4种同步模式设计最优接口选择策略
"""

import logging
import time
from typing import Dict, Any, List, Optional, Tuple

from .auth import FeishuAuth
from .base import RetryableAPIClient
from core.config import SyncMode


class OptimizedSheetAPI:
    """优化的飞书电子表格API客户端"""
    
    def __init__(self, auth: FeishuAuth, api_client: Optional[RetryableAPIClient] = None):
        """初始化优化的电子表格API客户端"""
        self.auth = auth
        self.api_client = api_client or auth.api_client
        self.logger = logging.getLogger(__name__)
        
        # API限制常量
        self.MAX_ROWS_PER_CALL = 5000
        self.MAX_COLS_PER_CALL = 100
        self.MAX_BATCH_RANGES = 10  # batch_update最大范围数（经验值）
        self.ERROR_CODE_REQUEST_TOO_LARGE = 90227
        
    def sync_data(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]],
                  sync_mode: SyncMode, row_batch_size: int = 500,
                  col_batch_size: int = 80, rate_limit_delay: float = 0.3) -> bool:
        """
        智能同步数据 - 根据同步模式选择最优API策略
        
        Args:
            spreadsheet_token: 电子表格Token
            sheet_id: 工作表ID
            values: 要同步的数据（第一行为表头）
            sync_mode: 同步模式
            row_batch_size: 行批次大小
            col_batch_size: 列批次大小
            rate_limit_delay: 接口调用间隔
            
        Returns:
            是否同步成功
        """
        if not values:
            self.logger.warning("同步数据为空")
            return True
            
        total_rows = len(values)
        total_cols = len(values[0]) if values else 0
        
        self.logger.info(f"🚀 开始智能同步: {total_rows} 行 × {total_cols} 列")
        self.logger.info(f"📋 同步模式: {sync_mode.value}")
        self.logger.info(f"⚙️  初始批次配置: {row_batch_size} 行/批 × {col_batch_size} 列/批")
        
        # 根据同步模式选择最优策略
        if sync_mode == SyncMode.CLONE:
            return self._sync_clone_optimized(spreadsheet_token, sheet_id, values,
                                            row_batch_size, col_batch_size, rate_limit_delay)
        elif sync_mode == SyncMode.INCREMENTAL:
            return self._sync_incremental_optimized(spreadsheet_token, sheet_id, values, 
                                                  row_batch_size, col_batch_size, rate_limit_delay)
        elif sync_mode == SyncMode.OVERWRITE:
            return self._sync_overwrite_optimized(spreadsheet_token, sheet_id, values, 
                                                row_batch_size, col_batch_size, rate_limit_delay)
        else:  # FULL
            return self._sync_full_optimized(spreadsheet_token, sheet_id, values, 
                                           row_batch_size, col_batch_size, rate_limit_delay)
    
    def _sync_clone_optimized(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]],
                             row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        克隆同步优化策略：
        1. 根据初始配置创建数据块。
        2. 逐个上传数据块。
        3. 如果遇到“请求过大”的错误，则自动将该块对半分割并递归重试。
        """
        self.logger.info("🔄 执行克隆同步优化策略 (具备自动二分重试能力)")
        
        data_chunks = self._create_data_chunks(values, row_batch_size, col_batch_size)
        total_chunks = len(data_chunks)
        
        self.logger.info(f"📦 初始数据分块完成: 共 {total_chunks} 个数据块")

        for i, chunk in enumerate(data_chunks, 1):
            self.logger.info(f"--- 开始处理初始数据块 {i}/{total_chunks} ---")
            if not self._upload_chunk_with_auto_split(spreadsheet_token, sheet_id, chunk, rate_limit_delay):
                self.logger.error(f"❌ 初始数据块 {i}/{total_chunks} (行 {chunk['start_row']}-{chunk['end_row']}) 最终上传失败")
                return False
            self.logger.info(f"--- ✅ 成功处理初始数据块 {i}/{total_chunks} ---")
            
        self.logger.info(f"🎉 克隆同步全部完成: 成功处理 {total_chunks} 个初始数据块")
        return True
    
    def _sync_incremental_optimized(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                                   row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        增量同步优化策略：使用 append 接口的 INSERT_ROWS 模式
        优势：自动插入行，确保不覆盖现有数据
        """
        self.logger.info("➕ 执行增量同步优化策略 (使用 append + INSERT_ROWS)")
        
        # 去掉表头，只追加数据行
        data_rows = values[1:] if len(values) > 1 else []
        if not data_rows:
            self.logger.info("无数据行需要追加")
            return True
        
        return self._append_data_with_insert_rows(spreadsheet_token, sheet_id, data_rows, 
                                                row_batch_size, col_batch_size, rate_limit_delay)
    
    def _sync_overwrite_optimized(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                                 row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        覆盖同步优化策略：使用 PUT values 直接覆盖
        优势：简单直接，精确控制覆盖范围
        """
        self.logger.info("🔄 执行覆盖同步优化策略 (使用 PUT values)")
        
        return self._write_data_direct_overwrite(spreadsheet_token, sheet_id, values, 
                                               row_batch_size, col_batch_size, rate_limit_delay)
    
    def _sync_full_optimized(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                            row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        全量同步优化策略：混合使用 PUT values + append
        优势：更新现有数据精确，追加新数据高效
        """
        self.logger.info("🔄 执行全量同步优化策略 (混合 PUT + append)")
        
        # 这里需要现有数据对比逻辑，暂时使用克隆同步策略
        return self._sync_clone_optimized(spreadsheet_token, sheet_id, values, 
                                        row_batch_size, col_batch_size, rate_limit_delay)
    
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

    def _build_range_string(self, sheet_id: str, start_row: int, start_col: int, end_row: int, end_col: int) -> str:
        """构建范围字符串"""
        start_col_letter = self._column_number_to_letter(start_col)
        end_col_letter = self._column_number_to_letter(end_col)
        return f"{sheet_id}!{start_col_letter}{start_row}:{end_col_letter}{end_row}"
    
    def _column_number_to_letter(self, col_num: int) -> str:
        """将列号转换为字母"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result or "A"
    
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

    def _batch_update_ranges(self, spreadsheet_token: str, value_ranges: List[Dict]) -> Tuple[bool, Optional[int]]:
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
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量写入失败: 错误码 {code}, 错误信息: {error_msg}")
            self.logger.debug(f"API响应: {result}")
            return False, code
        
        # 记录详细的写入结果
        responses = result.get("data", {}).get("responses", [])
        total_cells = sum(resp.get("updatedCells", 0) for resp in responses)
        self.logger.debug(f"批量写入成功: {len(responses)} 个范围, 共 {total_cells} 个单元格")
        
        return True, 0
    
    def _append_data_with_insert_rows(self, spreadsheet_token: str, sheet_id: str, data_rows: List[List[Any]], 
                                     row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        使用INSERT_ROWS模式追加数据
        """
        total_rows = len(data_rows)
        total_cols = len(data_rows[0]) if data_rows else 0
        
        success_count = 0
        batch_count = 0
        
        # 按列分块
        for col_start in range(0, total_cols, col_batch_size):
            col_end = min(col_start + col_batch_size, total_cols)
            
            # 按行分块
            for row_start in range(0, total_rows, row_batch_size):
                row_end = min(row_start + row_batch_size, total_rows)
                
                # 提取批次数据
                batch_data = []
                for row_idx in range(row_start, row_end):
                    chunk_row = data_rows[row_idx][col_start:col_end]
                    while len(chunk_row) < (col_end - col_start):
                        chunk_row.append("")
                    batch_data.append(chunk_row)
                
                if not batch_data:
                    continue
                
                batch_count += 1
                
                # 构建追加范围
                start_col_letter = self._column_number_to_letter(col_start + 1)
                end_col_letter = self._column_number_to_letter(col_end)
                append_range = f"{sheet_id}!{start_col_letter}:{end_col_letter}"
                
                self.logger.info(f"📤 追加批次 {batch_count}: 行 {row_start + 1}-{row_end}, "
                               f"列 {start_col_letter}-{end_col_letter} ({len(batch_data)} 行)")
                
                if self._append_single_batch_with_insert(spreadsheet_token, append_range, batch_data):
                    success_count += 1
                    self.logger.info(f"✅ 批次 {batch_count} 追加成功")
                else:
                    self.logger.error(f"❌ 批次 {batch_count} 追加失败")
                    return False
                
                # 频率控制
                if rate_limit_delay > 0:
                    time.sleep(rate_limit_delay)
        
        self.logger.info(f"🎉 增量同步完成: 成功追加 {success_count}/{batch_count} 个批次")
        return success_count == batch_count
    
    def _append_single_batch_with_insert(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        使用INSERT_ROWS模式追加单个批次
        """
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append?insertDataOption=INSERT_ROWS"
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
            self.logger.error(f"追加数据响应解析失败: {e}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"追加数据失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            return False
        
        return True
    
    def _write_data_direct_overwrite(self, spreadsheet_token: str, sheet_id: str, values: List[List[Any]], 
                                    row_batch_size: int, col_batch_size: int, rate_limit_delay: float) -> bool:
        """
        直接覆盖写入数据
        """
        # 使用克隆策略的分块逻辑，但用PUT接口
        data_chunks = self._create_data_chunks(values, row_batch_size, col_batch_size)
        total_chunks = len(data_chunks)
        
        success_count = 0
        
        for i, chunk in enumerate(data_chunks, 1):
            range_str = self._build_range_string(sheet_id, chunk['start_row'], 
                                               chunk['start_col'], chunk['end_row'], chunk['end_col'])
            
            self.logger.info(f"📤 覆盖批次 {i}: 行 {chunk['start_row']}-{chunk['end_row']}, "
                           f"列 {self._column_number_to_letter(chunk['start_col'])}-{self._column_number_to_letter(chunk['end_col'])} "
                           f"({len(chunk['data'])} 行)")
            
            if self._write_single_range(spreadsheet_token, range_str, chunk['data']):
                success_count += 1
                self.logger.info(f"✅ 批次 {i} 覆盖成功")
            else:
                self.logger.error(f"❌ 批次 {i} 覆盖失败")
                return False
            
            # 频率控制
            if rate_limit_delay > 0:
                time.sleep(rate_limit_delay)
        
        self.logger.info(f"🎉 覆盖同步完成: 成功覆盖 {success_count}/{total_chunks} 个批次")
        return success_count == total_chunks
    
    def _write_single_range(self, spreadsheet_token: str, range_str: str, values: List[List[Any]]) -> bool:
        """
        写入单个范围
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
            self.logger.error(f"写入数据响应解析失败: {e}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"写入数据失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            return False
        
        return True