#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF_Sheet (Excel To Feishu Sheet) - 本地表格同步到飞书电子表格工具
支持四种同步模式：全量、增量、覆盖、克隆
针对飞书电子表格API优化的企业级数据同步工具
"""

import pandas as pd
import requests
import yaml
import time
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from enum import Enum
import sys
import hashlib


class SyncMode(Enum):
    """同步模式枚举"""
    FULL = "full"          # 全量同步：已存在的更新，不存在的新增
    INCREMENTAL = "incremental"  # 增量同步：只新增不存在的记录
    OVERWRITE = "overwrite"     # 覆盖同步：删除已存在的，然后新增全部
    CLONE = "clone"             # 克隆同步：清空全部，然后新增全部


@dataclass
class SyncConfig:
    """同步配置"""
    # 基础配置
    file_path: str
    app_id: str
    app_secret: str
    
    # 电子表格配置
    spreadsheet_token: str
    sheet_id: str
    start_row: int = 1  # 开始行号（1-based）
    start_column: str = "A"  # 开始列号
    
    # 同步设置
    sync_mode: SyncMode = SyncMode.FULL
    index_column: Optional[str] = None  # 索引列名，用于记录比对
    
    # 性能设置
    batch_size: int = 1000  # 批处理大小
    rate_limit_delay: float = 0.1  # 接口调用间隔
    max_retries: int = 3  # 最大重试次数
    
    # 日志设置
    log_level: str = "INFO"
    
    def __post_init__(self):
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode(self.sync_mode)


class RateLimiter:
    """接口频率限制器"""
    def __init__(self, delay: float = 0.1):
        self.delay = delay
        self.last_call = 0
    
    def wait(self):
        """等待以遵守频率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_call
        if time_since_last < self.delay:
            time.sleep(self.delay - time_since_last)
        self.last_call = time.time()


class RetryableAPIClient:
    """可重试的API客户端"""
    def __init__(self, max_retries: int = 3, rate_limiter: Optional[RateLimiter] = None):
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter()
        self.logger = logging.getLogger(__name__)
    
    def call_api(self, method: str, url: str, **kwargs) -> requests.Response:
        """调用API并处理重试"""
        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.wait()
                
                response = requests.request(method, url, timeout=60, **kwargs)
                
                # 检查是否需要重试
                if response.status_code == 429:  # 频率限制
                    if attempt < self.max_retries:
                        wait_time = 2 ** attempt  # 指数退避
                        self.logger.warning(f"频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                
                if response.status_code >= 500:  # 服务器错误
                    if attempt < self.max_retries:
                        wait_time = 2 ** attempt
                        self.logger.warning(f"服务器错误 {response.status_code}，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"请求异常 {e}，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                raise
        
        raise Exception(f"API调用失败，已重试 {self.max_retries} 次")


class FeishuSheetAPIClient:
    """飞书电子表格API客户端"""
    def __init__(self, config: SyncConfig):
        self.config = config
        self.tenant_access_token = None
        self.token_expires_at = None
        self.api_client = RetryableAPIClient(
            max_retries=config.max_retries,
            rate_limiter=RateLimiter(config.rate_limit_delay)
        )
        self.logger = logging.getLogger(__name__)
    
    def get_tenant_access_token(self) -> str:
        """获取租户访问令牌"""
        # 检查token是否过期
        if (self.tenant_access_token and self.token_expires_at and 
            datetime.now() < self.token_expires_at - timedelta(minutes=5)):
            return self.tenant_access_token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {
            "app_id": self.config.app_id,
            "app_secret": self.config.app_secret
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            raise Exception(f"获取访问令牌响应解析失败: {e}, HTTP状态码: {response.status_code}")
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            raise Exception(f"获取访问令牌失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
        
        self.tenant_access_token = result["tenant_access_token"]
        # 设置过期时间（提前5分钟刷新）
        expires_in = result.get("expire", 7200)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        self.logger.info("成功获取租户访问令牌")
        return self.tenant_access_token
    
    def get_auth_headers(self) -> Dict[str, str]:
        """获取认证头"""
        token = self.get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
    
    def get_sheet_info(self, spreadsheet_token: str) -> Dict[str, Any]:
        """获取电子表格信息"""
        url = f"https://open.feishu.cn/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}"
        headers = self.get_auth_headers()
        
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
        """读取电子表格数据"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_str}"
        headers = self.get_auth_headers()
        
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
        """写入电子表格数据"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values"
        headers = self.get_auth_headers()
        
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
        """追加电子表格数据"""
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_append"
        headers = self.get_auth_headers()
        
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
        """清空电子表格数据"""
        # 通过写入空数据来清空
        return self.write_sheet_data(spreadsheet_token, range_str, [[]])


class XTFSheetSyncEngine:
    """XTF电子表格同步引擎"""
    
    def __init__(self, config: SyncConfig):
        """初始化同步引擎"""
        self.config = config
        self.api_client = FeishuSheetAPIClient(config)
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # 添加转换统计
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
    
    def setup_logging(self):
        """设置日志"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"xtf_sheet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        # 清除已有的处理器
        logging.getLogger().handlers.clear()
        
        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
    
    def column_number_to_letter(self, col_num: int) -> str:
        """将列号转换为字母（1->A, 2->B, ..., 26->Z, 27->AA）"""
        result = ""
        while col_num > 0:
            col_num -= 1
            result = chr(65 + col_num % 26) + result
            col_num //= 26
        return result
    
    def column_letter_to_number(self, col_letter: str) -> int:
        """将列字母转换为数字（A->1, B->2, ..., Z->26, AA->27）"""
        result = 0
        for char in col_letter:
            result = result * 26 + (ord(char) - ord('A') + 1)
        return result
    
    def get_range_string(self, start_row: int, start_col: str, end_row: int, end_col: str) -> str:
        """生成范围字符串"""
        return f"{self.config.sheet_id}!{start_col}{start_row}:{end_col}{end_row}"
    
    def df_to_values(self, df: pd.DataFrame, include_headers: bool = True) -> List[List[Any]]:
        """将DataFrame转换为电子表格值格式"""
        values = []
        
        # 添加表头
        if include_headers:
            values.append(df.columns.tolist())
        
        # 添加数据行
        for _, row in df.iterrows():
            row_values = []
            for value in row:
                if pd.isnull(value):
                    row_values.append("")
                else:
                    # 转换为字符串或基本类型
                    if isinstance(value, (int, float)):
                        row_values.append(value)
                    elif isinstance(value, bool):
                        row_values.append(value)
                    else:
                        row_values.append(str(value))
            values.append(row_values)
        
        return values
    
    def values_to_df(self, values: List[List[Any]]) -> pd.DataFrame:
        """将电子表格值格式转换为DataFrame"""
        if not values:
            return pd.DataFrame()
        
        # 第一行作为表头
        headers = values[0] if values else []
        data_rows = values[1:] if len(values) > 1 else []
        
        # 创建DataFrame
        if data_rows:
            df = pd.DataFrame(data_rows, columns=headers)
        else:
            df = pd.DataFrame(columns=headers)
        
        return df
    
    def get_index_value_hash(self, row: pd.Series) -> Optional[str]:
        """计算索引值的哈希"""
        if self.config.index_column and self.config.index_column in row:
            value = str(row[self.config.index_column])
            return hashlib.md5(value.encode('utf-8')).hexdigest()
        return None
    
    def build_data_index(self, df: pd.DataFrame) -> Dict[str, int]:
        """构建数据索引（哈希 -> 行号）"""
        index = {}
        if not self.config.index_column:
            return index
        
        for idx, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            if index_hash:
                index[index_hash] = idx
        
        return index
    
    def get_current_sheet_data(self) -> pd.DataFrame:
        """获取当前电子表格数据"""
        # 先获取一个较大的范围来确定实际数据范围
        range_str = f"{self.config.sheet_id}!A1:ZZ10000"
        
        try:
            values = self.api_client.get_sheet_data(self.config.spreadsheet_token, range_str)
            return self.values_to_df(values)
        except Exception as e:
            self.logger.warning(f"获取当前电子表格数据失败: {e}")
            return pd.DataFrame()
    
    def sync_full(self, df: pd.DataFrame) -> bool:
        """全量同步：更新存在的，新增不存在的"""
        self.logger.info("开始全量同步...")
        
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行完全覆盖操作")
            return self.sync_clone(df)
        
        # 获取现有数据
        current_df = self.get_current_sheet_data()
        
        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)
        
        # 构建索引
        current_index = self.build_data_index(current_df)
        
        # 分类数据
        update_rows = []
        new_rows = []
        
        for _, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            if index_hash and index_hash in current_index:
                # 更新现有行
                current_row_idx = current_index[index_hash]
                update_rows.append((current_row_idx, row))
            else:
                # 新增行
                new_rows.append(row)
        
        self.logger.info(f"全量同步计划: 更新 {len(update_rows)} 行，新增 {len(new_rows)} 行")
        
        # 执行更新
        success = True
        if update_rows:
            # 更新现有行
            updated_df = current_df.copy()
            for current_row_idx, new_row in update_rows:
                for col in df.columns:
                    if col in updated_df.columns:
                        updated_df.iloc[current_row_idx][col] = new_row[col]
            
            # 写入更新后的数据
            values = self.df_to_values(updated_df)
            end_col = self.column_number_to_letter(len(updated_df.columns))
            range_str = self.get_range_string(1, "A", len(values), end_col)
            success = self.api_client.write_sheet_data(self.config.spreadsheet_token, range_str, values)
        
        # 追加新行
        if new_rows and success:
            new_df = pd.DataFrame(new_rows)
            new_values = self.df_to_values(new_df, include_headers=False)
            
            if new_values:
                # 计算追加的起始行
                start_row = len(current_df) + 2  # +1 for header, +1 for next row
                end_col_letter = self.column_number_to_letter(len(df.columns))
                range_str = self.get_range_string(start_row, "A", start_row + len(new_values) - 1, end_col_letter)
                success = self.api_client.append_sheet_data(self.config.spreadsheet_token, range_str, new_values)
        
        return success
    
    def sync_incremental(self, df: pd.DataFrame) -> bool:
        """增量同步：只新增不存在的记录"""
        self.logger.info("开始增量同步...")
        
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将新增全部数据")
            # 追加所有数据
            values = self.df_to_values(df)
            range_str = f"{self.config.sheet_id}!A:A"  # 让系统自动确定追加位置
            return self.api_client.append_sheet_data(self.config.spreadsheet_token, range_str, values)
        
        # 获取现有数据
        current_df = self.get_current_sheet_data()
        
        if current_df.empty:
            self.logger.info("电子表格为空，新增全部数据")
            return self.sync_clone(df)
        
        # 构建索引
        current_index = self.build_data_index(current_df)
        
        # 筛选需要新增的记录
        new_rows = []
        for _, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            if not index_hash or index_hash not in current_index:
                new_rows.append(row)
        
        self.logger.info(f"增量同步计划: 新增 {len(new_rows)} 行")
        
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            new_values = self.df_to_values(new_df, include_headers=False)
            
            # 追加新数据
            range_str = f"{self.config.sheet_id}!A:A"  # 让系统自动确定追加位置
            return self.api_client.append_sheet_data(self.config.spreadsheet_token, range_str, new_values)
        else:
            self.logger.info("没有新记录需要同步")
            return True
    
    def sync_overwrite(self, df: pd.DataFrame) -> bool:
        """覆盖同步：删除已存在的，然后新增全部"""
        self.logger.info("开始覆盖同步...")
        
        if not self.config.index_column:
            self.logger.error("覆盖同步模式需要指定索引列")
            return False
        
        # 获取现有数据
        current_df = self.get_current_sheet_data()
        
        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)
        
        # 找出需要删除的记录并构建新的数据集
        new_df_rows = []
        deleted_count = 0
        
        # 保留不在新数据中的现有记录
        for _, row in current_df.iterrows():
            index_hash = self.get_index_value_hash(row)
            if index_hash:
                # 检查是否在新数据中
                found_in_new = False
                for _, new_row in df.iterrows():
                    new_index_hash = self.get_index_value_hash(new_row)
                    if new_index_hash == index_hash:
                        found_in_new = True
                        break
                
                if not found_in_new:
                    new_df_rows.append(row)
                else:
                    deleted_count += 1
        
        # 添加新数据
        for _, row in df.iterrows():
            new_df_rows.append(row)
        
        self.logger.info(f"覆盖同步计划: 删除 {deleted_count} 行，新增 {len(df)} 行")
        
        # 重写整个表格
        if new_df_rows:
            new_df = pd.DataFrame(new_df_rows)
            values = self.df_to_values(new_df)
            end_col = self.column_number_to_letter(len(new_df.columns))
            range_str = self.get_range_string(1, "A", len(values), end_col)
            
            # 先清空现有数据，然后写入新数据
            return self.api_client.write_sheet_data(self.config.spreadsheet_token, range_str, values)
        else:
            # 如果没有数据，清空表格
            return self.api_client.clear_sheet_data(self.config.spreadsheet_token, f"{self.config.sheet_id}!A:Z")
    
    def sync_clone(self, df: pd.DataFrame) -> bool:
        """克隆同步：清空全部，然后新增全部"""
        self.logger.info("开始克隆同步...")
        
        # 转换数据格式
        values = self.df_to_values(df)
        end_col = self.column_number_to_letter(len(df.columns))
        range_str = self.get_range_string(1, "A", len(values), end_col)
        
        self.logger.info(f"克隆同步计划: 清空现有数据，新增 {len(df)} 行")
        
        # 直接写入数据（会覆盖现有数据）
        return self.api_client.write_sheet_data(self.config.spreadsheet_token, range_str, values)
    
    def sync(self, df: pd.DataFrame) -> bool:
        """执行同步"""
        self.logger.info(f"开始执行 {self.config.sync_mode.value} 同步模式")
        self.logger.info(f"数据源: {len(df)} 行 x {len(df.columns)} 列")
        
        # 重置转换统计
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
        
        # 根据同步模式执行对应操作
        sync_result = False
        if self.config.sync_mode == SyncMode.FULL:
            sync_result = self.sync_full(df)
        elif self.config.sync_mode == SyncMode.INCREMENTAL:
            sync_result = self.sync_incremental(df)
        elif self.config.sync_mode == SyncMode.OVERWRITE:
            sync_result = self.sync_overwrite(df)
        elif self.config.sync_mode == SyncMode.CLONE:
            sync_result = self.sync_clone(df)
        else:
            self.logger.error(f"不支持的同步模式: {self.config.sync_mode}")
            return False
        
        return sync_result


class ConfigManager:
    """配置管理器"""
    
    @staticmethod
    def load_from_file(config_file: str) -> Optional[Dict[str, Any]]:
        """从YAML文件加载配置"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            print(f"配置文件不存在: {config_file}")
            return None
        except yaml.YAMLError as e:
            print(f"YAML配置文件格式错误: {e}")
            return None
    
    @staticmethod
    def save_to_file(config: Dict[str, Any], config_file: str):
        """保存配置到YAML文件"""
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
    
    @staticmethod
    def parse_args() -> argparse.Namespace:
        """解析命令行参数"""
        parser = argparse.ArgumentParser(description='XTF_Sheet - Excel To Feishu 同步工具（支持多维表格和电子表格）')
        
        # 基础配置
        parser.add_argument('--config', '-c', type=str, default='config.yaml',
                          help='配置文件路径 (默认: config.yaml)')
        parser.add_argument('--file-path', type=str, help='Excel文件路径')
        parser.add_argument('--app-id', type=str, help='飞书应用ID')
        parser.add_argument('--app-secret', type=str, help='飞书应用密钥')
        
        # 目标平台配置
        parser.add_argument('--target-type', type=str, choices=['bitable', 'sheet'],
                          help='目标类型: bitable(多维表格) 或 sheet(电子表格)')
        
        # 多维表格配置
        parser.add_argument('--app-token', type=str, help='多维表格应用Token')
        parser.add_argument('--table-id', type=str, help='数据表ID')
        parser.add_argument('--create-missing-fields', action='store_true', help='自动创建缺失字段')
        
        # 电子表格配置
        parser.add_argument('--spreadsheet-token', type=str, help='电子表格Token')
        parser.add_argument('--sheet-id', type=str, help='工作表ID')
        parser.add_argument('--start-row', type=int, help='开始行号')
        parser.add_argument('--start-column', type=str, help='开始列号')
        
        # 同步设置
        parser.add_argument('--sync-mode', type=str, 
                          choices=['full', 'incremental', 'overwrite', 'clone'],
                          help='同步模式')
        parser.add_argument('--index-column', type=str, help='索引列名')
        
        # 性能设置
        parser.add_argument('--batch-size', type=int, help='批处理大小')
        parser.add_argument('--rate-limit-delay', type=float, help='接口调用间隔秒数')
        parser.add_argument('--max-retries', type=int, help='最大重试次数')
        
        # 日志设置
        parser.add_argument('--log-level', type=str, 
                          choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                          help='日志级别')
        
        return parser.parse_args()
    
    @classmethod
    def create_config(cls) -> SyncConfig:
        """创建配置对象"""
        args = cls.parse_args()
        
        # 先设置默认值
        config_data = {
            'sync_mode': 'full',
            'start_row': 1,
            'start_column': 'A',
            'batch_size': 1000,
            'rate_limit_delay': 0.1,
            'max_retries': 3,
            'log_level': 'INFO'
        }
        
        # 尝试从配置文件加载，覆盖默认值
        if Path(args.config).exists():
            file_config = cls.load_from_file(args.config)
            if file_config:
                config_data.update(file_config)
                print(f"✅ 已从配置文件加载参数: {args.config}")
        else:
            print(f"⚠️  配置文件 {args.config} 不存在，使用默认值")
        
        # 命令行参数覆盖文件配置
        cli_overrides = []
        
        # 基础参数
        if args.file_path:
            config_data['file_path'] = args.file_path
            cli_overrides.append(f"file_path={args.file_path}")
        if args.app_id:
            config_data['app_id'] = args.app_id
            cli_overrides.append(f"app_id={args.app_id[:8]}...")
        if args.app_secret:
            config_data['app_secret'] = args.app_secret
            cli_overrides.append(f"app_secret=***")
        if args.spreadsheet_token:
            config_data['spreadsheet_token'] = args.spreadsheet_token
            cli_overrides.append(f"spreadsheet_token={args.spreadsheet_token[:8]}...")
        if args.sheet_id:
            config_data['sheet_id'] = args.sheet_id
            cli_overrides.append(f"sheet_id={args.sheet_id}")
        if args.index_column:
            config_data['index_column'] = args.index_column
            cli_overrides.append(f"index_column={args.index_column}")
        if args.start_row is not None:
            config_data['start_row'] = args.start_row
            cli_overrides.append(f"start_row={args.start_row}")
        if args.start_column:
            config_data['start_column'] = args.start_column
            cli_overrides.append(f"start_column={args.start_column}")
        
        # 高级参数
        if args.sync_mode is not None:
            config_data['sync_mode'] = args.sync_mode
            cli_overrides.append(f"sync_mode={args.sync_mode}")
        if args.batch_size is not None:
            config_data['batch_size'] = args.batch_size
            cli_overrides.append(f"batch_size={args.batch_size}")
        if args.rate_limit_delay is not None:
            config_data['rate_limit_delay'] = args.rate_limit_delay
            cli_overrides.append(f"rate_limit_delay={args.rate_limit_delay}")
        if args.max_retries is not None:
            config_data['max_retries'] = args.max_retries
            cli_overrides.append(f"max_retries={args.max_retries}")
        if args.log_level is not None:
            config_data['log_level'] = args.log_level
            cli_overrides.append(f"log_level={args.log_level}")
        
        # 显示命令行覆盖的参数
        if cli_overrides:
            print(f"🔧 命令行参数覆盖: {', '.join(cli_overrides)}")
        
        # 验证必需参数
        required_fields = ['file_path', 'app_id', 'app_secret', 'spreadsheet_token', 'sheet_id']
        missing_fields = [f for f in required_fields if not config_data.get(f)]
        
        if missing_fields:
            print(f"\n❌ 错误: 缺少必需参数: {', '.join(missing_fields)}")
            print("💡 请通过以下方式提供这些参数:")
            print("   1. 在配置文件中设置")
            print("   2. 通过命令行参数指定")
            print("\n命令行参数示例:")
            for field in missing_fields:
                field_name = field.replace('_', '-')
                print(f"   --{field_name} <值>")
            sys.exit(1)
        
        return SyncConfig(**config_data)


def create_sample_config(config_file: str = "config.yaml"):
    """创建示例配置文件"""
    sample_config = {
        "file_path": "data.xlsx",
        "app_id": "cli_your_app_id",
        "app_secret": "your_app_secret",
        "spreadsheet_token": "your_spreadsheet_token",
        "sheet_id": "your_sheet_id",
        "sync_mode": "full",
        "index_column": "ID",
        "start_row": 1,
        "start_column": "A",
        "batch_size": 1000,
        "rate_limit_delay": 0.1,
        "max_retries": 3,
        "log_level": "INFO"
    }
    
    if not Path(config_file).exists():
        ConfigManager.save_to_file(sample_config, config_file)
        print(f"已创建示例配置文件: {config_file}")
        print("请编辑配置文件并填入正确的参数值")
        return True
    else:
        print(f"配置文件 {config_file} 已存在")
        return False


def main():
    """主函数"""
    print("=" * 70)
    print("     XTF_Sheet 电子表格同步工具")
    print("     支持四种同步模式：全量、增量、覆盖、克隆")
    print("=" * 70)
    
    try:
        # 先解析命令行参数以获取配置文件路径
        import argparse
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('--config', '-c', type=str, default='config.yaml')
        args, _ = parser.parse_known_args()
        config_file_path = args.config
        
        # 如果指定的配置文件不存在，创建示例配置
        if not Path(config_file_path).exists():
            print(f"配置文件不存在: {config_file_path}")
            if create_sample_config(config_file_path):
                print(f"请编辑 {config_file_path} 文件并重新运行程序")
            return
        
        # 加载配置
        config = ConfigManager.create_config()
        
        # 显示加载的配置信息
        print(f"\n📋 已加载配置:")
        print(f"  配置文件: {config_file_path}")
        print(f"  Excel文件: {config.file_path}")
        print(f"  电子表格Token: {config.spreadsheet_token[:8]}...")
        print(f"  工作表ID: {config.sheet_id}")
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  开始位置: {config.start_column}{config.start_row}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  接口调用间隔: {config.rate_limit_delay}秒")
        print(f"  最大重试次数: {config.max_retries}")
        print(f"  日志级别: {config.log_level}")
        
        # 验证文件
        file_path = Path(config.file_path)
        if not file_path.exists():
            print(f"\n❌ 错误: Excel文件不存在 - {file_path}")
            print("请检查配置文件中的 file_path 参数")
            return
        
        # 读取Excel文件
        print(f"\n📖 正在读取文件: {file_path}")
        try:
            df = pd.read_excel(file_path)
            print(f"✅ 文件读取成功，共 {len(df)} 行，{len(df.columns)} 列")
            print(f"📊 列名: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"❌ 文件读取失败: {e}")
            return
        
        # 创建同步引擎
        sync_engine = XTFSheetSyncEngine(config)
        
        # 执行同步
        print(f"\n🚀 开始执行 {config.sync_mode.value} 同步...")
        start_time = time.time()
        
        success = sync_engine.sync(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            print(f"\n✅ 同步完成！耗时: {duration:.2f} 秒")
            print(f"📊 同步到电子表格: https://feishu.cn/sheets/{config.spreadsheet_token}")
        else:
            print(f"\n❌ 同步过程中出现错误，请查看日志文件")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {str(e)}")
        logging.error(f"程序异常: {e}", exc_info=True)


if __name__ == "__main__":
    main()