#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF (Excel To Feishu) - 本地表格同步到飞书多维表格工具
支持四种同步模式：全量、增量、覆盖、克隆
具备智能字段管理、频率限制、重试机制等企业级功能
"""

import pandas as pd
import requests
import json
import time
import logging
import argparse
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List, Set, Tuple, Union
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
    app_token: str
    table_id: str
    
    # 同步设置
    sync_mode: SyncMode = SyncMode.FULL
    index_column: str = None  # 索引列名，用于记录比对
    
    # 性能设置
    batch_size: int = 500  # 批处理大小
    rate_limit_delay: float = 0.5  # 接口调用间隔
    max_retries: int = 3  # 最大重试次数
    
    # 字段管理
    create_missing_fields: bool = True
    
    # 日志设置
    log_level: str = "INFO"
    
    def __post_init__(self):
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode(self.sync_mode)


class RateLimiter:
    """接口频率限制器"""
    def __init__(self, delay: float = 0.5):
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
    def __init__(self, max_retries: int = 3, rate_limiter: RateLimiter = None):
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


class FeishuAPIClient:
    """飞书API客户端"""
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
        result = response.json()
        
        if result.get("code") != 0:
            raise Exception(f"获取访问令牌失败: {result.get('msg')}")
        
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
    
    def list_fields(self, app_token: str, table_id: str) -> List[Dict[str, Any]]:
        """列出表格字段"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.get_auth_headers()
        
        all_fields = []
        page_token = None
        
        while True:
            params = {"page_size": 100}
            if page_token:
                params["page_token"] = page_token
            
            response = self.api_client.call_api("GET", url, headers=headers, params=params)
            result = response.json()
            
            if result.get("code") != 0:
                raise Exception(f"获取字段列表失败: {result.get('msg')}")
            
            data = result.get("data", {})
            all_fields.extend(data.get("items", []))
            
            if not data.get("has_more"):
                break
            page_token = data.get("page_token")
        
        return all_fields
    
    def create_field(self, app_token: str, table_id: str, field_name: str, field_type: int = 1) -> bool:
        """创建字段"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.get_auth_headers()
        data = {
            "field_name": field_name,
            "type": field_type  # 1=多行文本
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        result = response.json()
        
        if result.get("code") != 0:
            self.logger.error(f"创建字段 '{field_name}' 失败: {result.get('msg')}")
            return False
        
        self.logger.info(f"创建字段 '{field_name}' 成功")
        return True
    
    def search_records(self, app_token: str, table_id: str, page_token: str = None, 
                      page_size: int = 500) -> Tuple[List[Dict], Optional[str]]:
        """搜索记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        headers = self.get_auth_headers()
        
        # 分页参数应该作为查询参数，不是请求体
        params = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        
        # 请求体可以包含过滤条件、排序等（当前为空，只做简单查询）
        data = {}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        result = response.json()
        
        if result.get("code") != 0:
            raise Exception(f"搜索记录失败: {result.get('msg')}")
        
        result_data = result.get("data", {})
        records = result_data.get("items", [])
        next_page_token = result_data.get("page_token") if result_data.get("has_more") else None
        
        return records, next_page_token
    
    def get_all_records(self, app_token: str, table_id: str) -> List[Dict]:
        """获取所有记录"""
        all_records = []
        page_token = None
        
        while True:
            records, page_token = self.search_records(app_token, table_id, page_token)
            all_records.extend(records)
            
            if not page_token:
                break
        
        return all_records
    
    def batch_create_records(self, app_token: str, table_id: str, records: List[Dict]) -> bool:
        """批量创建记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        headers = self.get_auth_headers()
        
        # 生成唯一的client_token，并添加性能优化参数
        client_token = str(uuid.uuid4())
        params = {
            "client_token": client_token,
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id"
        }
        
        data = {"records": records}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        result = response.json()
        
        if result.get("code") != 0:
            self.logger.error(f"批量创建记录失败: {result.get('msg')}")
            return False
        
        return True
    
    def batch_update_records(self, app_token: str, table_id: str, records: List[Dict]) -> bool:
        """批量更新记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        headers = self.get_auth_headers()
        
        # 添加查询参数提高性能
        params = {
            "ignore_consistency_check": "true",  # 忽略一致性检查，提高性能
            "user_id_type": "open_id"
        }
        
        data = {"records": records}
        
        response = self.api_client.call_api("POST", url, headers=headers, params=params, json=data)
        result = response.json()
        
        if result.get("code") != 0:
            self.logger.error(f"批量更新记录失败: {result.get('msg')}")
            return False
        
        return True
    
    def batch_delete_records(self, app_token: str, table_id: str, record_ids: List[str]) -> bool:
        """批量删除记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = self.get_auth_headers()
        data = {"records": record_ids}
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        result = response.json()
        
        if result.get("code") != 0:
            self.logger.error(f"批量删除记录失败: {result.get('msg')}")
            return False
        
        return True


class XTFSyncEngine:
    """XTF同步引擎 - 支持四种同步模式的智能同步"""
    
    def __init__(self, config: SyncConfig):
        """
        初始化同步引擎
        
        Args:
            config: 同步配置对象
        """
        self.config = config
        self.api_client = FeishuAPIClient(config)
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
    def setup_logging(self):
        """设置日志"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"xtf_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
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
        
    def get_field_types(self) -> Dict[str, int]:
        """获取字段类型映射"""
        try:
            existing_fields = self.api_client.list_fields(self.config.app_token, self.config.table_id)
            field_types = {}
            for field in existing_fields:
                field_name = field.get('field_name', '')
                field_type = field.get('type', 1)  # 默认为文本类型
                field_types[field_name] = field_type
            
            self.logger.debug(f"获取到 {len(field_types)} 个字段类型信息")
            return field_types
            
        except Exception as e:
            self.logger.warning(f"获取字段类型失败: {e}，将使用智能类型检测")
            return {}

    def ensure_fields_exist(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, int]]:
        """确保所需字段存在于目标表中，返回成功状态和字段类型映射"""
        try:
            # 获取现有字段
            existing_fields = self.api_client.list_fields(self.config.app_token, self.config.table_id)
            existing_field_names = {field['field_name'] for field in existing_fields}
            
            # 构建字段类型映射
            field_types = {}
            for field in existing_fields:
                field_name = field.get('field_name', '')
                field_type = field.get('type', 1)
                field_types[field_name] = field_type
            
            if self.config.create_missing_fields:
                # 找出缺失的字段
                required_fields = set(df.columns)
                missing_fields = required_fields - existing_field_names
                
                if missing_fields:
                    self.logger.info(f"需要创建 {len(missing_fields)} 个缺失字段: {', '.join(missing_fields)}")
                    
                    for field_name in missing_fields:
                        success = self.api_client.create_field(
                            self.config.app_token, 
                            self.config.table_id, 
                            field_name
                        )
                        if not success:
                            return False, field_types
                        # 新创建的字段默认为文本类型
                        field_types[field_name] = 1
                    
                    # 等待字段创建完成
                    time.sleep(2)
                else:
                    self.logger.info("所有必需字段已存在")
            
            return True, field_types
            
        except Exception as e:
            self.logger.error(f"字段检查失败: {e}")
            return False, {}
    
    def get_index_value_hash(self, row: pd.Series) -> str:
        """计算索引值的哈希"""
        if self.config.index_column and self.config.index_column in row:
            value = str(row[self.config.index_column])
            return hashlib.md5(value.encode('utf-8')).hexdigest()
        return None
    
    def build_record_index(self, records: List[Dict]) -> Dict[str, Dict]:
        """构建记录索引"""
        index = {}
        if not self.config.index_column:
            return index
        
        for record in records:
            fields = record.get('fields', {})
            if self.config.index_column in fields:
                index_value = str(fields[self.config.index_column])
                index_hash = hashlib.md5(index_value.encode('utf-8')).hexdigest()
                index[index_hash] = record
        
        return index
    
    def convert_field_value(self, field_name: str, value, field_types: Dict[str, int] = None):
        """根据字段类型转换数值"""
        if pd.isnull(value):
            return None
            
        # 如果没有字段类型信息，则按数据类型智能判断
        if field_types is None or field_name not in field_types:
            return self.smart_convert_value(value)
        
        field_type = field_types[field_name]
        
        # 根据飞书字段类型转换
        if field_type == 1:  # 文本
            return str(value)
        elif field_type == 2:  # 数字
            try:
                # 先尝试转为整数，再尝试浮点数
                if isinstance(value, (int, float)):
                    return value
                str_val = str(value).strip()
                if '.' in str_val:
                    return float(str_val)
                return int(str_val)
            except (ValueError, TypeError):
                return str(value)  # 转换失败时保持字符串
        elif field_type == 5:  # 日期
            return self.convert_to_timestamp(value)
        elif field_type == 7:  # 复选框
            return self.convert_to_boolean(value)
        elif field_type == 3:  # 单选
            return str(value)
        elif field_type == 4:  # 多选
            if isinstance(value, str):
                # 如果是字符串，尝试按分隔符拆分
                if ',' in value:
                    return [v.strip() for v in value.split(',') if v.strip()]
                elif ';' in value:
                    return [v.strip() for v in value.split(';') if v.strip()]
                elif '|' in value:
                    return [v.strip() for v in value.split('|') if v.strip()]
                else:
                    return [str(value)]
            elif isinstance(value, (list, tuple)):
                return [str(v) for v in value if v]
            else:
                return [str(value)]
        elif field_type == 13:  # 电话号码
            return str(value)
        elif field_type == 22:  # 地理位置
            return str(value)
        else:
            # 其他类型默认转为字符串
            return str(value)
    
    def smart_convert_value(self, value):
        """智能转换数值类型（当没有字段类型信息时）"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, (int, float)):
            return value
        elif isinstance(value, str):
            str_val = value.strip().lower()
            # 布尔值检测
            if str_val in ['true', '是', 'yes', '1']:
                return True
            elif str_val in ['false', '否', 'no', '0']:
                return False
            # 数字检测
            try:
                if '.' in str_val:
                    return float(str_val)
                return int(str_val)
            except (ValueError, TypeError):
                pass
            # 日期检测（简单的时间戳检测）
            if str_val.isdigit() and len(str_val) >= 10:
                try:
                    timestamp = int(str_val)
                    # 检查是否是合理的时间戳范围（2000年到2050年）
                    if 946684800000 <= timestamp <= 2524608000000:  # 毫秒级时间戳
                        return timestamp
                    elif 946684800 <= timestamp <= 2524608000:  # 秒级时间戳，转为毫秒
                        return timestamp * 1000
                except (ValueError, TypeError):
                    pass
        return str(value)
    
    def convert_to_timestamp(self, value):
        """转换为毫秒级时间戳"""
        if isinstance(value, (int, float)):
            # 如果已经是数字，检查是否需要转换
            if value > 2524608000:  # 大于2050年的秒级时间戳，认为是毫秒级
                return int(value)
            else:  # 认为是秒级，转为毫秒级
                return int(value * 1000)
        
        # 如果是字符串数字，先转为数字再判断
        if isinstance(value, str) and value.isdigit():
            num_value = int(value)
            if num_value > 2524608000:  # 毫秒级时间戳
                return num_value
            elif num_value > 946684800:  # 秒级时间戳，转为毫秒级
                return num_value * 1000
        
        try:
            import datetime as dt
            # 尝试解析字符串日期
            if isinstance(value, str):
                # 常见日期格式
                formats = [
                    '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%d',
                    '%Y/%m/%d %H:%M:%S',
                    '%Y/%m/%d',
                    '%m/%d/%Y',
                    '%d/%m/%Y'
                ]
                for fmt in formats:
                    try:
                        dt_obj = dt.datetime.strptime(value, fmt)
                        return int(dt_obj.timestamp() * 1000)
                    except ValueError:
                        continue
            
            # 如果是pandas的Timestamp对象
            if hasattr(value, 'timestamp'):
                return int(value.timestamp() * 1000)
                
        except Exception:
            pass
        
        # 转换失败，返回字符串
        return str(value)
    
    def convert_to_boolean(self, value):
        """转换为布尔值"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, (int, float)):
            return bool(value)
        elif isinstance(value, str):
            str_val = value.strip().lower()
            if str_val in ['true', '是', 'yes', '1', 'on', 'checked']:
                return True
            elif str_val in ['false', '否', 'no', '0', 'off', 'unchecked', '']:
                return False
        return bool(value)

    def df_to_records(self, df: pd.DataFrame, field_types: Dict[str, int] = None) -> List[Dict]:
        """将DataFrame转换为飞书记录格式"""
        records = []
        for _, row in df.iterrows():
            fields = {}
            for k, v in row.to_dict().items():
                if pd.notnull(v):
                    converted_value = self.convert_field_value(str(k), v, field_types)
                    if converted_value is not None:
                        fields[str(k)] = converted_value
            
            record = {"fields": fields}
            records.append(record)
        return records
    
    def process_in_batches(self, items: List[Any], batch_size: int, 
                          processor_func, *args, **kwargs) -> bool:
        """分批处理数据"""
        total_batches = (len(items) + batch_size - 1) // batch_size
        success_count = 0
        
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            try:
                if processor_func(batch, *args, **kwargs):
                    success_count += 1
                    self.logger.info(f"批次 {batch_num}/{total_batches} 处理成功 ({len(batch)} 条记录)")
                else:
                    self.logger.error(f"批次 {batch_num}/{total_batches} 处理失败")
            except Exception as e:
                self.logger.error(f"批次 {batch_num}/{total_batches} 处理异常: {e}")
        
        self.logger.info(f"批处理完成: {success_count}/{total_batches} 个批次成功")
        return success_count == total_batches
        
    def sync_full(self, df: pd.DataFrame, field_types: Dict[str, int] = None) -> bool:
        """全量同步：已存在索引值的更新，不存在的新增"""
        self.logger.info("开始全量同步...")
        
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            new_records = self.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.api_client.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.api_client.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.build_record_index(existing_records)
        
        # 分类本地数据
        records_to_update = []
        records_to_create = []
        
        for _, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            
            # 使用字段类型转换构建记录
            fields = {}
            for k, v in row.to_dict().items():
                if pd.notnull(v):
                    converted_value = self.convert_field_value(str(k), v, field_types)
                    if converted_value is not None:
                        fields[str(k)] = converted_value
            
            record = {"fields": fields}
            
            if index_hash and index_hash in existing_index:
                # 需要更新的记录
                existing_record = existing_index[index_hash]
                record["record_id"] = existing_record["record_id"]
                records_to_update.append(record)
            else:
                # 需要新增的记录
                records_to_create.append(record)
        
        self.logger.info(f"全量同步计划: 更新 {len(records_to_update)} 条，新增 {len(records_to_create)} 条")
        
        # 执行更新
        update_success = True
        if records_to_update:
            update_success = self.process_in_batches(
                records_to_update, self.config.batch_size,
                self.api_client.batch_update_records,
                self.config.app_token, self.config.table_id
            )
        
        # 执行新增
        create_success = True
        if records_to_create:
            create_success = self.process_in_batches(
                records_to_create, self.config.batch_size,
                self.api_client.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        return update_success and create_success
    
    def sync_incremental(self, df: pd.DataFrame, field_types: Dict[str, int] = None) -> bool:
        """增量同步：只新增不存在索引值的记录"""
        self.logger.info("开始增量同步...")
        
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            new_records = self.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.api_client.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.api_client.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.build_record_index(existing_records)
        
        # 筛选出需要新增的记录
        records_to_create = []
        
        for _, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            
            if not index_hash or index_hash not in existing_index:
                # 使用字段类型转换构建记录
                fields = {}
                for k, v in row.to_dict().items():
                    if pd.notnull(v):
                        converted_value = self.convert_field_value(str(k), v, field_types)
                        if converted_value is not None:
                            fields[str(k)] = converted_value
                
                record = {"fields": fields}
                records_to_create.append(record)
        
        self.logger.info(f"增量同步计划: 新增 {len(records_to_create)} 条记录")
        
        if records_to_create:
            return self.process_in_batches(
                records_to_create, self.config.batch_size,
                self.api_client.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        else:
            self.logger.info("没有新记录需要同步")
            return True
    
    def sync_overwrite(self, df: pd.DataFrame, field_types: Dict[str, int] = None) -> bool:
        """覆盖同步：删除已存在索引值的记录，然后新增全部记录"""
        self.logger.info("开始覆盖同步...")
        
        if not self.config.index_column:
            self.logger.error("覆盖同步模式需要指定索引列")
            return False
        
        # 获取现有记录并建立索引
        existing_records = self.api_client.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.build_record_index(existing_records)
        
        # 找出需要删除的记录
        record_ids_to_delete = []
        
        for _, row in df.iterrows():
            index_hash = self.get_index_value_hash(row)
            if index_hash and index_hash in existing_index:
                existing_record = existing_index[index_hash]
                record_ids_to_delete.append(existing_record["record_id"])
        
        self.logger.info(f"覆盖同步计划: 删除 {len(record_ids_to_delete)} 条已存在记录，然后新增 {len(df)} 条记录")
        
        # 删除已存在的记录
        delete_success = True
        if record_ids_to_delete:
            delete_success = self.process_in_batches(
                record_ids_to_delete, self.config.batch_size,
                self.api_client.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        new_records = self.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.api_client.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def sync_clone(self, df: pd.DataFrame, field_types: Dict[str, int] = None) -> bool:
        """克隆同步：清空全部已有记录，然后新增全部记录"""
        self.logger.info("开始克隆同步...")
        
        # 获取所有现有记录
        existing_records = self.api_client.get_all_records(self.config.app_token, self.config.table_id)
        existing_record_ids = [record["record_id"] for record in existing_records]
        
        self.logger.info(f"克隆同步计划: 删除 {len(existing_record_ids)} 条已有记录，然后新增 {len(df)} 条记录")
        
        # 删除所有记录
        delete_success = True
        if existing_record_ids:
            delete_success = self.process_in_batches(
                existing_record_ids, self.config.batch_size,
                self.api_client.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        new_records = self.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.api_client.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def sync(self, df: pd.DataFrame) -> bool:
        """执行同步"""
        self.logger.info(f"开始执行 {self.config.sync_mode.value} 同步模式")
        self.logger.info(f"数据源: {len(df)} 行 x {len(df.columns)} 列")
        
        # 确保字段存在并获取字段类型信息
        success, field_types = self.ensure_fields_exist(df)
        if not success:
            self.logger.error("字段创建失败，同步终止")
            return False
        
        self.logger.info(f"获取到 {len(field_types)} 个字段的类型信息")
        
        # 根据同步模式执行对应操作
        if self.config.sync_mode == SyncMode.FULL:
            return self.sync_full(df, field_types)
        elif self.config.sync_mode == SyncMode.INCREMENTAL:
            return self.sync_incremental(df, field_types)
        elif self.config.sync_mode == SyncMode.OVERWRITE:
            return self.sync_overwrite(df, field_types)
        elif self.config.sync_mode == SyncMode.CLONE:
            return self.sync_clone(df, field_types)
        else:
            self.logger.error(f"不支持的同步模式: {self.config.sync_mode}")
            return False


class ConfigManager:
    """配置管理器"""
    
    @staticmethod
    def load_from_file(config_file: str) -> Optional[Dict[str, Any]]:
        """从文件加载配置"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"配置文件不存在: {config_file}")
            return None
        except json.JSONDecodeError as e:
            print(f"配置文件格式错误: {e}")
            return None
    
    @staticmethod
    def save_to_file(config: Dict[str, Any], config_file: str):
        """保存配置到文件"""
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    
    @staticmethod
    def parse_args() -> argparse.Namespace:
        """解析命令行参数"""
        parser = argparse.ArgumentParser(description='XTF - Excel To Feishu 同步工具')
        
        # 基础配置
        parser.add_argument('--config', '-c', type=str, default='config.json',
                          help='配置文件路径 (默认: config.json)')
        parser.add_argument('--file-path', type=str, help='Excel文件路径')
        parser.add_argument('--app-id', type=str, help='飞书应用ID')
        parser.add_argument('--app-secret', type=str, help='飞书应用密钥')
        parser.add_argument('--app-token', type=str, help='多维表格应用Token')
        parser.add_argument('--table-id', type=str, help='数据表ID')
        
        # 同步设置
        parser.add_argument('--sync-mode', type=str, 
                          choices=['full', 'incremental', 'overwrite', 'clone'],
                          default='full', help='同步模式 (默认: full)')
        parser.add_argument('--index-column', type=str, help='索引列名')
        
        # 性能设置
        parser.add_argument('--batch-size', type=int, default=500, 
                          help='批处理大小 (默认: 500)')
        parser.add_argument('--rate-limit-delay', type=float, default=0.5,
                          help='接口调用间隔秒数 (默认: 0.5)')
        parser.add_argument('--max-retries', type=int, default=3,
                          help='最大重试次数 (默认: 3)')
        
        # 功能开关
        parser.add_argument('--no-create-fields', action='store_true',
                          help='不自动创建缺失字段')
        
        # 日志设置
        parser.add_argument('--log-level', type=str, 
                          choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                          default='INFO', help='日志级别 (默认: INFO)')
        
        return parser.parse_args()
    
    @classmethod
    def create_config(cls) -> SyncConfig:
        """创建配置对象"""
        args = cls.parse_args()
        
        # 尝试从配置文件加载
        config_data = {}
        if Path(args.config).exists():
            file_config = cls.load_from_file(args.config)
            if file_config:
                config_data.update(file_config)
                print(f"已加载配置文件: {args.config}")
        
        # 命令行参数覆盖文件配置
        if args.file_path:
            config_data['file_path'] = args.file_path
        if args.app_id:
            config_data['app_id'] = args.app_id
        if args.app_secret:
            config_data['app_secret'] = args.app_secret
        if args.app_token:
            config_data['app_token'] = args.app_token
        if args.table_id:
            config_data['table_id'] = args.table_id
        if args.index_column:
            config_data['index_column'] = args.index_column
        
        config_data['sync_mode'] = args.sync_mode
        config_data['batch_size'] = args.batch_size
        config_data['rate_limit_delay'] = args.rate_limit_delay
        config_data['max_retries'] = args.max_retries
        config_data['create_missing_fields'] = not args.no_create_fields
        config_data['log_level'] = args.log_level
        
        # 验证必需参数
        required_fields = ['file_path', 'app_id', 'app_secret', 'app_token', 'table_id']
        missing_fields = [f for f in required_fields if not config_data.get(f)]
        
        if missing_fields:
            print(f"错误: 缺少必需参数: {', '.join(missing_fields)}")
            print("请通过配置文件或命令行参数提供这些值")
            sys.exit(1)
        
        return SyncConfig(**config_data)


def create_sample_config():
    """创建示例配置文件"""
    sample_config = {
        "file_path": "data.xlsx",
        "app_id": "cli_your_app_id",
        "app_secret": "your_app_secret",
        "app_token": "your_app_token",
        "table_id": "your_table_id",
        "sync_mode": "full",
        "index_column": "ID",
        "batch_size": 500,
        "rate_limit_delay": 0.5,
        "max_retries": 3,
        "create_missing_fields": True,
        "log_level": "INFO"
    }
    
    config_file = "config.json"
    if not Path(config_file).exists():
        ConfigManager.save_to_file(sample_config, config_file)
        print(f"已创建示例配置文件: {config_file}")
        print("请编辑配置文件并填入正确的参数值")
    else:
        print(f"配置文件 {config_file} 已存在")


def main():
    """主函数"""
    print("=" * 70)
    print("     XTF工具")
    print("     支持四种同步模式：全量、增量、覆盖、克隆")
    print("=" * 70)
    
    try:
        # 如果没有配置文件，创建示例配置
        if not Path('config.json').exists():
            create_sample_config()
            return
        
        # 加载配置
        config = ConfigManager.create_config()
        
        # 验证文件
        file_path = Path(config.file_path)
        if not file_path.exists():
            print(f"错误: 文件不存在 - {file_path}")
            return
        
        # 读取Excel文件
        print(f"\n正在读取文件: {file_path}")
        try:
            df = pd.read_excel(file_path)
            print(f"文件读取成功，共 {len(df)} 行，{len(df.columns)} 列")
            print(f"列名: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"文件读取失败: {e}")
            return
        
        # 显示同步配置
        print(f"\n同步配置:")
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  自动创建字段: {'是' if config.create_missing_fields else '否'}")
        
        # 创建同步引擎
        sync_engine = XTFSyncEngine(config)
        
        # 执行同步
        print(f"\n开始执行 {config.sync_mode.value} 同步...")
        start_time = time.time()
        
        success = sync_engine.sync(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            print(f"\n✅ 同步完成！耗时: {duration:.2f} 秒")
        else:
            print(f"\n❌ 同步过程中出现错误，请查看日志")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {str(e)}")
        logging.error(f"程序异常: {e}", exc_info=True)


if __name__ == "__main__":
    main()