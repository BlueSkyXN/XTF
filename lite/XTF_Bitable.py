#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF (Excel To Feishu) - 本地表格同步到飞书多维表格工具
支持四种同步模式：全量、增量、覆盖、克隆
具备智能字段管理、频率限制、重试机制等企业级功能
"""

import pandas as pd
import requests
import yaml
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
    index_column: Optional[str] = None  # 索引列名，用于记录比对
    
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
        """创建字段"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/fields"
        headers = self.get_auth_headers()
        data = {
            "field_name": field_name,
            "type": field_type  # 1=多行文本
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
        
        self.logger.info(f"创建字段 '{field_name}' 成功")
        return True
    
    def search_records(self, app_token: str, table_id: str, page_token: Optional[str] = None,
                      page_size: int = 500) -> Tuple[List[Dict], Optional[str]]:
        """搜索记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/search"
        headers = self.get_auth_headers()
        
        # 分页参数应该作为查询参数，不是请求体
        params: Dict[str, Union[int, str]] = {"page_size": page_size}
        if page_token:
            params["page_token"] = page_token
        
        # 请求体可以包含过滤条件、排序等（当前为空，只做简单查询）
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
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量创建记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量创建记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            # 记录详细的调试信息
            self.logger.debug(f"创建失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.debug(f"成功创建 {len(records)} 条记录")
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
        
        try:
            result = response.json()
        except ValueError as e:
            self.logger.error(f"批量更新记录响应解析失败: {e}, HTTP状态码: {response.status_code}")
            self.logger.debug(f"响应内容: {response.text[:500]}")
            return False
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            self.logger.error(f"批量更新记录失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
            # 记录详细的调试信息
            self.logger.debug(f"更新失败的记录数量: {len(records)}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.debug(f"成功更新 {len(records)} 条记录")
        return True
    
    def batch_delete_records(self, app_token: str, table_id: str, record_ids: List[str]) -> bool:
        """批量删除记录"""
        url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
        headers = self.get_auth_headers()
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
            # 记录详细的调试信息
            self.logger.debug(f"删除失败的记录数量: {len(record_ids)}")
            self.logger.debug(f"API响应: {result}")
            return False
        
        self.logger.debug(f"成功删除 {len(record_ids)} 条记录")
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
        
        # 添加类型转换统计
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
    
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
                    
                    # 分析每个缺失字段的数据特征并创建合适类型的字段
                    for field_name in missing_fields:
                        analysis = self.analyze_excel_column_data(df, field_name)
                        suggested_type = analysis['suggested_feishu_type']
                        confidence = analysis['confidence']
                        
                        self.logger.info(f"字段 '{field_name}': {analysis['analysis']}, "
                                       f"建议类型: {self._get_field_type_name(suggested_type)} "
                                       f"(置信度: {confidence:.1%})")
                        
                        success = self.api_client.create_field(
                            self.config.app_token, 
                            self.config.table_id, 
                            field_name,
                            suggested_type
                        )
                        if not success:
                            return False, field_types
                        
                        # 记录新创建字段的类型
                        field_types[field_name] = suggested_type
                    
                    # 等待字段创建完成
                    time.sleep(2)
                else:
                    self.logger.info("所有必需字段已存在")
            
            return True, field_types
            
        except Exception as e:
            self.logger.error(f"字段检查失败: {e}")
            return False, {}
    
    def _get_field_type_name(self, field_type: int) -> str:
        """获取字段类型的中文名称"""
        type_names = {
            1: "文本", 2: "数字", 3: "单选", 4: "多选", 5: "日期", 
            7: "复选框", 11: "人员", 13: "电话", 15: "超链接", 
            17: "附件", 18: "单向关联", 21: "双向关联", 22: "地理位置", 23: "群组"
        }
        return type_names.get(field_type, f"未知类型({field_type})")
    
    def get_index_value_hash(self, row: pd.Series) -> Optional[str]:
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
    
    def analyze_excel_column_data(self, df: pd.DataFrame, column_name: str) -> Dict[str, Any]:
        """分析Excel列的数据特征，用于推断合适的飞书字段类型"""
        column_data = df[column_name].dropna()
        total_count = len(column_data)
        
        if total_count == 0:
            return {
                'primary_type': 'string',
                'suggested_feishu_type': 1,  # 文本
                'confidence': 0.5,
                'analysis': '列为空，默认文本类型'
            }
        
        # 数据类型统计
        type_stats = {
            'string': 0,
            'number': 0,
            'datetime': 0,
            'boolean': 0
        }
        
        unique_values = set()
        for value in column_data:
            unique_values.add(str(value))
            
            # 数值检测
            if isinstance(value, (int, float)):
                type_stats['number'] += 1
            elif isinstance(value, str):
                str_val = str(value).strip()
                # 布尔值检测
                if str_val.lower() in ['true', 'false', '是', '否', 'yes', 'no', '1', '0', 'on', 'off']:
                    type_stats['boolean'] += 1
                # 数字检测
                elif self._is_number_string(str_val):
                    type_stats['number'] += 1
                # 时间戳检测
                elif self._is_timestamp_string(str_val):
                    type_stats['datetime'] += 1
                # 日期格式检测
                elif self._is_date_string(str_val):
                    type_stats['datetime'] += 1
                else:
                    type_stats['string'] += 1
            else:
                type_stats['string'] += 1
        
        # 计算主要类型
        primary_type = max(type_stats.keys(), key=lambda k: type_stats[k])
        confidence = type_stats[primary_type] / total_count
        
        # 推断飞书字段类型
        suggested_type = self._suggest_feishu_field_type(
            primary_type, unique_values, total_count, confidence
        )
        
        return {
            'primary_type': primary_type,
            'suggested_feishu_type': suggested_type,
            'confidence': confidence,
            'unique_count': len(unique_values),
            'total_count': total_count,
            'type_distribution': type_stats,
            'analysis': f'{primary_type}类型占比{confidence:.1%}'
        }
    
    def _is_number_string(self, s: str) -> bool:
        """检测字符串是否为数字"""
        try:
            float(s.replace(',', ''))  # 支持千分位分隔符
            return True
        except ValueError:
            return False
    
    def _is_timestamp_string(self, s: str) -> bool:
        """检测字符串是否为时间戳"""
        if not s.isdigit():
            return False
        try:
            timestamp = int(s)
            # 检查是否是合理的时间戳范围（1970年到2100年）
            return 0 <= timestamp <= 4102444800 or 0 <= timestamp <= 4102444800000
        except ValueError:
            return False
    
    def _is_date_string(self, s: str) -> bool:
        """检测字符串是否为日期格式"""
        date_patterns = [
            r'\d{4}-\d{1,2}-\d{1,2}',  # 2024-01-01
            r'\d{4}/\d{1,2}/\d{1,2}',  # 2024/01/01
            r'\d{1,2}/\d{1,2}/\d{4}',  # 01/01/2024
            r'\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}',  # 2024-01-01 12:00:00
        ]
        import re
        for pattern in date_patterns:
            if re.match(pattern, s):
                return True
        return False
    
    def _suggest_feishu_field_type(self, primary_type: str, unique_values: set, 
                                  total_count: int, confidence: float) -> int:
        """根据数据特征推荐飞书字段类型"""
        unique_count = len(unique_values)
        
        if primary_type == 'number':
            return 2  # 数字字段
        elif primary_type == 'datetime':
            return 5  # 日期字段
        elif primary_type == 'boolean':
            return 7  # 复选框字段
        elif primary_type == 'string':
            # 字符串类型的细分判断
            if unique_count <= 20 and unique_count / total_count <= 0.5:
                # 唯一值较少且重复率高，推荐单选
                return 3  # 单选字段
            elif any(',' in str(v) or ';' in str(v) or '|' in str(v) for v in unique_values):
                # 包含分隔符，可能是多选
                return 4  # 多选字段
            else:
                return 1  # 文本字段
        
        return 1  # 默认文本字段
    
    def convert_field_value_safe(self, field_name: str, value, field_types: Optional[Dict[str, int]] = None):
        """安全的字段值转换，强制转换为飞书字段类型"""
        if pd.isnull(value):
            return None
            
        # 如果没有字段类型信息，使用智能转换
        if field_types is None or field_name not in field_types:
            return self.smart_convert_value(value)
        
        field_type = field_types[field_name]
        
        # 强制转换为目标类型，按飞书字段类型进行转换
        try:
            converted_value = self._force_convert_to_feishu_type(value, field_name, field_type)
            if converted_value is not None:
                self.conversion_stats['success'] += 1
                return converted_value
            else:
                self.conversion_stats['failed'] += 1
                return None
        except Exception as e:
            self.logger.warning(f"字段 '{field_name}' 强制转换失败: {e}, 原始值: '{value}'")
            self.conversion_stats['failed'] += 1
            return None
    
    def _force_convert_to_feishu_type(self, value, field_name: str, field_type: int):
        """强制转换值为指定的飞书字段类型"""
        
        if field_type == 1:  # 文本字段 - 所有值都可以转换为文本
            return str(value)
            
        elif field_type == 2:  # 数字字段 - 强制转换为数字
            return self._force_to_number(value, field_name)
            
        elif field_type == 3:  # 单选字段 - 转换为单个字符串
            return self._force_to_single_choice(value, field_name)
            
        elif field_type == 4:  # 多选字段 - 转换为字符串数组
            return self._force_to_multi_choice(value, field_name)
            
        elif field_type == 5:  # 日期字段 - 强制转换为时间戳
            return self._force_to_timestamp(value, field_name)
            
        elif field_type == 7:  # 复选框字段 - 强制转换为布尔值
            return self._force_to_boolean(value, field_name)
            
        elif field_type == 11:  # 人员字段
            return self.convert_to_user_field(value)
            
        elif field_type == 13:  # 电话号码字段
            return str(value)
            
        elif field_type == 15:  # 超链接字段
            return self.convert_to_url_field(value)
            
        elif field_type == 17:  # 附件字段
            return self.convert_to_attachment_field(value)
            
        elif field_type in [18, 21]:  # 关联字段
            return self.convert_to_link_field(value)
            
        elif field_type == 22:  # 地理位置字段
            return str(value)
            
        elif field_type == 23:  # 群组字段
            return self.convert_to_user_field(value)
            
        elif field_type in [19, 20, 1001, 1002, 1003, 1004, 1005]:  # 只读字段
            self.logger.debug(f"字段 '{field_name}' 是只读字段，跳过设置")
            return None
            
        else:
            # 未知类型，默认转为字符串
            return str(value)
    
    def _force_to_number(self, value, field_name: str):
        """强制转换为数字，处理各种异常情况"""
        if isinstance(value, (int, float)):
            return value
        
        if isinstance(value, str):
            str_val = value.strip()
            
            # 处理空字符串
            if not str_val:
                return None
                
            # 处理常见的非数字表示
            non_numeric_map = {
                'null': None, 'n/a': None, 'na': None, '无': None, '空': None,
                '待定': None, 'tbd': None, 'pending': None, '未知': None,
            }
            if str_val.lower() in non_numeric_map:
                return non_numeric_map[str_val.lower()]
            
            # 清理数字字符串
            cleaned = str_val.replace(',', '').replace('￥', '').replace('$', '').replace('%', '')
            
            try:
                # 尝试转换为数字
                if '.' in cleaned:
                    return float(cleaned)
                return int(cleaned)
            except ValueError:
                # 如果包含文字，尝试提取数字部分
                import re
                numbers = re.findall(r'-?\d+\.?\d*', cleaned)
                if numbers:
                    try:
                        num = float(numbers[0]) if '.' in numbers[0] else int(numbers[0])
                        self.logger.warning(f"字段 '{field_name}': 从 '{value}' 中提取数字 {num}")
                        return num
                    except ValueError:
                        pass
                
                # 完全无法转换时，记录警告并返回None
                self.logger.warning(f"字段 '{field_name}': 无法将 '{value}' 转换为数字，将忽略此值")
                return None
        
        # 其他类型尝试直接转换
        try:
            return float(value)
        except (ValueError, TypeError):
            self.logger.warning(f"字段 '{field_name}': 无法将 {type(value).__name__} '{value}' 转换为数字")
            return None
    
    def _force_to_single_choice(self, value, field_name: str):
        """强制转换为单选值"""
        if isinstance(value, str):
            # 如果包含分隔符，取第一个值
            for separator in [',', ';', '|', '\n']:
                if separator in value:
                    first_value = value.split(separator)[0].strip()
                    if first_value:
                        self.logger.info(f"字段 '{field_name}': 多值转单选，选择第一个值: '{first_value}'")
                        return first_value
            return value.strip()
        
        return str(value)
    
    def _force_to_multi_choice(self, value, field_name: str):
        """强制转换为多选值数组"""
        if isinstance(value, str):
            # 尝试按分隔符拆分
            for separator in [',', ';', '|', '\n']:
                if separator in value:
                    values = [v.strip() for v in value.split(separator) if v.strip()]
                    return values if values else [str(value)]
            return [value.strip()] if value.strip() else []
        elif isinstance(value, (list, tuple)):
            return [str(v) for v in value if v]
        else:
            return [str(value)]
    
    def _force_to_timestamp(self, value, field_name: str):
        """强制转换为时间戳，增强日期解析能力"""
        # 如果已经是数字时间戳
        if isinstance(value, (int, float)):
            if value > 2524608000:  # 毫秒级
                return int(value)
            elif value > 946684800:  # 秒级，转为毫秒级
                return int(value * 1000)
            else:
                self.logger.warning(f"字段 '{field_name}': 数字 {value} 不在有效时间戳范围内")
                return None
        
        if isinstance(value, str):
            str_val = value.strip()
            
            # 处理纯数字字符串时间戳
            if str_val.isdigit():
                return self._force_to_timestamp(int(str_val), field_name)
            
            # 处理常见的非日期表示
            if str_val.lower() in ['null', 'n/a', 'na', '无', '空', '待定', 'tbd']:
                return None
            
            # 尝试解析各种日期格式
            import datetime as dt
            date_formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d',
                '%Y/%m/%d %H:%M:%S',
                '%Y/%m/%d',
                '%m/%d/%Y',
                '%d/%m/%Y',
                '%Y年%m月%d日',
                '%m月%d日',
                '%Y-%m-%d %H:%M',
                '%Y/%m/%d %H:%M'
            ]
            
            for fmt in date_formats:
                try:
                    dt_obj = dt.datetime.strptime(str_val, fmt)
                    return int(dt_obj.timestamp() * 1000)
                except ValueError:
                    continue
            
            # 如果都解析失败，记录警告
            self.logger.warning(f"字段 '{field_name}': 无法解析日期格式 '{value}'，将忽略此值")
            return None
        
        # 处理pandas时间戳
        if hasattr(value, 'timestamp'):
            return int(value.timestamp() * 1000)
        
        self.logger.warning(f"字段 '{field_name}': 无法将 {type(value).__name__} '{value}' 转换为时间戳")
        return None
    
    def _force_to_boolean(self, value, field_name: str):
        """强制转换为布尔值"""
        if isinstance(value, bool):
            return value
        
        if isinstance(value, (int, float)):
            return bool(value)
        
        if isinstance(value, str):
            str_val = value.strip().lower()
            
            # 真值映射
            true_values = ['true', '是', 'yes', '1', 'on', 'checked', '对', '正确', 'ok', 'y']
            # 假值映射
            false_values = ['false', '否', 'no', '0', 'off', 'unchecked', '', '错', '错误', 'n']
            
            if str_val in true_values:
                return True
            elif str_val in false_values:
                return False
            else:
                # 如果无法识别，按内容长度判断（非空为真）
                result = len(str_val) > 0
                self.logger.warning(f"字段 '{field_name}': 无法识别布尔值 '{value}'，按非空规则转换为 {result}")
                return result
        
        # 其他类型按Python的bool()规则转换
        return bool(value)

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

    def convert_to_user_field(self, value):
        """转换为人员字段格式"""
        if pd.isnull(value) or not value:
            return None
        
        # 如果已经是正确的字典格式
        if isinstance(value, dict) and 'id' in value:
            return [value]
        elif isinstance(value, list):
            # 如果是列表，检查每个元素
            result = []
            for item in value:
                if isinstance(item, dict) and 'id' in item:
                    result.append(item)
                elif isinstance(item, str) and item.strip():
                    result.append({"id": item.strip()})
            return result if result else None
        elif isinstance(value, str):
            # 字符串格式，可能是用户ID或多个用户ID用分隔符分开
            user_ids = []
            if ',' in value:
                user_ids = [uid.strip() for uid in value.split(',') if uid.strip()]
            elif ';' in value:
                user_ids = [uid.strip() for uid in value.split(';') if uid.strip()]
            else:
                user_ids = [value.strip()] if value.strip() else []
            
            return [{"id": uid} for uid in user_ids] if user_ids else None
        
        return None
    
    def convert_to_url_field(self, value):
        """转换为超链接字段格式"""
        if pd.isnull(value) or not value:
            return None
        
        # 如果已经是正确的字典格式
        if isinstance(value, dict) and 'link' in value:
            return value
        elif isinstance(value, str):
            # 简单URL字符串
            url_str = value.strip()
            if url_str.startswith(('http://', 'https://')):
                return {
                    "text": url_str,
                    "link": url_str
                }
            else:
                # 不是有效URL，作为文本处理
                return str(value)
        
        return str(value)
    
    def convert_to_attachment_field(self, value):
        """转换为附件字段格式"""
        if pd.isnull(value) or not value:
            return None
        
        # 如果已经是正确的字典格式
        if isinstance(value, dict) and 'file_token' in value:
            return [value]
        elif isinstance(value, list):
            result = []
            for item in value:
                if isinstance(item, dict) and 'file_token' in item:
                    result.append(item)
                elif isinstance(item, str) and item.strip():
                    result.append({"file_token": item.strip()})
            return result if result else None
        elif isinstance(value, str):
            # 字符串格式，可能是file_token
            token = value.strip()
            return [{"file_token": token}] if token else None
        
        return None
    
    def convert_to_link_field(self, value):
        """转换为关联字段格式"""
        if pd.isnull(value) or not value:
            return None
        
        # 如果已经是列表格式
        if isinstance(value, list):
            return [str(item) for item in value if item]
        elif isinstance(value, str):
            # 字符串格式，可能是record_id或多个record_id用分隔符分开
            record_ids = []
            if ',' in value:
                record_ids = [rid.strip() for rid in value.split(',') if rid.strip()]
            elif ';' in value:
                record_ids = [rid.strip() for rid in value.split(';') if rid.strip()]
            else:
                record_ids = [value.strip()] if value.strip() else []
            
            return record_ids if record_ids else None
        
        return [str(value)] if value else None

    def df_to_records(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> List[Dict]:
        """将DataFrame转换为飞书记录格式"""
        records = []
        for _, row in df.iterrows():
            fields = {}
            for k, v in row.to_dict().items():
                if pd.notnull(v):
                    converted_value = self.convert_field_value_safe(str(k), v, field_types)
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
                # 修复参数传递顺序：先传递固定参数，再传递批次数据
                if processor_func(*args, batch, **kwargs):
                    success_count += 1
                    self.logger.info(f"批次 {batch_num}/{total_batches} 处理成功 ({len(batch)} 条记录)")
                else:
                    self.logger.error(f"批次 {batch_num}/{total_batches} 处理失败")
            except Exception as e:
                self.logger.error(f"批次 {batch_num}/{total_batches} 处理异常: {e}")
        
        self.logger.info(f"批处理完成: {success_count}/{total_batches} 个批次成功")
        return success_count == total_batches
        
    def sync_full(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
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
                    converted_value = self.convert_field_value_safe(str(k), v, field_types)
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
    
    def sync_incremental(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
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
                        converted_value = self.convert_field_value_safe(str(k), v, field_types)
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
    
    def sync_overwrite(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
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
    
    def sync_clone(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
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
        
        # 重置转换统计
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
        
        # 确保字段存在并获取字段类型信息
        success, field_types = self.ensure_fields_exist(df)
        if not success:
            self.logger.error("字段创建失败，同步终止")
            return False
        
        self.logger.info(f"获取到 {len(field_types)} 个字段的类型信息")
        
        # 显示字段类型映射摘要
        self._show_field_analysis_summary(df, field_types)
        
        # 预检查：分析数据与字段类型的匹配情况
        self.logger.info("\n🔍 正在分析数据与字段类型匹配情况...")
        mismatch_warnings = []
        sample_size = min(50, len(df))  # 检查前50行作为样本
        
        for _, row in df.head(sample_size).iterrows():
            for col_name, value in row.to_dict().items():
                if pd.notnull(value) and col_name in field_types:
                    field_type = field_types[col_name]
                    # 简单的类型不匹配检测
                    if field_type == 2 and isinstance(value, str):  # 数字字段但是字符串值
                        if not self._is_number_string(str(value).strip()):
                            mismatch_warnings.append(f"字段 '{col_name}' 是数字类型，但包含非数字值: '{value}'")
                    elif field_type == 5 and isinstance(value, str):  # 日期字段但是字符串值
                        if not (self._is_timestamp_string(str(value)) or self._is_date_string(str(value))):
                            mismatch_warnings.append(f"字段 '{col_name}' 是日期类型，但包含非日期值: '{value}'")
        
        if mismatch_warnings:
            unique_warnings = list(set(mismatch_warnings[:10]))  # 显示前10个唯一警告
            self.logger.warning(f"发现 {len(set(mismatch_warnings))} 种数据类型不匹配情况（样本检查）:")
            for warning in unique_warnings:
                self.logger.warning(f"  • {warning}")
            self.logger.info("程序将自动进行强制类型转换...")
        else:
            self.logger.info("✅ 数据类型匹配良好")
        
        # 根据同步模式执行对应操作
        sync_result = False
        if self.config.sync_mode == SyncMode.FULL:
            sync_result = self.sync_full(df, field_types)
        elif self.config.sync_mode == SyncMode.INCREMENTAL:
            sync_result = self.sync_incremental(df, field_types)
        elif self.config.sync_mode == SyncMode.OVERWRITE:
            sync_result = self.sync_overwrite(df, field_types)
        elif self.config.sync_mode == SyncMode.CLONE:
            sync_result = self.sync_clone(df, field_types)
        else:
            self.logger.error(f"不支持的同步模式: {self.config.sync_mode}")
            return False
        
        # 输出转换统计信息
        self.report_conversion_stats()
        
        return sync_result
    
    def report_conversion_stats(self):
        """输出数据转换统计报告"""
        total_conversions = self.conversion_stats['success'] + self.conversion_stats['failed']
        
        if total_conversions > 0:
            success_rate = (self.conversion_stats['success'] / total_conversions) * 100
            
            self.logger.info("=" * 60)
            self.logger.info("🔄 数据类型转换统计报告")
            self.logger.info("=" * 60)
            self.logger.info(f"📊 总转换次数: {total_conversions}")
            self.logger.info(f"✅ 成功转换: {self.conversion_stats['success']} ({success_rate:.1f}%)")
            self.logger.info(f"❌ 失败转换: {self.conversion_stats['failed']}")
            
            if self.conversion_stats['failed'] > 0:
                failure_rate = (self.conversion_stats['failed'] / total_conversions) * 100
                self.logger.warning(f"失败率: {failure_rate:.1f}%")
            
            if self.conversion_stats['warnings']:
                warning_count = len(self.conversion_stats['warnings'])
                self.logger.info(f"⚠️  警告数量: {warning_count}")
                
                # 去重并统计相同警告的数量
                warning_counts = {}
                for warning in self.conversion_stats['warnings']:
                    warning_counts[warning] = warning_counts.get(warning, 0) + 1
                
                self.logger.info("\n⚠️  数据转换警告详情:")
                for warning, count in warning_counts.items():
                    self.logger.warning(f"  [{count}次] {warning}")
            
            self.logger.info("\n💡 优化建议:")
            if success_rate < 90:
                self.logger.info("1. 数据质量较低，建议清理Excel数据")
                self.logger.info("2. 检查数据格式是否标准化")
            if self.conversion_stats['failed'] > 0:
                self.logger.info("3. 查看上述警告，调整数据格式或飞书字段类型")
                self.logger.info("4. 对于无法转换的字段，考虑使用文本类型")
            
            self.logger.info("\n📋 字段类型转换规则:")
            self.logger.info("• 数字字段: 自动提取数值，清理货币符号和千分位")
            self.logger.info("• 单选字段: 多值时自动选择第一个")
            self.logger.info("• 多选字段: 支持逗号、分号、竖线分隔")
            self.logger.info("• 日期字段: 支持多种日期格式自动识别")
            self.logger.info("• 布尔字段: 智能识别是/否、true/false等")
            
            self.logger.info("=" * 60)
        else:
            self.logger.info("📊 没有进行数据类型转换")
    
    def _show_field_analysis_summary(self, df: pd.DataFrame, field_types: Dict[str, int]):
        """显示字段分析摘要"""
        self.logger.info("\n📋 字段类型映射摘要:")
        self.logger.info("-" * 50)
        
        for col_name in df.columns:
            if col_name in field_types:
                field_type = field_types[col_name]
                type_name = self._get_field_type_name(field_type)
                self.logger.info(f"  {col_name} → {type_name} (类型码: {field_type})")
            else:
                self.logger.warning(f"  {col_name} → 未知字段类型")
                
        self.logger.info("-" * 50)


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
        parser = argparse.ArgumentParser(description='XTF - Excel To Feishu 同步工具')
        
        # 基础配置
        parser.add_argument('--config', '-c', type=str, default='config.yaml',
                          help='配置文件路径 (默认: config.yaml)')
        parser.add_argument('--file-path', type=str, help='Excel文件路径')
        parser.add_argument('--app-id', type=str, help='飞书应用ID')
        parser.add_argument('--app-secret', type=str, help='飞书应用密钥')
        parser.add_argument('--app-token', type=str, help='多维表格应用Token')
        parser.add_argument('--table-id', type=str, help='数据表ID')
        
        # 同步设置
        parser.add_argument('--sync-mode', type=str, 
                          choices=['full', 'incremental', 'overwrite', 'clone'],
                          help='同步模式')
        parser.add_argument('--index-column', type=str, help='索引列名')
        
        # 性能设置
        parser.add_argument('--batch-size', type=int,
                          help='批处理大小')
        parser.add_argument('--rate-limit-delay', type=float,
                          help='接口调用间隔秒数')
        parser.add_argument('--max-retries', type=int,
                          help='最大重试次数')
        
        # 功能开关
        parser.add_argument('--no-create-fields', action='store_true',
                          help='不自动创建缺失字段')
        
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
            'batch_size': 500,
            'rate_limit_delay': 0.5,
            'max_retries': 3,
            'create_missing_fields': True,
            'log_level': 'INFO'
        }
        
        # 尝试从配置文件加载，覆盖默认值
        if Path(args.config).exists():
            file_config = cls.load_from_file(args.config)
            if file_config:
                config_data.update(file_config)
                print(f"✅ 已从配置文件加载参数: {args.config}")
                
                # 显示从配置文件加载的参数
                loaded_params = []
                for key, value in file_config.items():
                    if key in config_data:
                        loaded_params.append(f"{key}={value}")
                if loaded_params:
                    print(f"📋 配置文件参数: {', '.join(loaded_params)}")
            else:
                print(f"⚠️  配置文件 {args.config} 加载失败，使用默认值")
        else:
            print(f"⚠️  配置文件 {args.config} 不存在，使用默认值")
        
        # 命令行参数覆盖文件配置（只有当明确提供时）
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
        if args.app_token:
            config_data['app_token'] = args.app_token
            cli_overrides.append(f"app_token={args.app_token[:8]}...")
        if args.table_id:
            config_data['table_id'] = args.table_id
            cli_overrides.append(f"table_id={args.table_id}")
        if args.index_column:
            config_data['index_column'] = args.index_column
            cli_overrides.append(f"index_column={args.index_column}")
        
        # 高级参数（只有明确提供时才覆盖）
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
        if args.no_create_fields:  # 这个是action='store_true'，只有指定时才为True
            config_data['create_missing_fields'] = False
            cli_overrides.append("create_missing_fields=False")
        if args.log_level is not None:
            config_data['log_level'] = args.log_level
            cli_overrides.append(f"log_level={args.log_level}")
        
        # 显示命令行覆盖的参数
        if cli_overrides:
            print(f"🔧 命令行参数覆盖: {', '.join(cli_overrides)}")
        
        # 验证必需参数
        required_fields = ['file_path', 'app_id', 'app_secret', 'app_token', 'table_id']
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
    print("     XTF工具")
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
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  接口调用间隔: {config.rate_limit_delay}秒")
        print(f"  最大重试次数: {config.max_retries}")
        print(f"  自动创建字段: {'是' if config.create_missing_fields else '否'}")
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
        sync_engine = XTFSyncEngine(config)
        
        # 执行同步
        print(f"\n🚀 开始执行 {config.sync_mode.value} 同步...")
        start_time = time.time()
        
        success = sync_engine.sync(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            print(f"\n✅ 同步完成！耗时: {duration:.2f} 秒")
        else:
            print(f"\n❌ 同步过程中出现错误，请查看日志文件")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {str(e)}")
        logging.error(f"程序异常: {e}", exc_info=True)


if __name__ == "__main__":
    main()