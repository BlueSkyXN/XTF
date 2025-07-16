#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据转换模块
提供Excel数据到飞书字段格式的转换功能
"""

import re
import pandas as pd
import hashlib
import logging
from typing import Dict, Any, List, Optional
import datetime as dt


class DataConverter:
    """数据转换器"""
    
    def __init__(self):
        """初始化数据转换器"""
        self.logger = logging.getLogger(__name__)
        
        # 类型转换统计
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
    
    def reset_stats(self):
        """重置转换统计"""
        self.conversion_stats = {
            'success': 0,
            'failed': 0,
            'warnings': []
        }
    
    def get_index_value_hash(self, row: pd.Series, index_column: Optional[str]) -> Optional[str]:
        """计算索引值的哈希"""
        if index_column and index_column in row:
            value = str(row[index_column])
            return hashlib.md5(value.encode('utf-8')).hexdigest()
        return None
    
    def build_record_index(self, records: List[Dict], index_column: Optional[str]) -> Dict[str, Dict]:
        """构建记录索引"""
        index = {}
        if not index_column:
            return index
        
        for record in records:
            fields = record.get('fields', {})
            if index_column in fields:
                index_value = str(fields[index_column])
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
    
    def get_field_type_name(self, field_type: int) -> str:
        """获取字段类型的中文名称"""
        type_names = {
            1: "文本", 2: "数字", 3: "单选", 4: "多选", 5: "日期", 
            7: "复选框", 11: "人员", 13: "电话", 15: "超链接", 
            17: "附件", 18: "单向关联", 21: "双向关联", 22: "地理位置", 23: "群组"
        }
        return type_names.get(field_type, f"未知类型({field_type})")
    
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