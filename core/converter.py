#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一数据转换模块
提供多维表格和电子表格的数据转换功能
"""

import re
import pandas as pd
import hashlib
import logging
from typing import Dict, Any, List, Optional
import datetime as dt

from .config import TargetType


class DataConverter:
    """统一数据转换器"""
    
    def __init__(self, target_type: TargetType):
        """
        初始化数据转换器
        
        Args:
            target_type: 目标类型（多维表格或电子表格）
        """
        self.target_type = target_type
        self.logger = logging.getLogger('XTF.converter')
        
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
    
    # ========== 多维表格转换方法 ==========
    
    def build_record_index(self, records: List[Dict], index_column: Optional[str]) -> Dict[str, Dict]:
        """构建多维表格记录索引"""
        index = {}
        if not index_column:
            return index
        
        for record in records:
            fields = record.get('fields', {})
            if index_column in fields:
                raw_value = fields[index_column]
                
                # 处理富文本格式：[{'text': '内容', 'type': 'text'}] 
                if isinstance(raw_value, list) and len(raw_value) > 0:
                    if isinstance(raw_value[0], dict) and 'text' in raw_value[0]:
                        index_value = raw_value[0]['text']
                    else:
                        index_value = str(raw_value[0])
                elif isinstance(raw_value, dict) and 'text' in raw_value:
                    index_value = raw_value['text']
                else:
                    index_value = str(raw_value)
                
                index_hash = hashlib.md5(index_value.encode('utf-8')).hexdigest()
                index[index_hash] = record
        
        return index
    
    def _detect_excel_validation(self, df: pd.DataFrame, column_name: str) -> tuple:
        """
        检测Excel列是否包含数据验证(下拉列表)
        
        Returns:
            (是否有验证, 验证类型描述)
        """
        try:
            # 方法1: 检查pandas是否保留了Excel验证信息
            if hasattr(df, '_excel_validation_info'):
                validation_info = df._excel_validation_info.get(column_name)
                if validation_info:
                    return True, validation_info.get('type', 'unknown')
            
            # 方法2: 基于数据模式推断
            column_data = df[column_name].dropna()
            if len(column_data) == 0:
                return False, "empty"
            
            unique_values = set(str(v) for v in column_data)
            unique_count = len(unique_values)
            total_count = len(column_data)
            
            # 下拉列表特征检测
            validation_indicators = []
            
            # 特征1: 极少的唯一值且高重复率
            if unique_count <= 8 and unique_count / total_count <= 0.3:
                validation_indicators.append("low_unique_high_repeat")
            
            # 特征2: 值都是简短的标识符
            if all(len(str(v)) <= 20 for v in unique_values):
                validation_indicators.append("short_identifiers")
            
            # 特征3: 没有特殊字符和复杂格式
            if all(not re.search(r'[^\w\s\-_()(（）)]', str(v)) for v in unique_values):
                validation_indicators.append("simple_format")
            
            # 特征4: 值看起来像枚举选项
            enum_patterns = [
                r'^(状态|级别|类型|分类)[\w\s]*$',  # 状态类
                r'^(高|中|低)$',                   # 等级类
                r'^(是|否|true|false)$',          # 布尔类
                r'^(完成|进行中|待开始|已取消)$',    # 流程状态
                r'^[A-Z]{1,3}$',                  # 简短代码
            ]
            
            enum_matches = sum(
                1 for v in unique_values 
                if any(re.match(pattern, str(v), re.IGNORECASE) for pattern in enum_patterns)
            )
            
            if enum_matches >= unique_count * 0.6:  # 60%以上匹配枚举模式
                validation_indicators.append("enum_pattern")
            
            # 判断是否可能是下拉列表
            if len(validation_indicators) >= 3:
                return True, f"suspected_dropdown({','.join(validation_indicators)})"
            elif len(validation_indicators) >= 2 and unique_count <= 5:
                return True, f"possible_dropdown({','.join(validation_indicators)})"
            
            return False, "no_validation_detected"
            
        except Exception as e:
            return False, f"detection_error: {e}"
    
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
                # 布尔值检测 - 排除纯数字字符串
                if str_val.lower() in ['true', 'false', '是', '否', 'yes', 'no', 'on', 'off']:
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
        
        # 推断飞书字段类型 (传统方法，保持兼容性)
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
        is_timestamp, _ = self._is_timestamp_enhanced(s)
        return is_timestamp
    
    def _is_timestamp_enhanced(self, s: str) -> tuple:
        """增强的时间戳检测"""
        from datetime import datetime
        
        if not s.isdigit():
            return False, 0.0
        
        try:
            timestamp = int(s)
            current_year = datetime.now().year
            
            # 秒级时间戳: 1970-2050年
            if 946684800 <= timestamp <= 2524608000:  # 2000-2050
                confidence = 0.9 if 1640995200 <= timestamp <= 1893456000 else 0.7  # 2022-2030更高置信度
                return True, confidence
                
            # 毫秒级时间戳: 2000-2050年  
            elif 946656000000 <= timestamp <= 2524579200000:
                confidence = 0.85
                return True, confidence
                
            # 微秒级(Excel有时导出): 过于长的数字降低置信度
            elif len(s) >= 13:
                return True, 0.3
                
        except ValueError:
            pass
            
        return False, 0.0
    
    def _is_date_string(self, s: str) -> bool:
        """检测字符串是否为日期格式"""
        is_date, _, _ = self._is_date_string_enhanced(s)
        return is_date
    
    def _is_date_string_enhanced(self, s: str) -> tuple:
        """
        增强的日期检测，返回(是否日期, 置信度, 检测到的格式)
        """
        from datetime import datetime
        
        s = s.strip()
        if not s:
            return False, 0.0, ""
        
        # 扩展的日期格式模式 (按常见程度排序)
        date_patterns = [
            # 标准ISO格式 (最高置信度)
            (r'^\d{4}-\d{2}-\d{2}$', '%Y-%m-%d', 0.95),                    # 2024-01-01
            (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', '%Y-%m-%d %H:%M:%S', 0.95), # 2024-01-01 12:30:45
            (r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', '%Y-%m-%d %H:%M', 0.9),   # 2024-01-01 12:30
            
            # 常见分隔符格式
            (r'^\d{4}/\d{1,2}/\d{1,2}$', '%Y/%m/%d', 0.85),               # 2024/1/1
            (r'^\d{1,2}/\d{1,2}/\d{4}$', '%m/%d/%Y', 0.7),                # 1/1/2024 (存在歧义)
            (r'^\d{1,2}-\d{1,2}-\d{4}$', '%m-%d-%Y', 0.7),                # 1-1-2024
            
            # 中文格式
            (r'^\d{4}年\d{1,2}月\d{1,2}日$', '%Y年%m月%d日', 0.9),         # 2024年1月1日
            (r'^\d{1,2}月\d{1,2}日$', '%m月%d日', 0.8),                    # 1月1日
            (r'^\d{4}\.\d{1,2}\.\d{1,2}$', '%Y.%m.%d', 0.8),              # 2024.1.1
            
            # Excel常见格式
            (r'^\d{4}-\d{1,2}-\d{1,2}T\d{2}:\d{2}:\d{2}', '%Y-%m-%dT%H:%M:%S', 0.95), # ISO时间
        ]
        
        for pattern, fmt, base_confidence in date_patterns:
            if re.match(pattern, s):
                try:
                    # 尝试解析验证日期有效性
                    if 'T' in fmt:  # ISO格式特殊处理
                        parsed_date = datetime.fromisoformat(s.replace('T', ' ')[:19])
                    else:
                        parsed_date = datetime.strptime(s, fmt)
                    
                    # 合理性检查: 1900-2100年
                    if 1900 <= parsed_date.year <= 2100:
                        # 根据完整性调整置信度
                        if parsed_date.hour == 0 and parsed_date.minute == 0:
                            confidence = base_confidence * 0.95  # 纯日期略降置信度
                        else:
                            confidence = base_confidence  # 包含时间信息高置信度
                        
                        return True, confidence, fmt
                        
                except ValueError:
                    continue  # 格式匹配但解析失败，继续下一个
        
        return False, 0.0, ""
    
    def _suggest_feishu_field_type(self, primary_type: str, unique_values: set, 
                                  total_count: int, confidence: float) -> int:
        """根据数据特征推荐飞书字段类型 (保留兼容方法，建议使用analyze_excel_column_data_enhanced)"""
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
    
    def _suggest_feishu_field_type_raw(self) -> tuple:
        """
        原值策略 - 所有字段都使用文本类型，保持原始数据
        
        Returns:
            (字段类型, 推荐理由)
        """
        return 1, "raw策略，所有字段使用文本类型保持原值"
    
    def _suggest_feishu_field_type_base(self, primary_type: str, unique_values: set, 
                                       total_count: int, confidence: float) -> tuple:
        """
        基础策略 - 仅创建文本/数字/日期三种基础类型
        
        Returns:
            (字段类型, 推荐理由)
        """
        # 1. 数字类型
        if primary_type == 'number' and confidence >= 0.8:
            return 2, f"数字类型，置信度{confidence:.1%}"
        
        # 2. 日期类型 - 需要高置信度
        if primary_type == 'datetime' and confidence >= 0.85:
            return 5, f"日期类型，置信度{confidence:.1%}"
        
        # 3. 所有其他情况都使用文本类型
        if primary_type == 'datetime':
            return 1, f"日期格式置信度不够({confidence:.1%})，使用文本类型"
        elif primary_type == 'number':
            return 1, f"数字格式置信度不够({confidence:.1%})，使用文本类型"
        else:
            return 1, "基础策略，使用文本类型"
    
    def _suggest_feishu_field_type_auto(self, primary_type: str, unique_values: set, 
                                       total_count: int, confidence: float,
                                       has_excel_validation: bool = False) -> tuple:
        """
        自动策略 - 在基础类型上增加Excel类型检测（单选多选等）
        
        Args:
            has_excel_validation: 是否检测到Excel数据验证(下拉列表等)
        
        Returns:
            (字段类型, 推荐理由)
        """
        unique_count = len(unique_values)
        
        # 1. 基础类型：数字
        if primary_type == 'number' and confidence >= 0.8:
            return 2, f"数字类型，置信度{confidence:.1%}"
        
        # 2. 基础类型：日期
        if primary_type == 'datetime' and confidence >= 0.85:
            return 5, f"日期类型，置信度{confidence:.1%}"
        
        # 3. Excel类型检测：仅在检测到Excel验证时推荐
        if primary_type == 'string' and has_excel_validation:
            if unique_count <= 15 and unique_count / total_count <= 0.4:
                return 3, f"Excel下拉列表，唯一值{unique_count}个，推荐单选"
            elif any(',' in str(v) or ';' in str(v) or '|' in str(v) for v in unique_values):
                return 4, f"Excel下拉列表包含分隔符，推荐多选"
        
        # 4. 所有其他情况使用文本类型
        if primary_type == 'datetime':
            return 1, f"日期格式置信度不够({confidence:.1%})，使用文本类型"
        elif primary_type == 'number':
            return 1, f"数字格式置信度不够({confidence:.1%})，使用文本类型"
        else:
            return 1, "未检测到Excel验证，使用文本类型"
    
    def _suggest_feishu_field_type_intelligence(self, primary_type: str, unique_values: set, 
                                               total_count: int, confidence: float,
                                               config) -> tuple:
        """
        智能策略 - 基于置信度算法，使用配置文件中的阈值
        
        Returns:
            (字段类型, 推荐理由)
        """
        unique_count = len(unique_values)
        
        # 1. 数字类型
        if primary_type == 'number' and confidence >= 0.8:
            return 2, f"数字类型，置信度{confidence:.1%}"
        
        # 2. 日期类型
        if primary_type == 'datetime' and confidence >= getattr(config, 'intelligence_date_confidence', 0.85):
            return 5, f"日期类型，置信度{confidence:.1%}"
        
        # 3. 布尔类型
        if (primary_type == 'boolean' and 
            confidence >= getattr(config, 'intelligence_boolean_confidence', 0.95) and 
            unique_count <= 3):
            return 7, f"布尔类型，置信度{confidence:.1%}"
        
        # 4. 字符串类型的智能判断
        if primary_type == 'string':
            choice_threshold = getattr(config, 'intelligence_choice_confidence', 0.9)
            # 单选检测
            if (unique_count <= 20 and 
                unique_count / total_count <= 0.5 and
                confidence >= choice_threshold):
                return 3, f"智能判断为单选（{unique_count}个选项，置信度{confidence:.1%}）"
            
            # 多选检测
            elif (any(',' in str(v) or ';' in str(v) or '|' in str(v) for v in unique_values) and
                  confidence >= choice_threshold):
                return 4, f"智能检测到多选模式，置信度{confidence:.1%}"
        
        # 5. 兜底策略
        if primary_type == 'datetime':
            date_threshold = getattr(config, 'intelligence_date_confidence', 0.85)
            return 1, f"日期置信度不够({confidence:.1%}<{date_threshold:.1%})，使用文本类型"
        elif primary_type == 'boolean':
            bool_threshold = getattr(config, 'intelligence_boolean_confidence', 0.95)
            return 1, f"布尔置信度不够({confidence:.1%}<{bool_threshold:.1%})，使用文本类型"
        elif primary_type == 'string':
            choice_threshold = getattr(config, 'intelligence_choice_confidence', 0.9)
            return 1, f"选择类型置信度不够({confidence:.1%}<{choice_threshold:.1%})，使用文本类型"
        else:
            return 1, "智能分析无法确定类型，使用文本类型"
    
    def get_field_type_name(self, field_type: int) -> str:
        """获取字段类型的中文名称"""
        type_names = {
            1: "文本", 2: "数字", 3: "单选", 4: "多选", 5: "日期", 
            7: "复选框", 11: "人员", 13: "电话", 15: "超链接", 
            17: "附件", 18: "单向关联", 21: "双向关联", 22: "地理位置", 23: "群组"
        }
        return type_names.get(field_type, f"未知类型({field_type})")
    
    def analyze_excel_column_data_enhanced(self, df: pd.DataFrame, column_name: str, 
                                          strategy: str = 'base', config = None) -> Dict[str, Any]:
        """
        增强的Excel列数据分析 - 支持三种字段类型策略
        
        Args:
            df: Excel数据
            column_name: 列名
            strategy: 字段类型策略 ('base' | 'auto' | 'intelligence')
            config: 配置对象 (intelligence策略必需)
            
        Returns:
            包含推荐字段类型和理由的分析结果
        """
        from .config import FieldTypeStrategy
        
        # 1. 检测Excel验证信息（仅auto策略需要）
        has_validation = False
        validation_type = "not_checked"
        if strategy == FieldTypeStrategy.AUTO.value:
            has_validation, validation_type = self._detect_excel_validation(df, column_name)
        
        # 2. 基础数据分析
        analysis = self.analyze_excel_column_data(df, column_name)  # 复用现有逻辑
        
        # 3. 增强的日期检测
        if analysis['primary_type'] == 'string':
            column_data = df[column_name].dropna()
            date_confidence_sum = 0
            date_count = 0
            
            for value in column_data:
                is_date, confidence_val, format_type = self._is_date_string_enhanced(str(value))
                if is_date:
                    date_confidence_sum += confidence_val
                    date_count += 1
            
            if date_count > 0:
                avg_date_confidence = date_confidence_sum / len(column_data)
                if avg_date_confidence >= 0.6:  # 60%以上是高质量日期
                    analysis['primary_type'] = 'datetime'
                    analysis['confidence'] = avg_date_confidence
        
        # 4. 应用字段类型策略
        unique_values = set(str(v) for v in df[column_name].dropna())
        
        if strategy == FieldTypeStrategy.RAW.value:
            suggested_type, reason = self._suggest_feishu_field_type_raw()
        elif strategy == FieldTypeStrategy.BASE.value:
            suggested_type, reason = self._suggest_feishu_field_type_base(
                analysis['primary_type'], unique_values, analysis['total_count'], analysis['confidence']
            )
        elif strategy == FieldTypeStrategy.AUTO.value:
            suggested_type, reason = self._suggest_feishu_field_type_auto(
                analysis['primary_type'], unique_values, analysis['total_count'], 
                analysis['confidence'], has_validation
            )
        elif strategy == FieldTypeStrategy.INTELLIGENCE.value:
            if config is None:
                raise ValueError("Intelligence策略需要配置对象")
            suggested_type, reason = self._suggest_feishu_field_type_intelligence(
                analysis['primary_type'], unique_values, analysis['total_count'], 
                analysis['confidence'], config
            )
        else:
            # 兜底使用基础策略
            suggested_type, reason = self._suggest_feishu_field_type_base(
                analysis['primary_type'], unique_values, analysis['total_count'], analysis['confidence']
            )
        
        # 5. 更新分析结果
        analysis.update({
            'suggested_feishu_type': suggested_type,
            'recommendation_reason': reason,
            'has_excel_validation': has_validation,
            'validation_type': validation_type,
            'strategy_used': strategy
        })
        
        return analysis
    
    def convert_field_value_safe(self, field_name: str, value, field_types: Optional[Dict[str, int]] = None):
        """安全的字段值转换"""
        if pd.isnull(value):
            return None
        
        # 多维表格模式使用复杂转换
        if self.target_type == TargetType.BITABLE:
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
        else:
            # 电子表格模式使用简单转换
            return self.simple_convert_value(value)
    
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
        """强制转换为数字"""
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
                    return [v.strip() for v in value.split(separator) if v.strip()]
            return [value.strip()] if value.strip() else []
        elif isinstance(value, (list, tuple)):
            return [str(v) for v in value if v]
        else:
            return [str(value)]
    
    def _force_to_timestamp(self, value, field_name: str):
        """强制转换为时间戳"""
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
    
    def simple_convert_value(self, value):
        """简单转换数值类型（电子表格模式）"""
        if pd.isnull(value):
            return ""
        else:
            # 转换为字符串或基本类型
            if isinstance(value, (int, float)):
                return value
            elif isinstance(value, bool):
                return value
            else:
                return str(value)
    
    # ========== 复杂字段类型转换（多维表格专用） ==========
    
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

    # ========== 电子表格转换方法 ==========
    
    def build_data_index(self, df: pd.DataFrame, index_column: Optional[str]) -> Dict[str, int]:
        """构建电子表格数据索引（哈希 -> 行号）"""
        index = {}
        if not index_column:
            return index
        
        for idx, row in df.iterrows():
            index_hash = self.get_index_value_hash(row, index_column)
            if index_hash:
                index[index_hash] = idx
        
        return index
    
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
    
    def df_to_values(self, df: pd.DataFrame, include_headers: bool = True, 
                     selected_columns: Optional[List[str]] = None) -> List[List[Any]]:
        """将DataFrame转换为电子表格值格式，支持列过滤"""
        values = []
        
        # 应用列过滤
        if selected_columns:
            # 验证列是否存在
            valid_columns = [col for col in selected_columns if col in df.columns]
            if len(valid_columns) != len(selected_columns):
                missing = [col for col in selected_columns if col not in df.columns]
                self.logger.warning(f"指定的列不存在: {missing}")
            df = df[valid_columns] if valid_columns else df
        
        # 添加表头
        if include_headers:
            values.append(df.columns.tolist())
        
        # 添加数据行
        for _, row in df.iterrows():
            row_values = []
            for value in row:
                converted_value = self.simple_convert_value(value)
                row_values.append(converted_value)
            values.append(row_values)
        
        return values
    
    def df_to_column_data(self, df: pd.DataFrame, selected_columns: Optional[List[str]] = None) -> Dict[str, List[Any]]:
        """将DataFrame转换为按列组织的数据字典，用于选择性列操作"""
        # 应用列过滤
        if selected_columns:
            valid_columns = [col for col in selected_columns if col in df.columns]
            if len(valid_columns) != len(selected_columns):
                missing = [col for col in selected_columns if col not in df.columns]
                self.logger.warning(f"指定的列不存在: {missing}")
            df = df[valid_columns] if valid_columns else df
        
        column_data = {}
        for col in df.columns:
            # 修复：不应包含表头，只包含数据
            col_values = []
            # 添加数据
            for value in df[col]:
                converted_value = self.simple_convert_value(value)
                col_values.append(converted_value)
            column_data[col] = col_values
        
        return column_data
    
    def get_column_positions(self, df: pd.DataFrame, selected_columns: Optional[List[str]] = None) -> Dict[str, int]:
        """获取列在原始DataFrame中的位置映射（1-based）"""
        if selected_columns:
            valid_columns = [col for col in selected_columns if col in df.columns]
        else:
            valid_columns = df.columns.tolist()
        
        positions = {}
        for col in valid_columns:
            # 在完整DataFrame中的位置（1-based）
            positions[col] = df.columns.get_loc(col) + 1
        
        return positions
    
    def values_to_df(self, values: List[List[Any]]) -> pd.DataFrame:
        """将电子表格值格式转换为DataFrame"""
        if not values:
            return pd.DataFrame()
        
        # 清理数据：移除完全空的行和列
        cleaned_values = []
        for row in values:
            # 移除行尾的空值
            while row and (row[-1] is None or row[-1] == '' or str(row[-1]).strip() == ''):
                row = row[:-1]
            # 如果行不为空，则保留
            if row and any(cell is not None and str(cell).strip() != '' for cell in row):
                cleaned_values.append(row)
        
        if not cleaned_values:
            return pd.DataFrame()
        
        # 第一行作为表头
        headers = cleaned_values[0] if cleaned_values else []
        data_rows = cleaned_values[1:] if len(cleaned_values) > 1 else []
        
        # 清理表头：移除空的列名
        valid_headers = []
        valid_col_indices = []
        for i, header in enumerate(headers):
            if header is not None and str(header).strip() != '':
                valid_headers.append(str(header).strip())
                valid_col_indices.append(i)
        
        # 如果没有有效的表头，返回空DataFrame
        if not valid_headers:
            return pd.DataFrame()
        
        # 清理数据行：只保留有效列的数据
        cleaned_data_rows = []
        for row in data_rows:
            cleaned_row = []
            for i in valid_col_indices:
                if i < len(row):
                    cleaned_row.append(row[i])
                else:
                    cleaned_row.append(None)
            cleaned_data_rows.append(cleaned_row)
        
        # 创建DataFrame
        if cleaned_data_rows:
            df = pd.DataFrame(cleaned_data_rows, columns=valid_headers)
        else:
            df = pd.DataFrame(columns=valid_headers)
        
        return df
    
    def get_range_string(self, sheet_id: str, start_row: int, start_col: str, end_row: int, end_col: str) -> str:
        """生成范围字符串"""
        return f"{sheet_id}!{start_col}{start_row}:{end_col}{end_row}"

    # ========== 统一接口方法 ==========
    
    def df_to_records(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> List[Dict]:
        """将DataFrame转换为飞书记录格式（多维表格模式）"""
        if self.target_type != TargetType.BITABLE:
            raise ValueError("df_to_records 只支持多维表格模式")
        
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
            if self.target_type == TargetType.BITABLE:
                self.logger.info("• 数字字段: 自动提取数值，清理货币符号和千分位")
                self.logger.info("• 单选字段: 多值时自动选择第一个")
                self.logger.info("• 多选字段: 支持逗号、分号、竖线分隔")
                self.logger.info("• 日期字段: 支持多种日期格式自动识别")
                self.logger.info("• 布尔字段: 智能识别是/否、true/false等")
            else:
                self.logger.info("• 电子表格模式: 保持原始数据类型，简单转换")
            
            self.logger.info("=" * 60)
        else:
            self.logger.info("📊 没有进行数据类型转换")
    
    def generate_sheet_field_config(self, df: pd.DataFrame, strategy: str = 'base', 
                                   config = None) -> Dict[str, Any]:
        """
        为电子表格生成智能字段配置
        
        Args:
            df: Excel数据
            strategy: 字段类型策略
            config: 配置对象
            
        Returns:
            字段配置字典 {
                'dropdown_configs': [{'column': 'A', 'options': [...], 'colors': [...]}],
                'date_columns': ['B', 'C'],
                'number_columns': ['D', 'E']
            }
        """
        field_config = {
            'dropdown_configs': [],
            'date_columns': [],
            'number_columns': []
        }
        
        for column_name in df.columns:
            # 分析每列数据
            analysis = self.analyze_excel_column_data_enhanced(df, column_name, strategy, config)
            
            # 根据分析结果生成配置
            if analysis['suggested_feishu_type'] == 3:  # 单选
                # 生成下拉列表配置
                unique_values = list(set(str(v) for v in df[column_name].dropna()))
                if len(unique_values) <= 20:  # 合理的选项数量
                    colors = self._generate_option_colors(unique_values)
                    field_config['dropdown_configs'].append({
                        'column': column_name,
                        'options': unique_values,
                        'colors': colors,
                        'multiple': False
                    })
            elif analysis['suggested_feishu_type'] == 4:  # 多选
                # 生成多选下拉列表配置
                all_options = set()
                for value in df[column_name].dropna():
                    value_str = str(value)
                    # 按分隔符拆分
                    for sep in [',', ';', '|']:
                        if sep in value_str:
                            all_options.update(opt.strip() for opt in value_str.split(sep))
                            break
                    else:
                        all_options.add(value_str)
                
                if len(all_options) <= 30:  # 多选允许更多选项
                    colors = self._generate_option_colors(list(all_options))
                    field_config['dropdown_configs'].append({
                        'column': column_name,
                        'options': list(all_options),
                        'colors': colors,
                        'multiple': True
                    })
            elif analysis['suggested_feishu_type'] == 5:  # 日期
                field_config['date_columns'].append(column_name)
            elif analysis['suggested_feishu_type'] == 2:  # 数字
                field_config['number_columns'].append(column_name)
        
        return field_config
    
    def _generate_option_colors(self, options: List[str]) -> List[str]:
        """
        为下拉列表选项生成颜色
        
        Args:
            options: 选项列表
            
        Returns:
            颜色列表
        """
        # 预定义的颜色集合
        color_palette = [
            "#1FB6C1",  # 浅蓝色
            "#F006C2",  # 玫红色
            "#FB16C3",  # 粉红色
            "#FFB6C1",  # 淡粉色
            "#32CD32",  # 绿色
            "#FF6347",  # 番茄色
            "#9370DB",  # 紫色
            "#FFD700",  # 金色
            "#FF8C00",  # 橙色
            "#20B2AA",  # 青色
            "#9400D3",  # 深紫色
            "#FF1493",  # 深粉色
            "#00CED1",  # 深绿松石色
            "#FF69B4",  # 热粉色
            "#8A2BE2",  # 蓝紫色
        ]
        
        # 循环使用颜色
        colors = []
        for i, option in enumerate(options):
            colors.append(color_palette[i % len(color_palette)])
        
        return colors