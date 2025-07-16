#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步引擎模块
提供XTF同步引擎，支持四种同步模式的智能同步
"""

import pandas as pd
import time
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

from .config import SyncConfig, SyncMode
from .converter import DataConverter
from api import FeishuAuth, RetryableAPIClient, BitableAPI, RateLimiter


class XTFSyncEngine:
    """XTF同步引擎 - 支持四种同步模式的智能同步"""
    
    def __init__(self, config: SyncConfig):
        """
        初始化同步引擎
        
        Args:
            config: 同步配置对象
        """
        self.config = config
        
        # 初始化API组件
        self.auth = FeishuAuth(config.app_id, config.app_secret)
        self.api_client = RetryableAPIClient(
            max_retries=config.max_retries,
            rate_limiter=RateLimiter(config.rate_limit_delay)
        )
        self.bitable_api = BitableAPI(self.auth, self.api_client)
        
        # 初始化数据转换器
        self.converter = DataConverter()
        
        # 设置日志
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
            existing_fields = self.bitable_api.list_fields(self.config.app_token, self.config.table_id)
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
            existing_fields = self.bitable_api.list_fields(self.config.app_token, self.config.table_id)
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
                        analysis = self.converter.analyze_excel_column_data(df, field_name)
                        suggested_type = analysis['suggested_feishu_type']
                        confidence = analysis['confidence']
                        
                        self.logger.info(f"字段 '{field_name}': {analysis['analysis']}, "
                                       f"建议类型: {self.converter.get_field_type_name(suggested_type)} "
                                       f"(置信度: {confidence:.1%})")
                        
                        success = self.bitable_api.create_field(
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
            new_records = self.converter.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.bitable_api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.bitable_api.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        
        # 分类本地数据
        records_to_update = []
        records_to_create = []
        
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            
            # 使用字段类型转换构建记录
            fields = {}
            for k, v in row.to_dict().items():
                if pd.notnull(v):
                    converted_value = self.converter.convert_field_value_safe(str(k), v, field_types)
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
                self.bitable_api.batch_update_records,
                self.config.app_token, self.config.table_id
            )
        
        # 执行新增
        create_success = True
        if records_to_create:
            create_success = self.process_in_batches(
                records_to_create, self.config.batch_size,
                self.bitable_api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        return update_success and create_success
    
    def sync_incremental(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
        """增量同步：只新增不存在索引值的记录"""
        self.logger.info("开始增量同步...")
        
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            new_records = self.converter.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.bitable_api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.bitable_api.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        
        # 筛选出需要新增的记录
        records_to_create = []
        
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            
            if not index_hash or index_hash not in existing_index:
                # 使用字段类型转换构建记录
                fields = {}
                for k, v in row.to_dict().items():
                    if pd.notnull(v):
                        converted_value = self.converter.convert_field_value_safe(str(k), v, field_types)
                        if converted_value is not None:
                            fields[str(k)] = converted_value
                
                record = {"fields": fields}
                records_to_create.append(record)
        
        self.logger.info(f"增量同步计划: 新增 {len(records_to_create)} 条记录")
        
        if records_to_create:
            return self.process_in_batches(
                records_to_create, self.config.batch_size,
                self.bitable_api.batch_create_records,
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
        existing_records = self.bitable_api.get_all_records(self.config.app_token, self.config.table_id)
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        
        # 找出需要删除的记录
        record_ids_to_delete = []
        
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            if index_hash and index_hash in existing_index:
                existing_record = existing_index[index_hash]
                record_ids_to_delete.append(existing_record["record_id"])
        
        self.logger.info(f"覆盖同步计划: 删除 {len(record_ids_to_delete)} 条已存在记录，然后新增 {len(df)} 条记录")
        
        # 删除已存在的记录
        delete_success = True
        if record_ids_to_delete:
            delete_success = self.process_in_batches(
                record_ids_to_delete, self.config.batch_size,
                self.bitable_api.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        new_records = self.converter.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.bitable_api.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def sync_clone(self, df: pd.DataFrame, field_types: Optional[Dict[str, int]] = None) -> bool:
        """克隆同步：清空全部已有记录，然后新增全部记录"""
        self.logger.info("开始克隆同步...")
        
        # 获取所有现有记录
        existing_records = self.bitable_api.get_all_records(self.config.app_token, self.config.table_id)
        existing_record_ids = [record["record_id"] for record in existing_records]
        
        self.logger.info(f"克隆同步计划: 删除 {len(existing_record_ids)} 条已有记录，然后新增 {len(df)} 条记录")
        
        # 删除所有记录
        delete_success = True
        if existing_record_ids:
            delete_success = self.process_in_batches(
                existing_record_ids, self.config.batch_size,
                self.bitable_api.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        new_records = self.converter.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.bitable_api.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def sync(self, df: pd.DataFrame) -> bool:
        """执行同步"""
        self.logger.info(f"开始执行 {self.config.sync_mode.value} 同步模式")
        self.logger.info(f"数据源: {len(df)} 行 x {len(df.columns)} 列")
        
        # 重置转换统计
        self.converter.reset_stats()
        
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
                        if not self.converter._is_number_string(str(value).strip()):
                            mismatch_warnings.append(f"字段 '{col_name}' 是数字类型，但包含非数字值: '{value}'")
                    elif field_type == 5 and isinstance(value, str):  # 日期字段但是字符串值
                        if not (self.converter._is_timestamp_string(str(value)) or self.converter._is_date_string(str(value))):
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
        self.converter.report_conversion_stats()
        
        return sync_result
    
    def _show_field_analysis_summary(self, df: pd.DataFrame, field_types: Dict[str, int]):
        """显示字段分析摘要"""
        self.logger.info("\n📋 字段类型映射摘要:")
        self.logger.info("-" * 50)
        
        for col_name in df.columns:
            if col_name in field_types:
                field_type = field_types[col_name]
                type_name = self.converter.get_field_type_name(field_type)
                self.logger.info(f"  {col_name} → {type_name} (类型码: {field_type})")
            else:
                self.logger.warning(f"  {col_name} → 未知字段类型")
                
        self.logger.info("-" * 50)