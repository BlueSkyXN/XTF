#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一同步引擎模块
提供多维表格和电子表格的统一同步引擎
"""

import pandas as pd
import time
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Union, Tuple

from .config import SyncConfig, SyncMode, TargetType
from .converter import DataConverter
from api import FeishuAuth, RetryableAPIClient, BitableAPI, SheetAPI, RateLimiter


class XTFSyncEngine:
    """统一同步引擎 - 支持多维表格和电子表格"""
    
    def __init__(self, config: SyncConfig):
        """
        初始化同步引擎
        
        Args:
            config: 统一同步配置对象
        """
        self.config = config
        
        # 初始化API组件
        self.auth = FeishuAuth(config.app_id, config.app_secret)
        self.api_client = RetryableAPIClient(
            max_retries=config.max_retries,
            rate_limiter=RateLimiter(config.rate_limit_delay)
        )
        
        # 根据目标类型选择API客户端
        if config.target_type == TargetType.BITABLE:
            self.api: Union[BitableAPI, SheetAPI] = BitableAPI(self.auth, self.api_client)
        else:  # SHEET
            self.api: Union[BitableAPI, SheetAPI] = SheetAPI(self.auth, self.api_client)
        
        # 初始化数据转换器
        self.converter = DataConverter(config.target_type)
        
        # 设置日志
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self):
        """设置日志"""
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        target_name = "bitable" if self.config.target_type == TargetType.BITABLE else "sheet"
        log_file = log_dir / f"xtf_{target_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
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
    
    # ========== 多维表格专用方法 ==========
    
    def get_field_types(self) -> Dict[str, int]:
        """获取多维表格字段类型映射"""
        if self.config.target_type != TargetType.BITABLE:
            return {}
            
        try:
            if not isinstance(self.api, BitableAPI):
                return {}
            existing_fields = self.api.list_fields(self.config.app_token, self.config.table_id)
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
        """确保多维表格所需字段存在"""
        if self.config.target_type != TargetType.BITABLE:
            return True, {}
            
        try:
            # 获取现有字段
            existing_fields = self.api.list_fields(self.config.app_token, self.config.table_id)
            existing_field_names = {field['field_name'] for field in existing_fields}
            
            # 构建字段类型映射
            field_types = {}
            for field in existing_fields:
                field_name = field.get('field_name', '')
                field_type = field.get('type', 1)
                field_types[field_name] = field_type
            
            if self.config.create_missing_fields:
                # 找出缺失的字段，保持原始列顺序
                required_fields = set(df.columns)
                missing_fields_set = required_fields - existing_field_names
                
                # 按照 DataFrame 列的原始顺序排列缺失字段
                missing_fields = [col for col in df.columns if col in missing_fields_set]
                
                if missing_fields:
                    self.logger.info(f"检测到 {len(missing_fields)} 个缺失字段")
                    self.logger.info(f"使用字段类型策略: {self.config.field_type_strategy.value}")
                    
                    # 分析每个缺失字段
                    creation_plan = []
                    for field_name in missing_fields:
                        # 使用增强的分析方法
                        analysis = self.converter.analyze_excel_column_data_enhanced(
                            df, field_name, self.config.field_type_strategy.value, self.config
                        )
                        
                        creation_plan.append({
                            'field_name': field_name,
                            'suggested_type': analysis['suggested_feishu_type'],
                            'confidence': analysis['confidence'],
                            'reason': analysis['recommendation_reason'],
                            'has_validation': analysis['has_excel_validation']
                        })
                    
                    # 显示创建计划
                    self.logger.info("=" * 60)
                    self.logger.info("📋 字段创建计划:")
                    for plan in creation_plan:
                        validation_mark = "📋" if plan['has_validation'] else "📝"
                        self.logger.info(
                            f"{validation_mark} {plan['field_name']}: "
                            f"{self.converter.get_field_type_name(plan['suggested_type'])} "
                            f"(置信度: {plan['confidence']:.1%}) - {plan['reason']}"
                        )
                    self.logger.info("=" * 60)
                    
                    # 执行字段创建
                    for plan in creation_plan:
                        success = self.api.create_field(
                            self.config.app_token,
                            self.config.table_id,
                            plan['field_name'],
                            plan['suggested_type']
                        )
                        
                        if not success:
                            self.logger.error(f"字段 '{plan['field_name']}' 创建失败")
                            return False, field_types
                        
                        # 记录新字段类型
                        field_types[plan['field_name']] = plan['suggested_type']
                    
                    # 等待字段创建完成
                    import time
                    time.sleep(2)
                    
                else:
                    self.logger.info("✅ 所有必需字段已存在，无需创建")
            
            return True, field_types
            
        except Exception as e:
            self.logger.error(f"字段检查失败: {e}")
            return False, {}
    
    def get_all_bitable_records(self) -> List[Dict]:
        """获取所有多维表格记录"""
        if self.config.target_type != TargetType.BITABLE:
            return []
        return self.api.get_all_records(self.config.app_token, self.config.table_id)
    
    def process_in_batches(self, items: List[Any], batch_size: int, 
                          processor_func, *args, **kwargs) -> bool:
        """分批处理数据（多维表格模式）"""
        if self.config.target_type != TargetType.BITABLE:
            return False
            
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
    
    # ========== 电子表格专用方法 ==========
    
    def get_current_sheet_data(self) -> pd.DataFrame:
        """获取当前电子表格数据"""
        if self.config.target_type != TargetType.SHEET:
            return pd.DataFrame()
            
        # 先尝试获取一个合理的范围
        # 如果失败，尝试更小的范围，最终返回空DataFrame表示需要使用clone模式
        ranges_to_try = [
            f"{self.config.sheet_id}!A1:ZZ1000",  # 最多1000行
            f"{self.config.sheet_id}!A1:Z500",    # 最多500行，Z列
            f"{self.config.sheet_id}!A1:J100",    # 最多100行，J列
            f"{self.config.sheet_id}!A1:E50"      # 最多50行，E列
        ]
        
        for range_str in ranges_to_try:
            try:
                values = self.api.get_sheet_data(self.config.spreadsheet_token, range_str)
                df = self.converter.values_to_df(values)
                if not df.empty:
                    # 检查是否包含有效数据（至少有一行数据包含非空值）
                    has_valid_data = False
                    for _, row in df.iterrows():
                        if any(pd.notnull(val) and str(val).strip() != '' for val in row):
                            has_valid_data = True
                            break
                    
                    if has_valid_data:
                        self.logger.info(f"成功获取电子表格数据: {len(df)} 行 x {len(df.columns)} 列")
                        return df
                    else:
                        # 数据全为空，当作表格为空
                        self.logger.info("电子表格数据全为空")
                        return pd.DataFrame()
                else:
                    # 如果数据为空，说明表格确实是空的
                    self.logger.info("电子表格为空")
                    return pd.DataFrame()
            except Exception as e:
                self.logger.debug(f"尝试范围 {range_str} 失败: {e}")
                continue
        
        # 所有范围都失败了，可能表格数据过大，返回空DataFrame触发clone模式
        self.logger.warning("无法获取电子表格数据，可能数据量过大，将使用覆盖模式")
        return pd.DataFrame()
    
    # ========== 统一同步方法 ==========
    
    def sync_full(self, df: pd.DataFrame) -> bool:
        """全量同步：已存在的更新，不存在的新增"""
        self.logger.info("开始全量同步...")
        
        if self.config.target_type == TargetType.BITABLE:
            return self._sync_full_bitable(df)
        else:  # SHEET
            return self._sync_full_sheet(df)
    
    def _sync_full_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格全量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            field_types = self.get_field_types()
            new_records = self.converter.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.get_all_bitable_records()
        self.logger.info(f"🔍 获取到现有记录数量: {len(existing_records)}")
        
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        self.logger.info(f"🔍 构建索引成功，索引数量: {len(existing_index)}")
        
        # 打印前几个现有记录的索引列值用于调试
        if existing_records and len(existing_records) > 0:
            for i, record in enumerate(existing_records[:3]):
                fields = record.get('fields', {})
                index_value = fields.get(self.config.index_column, '未找到')
                self.logger.info(f"🔍 现有记录 {i+1} 索引列 '{self.config.index_column}' 值: '{index_value}'")
        
        field_types = self.get_field_types()
        
        # 分类本地数据
        records_to_update = []
        records_to_create = []
        
        for i, (_, row) in enumerate(df.iterrows()):
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            index_value = row.get(self.config.index_column, '未找到')
            
            # 打印前几条记录的匹配信息用于调试
            if i < 3:
                self.logger.info(f"🔍 新数据记录 {i+1} 索引列 '{self.config.index_column}' 值: '{index_value}' -> 哈希: {index_hash}")
                self.logger.info(f"🔍 哈希是否在现有索引中: {index_hash in existing_index if index_hash else False}")
            
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
                self.api.batch_update_records,
                self.config.app_token, self.config.table_id
            )
        
        # 执行新增
        create_success = True
        if records_to_create:
            create_success = self.process_in_batches(
                records_to_create, self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        return update_success and create_success
    
    def _sync_full_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格全量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行完全覆盖操作")
            return self.sync_clone(df)
        
        # 获取现有数据
        current_df = self.get_current_sheet_data()
        
        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)
        
        # 构建索引
        current_index = self.converter.build_data_index(current_df, self.config.index_column)
        
        # 分类数据
        update_rows = []
        new_rows = []
        
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
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
            values = self.converter.df_to_values(updated_df)
            success = self.api.write_sheet_data(
                self.config.spreadsheet_token, 
                self.config.sheet_id, 
                values,
                self.config.batch_size,
                80  # 列批次大小，保持安全裕度
            )
        
        # 追加新行
        if new_rows and success:
            new_df = pd.DataFrame(new_rows)
            new_values = self.converter.df_to_values(new_df, include_headers=False)
            
            if new_values:
                self.logger.info(f"开始追加 {len(new_values)} 行新数据，使用批次大小: {self.config.batch_size}")
                success = self.api.append_sheet_data(
                    self.config.spreadsheet_token, 
                    self.config.sheet_id, 
                    new_values,
                    self.config.batch_size,
                    80  # 列批次大小，保持安全裕度
                )
        
        return success
    
    def sync_incremental(self, df: pd.DataFrame) -> bool:
        """增量同步：只新增不存在的记录"""
        self.logger.info("开始增量同步...")
        
        if self.config.target_type == TargetType.BITABLE:
            return self._sync_incremental_bitable(df)
        else:  # SHEET
            return self._sync_incremental_sheet(df)
    
    def _sync_incremental_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格增量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行纯新增操作")
            field_types = self.get_field_types()
            new_records = self.converter.df_to_records(df, field_types)
            return self.process_in_batches(
                new_records, self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        
        # 获取现有记录并建立索引
        existing_records = self.get_all_bitable_records()
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        field_types = self.get_field_types()
        
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
                self.api.batch_create_records,
                self.config.app_token, self.config.table_id
            )
        else:
            self.logger.info("没有新记录需要同步")
            return True
    
    def _sync_incremental_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格增量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将新增全部数据")
            # 追加所有数据
            values = self.converter.df_to_values(df)
            self.logger.info(f"未指定索引列，开始追加全部 {len(values)} 行数据，使用批次大小: {self.config.batch_size}")
            return self.api.append_sheet_data(
                self.config.spreadsheet_token, 
                self.config.sheet_id, 
                values,
                self.config.batch_size,
                80  # 列批次大小，保持安全裕度
            )
        
        # 获取现有数据
        current_df = self.get_current_sheet_data()
        
        if current_df.empty:
            self.logger.info("电子表格为空，新增全部数据")
            # 使用克隆同步（会先写入数据再设置格式）
            return self.sync_clone(df)
        
        # 构建索引
        current_index = self.converter.build_data_index(current_df, self.config.index_column)
        
        # 筛选需要新增的记录
        new_rows = []
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            if not index_hash or index_hash not in current_index:
                new_rows.append(row)
        
        self.logger.info(f"增量同步计划: 新增 {len(new_rows)} 行")
        
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            new_values = self.converter.df_to_values(new_df, include_headers=False)
            
            # 追加新数据
            self.logger.info(f"开始增量追加 {len(new_values)} 行数据，使用批次大小: {self.config.batch_size}")
            return self.api.append_sheet_data(
                self.config.spreadsheet_token, 
                self.config.sheet_id, 
                new_values,
                self.config.batch_size,
                80  # 列批次大小，保持安全裕度
            )
        else:
            self.logger.info("没有新记录需要同步")
            return True
    
    def sync_overwrite(self, df: pd.DataFrame) -> bool:
        """覆盖同步：删除已存在的，然后新增全部"""
        self.logger.info("开始覆盖同步...")
        
        if not self.config.index_column:
            self.logger.error("覆盖同步模式需要指定索引列")
            return False
        
        if self.config.target_type == TargetType.BITABLE:
            return self._sync_overwrite_bitable(df)
        else:  # SHEET
            return self._sync_overwrite_sheet(df)
    
    def _sync_overwrite_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格覆盖同步"""
        # 获取现有记录并建立索引
        existing_records = self.get_all_bitable_records()
        existing_index = self.converter.build_record_index(existing_records, self.config.index_column)
        field_types = self.get_field_types()
        
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
                self.api.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        new_records = self.converter.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.api.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def _sync_overwrite_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格覆盖同步"""
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
            index_hash = self.converter.get_index_value_hash(row, self.config.index_column)
            if index_hash:
                # 检查是否在新数据中
                found_in_new = False
                for _, new_row in df.iterrows():
                    new_index_hash = self.converter.get_index_value_hash(new_row, self.config.index_column)
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
            values = self.converter.df_to_values(new_df)
            
            # 先清空现有数据，然后写入新数据
            return self.api.write_sheet_data(
                self.config.spreadsheet_token, 
                self.config.sheet_id, 
                values,
                self.config.batch_size,
                80  # 列批次大小，保持安全裕度
            )
        else:
            # 如果没有数据，清空表格
            return self.api.clear_sheet_data(self.config.spreadsheet_token, f"{self.config.sheet_id}!A:Z")
    
    def sync_clone(self, df: pd.DataFrame) -> bool:
        """克隆同步：清空全部，然后新增全部"""
        self.logger.info("开始克隆同步...")
        
        if self.config.target_type == TargetType.BITABLE:
            return self._sync_clone_bitable(df)
        else:  # SHEET
            return self._sync_clone_sheet(df)
    
    def _sync_clone_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格克隆同步"""
        # 获取所有现有记录
        existing_records = self.get_all_bitable_records()
        existing_record_ids = [record["record_id"] for record in existing_records]
        
        self.logger.info(f"克隆同步计划: 删除 {len(existing_record_ids)} 条已有记录，然后新增 {len(df)} 条记录")
        
        # 删除所有记录
        delete_success = True
        if existing_record_ids:
            delete_success = self.process_in_batches(
                existing_record_ids, self.config.batch_size,
                self.api.batch_delete_records,
                self.config.app_token, self.config.table_id
            )
        
        # 新增全部记录
        field_types = self.get_field_types()
        new_records = self.converter.df_to_records(df, field_types)
        create_success = self.process_in_batches(
            new_records, self.config.batch_size,
            self.api.batch_create_records,
            self.config.app_token, self.config.table_id
        )
        
        return delete_success and create_success
    
    def _sync_clone_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格克隆同步"""
        # 转换数据格式
        values = self.converter.df_to_values(df)
        
        self.logger.info(f"克隆同步计划: 清空现有数据，新增 {len(df)} 行")
        self.logger.info(f"使用配置的批次大小: {self.config.batch_size} 行/批")
        
        # 使用二维分块写入数据（会覆盖现有数据）
        write_success = self.api.write_sheet_data(
            self.config.spreadsheet_token, 
            self.config.sheet_id, 
            values,
            self.config.batch_size,
            80  # 列批次大小，保持安全裕度
        )
        
        # 数据写入成功后，再应用智能字段配置
        if write_success:
            if not self._setup_sheet_intelligence(df):
                self.logger.warning("智能字段配置失败，但数据同步已完成")
        
        return write_success
    
    def _setup_sheet_intelligence(self, df: pd.DataFrame) -> bool:
        """
        为电子表格设置智能字段配置
        
        Args:
            df: 数据DataFrame
            
        Returns:
            是否设置成功
        """
        if self.config.target_type != TargetType.SHEET:
            return True
        
        # 不同策略的配置范围不同
        strategy_name = self.config.field_type_strategy.value
        self.logger.info(f"开始电子表格智能字段配置 ({strategy_name}策略)...")
        
        # raw策略：不应用任何格式化，直接返回成功
        if strategy_name == 'raw':
            self.logger.info("raw策略：跳过所有格式化，保持原始数据")
            return True
        
        # 生成字段配置
        field_config = self.converter.generate_sheet_field_config(
            df, self.config.field_type_strategy.value, self.config
        )
        
        success = True
        
        # 1. 配置下拉列表 (base策略跳过)
        if strategy_name != 'base':
            for dropdown_config in field_config['dropdown_configs']:
                column_name = dropdown_config['column']
                
                # 计算列索引
                col_index = list(df.columns).index(column_name)
                col_letter = self.converter.column_number_to_letter(col_index + 1)
                
                # 设置下拉列表范围 (从第2行开始，避免覆盖标题)
                actual_end_row = len(df) + 1  # +1 for header row
                range_str = f"{self.config.sheet_id}!{col_letter}2:{col_letter}{actual_end_row}"
                
                # 确保使用SheetAPI并检查token
                if not isinstance(self.api, SheetAPI):
                    self.logger.error("API类型不匹配，需要SheetAPI")
                    continue
                    
                if not self.config.spreadsheet_token:
                    self.logger.error("电子表格Token为空")
                    continue
                
                # 设置下拉列表
                dropdown_success = self.api.set_dropdown_validation(
                    self.config.spreadsheet_token,
                    range_str,
                    dropdown_config['options'],
                    dropdown_config['multiple'],
                    dropdown_config['colors']
                )
                
                if dropdown_success:
                    self.logger.info(f"成功为列 '{column_name}' 设置下拉列表")
                else:
                    self.logger.error(f"为列 '{column_name}' 设置下拉列表失败")
                    # 不设置success = False，允许继续其他列的操作
        else:
            self.logger.info("base策略跳过下拉列表配置")
        
        # 2. 配置日期格式
        if field_config['date_columns'] and isinstance(self.api, SheetAPI) and self.config.spreadsheet_token:
            date_ranges = []
            for column_name in field_config['date_columns']:
                col_index = list(df.columns).index(column_name)
                col_letter = self.converter.column_number_to_letter(col_index + 1)
                actual_end_row = len(df) + 1  # +1 for header row
                range_str = f"{self.config.sheet_id}!{col_letter}2:{col_letter}{actual_end_row}"
                date_ranges.append(range_str)
            
            # 设置日期格式
            date_success = self.api.set_date_format(
                self.config.spreadsheet_token,
                date_ranges,
                "yyyy/MM/dd"
            )
            
            if date_success:
                self.logger.info(f"成功为 {len(date_ranges)} 个日期列设置格式")
            else:
                self.logger.error("设置日期格式失败")
                # 不设置success = False，允许继续其他操作
        
        # 3. 配置数字格式
        if field_config['number_columns'] and isinstance(self.api, SheetAPI) and self.config.spreadsheet_token:
            number_ranges = []
            for column_name in field_config['number_columns']:
                col_index = list(df.columns).index(column_name)
                col_letter = self.converter.column_number_to_letter(col_index + 1)
                actual_end_row = len(df) + 1  # +1 for header row
                range_str = f"{self.config.sheet_id}!{col_letter}2:{col_letter}{actual_end_row}"
                number_ranges.append(range_str)
            
            # 设置数字格式
            number_success = self.api.set_number_format(
                self.config.spreadsheet_token,
                number_ranges,
                "#,##0.00"
            )
            
            if number_success:
                self.logger.info(f"成功为 {len(number_ranges)} 个数字列设置格式")
            else:
                self.logger.error("设置数字格式失败")
                # 不设置success = False，允许继续其他操作
        
        # 输出配置摘要
        dropdown_count = len(field_config['dropdown_configs']) if strategy_name != 'base' else 0
        date_count = len(field_config['date_columns'])
        number_count = len(field_config['number_columns'])
        total_configs = dropdown_count + date_count + number_count
        
        if total_configs > 0:
            config_summary = []
            if dropdown_count > 0:
                config_summary.append(f"{dropdown_count}个下拉列表")
            if date_count > 0:
                config_summary.append(f"{date_count}个日期格式")
            if number_count > 0:
                config_summary.append(f"{number_count}个数字格式")
            
            self.logger.info(f"智能字段配置完成: {', '.join(config_summary)}")
        else:
            self.logger.info("未检测到需要智能配置的字段")
        
        return success
    
    def sync(self, df: pd.DataFrame) -> bool:
        """执行同步"""
        target_name = "多维表格" if self.config.target_type == TargetType.BITABLE else "电子表格"
        self.logger.info(f"开始执行 {target_name} {self.config.sync_mode.value} 同步模式")
        self.logger.info(f"数据源: {len(df)} 行 x {len(df.columns)} 列")
        
        # 重置转换统计
        self.converter.reset_stats()
        
        # 多维表格模式需要确保字段存在
        if self.config.target_type == TargetType.BITABLE:
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
        
        # 输出转换统计信息（仅多维表格模式）
        if self.config.target_type == TargetType.BITABLE:
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