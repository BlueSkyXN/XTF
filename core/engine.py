#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一同步引擎模块

模块概述：
    此模块实现了 XTF 工具的核心同步逻辑，提供统一的同步引擎类
    XTFSyncEngine，支持多维表格（Bitable）和电子表格（Sheet）
    两种目标类型的数据同步。

主要功能：
    1. 统一的同步入口和流程控制
    2. 多维表格字段管理（自动创建缺失字段）
    3. 四种同步模式的具体实现
    4. 批量数据处理与分块上传
    5. 选择性列同步支持
    6. 日志系统配置和管理
    7. 全局请求控制器集成

核心类：
    XTFSyncEngine:
        统一同步引擎，根据配置的目标类型自动选择对应的 API 客户端
        和同步策略，执行数据同步操作。

同步模式说明：
    - full（全量同步）：
        对比索引列，已存在的记录更新，不存在的新增

    - incremental（增量同步）：
        仅新增本地有而远程没有的记录，跳过已存在记录

    - overwrite（覆盖同步）：
        先删除远程表中与本地数据索引匹配的记录，再新增全部本地数据

    - clone（克隆同步）：
        清空远程表全部数据，然后完整写入本地数据

同步流程：
    1. 初始化日志和 API 客户端
    2. 获取/创建远程表字段（Bitable）
    3. 获取远程现有数据
    4. 根据同步模式执行相应操作
    5. 批量处理数据（分块、重试、错误处理）
    6. 返回同步结果

依赖关系：
    内部模块：
        - core.config: 配置类（SyncConfig, SyncMode, TargetType）
        - core.converter: 数据转换（DataConverter）
        - api: API客户端（FeishuAuth, BitableAPI, SheetAPI等）
    外部依赖：
        - pandas: 数据处理
        - logging: 日志记录

性能优化：
    1. 批量操作减少 API 调用次数
    2. 预分块机制应对大数据量
    3. 智能重试和频控策略
    4. 选择性列同步减少数据传输

错误处理：
    - 三层数据上传保护机制
    - 自动二分重试（针对请求过大错误）
    - 详细的错误日志和状态反馈

使用示例：
    >>> from core.config import SyncConfig, TargetType
    >>> from core.engine import XTFSyncEngine
    >>>
    >>> config = SyncConfig(...)
    >>> engine = XTFSyncEngine(config)
    >>> success = engine.sync(dataframe)

注意事项：
    1. 同步前会自动设置日志，日志文件保存在 logs/ 目录
    2. Bitable 模式支持自动创建缺失字段
    3. 大数据量建议调整 batch_size 参数
    4. clone 模式会清空远程表，请谨慎使用

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import pandas as pd
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

        # 设置日志（必须先设置，因为其他初始化可能需要日志）
        self.setup_logging()
        self.logger = logging.getLogger("XTF.engine")

        # 初始化全局请求控制器（如果配置了高级重试和频控策略）
        self._init_global_controller()

        # 初始化API组件
        self.auth = FeishuAuth(config.app_id, config.app_secret)
        self.api_client = RetryableAPIClient(
            max_retries=config.max_retries,
            rate_limiter=RateLimiter(config.rate_limit_delay),
        )

        # 根据目标类型选择API客户端
        self.api: Union[BitableAPI, SheetAPI]
        if config.target_type == TargetType.BITABLE:
            self.api = BitableAPI(self.auth, self.api_client)
        else:  # SHEET
            self.api = SheetAPI(
                self.auth,
                self.api_client,
                start_row=self.config.start_row,
                start_column=self.config.start_column,
                scan_max_rows=self.config.sheet_scan_max_rows,
                scan_max_cols=self.config.sheet_scan_max_cols,
                write_max_rows=self.config.sheet_write_max_rows,
                write_max_cols=self.config.sheet_write_max_cols,
                value_render_option=self.config.sheet_value_render_option,
                datetime_render_option=self.config.sheet_datetime_render_option,
            )
        # 初始化数据转换器
        self.converter = DataConverter(config.target_type)
        # 缓存工作表网格属性，避免重复请求
        self._sheet_grid_cache: Optional[Tuple[int, int]] = None
        self._sheet_grid_cache_key: Optional[Tuple[str, str]] = None

    def _init_global_controller(self):
        """初始化全局请求控制器"""
        try:
            from .config import ConfigManager

            # 使用配置管理器创建全局控制器
            global_controller = ConfigManager.create_request_controller(self.config)
            if global_controller:
                self.logger.info(
                    f"已初始化全局请求控制器 - 重试策略: {self.config.retry_strategy_type}, "
                    f"频控策略: {self.config.rate_limit_strategy_type}"
                )
            else:
                self.logger.info(
                    f"使用传统控制模式 - 重试次数: {self.config.max_retries}, "
                    f"频控间隔: {self.config.rate_limit_delay}s"
                )
        except Exception as e:
            self.logger.warning(f"初始化全局控制器失败，回退到传统模式: {e}")

    def setup_logging(self):
        """设置日志"""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)

        target_name = (
            "bitable" if self.config.target_type == TargetType.BITABLE else "sheet"
        )
        log_file = (
            log_dir
            / f"xtf_{target_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        # 获取XTF专用的logger，避免全局污染
        xtf_logger = logging.getLogger("XTF")
        xtf_logger.handlers.clear()

        level = getattr(logging, self.config.log_level.upper(), logging.INFO)
        xtf_logger.setLevel(level)

        # 设置格式
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 文件处理器
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        xtf_logger.addHandler(file_handler)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        xtf_logger.addHandler(console_handler)

        # 防止传播到根logger
        xtf_logger.propagate = False

    # ========== 多维表格专用方法 ==========

    def get_field_types(self) -> Dict[str, int]:
        """获取多维表格字段类型映射"""
        if self.config.target_type != TargetType.BITABLE:
            return {}

        try:
            if not isinstance(self.api, BitableAPI):
                return {}
            if not self.config.app_token or not self.config.table_id:
                self.logger.error("多维表格的 app_token 或 table_id 未配置")
                return {}
            existing_fields = self.api.list_fields(
                self.config.app_token, self.config.table_id
            )
            field_types = {}
            for field in existing_fields:
                field_name = field.get("field_name", "")
                field_type = field.get("type", 1)  # 默认为文本类型
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
            if not isinstance(self.api, BitableAPI):
                return False, {}
            if not self.config.app_token or not self.config.table_id:
                self.logger.error("多维表格的 app_token 或 table_id 未配置")
                return False, {}

            # 获取现有字段
            existing_fields = self.api.list_fields(
                self.config.app_token, self.config.table_id
            )
            existing_field_names = {field["field_name"] for field in existing_fields}

            # 构建字段类型映射
            field_types = {}
            for field in existing_fields:
                field_name = field.get("field_name", "")
                field_type = field.get("type", 1)
                field_types[field_name] = field_type

            if self.config.create_missing_fields:
                # 找出缺失的字段，保持原始列顺序
                required_fields = set(df.columns)
                missing_fields_set = required_fields - existing_field_names

                # 按照 DataFrame 列的原始顺序排列缺失字段
                missing_fields = [
                    col for col in df.columns if col in missing_fields_set
                ]

                if missing_fields:
                    self.logger.info(f"检测到 {len(missing_fields)} 个缺失字段")
                    self.logger.info(
                        f"使用字段类型策略: {self.config.field_type_strategy.value}"
                    )

                    # 分析每个缺失字段
                    creation_plan = []
                    for field_name in missing_fields:
                        # 使用增强的分析方法
                        analysis = self.converter.analyze_excel_column_data_enhanced(
                            df,
                            field_name,
                            self.config.field_type_strategy.value,
                            self.config,
                        )

                        creation_plan.append(
                            {
                                "field_name": field_name,
                                "suggested_type": analysis["suggested_feishu_type"],
                                "confidence": analysis["confidence"],
                                "reason": analysis["recommendation_reason"],
                                "has_validation": analysis["has_excel_validation"],
                            }
                        )

                    # 显示创建计划
                    self.logger.info("=" * 60)
                    self.logger.info("📋 字段创建计划:")
                    for plan in creation_plan:
                        validation_mark = "📋" if plan["has_validation"] else "📝"
                        self.logger.info(
                            f"{validation_mark} {plan['field_name']}: "
                            f"{self.converter.get_field_type_name(plan['suggested_type'])} "
                            f"(置信度: {plan['confidence']:.1%}) - {plan['reason']}"
                        )
                    self.logger.info("=" * 60)

                    # 执行字段创建
                    for plan in creation_plan:
                        if not isinstance(self.api, BitableAPI):
                            continue
                        success = self.api.create_field(
                            self.config.app_token,
                            self.config.table_id,
                            plan["field_name"],
                            plan["suggested_type"],
                        )

                        if not success:
                            self.logger.error(f"字段 '{plan['field_name']}' 创建失败")
                            return False, field_types

                        # 记录新字段类型
                        field_types[plan["field_name"]] = plan["suggested_type"]

                    # 等待字段创建完成
                    import time

                    time.sleep(2)

                else:
                    self.logger.info("✅ 所有必需字段已存在，无需创建")

            return True, field_types

        except Exception as e:
            self.logger.error(f"字段检查失败: {e}")
            return False, {}

    def get_all_bitable_records(
        self, field_names: Optional[List[str]] = None
    ) -> List[Dict]:
        """获取所有多维表格记录

        Args:
            field_names: 指定返回的字段名称列表，为None时返回全部字段。
                         用于减少不必要的数据传输，提升查询性能。
        """
        if not isinstance(self.api, BitableAPI):
            return []
        if not self.config.app_token or not self.config.table_id:
            self.logger.error("多维表格的 app_token 或 table_id 未配置")
            return []
        return self.api.get_all_records(
            self.config.app_token, self.config.table_id, field_names=field_names
        )

    def process_in_batches(
        self, items: List[Any], batch_size: int, processor_func, *args, **kwargs
    ) -> bool:
        """分批处理数据（多维表格模式）"""
        if self.config.target_type != TargetType.BITABLE:
            return False

        # 按接口上限自动限制批大小，避免超限请求
        max_batch_size = self._get_operation_max_batch_size(processor_func)
        effective_batch_size = batch_size
        if max_batch_size and batch_size > max_batch_size:
            self.logger.warning(
                f"{self._get_operation_type(processor_func)}批处理大小 {batch_size} 超过接口上限 {max_batch_size}，已自动降至 {max_batch_size}"
            )
            effective_batch_size = max_batch_size

        total_batches = (len(items) + effective_batch_size - 1) // effective_batch_size
        success_count = 0

        # 获取操作类型用于日志显示
        operation_type = self._get_operation_type(processor_func)

        for i in range(0, len(items), effective_batch_size):
            batch = items[i : i + effective_batch_size]
            batch_num = i // effective_batch_size + 1
            start_row = i + 1  # Excel行号从1开始
            end_row = min(i + len(batch), len(items))

            try:
                # 修复参数传递顺序：先传递固定参数，再传递批次数据
                if processor_func(*args, batch, **kwargs):
                    success_count += 1
                    # 显示具体的行范围信息
                    range_info = (
                        f"第{start_row}-{end_row}行"
                        if start_row != end_row
                        else f"第{start_row}行"
                    )
                    self.logger.info(
                        f"✅ {operation_type}成功: 批次{batch_num}/{total_batches}, {len(batch)}条记录 ({range_info})"
                    )
                else:
                    self.logger.error(
                        f"❌ {operation_type}失败: 批次{batch_num}/{total_batches}"
                    )
            except Exception as e:
                self.logger.error(
                    f"❌ {operation_type}异常: 批次{batch_num}/{total_batches}, 错误: {e}"
                )

        self.logger.info(
            f"🎉 {operation_type}完成: {success_count}/{total_batches} 个批次成功"
        )
        return success_count == total_batches

    def _get_operation_type(self, processor_func) -> str:
        """根据处理函数获取操作类型"""
        func_name = getattr(processor_func, "__name__", str(processor_func))
        if "create" in func_name:
            return "批量创建"
        elif "update" in func_name:
            return "批量更新"
        elif "delete" in func_name:
            return "批量删除"
        else:
            return "批量处理"

    def _get_operation_max_batch_size(self, processor_func) -> Optional[int]:
        """根据处理函数获取批量接口上限"""
        func_name = getattr(processor_func, "__name__", str(processor_func))
        if "create" in func_name:
            return BitableAPI.MAX_BATCH_CREATE_SIZE
        if "update" in func_name:
            return BitableAPI.MAX_BATCH_UPDATE_SIZE
        if "delete" in func_name:
            return BitableAPI.MAX_BATCH_DELETE_SIZE
        return None

    # ========== 电子表格专用方法 ==========

    def _get_sheet_grid_properties(self) -> Optional[Tuple[int, int]]:
        """获取工作表网格属性（行数、列数）"""
        if self.config.target_type != TargetType.SHEET:
            return None
        if not isinstance(self.api, SheetAPI):
            return None
        if not self.config.spreadsheet_token or not self.config.sheet_id:
            return None
        cache_key = (self.config.spreadsheet_token, self.config.sheet_id)
        if self._sheet_grid_cache_key == cache_key and self._sheet_grid_cache:
            return self._sheet_grid_cache
        try:
            grid = self.api.get_sheet_grid_properties(
                self.config.spreadsheet_token, self.config.sheet_id
            )
            self._sheet_grid_cache = grid
            self._sheet_grid_cache_key = cache_key
            return grid
        except Exception as e:
            self.logger.warning(f"获取工作表网格属性失败: {e}")
            return None

    def _build_sheet_full_range(self) -> Optional[str]:
        """构建覆盖整个工作表的范围字符串（基于网格属性）"""
        grid = self._get_sheet_grid_properties()
        if not grid:
            return None
        row_count, col_count = grid
        if row_count <= 0 or col_count <= 0:
            return None
        if not isinstance(self.api, SheetAPI):
            return None
        end_col = self.api.column_number_to_letter(col_count)
        return f"A1:{end_col}{row_count}"

    def get_current_sheet_data(self) -> pd.DataFrame:
        """获取当前电子表格数据"""
        if self.config.target_type != TargetType.SHEET:
            return pd.DataFrame()

        # 构建从配置起始点开始的读取范围
        start_cell = f"{self.config.start_column}{self.config.start_row}"
        read_range = None
        end_row = None
        end_col = None

        # 优先使用工作表网格属性精确限定范围
        grid = self._get_sheet_grid_properties()
        if grid and isinstance(self.api, SheetAPI):
            row_count, col_count = grid
            start_col_num = self.api.column_letter_to_number(self.config.start_column)
            if row_count < self.config.start_row or col_count < start_col_num:
                self.logger.info(
                    f"工作表网格范围小于起始位置: "
                    f"row_count={row_count}, column_count={col_count}, "
                    f"start={start_cell}"
                )
                return pd.DataFrame()

            end_row = row_count
            end_col = self.api.column_number_to_letter(col_count)
            read_range = (
                f"{self.config.sheet_id}!"
                f"{self.config.start_column}{self.config.start_row}:{end_col}{end_row}"
            )
        else:
            # 兜底：使用历史默认范围（注意：可能较大）
            end_row = 500000
            end_col = "ZZ"
            read_range = f"{self.config.sheet_id}!{start_cell}:{end_col}{end_row}"
            self.logger.warning("无法获取工作表网格属性，退回默认读取范围，可能较大")

        self.logger.info(f"尝试从范围读取数据: {read_range}")

        try:
            if not isinstance(self.api, SheetAPI):
                return pd.DataFrame()
            if not self.config.spreadsheet_token:
                self.logger.error("电子表格的 spreadsheet_token 未配置")
                return pd.DataFrame()
            if not self.config.sheet_id:
                self.logger.error("电子表格的 sheet_id 未配置")
                return pd.DataFrame()

            if not (end_row and end_col):
                return pd.DataFrame()

            values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            df = self.converter.values_to_df(values)

            if not df.empty:
                # 检查是否包含有效数据（至少有一行数据包含非空值）
                has_valid_data = False
                for _, row in df.iterrows():
                    if any(pd.notnull(val) and str(val).strip() != "" for val in row):
                        has_valid_data = True
                        break

                if has_valid_data:
                    self.logger.info(
                        f"成功获取电子表格数据: {len(df)} 行 x {len(df.columns)} 列 (从 {start_cell} 开始)"
                    )
                    return df

            # 如果df为空或数据全为空，说明表格在指定范围确实是空的
            self.logger.info(f"在范围 {read_range} 内未找到有效数据，视为空表")
            return pd.DataFrame()

        except Exception as e:
            self.logger.warning(f"尝试从范围 {read_range} 读取数据失败: {e}")
            self.logger.warning("无法获取电子表格数据，将使用覆盖模式")
            return pd.DataFrame()

    def get_sheet_data_with_validation(
        self,
    ) -> tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[set]]:
        """
        获取电子表格数据（支持双读用于结果检测）

        Returns:
            (result_df, formula_df, formula_columns):
            - result_df: 计算结果数据（用于比较）
            - formula_df: 公式数据（仅在启用 validate_results 时返回）
            - formula_columns: 包含公式的列集合（列名）
        """
        if not self.config.sheet_validate_results:
            # 未启用检测，使用原有单次读取逻辑
            return self.get_current_sheet_data(), None, None

        # 启用检测，执行双读
        if not isinstance(self.api, SheetAPI):
            return pd.DataFrame(), None, None

        if not self.config.spreadsheet_token or not self.config.sheet_id:
            return pd.DataFrame(), None, None

        # 获取网格范围
        grid = self._get_sheet_grid_properties()
        if not grid:
            self.logger.warning("无法获取工作表网格属性，无法进行双读")
            return self.get_current_sheet_data(), None, None

        row_count, col_count = grid
        start_col_num = self.api.column_letter_to_number(self.config.start_column)
        if row_count < self.config.start_row or col_count < start_col_num:
            self.logger.info("工作表范围小于起始位置，视为空表")
            return pd.DataFrame(), None, None

        end_row = row_count
        end_col = self.api.column_number_to_letter(col_count)

        self.logger.info("🔍 启用结果检测，开始双读云端数据...")

        # 第一次读取：公式模式
        self.logger.info("  📖 读取公式数据...")
        try:
            # 临时设置读取选项为 Formula
            original_value_option = self.config.sheet_value_render_option
            original_datetime_option = self.config.sheet_datetime_render_option

            # 强制使用 Formula 模式读取
            self.config.sheet_value_render_option = "Formula"
            self.config.sheet_datetime_render_option = None

            formula_values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            formula_df = self.converter.values_to_df(formula_values)

            # 恢复原有配置
            self.config.sheet_value_render_option = original_value_option
            self.config.sheet_datetime_render_option = original_datetime_option

        except Exception as e:
            self.logger.warning(f"读取公式数据失败: {e}")
            return self.get_current_sheet_data(), None, None

        # 第二次读取：结果模式
        self.logger.info("  📊 读取计算结果数据...")
        try:
            # 使用配置的读取选项（或 FormattedValue 作为默认）
            if not self.config.sheet_value_render_option:
                self.config.sheet_value_render_option = "FormattedValue"
            if not self.config.sheet_datetime_render_option:
                self.config.sheet_datetime_render_option = "FormattedString"

            result_values = self.api.get_sheet_data_chunked(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                self.config.start_row,
                end_row,
                self.config.start_column,
                end_col,
            )
            result_df = self.converter.values_to_df(result_values)

        except Exception as e:
            self.logger.warning(f"读取结果数据失败: {e}")
            return self.get_current_sheet_data(), None, None

        # 识别公式列
        if formula_df.empty:
            formula_columns = set()
        else:
            # 转换为二维列表用于识别
            formula_data = [formula_df.columns.tolist()] + formula_df.values.tolist()
            formula_columns = set(
                str(col)
                for col in self.api.identify_formula_columns(
                    formula_data, headers=formula_df.columns.tolist()
                )
            )

        if formula_columns:
            self.logger.info(f"  🔒 识别到公式列: {sorted(formula_columns)}")
        else:
            self.logger.info("  ℹ️  未识别到公式列")

        return result_df, formula_df, formula_columns

    def validate_and_report_differences(
        self,
        local_df: pd.DataFrame,
        remote_result_df: pd.DataFrame,
        formula_columns: Optional[set],
    ) -> Dict[str, Any]:
        """
        检测本地数据与云端结果的差异，生成列级差异报告

        Args:
            local_df: 本地数据
            remote_result_df: 云端结果数据
            formula_columns: 公式列集合

        Returns:
            差异统计字典
        """
        if formula_columns is None:
            formula_columns = set()

        formula_diff: Dict[str, int] = {}
        data_diff: Dict[str, int] = {}
        error_diff: Dict[str, str] = {}
        diff_stats: Dict[str, Any] = {
            "formula_columns": formula_diff,  # 公式列差异: {列名: 差异行数}
            "data_columns": data_diff,  # 数据列差异: {列名: 差异行数}
            "error_columns": error_diff,  # 异常列: {列名: 错误信息}
            "total_rows": len(local_df),
        }

        # 遍历所有列
        for col in local_df.columns:
            if col not in remote_result_df.columns:
                error_diff[str(col)] = "云端不存在此列"
                continue

            try:
                diff_count = 0
                local_col = local_df[col]
                remote_col = remote_result_df[col]

                # 逐行比较
                for idx in range(len(local_col)):
                    if idx >= len(remote_col):
                        diff_count += 1
                        continue

                    local_val = local_col.iloc[idx]
                    remote_val = remote_col.iloc[idx]

                    if not self._values_equal(local_val, remote_val):
                        diff_count += 1

                # 记录差异
                if diff_count > 0:
                    if col in formula_columns:
                        formula_diff[str(col)] = diff_count
                    else:
                        data_diff[str(col)] = diff_count

            except Exception as e:
                error_diff[str(col)] = str(e)

        return diff_stats

    def _values_equal(self, val1: Any, val2: Any) -> bool:
        """
        比较两个值是否相等（考虑数值容差）

        Args:
            val1: 第一个值
            val2: 第二个值

        Returns:
            是否相等
        """
        import pandas as pd

        # 都是空值
        if pd.isnull(val1) and pd.isnull(val2):
            return True

        # 一个空一个不空
        if pd.isnull(val1) or pd.isnull(val2):
            return False

        # 都是数值类型
        try:
            num1 = float(val1)
            num2 = float(val2)
            return abs(num1 - num2) <= self.config.sheet_diff_tolerance
        except (ValueError, TypeError):
            pass

        # 字符串比较
        return str(val1).strip() == str(val2).strip()

    def print_column_diff_report(self, diff_stats: Dict[str, Any]):
        """
        打印列级差异报告

        Args:
            diff_stats: 差异统计字典
        """
        if not self.config.sheet_report_column_diff:
            return

        total_rows = diff_stats["total_rows"]
        formula_cols = diff_stats["formula_columns"]
        data_cols = diff_stats["data_columns"]
        error_cols = diff_stats["error_columns"]

        # 统计信息
        total_cols = len(formula_cols) + len(data_cols) + len(error_cols)
        diff_cols = len(formula_cols) + len(data_cols)

        print("\n" + "=" * 60)
        print("📊 列差异检测报告")
        print(f"时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("模式: 逻辑同步+结果检测")
        print("=" * 60)

        if formula_cols:
            print("\n🔒 公式列（已保护，不覆盖）:")
            for col, diff_count in sorted(formula_cols.items()):
                pct = (diff_count / total_rows * 100) if total_rows > 0 else 0
                print(f"  ✓ {col}: {diff_count}/{total_rows} 行结果不一致 ({pct:.2f}%)")
            if self.config.sheet_protect_formulas:
                print("  → 建议: 检查输入数据列是否变化")

        if data_cols:
            print("\n📝 数据列（已同步）:")
            for col, diff_count in sorted(data_cols.items()):
                print(f"  ✓ {col}: {diff_count} 行差异 → 已更新")

        if error_cols:
            print("\n⚠️  异常列（类型不匹配或无法比较）:")
            for col, error in sorted(error_cols.items()):
                print(f"  ✗ {col}: {error}")

        print("\n" + "=" * 60)
        print(f"总计: {diff_cols}/{total_cols} 列有差异")
        if self.config.sheet_protect_formulas:
            print(f"同步完成: {len(data_cols)}/{total_cols} 列")
            print(f"保护跳过: {len(formula_cols)}/{total_cols} 列")
        else:
            print(f"同步完成: {len(data_cols) + len(formula_cols)}/{total_cols} 列")
        print("=" * 60 + "\n")

    # ========== 选择性同步辅助方法 ==========

    def _get_effective_selective_columns(self, df: pd.DataFrame) -> List[str]:
        """获取选择性同步实际生效的列（含索引列）"""
        if (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            return df.columns.tolist()

        target_columns = self.config.selective_sync.columns.copy()

        # 自动包含索引列（用于匹配逻辑）
        if (
            self.config.selective_sync.auto_include_index
            and self.config.index_column
            and self.config.index_column not in target_columns
        ):
            target_columns.append(self.config.index_column)
            self.logger.info(f"自动包含索引列: {self.config.index_column}")

        # 去重，保留顺序
        deduped_columns = []
        seen = set()
        for col in target_columns:
            if col not in seen:
                seen.add(col)
                deduped_columns.append(col)

        # 验证列是否存在
        missing_columns = [col for col in deduped_columns if col not in df.columns]
        if missing_columns:
            self.logger.warning(f"指定的列不存在于数据中: {missing_columns}")
            deduped_columns = [col for col in deduped_columns if col in df.columns]

        # 保持列顺序（如果启用）
        if self.config.selective_sync.preserve_column_order:
            return [col for col in df.columns if col in deduped_columns]

        return deduped_columns

    def _apply_selective_filter(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用选择性列过滤"""
        if (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            return df

        # 获取要处理的列
        target_columns = self._get_effective_selective_columns(df)
        return df[target_columns]

    # ========== Bitable 字段查询优化 ==========

    def _get_bitable_fetch_field_names(
        self, df: pd.DataFrame, mode: str
    ) -> Optional[List[str]]:
        """
        根据同步模式计算获取远程记录时需要的字段列表。

        通过飞书查询记录API的 field_names 参数，只返回必要的字段，
        减少不必要的数据传输，提升查询性能。

        Args:
            df: 本地数据 DataFrame
            mode: 同步模式 ('full', 'incremental', 'overwrite', 'clone')

        Returns:
            field_names 列表，None 表示获取全部字段
        """
        if mode == "clone":
            # clone 模式只需 record_id（API 固定返回），使用空 field_names 返回最小字段集
            return []

        index_col = self.config.index_column
        if not index_col:
            return None  # 无索引列时无法优化

        # full / incremental / overwrite：仅需索引列用于匹配和获取 record_id
        if mode in ("full", "incremental", "overwrite"):
            return [index_col]

        return None

    # ========== 统一同步方法 ==========

    def sync_full(self, df: pd.DataFrame) -> bool:
        """全量同步：已存在的更新，不存在的新增"""
        self.logger.info("开始全量同步...")

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

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
            if (
                isinstance(self.api, BitableAPI)
                and self.config.app_token
                and self.config.table_id
            ):
                return self.process_in_batches(
                    new_records,
                    self.config.batch_size,
                    self.api.batch_create_records,
                    self.config.app_token,
                    self.config.table_id,
                )
            return False

        # 获取现有记录并建立索引（使用field_names优化，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "full")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        self.logger.info(f"🔍 获取到现有记录数量: {len(existing_records)}")

        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )
        self.logger.info(f"🔍 构建索引成功，索引数量: {len(existing_index)}")

        # 打印前几个现有记录的索引列值用于调试
        if existing_records and len(existing_records) > 0:
            for i, record in enumerate(existing_records[:3]):
                fields = record.get("fields", {})
                index_value = fields.get(self.config.index_column, "未找到")
                normalized_value = self.converter._normalize_index_value(
                    index_value, field_types.get(self.config.index_column)
                )
                self.logger.info(
                    f"🔍 现有记录 {i + 1} 索引列 '{self.config.index_column}' 值: '{index_value}' -> 规范化: '{normalized_value}'"
                )

        # 分类本地数据
        records_to_update = []
        records_to_create = []

        for i, (_, row) in enumerate(df.iterrows()):
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )
            index_value = row.get(self.config.index_column, "未找到")

            # 打印前几条记录的匹配信息用于调试
            if i < 3:
                self.logger.info(
                    f"🔍 新数据记录 {i + 1} 索引列 '{self.config.index_column}' 值: '{index_value}' -> 哈希: {index_hash}"
                )
                self.logger.info(
                    f"🔍 哈希是否在现有索引中: {index_hash in existing_index if index_hash else False}"
                )

            # 使用字段类型转换构建记录
            fields = {}
            for k, v in row.to_dict().items():
                if not self.converter._is_empty_value(v):
                    converted_value = self.converter.convert_field_value_safe(
                        str(k), v, field_types
                    )
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

        self.logger.info(
            f"全量同步计划: 更新 {len(records_to_update)} 条，新增 {len(records_to_create)} 条"
        )

        # 执行更新
        update_success = True
        if (
            records_to_update
            and isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            update_success = self.process_in_batches(
                records_to_update,
                self.config.batch_size,
                self.api.batch_update_records,
                self.config.app_token,
                self.config.table_id,
            )

        # 执行新增
        create_success = True
        if (
            records_to_create
            and isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            create_success = self.process_in_batches(
                records_to_create,
                self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token,
                self.config.table_id,
            )

        return update_success and create_success

    def _sync_full_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格全量同步"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将执行完全覆盖操作")
            return self.sync_clone(df)

        # 获取现有数据（支持双读和差异检测）
        current_df, formula_df, formula_columns = self.get_sheet_data_with_validation()

        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)

        # ⭐ 关键修改：检查是否启用选择性同步，使用精确列级控制
        if self.config.selective_sync.enabled and self.config.selective_sync.columns:
            self.logger.info(
                f"🎯 启用精确列级控制同步: {self.config.selective_sync.columns}"
            )
            return self._sync_selective_columns_sheet(df, current_df)

        # 差异检测与报告
        if self.config.sheet_validate_results and formula_columns is not None:
            diff_stats = self.validate_and_report_differences(
                df, current_df, formula_columns
            )
            self.print_column_diff_report(diff_stats)

        # 公式保护：过滤掉公式列
        sync_df = df
        if self.config.sheet_protect_formulas and formula_columns:
            # 只同步非公式列
            non_formula_cols = [col for col in df.columns if col not in formula_columns]
            if not non_formula_cols:
                self.logger.warning("所有列都是公式列，且启用了公式保护，无需同步")
                return True
            sync_df = df[non_formula_cols].copy()
            self.logger.info(
                f"🔒 公式保护已启用，仅同步 {len(non_formula_cols)} 个数据列"
            )

        # 原有的完整表格同步逻辑
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 分类数据
        update_rows = []
        new_rows = []

        for _, row in sync_df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 更新现有行
                current_row_idx = current_index[index_hash]
                update_rows.append((current_row_idx, row))
            else:
                # 新增行
                new_rows.append(row)

        self.logger.info(
            f"全量同步计划: 更新 {len(update_rows)} 行，新增 {len(new_rows)} 行"
        )

        # 执行更新
        success = True
        if update_rows:
            # 更新现有行
            updated_df = current_df.copy()
            for current_row_idx, new_row in update_rows:
                for col in sync_df.columns:
                    if col in updated_df.columns:
                        # 使用 .iloc 双索引避免链式赋值问题 (SettingWithCopyWarning)
                        updated_df.iloc[
                            current_row_idx, updated_df.columns.get_loc(col)
                        ] = new_row[col]

            # 写入更新后的数据
            values = self.converter.df_to_values(updated_df)
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                success = self.api.write_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    values,
                    self.config.batch_size,
                    80,  # 列批次大小，保持安全裕度
                    self.config.rate_limit_delay,
                )

        # 追加新行
        if new_rows and success:
            new_df = pd.DataFrame(new_rows)
            new_values = self.converter.df_to_values(new_df, include_headers=False)

            if (
                new_values
                and isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info(f"开始追加 {len(new_values)} 行新数据")
                success = self.api.append_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    new_values,
                    self.config.batch_size,
                    self.config.rate_limit_delay,
                )

        return success

    def _sync_selective_columns_sheet(
        self, df: pd.DataFrame, current_df: pd.DataFrame
    ) -> bool:
        """电子表格选择性列同步 - 使用精确列控制"""
        columns = self._get_effective_selective_columns(df)
        if not columns:
            self.logger.warning("选择性列同步未配置 columns 或无可用列，已跳过")
            return False
        self.logger.info(f"🎯 启用精确列控制同步: {columns}")

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 准备更新数据映射 {row_idx: {col: value}}
        update_data_map: Dict[int, Dict[str, Any]] = {}
        new_rows: List[pd.Series] = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 更新现有行
                current_row_idx = current_index[index_hash]
                if current_row_idx not in update_data_map:
                    update_data_map[current_row_idx] = {}

                # 只更新指定列
                for col in columns:
                    if col in df.columns:
                        update_data_map[current_row_idx][col] = row[col]
            else:
                # 新增行
                new_rows.append(row)

        self.logger.info(
            f"精确列控制计划: 更新 {len(update_data_map)} 行的指定列，新增 {len(new_rows)} 行"
        )

        success = True

        # 执行选择性列更新
        if update_data_map:
            success = self._update_selective_columns(current_df, update_data_map)

        # 追加新行（如果有）
        if new_rows and success:
            new_df = pd.DataFrame(new_rows)
            new_values = self.converter.df_to_values(new_df, include_headers=False)

            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info(f"开始追加 {len(new_values)} 行新数据")
                success = self.api.append_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    new_values,
                    self.config.batch_size,
                    self.config.rate_limit_delay,
                )

        return success

    def _update_selective_columns(
        self, current_df: pd.DataFrame, update_data_map: Dict[int, Dict[str, Any]]
    ) -> bool:
        """使用精确列控制更新数据"""
        if not update_data_map:
            return True

        # 准备按列组织的更新数据
        columns_to_update: set[str] = set()
        for row_updates in update_data_map.values():
            columns_to_update.update(row_updates.keys())

        self.logger.info(f"🔄 准备更新列: {list(columns_to_update)}")

        # 使用 converter 的 df_to_column_data 和 get_column_positions
        # 构建需要更新的列数据
        column_data = {}
        for col in columns_to_update:
            col_data = []
            for row_idx in range(len(current_df)):
                if row_idx in update_data_map and col in update_data_map[row_idx]:
                    # 使用新值
                    converted_value = self.converter.simple_convert_value(
                        update_data_map[row_idx][col]
                    )
                    col_data.append(converted_value)
                else:
                    # 保持原值
                    original_value = (
                        current_df.iloc[row_idx][col]
                        if col in current_df.columns
                        else ""
                    )
                    converted_value = self.converter.simple_convert_value(
                        original_value
                    )
                    col_data.append(converted_value)
            column_data[col] = col_data

        # 获取起始列偏移量
        start_col_offset = 0
        if isinstance(self.api, SheetAPI):
            start_col_offset = self.api.start_col_num - 1

        # 获取列位置映射（考虑起始列偏移）
        column_positions = self.converter.get_column_positions(
            current_df, list(columns_to_update), start_col_offset
        )

        self.logger.info(f"📍 列位置映射: {column_positions}")

        # 使用精确列写入API
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            # start_row 需要考虑配置的起始行 + 表头行
            actual_start_row = self.config.start_row + 1
            # 如果 optimize_ranges 为 False，设置 max_gap=0 禁用合并
            effective_max_gap = (
                self.config.selective_sync.max_gap_for_merge
                if self.config.selective_sync.optimize_ranges
                else 0
            )
            return self.api.write_selective_columns(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                column_data,
                column_positions,
                start_row=actual_start_row,
                rate_limit_delay=self.config.rate_limit_delay,
                max_gap=effective_max_gap,
            )

        return False

    def sync_incremental(self, df: pd.DataFrame) -> bool:
        """增量同步：只新增不存在的记录"""
        self.logger.info("开始增量同步...")

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

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
            if (
                isinstance(self.api, BitableAPI)
                and self.config.app_token
                and self.config.table_id
            ):
                return self.process_in_batches(
                    new_records,
                    self.config.batch_size,
                    self.api.batch_create_records,
                    self.config.app_token,
                    self.config.table_id,
                )
            return False

        # 获取现有记录并建立索引（仅获取索引列，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "incremental")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )

        # 筛选出需要新增的记录
        records_to_create = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )

            if not index_hash or index_hash not in existing_index:
                # 使用字段类型转换构建记录
                fields = {}
                for k, v in row.to_dict().items():
                    if not self.converter._is_empty_value(v):
                        converted_value = self.converter.convert_field_value_safe(
                            str(k), v, field_types
                        )
                        if converted_value is not None:
                            fields[str(k)] = converted_value

                record = {"fields": fields}
                records_to_create.append(record)

        self.logger.info(f"增量同步计划: 新增 {len(records_to_create)} 条记录")

        if (
            records_to_create
            and isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            return self.process_in_batches(
                records_to_create,
                self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token,
                self.config.table_id,
            )
        else:
            self.logger.info("没有新记录需要同步")
            return True

    def _sync_incremental_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格增量同步 - 使用优化API策略"""
        if not self.config.index_column:
            self.logger.warning("未指定索引列，将新增全部数据")

            # ⭐ 检查选择性同步：如果启用，需要用列级控制追加
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                self.logger.info(
                    f"🎯 增量同步启用精确列控制: {self.config.selective_sync.columns}"
                )
                return self._append_selective_columns(df)

            # 常规增量同步策略
            values = self.converter.df_to_values(
                df, include_headers=False
            )  # 追加不需要表头
            self.logger.info("使用append接口进行增量同步")
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                return self.api.append_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    values,
                    self.config.batch_size,
                    self.config.rate_limit_delay,
                )
            return False

        # 获取现有数据
        current_df = self.get_current_sheet_data()

        if current_df.empty:
            self.logger.info("电子表格为空，新增全部数据")
            # ⭐ 检查选择性同步
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                return self._append_selective_columns(df)
            # 使用克隆同步（会先写入数据再设置格式）
            return self.sync_clone(df)

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 筛选需要新增的记录
        new_rows = []
        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if not index_hash or index_hash not in current_index:
                new_rows.append(row)

        self.logger.info(f"增量同步计划: 新增 {len(new_rows)} 行")

        if new_rows:
            new_df = pd.DataFrame(new_rows)

            # ⭐ 检查选择性同步：如果启用，需要用列级控制追加
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                return self._append_selective_columns(new_df)

            # 常规追加
            new_values = self.converter.df_to_values(new_df, include_headers=False)

            # 追加新数据
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info(f"开始增量追加 {len(new_values)} 行数据")
                return self.api.append_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    new_values,
                    self.config.batch_size,
                    self.config.rate_limit_delay,
                )
            return False
        else:
            self.logger.info("没有新记录需要同步")
            return True

    def _append_selective_columns(self, df: pd.DataFrame) -> bool:
        """选择性列的追加操作"""
        if (
            not self.config.selective_sync.enabled
            or not self.config.selective_sync.columns
        ):
            self.logger.warning("选择性同步未启用或未指定列，使用常规追加")
            values = self.converter.df_to_values(df, include_headers=False)
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                return self.api.append_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    values,
                    self.config.batch_size,
                    self.config.rate_limit_delay,
                )
            return False

        # 获取当前表格数据以确定正确的列位置
        current_df = self.get_current_sheet_data()

        effective_columns = self._get_effective_selective_columns(df)
        if not effective_columns:
            self.logger.warning("选择性列追加无可用列，已跳过")
            return False

        if current_df.empty:
            # 如果表格为空，先写入表头，然后追加数据
            self.logger.info("表格为空，先创建表头然后追加选择性列数据")
            header_values = [effective_columns]

            # 写入表头
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                header_success = self.api.write_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    header_values,
                    self.config.batch_size,
                    80,
                    self.config.rate_limit_delay,
                )
                if not header_success:
                    return False

                # 更新current_df为包含表头的空数据框
                current_df = pd.DataFrame(columns=effective_columns)

        # 准备选择性列数据
        column_data = self.converter.df_to_column_data(df, effective_columns)

        # 获取起始列偏移量
        start_col_offset = 0
        if isinstance(self.api, SheetAPI):
            start_col_offset = self.api.start_col_num - 1

        column_positions = self.converter.get_column_positions(
            current_df, effective_columns, start_col_offset
        )

        # 计算起始行：配置的起始行 + 当前数据行数 + 1（表头）
        start_row = self.config.start_row + len(current_df) + 1

        self.logger.info(
            f"🎯 选择性列追加: {list(column_data.keys())} 从第{start_row}行开始"
        )

        # 使用精确列追加API
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            effective_max_gap = (
                self.config.selective_sync.max_gap_for_merge
                if self.config.selective_sync.optimize_ranges
                else 0
            )
            return self.api.write_selective_columns(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                column_data,
                column_positions,
                start_row=start_row,
                rate_limit_delay=self.config.rate_limit_delay,
                max_gap=effective_max_gap,
            )

        return False

    def sync_overwrite(self, df: pd.DataFrame) -> bool:
        """覆盖同步：删除已存在的，然后新增全部"""
        self.logger.info("开始覆盖同步...")

        if not self.config.index_column:
            self.logger.error("覆盖同步模式需要指定索引列")
            return False

        # 检查是否启用选择性同步
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)
            self.logger.info(
                f"选择性同步已启用，处理 {len(self.config.selective_sync.columns) if self.config.selective_sync.columns else '所有'} 列"
            )

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_overwrite_bitable(df)
        else:  # SHEET
            return self._sync_overwrite_sheet(df)

    def _sync_overwrite_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格覆盖同步"""
        # 获取现有记录并建立索引（仅获取索引列，减少数据传输）
        fetch_fields = self._get_bitable_fetch_field_names(df, "overwrite")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        field_types = self.get_field_types()
        existing_index = self.converter.build_record_index(
            existing_records, self.config.index_column, field_types
        )

        # 找出需要删除的记录
        record_ids_to_delete = []

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column, field_types
            )
            if index_hash and index_hash in existing_index:
                existing_record = existing_index[index_hash]
                record_ids_to_delete.append(existing_record["record_id"])

        self.logger.info(
            f"覆盖同步计划: 删除 {len(record_ids_to_delete)} 条已存在记录，然后新增 {len(df)} 条记录"
        )

        # 删除已存在的记录
        delete_success = True
        if (
            record_ids_to_delete
            and isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            delete_success = self.process_in_batches(
                record_ids_to_delete,
                self.config.batch_size,
                self.api.batch_delete_records,
                self.config.app_token,
                self.config.table_id,
            )

        # 新增全部记录
        new_records = self.converter.df_to_records(df, field_types)
        create_success = False
        if (
            isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            create_success = self.process_in_batches(
                new_records,
                self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token,
                self.config.table_id,
            )

        return delete_success and create_success

    def _sync_overwrite_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格覆盖同步"""
        # 获取现有数据
        current_df = self.get_current_sheet_data()

        if current_df.empty:
            self.logger.info("电子表格为空，执行新增操作")
            return self.sync_clone(df)

        # ⭐ 检查是否启用选择性同步，使用精确列级控制
        if self.config.selective_sync.enabled and self.config.selective_sync.columns:
            self.logger.info(
                f"🎯 覆盖同步启用精确列控制: {self.config.selective_sync.columns}"
            )
            return self._sync_overwrite_selective_columns_sheet(df, current_df)

        # 原有的完整表格覆盖逻辑
        new_df_rows = []
        deleted_count = 0

        # 保留不在新数据中的现有记录
        for _, row in current_df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash:
                # 检查是否在新数据中
                found_in_new = False
                for _, new_row in df.iterrows():
                    new_index_hash = self.converter.get_index_value_hash(
                        new_row, self.config.index_column
                    )
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

            # 使用优化API策略覆盖写入
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                self.logger.info("使用write_sheet_data覆盖写入")
                return self.api.write_sheet_data(
                    self.config.spreadsheet_token,
                    self.config.sheet_id,
                    values,
                    self.config.batch_size,
                    80,  # col_batch_size
                    self.config.rate_limit_delay,
                )
            return False
        else:
            # 如果没有数据，清空表格
            if (
                isinstance(self.api, SheetAPI)
                and self.config.spreadsheet_token
                and self.config.sheet_id
            ):
                clear_range = self._build_sheet_full_range()
                if not clear_range:
                    self.logger.error("无法获取工作表网格范围，清空失败")
                    return False
                return self.api.clear_sheet_data(
                    self.config.spreadsheet_token, self.config.sheet_id, clear_range
                )
            return False

    def _sync_overwrite_selective_columns_sheet(
        self, df: pd.DataFrame, current_df: pd.DataFrame
    ) -> bool:
        """电子表格选择性列覆盖同步"""
        columns = self._get_effective_selective_columns(df)
        if not columns:
            self.logger.warning("选择性列覆盖同步未配置 columns 或无可用列，已跳过")
            return False
        self.logger.info(f"🎯 选择性列覆盖同步: {columns}")

        # 构建索引
        current_index = self.converter.build_data_index(
            current_df, self.config.index_column
        )

        # 准备数据映射 {row_idx: {col: value}}
        update_data_map: Dict[int, Dict[str, Any]] = {}  # 更新现有行的指定列
        new_rows: List[pd.Series] = []  # 全新的行

        for _, row in df.iterrows():
            index_hash = self.converter.get_index_value_hash(
                row, self.config.index_column
            )
            if index_hash and index_hash in current_index:
                # 覆盖现有行的指定列
                current_row_idx = current_index[index_hash]
                if current_row_idx not in update_data_map:
                    update_data_map[current_row_idx] = {}

                # 只覆盖指定列
                for col in columns:
                    if col in df.columns:
                        update_data_map[current_row_idx][col] = row[col]
            else:
                # 全新行
                new_rows.append(row)

        self.logger.info(
            f"选择性列覆盖计划: 覆盖 {len(update_data_map)} 行的指定列，新增 {len(new_rows)} 行"
        )

        success = True

        # 执行选择性列覆盖更新
        if update_data_map:
            success = self._update_selective_columns(current_df, update_data_map)

        # 追加新行（如果有）
        if new_rows and success:
            # 对于新行，也应该只包含选择性列
            if (
                self.config.selective_sync.enabled
                and self.config.selective_sync.columns
            ):
                success = self._append_selective_columns(pd.DataFrame(new_rows))
            else:
                # 常规追加
                new_df = pd.DataFrame(new_rows)
                new_values = self.converter.df_to_values(new_df, include_headers=False)

                if (
                    isinstance(self.api, SheetAPI)
                    and self.config.spreadsheet_token
                    and self.config.sheet_id
                ):
                    success = self.api.append_sheet_data(
                        self.config.spreadsheet_token,
                        self.config.sheet_id,
                        new_values,
                        self.config.batch_size,
                        self.config.rate_limit_delay,
                    )

        return success

    def sync_clone(self, df: pd.DataFrame) -> bool:
        """克隆同步：清空全部，然后新增全部"""
        self.logger.info("开始克隆同步...")

        if self.config.target_type == TargetType.BITABLE:
            return self._sync_clone_bitable(df)
        else:  # SHEET
            return self._sync_clone_sheet(df)

    def _sync_clone_bitable(self, df: pd.DataFrame) -> bool:
        """多维表格克隆同步"""
        # 获取所有现有记录（仅获取最小字段集，clone模式只需record_id）
        fetch_fields = self._get_bitable_fetch_field_names(df, "clone")
        existing_records = self.get_all_bitable_records(field_names=fetch_fields)
        existing_record_ids = [record["record_id"] for record in existing_records]

        self.logger.info(
            f"克隆同步计划: 删除 {len(existing_record_ids)} 条已有记录，然后新增 {len(df)} 条记录"
        )

        # 删除所有记录
        delete_success = True
        if (
            existing_record_ids
            and isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            delete_success = self.process_in_batches(
                existing_record_ids,
                self.config.batch_size,
                self.api.batch_delete_records,
                self.config.app_token,
                self.config.table_id,
            )

        # 新增全部记录
        field_types = self.get_field_types()
        new_records = self.converter.df_to_records(df, field_types)
        create_success = False
        if (
            isinstance(self.api, BitableAPI)
            and self.config.app_token
            and self.config.table_id
        ):
            create_success = self.process_in_batches(
                new_records,
                self.config.batch_size,
                self.api.batch_create_records,
                self.config.app_token,
                self.config.table_id,
            )

        return delete_success and create_success

    def _sync_clone_sheet(self, df: pd.DataFrame) -> bool:
        """电子表格克隆同步 - 使用优化API策略"""
        # 转换数据格式
        values = self.converter.df_to_values(df)

        self.logger.info(f"克隆同步计划: 清空现有数据，新增 {len(df)} 行")
        self.logger.info("使用write_sheet_data进行克隆写入")

        # 首先清空表格的一个大范围
        if (
            isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
            and self.config.sheet_id
        ):
            self.logger.info("清空现有数据...")
            clear_range = self._build_sheet_full_range()
            if not clear_range:
                self.logger.error("无法获取工作表网格范围，清空失败")
                return False
            clear_success = self.api.clear_sheet_data(
                self.config.spreadsheet_token, self.config.sheet_id, clear_range
            )
            if not clear_success:
                self.logger.error("清空电子表格失败，终止克隆同步")
                return False

            # 使用增强的写入方法
            write_success = self.api.write_sheet_data(
                self.config.spreadsheet_token,
                self.config.sheet_id,
                values,
                self.config.batch_size,
                80,  # col_batch_size
                self.config.rate_limit_delay,
            )
        else:
            write_success = False

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

        if not isinstance(self.api, SheetAPI):
            self.logger.error(
                "内部逻辑错误: _setup_sheet_intelligence 应该只被 SheetAPI 调用"
            )
            return False

        # 不同策略的配置范围不同
        strategy_name = self.config.field_type_strategy.value
        self.logger.info(f"开始电子表格智能字段配置 ({strategy_name}策略)...")

        # raw策略：不应用任何格式化，直接返回成功
        if strategy_name == "raw":
            self.logger.info("raw策略：跳过所有格式化，保持原始数据")
            return True

        # 生成字段配置
        field_config = self.converter.generate_sheet_field_config(
            df, self.config.field_type_strategy.value, self.config
        )

        success = True

        # 1. 配置下拉列表 (base策略跳过)
        if strategy_name != "base":
            for dropdown_config in field_config["dropdown_configs"]:
                column_name = dropdown_config["column"]

                # 计算列的绝对位置
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                # 计算行的绝对范围 (数据行，不含表头)
                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                # 仅在有数据行时才设置范围
                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                else:
                    self.logger.warning(
                        f"列 '{column_name}' 没有数据行，跳过下拉列表设置"
                    )
                    continue

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
                    dropdown_config["options"],
                    dropdown_config["multiple"],
                    dropdown_config["colors"],
                )

                if dropdown_success:
                    self.logger.info(f"成功为列 '{column_name}' 设置下拉列表")
                else:
                    self.logger.error(f"为列 '{column_name}' 设置下拉列表失败")
                    # 不设置success = False，允许继续其他列的操作
        else:
            self.logger.info("base策略跳过下拉列表配置")

        # 2. 配置日期格式
        if (
            field_config["date_columns"]
            and isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
        ):
            date_ranges = []
            for column_name in field_config["date_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    date_ranges.append(range_str)

            # 设置日期格式
            date_success = self.api.set_date_format(
                self.config.spreadsheet_token, date_ranges, "yyyy/MM/dd"
            )

            if date_success:
                self.logger.info(f"成功为 {len(date_ranges)} 个日期列设置格式")
            else:
                self.logger.error("设置日期格式失败")
                # 不设置success = False，允许继续其他操作

        # 3. 配置数字格式
        if (
            field_config["number_columns"]
            and isinstance(self.api, SheetAPI)
            and self.config.spreadsheet_token
        ):
            number_ranges = []
            for column_name in field_config["number_columns"]:
                start_col_num = self.api.column_letter_to_number(
                    self.config.start_column
                )
                col_index_in_df = list(df.columns).index(column_name)
                actual_col_num = start_col_num + col_index_in_df
                col_letter = self.api.column_number_to_letter(actual_col_num)

                start_data_row = self.config.start_row + 1
                end_data_row = self.config.start_row + len(df)

                if end_data_row >= start_data_row:
                    range_str = f"{self.config.sheet_id}!{col_letter}{start_data_row}:{col_letter}{end_data_row}"
                    number_ranges.append(range_str)

            # 设置数字格式
            number_success = self.api.set_number_format(
                self.config.spreadsheet_token, number_ranges, "#,##0.00"
            )

            if number_success:
                self.logger.info(f"成功为 {len(number_ranges)} 个数字列设置格式")
            else:
                self.logger.error("设置数字格式失败")
                # 不设置success = False，允许继续其他操作

        # 输出配置摘要
        dropdown_count = (
            len(field_config["dropdown_configs"]) if strategy_name != "base" else 0
        )
        date_count = len(field_config["date_columns"])
        number_count = len(field_config["number_columns"])
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
        target_name = (
            "多维表格" if self.config.target_type == TargetType.BITABLE else "电子表格"
        )
        self.logger.info(
            f"开始执行 {target_name} {self.config.sync_mode.value} 同步模式"
        )
        self.logger.info(f"数据源: {len(df)} 行 x {len(df.columns)} 列")

        # 重置转换统计
        self.converter.reset_stats()

        # 选择性同步前置过滤（影响字段创建/置信度分析范围）
        if self.config.selective_sync.enabled:
            df = self._apply_selective_filter(df)

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
                        if field_type == 2 and isinstance(
                            value, str
                        ):  # 数字字段但是字符串值
                            if not self.converter._is_number_string(str(value).strip()):
                                mismatch_warnings.append(
                                    f"字段 '{col_name}' 是数字类型，但包含非数字值: '{value}'"
                                )
                        elif field_type == 5 and isinstance(
                            value, str
                        ):  # 日期字段但是字符串值
                            if not (
                                self.converter._is_timestamp_string(str(value))
                                or self.converter._is_date_string(str(value))
                            ):
                                mismatch_warnings.append(
                                    f"字段 '{col_name}' 是日期类型，但包含非日期值: '{value}'"
                                )

            if mismatch_warnings:
                unique_warnings = list(
                    set(mismatch_warnings[:10])
                )  # 显示前10个唯一警告
                self.logger.warning(
                    f"发现 {len(set(mismatch_warnings))} 种数据类型不匹配情况（样本检查）:"
                )
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

    def _show_field_analysis_summary(
        self, df: pd.DataFrame, field_types: Dict[str, int]
    ):
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
