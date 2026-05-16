#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一配置管理模块

模块概述：
    此模块提供 XTF 工具的完整配置管理功能，支持多维表格（Bitable）
    和电子表格（Sheet）两种目标类型的配置，实现配置的统一管理和验证。

主要功能：
    1. 配置数据结构定义（枚举、数据类）
    2. 配置文件的读取和保存（YAML格式）
    3. 命令行参数解析
    4. 配置验证和默认值处理
    5. 高级控制器的创建

核心类：
    枚举类型：
        - FieldTypeStrategy: 字段类型选择策略（raw/base/auto/intelligence）
        - SyncMode: 同步模式（full/incremental/overwrite/clone）
        - TargetType: 目标类型（bitable/sheet）

    配置数据类：
        - SelectiveSyncConfig: 选择性同步配置
        - SyncConfig: 统一同步配置（主配置类）

    管理器类：
        - ConfigManager: 配置管理器（静态方法集合）

配置优先级：
    命令行参数 > 配置文件 > 智能推断 > 系统默认值

配置验证规则：
    - Bitable模式：必须提供 app_token 和 table_id
    - Sheet模式：必须提供 spreadsheet_token 和 sheet_id
    - SelectiveSync：与 Clone 模式互斥
    - 列名验证：不允许重复、空值、None

使用示例：
    # 从配置文件创建配置
    >>> config = ConfigManager.create_config()

    # 创建示例配置文件
    >>> create_sample_config("config.yaml", TargetType.BITABLE)

    # 直接创建配置对象
    >>> config = SyncConfig(
    ...     file_path="data.xlsx",
    ...     app_id="cli_xxx",
    ...     app_secret="xxx",
    ...     target_type=TargetType.SHEET,
    ...     spreadsheet_token="xxx",
    ...     sheet_id="xxx"
    ... )

依赖关系：
    内部依赖：
        - core.control: 高级控制器（GlobalRequestController）
    外部依赖：
        - yaml: YAML配置文件解析
        - argparse: 命令行参数解析
        - dataclasses: 数据类支持

注意事项：
    1. SyncConfig 的 __post_init__ 会自动进行类型转换和验证
    2. Intelligence 策略仅支持配置文件设置，不支持命令行
    3. SelectiveSync 的 max_gap_for_merge 上限为 50
    4. 配置文件支持 UTF-8 编码的中文内容

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import argparse
import sys
from pathlib import Path
from enum import Enum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Union

import yaml  # type: ignore[import-untyped]


class FieldTypeStrategy(Enum):
    """字段类型选择策略枚举"""

    BASE = "base"  # 基础策略 - 仅创建文本/数字/日期三种基础类型【默认】
    AUTO = "auto"  # 自动策略 - 增加Excel类型检测（单选多选等）
    INTELLIGENCE = "intelligence"  # 智能策略 - 基于置信度算法，仅支持配置文件
    RAW = "raw"  # 原值策略 - 不应用任何格式化，保持原始数据


class SyncMode(Enum):
    """同步模式枚举"""

    FULL = "full"  # 全量同步：已存在的更新，不存在的新增
    INCREMENTAL = "incremental"  # 增量同步：只新增不存在的记录
    OVERWRITE = "overwrite"  # 覆盖同步：删除已存在的，然后新增全部
    CLONE = "clone"  # 克隆同步：清空全部，然后新增全部


class TargetType(Enum):
    """目标类型枚举"""

    BITABLE = "bitable"  # 多维表格
    SHEET = "sheet"  # 电子表格


@dataclass
class SelectiveSyncConfig:
    """选择性同步配置"""

    enabled: bool = False
    columns: Optional[List[str]] = None
    auto_include_index: bool = True
    optimize_ranges: bool = True
    max_gap_for_merge: int = 2
    preserve_column_order: bool = True


@dataclass
class SyncConfig:
    """统一同步配置"""

    # 基础配置
    file_path: str
    app_id: str
    app_secret: str
    target_type: TargetType

    # Excel工作表选择
    excel_sheet_name: Optional[Union[str, int]] = (
        None  # 工作表名称或索引（0=第一个sheet）
    )

    # 多维表格配置（target_type=bitable时使用）
    app_token: Optional[str] = None
    table_id: Optional[str] = None
    create_missing_fields: bool = True

    # 智能字段类型选择配置
    field_type_strategy: FieldTypeStrategy = FieldTypeStrategy.BASE

    # Intelligence策略专用配置（仅配置文件支持）
    intelligence_date_confidence: float = 0.85  # 日期类型置信度
    intelligence_choice_confidence: float = 0.9  # 选择类型置信度
    intelligence_boolean_confidence: float = 0.95  # 布尔类型置信度

    # 电子表格配置（target_type=sheet时使用）
    spreadsheet_token: Optional[str] = None
    sheet_id: Optional[str] = None
    start_row: int = 1  # 开始行号（1-based）
    start_column: str = "A"  # 开始列号
    # 电子表格读取渲染选项（可选）
    sheet_value_render_option: Optional[str] = (
        None  # ToString/Formula/FormattedValue/UnformattedValue
    )
    sheet_datetime_render_option: Optional[str] = None  # FormattedString
    # 电子表格读取/写入范围控制（默认 5000x100）
    sheet_scan_max_rows: int = 5000
    sheet_scan_max_cols: int = 100
    sheet_write_max_rows: int = 5000
    sheet_write_max_cols: int = 100

    # 电子表格逻辑同步与结果检测（默认全部关闭）
    sheet_validate_results: bool = False  # 是否启用结果检测
    sheet_protect_formulas: bool = False  # 是否保护云端公式列不被覆盖
    sheet_report_column_diff: bool = False  # 是否输出列级差异报告
    sheet_diff_tolerance: float = 0.001  # 数值差异容忍度（浮点数比较）

    # 同步设置
    sync_mode: SyncMode = SyncMode.FULL
    index_column: Optional[str] = None  # 索引列名，用于记录比对

    # 性能设置
    batch_size: int = 500  # 批处理大小
    rate_limit_delay: float = 0.01  # 接口调用间隔
    max_retries: int = 3  # 最大重试次数

    # 高级控制开关
    enable_advanced_control: bool = False  # 是否启用高级重试和频控策略

    # 高级重试配置（仅当enable_advanced_control=True时生效）
    retry_strategy_type: str = (
        "exponential_backoff"  # 重试策略: exponential_backoff, linear_growth, fixed_wait
    )
    retry_initial_delay: float = 0.5  # 重试初始延迟时间（秒），支持小于1的数
    retry_max_wait_time: Optional[float] = None  # 最大单次等待时间（秒）
    retry_multiplier: float = 2.0  # 指数退避倍数（仅指数退避策略使用）
    retry_increment: float = 0.5  # 线性增长步长（仅线性增长策略使用）

    # 高级频控配置（仅当enable_advanced_control=True时生效）
    rate_limit_strategy_type: str = (
        "fixed_wait"  # 频控策略: fixed_wait, sliding_window, fixed_window
    )
    rate_limit_window_size: float = 1.0  # 时间窗大小（秒），支持小于1的数
    rate_limit_max_requests: int = 10  # 时间窗内的最大请求数

    # 日志设置
    log_level: str = "INFO"

    # 选择性同步配置
    selective_sync: SelectiveSyncConfig = field(default_factory=SelectiveSyncConfig)

    def __post_init__(self):
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode(self.sync_mode)
        if isinstance(self.target_type, str):
            self.target_type = TargetType(self.target_type)
        if isinstance(self.field_type_strategy, str):
            self.field_type_strategy = FieldTypeStrategy(self.field_type_strategy)

        # 验证必需参数
        if self.target_type == TargetType.BITABLE:
            if not self.app_token or not self.table_id:
                raise ValueError("多维表格模式需要app_token和table_id")
        elif self.target_type == TargetType.SHEET:
            if not self.spreadsheet_token or not self.sheet_id:
                raise ValueError("电子表格模式需要spreadsheet_token和sheet_id")

        # 验证 selective 配置
        if self.selective_sync.enabled:
            if self.sync_mode == SyncMode.CLONE:
                raise ValueError("Clone 模式不支持 selective 同步")
            if not self.selective_sync.columns:
                raise ValueError("启用 selective 同步时必须指定 columns")

            # 增强的配置组合有效性检查
            self._validate_selective_sync_config()

        # 规范化读取渲染参数（大小写容错）
        if self.sheet_value_render_option is not None:
            val = str(self.sheet_value_render_option).strip()
            if not val or val.lower() == "none":
                self.sheet_value_render_option = None
            else:
                mapping = {
                    "tostring": "ToString",
                    "formula": "Formula",
                    "formattedvalue": "FormattedValue",
                    "unformattedvalue": "UnformattedValue",
                }
                self.sheet_value_render_option = mapping.get(val.lower(), val)

        if self.sheet_datetime_render_option is not None:
            val = str(self.sheet_datetime_render_option).strip()
            if not val or val.lower() == "none":
                self.sheet_datetime_render_option = None
            else:
                mapping = {"formattedstring": "FormattedString"}
                self.sheet_datetime_render_option = mapping.get(val.lower(), val)

        # 验证 sheet 扫描/写入范围参数
        if self.sheet_scan_max_rows <= 0 or self.sheet_scan_max_cols <= 0:
            raise ValueError("sheet_scan_max_rows/sheet_scan_max_cols 必须为正整数")
        if self.sheet_write_max_rows <= 0 or self.sheet_write_max_cols <= 0:
            raise ValueError("sheet_write_max_rows/sheet_write_max_cols 必须为正整数")

        # 读取渲染参数合法性校验（可选）
        allowed_value_render = {
            None,
            "ToString",
            "Formula",
            "FormattedValue",
            "UnformattedValue",
        }
        allowed_datetime_render = {None, "FormattedString"}
        if self.sheet_value_render_option not in allowed_value_render:
            raise ValueError(
                f"sheet_value_render_option 无效: {self.sheet_value_render_option}"
            )
        if self.sheet_datetime_render_option not in allowed_datetime_render:
            raise ValueError(
                f"sheet_datetime_render_option 无效: {self.sheet_datetime_render_option}"
            )

        # 验证逻辑同步与结果检测配置
        if self.sheet_diff_tolerance < 0:
            raise ValueError("sheet_diff_tolerance 不能为负数")
        if self.sheet_protect_formulas and not self.sheet_validate_results:
            # 保护公式时必须启用结果检测（需要双读）
            self.sheet_validate_results = True

    def _validate_selective_sync_config(self):
        """验证selective_sync配置的详细有效性"""
        columns = self.selective_sync.columns

        # 1. 检查列表元素有效性
        if not isinstance(columns, list):
            raise ValueError("selective_sync.columns 必须是列表类型")

        valid_columns = []
        for i, col in enumerate(columns):
            if col is None:
                raise ValueError(f"selective_sync.columns[{i}] 不能为 None")
            if not isinstance(col, str):
                raise ValueError(
                    f"selective_sync.columns[{i}] 必须是字符串类型，当前为 {type(col)}"
                )
            if not col.strip():
                raise ValueError(f"selective_sync.columns[{i}] 不能为空字符串")

            col_clean = col.strip()
            valid_columns.append(col_clean)

        # 2. 检查重复列名
        if len(valid_columns) != len(set(valid_columns)):
            duplicates = [col for col in valid_columns if valid_columns.count(col) > 1]
            raise ValueError(
                f"selective_sync.columns 包含重复的列名: {list(set(duplicates))}"
            )

        # 3. 验证范围优化参数
        if not isinstance(self.selective_sync.max_gap_for_merge, int):
            raise ValueError("selective_sync.max_gap_for_merge 必须是整数")
        if self.selective_sync.max_gap_for_merge < 0:
            raise ValueError("selective_sync.max_gap_for_merge 不能为负数")
        if self.selective_sync.max_gap_for_merge > 50:  # 设置合理上限
            raise ValueError("selective_sync.max_gap_for_merge 不应超过50（性能考虑）")

        # 更新清理后的列名列表
        self.selective_sync.columns = valid_columns


class ConfigManager:
    """统一配置管理器"""

    @staticmethod
    def load_from_file(config_file: str) -> Optional[Dict[str, Any]]:
        """从YAML文件加载配置"""
        try:
            with open(config_file, "r", encoding="utf-8") as f:
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
        with open(config_file, "w", encoding="utf-8") as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)

    @staticmethod
    def parse_target_type() -> TargetType:
        """解析目标类型"""
        parser = argparse.ArgumentParser(
            description="XTF - Excel To Feishu 统一同步工具"
        )

        # 添加目标类型参数
        parser.add_argument(
            "--target-type",
            type=str,
            choices=["bitable", "sheet"],
            help="目标类型: bitable(多维表格) 或 sheet(电子表格)",
        )
        parser.add_argument(
            "--config",
            "-c",
            type=str,
            default="config.yaml",
            help="配置文件路径 (默认: config.yaml)",
        )

        # 只解析已知参数，忽略其他参数
        args, _ = parser.parse_known_args()

        # 如果没有指定目标类型，尝试从配置文件推断
        if not args.target_type:
            if Path(args.config).exists():
                try:
                    config_data = ConfigManager.load_from_file(args.config)
                    if config_data:
                        # 首先检查 target_type 参数
                        if config_data.get("target_type"):
                            target_type_val = config_data.get("target_type")
                            if target_type_val == "bitable":
                                return TargetType.BITABLE
                            elif target_type_val == "sheet":
                                return TargetType.SHEET
                        # 如果配置中有app_token和table_id，推断为多维表格
                        elif config_data.get("app_token") and config_data.get(
                            "table_id"
                        ):
                            return TargetType.BITABLE
                        # 如果配置中有spreadsheet_token和sheet_id，推断为电子表格
                        elif config_data.get("spreadsheet_token") and config_data.get(
                            "sheet_id"
                        ):
                            return TargetType.SHEET
                except Exception:
                    pass

            # 默认使用多维表格
            print("⚠️  未指定目标类型，默认使用多维表格模式")
            print("💡 可以通过 --target-type bitable|sheet 指定目标类型")
            return TargetType.BITABLE

        return TargetType(args.target_type)

    @staticmethod
    def parse_args() -> argparse.Namespace:
        """解析命令行参数"""
        parser = argparse.ArgumentParser(
            description="XTF - Excel To Feishu 统一同步工具"
        )

        # 基础配置
        parser.add_argument(
            "--config",
            "-c",
            type=str,
            default="config.yaml",
            help="配置文件路径 (默认: config.yaml)",
        )
        parser.add_argument("--file-path", type=str, help="Excel文件路径")
        parser.add_argument("--app-id", type=str, help="飞书应用ID")
        parser.add_argument("--app-secret", type=str, help="飞书应用密钥")
        parser.add_argument(
            "--target-type",
            type=str,
            choices=["bitable", "sheet"],
            help="目标类型: bitable(多维表格) 或 sheet(电子表格)",
        )

        # 多维表格配置
        parser.add_argument("--app-token", type=str, help="多维表格应用Token")
        parser.add_argument("--table-id", type=str, help="数据表ID")
        parser.add_argument(
            "--create-missing-fields",
            type=str,
            choices=["true", "false"],
            help="是否自动创建缺失字段",
        )
        parser.add_argument(
            "--no-create-fields",
            action="store_true",
            help="不自动创建缺失字段（兼容参数）",
        )
        parser.add_argument(
            "--field-type-strategy",
            type=str,
            choices=["raw", "base", "auto", "intelligence"],
            help="字段类型选择策略",
        )

        # 电子表格配置
        parser.add_argument("--spreadsheet-token", type=str, help="电子表格Token")
        parser.add_argument("--sheet-id", type=str, help="工作表ID")
        parser.add_argument("--start-row", type=int, help="开始行号")
        parser.add_argument("--start-column", type=str, help="开始列号")

        # 同步设置
        parser.add_argument(
            "--sync-mode",
            type=str,
            choices=["full", "incremental", "overwrite", "clone"],
            help="同步模式",
        )
        parser.add_argument("--index-column", type=str, help="索引列名")

        # 性能设置
        parser.add_argument("--batch-size", type=int, help="批处理大小")
        parser.add_argument("--rate-limit-delay", type=float, help="接口调用间隔秒数")
        parser.add_argument("--max-retries", type=int, help="最大重试次数")

        # 日志设置
        parser.add_argument(
            "--log-level",
            type=str,
            choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            help="日志级别",
        )

        return parser.parse_args()

    @classmethod
    def create_config(cls) -> SyncConfig:
        """创建配置对象"""
        # 先获取目标类型
        target_type = cls.parse_target_type()

        args = cls.parse_args()

        # 根据目标类型设置默认值
        if target_type == TargetType.BITABLE:
            config_data = {
                "target_type": target_type.value,
                "sync_mode": "full",
                "batch_size": 500,
                "rate_limit_delay": 0.01,
                "max_retries": 3,
                "create_missing_fields": True,
                "field_type_strategy": "base",
                "intelligence_date_confidence": 0.85,
                "intelligence_choice_confidence": 0.9,
                "intelligence_boolean_confidence": 0.95,
                "log_level": "INFO",
            }
        else:  # SHEET
            config_data = {
                "target_type": target_type.value,
                "sync_mode": "full",
                "start_row": 1,
                "start_column": "A",
                "batch_size": 1000,
                "rate_limit_delay": 0.1,
                "max_retries": 3,
                "log_level": "INFO",
                "sheet_value_render_option": None,
                "sheet_datetime_render_option": None,
                "sheet_scan_max_rows": 5000,
                "sheet_scan_max_cols": 100,
                "sheet_write_max_rows": 5000,
                "sheet_write_max_cols": 100,
                "sheet_validate_results": False,
                "sheet_protect_formulas": False,
                "sheet_report_column_diff": False,
                "sheet_diff_tolerance": 0.001,
                "selective_sync": {
                    "enabled": False,
                    "columns": None,
                    "auto_include_index": True,
                    "optimize_ranges": True,
                    "max_gap_for_merge": 2,
                    "preserve_column_order": True,
                },
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

        # 确保target_type在配置数据中
        config_data["target_type"] = target_type.value

        # 命令行参数覆盖文件配置
        cli_overrides = []

        # 基础参数
        if args.file_path:
            config_data["file_path"] = args.file_path
            cli_overrides.append(f"file_path={args.file_path}")
        if args.app_id:
            config_data["app_id"] = args.app_id
            cli_overrides.append(f"app_id={args.app_id[:8]}...")
        if args.app_secret:
            config_data["app_secret"] = args.app_secret
            cli_overrides.append("app_secret=***")
        if args.target_type:
            config_data["target_type"] = args.target_type
            cli_overrides.append(f"target_type={args.target_type}")

        # 多维表格参数
        if args.app_token:
            config_data["app_token"] = args.app_token
            cli_overrides.append(f"app_token={args.app_token[:8]}...")
        if args.table_id:
            config_data["table_id"] = args.table_id
            cli_overrides.append(f"table_id={args.table_id}")
        # 处理create_missing_fields参数（支持两种方式）
        if args.create_missing_fields is not None:
            config_data["create_missing_fields"] = (
                args.create_missing_fields.lower() == "true"
            )
            cli_overrides.append(f"create_missing_fields={args.create_missing_fields}")
        elif args.no_create_fields:
            config_data["create_missing_fields"] = False
            cli_overrides.append("create_missing_fields=False")
        if args.field_type_strategy:
            config_data["field_type_strategy"] = args.field_type_strategy
            cli_overrides.append(f"field_type_strategy={args.field_type_strategy}")

        # 电子表格参数
        if args.spreadsheet_token:
            config_data["spreadsheet_token"] = args.spreadsheet_token
            cli_overrides.append(f"spreadsheet_token={args.spreadsheet_token[:8]}...")
        if args.sheet_id:
            config_data["sheet_id"] = args.sheet_id
            cli_overrides.append(f"sheet_id={args.sheet_id}")
        if args.start_row is not None:
            config_data["start_row"] = args.start_row
            cli_overrides.append(f"start_row={args.start_row}")
        if args.start_column:
            config_data["start_column"] = args.start_column
            cli_overrides.append(f"start_column={args.start_column}")

        # Sheet 扫描/写入与读取渲染参数（仅配置文件支持，不做命令行）

        # 通用参数
        if args.index_column:
            config_data["index_column"] = args.index_column
            cli_overrides.append(f"index_column={args.index_column}")
        if args.sync_mode is not None:
            config_data["sync_mode"] = args.sync_mode
            cli_overrides.append(f"sync_mode={args.sync_mode}")
        if args.batch_size is not None:
            config_data["batch_size"] = args.batch_size
            cli_overrides.append(f"batch_size={args.batch_size}")
        if args.rate_limit_delay is not None:
            config_data["rate_limit_delay"] = args.rate_limit_delay
            cli_overrides.append(f"rate_limit_delay={args.rate_limit_delay}")
        if args.max_retries is not None:
            config_data["max_retries"] = args.max_retries
            cli_overrides.append(f"max_retries={args.max_retries}")
        if args.log_level is not None:
            config_data["log_level"] = args.log_level
            cli_overrides.append(f"log_level={args.log_level}")

        # 显示命令行覆盖的参数
        if cli_overrides:
            print(f"🔧 命令行参数覆盖: {', '.join(cli_overrides)}")

        # 处理 selective_sync 配置
        if "selective_sync" in config_data and isinstance(
            config_data["selective_sync"], dict
        ):
            selective_config = config_data["selective_sync"]
            config_data["selective_sync"] = SelectiveSyncConfig(**selective_config)
        elif "selective_sync" not in config_data:
            config_data["selective_sync"] = SelectiveSyncConfig()

        # 验证必需参数
        required_fields = ["file_path", "app_id", "app_secret"]
        if target_type == TargetType.BITABLE:
            required_fields.extend(["app_token", "table_id"])
        else:  # SHEET
            required_fields.extend(["spreadsheet_token", "sheet_id"])

        missing_fields = [f for f in required_fields if not config_data.get(f)]

        if missing_fields:
            print(f"\n❌ 错误: 缺少必需参数: {', '.join(missing_fields)}")
            print("💡 请通过以下方式提供这些参数:")
            print("   1. 在配置文件中设置")
            print("   2. 通过命令行参数指定")
            print("\n命令行参数示例:")
            for field in missing_fields:
                field_name = field.replace("_", "-")
                print(f"   --{field_name} <值>")
            sys.exit(1)

        return SyncConfig(**config_data)

    @staticmethod
    def create_request_controller(config: SyncConfig):
        """从配置创建请求控制器"""
        # 检查是否启用高级控制
        if not config.enable_advanced_control:
            return None  # 返回None表示使用传统控制方式

        from .control import GlobalRequestController

        # 准备重试配置
        retry_config = {
            "initial_delay": config.retry_initial_delay,
            "max_retries": config.max_retries,  # 保持向后兼容，使用传统的max_retries
            "max_wait_time": config.retry_max_wait_time,
            "multiplier": config.retry_multiplier,
            "increment": config.retry_increment,
        }

        # 准备频控配置
        rate_limit_config = {
            "delay": config.rate_limit_delay,  # 保持向后兼容，使用传统的rate_limit_delay
            "window_size": config.rate_limit_window_size,
            "max_requests": config.rate_limit_max_requests,
        }

        # 创建全局控制器
        return GlobalRequestController.create_from_config(
            retry_type=config.retry_strategy_type,
            retry_config=retry_config,
            rate_limit_type=config.rate_limit_strategy_type,
            rate_limit_config=rate_limit_config,
        )


def create_sample_config(
    config_file: str = "config.yaml", target_type: TargetType = TargetType.BITABLE
):
    """创建示例配置文件"""
    if target_type == TargetType.BITABLE:
        sample_config = {
            "file_path": "data.xlsx",
            "app_id": "cli_your_app_id",
            "app_secret": "your_app_secret",
            "target_type": "bitable",
            "app_token": "your_app_token",
            "table_id": "your_table_id",
            "sync_mode": "full",
            "index_column": "ID",
            "batch_size": 500,
            "rate_limit_delay": 0.01,
            "max_retries": 3,
            "create_missing_fields": True,
            "field_type_strategy": "base",
            "intelligence_date_confidence": 0.85,
            "intelligence_choice_confidence": 0.9,
            "intelligence_boolean_confidence": 0.95,
            "log_level": "INFO",
        }
    else:  # SHEET
        sample_config = {
            "file_path": "data.xlsx",
            "app_id": "cli_your_app_id",
            "app_secret": "your_app_secret",
            "target_type": "sheet",
            "spreadsheet_token": "your_spreadsheet_token",
            "sheet_id": "your_sheet_id",
            "sync_mode": "full",
            "index_column": "ID",
            "start_row": 1,
            "start_column": "A",
            "batch_size": 1000,
            "rate_limit_delay": 0.1,
            "max_retries": 3,
            "log_level": "INFO",
            "sheet_value_render_option": None,
            "sheet_datetime_render_option": None,
            "sheet_scan_max_rows": 5000,
            "sheet_scan_max_cols": 100,
            "sheet_write_max_rows": 5000,
            "sheet_write_max_cols": 100,
            "sheet_validate_results": False,
            "sheet_protect_formulas": False,
            "sheet_report_column_diff": False,
            "sheet_diff_tolerance": 0.001,
            "selective_sync": {
                "enabled": False,
                "columns": ["column1", "column2", "column3"],
                "auto_include_index": True,
                "optimize_ranges": True,
                "max_gap_for_merge": 2,
                "preserve_column_order": True,
            },
        }

    if not Path(config_file).exists():
        ConfigManager.save_to_file(sample_config, config_file)
        print(f"已创建示例配置文件: {config_file}")
        print("请编辑配置文件并填入正确的参数值")
        return True
    else:
        print(f"配置文件 {config_file} 已存在")
        return False


def get_target_description(target_type: TargetType) -> str:
    """获取目标类型的描述"""
    descriptions = {
        TargetType.BITABLE: "多维表格 (支持智能字段管理、复杂数据类型)",
        TargetType.SHEET: "电子表格 (简单快速、适合基础数据同步)",
    }
    return descriptions.get(target_type, "未知类型")
