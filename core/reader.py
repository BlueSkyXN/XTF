#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据文件读取模块

模块概述：
    此模块提供统一的数据文件读取功能，支持多种文件格式的自动检测
    和读取。作为 XTF 工具的输入层，负责将各种格式的数据文件转换
    为 pandas DataFrame 供后续处理。

格式支持状态：
    - Excel (.xlsx/.xls): ✅ 稳定支持，生产就绪
        - 优先使用 Calamine 引擎（Rust实现，性能提升4-20倍）
        - 自动降级到 OpenPyXL 引擎（Python实现，稳定可靠）
    - CSV (.csv): 🧪 实验性支持，测试阶段
        - 自动处理编码问题（UTF-8/GBK）
        - 生产环境建议使用 Excel 格式

主要功能：
    1. 文件格式自动检测（基于扩展名）
    2. Excel 文件智能读取（引擎自动选择）
    3. CSV 文件编码自适应
    4. 统一的错误处理
    5. 格式支持查询

核心类：
    DataFileReader:
        数据文件读取器，提供统一的文件读取接口。
        根据文件扩展名自动选择合适的读取方式。

读取流程：
    1. 检查文件是否存在
    2. 根据扩展名判断文件格式
    3. 调用对应的读取方法
    4. 返回 DataFrame 或抛出异常

Excel 读取策略：
    1. 优先尝试 Calamine 引擎（高性能）
    2. Calamine 失败则降级到 OpenPyXL
    3. 两者都失败则抛出异常

CSV 编码处理：
    1. 首先尝试 UTF-8 编码
    2. UTF-8 失败则尝试 GBK（中文Windows Excel导出常用）
    3. 两者都失败则抛出异常并提示手动指定编码

使用示例：
    >>> from core.reader import DataFileReader
    >>> reader = DataFileReader()
    >>> 
    >>> # 读取 Excel 文件
    >>> df = reader.read_file(Path('data.xlsx'))
    >>> 
    >>> # 读取 CSV 文件
    >>> df = reader.read_file(Path('data.csv'))
    >>> 
    >>> # 带额外参数读取
    >>> df = reader.read_file(Path('data.xlsx'), sheet_name='Sheet2')
    >>> 
    >>> # 检查格式支持
    >>> if DataFileReader.is_supported(Path('file.xlsx')):
    ...     df = reader.read_file(Path('file.xlsx'))

类方法说明：
    is_supported(file_path): 检查文件格式是否支持
    get_supported_formats(): 获取支持的格式列表字符串

依赖关系：
    内部模块：
        - utils.excel_reader: 智能Excel读取引擎（可选）
    外部依赖：
        - pandas: DataFrame 支持
        - pathlib: 路径处理
        - logging: 日志记录

向后兼容性：
    - Excel 读取逻辑与原有 pd.read_excel() 完全一致
    - 不影响任何现有 Excel 处理功能
    - 仅在输入层增加格式识别

注意事项：
    1. CSV 格式当前为实验性功能，生产环境请使用 Excel
    2. 文件路径必须是 Path 对象
    3. 读取失败会抛出相应异常（FileNotFoundError/ValueError）
    4. 支持传递额外参数到底层 pandas 读取函数

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any

# 导入智能Excel读取引擎（性能优化）
try:
    from utils.excel_reader import smart_read_excel

    SMART_EXCEL_AVAILABLE = True
except ImportError:
    SMART_EXCEL_AVAILABLE = False


class DataFileReader:
    """
    数据文件读取器

    支持的文件格式：
    - Excel: .xlsx, .xls (✅ 稳定支持，生产就绪)
    - CSV: .csv (🧪 实验性支持，测试阶段)

    特性：
    - 自动根据文件扩展名选择读取方式
    - Excel格式完全支持，保持原有稳定性
    - CSV自动处理编码问题（UTF-8/GBK）
    - 统一的错误处理
    - 易于扩展新格式

    向后兼容性保证：
    - Excel读取逻辑与原有 pd.read_excel() 完全一致
    - 不影响任何现有Excel处理功能
    - 仅在输入层增加格式识别，处理层和输出层无需修改
    """

    # 支持的文件格式
    SUPPORTED_FORMATS = {
        ".xlsx": "Excel 2007+ (稳定)",
        ".xls": "Excel 97-2003 (稳定)",
        ".csv": "CSV (实验性)",
    }

    def __init__(self):
        """初始化文件读取器"""
        self.logger = logging.getLogger("XTF.reader")

    def read_file(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        根据文件扩展名自动选择读取方式

        Args:
            file_path: 文件路径
            **kwargs: 额外的读取参数，传递给底层的pandas读取函数

        Returns:
            pd.DataFrame: 读取的数据

        Raises:
            ValueError: 不支持的文件格式
            FileNotFoundError: 文件不存在

        Examples:
            >>> reader = DataFileReader()
            >>> df = reader.read_file(Path('data.csv'))
            >>> df = reader.read_file(Path('data.xlsx'))
        """
        if not file_path.exists():
            raise FileNotFoundError(f"文件不存在: {file_path}")

        file_ext = file_path.suffix.lower()

        self.logger.info(f"检测到文件格式: {file_ext}")

        if file_ext == ".csv":
            return self._read_csv(file_path, **kwargs)
        elif file_ext in [".xlsx", ".xls"]:
            return self._read_excel(file_path, **kwargs)
        else:
            supported = ", ".join(self.SUPPORTED_FORMATS.keys())
            raise ValueError(
                f"不支持的文件格式: {file_ext}\n" f"支持的格式: {supported}"
            )

    def _read_excel(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        读取Excel文件

        优先使用 smart_read_excel（Calamine引擎，性能提升4-20倍）
        smart_read_excel不可用时使用传统的 pd.read_excel

        Args:
            file_path: Excel文件路径
            **kwargs: pandas.read_excel的额外参数

        Returns:
            pd.DataFrame: 读取的数据

        Note:
            smart_read_excel内部已实现 Calamine → OpenPyXL 的自动降级
            如果smart_read_excel失败，说明两个引擎都已尝试失败
        """
        if SMART_EXCEL_AVAILABLE:
            # 使用智能Excel读取引擎（内部包含 Calamine → OpenPyXL 自动降级）
            self.logger.debug(f"使用 smart_read_excel 读取文件: {file_path}")
            try:
                df = smart_read_excel(file_path, **kwargs)
                self.logger.info(
                    f"Excel文件读取成功: {len(df)} 行 × {len(df.columns)} 列"
                )
                return df
            except Exception as e:
                # smart_read_excel 内部已尝试 Calamine 和 OpenPyXL，都失败了
                self.logger.error(f"Excel文件读取失败（所有引擎已尝试）: {e}")
                raise
        else:
            # smart_read_excel 不可用，使用传统方式作为兜底
            self.logger.debug(
                f"使用 pd.read_excel (OpenPyXL引擎) 读取文件: {file_path}"
            )
            try:
                df = pd.read_excel(file_path, **kwargs)
                self.logger.info(
                    f"Excel文件读取成功 (OpenPyXL引擎): {len(df)} 行 × {len(df.columns)} 列"
                )
                return df
            except Exception as e:
                self.logger.error(f"Excel文件读取失败: {e}")
                raise

    def _read_csv(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        读取CSV文件，自动处理编码问题

        🧪 实验性功能：当前处于测试阶段，仅建议在测试环境使用

        Args:
            file_path: CSV文件路径
            **kwargs: pandas.read_csv的额外参数

        Returns:
            pd.DataFrame: 读取的数据

        Note:
            - 🧪 当前为实验性功能，生产环境请使用Excel格式
            - 优先使用UTF-8编码
            - UTF-8失败时自动尝试GBK编码（中文Windows Excel导出常用）
            - 默认使用逗号作为分隔符
            - 默认第一行为表头
        """
        # 设置合理的默认值
        default_kwargs = {
            "encoding": "utf-8",  # 优先尝试UTF-8
            "sep": ",",  # 逗号分隔
            "header": 0,  # 第一行为表头
        }

        # 用户参数覆盖默认值
        default_kwargs.update(kwargs)

        self.logger.debug(f"使用 pd.read_csv 读取文件: {file_path}")
        self.logger.debug(
            f"CSV参数: encoding={default_kwargs.get('encoding')}, "
            f"sep={default_kwargs.get('sep')}, "
            f"header={default_kwargs.get('header')}"
        )

        try:
            # 首次尝试（通常是UTF-8）
            df = pd.read_csv(file_path, **default_kwargs)
            self.logger.info(
                f"CSV文件读取成功 (编码: {default_kwargs.get('encoding')}): "
                f"{len(df)} 行 × {len(df.columns)} 列"
            )
            return df

        except UnicodeDecodeError as e:
            # UTF-8失败，尝试GBK（中文Excel导出的CSV常用）
            self.logger.warning(f"UTF-8编码读取失败，尝试GBK编码: {e}")
            default_kwargs["encoding"] = "gbk"

            try:
                df = pd.read_csv(file_path, **default_kwargs)
                self.logger.info(
                    f"CSV文件读取成功 (编码: GBK): "
                    f"{len(df)} 行 × {len(df.columns)} 列"
                )
                return df
            except Exception as e2:
                self.logger.error(f"GBK编码读取也失败: {e2}")
                raise ValueError(
                    f"无法读取CSV文件，尝试了UTF-8和GBK编码都失败。\n"
                    f"请检查文件编码或手动指定 encoding 参数。\n"
                    f"原始错误: {e2}"
                )

        except Exception as e:
            self.logger.error(f"CSV文件读取失败: {e}")
            raise

    @classmethod
    def get_supported_formats(cls) -> str:
        """
        获取支持的格式列表字符串

        Returns:
            str: 格式化的支持格式列表

        Example:
            >>> DataFileReader.get_supported_formats()
            '.xlsx (Excel 2007+), .xls (Excel 97-2003), .csv (CSV)'
        """
        formats = [f"{ext} ({desc})" for ext, desc in cls.SUPPORTED_FORMATS.items()]
        return ", ".join(formats)

    @classmethod
    def is_supported(cls, file_path: Path) -> bool:
        """
        检查文件格式是否支持

        Args:
            file_path: 文件路径

        Returns:
            bool: 是否支持该格式
        """
        file_ext = file_path.suffix.lower()
        return file_ext in cls.SUPPORTED_FORMATS
