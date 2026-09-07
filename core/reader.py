#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Read Excel/CSV files into DataFrames without silently changing headers.

Excel uses the Calamine-first helper when available, otherwise pandas. CSV is
experimental and retries decoding with GBK after a UnicodeDecodeError.

Default reads preserve text and reject blank or duplicate headers. Explicit
header/name/column/type options are passed through to pandas instead.
"""

import pandas as pd
import logging
from pathlib import Path

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
    - Excel: .xlsx, .xls（.xls 需要 Calamine 等支持该格式的引擎）
    - CSV: .csv (🧪 实验性支持，测试阶段)

    特性：
    - 自动根据文件扩展名选择读取方式
    - Excel格式完全支持，保持原有稳定性
    - CSV自动处理编码问题（UTF-8/GBK）
    - 统一的错误处理
    - 易于扩展新格式

    向后兼容性保证：
    - 默认读取保留原始表头与文本；显式自定义读取参数传递给 pandas
    - 空白或重复表头会明确拒绝，不依赖 pandas 自动改名
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

        if file_ext not in self.SUPPORTED_FORMATS:
            supported = ", ".join(self.SUPPORTED_FORMATS.keys())
            raise ValueError(f"不支持的文件格式: {file_ext}\n支持的格式: {supported}")
        reader = self._read_csv if file_ext == ".csv" else self._read_excel
        preserve_headers = not any(
            key in kwargs
            for key in (
                "header",
                "names",
                "usecols",
                "dtype",
                "converters",
                "index_col",
            )
        )
        if not preserve_headers:
            return reader(file_path, **kwargs)
        options = dict(kwargs)
        options.update(header=None, dtype=object)
        options.setdefault("keep_default_na", False)
        if options.get("nrows") is not None:
            options["nrows"] += 1  # The raw read includes the header row.
        raw = reader(file_path, **options)
        if raw.empty:
            return pd.DataFrame()
        header = list(raw.iloc[0])
        frame = raw.iloc[1:].copy().reset_index(drop=True)
        frame.columns = header
        # Validate before any target reads; pandas must not silently mangle duplicates.
        from .snapshot import SourceTable

        return SourceTable.from_dataframe(frame).to_dataframe()

    def _read_excel(self, file_path: Path, **kwargs) -> pd.DataFrame:
        """
        读取Excel文件

        优先使用 smart_read_excel（Calamine引擎，性能取决于文件和运行环境）
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
