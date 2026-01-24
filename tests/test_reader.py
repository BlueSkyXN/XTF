#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件读取模块测试

模块概述：
    此模块测试 core/reader.py 中的文件读取功能，包括支持的格式、
    Excel 读取、CSV 读取以及各种边界情况。

测试覆盖：
    初始化测试（TestDataFileReaderInit）：
        - 读取器初始化
    
    支持格式测试（TestSupportedFormats）：
        - xlsx 格式支持
        - xls 格式支持
        - csv 格式支持
        - 获取支持格式字符串
    
    格式支持检查测试（TestIsSupported）：
        - xlsx 支持
        - xls 支持
        - csv 支持
        - 不支持的格式（txt, json, xml）
        - 大小写不敏感
    
    Excel 文件读取测试（TestReadExcel）：
        - 读取 xlsx 文件
        - 文件不存在异常
        - 带额外参数读取
    
    CSV 文件读取测试（TestReadCsv）：
        - 读取 CSV 文件
        - UTF-8 编码
        - GBK 编码
        - 带额外参数读取
    
    不支持格式读取测试（TestReadUnsupportedFormat）：
        - 读取 txt 格式
        - 读取 json 格式
    
    文件格式自动检测测试（TestReadFileAutoDetection）：
        - 自动检测 xlsx
        - 自动检测 csv
    
    边界情况测试（TestEdgeCases）：
        - 空 Excel 文件
        - 空 CSV 文件
        - 特殊字符
        - 大文件
        - 不同分隔符的 CSV

测试策略：
    - 使用 pytest 的 tmp_path fixture 创建临时文件
    - 验证返回的 DataFrame 内容
    - 测试异常情况和错误消息

依赖关系：
    测试目标：
        - core.reader.DataFileReader
    测试工具：
        - pytest
        - pandas

作者: XTF Team
版本: 1.7.3+
"""

import pytest
import pandas as pd
from pathlib import Path

from core.reader import DataFileReader


class TestDataFileReaderInit:
    """文件读取器初始化测试"""

    def test_init(self):
        """测试初始化"""
        reader = DataFileReader()
        assert reader.logger is not None


class TestSupportedFormats:
    """支持格式测试"""

    def test_supported_formats_contains_xlsx(self):
        """测试支持 xlsx 格式"""
        assert ".xlsx" in DataFileReader.SUPPORTED_FORMATS

    def test_supported_formats_contains_xls(self):
        """测试支持 xls 格式"""
        assert ".xls" in DataFileReader.SUPPORTED_FORMATS

    def test_supported_formats_contains_csv(self):
        """测试支持 csv 格式"""
        assert ".csv" in DataFileReader.SUPPORTED_FORMATS

    def test_get_supported_formats(self):
        """测试获取支持格式字符串"""
        formats = DataFileReader.get_supported_formats()
        assert ".xlsx" in formats
        assert ".xls" in formats
        assert ".csv" in formats


class TestIsSupported:
    """格式支持检查测试"""

    def test_is_supported_xlsx(self):
        """测试 xlsx 格式支持"""
        assert DataFileReader.is_supported(Path("test.xlsx")) is True

    def test_is_supported_xls(self):
        """测试 xls 格式支持"""
        assert DataFileReader.is_supported(Path("test.xls")) is True

    def test_is_supported_csv(self):
        """测试 csv 格式支持"""
        assert DataFileReader.is_supported(Path("test.csv")) is True

    def test_is_supported_unsupported(self):
        """测试不支持的格式"""
        assert DataFileReader.is_supported(Path("test.txt")) is False
        assert DataFileReader.is_supported(Path("test.json")) is False
        assert DataFileReader.is_supported(Path("test.xml")) is False

    def test_is_supported_case_insensitive(self):
        """测试格式检查大小写不敏感"""
        assert DataFileReader.is_supported(Path("test.XLSX")) is True
        assert DataFileReader.is_supported(Path("test.Csv")) is True


class TestReadExcel:
    """Excel 文件读取测试"""

    def test_read_xlsx_file(self, temp_excel_file):
        """测试读取 xlsx 文件"""
        reader = DataFileReader()
        df = reader.read_file(temp_excel_file)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "ID" in df.columns
        assert "Name" in df.columns

    def test_read_excel_file_not_found(self, tmp_path):
        """测试读取不存在的文件"""
        reader = DataFileReader()
        nonexistent_file = tmp_path / "nonexistent.xlsx"

        with pytest.raises(FileNotFoundError, match="文件不存在"):
            reader.read_file(nonexistent_file)

    def test_read_excel_with_kwargs(self, temp_excel_file):
        """测试带额外参数的读取"""
        reader = DataFileReader()
        # 使用 nrows 参数限制读取行数
        df = reader.read_file(temp_excel_file, nrows=2)

        assert len(df) == 2


class TestReadCsv:
    """CSV 文件读取测试"""

    def test_read_csv_file(self, temp_csv_file):
        """测试读取 CSV 文件"""
        reader = DataFileReader()
        df = reader.read_file(temp_csv_file)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "ID" in df.columns
        assert "Name" in df.columns

    def test_read_csv_utf8(self, tmp_path, sample_dataframe):
        """测试读取 UTF-8 编码的 CSV"""
        csv_file = tmp_path / "utf8.csv"
        sample_dataframe.to_csv(csv_file, index=False, encoding="utf-8")

        reader = DataFileReader()
        df = reader.read_file(csv_file)

        assert len(df) == 5

    def test_read_csv_gbk(self, tmp_path):
        """测试读取 GBK 编码的 CSV"""
        csv_file = tmp_path / "gbk.csv"
        df = pd.DataFrame(
            {"ID": [1, 2], "姓名": ["张三", "李四"], "城市": ["北京", "上海"]}
        )
        df.to_csv(csv_file, index=False, encoding="gbk")

        reader = DataFileReader()
        result = reader.read_file(csv_file)

        assert len(result) == 2
        assert "姓名" in result.columns

    def test_read_csv_with_kwargs(self, temp_csv_file):
        """测试带额外参数的 CSV 读取"""
        reader = DataFileReader()
        df = reader.read_file(temp_csv_file, nrows=3)

        assert len(df) == 3


class TestReadUnsupportedFormat:
    """不支持格式读取测试"""

    def test_read_unsupported_format(self, tmp_path):
        """测试读取不支持的格式"""
        txt_file = tmp_path / "test.txt"
        txt_file.write_text("some content")

        reader = DataFileReader()
        with pytest.raises(ValueError, match="不支持的文件格式"):
            reader.read_file(txt_file)

    def test_read_json_format(self, tmp_path):
        """测试读取 JSON 格式（不支持）"""
        json_file = tmp_path / "test.json"
        json_file.write_text('{"key": "value"}')

        reader = DataFileReader()
        with pytest.raises(ValueError, match="不支持的文件格式"):
            reader.read_file(json_file)


class TestReadFileAutoDetection:
    """文件格式自动检测测试"""

    def test_auto_detect_xlsx(self, temp_excel_file):
        """测试自动检测 xlsx 格式"""
        reader = DataFileReader()
        df = reader.read_file(temp_excel_file)

        assert isinstance(df, pd.DataFrame)

    def test_auto_detect_csv(self, temp_csv_file):
        """测试自动检测 csv 格式"""
        reader = DataFileReader()
        df = reader.read_file(temp_csv_file)

        assert isinstance(df, pd.DataFrame)


class TestEdgeCases:
    """边界情况测试"""

    def test_read_empty_excel(self, tmp_path):
        """测试读取空 Excel 文件"""
        empty_file = tmp_path / "empty.xlsx"
        pd.DataFrame().to_excel(empty_file, index=False)

        reader = DataFileReader()
        df = reader.read_file(empty_file)

        assert len(df) == 0

    def test_read_empty_csv(self, tmp_path):
        """测试读取空 CSV 文件"""
        empty_file = tmp_path / "empty.csv"
        # 创建一个带表头但无数据的 CSV
        empty_file.write_text("col1,col2,col3\n")

        reader = DataFileReader()
        df = reader.read_file(empty_file)

        assert len(df) == 0
        assert len(df.columns) == 3

    def test_read_excel_with_special_characters(self, tmp_path):
        """测试读取包含特殊字符的 Excel"""
        special_file = tmp_path / "special.xlsx"
        df = pd.DataFrame(
            {
                "列名": ["值1", "值2"],
                "Column": ['Value with "quotes"', "Value with 'single quotes'"],
                "特殊": ["@#$%^&*()", "你好世界"],
            }
        )
        df.to_excel(special_file, index=False)

        reader = DataFileReader()
        result = reader.read_file(special_file)

        assert len(result) == 2
        assert "列名" in result.columns

    def test_read_large_excel(self, tmp_path):
        """测试读取较大的 Excel 文件"""
        large_file = tmp_path / "large.xlsx"
        df = pd.DataFrame(
            {
                "ID": range(1000),
                "Name": [f"Name_{i}" for i in range(1000)],
                "Value": [i * 1.5 for i in range(1000)],
            }
        )
        df.to_excel(large_file, index=False)

        reader = DataFileReader()
        result = reader.read_file(large_file)

        assert len(result) == 1000

    def test_read_csv_with_different_separators(self, tmp_path):
        """测试读取不同分隔符的 CSV"""
        # 制表符分隔
        tsv_file = tmp_path / "data.csv"
        tsv_file.write_text("ID\tName\n1\tAlice\n2\tBob")

        reader = DataFileReader()
        # 需要指定分隔符
        df = reader.read_file(tsv_file, sep="\t")

        assert len(df) == 2
        assert "ID" in df.columns
