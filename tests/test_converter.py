#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据转换模块测试
测试 core/converter.py 中的数据转换功能
"""

import sys
import pytest
import pandas as pd
import hashlib
from pathlib import Path

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.config import TargetType, FieldTypeStrategy
from core.converter import DataConverter


class TestDataConverterInit:
    """数据转换器初始化测试"""

    def test_init_bitable(self):
        """测试多维表格模式初始化"""
        converter = DataConverter(TargetType.BITABLE)
        assert converter.target_type == TargetType.BITABLE
        assert converter.conversion_stats['success'] == 0
        assert converter.conversion_stats['failed'] == 0

    def test_init_sheet(self):
        """测试电子表格模式初始化"""
        converter = DataConverter(TargetType.SHEET)
        assert converter.target_type == TargetType.SHEET

    def test_reset_stats(self):
        """测试重置统计"""
        converter = DataConverter(TargetType.BITABLE)
        converter.conversion_stats['success'] = 100
        converter.conversion_stats['failed'] = 10
        converter.reset_stats()
        assert converter.conversion_stats['success'] == 0
        assert converter.conversion_stats['failed'] == 0


class TestIndexValueHash:
    """索引值哈希测试"""

    def test_get_index_value_hash(self):
        """测试索引值哈希计算"""
        converter = DataConverter(TargetType.BITABLE)
        row = pd.Series({'ID': '123', 'Name': 'Test'})
        hash_value = converter.get_index_value_hash(row, 'ID')

        expected_hash = hashlib.md5('123'.encode('utf-8')).hexdigest()
        assert hash_value == expected_hash

    def test_get_index_value_hash_no_index(self):
        """测试无索引列时返回 None"""
        converter = DataConverter(TargetType.BITABLE)
        row = pd.Series({'ID': '123', 'Name': 'Test'})
        hash_value = converter.get_index_value_hash(row, None)
        assert hash_value is None

    def test_get_index_value_hash_missing_column(self):
        """测试索引列不存在时返回 None"""
        converter = DataConverter(TargetType.BITABLE)
        row = pd.Series({'Name': 'Test'})
        hash_value = converter.get_index_value_hash(row, 'ID')
        assert hash_value is None


class TestBuildRecordIndex:
    """记录索引构建测试"""

    def test_build_record_index(self, sample_records):
        """测试构建记录索引"""
        converter = DataConverter(TargetType.BITABLE)
        index = converter.build_record_index(sample_records, 'ID')

        # 应该有3条记录
        assert len(index) == 3

        # 检查哈希键对应正确的记录
        hash_1 = hashlib.md5('1'.encode('utf-8')).hexdigest()
        assert hash_1 in index
        assert index[hash_1]['record_id'] == 'rec001'

    def test_build_record_index_no_index_column(self, sample_records):
        """测试无索引列时返回空索引"""
        converter = DataConverter(TargetType.BITABLE)
        index = converter.build_record_index(sample_records, None)
        assert len(index) == 0

    def test_build_record_index_rich_text_format(self):
        """测试富文本格式的索引值"""
        converter = DataConverter(TargetType.BITABLE)
        records = [
            {
                'record_id': 'rec001',
                'fields': {
                    'ID': [{'text': 'test_value', 'type': 'text'}]
                }
            }
        ]
        index = converter.build_record_index(records, 'ID')

        hash_key = hashlib.md5('test_value'.encode('utf-8')).hexdigest()
        assert hash_key in index


class TestTypeDetection:
    """类型检测测试"""

    def test_is_number_string(self):
        """测试数字字符串检测"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._is_number_string("123") is True
        assert converter._is_number_string("123.45") is True
        assert converter._is_number_string("1,234.56") is True
        assert converter._is_number_string("-123") is True
        assert converter._is_number_string("abc") is False
        assert converter._is_number_string("12abc") is False

    def test_is_date_string(self):
        """测试日期字符串检测"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._is_date_string("2024-01-01") is True
        assert converter._is_date_string("2024/01/01") is True
        assert converter._is_date_string("2024年1月1日") is True
        assert converter._is_date_string("not a date") is False
        assert converter._is_date_string("") is False

    def test_is_date_string_enhanced(self):
        """测试增强的日期检测"""
        converter = DataConverter(TargetType.BITABLE)

        is_date, confidence, fmt = converter._is_date_string_enhanced("2024-01-01")
        assert is_date is True
        assert confidence > 0.8

        is_date, confidence, fmt = converter._is_date_string_enhanced("2024-01-01 12:30:45")
        assert is_date is True
        assert confidence > 0.9

        is_date, confidence, fmt = converter._is_date_string_enhanced("not a date")
        assert is_date is False
        assert confidence == 0.0

    def test_is_timestamp_enhanced(self):
        """测试时间戳检测"""
        converter = DataConverter(TargetType.BITABLE)

        # 秒级时间戳 (2024-01-01)
        is_ts, confidence = converter._is_timestamp_enhanced("1704067200")
        assert is_ts is True
        assert confidence > 0.5

        # 毫秒级时间戳
        is_ts, confidence = converter._is_timestamp_enhanced("1704067200000")
        assert is_ts is True

        # 非时间戳
        is_ts, confidence = converter._is_timestamp_enhanced("abc")
        assert is_ts is False


class TestAnalyzeExcelColumnData:
    """Excel 列数据分析测试"""

    def test_analyze_number_column(self, sample_dataframe):
        """测试数字列分析"""
        converter = DataConverter(TargetType.BITABLE)
        analysis = converter.analyze_excel_column_data(sample_dataframe, 'Age')

        assert analysis['primary_type'] == 'number'
        assert analysis['confidence'] == 1.0

    def test_analyze_string_column(self, sample_dataframe):
        """测试字符串列分析"""
        converter = DataConverter(TargetType.BITABLE)
        analysis = converter.analyze_excel_column_data(sample_dataframe, 'Name')

        assert analysis['primary_type'] == 'string'

    def test_analyze_empty_column(self):
        """测试空列分析"""
        converter = DataConverter(TargetType.BITABLE)
        df = pd.DataFrame({'Empty': [None, None, None]})
        analysis = converter.analyze_excel_column_data(df, 'Empty')

        assert analysis['primary_type'] == 'string'
        assert analysis['confidence'] == 0.5


class TestFieldTypeStrategies:
    """字段类型策略测试"""

    def test_raw_strategy(self):
        """测试 RAW 策略 - 所有字段使用文本类型"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_raw()

        assert field_type == 1  # 文本类型
        assert "raw" in reason.lower()

    def test_base_strategy_number(self):
        """测试 BASE 策略 - 数字类型"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_base(
            'number', {'1', '2', '3'}, 100, 0.95
        )

        assert field_type == 2  # 数字类型

    def test_base_strategy_date(self):
        """测试 BASE 策略 - 日期类型"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_base(
            'datetime', {'2024-01-01'}, 100, 0.9
        )

        assert field_type == 5  # 日期类型

    def test_base_strategy_low_confidence(self):
        """测试 BASE 策略 - 低置信度回退到文本"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_base(
            'number', {'1', '2', 'abc'}, 100, 0.5
        )

        assert field_type == 1  # 文本类型

    def test_auto_strategy_with_validation(self):
        """测试 AUTO 策略 - 检测到 Excel 验证"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_auto(
            'string', {'A', 'B', 'C'}, 100, 0.9, has_excel_validation=True
        )

        assert field_type == 3  # 单选类型

    def test_auto_strategy_without_validation(self):
        """测试 AUTO 策略 - 未检测到 Excel 验证"""
        converter = DataConverter(TargetType.BITABLE)
        field_type, reason = converter._suggest_feishu_field_type_auto(
            'string', {'A', 'B', 'C'}, 100, 0.9, has_excel_validation=False
        )

        assert field_type == 1  # 文本类型


class TestForceConversion:
    """强制转换测试"""

    def test_force_to_number(self):
        """测试强制转换为数字"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._force_to_number(123, 'test') == 123
        assert converter._force_to_number("456", 'test') == 456
        assert converter._force_to_number("123.45", 'test') == 123.45
        assert converter._force_to_number("1,234", 'test') == 1234
        assert converter._force_to_number("$100", 'test') == 100
        assert converter._force_to_number("", 'test') is None
        assert converter._force_to_number("n/a", 'test') is None

    def test_force_to_boolean(self):
        """测试强制转换为布尔值"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._force_to_boolean(True, 'test') is True
        assert converter._force_to_boolean(False, 'test') is False
        assert converter._force_to_boolean("是", 'test') is True
        assert converter._force_to_boolean("否", 'test') is False
        assert converter._force_to_boolean("true", 'test') is True
        assert converter._force_to_boolean("false", 'test') is False
        assert converter._force_to_boolean("yes", 'test') is True
        assert converter._force_to_boolean("no", 'test') is False
        assert converter._force_to_boolean(1, 'test') is True
        assert converter._force_to_boolean(0, 'test') is False

    def test_force_to_single_choice(self):
        """测试强制转换为单选值"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._force_to_single_choice("A", 'test') == "A"
        assert converter._force_to_single_choice("A,B,C", 'test') == "A"
        assert converter._force_to_single_choice("A;B;C", 'test') == "A"
        assert converter._force_to_single_choice("  Value  ", 'test') == "Value"

    def test_force_to_multi_choice(self):
        """测试强制转换为多选值"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter._force_to_multi_choice("A", 'test') == ["A"]
        assert converter._force_to_multi_choice("A,B,C", 'test') == ["A", "B", "C"]
        assert converter._force_to_multi_choice("A;B;C", 'test') == ["A", "B", "C"]
        assert converter._force_to_multi_choice(["A", "B"], 'test') == ["A", "B"]

    def test_force_to_timestamp(self):
        """测试强制转换为时间戳"""
        converter = DataConverter(TargetType.BITABLE)

        # 毫秒级时间戳直接返回
        result = converter._force_to_timestamp(1704067200000, 'test')
        assert result == 1704067200000

        # 秒级时间戳转为毫秒级
        result = converter._force_to_timestamp(1704067200, 'test')
        assert result == 1704067200000

        # 日期字符串
        result = converter._force_to_timestamp("2024-01-01", 'test')
        assert result is not None
        assert result > 0

        # 无效值
        assert converter._force_to_timestamp("n/a", 'test') is None


class TestConvertFieldValueSafe:
    """安全字段值转换测试"""

    def test_convert_null_value(self):
        """测试 null 值转换"""
        converter = DataConverter(TargetType.BITABLE)
        assert converter.convert_field_value_safe('test', None) is None
        assert converter.convert_field_value_safe('test', pd.NA) is None

    def test_convert_with_field_types(self):
        """测试带字段类型的转换"""
        converter = DataConverter(TargetType.BITABLE)
        field_types = {'Age': 2}  # 数字类型

        result = converter.convert_field_value_safe('Age', "25", field_types)
        assert result == 25

    def test_convert_without_field_types(self):
        """测试不带字段类型的智能转换"""
        converter = DataConverter(TargetType.BITABLE)

        result = converter.convert_field_value_safe('test', "123", None)
        assert result == 123  # 智能识别为数字


class TestSimpleConvertValue:
    """简单值转换测试（电子表格模式）"""

    def test_simple_convert_number(self):
        """测试数字转换"""
        converter = DataConverter(TargetType.SHEET)

        assert converter.simple_convert_value(123) == 123
        assert converter.simple_convert_value(123.45) == 123.45

    def test_simple_convert_string(self):
        """测试字符串转换"""
        converter = DataConverter(TargetType.SHEET)

        assert converter.simple_convert_value("Hello") == "Hello"

    def test_simple_convert_null(self):
        """测试 null 值转换"""
        converter = DataConverter(TargetType.SHEET)

        assert converter.simple_convert_value(None) == ""
        assert converter.simple_convert_value(pd.NA) == ""


class TestColumnConversion:
    """列转换测试"""

    def test_column_number_to_letter(self):
        """测试列号转字母"""
        converter = DataConverter(TargetType.SHEET)

        assert converter.column_number_to_letter(1) == "A"
        assert converter.column_number_to_letter(26) == "Z"
        assert converter.column_number_to_letter(27) == "AA"
        assert converter.column_number_to_letter(52) == "AZ"
        assert converter.column_number_to_letter(53) == "BA"

    def test_column_letter_to_number(self):
        """测试列字母转数字"""
        converter = DataConverter(TargetType.SHEET)

        assert converter.column_letter_to_number("A") == 1
        assert converter.column_letter_to_number("Z") == 26
        assert converter.column_letter_to_number("AA") == 27
        assert converter.column_letter_to_number("AZ") == 52
        assert converter.column_letter_to_number("BA") == 53


class TestDfToValues:
    """DataFrame 转值列表测试"""

    def test_df_to_values_with_headers(self, sample_dataframe):
        """测试带表头的转换"""
        converter = DataConverter(TargetType.SHEET)
        values = converter.df_to_values(sample_dataframe, include_headers=True)

        assert len(values) == 6  # 1 header + 5 data rows
        assert values[0] == list(sample_dataframe.columns)

    def test_df_to_values_without_headers(self, sample_dataframe):
        """测试不带表头的转换"""
        converter = DataConverter(TargetType.SHEET)
        values = converter.df_to_values(sample_dataframe, include_headers=False)

        assert len(values) == 5  # 5 data rows only

    def test_df_to_values_selected_columns(self, sample_dataframe):
        """测试选择特定列"""
        converter = DataConverter(TargetType.SHEET)
        values = converter.df_to_values(
            sample_dataframe,
            include_headers=True,
            selected_columns=['ID', 'Name']
        )

        assert len(values[0]) == 2
        assert values[0] == ['ID', 'Name']


class TestValuesToDf:
    """值列表转 DataFrame 测试"""

    def test_values_to_df(self):
        """测试值列表转 DataFrame"""
        converter = DataConverter(TargetType.SHEET)
        values = [
            ['ID', 'Name', 'Age'],
            [1, 'Alice', 25],
            [2, 'Bob', 30]
        ]
        df = converter.values_to_df(values)

        assert len(df) == 2
        assert list(df.columns) == ['ID', 'Name', 'Age']
        assert df.iloc[0]['Name'] == 'Alice'

    def test_values_to_df_empty(self):
        """测试空值列表"""
        converter = DataConverter(TargetType.SHEET)
        df = converter.values_to_df([])

        assert len(df) == 0

    def test_values_to_df_clean_empty_rows(self):
        """测试清理空行"""
        converter = DataConverter(TargetType.SHEET)
        values = [
            ['ID', 'Name'],
            [1, 'Alice'],
            [None, None],  # 空行
            ['', '']  # 空行
        ]
        df = converter.values_to_df(values)

        assert len(df) == 1


class TestGetRangeString:
    """范围字符串生成测试"""

    def test_get_range_string(self):
        """测试生成范围字符串"""
        converter = DataConverter(TargetType.SHEET)
        range_str = converter.get_range_string('sheet1', 1, 'A', 10, 'E')

        assert range_str == "sheet1!A1:E10"


class TestFieldTypeName:
    """字段类型名称测试"""

    def test_get_field_type_name(self):
        """测试获取字段类型中文名称"""
        converter = DataConverter(TargetType.BITABLE)

        assert converter.get_field_type_name(1) == "文本"
        assert converter.get_field_type_name(2) == "数字"
        assert converter.get_field_type_name(3) == "单选"
        assert converter.get_field_type_name(4) == "多选"
        assert converter.get_field_type_name(5) == "日期"
        assert converter.get_field_type_name(7) == "复选框"
        assert "未知" in converter.get_field_type_name(999)


class TestDfToRecords:
    """DataFrame 转记录测试"""

    def test_df_to_records_bitable(self, sample_dataframe):
        """测试多维表格模式的记录转换"""
        converter = DataConverter(TargetType.BITABLE)
        records = converter.df_to_records(sample_dataframe)

        assert len(records) == 5
        assert 'fields' in records[0]
        assert 'ID' in records[0]['fields']

    def test_df_to_records_sheet_raises_error(self, sample_dataframe):
        """测试电子表格模式调用 df_to_records 抛出错误"""
        converter = DataConverter(TargetType.SHEET)

        with pytest.raises(ValueError, match="只支持多维表格模式"):
            converter.df_to_records(sample_dataframe)
