#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF 测试配置文件

模块概述：
    此模块提供 pytest 测试框架的配置和共享 fixtures。Fixtures 是
    pytest 的核心功能，用于提供测试所需的数据和对象，实现测试
    代码的复用和隔离。

主要功能：
    1. 定义共享的测试 fixtures
    2. 提供测试数据生成器
    3. 管理临时文件和清理
    4. 配置对象的工厂方法

Fixtures 分类：
    配置 Fixtures：
        - sample_bitable_config: 多维表格测试配置
        - sample_sheet_config: 电子表格测试配置
        - sample_selective_sync_config: 选择性同步配置
        - sample_config_dict: 配置字典
    
    数据 Fixtures：
        - sample_dataframe: 基础测试 DataFrame
        - sample_dataframe_with_types: 多类型测试 DataFrame
        - sample_records: 飞书记录格式数据
    
    文件 Fixtures：
        - temp_excel_file: 临时 Excel 文件
        - temp_csv_file: 临时 CSV 文件
        - temp_config_file: 临时配置文件

使用示例：
    # 在测试中使用 fixture
    def test_something(sample_bitable_config):
        config = sample_bitable_config
        assert config.target_type == TargetType.BITABLE
    
    # 使用多个 fixtures
    def test_sync(sample_sheet_config, sample_dataframe):
        config = sample_sheet_config
        df = sample_dataframe
        # 执行测试...

Fixture 作用域：
    - function（默认）：每个测试函数创建新实例
    - class：每个测试类创建一个实例
    - module：每个模块创建一个实例
    - session：整个测试会话创建一个实例

临时文件处理：
    使用 pytest 的 tmp_path fixture 创建临时目录，
    测试结束后自动清理。

依赖关系：
    内部模块：
        - core.config: 配置类
    外部依赖：
        - pytest: 测试框架
        - pandas: 数据处理
        - yaml: 配置文件

作者: XTF Team
版本: 1.7.3+
"""

from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest

from core.config import (
    SyncConfig,
    SelectiveSyncConfig,
    TargetType,
    SyncMode,
    FieldTypeStrategy,
)


@pytest.fixture
def sample_bitable_config() -> SyncConfig:
    """返回用于多维表格测试的配置对象"""
    return SyncConfig(
        file_path="test_data.xlsx",
        app_id="cli_test_app_id",
        app_secret="test_app_secret",
        target_type=TargetType.BITABLE,
        app_token="test_app_token",
        table_id="test_table_id",
        sync_mode=SyncMode.FULL,
        index_column="ID",
        batch_size=500,
        rate_limit_delay=0.5,
        max_retries=3,
        create_missing_fields=True,
        field_type_strategy=FieldTypeStrategy.BASE,
        log_level="INFO",
    )


@pytest.fixture
def sample_sheet_config() -> SyncConfig:
    """返回用于电子表格测试的配置对象"""
    return SyncConfig(
        file_path="test_data.xlsx",
        app_id="cli_test_app_id",
        app_secret="test_app_secret",
        target_type=TargetType.SHEET,
        spreadsheet_token="test_spreadsheet_token",
        sheet_id="test_sheet_id",
        sync_mode=SyncMode.FULL,
        index_column="ID",
        start_row=1,
        start_column="A",
        batch_size=1000,
        rate_limit_delay=0.1,
        max_retries=3,
        log_level="INFO",
    )


@pytest.fixture
def sample_selective_sync_config() -> SelectiveSyncConfig:
    """返回选择性同步配置"""
    return SelectiveSyncConfig(
        enabled=True,
        columns=["Name", "Age", "City"],
        auto_include_index=True,
        optimize_ranges=True,
        max_gap_for_merge=2,
        preserve_column_order=True,
    )


@pytest.fixture
def sample_dataframe() -> pd.DataFrame:
    """返回测试用的 DataFrame"""
    return pd.DataFrame(
        {
            "ID": [1, 2, 3, 4, 5],
            "Name": ["Alice", "Bob", "Charlie", "David", "Eve"],
            "Age": [25, 30, 35, 40, 45],
            "City": ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou"],
            "Date": [
                "2024-01-01",
                "2024-02-01",
                "2024-03-01",
                "2024-04-01",
                "2024-05-01",
            ],
            "Score": [85.5, 90.0, 78.5, 88.0, 92.5],
            "Active": ["是", "否", "是", "是", "否"],
        }
    )


@pytest.fixture
def sample_dataframe_with_types() -> pd.DataFrame:
    """返回包含多种数据类型的测试 DataFrame"""
    return pd.DataFrame(
        {
            "TextCol": ["Hello", "World", "Test", None, ""],
            "NumberCol": [1, 2.5, 3, None, 0],
            "DateCol": ["2024-01-01", "2024/02/01", "2024年3月1日", None, ""],
            "BoolCol": ["是", "否", "true", "false", None],
            "ChoiceCol": ["A", "B", "A", "C", "B"],
            "MultiChoiceCol": ["A,B", "B,C", "A", "A,B,C", None],
            "TimestampCol": ["1704067200", "1706745600", None, "", "1709337600"],
        }
    )


@pytest.fixture
def sample_config_dict() -> Dict[str, Any]:
    """返回测试用的配置字典"""
    return {
        "file_path": "test_data.xlsx",
        "app_id": "cli_test_app_id",
        "app_secret": "test_app_secret",
        "target_type": "bitable",
        "app_token": "test_app_token",
        "table_id": "test_table_id",
        "sync_mode": "full",
        "index_column": "ID",
        "batch_size": 500,
        "rate_limit_delay": 0.5,
        "max_retries": 3,
        "create_missing_fields": True,
        "field_type_strategy": "base",
        "log_level": "INFO",
    }


@pytest.fixture
def sample_records() -> list:
    """返回测试用的飞书记录列表"""
    return [
        {
            "record_id": "rec001",
            "fields": {
                "ID": [{"text": "1", "type": "text"}],
                "Name": [{"text": "Alice", "type": "text"}],
                "Age": 25,
            },
        },
        {
            "record_id": "rec002",
            "fields": {
                "ID": [{"text": "2", "type": "text"}],
                "Name": [{"text": "Bob", "type": "text"}],
                "Age": 30,
            },
        },
        {
            "record_id": "rec003",
            "fields": {
                "ID": [{"text": "3", "type": "text"}],
                "Name": [{"text": "Charlie", "type": "text"}],
                "Age": 35,
            },
        },
    ]


@pytest.fixture
def temp_excel_file(tmp_path, sample_dataframe) -> Path:
    """创建临时 Excel 文件用于测试"""
    file_path = tmp_path / "test_data.xlsx"
    sample_dataframe.to_excel(file_path, index=False)
    return file_path


@pytest.fixture
def temp_csv_file(tmp_path, sample_dataframe) -> Path:
    """创建临时 CSV 文件用于测试"""
    file_path = tmp_path / "test_data.csv"
    sample_dataframe.to_csv(file_path, index=False, encoding="utf-8")
    return file_path


@pytest.fixture
def temp_config_file(tmp_path, sample_config_dict) -> Path:
    """创建临时配置文件用于测试"""
    import yaml

    file_path = tmp_path / "test_config.yaml"
    with open(file_path, "w", encoding="utf-8") as f:
        yaml.dump(sample_config_dict, f, allow_unicode=True)
    return file_path
