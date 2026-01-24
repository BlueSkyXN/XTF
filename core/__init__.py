#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF 核心模块包

模块概述：
    此包包含 XTF 工具的核心业务逻辑，包括配置管理、数据转换、
    同步引擎、文件读取和流程控制等功能模块。

包结构：
    core/
    ├── __init__.py     - 包初始化，导出公共接口
    ├── config.py       - 配置管理（SyncConfig, ConfigManager）
    ├── converter.py    - 数据转换（DataConverter）
    ├── engine.py       - 同步引擎（XTFSyncEngine）
    ├── reader.py       - 文件读取（DataFileReader）
    └── control.py      - 重试与频控策略

导出的类和函数：
    配置相关：
        - SyncConfig: 统一同步配置数据类
        - SyncMode: 同步模式枚举（full/incremental/overwrite/clone）
        - TargetType: 目标类型枚举（bitable/sheet）
        - ConfigManager: 配置管理器
        - create_sample_config: 创建示例配置文件
        - get_target_description: 获取目标类型描述

    数据处理：
        - DataConverter: 数据转换器
        - XTFSyncEngine: 统一同步引擎
        - DataFileReader: 数据文件读取器

使用示例：
    >>> from core import SyncConfig, XTFSyncEngine, TargetType
    >>> config = SyncConfig(
    ...     file_path="data.xlsx",
    ...     app_id="xxx",
    ...     app_secret="xxx",
    ...     target_type=TargetType.BITABLE,
    ...     app_token="xxx",
    ...     table_id="xxx"
    ... )
    >>> engine = XTFSyncEngine(config)
    >>> engine.sync(dataframe)

设计原则：
    - 单一职责：每个模块专注于特定功能领域
    - 高内聚低耦合：模块间通过清晰接口交互
    - 可扩展性：支持新的目标类型和同步模式

作者: XTF Team
版本: 1.7.3+
"""

from .config import (
    SyncConfig,
    SyncMode,
    TargetType,
    ConfigManager,
    create_sample_config,
    get_target_description,
)
from .converter import DataConverter
from .engine import XTFSyncEngine
from .reader import DataFileReader

__all__ = [
    "SyncConfig",
    "SyncMode",
    "TargetType",
    "ConfigManager",
    "create_sample_config",
    "get_target_description",
    "DataConverter",
    "XTFSyncEngine",
    "DataFileReader",
]
