#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF 2.0 运行时配置模型

模块概述：
    此模块只定义稳定的内部枚举。不可变配置图位于 core.runtime_config；YAML v2
    加载、CLI 参数解析、precedence、模板生成和 source tracking 由 xtf_cli.config 负责。

主要功能：
    1. 同步、匹配、source、target 和字段策略枚举

核心类：
    枚举类型：
        - FieldTypeStrategy: 字段类型选择策略（raw/base/auto/intelligence）
        - SyncMode: 同步模式（full/incremental/overwrite/clone）
        - TargetType: 目标类型（bitable/sheet）
        - SourceType: 数据源类型（file/bitable）

配置验证与嵌套结构见 `core.runtime_config.RuntimeConfig`。

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

from enum import Enum


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


class MatchStrategy(Enum):
    """非 clone 模式的记录匹配策略。"""

    BY_KEY = "by_key"
    APPEND_ONLY = "append_only"


class TargetType(Enum):
    """目标类型枚举"""

    BITABLE = "bitable"  # 多维表格
    SHEET = "sheet"  # 电子表格


class SourceType(Enum):
    """同步数据源类型。"""

    FILE = "file"
    BITABLE = "bitable"
