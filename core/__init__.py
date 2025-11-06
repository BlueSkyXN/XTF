"""
核心模块
提供统一的配置管理、数据转换、同步引擎、文件读取和主入口功能
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