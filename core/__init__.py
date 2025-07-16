"""
核心模块
提供统一的配置管理、数据转换、同步引擎和主入口功能
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
# main 已内嵌到 XTF.py，无需单独导出

__all__ = [
    "SyncConfig",
    "SyncMode",
    "TargetType",
    "ConfigManager",
    "create_sample_config",
    "get_target_description",
    "DataConverter",
    "XTFSyncEngine",
]

__all__ = [
    "SyncConfig",
    "SyncMode",
    "TargetType",
    "ConfigManager",
    "create_sample_config",
    "get_target_description",
    "DataConverter",
    "XTFSyncEngine",
    "main",
]