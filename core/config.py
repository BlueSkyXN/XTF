#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置管理模块
提供同步配置和配置管理功能
"""

import yaml
import argparse
import sys
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Dict, Any


class SyncMode(Enum):
    """同步模式枚举"""
    FULL = "full"          # 全量同步：已存在的更新，不存在的新增
    INCREMENTAL = "incremental"  # 增量同步：只新增不存在的记录
    OVERWRITE = "overwrite"     # 覆盖同步：删除已存在的，然后新增全部
    CLONE = "clone"             # 克隆同步：清空全部，然后新增全部


@dataclass
class SyncConfig:
    """同步配置"""
    # 基础配置
    file_path: str
    app_id: str
    app_secret: str
    app_token: str
    table_id: str
    
    # 同步设置
    sync_mode: SyncMode = SyncMode.FULL
    index_column: Optional[str] = None  # 索引列名，用于记录比对
    
    # 性能设置
    batch_size: int = 500  # 批处理大小
    rate_limit_delay: float = 0.5  # 接口调用间隔
    max_retries: int = 3  # 最大重试次数
    
    # 字段管理
    create_missing_fields: bool = True
    
    # 日志设置
    log_level: str = "INFO"
    
    def __post_init__(self):
        if isinstance(self.sync_mode, str):
            self.sync_mode = SyncMode(self.sync_mode)


class ConfigManager:
    """配置管理器"""
    
    @staticmethod
    def load_from_file(config_file: str) -> Optional[Dict[str, Any]]:
        """从YAML文件加载配置"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
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
        with open(config_file, 'w', encoding='utf-8') as f:
            yaml.dump(config, f, default_flow_style=False, allow_unicode=True, indent=2)
    
    @staticmethod
    def parse_args() -> argparse.Namespace:
        """解析命令行参数"""
        parser = argparse.ArgumentParser(description='XTF - Excel To Feishu 同步工具')
        
        # 基础配置
        parser.add_argument('--config', '-c', type=str, default='config.yaml',
                          help='配置文件路径 (默认: config.yaml)')
        parser.add_argument('--file-path', type=str, help='Excel文件路径')
        parser.add_argument('--app-id', type=str, help='飞书应用ID')
        parser.add_argument('--app-secret', type=str, help='飞书应用密钥')
        parser.add_argument('--app-token', type=str, help='多维表格应用Token')
        parser.add_argument('--table-id', type=str, help='数据表ID')
        
        # 同步设置
        parser.add_argument('--sync-mode', type=str, 
                          choices=['full', 'incremental', 'overwrite', 'clone'],
                          help='同步模式')
        parser.add_argument('--index-column', type=str, help='索引列名')
        
        # 性能设置
        parser.add_argument('--batch-size', type=int, help='批处理大小')
        parser.add_argument('--rate-limit-delay', type=float, help='接口调用间隔秒数')
        parser.add_argument('--max-retries', type=int, help='最大重试次数')
        
        # 功能开关
        parser.add_argument('--no-create-fields', action='store_true',
                          help='不自动创建缺失字段')
        
        # 日志设置
        parser.add_argument('--log-level', type=str, 
                          choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                          help='日志级别')
        
        return parser.parse_args()
    
    @classmethod
    def create_config(cls) -> SyncConfig:
        """创建配置对象"""
        args = cls.parse_args()
        
        # 先设置默认值
        config_data = {
            'sync_mode': 'full',
            'batch_size': 500,
            'rate_limit_delay': 0.5,
            'max_retries': 3,
            'create_missing_fields': True,
            'log_level': 'INFO'
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
        
        # 命令行参数覆盖文件配置（只有当明确提供时）
        cli_overrides = []
        
        # 基础参数
        if args.file_path:
            config_data['file_path'] = args.file_path
            cli_overrides.append(f"file_path={args.file_path}")
        if args.app_id:
            config_data['app_id'] = args.app_id
            cli_overrides.append(f"app_id={args.app_id[:8]}...")
        if args.app_secret:
            config_data['app_secret'] = args.app_secret
            cli_overrides.append(f"app_secret=***")
        if args.app_token:
            config_data['app_token'] = args.app_token
            cli_overrides.append(f"app_token={args.app_token[:8]}...")
        if args.table_id:
            config_data['table_id'] = args.table_id
            cli_overrides.append(f"table_id={args.table_id}")
        if args.index_column:
            config_data['index_column'] = args.index_column
            cli_overrides.append(f"index_column={args.index_column}")
        
        # 高级参数（只有明确提供时才覆盖）
        if args.sync_mode is not None:
            config_data['sync_mode'] = args.sync_mode
            cli_overrides.append(f"sync_mode={args.sync_mode}")
        if args.batch_size is not None:
            config_data['batch_size'] = args.batch_size
            cli_overrides.append(f"batch_size={args.batch_size}")
        if args.rate_limit_delay is not None:
            config_data['rate_limit_delay'] = args.rate_limit_delay
            cli_overrides.append(f"rate_limit_delay={args.rate_limit_delay}")
        if args.max_retries is not None:
            config_data['max_retries'] = args.max_retries
            cli_overrides.append(f"max_retries={args.max_retries}")
        if args.no_create_fields:  # 这个是action='store_true'，只有指定时才为True
            config_data['create_missing_fields'] = False
            cli_overrides.append("create_missing_fields=False")
        if args.log_level is not None:
            config_data['log_level'] = args.log_level
            cli_overrides.append(f"log_level={args.log_level}")
        
        # 显示命令行覆盖的参数
        if cli_overrides:
            print(f"🔧 命令行参数覆盖: {', '.join(cli_overrides)}")
        
        # 验证必需参数
        required_fields = ['file_path', 'app_id', 'app_secret', 'app_token', 'table_id']
        missing_fields = [f for f in required_fields if not config_data.get(f)]
        
        if missing_fields:
            print(f"\n❌ 错误: 缺少必需参数: {', '.join(missing_fields)}")
            print("💡 请通过以下方式提供这些参数:")
            print("   1. 在配置文件中设置")
            print("   2. 通过命令行参数指定")
            print("\n命令行参数示例:")
            for field in missing_fields:
                field_name = field.replace('_', '-')
                print(f"   --{field_name} <值>")
            sys.exit(1)
        
        return SyncConfig(**config_data)


def create_sample_config(config_file: str = "config.yaml"):
    """创建示例配置文件"""
    sample_config = {
        "file_path": "data.xlsx",
        "app_id": "cli_your_app_id",
        "app_secret": "your_app_secret",
        "app_token": "your_app_token",
        "table_id": "your_table_id",
        "sync_mode": "full",
        "index_column": "ID",
        "batch_size": 500,
        "rate_limit_delay": 0.5,
        "max_retries": 3,
        "create_missing_fields": True,
        "log_level": "INFO"
    }
    
    if not Path(config_file).exists():
        ConfigManager.save_to_file(sample_config, config_file)
        print(f"已创建示例配置文件: {config_file}")
        print("请编辑配置文件并填入正确的参数值")
        return True
    else:
        print(f"配置文件 {config_file} 已存在")
        return False