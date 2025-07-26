#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF (Excel To Feishu) - 统一入口
支持多维表格和电子表格同步
"""

import pandas as pd
import time
import logging
from pathlib import Path

# 导入核心模块
from core.config import (
    SyncConfig,
    ConfigManager,
    TargetType,
    create_sample_config,
    get_target_description,
)
from core.engine import XTFSyncEngine


def setup_logger():
    """设置基础日志器"""
    logger = logging.getLogger()
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def main():
    """主函数"""
    logger = setup_logger()
    
    print("=" * 70)
    print("     XTF工具 (模块化统一版本)")
    print("     支持多维表格和电子表格同步")
    print("     支持四种同步模式：全量、增量、覆盖、克隆")
    print("=" * 70)
    
    try:
        # 解析目标类型
        target_type = ConfigManager.parse_target_type()
        print(f"\n🎯 目标类型: {target_type.value}")
        print(f"📝 描述: {get_target_description(target_type)}")
        
        # 获取配置文件路径
        import argparse
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('--config', '-c', type=str, default='config.yaml')
        args, _ = parser.parse_known_args()
        config_file = args.config
        
        # 如果配置文件不存在，创建示例配置
        if not Path(config_file).exists():
            print(f"配置文件不存在: {config_file}")
            if create_sample_config(config_file, target_type):
                print(f"请编辑 {config_file} 并重新运行")
            return
        
        # 创建配置和同步引擎
        config = ConfigManager.create_config()

        # 根据配置调整日志级别
        # 修复: 从配置中读取日志级别并应用，添加安全验证
        level = getattr(logging, config.log_level.upper(), logging.INFO)
        logger.setLevel(level)
        
        engine = XTFSyncEngine(config)
        
        # 显示配置信息
        print(f"\n📋 已加载配置:")
        print(f"  配置文件: {config_file}")
        print(f"  Excel文件: {config.file_path}")
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  接口调用间隔: {config.rate_limit_delay}秒")
        print(f"  最大重试次数: {config.max_retries}")
        print(f"  日志级别: {config.log_level}")
        
        # 目标特定信息
        if target_type == TargetType.BITABLE and config.app_token:
            token_display = config.app_token[:8] + "..." if len(config.app_token) >= 8 else config.app_token + "..."
            print(f"  多维表格Token: {token_display}")
            print(f"  数据表ID: {config.table_id}")
            print(f"  自动创建字段: {'是' if config.create_missing_fields else '否'}")
        elif target_type == TargetType.SHEET and config.spreadsheet_token:
            token_display = config.spreadsheet_token[:8] + "..." if len(config.spreadsheet_token) >= 8 else config.spreadsheet_token + "..."
            print(f"  电子表格Token: {token_display}")
            print(f"  工作表ID: {config.sheet_id}")
            print(f"  开始位置: {config.start_column}{config.start_row}")
        
        # 验证Excel文件
        file_path = Path(config.file_path)
        if not file_path.exists():
            print(f"\n❌ 错误: 文件不存在 - {file_path}")
            return
        
        print(f"\n📖 读取文件: {file_path}")
        df = pd.read_excel(file_path)
        print(f"✅ 文件读取成功，共 {len(df)} 行，{len(df.columns)} 列")
        
        # 执行同步
        print(f"\n🚀 开始执行 {config.sync_mode.value} 同步...")
        start = time.time()
        success = engine.sync(df)
        duration = time.time() - start
        
        if success:
            print(f"\n✅ 同步完成！耗时 {duration:.2f} 秒")
            if target_type == TargetType.BITABLE and config.app_token:
                print(f"🔗 多维表格链接: https://feishu.cn/base/{config.app_token}")
            elif target_type == TargetType.SHEET and config.spreadsheet_token:
                print(f"🔗 电子表格链接: https://feishu.cn/sheets/{config.spreadsheet_token}")
        else:
            print("\n❌ 同步出错，请查看日志")
            
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        logger.error("程序异常", exc_info=True)


if __name__ == "__main__":
    main()