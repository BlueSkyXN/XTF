#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF (Excel To Feishu) - 统一入口文件
本地表格同步到飞书多维表格工具
支持四种同步模式：全量、增量、覆盖、克隆
具备智能字段管理、频率限制、重试机制等企业级功能
"""

import pandas as pd
import time
import logging
from pathlib import Path

# 导入模块化组件
from core import SyncConfig, ConfigManager, XTFSyncEngine, create_sample_config


def setup_logger():
    """设置基础日志器"""
    logger = logging.getLogger()
    if not logger.handlers:  # 避免重复设置
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def main():
    """主函数"""
    # 设置基础日志
    logger = setup_logger()
    
    print("=" * 70)
    print("     XTF工具 (模块化版本)")
    print("     支持四种同步模式：全量、增量、覆盖、克隆")
    print("=" * 70)
    
    try:
        # 先解析命令行参数以获取配置文件路径
        import argparse
        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument('--config', '-c', type=str, default='config.yaml')
        args, _ = parser.parse_known_args()
        config_file_path = args.config
        
        # 如果指定的配置文件不存在，创建示例配置
        if not Path(config_file_path).exists():
            print(f"配置文件不存在: {config_file_path}")
            if create_sample_config(config_file_path):
                print(f"请编辑 {config_file_path} 文件并重新运行程序")
            return
        
        # 加载配置
        config = ConfigManager.create_config()
        
        # 显示加载的配置信息
        print(f"\n📋 已加载配置:")
        print(f"  配置文件: {config_file_path}")
        print(f"  Excel文件: {config.file_path}")
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  接口调用间隔: {config.rate_limit_delay}秒")
        print(f"  最大重试次数: {config.max_retries}")
        print(f"  自动创建字段: {'是' if config.create_missing_fields else '否'}")
        print(f"  日志级别: {config.log_level}")
        
        # 验证文件
        file_path = Path(config.file_path)
        if not file_path.exists():
            print(f"\n❌ 错误: Excel文件不存在 - {file_path}")
            print("请检查配置文件中的 file_path 参数")
            return
        
        # 读取Excel文件
        print(f"\n📖 正在读取文件: {file_path}")
        try:
            df = pd.read_excel(file_path)
            print(f"✅ 文件读取成功，共 {len(df)} 行，{len(df.columns)} 列")
            print(f"📊 列名: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"❌ 文件读取失败: {e}")
            return
        
        # 创建同步引擎
        print(f"\n🔧 正在初始化模块化同步引擎...")
        sync_engine = XTFSyncEngine(config)
        
        # 执行同步
        print(f"\n🚀 开始执行 {config.sync_mode.value} 同步...")
        start_time = time.time()
        
        success = sync_engine.sync(df)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if success:
            print(f"\n✅ 同步完成！耗时: {duration:.2f} 秒")
            print(f"📊 数据已同步到飞书多维表格")
            print(f"🔗 多维表格链接: https://feishu.cn/base/{config.app_token}")
        else:
            print(f"\n❌ 同步过程中出现错误，请查看日志文件")
            
    except KeyboardInterrupt:
        print("\n\n⚠️  用户中断操作")
    except Exception as e:
        print(f"\n❌ 程序运行出错: {str(e)}")
        logger.error(f"程序异常: {e}", exc_info=True)


if __name__ == "__main__":
    main()