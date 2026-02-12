#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF (Excel To Feishu) - 统一入口模块

模块概述：
    XTF（Excel To Feishu）是一款企业级数据同步工具，专门用于将本地Excel/CSV数据
    同步到飞书平台。本模块作为程序的统一入口，整合了多维表格（Bitable）和电子表格
    （Sheet）两种目标类型的同步功能。

主要功能：
    1. 命令行参数解析与配置管理
    2. 数据文件读取与格式验证
    3. 同步引擎初始化与执行
    4. 日志系统配置
    5. 用户交互与状态反馈

支持的文件格式：
    - Excel (.xlsx/.xls): ✅ 稳定支持，生产就绪
      - 使用 Calamine 引擎（可选），性能提升 4-20 倍
      - 支持 OpenPyXL 引擎作为备选
    - CSV (.csv): 🧪 实验性支持，测试阶段
      - 自动处理 UTF-8/GBK 编码
      - 建议生产环境使用 Excel 格式

支持的同步模式：
    - full（全量同步）：更新已存在记录，新增不存在记录
    - incremental（增量同步）：仅新增不存在的记录
    - overwrite（覆盖同步）：删除已存在记录后新增
    - clone（克隆同步）：清空远程表后完全重建

使用示例：
    # 基本用法（使用配置文件）
    $ python XTF.py --target-type bitable --config config.yaml
    
    # 指定目标类型和同步模式
    $ python XTF.py --target-type sheet --sync-mode full
    
    # 调试模式
    $ python XTF.py --target-type bitable --log-level DEBUG

依赖关系：
    内部模块：
        - core.config: 配置管理（SyncConfig, ConfigManager）
        - core.engine: 同步引擎（XTFSyncEngine）
        - core.reader: 文件读取（DataFileReader）
        - utils.excel_reader: Excel引擎信息
    外部依赖：
        - pandas: 数据处理
        - logging: 日志记录
        - pathlib: 路径处理

注意事项：
    1. 首次运行时如果配置文件不存在，会自动生成示例配置
    2. 命令行参数优先级高于配置文件
    3. CSV 格式目前处于实验阶段，生产环境建议使用 Excel
    4. 同步过程会在 logs/ 目录生成详细日志文件

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
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
from core.reader import DataFileReader
from utils.excel_reader import print_engine_info


def setup_logger():
    """
    设置基础日志器
    
    初始化根日志器，配置控制台输出处理器和统一格式化器。
    此函数确保日志系统只被初始化一次，避免重复添加处理器。
    
    日志格式：
        时间戳 - 日志级别 - 消息内容
        示例：2026-01-24 10:30:45,123 - INFO - 同步开始
    
    Returns:
        logging.Logger: 配置好的根日志器实例
    
    注意：
        - 默认日志级别为 INFO
        - 实际运行时会根据配置文件或命令行参数调整日志级别
        - 更详细的日志输出到 logs/ 目录的日志文件中
    """
    logger = logging.getLogger()
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


def main():
    """
    主函数 - XTF程序入口点
    
    执行流程：
        1. 初始化日志系统
        2. 显示程序信息和 Excel 引擎状态
        3. 解析目标类型（bitable/sheet）
        4. 加载配置文件，若不存在则创建示例配置
        5. 创建同步配置和引擎实例
        6. 验证数据文件存在性和格式支持
        7. 读取数据文件到 DataFrame
        8. 执行数据同步
        9. 输出同步结果和链接
    
    异常处理：
        - KeyboardInterrupt: 用户中断（Ctrl+C），优雅退出
        - Exception: 捕获所有其他异常，记录错误日志
    
    返回值：
        无返回值，通过打印输出和日志记录同步状态
    
    注意：
        - 配置优先级：命令行参数 > 配置文件 > 智能推断 > 系统默认
        - CSV 文件会显示实验性警告
        - 同步成功后会显示飞书文档链接
    """
    logger = setup_logger()

    print("=" * 70)
    print("     XTF工具 (模块化统一版本)")
    print("     支持多维表格和电子表格同步")
    print("     支持Excel格式(.xlsx/.xls) + CSV格式(.csv 实验性)")
    print("     支持四种同步模式：全量、增量、覆盖、克隆")
    print("=" * 70)

    # 显示 Excel 引擎信息
    print_engine_info()

    try:
        # 解析目标类型
        target_type = ConfigManager.parse_target_type()
        print(f"\n🎯 目标类型: {target_type.value}")
        print(f"📝 描述: {get_target_description(target_type)}")

        # 获取配置文件路径
        import argparse

        parser = argparse.ArgumentParser(add_help=False)
        parser.add_argument("--config", "-c", type=str, default="config.yaml")
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
        print("\n📋 已加载配置:")
        print(f"  配置文件: {config_file}")
        print(f"  数据文件: {config.file_path}")
        if config.excel_sheet_name is not None:
            print(f"  Excel工作表: {config.excel_sheet_name}")
        print(f"  同步模式: {config.sync_mode.value}")
        print(f"  索引列: {config.index_column or '未指定'}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  接口调用间隔: {config.rate_limit_delay}秒")
        print(f"  最大重试次数: {config.max_retries}")
        print(f"  日志级别: {config.log_level}")

        # 目标特定信息
        if target_type == TargetType.BITABLE and config.app_token:
            token_display = (
                config.app_token[:8] + "..."
                if len(config.app_token) >= 8
                else config.app_token + "..."
            )
            print(f"  多维表格Token: {token_display}")
            print(f"  数据表ID: {config.table_id}")
            print(f"  自动创建字段: {'是' if config.create_missing_fields else '否'}")
        elif target_type == TargetType.SHEET and config.spreadsheet_token:
            token_display = (
                config.spreadsheet_token[:8] + "..."
                if len(config.spreadsheet_token) >= 8
                else config.spreadsheet_token + "..."
            )
            print(f"  电子表格Token: {token_display}")
            print(f"  工作表ID: {config.sheet_id}")
            print(f"  开始位置: {config.start_column}{config.start_row}")

        # 验证数据文件
        file_path = Path(config.file_path)
        if not file_path.exists():
            print(f"\n❌ 错误: 文件不存在 - {file_path}")
            return

        # 检查文件格式是否支持
        if not DataFileReader.is_supported(file_path):
            print(f"\n❌ 错误: 不支持的文件格式 - {file_path.suffix}")
            print(f"支持的格式: {DataFileReader.get_supported_formats()}")
            return

        # 使用统一的文件读取器
        print(f"\n📖 读取文件: {file_path}")
        print(f"   文件格式: {file_path.suffix.upper()}")

        # 如果是CSV文件，显示测试阶段警告
        if file_path.suffix.lower() == ".csv":
            print("   ⚠️  警告: CSV格式当前处于实验性测试阶段")
            print("   🏭 生产环境建议使用Excel格式(.xlsx/.xls)")

        # 准备读取参数
        is_excel_with_sheet = (
            config.excel_sheet_name is not None
            and file_path.suffix.lower() in ['.xlsx', '.xls']
        )

        read_kwargs = {}
        if is_excel_with_sheet:
            read_kwargs['sheet_name'] = config.excel_sheet_name

        try:
            reader = DataFileReader()
            df = reader.read_file(file_path, **read_kwargs)
            print(f"✅ 文件读取成功，共 {len(df)} 行，{len(df.columns)} 列")
            if is_excel_with_sheet:
                print(f"   读取工作表: {config.excel_sheet_name}")
        except ValueError as e:
            print(f"\n❌ 文件读取失败: {e}")
            if is_excel_with_sheet and ("Worksheet" in str(e) or "sheet" in str(e).lower()):
                print(f"💡 提示: 指定的工作表 '{config.excel_sheet_name}' 可能不存在，请检查名称或索引")
            return
        except Exception as e:
            print(f"\n❌ 文件读取异常: {e}")
            logger.error("文件读取异常", exc_info=True)
            return

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
                print(
                    f"🔗 电子表格链接: https://feishu.cn/sheets/{config.spreadsheet_token}"
                )
        else:
            print("\n❌ 同步出错，请查看日志")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断")
    except Exception as e:
        print(f"\n❌ 程序异常: {e}")
        logger.error("程序异常", exc_info=True)


if __name__ == "__main__":
    main()
