#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF 测试套件包

模块概述：
    此包包含 XTF 工具的所有单元测试和集成测试。使用 pytest 框架
    进行测试组织和执行。

包结构：
    tests/
    ├── __init__.py         - 测试包初始化
    ├── conftest.py         - pytest 配置和共享 fixtures
    ├── test_api_base.py    - API 基础模块测试
    ├── test_config.py      - 配置模块测试
    ├── test_control.py     - 控制模块测试
    ├── test_converter.py   - 转换模块测试
    └── test_reader.py      - 读取模块测试

运行测试：
    # 运行所有测试
    $ pytest tests/
    
    # 运行特定测试文件
    $ pytest tests/test_config.py
    
    # 运行带覆盖率报告
    $ pytest tests/ --cov=core --cov=api
    
    # 详细输出
    $ pytest tests/ -v

测试覆盖范围：
    - 配置管理（枚举、数据类、验证）
    - 数据转换（类型检测、值转换）
    - 文件读取（Excel、CSV）
    - API 客户端（重试、频控）
    - 控制策略（重试策略、频控策略）

作者: XTF Team
版本: 1.7.3+
"""
# XTF Test Suite
