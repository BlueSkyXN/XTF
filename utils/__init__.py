#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
XTF 工具模块包

模块概述：
    此包包含 XTF 工具的通用工具函数和辅助类，提供跨模块使用的
    公共功能。

包结构：
    utils/
    ├── __init__.py         - 包初始化
    ├── excel_reader.py     - Excel 智能读取模块
    └── validators.py       - 输入验证模块

当前功能：
    - Excel 智能读取（自动选择最优引擎）
    - 输入验证（防止 SSRF、路径遍历等安全漏洞）

设计原则：
    - 工具函数应是无状态的纯函数
    - 尽量减少对其他模块的依赖
    - 提供清晰的接口文档

作者: XTF Team
版本: 1.7.3+
"""
