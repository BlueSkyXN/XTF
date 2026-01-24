#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书 API 模块包

模块概述：
    此包封装了飞书开放平台的 API 调用，提供统一的认证管理、
    网络请求处理以及多维表格和电子表格的操作接口。

包结构：
    api/
    ├── __init__.py     - 包初始化，导出公共接口
    ├── auth.py         - 飞书认证管理（FeishuAuth）
    ├── base.py         - 基础网络层（RateLimiter, RetryableAPIClient）
    ├── bitable.py      - 多维表格 API（BitableAPI）
    └── sheet.py        - 电子表格 API（SheetAPI）

导出的类：
    认证相关：
        - FeishuAuth: 飞书认证管理器，负责获取和刷新访问令牌
    
    网络层：
        - RateLimiter: 接口频率限制器
        - RetryableAPIClient: 可重试的 API 客户端
    
    业务 API：
        - BitableAPI: 多维表格 API 客户端
        - SheetAPI: 电子表格 API 客户端

API 调用流程：
    1. FeishuAuth 获取/刷新 tenant_access_token
    2. RetryableAPIClient 处理请求重试和频率限制
    3. BitableAPI/SheetAPI 封装具体业务操作

使用示例：
    >>> from api import FeishuAuth, BitableAPI, RateLimiter, RetryableAPIClient
    >>> 
    >>> # 初始化认证
    >>> auth = FeishuAuth(app_id, app_secret)
    >>> 
    >>> # 初始化 API 客户端
    >>> rate_limiter = RateLimiter(delay=0.5)
    >>> api_client = RetryableAPIClient(max_retries=3, rate_limiter=rate_limiter)
    >>> 
    >>> # 初始化多维表格 API
    >>> bitable_api = BitableAPI(auth, api_client)
    >>> records = bitable_api.get_all_records(app_token, table_id)

设计原则：
    - 关注点分离：认证、网络、业务逻辑分层
    - 可复用性：基础组件可独立使用
    - 可扩展性：易于添加新的 API 类型

作者: XTF Team
版本: 1.7.3+
"""

from .auth import FeishuAuth
from .base import RateLimiter, RetryableAPIClient
from .bitable import BitableAPI
from .sheet import SheetAPI

__all__ = ["FeishuAuth", "RateLimiter", "RetryableAPIClient", "BitableAPI", "SheetAPI"]
