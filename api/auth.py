#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书认证模块

模块概述：
    此模块负责飞书开放平台的认证管理，包括获取、缓存和刷新
    租户访问令牌（tenant_access_token）。所有飞书 API 调用
    都需要通过此模块获取认证信息。

主要功能：
    1. 获取租户访问令牌
    2. 令牌缓存和自动刷新
    3. 生成 API 调用所需的认证头

核心类：
    FeishuAuth:
        飞书认证管理器，负责管理应用的认证生命周期。
        使用自建应用的 app_id 和 app_secret 获取令牌。

认证流程：
    1. 使用 app_id 和 app_secret 调用飞书认证接口
    2. 获取 tenant_access_token 和过期时间
    3. 缓存令牌，在过期前 5 分钟自动刷新
    4. 为 API 调用提供 Bearer Token 认证头

令牌管理策略：
    - 首次调用时获取新令牌
    - 令牌有效期内直接返回缓存的令牌
    - 令牌即将过期（5分钟内）时自动刷新
    - 默认令牌有效期为 2 小时（7200秒）

API 端点：
    获取令牌：POST https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal

使用示例：
    # 初始化认证管理器
    >>> auth = FeishuAuth(app_id="cli_xxx", app_secret="xxx")
    >>>
    >>> # 获取访问令牌
    >>> token = auth.get_tenant_access_token()
    >>>
    >>> # 获取认证头（推荐方式）
    >>> headers = auth.get_auth_headers()
    >>> # headers = {
    >>> #     "Authorization": "Bearer xxx",
    >>> #     "Content-Type": "application/json; charset=utf-8"
    >>> # }

错误处理：
    - 认证失败会抛出包含错误码和错误信息的异常
    - 响应解析失败会抛出包含 HTTP 状态码的异常
    - 常见错误码：
        - 99991663: app_id 不存在
        - 99991664: app_secret 错误
        - 10003: 应用未启用

依赖关系：
    内部模块：
        - api.base: RetryableAPIClient, RateLimiter
    外部依赖：
        - logging: 日志记录
        - datetime: 时间处理

安全注意事项：
    1. app_secret 是敏感信息，不要提交到代码仓库
    2. 令牌应妥善保管，不要在日志中输出完整令牌
    3. 建议使用环境变量或配置文件管理凭据

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from .base import RetryableAPIClient, RateLimiter


class FeishuAuth:
    """飞书认证管理器"""

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        api_client: Optional[RetryableAPIClient] = None,
    ):
        """
        初始化认证管理器

        Args:
            app_id: 飞书应用ID
            app_secret: 飞书应用密钥
            api_client: API客户端实例
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.api_client = api_client or RetryableAPIClient(
            rate_limiter=RateLimiter(0.5)
        )
        self.logger = logging.getLogger("XTF.auth")

        # Token管理
        self.tenant_access_token: Optional[str] = None
        self.token_expires_at: Optional[datetime] = None

    def get_tenant_access_token(self) -> str:
        """
        获取租户访问令牌

        Returns:
            访问令牌字符串

        Raises:
            Exception: 当获取令牌失败时
        """
        # 检查token是否过期
        token = self.tenant_access_token
        if (
            token
            and self.token_expires_at
            and datetime.now() < self.token_expires_at - timedelta(minutes=5)
        ):
            return token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {"app_id": self.app_id, "app_secret": self.app_secret}

        response = self.api_client.call_api("POST", url, headers=headers, json=data)

        try:
            result = response.json()
        except ValueError as e:
            raise Exception(
                f"获取访问令牌响应解析失败: {e}, HTTP状态码: {response.status_code}"
            )

        if result.get("code") != 0:
            error_msg = result.get("msg", "未知错误")
            raise Exception(
                f"获取访问令牌失败: 错误码 {result.get('code')}, 错误信息: {error_msg}"
            )

        token = result["tenant_access_token"]
        self.tenant_access_token = token
        # 设置过期时间（提前5分钟刷新）
        expires_in = result.get("expire", 7200)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        self.logger.info("成功获取租户访问令牌")
        return token

    def get_auth_headers(self) -> Dict[str, str]:
        """
        获取认证头

        Returns:
            包含认证信息的HTTP头字典
        """
        token = self.get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8",
        }
