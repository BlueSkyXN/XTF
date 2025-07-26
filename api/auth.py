#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
飞书认证模块
负责获取和管理飞书访问令牌
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

from .base import RetryableAPIClient, RateLimiter


class FeishuAuth:
    """飞书认证管理器"""
    
    def __init__(self, app_id: str, app_secret: str, api_client: Optional[RetryableAPIClient] = None):
        """
        初始化认证管理器
        
        Args:
            app_id: 飞书应用ID
            app_secret: 飞书应用密钥
            api_client: API客户端实例
        """
        self.app_id = app_id
        self.app_secret = app_secret
        self.api_client = api_client or RetryableAPIClient(rate_limiter=RateLimiter(0.5))
        self.logger = logging.getLogger('XTF.auth')
        
        # Token管理
        self.tenant_access_token = None
        self.token_expires_at = None
    
    def get_tenant_access_token(self) -> str:
        """
        获取租户访问令牌
        
        Returns:
            访问令牌字符串
            
        Raises:
            Exception: 当获取令牌失败时
        """
        # 检查token是否过期
        if (self.tenant_access_token and self.token_expires_at and 
            datetime.now() < self.token_expires_at - timedelta(minutes=5)):
            return self.tenant_access_token
        
        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        data = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        
        response = self.api_client.call_api("POST", url, headers=headers, json=data)
        
        try:
            result = response.json()
        except ValueError as e:
            raise Exception(f"获取访问令牌响应解析失败: {e}, HTTP状态码: {response.status_code}")
        
        if result.get("code") != 0:
            error_msg = result.get('msg', '未知错误')
            raise Exception(f"获取访问令牌失败: 错误码 {result.get('code')}, 错误信息: {error_msg}")
        
        self.tenant_access_token = result["tenant_access_token"]
        # 设置过期时间（提前5分钟刷新）
        expires_in = result.get("expire", 7200)
        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in)
        
        self.logger.info("成功获取租户访问令牌")
        return self.tenant_access_token
    
    def get_auth_headers(self) -> Dict[str, str]:
        """
        获取认证头
        
        Returns:
            包含认证信息的HTTP头字典
        """
        token = self.get_tenant_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }