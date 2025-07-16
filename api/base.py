#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础网络层模块
提供HTTP请求重试机制和频率限制功能
"""

import time
import logging
import requests
from typing import Optional


class RateLimiter:
    """接口频率限制器"""
    
    def __init__(self, delay: float = 0.5):
        """
        初始化频率限制器
        
        Args:
            delay: 调用间隔时间（秒）
        """
        self.delay = delay
        self.last_call = 0
    
    def wait(self):
        """等待以遵守频率限制"""
        current_time = time.time()
        time_since_last = current_time - self.last_call
        if time_since_last < self.delay:
            time.sleep(self.delay - time_since_last)
        self.last_call = time.time()


class RetryableAPIClient:
    """可重试的API客户端"""
    
    def __init__(self, max_retries: int = 3, rate_limiter: Optional[RateLimiter] = None):
        """
        初始化API客户端
        
        Args:
            max_retries: 最大重试次数
            rate_limiter: 频率限制器实例
        """
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter()
        self.logger = logging.getLogger(__name__)
    
    def call_api(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        调用API并处理重试
        
        Args:
            method: HTTP方法
            url: 请求URL
            **kwargs: 其他请求参数
            
        Returns:
            响应对象
            
        Raises:
            Exception: 当所有重试都失败时
        """
        for attempt in range(self.max_retries + 1):
            try:
                self.rate_limiter.wait()
                
                response = requests.request(method, url, timeout=60, **kwargs)
                
                # 检查是否需要重试
                if response.status_code == 429:  # 频率限制
                    if attempt < self.max_retries:
                        wait_time = 2 ** attempt  # 指数退避
                        self.logger.warning(f"频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                
                if response.status_code >= 500:  # 服务器错误
                    if attempt < self.max_retries:
                        wait_time = 2 ** attempt
                        self.logger.warning(f"服务器错误 {response.status_code}，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue
                
                return response
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = 2 ** attempt
                    self.logger.warning(f"请求异常 {e}，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                raise
        
        raise Exception(f"API调用失败，已重试 {self.max_retries} 次")