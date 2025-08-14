#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础网络层模块
提供HTTP请求重试机制和频率限制功能，支持新的统一控制系统
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
    """可重试的API客户端，支持新的统一控制系统"""
    
    def __init__(self, max_retries: int = 3, rate_limiter: Optional[RateLimiter] = None, 
                 use_global_controller: bool = True):
        """
        初始化API客户端
        
        Args:
            max_retries: 最大重试次数
            rate_limiter: 频率限制器实例（传统模式）
            use_global_controller: 是否使用全局统一控制器
        """
        self.max_retries = max_retries
        self.rate_limiter = rate_limiter or RateLimiter()
        self.use_global_controller = use_global_controller
        self.logger = logging.getLogger('XTF.base')
        
        # 尝试获取全局控制器
        self._controller = None
        if self.use_global_controller:
            try:
                from core.control import GlobalRequestController
                global_controller = GlobalRequestController()
                controller = global_controller.get_controller()
                if controller:
                    # 避免循环引用，直接使用控制器而不是API客户端
                    self._controller = controller
                else:
                    self.use_global_controller = False
            except ImportError:
                self.logger.warning("无法导入GlobalRequestController，回退到传统模式")
                self.use_global_controller = False
            except Exception as e:
                self.logger.warning(f"初始化全局控制器失败，回退到传统模式: {e}")
                self.use_global_controller = False
    
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
        # 如果配置了全局控制器并且可用，使用新的统一控制系统
        if self.use_global_controller and self._controller:
            def _make_request():
                response = requests.request(method, url, timeout=60, **kwargs)
                
                # 检查是否需要重试的响应状态
                if response.status_code == 429:  # 频率限制
                    raise requests.exceptions.RequestException(f"Rate limit exceeded: {response.status_code}")
                
                if response.status_code >= 500:  # 服务器错误
                    raise requests.exceptions.RequestException(f"Server error: {response.status_code}")
                
                return response
            
            return self._controller.execute_request(_make_request)
        
        # 否则使用传统的重试和频控机制（向后兼容）
        return self._call_api_legacy(method, url, **kwargs)
    
    def _call_api_legacy(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        传统的API调用方法（向后兼容）
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