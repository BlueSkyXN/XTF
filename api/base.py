#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基础网络层模块

模块概述：
    此模块提供 HTTP 请求的基础功能，包括频率限制和自动重试机制。
    作为所有飞书 API 调用的底层支撑，确保请求的稳定性和可靠性。

主要功能：
    1. 接口调用频率限制（防止触发 API 限流）
    2. 自动重试机制（处理临时性错误）
    3. 支持新的统一控制系统（可选）
    4. 指数退避策略（应对服务器繁忙）

核心类：
    RateLimiter:
        接口频率限制器，通过控制调用间隔确保不超过 API 限流阈值。
        使用简单的时间戳记录实现最小间隔控制。

    RetryableAPIClient:
        可重试的 API 客户端，自动处理常见错误并重试：
        - HTTP 429（频率限制）：等待后重试
        - HTTP 5xx（服务器错误）：指数退避后重试
        - 网络异常：指数退避后重试

重试策略：
    采用指数退避算法，等待时间为 2^attempt 秒：
    - 第1次重试：等待 1 秒
    - 第2次重试：等待 2 秒
    - 第3次重试：等待 4 秒
    以此类推...

与高级控制系统的集成：
    当配置了全局控制器时（enable_advanced_control=true），
    RetryableAPIClient 会使用 core.control 中定义的高级策略，
    否则使用传统的重试和频控机制（向后兼容）。

使用示例：
    # 基本使用
    >>> limiter = RateLimiter(delay=0.5)  # 500ms间隔
    >>> client = RetryableAPIClient(max_retries=3, rate_limiter=limiter)
    >>> response = client.call_api("GET", "https://api.example.com/data")

    # 带参数的请求
    >>> response = client.call_api(
    ...     "POST",
    ...     "https://api.example.com/create",
    ...     json={"name": "test"},
    ...     headers={"Authorization": "Bearer token"}
    ... )

配置参数：
    RateLimiter:
        - delay (float): 调用间隔时间，单位秒，默认 0.5

    RetryableAPIClient:
        - max_retries (int): 最大重试次数，默认 3
        - rate_limiter (RateLimiter): 频率限制器实例
        - use_global_controller (bool): 是否使用全局控制器，默认 True

依赖关系：
    内部模块：
        - core.control: 全局控制器（可选依赖）
    外部依赖：
        - requests: HTTP 请求库
        - time: 时间控制
        - logging: 日志记录

注意事项：
    1. 所有请求默认超时时间为 60 秒
    2. 重试只针对可恢复的错误（429、5xx、网络异常）
    3. 4xx 错误（除429外）不会触发重试
    4. 全局控制器导入失败时会自动回退到传统模式

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import time
import logging
from typing import Optional

import requests  # type: ignore[import-untyped]


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

    def __init__(
        self,
        max_retries: int = 3,
        rate_limiter: Optional[RateLimiter] = None,
        use_global_controller: bool = True,
    ):
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
        self.logger = logging.getLogger("XTF.base")

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
                    raise requests.exceptions.RequestException(
                        f"Rate limit exceeded: {response.status_code}"
                    )

                if response.status_code >= 500:  # 服务器错误
                    raise requests.exceptions.RequestException(
                        f"Server error: {response.status_code}"
                    )

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
                        wait_time = 2**attempt  # 指数退避
                        self.logger.warning(f"频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue

                if response.status_code >= 500:  # 服务器错误
                    if attempt < self.max_retries:
                        wait_time = 2**attempt
                        self.logger.warning(
                            f"服务器错误 {response.status_code}，等待 {wait_time} 秒后重试..."
                        )
                        time.sleep(wait_time)
                        continue

                return response

            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries:
                    wait_time = 2**attempt
                    self.logger.warning(f"请求异常 {e}，等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                    continue
                raise

        raise Exception(f"API调用失败，已重试 {self.max_retries} 次")
