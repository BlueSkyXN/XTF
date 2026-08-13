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
import random
from typing import Optional

import requests  # type: ignore[import-untyped]

from .sdk import FeishuResponseParser


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
        jitter_ratio: float = 0.1,
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
        self.jitter_ratio = max(0.0, jitter_ratio)
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

    def call_api(
        self, method: str, url: str, *, retry_transport: bool = True, **kwargs
    ) -> requests.Response:
        """
        调用 API，并在 transport 层处理网络异常与 HTTP 429/5xx 重试。

        标准模式和高级 controller 都会把重试耗尽后的最终 HTTP response
        返回给上层 parser，以保留状态码、log_id 和服务端 pacing 信息；只有
        始终未取得 response 的网络异常才会抛出 typed transport error。

        Args:
            method: HTTP方法
            url: 请求URL
            retry_transport: 是否对网络异常和 HTTP 429/5xx 做 transport 重试；
                无幂等键的 create/append 应传 False
            **kwargs: 其他请求参数

        Returns:
            HTTP 响应对象，包括重试耗尽后的最终错误响应

        Raises:
            FeishuAPIError: 所有网络尝试均未取得 HTTP response 时
        """
        if not retry_transport:
            return self._call_api_once(method, url, **kwargs)

        # 如果配置了全局控制器并且可用，使用新的统一控制系统
        if self.use_global_controller and self._controller:
            last_response = None
            last_failure_had_response = False

            def _make_request():
                nonlocal last_failure_had_response, last_response
                last_failure_had_response = False
                try:
                    response = requests.request(method, url, timeout=60, **kwargs)
                except requests.exceptions.RequestException:
                    # 最后一次尝试没有 response 时，不得返回此前缓存的 HTTP 错误。
                    last_response = None
                    raise
                last_response = response

                # 检查是否需要重试的响应状态
                if response.status_code == 429:  # 频率限制
                    last_failure_had_response = True
                    raise requests.exceptions.RequestException(
                        f"Rate limit exceeded: {response.status_code}"
                    )

                if response.status_code >= 500:  # 服务器错误
                    last_failure_had_response = True
                    raise requests.exceptions.RequestException(
                        f"Server error: {response.status_code}"
                    )

                return response

            try:
                return self._controller.execute_request(_make_request)
            except requests.exceptions.RequestException as exc:
                # 保留最终 HTTP response，供统一 parser 提取状态、log_id 和 pacing；
                # 没有 response 的网络失败则直接转换成 typed transport error。
                if last_failure_had_response and last_response is not None:
                    return last_response
                from .sdk import FeishuAPIError

                raise FeishuAPIError.from_transport(str(exc), cause=exc) from exc
            except Exception as exc:
                # controller 自身的频控/等待失败也属于 transport 控制边界。
                from .sdk import FeishuAPIError

                raise FeishuAPIError.from_transport(str(exc), cause=exc) from exc

        # 否则使用传统的重试和频控机制（向后兼容）
        return self._call_api_legacy(method, url, **kwargs)

    def _call_api_once(self, method: str, url: str, **kwargs) -> requests.Response:
        """Send once for mutations whose outcome cannot be replayed safely."""
        request_started = False
        try:
            if self.use_global_controller and self._controller:
                rate_limit_strategy = getattr(
                    self._controller, "rate_limit_strategy", None
                )
                if rate_limit_strategy and not rate_limit_strategy.wait_if_needed():
                    raise RuntimeError("频控限制：本次 mutation 未发送")
            else:
                self.rate_limiter.wait()
            request_started = True
            return requests.request(method, url, timeout=60, **kwargs)
        except requests.exceptions.RequestException as exc:
            from .sdk import FeishuAPIError

            error = FeishuAPIError.from_transport(str(exc), cause=exc)
            error.response_data = {"request_started": request_started}
            raise error from exc
        except Exception as exc:
            from .sdk import FeishuAPIError

            error = FeishuAPIError.from_transport(str(exc), cause=exc)
            error.response_data = {"request_started": request_started}
            raise error from exc

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
                        wait_time = self._retry_delay(response, attempt)
                        self.logger.warning(f"频率限制，等待 {wait_time} 秒后重试...")
                        time.sleep(wait_time)
                        continue

                if response.status_code >= 500:  # 服务器错误
                    if attempt < self.max_retries:
                        wait_time = self._retry_delay(response, attempt)
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
                from .sdk import FeishuAPIError

                raise FeishuAPIError.from_transport(str(e), cause=e) from e

        raise Exception(f"API调用失败，已重试 {self.max_retries} 次")

    def _retry_delay(self, response: requests.Response, attempt: int) -> float:
        """优先遵循服务端 pacing，再对本地指数退避加入少量 jitter。"""
        retry_after = FeishuResponseParser._parse_retry_after(response)
        if retry_after is not None:
            return retry_after
        base_delay = float(2**attempt)
        if self.jitter_ratio == 0:
            return base_delay
        return base_delay + random.uniform(0, base_delay * self.jitter_ratio)
