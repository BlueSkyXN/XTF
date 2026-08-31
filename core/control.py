#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一控制模块 - 高级重试与频控策略

模块概述：
    此模块提供可显式装配的重试和频率控制（频控）策略。每个 runtime
    拥有独立 RequestController，不使用进程全局状态。

主要功能：
    1. 重试策略实现（指数退避、线性增长、固定等待）
    2. 频控策略实现（固定等待、滑动窗口、固定窗口）
    3. 统一请求控制器（整合重试和频控）

重试策略详解：
    ExponentialBackoffRetry（指数退避）：
        延迟时间按指数增长，适合应对突发限流
        delay = initial_delay × multiplier^attempt
        示例：0.5s → 1s → 2s → 4s → 8s

    LinearGrowthRetry（线性增长）：
        延迟时间线性增长，适合稳定限流场景
        delay = initial_delay + increment × attempt
        示例：0.5s → 1s → 1.5s → 2s → 2.5s

    FixedWaitRetry（固定等待）：
        延迟时间恒定，适合已知限流间隔的场景
        delay = initial_delay（恒定）
        示例：1s → 1s → 1s → 1s

频控策略详解：
    FixedWaitRateLimit（固定等待频控）：
        每次请求后等待固定时间
        适合：简单场景，API 有明确的调用间隔要求

    SlidingWindowRateLimit（滑动时间窗频控）：
        在滑动的时间窗口内限制请求数量
        适合：需要平滑请求分布的场景
        特点：窗口随时间滑动，更精确的限流

    FixedWindowRateLimit（固定时间窗频控）：
        在固定的时间窗口内限制请求数量
        适合：API 按固定周期重置配额的场景
        特点：窗口边界固定，实现简单

配置示例（YAML v2）：
    control:
      advanced:
        enabled: true
        retry:
          strategy: exponential_backoff
          initial_delay: 0.5
          multiplier: 2.0
          max_wait_time: 30.0
        rate_limit:
          strategy: sliding_window
          window_size: 1.0
          max_requests: 10

使用示例：
    # 创建控制器
    >>> from core.control import build_request_controller
    >>> controller = build_request_controller(
    ...     retry_type="exponential_backoff",
    ...     retry_config={"initial_delay": 0.5, "max_retries": 3},
    ...     rate_limit_type="sliding_window",
    ...     rate_limit_config={"window_size": 1.0, "max_requests": 10}
    ... )

    # 执行请求
    >>> result = controller.execute_request(api_call)

设计模式：
    - 策略模式：重试和频控策略可灵活替换
    - 模板方法：基类定义接口，子类实现具体策略

依赖关系：
    外部依赖：
        - time: 时间控制
        - collections.deque: 滑动窗口数据结构

注意事项：
    1. 启用高级控制需设置 control.advanced.enabled: true
    2. 重试次数过多可能导致整体延迟增加
    3. 频控策略会阻塞当前线程

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-24
"""

import time
import logging
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

# ============================================================================
# 重试策略实现
# ============================================================================


@dataclass
class RetryConfig:
    """重试配置基类"""

    initial_delay: float = 0.5  # 初始延迟时间，支持小于1的数
    max_retries: int = 3  # 最大重试次数
    max_wait_time: Optional[float] = None  # 最大等待时间（可选）


class RetryStrategy(ABC):
    """重试策略抽象基类"""

    def __init__(self, config: RetryConfig):
        self.config = config

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """获取指定尝试次数的延迟时间"""
        pass

    def should_retry(self, attempt: int, elapsed_time: float = 0) -> bool:
        """判断是否应该重试"""
        if attempt >= self.config.max_retries:
            return False
        if (
            self.config.max_wait_time is not None
            and elapsed_time >= self.config.max_wait_time
        ):
            return False
        return True

    def wait(self, attempt: int) -> bool:
        """执行等待，返回是否应该继续重试"""
        delay = self.get_delay(attempt)
        if self.config.max_wait_time is not None and delay > self.config.max_wait_time:
            return False
        time.sleep(delay)
        return True


class ExponentialBackoffRetry(RetryStrategy):
    """指数退避重试策略"""

    def __init__(self, config: RetryConfig, multiplier: float = 2.0):
        super().__init__(config)
        self.multiplier = multiplier

    def get_delay(self, attempt: int) -> float:
        delay = self.config.initial_delay * (self.multiplier**attempt)
        if self.config.max_wait_time is not None:
            delay = min(delay, self.config.max_wait_time)
        return delay


class LinearGrowthRetry(RetryStrategy):
    """线性增长重试策略"""

    def __init__(self, config: RetryConfig, increment: float = 0.5):
        super().__init__(config)
        self.increment = increment

    def get_delay(self, attempt: int) -> float:
        delay = self.config.initial_delay + (self.increment * attempt)
        if self.config.max_wait_time is not None:
            delay = min(delay, self.config.max_wait_time)
        return delay


class FixedWaitRetry(RetryStrategy):
    """固定等待重试策略"""

    def get_delay(self, attempt: int) -> float:
        # attempt参数在固定延迟策略中不使用，但保持接口一致性
        _ = attempt  # 标记参数已使用
        return self.config.initial_delay


# ============================================================================
# 频控策略实现
# ============================================================================


@dataclass
class RateLimitConfig:
    """频控配置基类"""

    pass


class RateLimitStrategy(ABC):
    """频控策略抽象基类"""

    def __init__(self, config: RateLimitConfig):
        self.config = config

    @abstractmethod
    def can_proceed(self) -> bool:
        """检查是否可以继续执行请求"""
        pass

    @abstractmethod
    def wait_if_needed(self) -> bool:
        """如果需要等待则等待，返回是否成功等待"""
        pass

    def reset(self):
        """重置频控状态"""
        pass


@dataclass
class FixedWaitRateConfig(RateLimitConfig):
    """固定等待频控配置"""

    delay: float = 0.1  # 固定延迟时间


class FixedWaitRateLimit(RateLimitStrategy):
    """固定等待频控策略"""

    def __init__(self, config: FixedWaitRateConfig):
        super().__init__(config)
        self.config: FixedWaitRateConfig = config
        self.last_request_time: float = 0.0

    def can_proceed(self) -> bool:
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        return time_since_last >= self.config.delay

    def wait_if_needed(self) -> bool:
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.config.delay:
            wait_time = self.config.delay - time_since_last
            time.sleep(wait_time)

        self.last_request_time = time.time()
        return True

    def reset(self):
        self.last_request_time = 0


@dataclass
class SlidingWindowRateConfig(RateLimitConfig):
    """滑动时间窗频控配置"""

    window_size: float = 1.0  # 时间窗大小（秒）
    max_requests: int = 10  # 时间窗内的最大请求数


class SlidingWindowRateLimit(RateLimitStrategy):
    """滑动时间窗频控策略"""

    def __init__(self, config: SlidingWindowRateConfig):
        super().__init__(config)
        self.config: SlidingWindowRateConfig = config
        self.request_timestamps: deque[float] = deque()

    def _cleanup_old_requests(self):
        current_time = time.time()
        window_start = current_time - self.config.window_size
        while self.request_timestamps and self.request_timestamps[0] < window_start:
            self.request_timestamps.popleft()

    def can_proceed(self) -> bool:
        self._cleanup_old_requests()
        return len(self.request_timestamps) < self.config.max_requests

    def wait_if_needed(self) -> bool:
        self._cleanup_old_requests()

        if len(self.request_timestamps) < self.config.max_requests:
            self.request_timestamps.append(time.time())
            return True

        # 需要等待最早请求过期
        oldest_request = self.request_timestamps[0]
        wait_time = oldest_request + self.config.window_size - time.time()

        if wait_time > 0:
            time.sleep(wait_time)

        self._cleanup_old_requests()
        if len(self.request_timestamps) < self.config.max_requests:
            self.request_timestamps.append(time.time())
            return True

        return False

    def reset(self):
        self.request_timestamps.clear()


@dataclass
class FixedWindowRateConfig(RateLimitConfig):
    """固定时间窗频控配置"""

    window_size: float = 1.0  # 时间窗大小（秒）
    max_requests: int = 10  # 时间窗内的最大请求数


class FixedWindowRateLimit(RateLimitStrategy):
    """固定时间窗频控策略"""

    def __init__(self, config: FixedWindowRateConfig):
        super().__init__(config)
        self.config: FixedWindowRateConfig = config
        self.window_start_time = time.time()
        self.current_window_requests = 0

    def _get_current_window_start(self) -> float:
        current_time = time.time()
        return (current_time // self.config.window_size) * self.config.window_size

    def _is_new_window(self) -> bool:
        current_window_start = self._get_current_window_start()
        return current_window_start > self.window_start_time

    def can_proceed(self) -> bool:
        if self._is_new_window():
            self.window_start_time = self._get_current_window_start()
            self.current_window_requests = 0
        return self.current_window_requests < self.config.max_requests

    def wait_if_needed(self) -> bool:
        if self._is_new_window():
            self.window_start_time = self._get_current_window_start()
            self.current_window_requests = 0

        if self.current_window_requests < self.config.max_requests:
            self.current_window_requests += 1
            return True

        # 需要等待下一个时间窗
        next_window_start = self.window_start_time + self.config.window_size
        wait_time = next_window_start - time.time()

        if wait_time > 0:
            time.sleep(wait_time)

        self.window_start_time = self._get_current_window_start()
        self.current_window_requests = 1
        return True

    def reset(self):
        self.window_start_time = time.time()
        self.current_window_requests = 0


# ============================================================================
# 统一控制器
# ============================================================================


class RequestController:
    """统一请求控制器，整合重试和频控功能"""

    def __init__(
        self,
        retry_strategy: Optional[RetryStrategy] = None,
        rate_limit_strategy: Optional[RateLimitStrategy] = None,
    ):
        self.retry_strategy = retry_strategy
        self.rate_limit_strategy = rate_limit_strategy
        self.logger = logging.getLogger("XTF.control")

    def execute_request(self, func: Callable, *args, **kwargs) -> Any:
        """执行请求并应用重试和频控策略"""
        attempt = 0
        start_time = time.time()
        last_exception = None

        while True:
            try:
                # 应用频控策略
                if self.rate_limit_strategy:
                    if not self.rate_limit_strategy.wait_if_needed():
                        raise Exception("频控限制：已达到最大重试次数或请求限制")

                # 执行请求
                result = func(*args, **kwargs)
                return result

            except Exception as e:
                last_exception = e
                elapsed_time = time.time() - start_time

                # 检查是否应该重试
                if not self.retry_strategy or not self.retry_strategy.should_retry(
                    attempt, elapsed_time
                ):
                    self.logger.error(f"重试失败，已尝试 {attempt + 1} 次: {e}")
                    raise

                # 执行重试等待
                if not self.retry_strategy.wait(attempt):
                    self.logger.error(f"重试等待超时，已尝试 {attempt + 1} 次: {e}")
                    raise

                attempt += 1
                self.logger.warning(f"第 {attempt} 次重试，错误: {e}")

        if last_exception:
            raise last_exception


# ============================================================================
# Explicit controller construction
# ============================================================================


def build_request_controller(
    retry_type: str = "exponential_backoff",
    retry_config: Optional[dict[str, Any]] = None,
    rate_limit_type: str = "fixed_wait",
    rate_limit_config: Optional[dict[str, Any]] = None,
) -> RequestController:
    """Build one request controller without mutating process-global state."""
    retry_values = retry_config or {"initial_delay": 0.5, "max_retries": 3}
    base_retry_config = RetryConfig(
        **{
            key: value
            for key, value in retry_values.items()
            if key in {"initial_delay", "max_retries", "max_wait_time"}
        }
    )
    retry_strategy: Optional[RetryStrategy]
    if retry_type == "exponential_backoff":
        retry_strategy = ExponentialBackoffRetry(
            base_retry_config, retry_values.get("multiplier", 2.0)
        )
    elif retry_type == "linear_growth":
        retry_strategy = LinearGrowthRetry(
            base_retry_config, retry_values.get("increment", 0.5)
        )
    elif retry_type == "fixed_wait":
        retry_strategy = FixedWaitRetry(base_retry_config)
    else:
        raise ValueError(f"unsupported retry strategy: {retry_type}")

    rate_values = rate_limit_config or {"delay": 0.1}
    rate_limit_strategy: Optional[RateLimitStrategy]
    if rate_limit_type == "fixed_wait":
        rate_limit_strategy = FixedWaitRateLimit(
            FixedWaitRateConfig(
                **{key: value for key, value in rate_values.items() if key == "delay"}
            )
        )
    elif rate_limit_type == "sliding_window":
        rate_limit_strategy = SlidingWindowRateLimit(
            SlidingWindowRateConfig(
                **{
                    key: value
                    for key, value in rate_values.items()
                    if key in {"window_size", "max_requests"}
                }
            )
        )
    elif rate_limit_type == "fixed_window":
        rate_limit_strategy = FixedWindowRateLimit(
            FixedWindowRateConfig(
                **{
                    key: value
                    for key, value in rate_values.items()
                    if key in {"window_size", "max_requests"}
                }
            )
        )
    else:
        raise ValueError(f"unsupported rate-limit strategy: {rate_limit_type}")
    return RequestController(retry_strategy, rate_limit_strategy)
