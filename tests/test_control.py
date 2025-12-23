#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
控制模块测试
测试 core/control.py 中的重试和频控功能
"""

import time
import pytest
from unittest.mock import MagicMock, patch

from core.control import (
    RetryConfig,
    RetryStrategy,
    ExponentialBackoffRetry,
    LinearGrowthRetry,
    FixedWaitRetry,
    RateLimitConfig,
    RateLimitStrategy,
    FixedWaitRateConfig,
    FixedWaitRateLimit,
    SlidingWindowRateConfig,
    SlidingWindowRateLimit,
    FixedWindowRateConfig,
    FixedWindowRateLimit,
    RequestController,
    GlobalRequestController,
)


class TestRetryConfig:
    """重试配置测试"""

    def test_default_values(self):
        """测试默认值"""
        config = RetryConfig()
        assert config.initial_delay == 0.5
        assert config.max_retries == 3
        assert config.max_wait_time is None

    def test_custom_values(self):
        """测试自定义值"""
        config = RetryConfig(initial_delay=1.0, max_retries=5, max_wait_time=30.0)
        assert config.initial_delay == 1.0
        assert config.max_retries == 5
        assert config.max_wait_time == 30.0


class TestExponentialBackoffRetry:
    """指数退避重试策略测试"""

    def test_get_delay(self):
        """测试延迟计算"""
        config = RetryConfig(initial_delay=1.0, max_retries=5)
        strategy = ExponentialBackoffRetry(config, multiplier=2.0)

        assert strategy.get_delay(0) == 1.0  # 1.0 * 2^0
        assert strategy.get_delay(1) == 2.0  # 1.0 * 2^1
        assert strategy.get_delay(2) == 4.0  # 1.0 * 2^2
        assert strategy.get_delay(3) == 8.0  # 1.0 * 2^3

    def test_get_delay_with_max_wait_time(self):
        """测试带最大等待时间的延迟计算"""
        config = RetryConfig(initial_delay=1.0, max_retries=5, max_wait_time=5.0)
        strategy = ExponentialBackoffRetry(config, multiplier=2.0)

        assert strategy.get_delay(0) == 1.0
        assert strategy.get_delay(1) == 2.0
        assert strategy.get_delay(2) == 4.0
        assert strategy.get_delay(3) == 5.0  # 限制在最大值
        assert strategy.get_delay(4) == 5.0  # 限制在最大值

    def test_should_retry(self):
        """测试是否应该重试"""
        config = RetryConfig(initial_delay=1.0, max_retries=3)
        strategy = ExponentialBackoffRetry(config)

        assert strategy.should_retry(0) is True
        assert strategy.should_retry(1) is True
        assert strategy.should_retry(2) is True
        assert strategy.should_retry(3) is False

    def test_should_retry_with_max_wait_time(self):
        """测试带最大等待时间的重试判断"""
        config = RetryConfig(initial_delay=1.0, max_retries=10, max_wait_time=5.0)
        strategy = ExponentialBackoffRetry(config)

        assert strategy.should_retry(0, elapsed_time=0) is True
        assert strategy.should_retry(0, elapsed_time=4.9) is True
        assert strategy.should_retry(0, elapsed_time=5.0) is False
        assert strategy.should_retry(0, elapsed_time=10.0) is False


class TestLinearGrowthRetry:
    """线性增长重试策略测试"""

    def test_get_delay(self):
        """测试延迟计算"""
        config = RetryConfig(initial_delay=1.0, max_retries=5)
        strategy = LinearGrowthRetry(config, increment=0.5)

        assert strategy.get_delay(0) == 1.0  # 1.0 + 0.5 * 0
        assert strategy.get_delay(1) == 1.5  # 1.0 + 0.5 * 1
        assert strategy.get_delay(2) == 2.0  # 1.0 + 0.5 * 2
        assert strategy.get_delay(3) == 2.5  # 1.0 + 0.5 * 3

    def test_get_delay_with_max_wait_time(self):
        """测试带最大等待时间的延迟计算"""
        config = RetryConfig(initial_delay=1.0, max_retries=5, max_wait_time=2.0)
        strategy = LinearGrowthRetry(config, increment=0.5)

        assert strategy.get_delay(0) == 1.0
        assert strategy.get_delay(1) == 1.5
        assert strategy.get_delay(2) == 2.0  # 限制在最大值
        assert strategy.get_delay(3) == 2.0  # 限制在最大值


class TestFixedWaitRetry:
    """固定等待重试策略测试"""

    def test_get_delay(self):
        """测试延迟计算"""
        config = RetryConfig(initial_delay=1.0, max_retries=5)
        strategy = FixedWaitRetry(config)

        assert strategy.get_delay(0) == 1.0
        assert strategy.get_delay(1) == 1.0
        assert strategy.get_delay(2) == 1.0
        assert strategy.get_delay(10) == 1.0


class TestFixedWaitRateLimit:
    """固定等待频控策略测试"""

    def test_can_proceed_immediately(self):
        """测试首次请求可以立即执行"""
        config = FixedWaitRateConfig(delay=0.5)
        rate_limiter = FixedWaitRateLimit(config)

        assert rate_limiter.can_proceed() is True

    def test_can_proceed_after_delay(self):
        """测试延迟后可以执行"""
        config = FixedWaitRateConfig(delay=0.1)
        rate_limiter = FixedWaitRateLimit(config)

        rate_limiter.last_request_time = time.time()
        time.sleep(0.15)

        assert rate_limiter.can_proceed() is True

    def test_cannot_proceed_before_delay(self):
        """测试延迟前不能执行"""
        config = FixedWaitRateConfig(delay=1.0)
        rate_limiter = FixedWaitRateLimit(config)

        rate_limiter.last_request_time = time.time()

        assert rate_limiter.can_proceed() is False

    def test_wait_if_needed(self):
        """测试等待执行"""
        config = FixedWaitRateConfig(delay=0.1)
        rate_limiter = FixedWaitRateLimit(config)

        start_time = time.time()
        rate_limiter.last_request_time = start_time

        result = rate_limiter.wait_if_needed()

        assert result is True
        assert time.time() - start_time >= 0.1

    def test_reset(self):
        """测试重置"""
        config = FixedWaitRateConfig(delay=0.5)
        rate_limiter = FixedWaitRateLimit(config)

        rate_limiter.last_request_time = time.time()
        rate_limiter.reset()

        assert rate_limiter.last_request_time == 0


class TestSlidingWindowRateLimit:
    """滑动时间窗频控策略测试"""

    def test_can_proceed_within_limit(self):
        """测试在限制内可以执行"""
        config = SlidingWindowRateConfig(window_size=1.0, max_requests=5)
        rate_limiter = SlidingWindowRateLimit(config)

        assert rate_limiter.can_proceed() is True

    def test_cannot_proceed_at_limit(self):
        """测试达到限制后不能执行"""
        config = SlidingWindowRateConfig(window_size=1.0, max_requests=3)
        rate_limiter = SlidingWindowRateLimit(config)

        # 添加3个请求时间戳
        current_time = time.time()
        rate_limiter.request_timestamps.extend(
            [current_time - 0.1, current_time - 0.2, current_time - 0.3]
        )

        assert rate_limiter.can_proceed() is False

    def test_can_proceed_after_window_expires(self):
        """测试时间窗过期后可以执行"""
        config = SlidingWindowRateConfig(window_size=0.1, max_requests=2)
        rate_limiter = SlidingWindowRateLimit(config)

        # 添加过期的请求
        rate_limiter.request_timestamps.extend([time.time() - 0.2, time.time() - 0.2])

        assert rate_limiter.can_proceed() is True

    def test_reset(self):
        """测试重置"""
        config = SlidingWindowRateConfig(window_size=1.0, max_requests=5)
        rate_limiter = SlidingWindowRateLimit(config)

        rate_limiter.request_timestamps.extend([1, 2, 3])
        rate_limiter.reset()

        assert len(rate_limiter.request_timestamps) == 0


class TestFixedWindowRateLimit:
    """固定时间窗频控策略测试"""

    def test_can_proceed_new_window(self):
        """测试新时间窗可以执行"""
        config = FixedWindowRateConfig(window_size=1.0, max_requests=5)
        rate_limiter = FixedWindowRateLimit(config)

        assert rate_limiter.can_proceed() is True

    def test_cannot_proceed_at_limit(self):
        """测试达到限制后不能执行"""
        config = FixedWindowRateConfig(window_size=10.0, max_requests=3)
        rate_limiter = FixedWindowRateLimit(config)

        rate_limiter.current_window_requests = 3

        assert rate_limiter.can_proceed() is False

    def test_reset(self):
        """测试重置"""
        config = FixedWindowRateConfig(window_size=1.0, max_requests=5)
        rate_limiter = FixedWindowRateLimit(config)

        rate_limiter.current_window_requests = 3
        rate_limiter.reset()

        assert rate_limiter.current_window_requests == 0


class TestRequestController:
    """请求控制器测试"""

    def test_execute_request_success(self):
        """测试成功执行请求"""
        controller = RequestController()

        def success_func():
            return "success"

        result = controller.execute_request(success_func)
        assert result == "success"

    def test_execute_request_with_retry(self):
        """测试带重试的请求执行"""
        config = RetryConfig(initial_delay=0.01, max_retries=3)
        retry_strategy = FixedWaitRetry(config)
        controller = RequestController(retry_strategy=retry_strategy)

        call_count = 0

        def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Temporary error")
            return "success"

        result = controller.execute_request(failing_func)
        assert result == "success"
        assert call_count == 3

    def test_execute_request_max_retries_exceeded(self):
        """测试超过最大重试次数"""
        config = RetryConfig(initial_delay=0.01, max_retries=2)
        retry_strategy = FixedWaitRetry(config)
        controller = RequestController(retry_strategy=retry_strategy)

        def always_failing_func():
            raise Exception("Permanent error")

        with pytest.raises(Exception, match="Permanent error"):
            controller.execute_request(always_failing_func)

    def test_execute_request_with_rate_limit(self):
        """测试带频控的请求执行"""
        rate_config = FixedWaitRateConfig(delay=0.05)
        rate_limiter = FixedWaitRateLimit(rate_config)
        controller = RequestController(rate_limit_strategy=rate_limiter)

        start_time = time.time()

        def success_func():
            return "success"

        # 执行两次请求
        controller.execute_request(success_func)
        controller.execute_request(success_func)

        elapsed = time.time() - start_time
        assert elapsed >= 0.05  # 至少等待一次频控延迟


class TestGlobalRequestController:
    """全局请求控制器测试"""

    def test_singleton_pattern(self):
        """测试单例模式"""
        controller1 = GlobalRequestController()
        controller2 = GlobalRequestController()

        assert controller1 is controller2

    def test_create_from_config_exponential_backoff(self):
        """测试从配置创建（指数退避）"""
        controller = GlobalRequestController.create_from_config(
            retry_type="exponential_backoff",
            retry_config={"initial_delay": 1.0, "max_retries": 3, "multiplier": 2.0},
            rate_limit_type="fixed_wait",
            rate_limit_config={"delay": 0.1},
        )

        assert controller is not None
        inner_controller = controller.get_controller()
        assert inner_controller is not None
        assert isinstance(inner_controller.retry_strategy, ExponentialBackoffRetry)

    def test_create_from_config_linear_growth(self):
        """测试从配置创建（线性增长）"""
        controller = GlobalRequestController.create_from_config(
            retry_type="linear_growth",
            retry_config={"initial_delay": 0.5, "max_retries": 3, "increment": 0.5},
            rate_limit_type="fixed_wait",
            rate_limit_config={"delay": 0.1},
        )

        inner_controller = controller.get_controller()
        assert isinstance(inner_controller.retry_strategy, LinearGrowthRetry)

    def test_create_from_config_fixed_wait(self):
        """测试从配置创建（固定等待）"""
        controller = GlobalRequestController.create_from_config(
            retry_type="fixed_wait",
            retry_config={"initial_delay": 1.0, "max_retries": 3},
            rate_limit_type="fixed_wait",
            rate_limit_config={"delay": 0.1},
        )

        inner_controller = controller.get_controller()
        assert isinstance(inner_controller.retry_strategy, FixedWaitRetry)

    def test_create_from_config_sliding_window(self):
        """测试从配置创建（滑动时间窗）"""
        controller = GlobalRequestController.create_from_config(
            retry_type="exponential_backoff",
            retry_config={"initial_delay": 0.5, "max_retries": 3},
            rate_limit_type="sliding_window",
            rate_limit_config={"window_size": 1.0, "max_requests": 10},
        )

        inner_controller = controller.get_controller()
        assert isinstance(inner_controller.rate_limit_strategy, SlidingWindowRateLimit)

    def test_create_from_config_fixed_window(self):
        """测试从配置创建（固定时间窗）"""
        controller = GlobalRequestController.create_from_config(
            retry_type="exponential_backoff",
            retry_config={"initial_delay": 0.5, "max_retries": 3},
            rate_limit_type="fixed_window",
            rate_limit_config={"window_size": 1.0, "max_requests": 10},
        )

        inner_controller = controller.get_controller()
        assert isinstance(inner_controller.rate_limit_strategy, FixedWindowRateLimit)

    def test_get_api_client(self):
        """测试获取 API 客户端"""
        controller = GlobalRequestController.create_from_config()
        client = controller.get_api_client()

        assert client is not None
        assert client.controller is not None


class TestRetryStrategyWait:
    """重试策略等待方法测试"""

    def test_wait_returns_true(self):
        """测试等待方法返回 True"""
        config = RetryConfig(initial_delay=0.01, max_retries=3)
        strategy = FixedWaitRetry(config)

        result = strategy.wait(0)
        assert result is True

    def test_wait_respects_max_wait_time(self):
        """测试等待方法遵守最大等待时间"""
        config = RetryConfig(initial_delay=0.1, max_retries=3, max_wait_time=0.05)
        strategy = FixedWaitRetry(config)

        # 由于 initial_delay > max_wait_time，应该返回 False
        result = strategy.wait(0)
        assert result is False
