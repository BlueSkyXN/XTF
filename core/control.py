#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一控制模块
提供全局的重试和频控管理功能，兼容现有配置系统
"""

import time
import logging
import requests
import threading
from abc import ABC, abstractmethod
from typing import Optional, Callable, Any, Union
from dataclasses import dataclass
from collections import deque
from enum import Enum


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
        if self.config.max_wait_time is not None and elapsed_time >= self.config.max_wait_time:
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
        delay = self.config.initial_delay * (self.multiplier ** attempt)
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
        self.last_request_time = 0
    
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
        self.request_timestamps = deque()
    
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
    
    def __init__(self, 
                 retry_strategy: Optional[RetryStrategy] = None,
                 rate_limit_strategy: Optional[RateLimitStrategy] = None):
        self.retry_strategy = retry_strategy
        self.rate_limit_strategy = rate_limit_strategy
        self.logger = logging.getLogger('XTF.control')
    
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
                if not self.retry_strategy or not self.retry_strategy.should_retry(attempt, elapsed_time):
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


class EnhancedAPIClient:
    """增强的API客户端，兼容原有接口"""
    
    def __init__(self, controller: Optional[RequestController] = None):
        self.controller = controller
        self.logger = logging.getLogger('XTF.control')
    
    def call_api(self, method: str, url: str, **kwargs) -> requests.Response:
        """调用API，应用统一的重试和频控策略"""
        
        def _make_request():
            response = requests.request(method, url, timeout=60, **kwargs)
            
            # 检查是否需要重试的响应状态
            if response.status_code == 429:  # 频率限制
                raise requests.exceptions.RequestException(f"Rate limit exceeded: {response.status_code}")
            
            if response.status_code >= 500:  # 服务器错误
                raise requests.exceptions.RequestException(f"Server error: {response.status_code}")
            
            return response
        
        if self.controller:
            return self.controller.execute_request(_make_request)
        else:
            # 回退到直接执行
            return _make_request()


# ============================================================================
# 全局控制器单例
# ============================================================================

class GlobalRequestController:
    """全局请求控制器单例"""
    
    _instance = None
    _controller = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with GlobalRequestController._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def configure(self, controller: RequestController):
        """配置全局控制器"""
        with GlobalRequestController._lock:
            self._controller = controller
    
    def get_controller(self) -> Optional[RequestController]:
        """获取全局控制器实例"""
        with GlobalRequestController._lock:
            return self._controller
    
    def get_api_client(self) -> EnhancedAPIClient:
        """获取配置好的API客户端"""
        return EnhancedAPIClient(self._controller)
    
    @classmethod
    def create_from_config(cls, 
                          retry_type: str = "exponential_backoff",
                          retry_config: dict = None,
                          rate_limit_type: str = "fixed_wait", 
                          rate_limit_config: dict = None):
        """从配置创建全局控制器"""
        
        # 创建重试策略
        retry_strategy = None
        if retry_config is None:
            retry_config = {"initial_delay": 0.5, "max_retries": 3}
        
        base_retry_config = RetryConfig(**{k: v for k, v in retry_config.items() 
                                         if k in ['initial_delay', 'max_retries', 'max_wait_time']})
        
        if retry_type == "exponential_backoff":
            multiplier = retry_config.get('multiplier', 2.0)
            retry_strategy = ExponentialBackoffRetry(base_retry_config, multiplier)
        elif retry_type == "linear_growth":
            increment = retry_config.get('increment', 0.5)
            retry_strategy = LinearGrowthRetry(base_retry_config, increment)
        elif retry_type == "fixed_wait":
            retry_strategy = FixedWaitRetry(base_retry_config)
        
        # 创建频控策略
        rate_limit_strategy = None
        if rate_limit_config is None:
            rate_limit_config = {"delay": 0.1}
        
        if rate_limit_type == "fixed_wait":
            config = FixedWaitRateConfig(**{k: v for k, v in rate_limit_config.items() 
                                          if k in ['delay']})
            rate_limit_strategy = FixedWaitRateLimit(config)
        elif rate_limit_type == "sliding_window":
            config = SlidingWindowRateConfig(**{k: v for k, v in rate_limit_config.items() 
                                              if k in ['window_size', 'max_requests']})
            rate_limit_strategy = SlidingWindowRateLimit(config)
        elif rate_limit_type == "fixed_window":
            config = FixedWindowRateConfig(**{k: v for k, v in rate_limit_config.items() 
                                            if k in ['window_size', 'max_requests']})
            rate_limit_strategy = FixedWindowRateLimit(config)
        
        # 创建控制器
        controller = RequestController(retry_strategy, rate_limit_strategy)
        
        # 配置全局实例
        global_controller = cls()
        global_controller.configure(controller)
        
        return global_controller