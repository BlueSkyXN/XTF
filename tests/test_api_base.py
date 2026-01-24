#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API 基础模块测试

模块概述：
    此模块测试 api/base.py 中的 HTTP 客户端功能，包括频率限制器
    和可重试 API 客户端的各种场景。

测试覆盖：
    RateLimiter（频率限制器）：
        - 默认延迟配置
        - 自定义延迟配置
        - 首次调用无需等待
        - 强制执行调用间隔
        - 足够时间后无需额外等待
    
    RetryableAPIClient（可重试 API 客户端）：
        初始化测试：
            - 默认参数值
            - 自定义参数值
        
        API 调用测试：
            - 成功调用
            - 带参数调用
            - 服务器错误重试
            - 频率限制重试
            - 最大重试次数
            - 请求异常重试
        
        HTTP 方法测试：
            - GET 方法
            - POST 方法
            - PUT 方法
            - DELETE 方法
        
        指数退避测试：
            - 退避时间验证

测试策略：
    - 使用 unittest.mock 模拟 HTTP 请求
    - 使用 time.sleep 模拟验证等待时间
    - 独立测试各个功能点

依赖关系：
    测试目标：
        - api.base.RateLimiter
        - api.base.RetryableAPIClient
    测试工具：
        - pytest
        - unittest.mock
        - requests

作者: XTF Team
版本: 1.7.3+
"""

import time
import pytest
from unittest.mock import MagicMock, patch, Mock
import requests

from api.base import RateLimiter, RetryableAPIClient


class TestRateLimiter:
    """频率限制器测试"""

    def test_init_default_delay(self):
        """测试默认延迟"""
        limiter = RateLimiter()
        assert limiter.delay == 0.5
        assert limiter.last_call == 0

    def test_init_custom_delay(self):
        """测试自定义延迟"""
        limiter = RateLimiter(delay=1.0)
        assert limiter.delay == 1.0

    def test_wait_first_call(self):
        """测试首次调用无需等待"""
        limiter = RateLimiter(delay=0.5)

        start_time = time.time()
        limiter.wait()
        elapsed = time.time() - start_time

        # 首次调用应该几乎立即返回
        assert elapsed < 0.1

    def test_wait_enforces_delay(self):
        """测试等待强制执行延迟"""
        limiter = RateLimiter(delay=0.1)

        # 首次调用
        limiter.wait()
        start_time = time.time()

        # 第二次调用应该等待
        limiter.wait()
        elapsed = time.time() - start_time

        assert elapsed >= 0.1

    def test_wait_no_delay_after_sufficient_time(self):
        """测试足够时间后无需额外等待"""
        limiter = RateLimiter(delay=0.05)

        # 首次调用
        limiter.wait()

        # 等待足够长的时间
        time.sleep(0.1)

        start_time = time.time()
        limiter.wait()
        elapsed = time.time() - start_time

        # 不应该有额外等待
        assert elapsed < 0.05


class TestRetryableAPIClientInit:
    """可重试 API 客户端初始化测试"""

    def test_init_default_values(self):
        """测试默认值"""
        client = RetryableAPIClient(use_global_controller=False)

        assert client.max_retries == 3
        assert client.rate_limiter is not None
        assert client.use_global_controller is False

    def test_init_custom_values(self):
        """测试自定义值"""
        limiter = RateLimiter(delay=1.0)
        client = RetryableAPIClient(
            max_retries=5, rate_limiter=limiter, use_global_controller=False
        )

        assert client.max_retries == 5
        assert client.rate_limiter.delay == 1.0


class TestRetryableAPIClientCallAPI:
    """可重试 API 客户端调用测试"""

    @patch("requests.request")
    def test_call_api_success(self, mock_request):
        """测试成功的 API 调用"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        response = client.call_api("GET", "http://example.com/api")

        assert response.status_code == 200
        mock_request.assert_called_once()

    @patch("requests.request")
    def test_call_api_with_kwargs(self, mock_request):
        """测试带参数的 API 调用"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        client.call_api(
            "POST",
            "http://example.com/api",
            json={"key": "value"},
            headers={"Authorization": "Bearer token"},
        )

        mock_request.assert_called_once_with(
            "POST",
            "http://example.com/api",
            timeout=60,
            json={"key": "value"},
            headers={"Authorization": "Bearer token"},
        )

    @patch("time.sleep")
    @patch("requests.request")
    def test_call_api_retry_on_server_error(self, mock_request, mock_sleep):
        """测试服务器错误时重试"""
        mock_response_error = Mock()
        mock_response_error.status_code = 500

        mock_response_success = Mock()
        mock_response_success.status_code = 200

        mock_request.side_effect = [mock_response_error, mock_response_success]

        client = RetryableAPIClient(max_retries=3, use_global_controller=False)
        response = client.call_api("GET", "http://example.com/api")

        assert response.status_code == 200
        assert mock_request.call_count == 2

    @patch("time.sleep")
    @patch("requests.request")
    def test_call_api_retry_on_rate_limit(self, mock_request, mock_sleep):
        """测试频率限制时重试"""
        mock_response_rate_limited = Mock()
        mock_response_rate_limited.status_code = 429

        mock_response_success = Mock()
        mock_response_success.status_code = 200

        mock_request.side_effect = [mock_response_rate_limited, mock_response_success]

        client = RetryableAPIClient(max_retries=3, use_global_controller=False)
        response = client.call_api("GET", "http://example.com/api")

        assert response.status_code == 200
        assert mock_request.call_count == 2

    @patch("time.sleep")
    @patch("requests.request")
    def test_call_api_max_retries_exceeded(self, mock_request, mock_sleep):
        """测试超过最大重试次数"""
        mock_response = Mock()
        mock_response.status_code = 500

        mock_request.return_value = mock_response

        client = RetryableAPIClient(max_retries=2, use_global_controller=False)

        # 应该返回最后一次失败的响应，而不是抛出异常
        response = client.call_api("GET", "http://example.com/api")

        # 验证返回的是 500 响应
        assert response.status_code == 500

        # 初始调用 + 2次重试 = 3次调用
        assert mock_request.call_count == 3

    @patch("time.sleep")
    @patch("requests.request")
    def test_call_api_retry_on_request_exception(self, mock_request, mock_sleep):
        """测试请求异常时重试"""
        mock_response_success = Mock()
        mock_response_success.status_code = 200

        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            mock_response_success,
        ]

        client = RetryableAPIClient(max_retries=3, use_global_controller=False)
        response = client.call_api("GET", "http://example.com/api")

        assert response.status_code == 200
        assert mock_request.call_count == 2

    @patch("time.sleep")
    @patch("requests.request")
    def test_call_api_request_exception_max_retries(self, mock_request, mock_sleep):
        """测试请求异常超过最大重试次数"""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection failed"
        )

        client = RetryableAPIClient(max_retries=2, use_global_controller=False)

        with pytest.raises(requests.exceptions.ConnectionError):
            client.call_api("GET", "http://example.com/api")


class TestRetryableAPIClientHTTPMethods:
    """HTTP 方法测试"""

    @patch("requests.request")
    def test_get_method(self, mock_request):
        """测试 GET 方法"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        client.call_api("GET", "http://example.com/api")

        mock_request.assert_called_with("GET", "http://example.com/api", timeout=60)

    @patch("requests.request")
    def test_post_method(self, mock_request):
        """测试 POST 方法"""
        mock_response = Mock()
        mock_response.status_code = 201
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        client.call_api("POST", "http://example.com/api", json={"data": "test"})

        mock_request.assert_called_with(
            "POST", "http://example.com/api", timeout=60, json={"data": "test"}
        )

    @patch("requests.request")
    def test_put_method(self, mock_request):
        """测试 PUT 方法"""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        client.call_api("PUT", "http://example.com/api", json={"data": "update"})

        mock_request.assert_called_with(
            "PUT", "http://example.com/api", timeout=60, json={"data": "update"}
        )

    @patch("requests.request")
    def test_delete_method(self, mock_request):
        """测试 DELETE 方法"""
        mock_response = Mock()
        mock_response.status_code = 204
        mock_request.return_value = mock_response

        client = RetryableAPIClient(use_global_controller=False)
        client.call_api("DELETE", "http://example.com/api/1")

        mock_request.assert_called_with(
            "DELETE", "http://example.com/api/1", timeout=60
        )


class TestRetryableAPIClientExponentialBackoff:
    """指数退避测试"""

    @patch("time.sleep")
    @patch("requests.request")
    def test_exponential_backoff_timing(self, mock_request, mock_sleep):
        """测试指数退避时间"""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_request.return_value = mock_response

        client = RetryableAPIClient(max_retries=3, use_global_controller=False)

        # 调用会重试直到达到最大次数，然后返回最后的响应
        response = client.call_api("GET", "http://example.com/api")

        assert response.status_code == 500

        # 验证 sleep 被调用（包含频率限制和退避）
        assert mock_sleep.call_count > 0
