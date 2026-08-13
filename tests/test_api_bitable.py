#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""多维表格 API 分页与幂等契约测试。"""

from unittest.mock import Mock
from uuid import UUID

import pytest

from api.bitable import BitableAPI
from api.base import RateLimiter, RetryableAPIClient
from api.sdk import FeishuAPIError, PaginationError


def make_response(data):
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {"code": 0, "data": data}
    return response


def make_api(responses):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    client.call_api.side_effect = responses
    return BitableAPI(auth, client), client


def test_list_fields_collects_pages():
    api, client = make_api(
        [
            make_response(
                {"items": [{"field_name": "A"}], "has_more": True, "page_token": "p2"}
            ),
            make_response({"items": [{"field_name": "B"}], "has_more": False}),
        ]
    )

    fields = api.list_fields("app", "table")

    assert [field["field_name"] for field in fields] == ["A", "B"]
    assert client.call_api.call_args_list[1].kwargs["params"]["page_token"] == "p2"


def test_list_fields_rejects_has_more_without_token():
    api, _ = make_api(
        [make_response({"items": [], "has_more": True, "page_token": None})]
    )

    with pytest.raises(PaginationError, match="未提供 page_token"):
        api.list_fields("app", "table")


def test_get_all_records_rejects_has_more_without_token():
    api, _ = make_api(
        [make_response({"items": [{"record_id": "rec1"}], "has_more": True})]
    )

    with pytest.raises(PaginationError, match="未提供 page_token"):
        api.get_all_records("app", "table")


def test_search_records_rejects_has_more_without_token():
    api, _ = make_api(
        [make_response({"items": [{"record_id": "rec1"}], "has_more": True})]
    )

    with pytest.raises(PaginationError, match="未提供 page_token"):
        api.search_records("app", "table")


def test_get_all_records_rejects_repeated_page_token():
    api, _ = make_api(
        [
            make_response({"items": [], "has_more": True, "page_token": "repeated"}),
            make_response({"items": [], "has_more": True, "page_token": "repeated"}),
        ]
    )

    with pytest.raises(PaginationError, match="重复 page_token"):
        api.get_all_records("app", "table")


def test_search_records_keeps_public_tuple_contract():
    api, _ = make_api(
        [
            make_response(
                {
                    "items": [{"record_id": "rec1"}],
                    "has_more": True,
                    "page_token": "p2",
                }
            )
        ]
    )

    assert api.search_records("app", "table") == (
        [{"record_id": "rec1"}],
        "p2",
    )


def test_batch_create_uses_unique_uuid_client_tokens():
    api, client = make_api(
        [make_response({"records": []}), make_response({"records": []})]
    )

    assert api.batch_create_records("app", "table", [{"fields": {}}]) is True
    assert api.batch_create_records("app", "table", [{"fields": {}}]) is True

    first = client.call_api.call_args_list[0].kwargs["params"]["client_token"]
    second = client.call_api.call_args_list[1].kwargs["params"]["client_token"]
    assert first != second
    assert str(UUID(first)) == first
    assert str(UUID(second)) == second


def test_batch_create_reuses_client_token_for_business_retry(monkeypatch):
    retry_response = Mock()
    retry_response.status_code = 200
    retry_response.headers = {}
    retry_response.json.return_value = {"code": 1254291, "msg": "write conflict"}
    api, client = make_api([retry_response, make_response({"records": []})])
    monkeypatch.setattr("time.sleep", Mock())

    assert api.batch_create_records("app", "table", [{"fields": {}}]) is True

    first = client.call_api.call_args_list[0].kwargs["params"]["client_token"]
    second = client.call_api.call_args_list[1].kwargs["params"]["client_token"]
    assert first == second


@pytest.mark.parametrize("status", [429, 503])
def test_bitable_does_not_repeat_transport_http_retry(status, monkeypatch):
    retry_response = Mock()
    retry_response.status_code = status
    retry_response.headers = {"Retry-After": "1"}
    retry_response.json.return_value = {"code": 0, "msg": "busy"}
    api, client = make_api([retry_response, make_response({"records": []})])
    sleep = Mock()
    monkeypatch.setattr("time.sleep", sleep)

    assert api.batch_create_records("app", "table", [{"fields": {}}]) is False

    # HTTP 重试只属于 transport；注入 transport 返回最终响应后，业务层不再重试。
    client.call_api.assert_called_once()
    sleep.assert_not_called()


def test_bitable_does_not_retry_http_response_when_disabled():
    retry_response = Mock()
    retry_response.status_code = 503
    retry_response.headers = {}
    retry_response.json.return_value = {"code": 0, "msg": "busy"}
    api, client = make_api([retry_response])

    with pytest.raises(FeishuAPIError) as exc_info:
        api._call_api_with_biz_retry("GET", "https://example.test", max_retries=0)

    assert exc_info.value.http_status == 503
    client.call_api.assert_called_once()


@pytest.mark.parametrize(
    ("method_name", "items"),
    [
        ("batch_create_records", [{"fields": {}}]),
        ("batch_update_records", [{"record_id": "rec1", "fields": {}}]),
        ("batch_delete_records", ["rec1"]),
    ],
)
def test_boolean_batch_methods_return_false_for_typed_api_errors(method_name, items):
    error_response = Mock()
    error_response.status_code = 200
    error_response.headers = {"X-Tt-Logid": "log-test"}
    error_response.json.return_value = {"code": 1254003, "msg": "denied"}
    api, _ = make_api([error_response])

    result = getattr(api, method_name)("app", "table", items)

    assert result is False


def test_create_field_returns_false_for_invalid_json():
    response = Mock()
    response.status_code = 502
    response.headers = {}
    response.json.side_effect = ValueError("invalid json")
    api, _ = make_api([response])

    api._call_api_with_biz_retry = Mock(
        side_effect=FeishuAPIError(502, "invalid json", http_status=502)
    )
    assert api.create_field("app", "table", "Name") is False


def test_search_records_exposes_typed_api_error():
    error_response = Mock()
    error_response.status_code = 200
    error_response.headers = {"X-Tt-Logid": "log-search"}
    error_response.json.return_value = {"code": 1254003, "msg": "denied"}
    api, _ = make_api([error_response])

    with pytest.raises(FeishuAPIError) as exc_info:
        api.search_records("app", "table")

    assert exc_info.value.log_id == "log-search"


@pytest.mark.parametrize(
    "data",
    [
        None,
        {"items": None},
        {"items": "not-a-list"},
        {"items": {"x": 1}},
        {"items": [], "has_more": "false"},
        {"items": [], "has_more": True, "page_token": 123},
    ],
)
def test_pagination_rejects_invalid_data_shapes(data):
    api, _ = make_api([make_response(data)])

    with pytest.raises(FeishuAPIError, match="分页响应") as exc_info:
        api.get_all_records("app", "table")

    assert exc_info.value.kind == "invalid_response"


def test_business_retry_uses_transport_retry_budget(monkeypatch):
    retry_response = Mock()
    retry_response.status_code = 200
    retry_response.headers = {}
    retry_response.json.return_value = {"code": 1254290, "msg": "busy"}
    api, client = make_api([retry_response])
    client.max_retries = 0
    monkeypatch.setattr("time.sleep", Mock())

    with pytest.raises(FeishuAPIError):
        api.search_records("app", "table")

    client.call_api.assert_called_once()


def test_common_rate_limit_code_is_retried(monkeypatch):
    retry_response = Mock()
    retry_response.status_code = 200
    retry_response.headers = {}
    retry_response.json.return_value = {"code": 99991400, "msg": "rate limited"}
    api, client = make_api(
        [retry_response, make_response({"items": [], "has_more": False})]
    )
    client.max_retries = 1
    monkeypatch.setattr("time.sleep", Mock())

    assert api.search_records("app", "table") == ([], None)
    assert client.call_api.call_count == 2


def test_real_transport_does_not_multiply_http_retries(monkeypatch):
    response = Mock()
    response.status_code = 503
    response.headers = {}
    response.json.return_value = {"code": 0, "msg": "busy"}
    request = Mock(return_value=response)
    monkeypatch.setattr("requests.request", request)
    monkeypatch.setattr("time.sleep", Mock())
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = RetryableAPIClient(
        max_retries=3,
        rate_limiter=RateLimiter(0),
        use_global_controller=False,
        jitter_ratio=0,
    )
    api = BitableAPI(auth, client)

    assert api.batch_create_records("app", "table", [{"fields": {}}]) is False

    assert request.call_count == 4
