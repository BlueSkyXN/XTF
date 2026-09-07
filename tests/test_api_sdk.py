#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XTF 专用飞书 SDK 契约测试。"""

from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import Mock

import pytest

from api.sdk import (
    FeishuAPIError,
    FeishuResponseParser,
    Page,
    PaginationError,
    Paginator,
    PartialBatchError,
    run_batches,
)


def make_response(status=200, body=None, headers=None):
    response = Mock()
    response.status_code = status
    response.json.return_value = body if body is not None else {"code": 0, "data": {}}
    response.headers = headers or {}
    return response


class TestFeishuResponseParser:
    def test_success_returns_full_envelope(self):
        envelope = {"code": 0, "data": {"items": [1]}}

        assert FeishuResponseParser.parse(make_response(body=envelope)) == envelope

    def test_business_error_preserves_retry_and_log_metadata(self):
        response = make_response(
            body={"code": 1254291, "msg": "write conflict"},
            headers={"X-Tt-Logid": "log-test", "Retry-After": "3"},
        )

        with pytest.raises(FeishuAPIError) as exc_info:
            FeishuResponseParser.parse(response)

        error = exc_info.value
        assert error.code == 1254291
        assert error.retryable is True
        assert error.retry_after == 3
        assert error.log_id == "log-test"
        assert str(error).startswith("Feishu API error 1254291: write conflict")

    def test_non_json_http_error_is_typed(self):
        response = make_response(status=503)
        response.json.side_effect = ValueError("bad json")

        with pytest.raises(FeishuAPIError) as exc_info:
            FeishuResponseParser.parse(response)

        assert exc_info.value.http_status == 503
        assert exc_info.value.retryable is True

    def test_non_object_server_response_preserves_retry_metadata(self):
        response = make_response(
            status=503,
            body=[],
            headers={"retry-after": "2", "x-request-id": "request-test"},
        )

        with pytest.raises(FeishuAPIError) as exc_info:
            FeishuResponseParser.parse(response)

        error = exc_info.value
        assert error.retryable is True
        assert error.retry_after == 2
        assert error.log_id == "request-test"

    def test_http_error_uses_message_fallback(self):
        response = make_response(status=400, body={"message": "bad request"})

        with pytest.raises(FeishuAPIError, match="bad request"):
            FeishuResponseParser.parse(response)

    def test_retry_after_http_date_without_date_header(self):
        retry_at = datetime.now(timezone.utc) + timedelta(seconds=60)
        response = make_response(headers={"Retry-After": format_datetime(retry_at)})

        delay = FeishuResponseParser._parse_retry_after(response)

        assert delay is not None
        assert 1 <= delay <= 60


class TestPaginator:
    def test_collects_all_pages(self):
        pages = {
            None: Page([1], "next", True, {}),
            "next": Page([2], None, False, {}),
        }

        assert Paginator[int]().collect(lambda token: pages[token]) == [1, 2]

    def test_rejects_missing_next_token(self):
        with pytest.raises(PaginationError, match="未提供 page_token"):
            Paginator[int]().collect(lambda token: Page([1], None, True, {}))

    def test_rejects_repeated_next_token(self):
        def fetch(token):
            return Page([token], "same", True, {})

        with pytest.raises(PaginationError, match="重复 page_token"):
            Paginator[str]().collect(fetch)

    def test_rejects_non_list_items(self):
        with pytest.raises(PaginationError, match="items 必须是列表"):
            Paginator[str]().collect(lambda token: Page("bad", None, False, {}))


class TestRunBatches:
    def test_stops_after_first_failed_batch(self):
        calls = []

        def processor(batch):
            calls.append(batch)
            return len(calls) == 1

        with pytest.raises(PartialBatchError) as exc_info:
            run_batches("create", [1, 2, 3, 4, 5], 2, processor)

        assert calls == [[1, 2], [3, 4]]
        assert exc_info.value.processed == 2
        assert exc_info.value.failed_batch_index == 2

    def test_requires_boolean_result_without_success_predicate(self):
        with pytest.raises(PartialBatchError) as exc_info:
            run_batches("create", [1], 1, lambda batch: (False, 90202))

        assert isinstance(exc_info.value.cause, TypeError)

    def test_accepts_explicit_success_predicate(self):
        assert run_batches(
            "create",
            [1],
            1,
            lambda batch: (True, 0),
            success=lambda result: result[0],
        ) == [(True, 0)]


def test_public_api_exports_typed_sheet_contracts():
    from api import A1Range, FormulaVerificationResult, RangeChunker, SheetMetadata

    assert A1Range.parse("sh1!A1:B2").text == "sh1!A1:B2"
    assert FormulaVerificationResult("success", False).passed is True
    assert RangeChunker(5000, 100).chunk_count(A1Range.parse("sh1!A1:CW5001")) == 4
    assert SheetMetadata("sh1").sheet_id == "sh1"


def test_removed_python_facades_are_not_exported():
    import api
    import core
    from api.sheet import SheetAPI

    assert not hasattr(api, "XTFFeishuClient")
    assert not hasattr(api, "BitableAPI")
    assert not hasattr(core, "XTFSyncEngine")
    assert not hasattr(core, "SyncConfig")
    for name in (
        "write_sheet_data",
        "append_sheet_data",
        "write_selective_columns",
        "clear_sheet_data",
    ):
        assert not hasattr(SheetAPI, name)
