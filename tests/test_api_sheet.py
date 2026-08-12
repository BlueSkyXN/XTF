#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""电子表格 API 读取契约测试。"""

from unittest.mock import Mock

import pytest

from api.sheet import SheetAPI
from api.sdk import FeishuAPIError


def test_get_sheet_data_uses_single_network_read():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {
        "code": 0,
        "data": {"valueRange": {"values": [["A"], [1]]}},
    }
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api.get_sheet_data("sheet-token", "sh1!A1:A2") == [["A"], [1]]
    client.call_api.assert_called_once()


def test_get_sheet_data_passes_render_options():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {"code": 0, "data": {"valueRange": {"values": []}}}
    client.call_api.return_value = response
    api = SheetAPI(
        auth,
        client,
        value_render_option="Formula",
        datetime_render_option="FormattedString",
    )

    api.get_sheet_data("sheet-token", "sh1!A1:A2")

    assert client.call_api.call_args.kwargs["params"] == {
        "valueRenderOption": "Formula",
        "dateTimeRenderOption": "FormattedString",
    }


def test_get_sheet_data_rejects_invalid_range_before_network():
    auth = Mock()
    client = Mock()
    api = SheetAPI(auth, client)

    with pytest.raises(Exception, match="范围格式无效"):
        api.get_sheet_data("sheet-token", "A1:A2")
    client.call_api.assert_not_called()


def test_write_selective_columns_rejects_invalid_local_range_before_network():
    auth = Mock()
    client = Mock()
    api = SheetAPI(auth, client)

    result = api.write_selective_columns(
        "sheet-token",
        "bad!sheet",
        {"ID": [1]},
        {"ID": 1},
        rate_limit_delay=0,
    )

    assert result is False
    client.call_api.assert_not_called()


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (200, {"code": 90202, "msg": "invalid range"}),
        (503, {"code": 0, "msg": "busy"}),
    ],
)
def test_validate_range_size_keeps_false_contract_for_api_errors(status, body):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = status
    response.headers = {}
    response.json.return_value = body
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api._validate_range_size("sheet-token", "sh1!A1:A1") is False


def test_validate_range_size_accepts_successful_probe():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {"code": 0, "data": {}}
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api._validate_range_size("sheet-token", "sh1!A1:A1") is True


def test_optimize_column_ranges_with_zero_gap_keeps_columns_separate():
    api = SheetAPI(Mock(), Mock())

    ranges = api._optimize_column_ranges(
        {"B": [1], "C": [2], "E": [3]},
        {"B": 2, "C": 3, "E": 5},
        start_row=2,
        max_gap=0,
    )

    assert [item["range"] for item in ranges] == ["B2:B2", "C2:C2", "E2:E2"]


def test_optimize_column_ranges_never_crosses_unselected_columns():
    api = SheetAPI(Mock(), Mock())

    ranges = api._optimize_column_ranges(
        {"B": [1], "D": [2], "E": [3]},
        {"B": 2, "D": 4, "E": 5},
        start_row=2,
        max_gap=2,
    )

    assert [item["range"] for item in ranges] == ["B2:B2", "D2:E2"]
    assert [item["values"] for item in ranges] == [[[1]], [[2, 3]]]


@pytest.mark.parametrize(
    ("method_name", "args"),
    [
        ("_write_single_batch", ("sheet-token", "sh1!A1:A1", [[1]])),
        ("_append_single_batch", ("sheet-token", "sh1", [[1]])),
    ],
)
def test_sheet_boolean_tuple_writes_keep_false_and_error_code(method_name, args):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {"X-Tt-Logid": "log-write"}
    response.json.return_value = {"code": 90202, "msg": "invalid range"}
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert getattr(api, method_name)(*args) == (False, 90202)


def test_sheet_style_write_keeps_false_contract_for_typed_error():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 503
    response.headers = {"Retry-After": "2"}
    response.json.return_value = {"code": 0, "msg": "busy"}
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api._set_style_single_batch("sheet-token", ["sh1!A1:A1"], {}) is False


def test_batch_update_clear_preserves_legacy_ignored_error():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {"code": 90202, "msg": "invalid range"}
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api._batch_update_ranges("sheet-token", [], is_clear=True) == (True, 0)


@pytest.mark.parametrize("method_name", ["_write_single_batch", "_append_single_batch"])
def test_sheet_malformed_json_preserves_none_error_code(method_name):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.side_effect = ValueError("bad json")
    client.call_api.return_value = response
    api = SheetAPI(auth, client)
    args = (
        ("sheet-token", "sh1!A1:A1", [[1]])
        if method_name == "_write_single_batch"
        else ("sheet-token", "sh1", [[1]])
    )

    assert getattr(api, method_name)(*args) == (False, None)


def test_sheet_boolean_write_keeps_false_contract_for_transport_error():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    client.call_api.side_effect = FeishuAPIError.from_transport("offline")
    api = SheetAPI(auth, client)

    assert api._write_single_batch("sheet-token", "sh1!A1:A1", [[1]]) == (
        False,
        None,
    )
