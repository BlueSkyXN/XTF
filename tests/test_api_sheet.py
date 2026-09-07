#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""电子表格 API 读取契约测试。"""

from unittest.mock import Mock

import pytest

from api.sheet import SheetAPI


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


def test_get_sheet_data_encodes_token_and_a1_path_suffix():
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    response = Mock()
    response.status_code = 200
    response.headers = {}
    response.json.return_value = {
        "code": 0,
        "data": {"valueRange": {"values": [[1]]}},
    }
    client.call_api.return_value = response
    api = SheetAPI(auth, client)

    assert api.get_sheet_data("tok/../x", "数据!A1:A1") == [[1]]

    url = client.call_api.call_args.args[1]
    assert "/spreadsheets/tok%2F..%2Fx/values/%E6%95%B0%E6%8D%AE!A1%3AA1" in url


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


def test_range_projection_keeps_values_padding_and_input_order():
    api = SheetAPI(Mock(), Mock())
    data = {"D": [False, None], "B": ["001", "text "], "E": [3]}
    positions = {"E": 5, "untouched": 3, "B": 2, "D": 4}

    assert api._optimize_column_ranges(data, positions, 7) == [
        {"range": "B7:B8", "values": [["001"], ["text "]]},
        {"range": "D7:E8", "values": [[False, 3], [None, ""]]},
    ]
    assert data == {"D": [False, None], "B": ["001", "text "], "E": [3]}
    assert list(data) == ["D", "B", "E"]
    assert positions == {"E": 5, "untouched": 3, "B": 2, "D": 4}


def test_range_projection_preserves_first_name_for_duplicate_positions():
    api = SheetAPI(Mock(), Mock())
    assert api._optimize_column_ranges(
        {"first": [1], "second": [2]}, {"first": 2, "second": 2}, 1
    ) == [
        {"range": "B1:B1", "values": [[1]]},
        {"range": "B1:B1", "values": [[1]]},
    ]
    with pytest.raises(ValueError, match="未提供数据的列位置: 2"):
        api._optimize_column_ranges(
            {"selected": [1]}, {"unselected": 2, "selected": 2}, 1
        )


def test_range_projection_scans_positions_once_not_per_cell():
    class CountedPositions(dict):
        scans = 0

        def items(self):
            self.scans += 1
            return super().items()

    positions = CountedPositions(A=1, B=2, C=3)
    api = SheetAPI(Mock(), Mock())
    result = api._optimize_column_ranges(
        {"A": [1, 2], "B": [3, 4], "C": [5, 6]}, positions, 1
    )
    assert result == [{"range": "A1:C2", "values": [[1, 3, 5], [2, 4, 6]]}]
    assert positions.scans == 1


@pytest.mark.parametrize(
    ("column", "expected"),
    [(-1, "A"), (0, "A"), (1, "A"), (26, "Z"), (27, "AA"), (702, "ZZ"), (703, "AAA")],
)
def test_sheet_column_conversion_entrypoints_keep_same_result(column, expected):
    api = SheetAPI(Mock(), Mock())
    assert api.column_number_to_letter(column) == expected
    assert SheetAPI.column_number_to_letter_static(column) == expected
