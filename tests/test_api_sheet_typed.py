"""Focused tests for additive typed Sheet contracts."""

from unittest.mock import Mock

import pytest

from api.bitable_backend import MutationOutcome
from api.sheet import A1Range, FormulaVerificationResult, SheetAPI
from api.sdk import FeishuAPIError


def response(body=None, status=200):
    result = Mock()
    result.status_code = status
    result.headers = {}
    result.json.return_value = body or {"code": 0, "data": {}}
    return result


def make_api(*responses):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    client = Mock()
    client.call_api.side_effect = list(responses)
    return SheetAPI(auth, client), client


def test_a1_range_and_matrix_shape_are_strict_before_network():
    api, client = make_api()

    with pytest.raises(ValueError, match="rectangular"):
        api.write_values("token", "sh1!A1:B2", [[1, 2], [3]])
    with pytest.raises(ValueError, match="shape"):
        api.write_values("token", "sh1!A1:B2", [[1]])
    with pytest.raises(ValueError, match="non-empty"):
        api.write_values("token", "sh1!A1:A0", [])
    client.call_api.assert_not_called()


@pytest.mark.parametrize(
    "args",
    [
        ("", 1, 1, 1, 1),
        ("sh1", 0, 1, 1, 1),
        ("sh1", 2, 1, 1, 1),
        ("sh1", 1, 1, 2, 1),
    ],
)
def test_a1_range_direct_construction_rejects_invalid_bounds(args):
    with pytest.raises(ValueError):
        A1Range(*args)


def test_write_values_returns_fixed_actual_range_and_payload():
    api, client = make_api(response({"code": 0, "data": {}}))

    receipt = api.write_values("token/one", "sh1!B2:C3", [[1, 2], [3, 4]])

    assert receipt.operation == "write"
    assert receipt.actual_ranges == (A1Range.parse("sh1!B2:C3"),)
    assert receipt.outcome is MutationOutcome.ACCEPTED
    call = client.call_api.call_args
    assert "/spreadsheets/token%2Fone/values" in call.args[1]
    assert call.kwargs["json"] == {
        "valueRange": {"range": "sh1!B2:C3", "values": [[1, 2], [3, 4]]}
    }


def test_write_values_split_records_successful_leaf_ranges_only():
    too_large = FeishuAPIError(90227, "too large")
    api, client = make_api(
        too_large,
        response({"code": 0, "data": {}}),
        response({"code": 0, "data": {}}),
    )

    receipt = api.write_values("token", "sh1!A1:A4", [[1], [2], [3], [4]])

    assert [item.text for item in receipt.actual_ranges] == [
        "sh1!A1:A2",
        "sh1!A3:A4",
    ]
    assert client.call_api.call_count == 3


def test_write_values_splits_oversized_single_row_by_configured_columns():
    api, client = make_api(
        response({"code": 0, "data": {}}),
        response({"code": 0, "data": {}}),
    )
    api.write_max_cols = 2

    receipt = api.write_values("token", "sh1!A1:D1", [[1, 2, 3, 4]])

    assert [item.text for item in receipt.actual_ranges] == [
        "sh1!A1:B1",
        "sh1!C1:D1",
    ]
    assert client.call_api.call_count == 2


def test_append_without_server_range_keeps_scope_unknown():
    api, _ = make_api(response({"code": 0, "data": {}}))

    receipt = api.append_values("token", "sh1!A1:B2", [[1, 2], [3, 4]])

    assert receipt.actual_ranges == ()
    assert receipt.raw_metadata["unknown_scope"] is True
    assert receipt.readback.value == "unknown"


def test_append_respects_configured_row_chunks_and_server_actual_ranges():
    api, client = make_api(
        response(
            {
                "code": 0,
                "data": {"updates": {"updatedRange": "sh1!A10:B11"}},
            }
        ),
        response(
            {
                "code": 0,
                "data": {"updates": {"updatedRange": "sh1!A12:B12"}},
            }
        ),
    )
    api.write_max_rows = 2

    receipt = api.append_values("token", "sh1!A1:B3", [[1, 2], [3, 4], [5, 6]])

    assert [item.text for item in receipt.actual_ranges] == [
        "sh1!A10:B11",
        "sh1!A12:B12",
    ]
    assert client.call_api.call_count == 2


def test_append_accepts_snake_case_actual_range_metadata():
    api, _ = make_api(
        response(
            {
                "code": 0,
                "data": {"updates": {"actual_range": "sh1!C8:D8"}},
            }
        )
    )

    receipt = api.append_values("token", "sh1!C1:D1", [[1, 2]])

    assert receipt.actual_ranges == (A1Range.parse("sh1!C8:D8"),)


def test_typed_receipt_collects_server_update_metrics():
    api, _ = make_api(
        response(
            {
                "code": 0,
                "data": {
                    "updates": {
                        "updatedRange": "sh1!A1:B2",
                        "updatedRows": 2,
                        "updatedColumns": 2,
                        "updatedCells": 4,
                    }
                },
            }
        )
    )

    receipt = api.write_values("token", "sh1!A1:B2", [[1, 2], [3, 4]])

    assert receipt.updated_rows == 2
    assert receipt.updated_columns == 2
    assert receipt.updated_cells == 4


def test_transport_failure_returns_unknown_outcome_without_retry():
    api, client = make_api()
    client.call_api.side_effect = FeishuAPIError.from_transport("timeout")

    receipt = api.append_values("token", "sh1!A1:A1", [[1]])

    assert receipt.outcome is MutationOutcome.UNKNOWN_OUTCOME
    assert receipt.actual_ranges == ()
    client.call_api.assert_called_once()
    assert client.call_api.call_args.kwargs["retry_transport"] is False


def test_write_failure_preserves_successful_prefix_and_stops():
    api, client = make_api(
        response({"code": 0, "data": {}}),
        response({"code": 90202, "msg": "invalid range"}),
        response({"code": 0, "data": {}}),
    )
    api.write_max_rows = 1

    receipt = api.write_values("token", "sh1!A1:A3", [[1], [2], [3]])

    assert receipt.outcome is MutationOutcome.PARTIAL
    assert receipt.accepted_count == 1
    assert receipt.failed_batch_index == 2
    assert [item.text for item in receipt.actual_ranges] == ["sh1!A1:A1"]
    assert client.call_api.call_count == 2


def test_batch_update_requires_each_range_to_match_matrix():
    api, client = make_api()

    with pytest.raises(ValueError, match="shape"):
        api.batch_update_values("token", [{"range": "sh1!A1:B2", "values": [[1, 2]]}])
    client.call_api.assert_not_called()


def test_batch_update_rejects_configured_range_limit_before_network():
    api, client = make_api()
    api.write_max_rows = 1

    with pytest.raises(ValueError, match="exceeds configured write limits"):
        api.batch_update_values("token", [{"range": "sh1!A1:A2", "values": [[1], [2]]}])

    client.call_api.assert_not_called()


def test_clear_values_is_explicit_and_writes_empty_strings():
    api, client = make_api(response({"code": 0, "data": {}}))

    receipt = api.clear_values("token", "sh1!A1:B2")

    assert receipt.operation == "batch_update"
    assert receipt.actual_ranges == (A1Range.parse("sh1!A1:B2"),)
    assert client.call_api.call_args.kwargs["json"] == {
        "valueRanges": [{"range": "sh1!A1:B2", "values": [["", ""], ["", ""]]}]
    }


def test_query_sheets_returns_typed_metadata():
    api, client = make_api(
        response(
            {
                "code": 0,
                "data": {
                    "sheets": [
                        {
                            "sheet_id": "sh1",
                            "title": "Orders",
                            "hidden": False,
                            "grid_properties": {"row_count": 10},
                        }
                    ]
                },
            }
        )
    )

    sheets = api.query_sheets("token/one")

    assert sheets[0].sheet_id == "sh1"
    assert sheets[0].title == "Orders"
    assert sheets[0].grid_properties["row_count"] == 10
    assert "/spreadsheets/token%2Fone/sheets/query" in client.call_api.call_args.args[1]


def test_query_sheets_invalid_metadata_fails_instead_of_looking_empty():
    api, _ = make_api(
        response(
            {
                "code": 0,
                "data": {"sheets": [{"sheet_id": "sh1", "hidden": "false"}]},
            }
        )
    )

    with pytest.raises(FeishuAPIError, match="hidden must be boolean"):
        api.query_sheets("token")


@pytest.mark.parametrize(
    ("status", "has_more", "passed"),
    [
        ("success", False, True),
        ("success", True, False),
        ("errors_found", False, False),
        ("partial", False, False),
    ],
)
def test_verify_formulas_requires_success_and_no_more(status, has_more, passed):
    api, client = make_api(
        response(
            {
                "code": 0,
                "data": {
                    "output": '{"status": "%s", "has_more": %s, "total_errors": 0}'
                    % (status, str(has_more).lower())
                },
            }
        )
    )

    result = api.verify_formulas("token", ["sh1"], ["A2:B4"])

    assert isinstance(result, FormulaVerificationResult)
    assert result.passed is passed
    assert client.call_api.call_args.kwargs["json"]["tool_name"] == "verify_formula"
    assert isinstance(client.call_api.call_args.kwargs["json"]["input"], str)


def test_verify_formulas_malformed_output_is_invalid_response():
    api, _ = make_api(response({"code": 0, "data": {"output": "not-json"}}))

    result = api.verify_formulas("token", ["sh1"], ["A2:B4"])

    assert result.status == "invalid_response"
    assert result.passed is False


@pytest.mark.parametrize(
    "output",
    [
        '{"status":"success"}',
        '{"status":"unknown","has_more":false}',
        '{"status":"success","has_more":"false"}',
    ],
)
def test_verify_formulas_rejects_missing_or_invalid_status_contract(output):
    api, _ = make_api(response({"code": 0, "data": {"output": output}}))

    result = api.verify_formulas("token", ["sh1"], ["A2:B4"])

    assert result.status == "invalid_response"
    assert result.passed is False


def test_verify_formulas_rejects_prefixed_range_before_network():
    api, client = make_api()

    with pytest.raises(ValueError, match="must not include sheet prefix"):
        api.verify_formulas("token", ["sh1"], ["sh1!A2:B4"])

    client.call_api.assert_not_called()


def test_verify_formulas_requires_at_least_one_sheet_id_before_network():
    api, client = make_api()

    with pytest.raises(ValueError, match="non-empty sequence"):
        api.verify_formulas("token", [], ["A2:B4"])

    client.call_api.assert_not_called()
