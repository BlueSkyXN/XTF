from unittest.mock import Mock

import pytest

from api.bitable_backend import (
    CanonicalRecord,
    FieldKind,
    IncompleteReadError,
    MutationOutcome,
)
from api.bitable_v3 import BaseV3Backend, BaseV3MatrixError
from api.sdk import FeishuAPIError


def response(data, status=200):
    value = Mock()
    value.status_code = status
    value.headers = {}
    value.json.return_value = data
    return value


def make_backend(responses):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    transport = Mock()
    transport.call_api.side_effect = responses
    return BaseV3Backend(auth, transport), transport


def matrix(
    rows,
    *,
    fields=("Name",),
    field_ids=("fld_name",),
    types=("text",),
    has_more=False,
    timezone="Asia/Shanghai",
):
    return {
        "code": 0,
        "data": {
            "timezone": timezone,
            "fields": list(fields),
            "field_id_list": list(field_ids),
            "field_type_list": list(types),
            "record_id_list": [f"rec_{i}" for i in range(len(rows))],
            "data": rows,
            "has_more": has_more,
        },
    }


def fields_response(*fields, total=None):
    return response(
        {
            "code": 0,
            "data": {
                "fields": [
                    {"id": field_id, "name": name, "type": field_type}
                    for field_id, name, field_type in fields
                ],
                "total": len(fields) if total is None else total,
            },
        }
    )


def test_v3_matrix_reads_records_and_offsets_by_actual_rows():
    api, transport = make_backend(
        [
            response(matrix([["A"]], has_more=True)),
            response(matrix([["B"]], has_more=False)),
        ]
    )
    result = api.list_records("base", "table")
    assert [record.fields["Name"] for record in result.records] == ["A", "B"]
    assert result.fields[0].kind is FieldKind.TEXT
    assert transport.call_api.call_args_list[1].kwargs["params"]["offset"] == 1


def test_v3_list_projection_resolves_field_names_to_field_ids():
    api, transport = make_backend(
        [
            fields_response(("fld_name", "Name", "text")),
            response(matrix([["A"]])),
        ]
    )

    api.list_records("base", "table", ["Name"])

    assert transport.call_api.call_args_list[1].kwargs["params"]["field_id"] == [
        "fld_name"
    ]


def test_v3_matrix_rejects_parallel_array_mismatch_and_missing_has_more():
    api, _ = make_backend(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "timezone": "Asia/Shanghai",
                        "fields": ["Name"],
                        "field_id_list": [],
                        "field_type_list": ["text"],
                        "record_id_list": [],
                        "data": [],
                        "has_more": False,
                    },
                }
            )
        ]
    )
    with pytest.raises(BaseV3MatrixError, match="lengths differ"):
        api.list_records("base", "table")

    api, _ = make_backend(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "timezone": "Asia/Shanghai",
                        "fields": ["Name"],
                        "field_id_list": ["fld_name"],
                        "field_type_list": ["text"],
                        "record_id_list": [],
                        "data": [],
                    },
                }
            )
        ]
    )
    with pytest.raises(BaseV3MatrixError, match="has_more"):
        api.list_records("base", "table")


def test_v3_matrix_rejects_ragged_rows_and_schema_drift():
    api, _ = make_backend([response(matrix([["A", "extra"]]))])
    with pytest.raises(BaseV3MatrixError, match="schema has"):
        api.list_records("base", "table")

    api, _ = make_backend(
        [
            response(matrix([["A"]], has_more=True)),
            response(
                matrix(
                    [[1]], fields=("Name",), field_ids=("fld_name",), types=("number",)
                )
            ),
        ]
    )
    with pytest.raises(BaseV3MatrixError, match="changed"):
        api.list_records("base", "table")


def test_v3_matrix_allows_field_order_change_when_schema_identity_is_stable():
    api, _ = make_backend(
        [
            response(
                matrix(
                    [["A", 1]],
                    fields=("Name", "Count"),
                    field_ids=("fld_name", "fld_count"),
                    types=("text", "number"),
                    has_more=True,
                )
            ),
            response(
                matrix(
                    [[2, "B"]],
                    fields=("Count", "Name"),
                    field_ids=("fld_count", "fld_name"),
                    types=("number", "text"),
                )
            ),
        ]
    )
    result = api.list_records("base", "table")
    assert result.records[1].fields == {"Count": 2, "Name": "B"}


def test_v3_matrix_decodes_user_link_ids_and_preserves_unsupported_readonly():
    api, _ = make_backend(
        [
            response(
                matrix(
                    [[[{"id": "ou_1", "name": "Alice"}], [{"id": "rec2"}], "raw"]],
                    fields=("Owner", "Related", "Future"),
                    field_ids=("fld_owner", "fld_link", "fld_future"),
                    types=("user", "link", "future_type"),
                )
            )
        ]
    )

    result = api.list_records("base", "table")

    assert result.records[0].fields == {
        "Owner": ["ou_1"],
        "Related": ["rec2"],
        "Future": "raw",
    }
    assert result.fields[2].kind is FieldKind.UNSUPPORTED
    assert result.fields[2].writable is False


def test_v3_second_page_failure_carries_incomplete_partial_result():
    api, _ = make_backend(
        [
            response(matrix([["A"]], has_more=True)),
            response(
                {
                    "code": 0,
                    "data": {
                        "timezone": "Asia/Shanghai",
                        "fields": ["Name"],
                        "field_id_list": ["fld_name"],
                        "field_type_list": ["text"],
                        "record_id_list": [],
                        "data": [],
                    },
                }
            ),
        ]
    )

    with pytest.raises(IncompleteReadError) as exc_info:
        api.list_records("base", "table")

    partial = exc_info.value.partial_result
    assert partial.complete is False
    assert [record.fields["Name"] for record in partial.records] == ["A"]


def test_v3_fields_use_total_pagination_and_reject_incomplete_pages():
    api, transport = make_backend(
        [
            fields_response(("fld_a", "A", "text"), total=2),
            fields_response(("fld_b", "B", "number"), total=2),
        ]
    )
    fields = api.list_fields("base", "table")
    assert [field.name for field in fields] == ["A", "B"]
    assert transport.call_api.call_args_list[1].kwargs["params"]["offset"] == 1

    api, _ = make_backend([fields_response(total=1)])
    with pytest.raises(BaseV3MatrixError, match="empty page"):
        api.list_fields("base", "table")


def test_v3_batch_wire_shapes_and_ignored_fields():
    api, transport = make_backend(
        [
            fields_response(("fld_name", "Name", "text")),
            response(
                {
                    "code": 0,
                    "data": {
                        "record_id_list": ["rec_new"],
                        "ignored_fields": [{"id": "fld_x", "reason": "readonly"}],
                    },
                }
            ),
            response({"code": 0, "data": {}}),
            response({"code": 0, "data": {"record_not_found": ["rec_missing"]}}),
        ]
    )
    create = api.batch_create("base", "table", [CanonicalRecord(None, {"Name": "A"})])
    update = api.batch_update(
        "base", "table", [CanonicalRecord("rec_new", {"Name": "B"})]
    )
    delete = api.batch_delete("base", "table", ["rec_missing"])
    assert create.outcome is MutationOutcome.PARTIAL
    assert update.outcome is MutationOutcome.ACCEPTED
    assert delete.record_not_found == ("rec_missing",)
    assert transport.call_api.call_args_list[1].kwargs["json"] == {
        "create_records": [{"Name": "A"}]
    }
    assert transport.call_api.call_args_list[2].kwargs["json"] == {
        "update_records": {"rec_new": {"Name": "B"}}
    }
    assert transport.call_api.call_args_list[3].kwargs["json"] == {
        "record_id_list": ["rec_missing"]
    }


def test_v3_batch_get_uses_select_fields():
    api, transport = make_backend([response(matrix([["A"]]))])
    api.batch_get_records("base", "table", ["rec_0"], ["Name"])
    assert transport.call_api.call_args.kwargs["json"] == {
        "record_id_list": ["rec_0"],
        "select_fields": ["Name"],
    }


def test_v3_missing_schema_is_read_before_mutation_and_unknown_field_is_rejected():
    api, transport = make_backend([fields_response(("fld_name", "Name", "text"))])
    with pytest.raises(ValueError, match="unknown field"):
        api.batch_create("base", "table", [CanonicalRecord(None, {"Missing": "A"})])
    transport.call_api.assert_called_once()


@pytest.mark.parametrize(
    "field_type",
    [
        "attachment",
        "formula",
        "lookup",
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
        "auto_number",
        "not_support",
    ],
)
def test_v3_read_only_attachment_and_unsupported_fields_fail_closed(field_type):
    api, transport = make_backend([fields_response(("fld", "Value", field_type))])
    with pytest.raises(ValueError):
        api.batch_create("base", "table", [CanonicalRecord(None, {"Value": "x"})])
    transport.call_api.assert_called_once()


def test_v3_create_field_transport_failure_is_unknown_without_replay():
    api, transport = make_backend([])
    transport.call_api.side_effect = FeishuAPIError.from_transport("connection lost")
    receipt = api.create_field("base", "table", "New", "text")
    assert receipt.outcome is MutationOutcome.UNKNOWN_OUTCOME
    assert receipt.readback.value == "unknown"
    transport.call_api.assert_called_once()


@pytest.mark.parametrize(
    ("field_type", "properties", "value"),
    [
        ("location", {}, {"lat": 1}),
        ("select", {"multiple": False}, ["one", "two"]),
        ("user", {}, [None]),
        ("link", {}, [{"bad": "id"}]),
    ],
)
def test_v3_invalid_typed_values_fail_before_mutation(field_type, properties, value):
    api, transport = make_backend(
        [
            response(
                {
                    "code": 0,
                    "data": {
                        "fields": [
                            {
                                "id": "fld",
                                "name": "Value",
                                "type": field_type,
                                **properties,
                            }
                        ],
                        "total": 1,
                    },
                }
            )
        ]
    )
    with pytest.raises(ValueError):
        api.batch_create("base", "table", [CanonicalRecord(None, {"Value": value})])
    transport.call_api.assert_called_once()


@pytest.mark.parametrize("operation", ["batch_create", "batch_update", "batch_delete"])
def test_v3_batches_reject_more_than_200_before_request(operation):
    api, transport = make_backend([])
    items = [CanonicalRecord(None, {}) for _ in range(201)]
    with pytest.raises(ValueError, match="cannot exceed 200"):
        if operation == "batch_create":
            api.batch_create("base", "table", items)
        elif operation == "batch_update":
            api.batch_update(
                "base", "table", [CanonicalRecord(f"rec_{i}", {}) for i in range(201)]
            )
        else:
            api.batch_delete("base", "table", [f"rec_{i}" for i in range(201)])
    transport.call_api.assert_not_called()


def test_v3_unknown_code_and_transport_failure_never_fallback():
    api, transport = make_backend([response({"code": 999, "msg": "denied"})])
    with pytest.raises(FeishuAPIError) as exc_info:
        api.list_records("base", "table")
    assert exc_info.value.code == 999

    api, transport = make_backend([])
    api._field_cache[("base", "table")] = (
        api._field_schema("Name", "fld_name", "text", {}),
    )
    transport.call_api.side_effect = FeishuAPIError.from_transport("timeout")
    receipt = api.batch_create("base", "table", [CanonicalRecord(None, {"Name": "A"})])
    assert receipt.outcome is MutationOutcome.UNKNOWN_OUTCOME
    transport.call_api.assert_called_once()
