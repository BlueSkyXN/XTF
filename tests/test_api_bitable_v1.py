from unittest.mock import Mock

import pytest

from api.bitable_v1 import BitableV1Backend
from api.bitable_backend import (
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    MutationOutcome,
)
from api.sdk import FeishuAPIError


def response(data, status=200):
    value = Mock()
    value.status_code = status
    value.headers = {}
    value.json.return_value = {"code": 0, "data": data}
    return value


def fields_response(*fields):
    return response(
        {
            "items": [
                {
                    "field_id": field_id,
                    "field_name": name,
                    "type": field_type,
                }
                for field_id, name, field_type in fields
            ],
            "has_more": False,
        }
    )


def make_backend(responses):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    transport = Mock()
    transport.call_api.side_effect = responses
    return BitableV1Backend(auth, transport), transport


def test_v1_list_records_keeps_page_token_and_returns_typed_schema():
    api, transport = make_backend(
        [
            response(
                {
                    "items": [{"field_id": "fld_1", "field_name": "Name", "type": 1}],
                    "has_more": False,
                }
            ),
            response(
                {
                    "items": [{"record_id": "rec_1", "fields": {"Name": "A"}}],
                    "has_more": True,
                    "page_token": "p2",
                }
            ),
            response(
                {
                    "items": [{"record_id": "rec_2", "fields": {"Name": "B"}}],
                    "has_more": False,
                }
            ),
        ]
    )
    result = api.list_records("app", "table", ["Name"])
    assert result.backend is BitableBackendKind.BITABLE_V1
    assert result.complete is True
    assert [record.record_id for record in result.records] == ["rec_1", "rec_2"]
    assert result.fields[0].kind is FieldKind.TEXT
    assert transport.call_api.call_args_list[2].kwargs["params"]["page_token"] == "p2"


def test_v1_batch_get_uses_documented_endpoint_and_decodes_canonical_ids():
    api, transport = make_backend(
        [
            fields_response(
                ("fld_name", "Name", 1),
                ("fld_user", "Owner", 11),
                ("fld_link", "Related", 18),
            ),
            response(
                {
                    "records": [
                        {
                            "record_id": "rec1",
                            "fields": {
                                "Name": [{"text": "A", "type": "text"}],
                                "Owner": [{"id": "ou_1", "name": "Alice"}],
                                "Related": {"link_record_ids": ["rec2"]},
                            },
                        }
                    ],
                    "absent_record_ids": ["rec_missing"],
                    "forbidden_record_ids": [],
                }
            ),
        ]
    )
    api.list_fields("app", "table")

    result = api.batch_get_records("app", "table", ["rec1", "rec_missing"])

    assert result.records[0].fields == {
        "Name": "A",
        "Owner": ["ou_1"],
        "Related": ["rec2"],
    }
    assert result.record_not_found == ("rec_missing",)
    assert transport.call_api.call_args_list[-1].args[1].endswith("/records/batch_get")
    assert transport.call_api.call_args_list[-1].kwargs["json"] == {
        "record_ids": ["rec1", "rec_missing"],
        "user_id_type": "open_id",
    }


def test_v1_batch_create_reuses_one_uuid_token_and_returns_receipt():
    api, transport = make_backend(
        [
            fields_response(("fld_name", "Name", 1)),
            response({"records": [], "record_id_list": ["rec_new"]}),
        ]
    )
    receipt = api.batch_create("app", "table", [CanonicalRecord(None, {"Name": "A"})])
    assert receipt.outcome is MutationOutcome.ACCEPTED
    assert receipt.record_ids == ("rec_new",)
    params = transport.call_api.call_args_list[1].kwargs["params"]
    assert params["client_token"]
    assert params["user_id_type"] == "open_id"


def test_v1_batch_update_requires_record_ids_and_limits_batch():
    api, _ = make_backend([])
    with pytest.raises(ValueError, match="require record_id"):
        api.batch_update("app", "table", [CanonicalRecord(None, {"Name": "A"})])
    with pytest.raises(ValueError, match="cannot exceed"):
        api.batch_delete("app", "table", [f"rec_{i}" for i in range(501)])


def test_v1_transport_failure_is_unknown_outcome_without_replay():
    api, transport = make_backend([])
    api._field_cache[("app", "table")] = tuple(
        api._field_schema({"field_id": "fld_name", "field_name": "Name", "type": 1})
        for _ in [0]
    )
    transport.call_api.side_effect = FeishuAPIError.from_transport("connection lost")
    receipt = api.batch_create("app", "table", [CanonicalRecord(None, {"Name": "A"})])
    assert receipt.outcome is MutationOutcome.UNKNOWN_OUTCOME
    transport.call_api.assert_called_once()


def test_v1_missing_schema_is_read_before_mutation_and_unknown_field_is_rejected():
    api, transport = make_backend([fields_response(("fld_name", "Name", 1))])
    with pytest.raises(ValueError, match="unknown field"):
        api.batch_create("app", "table", [CanonicalRecord(None, {"Missing": "A"})])
    transport.call_api.assert_called_once()


@pytest.mark.parametrize("field_type", [17, 20, 19, 1001, 1002, 1003, 1004, 1005, 24])
def test_v1_read_only_attachment_and_unsupported_fields_fail_closed(field_type):
    api, transport = make_backend([fields_response(("fld", "Value", field_type))])
    with pytest.raises(ValueError):
        api.batch_create("app", "table", [CanonicalRecord(None, {"Value": "x"})])
    transport.call_api.assert_called_once()
