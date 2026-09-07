"""Regression cases derived from larksuite/cli 7fd6ef3c0718 (2026-09-04).

The response fixtures below are authored examples, NOT recordings of live Feishu.
See docs/API_CONTRACTS.md for the exact sources and untested server behavior.
"""

from datetime import datetime, timezone
from unittest.mock import Mock

import pandas as pd
import pytest

from api.bitable_backend import (
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    MutationReceipt,
    RecordReadResult,
)
from api.bitable_v3 import BaseV3Backend, BaseV3MatrixError
from core.config import SyncMode, TargetType
from core.converter import DataConverter
from tests.test_plan import make_file_bitable_engine


def make_backend(data):
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer test"}
    transport = Mock()
    response = Mock(status_code=200, headers={})
    response.json.return_value = {"code": 0, "data": data}
    transport.call_api.return_value = response
    return BaseV3Backend(auth, transport), transport


def matrix(ids=("rec_a",), rows=(("A",),), **overrides):
    data = {
        "timezone": "Asia/Shanghai",
        "fields": ["Name"],
        "field_id_list": ["fld_name"],
        "field_type_list": ["text"],
        "record_id_list": list(ids),
        "data": [list(row) for row in rows],
        "has_more": False,
    }
    data.update(overrides)
    return data


@pytest.mark.parametrize(
    "kind,raw,value,expected",
    [
        (FieldKind.DATETIME, "datetime", "2026-03-24T10:00:00+08:00", 1774317600000),
        (
            FieldKind.DATETIME,
            "datetime",
            datetime(2026, 3, 24, 2, tzinfo=timezone.utc),
            1774317600000,
        ),
        (FieldKind.SELECT, "select", "Todo", ["Todo"]),
    ],
)
def test_converter_to_v3_wire(kind, raw, value, expected):
    """Exercise the real converter followed by the real API encoder, not two mocks."""
    schema = FieldSchema("fld_value", "Value", kind, raw_type=raw)
    api, transport = make_backend({"record_id_list": ["rec_new"]})
    api._field_cache[("base", "table")] = (schema,)
    converted = DataConverter(TargetType.BITABLE).df_to_records(
        pd.DataFrame({"Value": [value]}), {"Value": schema}
    )
    api.batch_create("base", "table", [CanonicalRecord(None, converted[0]["fields"])])
    assert transport.call_api.call_args.kwargs["json"] == {
        "create_records": [{"Value": expected}]
    }


@pytest.mark.parametrize("type_code,multiple", [(3, False), (4, True)])
def test_create_select_preserves_cardinality(type_code, multiple):
    api, transport = make_backend({})
    api.create_field("base", "table", "Status", type_code)
    assert transport.call_api.call_args.kwargs["json"] == {
        "name": "Status",
        "type": "select",
        "multiple": multiple,
    }


def test_duplicate_update_ids_rejected_before_network():
    api, transport = make_backend({})
    api._field_cache[("base", "table")] = (
        FieldSchema("fld_name", "Name", FieldKind.TEXT, raw_type="text"),
    )
    with pytest.raises(ValueError, match="duplicate"):
        api.batch_update(
            "base",
            "table",
            [
                CanonicalRecord("rec_a", {"Name": "A"}),
                CanonicalRecord("rec_a", {"Name": "B"}),
            ],
        )
    transport.call_api.assert_not_called()


@pytest.mark.parametrize("ids", [["rec_a", "rec_a"], "rec_a", []])
def test_invalid_id_selection_not_sent(ids):
    api, transport = make_backend(matrix())
    with pytest.raises(ValueError):
        api.batch_get_records("base", "table", ids)
    transport.call_api.assert_not_called()


@pytest.mark.parametrize("kind", [FieldKind.USER, FieldKind.GROUP_CHAT])
def test_single_id_field_rejects_multiple_values(kind):
    api, _ = make_backend({})
    with pytest.raises(ValueError):
        api._encode_value(
            FieldSchema("fld", "Owner", kind, multiple=False), ["one", "two"]
        )


@pytest.mark.parametrize("kind", [FieldKind.USER, FieldKind.GROUP_CHAT, FieldKind.LINK])
def test_empty_object_id_rejected(kind):
    api, _ = make_backend({})
    with pytest.raises(ValueError):
        api._encode_value(FieldSchema("fld", "Owner", kind), [{"id": " "}])


@pytest.mark.parametrize(
    "data",
    [
        matrix(has_more=True),
        matrix(ids=(), rows=()),  # requested ID missing without explanation
        matrix(ids=("rec_other",)),
        matrix(record_not_found=["rec_a"]),  # simultaneously found and absent
        matrix(ids=(), rows=(), record_not_found=["rec_a", "rec_a"]),
        matrix(ids=("rec_a", "rec_a"), rows=(("A",), ("B",))),
        matrix(
            fields=["Name", "Name"],
            field_id_list=["fld1", "fld2"],
            field_type_list=["text", "text"],
            rows=(("A", "B"),),
        ),
        matrix(
            fields=["Name", "Other"],
            field_id_list=["fld1", "fld1"],
            field_type_list=["text", "text"],
            rows=(("A", "B"),),
        ),
    ],
)
def test_incomplete_or_ambiguous_batch_read_not_declared_complete(data):
    api, _ = make_backend(data)
    with pytest.raises(BaseV3MatrixError):
        api.batch_get_records("base", "table", ["rec_a"])


def test_explicit_not_found_is_complete():
    api, _ = make_backend(matrix(ids=(), rows=(), record_not_found=["rec_a"]))
    result = api.batch_get_records("base", "table", ["rec_a"])
    assert result.complete and result.record_not_found == ("rec_a",)


def test_batch_get_projection_limit_checked_before_network():
    api, transport = make_backend(matrix())
    with pytest.raises(ValueError):
        api.batch_get_records("base", "table", ["rec_a"], [f"C{i}" for i in range(101)])
    transport.call_api.assert_not_called()


@pytest.mark.parametrize("backend,limit", [("base_v3", 200), ("bitable_v1", 100)])
@pytest.mark.parametrize("operation", ["create", "update", "delete"])
def test_service_readback_respects_separate_get_limit(backend, limit, operation):
    engine = make_file_bitable_engine(
        SyncMode.FULL, backend=backend, verify_remote_writes=True
    )
    engine.api.max_batch_get_size = limit
    fields = (FieldSchema("fld", "Name", FieldKind.TEXT, raw_type="text"),)
    ids = [f"rec_{i}" for i in range(limit + 1)]
    requested = (
        ids
        if operation == "delete"
        else [
            CanonicalRecord(None if operation == "create" else rid, {"Name": "A"})
            for rid in ids
        ]
    )
    receipts = [
        MutationReceipt(
            operation=f"batch_{operation}",
            backend=BitableBackendKind(backend),
            requested_count=len(ids),
            accepted_count=len(ids),
            record_ids=tuple(ids) if operation == "create" else (),
        )
    ]
    sizes = []

    def read(app, table, record_ids, field_names=None):
        assert len(record_ids) <= limit, "readback exceeded backend limit"
        sizes.append(len(record_ids))
        return RecordReadResult(
            records=(
                ()
                if operation == "delete"
                else tuple(CanonicalRecord(rid, {"Name": "A"}) for rid in record_ids)
            ),
            fields=fields,
            complete=True,
            backend=BitableBackendKind(backend),
            record_not_found=tuple(record_ids) if operation == "delete" else (),
        )

    engine.api.batch_get_records.side_effect = read
    assert engine._verify_bitable_mutation(operation, requested, receipts)
    assert sizes == [limit, 1]
    assert receipts[0].verified_count == limit + 1


def test_service_readback_splits_wide_projection():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    engine.api.max_batch_get_size = 200
    names = [f"C{i}" for i in range(101)]
    requested = [CanonicalRecord("rec_a", dict.fromkeys(names, "A"))]
    receipts = [
        MutationReceipt(
            operation="batch_update",
            backend=BitableBackendKind.BASE_V3,
            requested_count=1,
            accepted_count=1,
        )
    ]
    sizes = []

    def read(app, table, ids, field_names=None):
        assert len(field_names) <= 100
        sizes.append(len(field_names))
        return RecordReadResult(
            records=(CanonicalRecord("rec_a", dict.fromkeys(field_names, "A")),),
            fields=tuple(
                FieldSchema(name, name, FieldKind.TEXT, raw_type="text")
                for name in field_names
            ),
            complete=True,
            backend=BitableBackendKind.BASE_V3,
        )

    engine.api.batch_get_records.side_effect = read
    assert engine._verify_bitable_mutation("update", requested, receipts)
    assert sizes == [100, 1]


def test_paginated_read_rejects_record_repeated_on_next_page():
    api, transport = make_backend({})
    transport.call_api.return_value.json.side_effect = [
        {"code": 0, "data": matrix(has_more=True)},
        {"code": 0, "data": matrix()},
    ]
    with pytest.raises(BaseV3MatrixError, match="duplicate"):
        api.list_records("base", "table")


@pytest.mark.parametrize("duplicate", ["id", "name"])
def test_duplicate_field_identity_rejected_in_schema_read(duplicate):
    a = {"id": "fld_a", "name": "A", "type": "text"}
    b = {"id": "fld_b", "name": "B", "type": "text"}
    b[duplicate] = a[duplicate]
    api, _ = make_backend({"fields": [a, b], "total": 2})
    with pytest.raises(BaseV3MatrixError, match="duplicate"):
        api.list_fields("base", "table")
