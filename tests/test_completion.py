"""Whole-plan validation and delayed visibility through real codecs/service code."""

from dataclasses import replace
from unittest.mock import Mock

import pandas as pd
import pytest

from api import FeishuAPIError, FieldKind, FieldSchema, CanonicalRecord, MutationReceipt
from api.bitable_backend import BitableBackendKind, RecordReadResult
from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from core.config import SyncMode, TargetType
from core.converter import DataConverter
from core.plan import (
    CreateRecordsAction,
    DeleteRecordsAction,
    ExecutionPlan,
    OutcomeStatus,
    UpdateRecordsAction,
)
from core.verification import cells_equal, sheet_values_equal, wait_for_readback
from tests.conftest import make_runtime_config
from tests.test_plan import make_file_bitable_engine
from tests.test_service import make_sheet_engine


def result(records=(), *, fields=(), missing=(), complete=True):
    return RecordReadResult(
        tuple(records),
        tuple(fields),
        complete,
        BitableBackendKind.BASE_V3,
        record_not_found=tuple(missing),
        timezone="Asia/Shanghai",
    )


def test_wait_reads_until_visible_with_no_wall_clock_sleep(readback_clock):
    check = Mock(side_effect=[False, False, True])
    update = Mock()
    observed = wait_for_readback(check, timeout=10, interval=0.5, on_wait=update)
    assert (
        observed.verified and observed.attempts == 3 and observed.elapsed_seconds == 1.5
    )
    update.assert_called_once_with()


def test_wait_last_probe_at_window_end(readback_clock):
    check = Mock(return_value=False)
    observed = wait_for_readback(check, timeout=1, interval=2)
    assert not observed.verified and observed.attempts == 2
    assert observed.elapsed_seconds == 1


def test_zero_wait_means_one_read(readback_clock):
    check = Mock(return_value=False)
    observed = wait_for_readback(check, timeout=0, interval=0.5)
    assert not observed.verified and observed.attempts == 1
    assert readback_clock[0] == 0


@pytest.mark.parametrize("kind,retryable", [("invalid_response", True), ("api", False)])
def test_wait_does_not_hide_permanent_errors(kind, retryable):
    check = Mock(
        side_effect=FeishuAPIError(1, "rejected", kind=kind, retryable=retryable)
    )
    with pytest.raises(FeishuAPIError):
        wait_for_readback(check, timeout=10, interval=0.5)
    assert check.call_count == 1


def test_wait_respects_retry_after(readback_clock):
    check = Mock(
        side_effect=[
            FeishuAPIError(429, "limited", retryable=True, retry_after=3),
            True,
        ]
    )
    assert wait_for_readback(check, timeout=10, interval=0.5).verified
    assert readback_clock[0] == 3


def test_wait_does_not_break_retry_after_to_fit_deadline(readback_clock):
    check = Mock(
        side_effect=FeishuAPIError(429, "limited", retryable=True, retry_after=30)
    )
    assert not wait_for_readback(check, timeout=1, interval=0.5).verified
    assert check.call_count == 1 and readback_clock[0] == 0


@pytest.mark.parametrize(
    "key,value",
    [
        ("verify_timeout_seconds", -1),
        ("verify_timeout_seconds", 301),
        ("verify_timeout_seconds", float("nan")),
        ("verify_timeout_seconds", True),
        ("verify_interval_seconds", 0),
        ("verify_interval_seconds", float("inf")),
        ("verify_interval_seconds", False),
    ],
)
def test_invalid_wait_configuration(key, value):
    with pytest.raises(ValueError):
        make_runtime_config(**{key: value})


@pytest.mark.parametrize(
    "kind,left,right,equal",
    [
        (FieldKind.TEXT, " A ", "A", False),
        (FieldKind.TEXT, "0001", "1", False),
        (FieldKind.NUMBER, 1, 1.0, True),
        (FieldKind.NUMBER, True, 1, False),
        (FieldKind.NUMBER, 1.01, 1.0101, False),
        (FieldKind.CHECKBOX, False, 0, False),
        (FieldKind.DATETIME, "2026-03-24T10:00:00+08:00", 1774317600000, True),
        (FieldKind.DATETIME, "2026-03-24T11:00:00+08:00", 1774317600000, False),
        (FieldKind.DATETIME, "2026-03-24 10:00:00", 1774317600000, True),
        (FieldKind.SELECT, "Todo", ["Todo"], True),
        (FieldKind.SELECT, ["a", "b"], ["b", "a"], True),
        (FieldKind.SELECT, ["a", "a"], ["a"], False),
        (FieldKind.USER, ["ou_a"], [{"id": "ou_a"}], True),
    ],
)
def test_written_values_not_key_equivalence(kind, left, right, equal):
    assert (
        cells_equal(left, right, FieldSchema("f", "F", kind), "Asia/Shanghai") is equal
    )


def test_sheet_empty_edges_are_optional_not_internal_row_coordinates():
    assert sheet_values_equal(
        [["ID", "Name"], [1, None], [None, None]], [["ID", "Name"], [1]]
    )
    assert not sheet_values_equal([["ID"], [None], [1]], [["ID"], [1]])
    assert not sheet_values_equal([[True]], [[1]])
    assert not sheet_values_equal([], [[" "]])


@pytest.mark.parametrize(
    "kind,invalid",
    [
        (2, "12garbage"),
        (2, "1$2"),
        (2, "1,2"),
        (2, "9007199254740993"),
        (7, "maybe"),
        (3, ["one", "two"]),
        (5, "03/04/2026"),
    ],
)
def test_nonempty_unconvertible_business_values_rejected(kind, invalid):
    converter = DataConverter(TargetType.BITABLE)
    with pytest.raises(ValueError):
        converter.convert_write_value("Field", invalid, {"Field": kind})


@pytest.mark.parametrize(
    "kind,value,expected",
    [
        (1, " A ", " A "),
        (2, "1e3", 1000),
        (2, "$1,200.50", 1200.5),
        (2, "12%", 12),
        (7, "否", False),
        (3, "a,b", "a,b"),
    ],
)
def test_valid_business_values_are_preserved(kind, value, expected):
    assert (
        DataConverter(TargetType.BITABLE).convert_write_value("F", value, {"F": kind})
        == expected
    )


@pytest.mark.parametrize("backend_type", [BaseV3Backend, BitableV1Backend])
def test_codec_validation_is_pure_and_validates_all_records(backend_type):
    backend = backend_type(Mock())
    fields = (
        FieldSchema(
            "n",
            "N",
            FieldKind.NUMBER,
            raw_type=2 if backend_type is BitableV1Backend else "number",
        ),
    )
    with pytest.raises(ValueError, match="第 201 条"):
        backend.validate_records(
            [CanonicalRecord(None, {"N": 1})] * 200
            + [CanonicalRecord(None, {"N": "bad"})],
            fields,
        )
    assert not backend.auth.mock_calls


def test_invalid_last_action_prevents_earlier_deletion():
    engine = make_file_bitable_engine(SyncMode.CLONE)
    engine.api = BaseV3Backend(Mock())
    engine.api.batch_delete = Mock()
    fields = (FieldSchema("n", "N", FieldKind.NUMBER, raw_type="number"),)
    plan = ExecutionPlan(
        "clone",
        "clone",
        {},
        {},
        (
            DeleteRecordsAction(record_ids=("rec_old",), scope={}),
            CreateRecordsAction(
                records=(CanonicalRecord(None, {"N": "bad"}),), scope={}
            ),
        ),
        bitable_fields=fields,
    )
    outcome = engine.execute_plan(plan)
    assert outcome.status is OutcomeStatus.FAILED and not outcome.applied
    assert outcome.error["kind"] == "validation"
    engine.api.batch_delete.assert_not_called()


@pytest.mark.parametrize(
    "mode", [SyncMode.FULL, SyncMode.CLONE, SyncMode.OVERWRITE, SyncMode.INCREMENTAL]
)
def test_planning_nonempty_invalid_value_fails_in_every_mode(mode):
    engine = make_file_bitable_engine(mode)
    with pytest.raises(ValueError, match="数字"):
        engine.plan(pd.DataFrame([{"ID": "not-a-number", "Name": "good"}]))
    engine.api.batch_create.assert_not_called()
    engine.api.batch_delete.assert_not_called()


def test_create_delayed_visibility_does_not_replay_write():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    field = FieldSchema("name", "Name", FieldKind.TEXT)
    engine.api.batch_create.return_value = MutationReceipt(
        "create", "base_v3", 1, 1, record_ids=("r",)
    )
    engine.api.batch_get_records.side_effect = [
        result(fields=(field,), missing=("r",)),
        result((CanonicalRecord("r", {"Name": "x"}),), fields=(field,)),
    ]
    plan = ExecutionPlan(
        "full",
        "full",
        {},
        {},
        (
            CreateRecordsAction(
                records=(CanonicalRecord(None, {"Name": "x"}),), scope={}
            ),
        ),
    )
    outcome = engine.execute_plan(plan)
    assert outcome.ok and outcome.verification[0]["attempts"] == 2
    engine.api.batch_create.assert_called_once()
    assert outcome.summary()["confirmed_by_unit"] == {"record": 1}


def test_update_partial_visibility_only_polls_unresolved_and_preserves_nonprefix_counts():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    engine._reset_action_execution_state()
    field = FieldSchema("n", "N", FieldKind.NUMBER)
    engine.api.batch_get_records.side_effect = [
        result(
            (CanonicalRecord("a", {"N": 0}), CanonicalRecord("b", {"N": 2})),
            fields=(field,),
        ),
        result((CanonicalRecord("a", {"N": 1}),), fields=(field,)),
    ]
    receipts = [
        MutationReceipt("update", "base_v3", 1, 1),
        MutationReceipt("update", "base_v3", 1, 1),
    ]
    assert engine._verify_bitable_mutation(
        "update",
        [CanonicalRecord("a", {"N": 1}), CanonicalRecord("b", {"N": 2})],
        receipts,
    )
    assert engine.api.batch_get_records.call_args_list[1].args[2] == ["a"]
    assert [r.verified_count for r in receipts] == [1, 1]


def test_nonprefix_confirmation_timeout_does_not_attribute_to_first_batch():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    engine.sync_config = replace(engine.sync_config, verify_timeout_seconds=0)
    engine._reset_action_execution_state()
    field = FieldSchema("n", "N", FieldKind.NUMBER)
    engine.api.batch_get_records.return_value = result(
        (CanonicalRecord("a", {"N": 0}), CanonicalRecord("b", {"N": 2})),
        fields=(field,),
    )
    receipts = [
        MutationReceipt("update", "base_v3", 1, 1),
        MutationReceipt("update", "base_v3", 1, 1),
    ]
    assert not engine._verify_bitable_mutation(
        "update",
        [CanonicalRecord("a", {"N": 1}), CanonicalRecord("b", {"N": 2})],
        receipts,
    )
    assert [r.verified_count for r in receipts] == [0, 1]


def test_delete_waits_for_absence_without_redeleting():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    engine.api.batch_delete.return_value = MutationReceipt("delete", "base_v3", 1, 1)
    engine.api.batch_get_records.side_effect = [
        result((CanonicalRecord("r", {}),)),
        result(missing=("r",)),
    ]
    outcome = engine.execute_plan(
        ExecutionPlan(
            "clone",
            "clone",
            {},
            {},
            (DeleteRecordsAction(record_ids=("r",), scope={}),),
        )
    )
    assert outcome.ok
    engine.api.batch_delete.assert_called_once()


def test_visibility_timeout_reports_accepted_and_prevents_next_action():
    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    engine.sync_config = replace(engine.sync_config, verify_timeout_seconds=0)
    engine.api.batch_update.return_value = MutationReceipt("update", "base_v3", 1, 1)
    engine.api.batch_get_records.return_value = result(
        (CanonicalRecord("r", {"Name": "old"}),),
        fields=(FieldSchema("n", "Name", FieldKind.TEXT),),
    )
    update = UpdateRecordsAction(
        records=(CanonicalRecord("r", {"Name": "new"}),), scope={}
    )
    create = CreateRecordsAction(
        records=(CanonicalRecord(None, {"Name": "create"}),), scope={}
    )
    outcome = engine.execute_plan(
        ExecutionPlan("full", "full", {}, {}, (update, create))
    )
    assert outcome.status is OutcomeStatus.PARTIAL
    assert (
        outcome.error["accepted_count"] == 1 and outcome.error["confirmed_count"] == 0
    )
    assert outcome.error["confirmation"]["status"] == "visibility_timeout"
    assert outcome.verification[0]["status"] == "visibility_timeout"
    engine.api.batch_create.assert_not_called()


def test_sheet_per_call_rendering_does_not_mutate_client():
    engine = make_sheet_engine(sheet_value_render_option="ToString")
    response = Mock(status_code=200, headers={})
    response.json.return_value = {
        "code": 0,
        "data": {"valueRange": {"range": "sh1!A1:B1", "values": [[1, "x"]]}},
    }
    engine.api.api_client.call_api.return_value = response
    assert engine.api.get_sheet_data(
        "token", "sh1!A1:B1", value_render_option="UnformattedValue"
    ) == [[1, "x"]]
    assert (
        engine.api.api_client.call_api.call_args.kwargs["params"]["valueRenderOption"]
        == "UnformattedValue"
    )
    assert engine.api.value_render_option == "ToString"


def test_sheet_clear_delayed_and_whitespace_not_empty():
    from api import A1Range

    engine = make_sheet_engine(verify_remote_writes=True)
    engine.api.clear_values = Mock(
        return_value=MutationReceipt(
            "clear", "sheet_v2", 1, 1, actual_ranges=(A1Range.parse("sh1!A1:B2"),)
        )
    )
    engine.api.get_sheet_data = Mock(side_effect=[[[" "]], []])
    assert engine._typed_sheet_clear("A1:B2")
    assert engine.api.get_sheet_data.call_count == 2
    engine.api.clear_values.assert_called_once()


@pytest.mark.parametrize("backend_type", [BitableV1Backend, BaseV3Backend])
@pytest.mark.parametrize("value", [True, False, 1.5, "not-a-date"])
def test_date_wire_validation_rejects_invalid_instants(backend_type, value):
    backend = backend_type(Mock())
    with pytest.raises(ValueError):
        backend.validate_records(
            (CanonicalRecord(None, {"When": value}),),
            (FieldSchema("d", "When", FieldKind.DATETIME),),
        )


@pytest.mark.parametrize(
    "granularity,zone",
    [("exact", None), ("day", "America/Los_Angeles"), ("day", "Asia/Shanghai")],
)
def test_nonkey_dates_do_not_change_with_index_grouping(granularity, zone):
    converter = DataConverter(
        TargetType.BITABLE,
        datetime_index_granularity=granularity,
        datetime_index_timezone=zone,
    )
    schema = {"When": FieldSchema("d", "When", FieldKind.DATETIME)}
    expected = 1774317600000
    assert (
        converter.convert_write_value("When", "2026-03-24T10:00:00+08:00", schema)
        == expected
    )
    assert (
        converter.convert_write_value("When", "2026-03-24 02:00:00", schema) == expected
    )


def test_sheet_confirmation_uses_formula_render_only_for_explicit_formula_objects():
    from api import A1Range

    for value, rendered, render_option in [
        ({"type": "formula", "text": "=1+1"}, "=1+1", "Formula"),
        ("=literal", "=literal", "UnformattedValue"),
    ]:
        engine = make_sheet_engine(verify_remote_writes=True)
        engine.api.get_sheet_data = Mock(side_effect=[[], [[rendered]]])
        receipt = MutationReceipt(
            "write", "sheet_v2", 1, 1, actual_ranges=(A1Range.parse("sh1!A1:A1"),)
        )
        assert engine._finalize_sheet_mutation(
            receipt, expected_ranges={"sh1!A1:A1": [[value]]}
        )
        assert all(
            call.kwargs["value_render_option"] == render_option
            for call in engine.api.get_sheet_data.call_args_list
        )


def test_schema_is_not_confirmed_before_the_schema_read():
    from core.plan import CreateFieldAction

    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=True)
    document = engine._confirmation_document(
        CreateFieldAction(field_name="Extra", suggested_type=1, scope={})
    )
    assert document["status"] == "pending_schema" and document["confirmed_count"] == 0


def test_disabled_format_confirmation_does_not_suggest_repeating_enabled_data_confirmation():
    from xtf_cli.runtime import _sync_result_message

    message = _sync_result_message(
        {
            "status": "success",
            "summary": {
                "accepted_by_unit": {"row": 1},
                "confirmed_by_unit": {"row": 1},
            },
            "verification": [{"status": "verified"}, {"status": "not_supported"}],
        },
        "cfg",
    )
    assert "--verify-remote-writes" not in message and "附加配置" in message


def test_partial_formula_scan_subdivides_ranges_without_writing():
    from api import A1Range
    from api.sheet import FormulaVerificationResult

    engine = make_sheet_engine(sheet_verify_formulas=True)
    visited = []

    def scan(_token, _sheets, ranges, **_kwargs):
        visited.append(ranges)
        region = A1Range.parse("sh1!" + ranges[0])
        return (
            FormulaVerificationResult("partial", True)
            if region.row_count > 1
            else FormulaVerificationResult("success", False)
        )

    engine.api.verify_formulas = Mock(side_effect=scan)
    assert engine._verify_formula_ranges(["B2:D3"])
    assert visited == [["B2:D3"], ["B2:D2"], ["B3:D3"]]
    assert engine._last_action_confirmation["formula_scan"]["complete_ranges"] == 2
    assert len(engine.api.api_client.call_api.call_args_list) == 0


def test_partial_formula_scan_single_cell_is_not_success():
    from api.sheet import FormulaVerificationResult

    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock(
        return_value=FormulaVerificationResult("partial", True)
    )
    assert not engine._verify_formula_ranges(["B2:B2"])
    assert engine.api.verify_formulas.call_count == 1
    assert engine._last_action_confirmation["status"] == "failed"


def test_partial_formula_scan_budget_is_finite():
    from api.sheet import FormulaVerificationResult

    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.MAX_FORMULA_PROBES = 1
    engine.api.verify_formulas = Mock(
        return_value=FormulaVerificationResult("partial", True)
    )
    assert not engine._verify_formula_ranges(["B2:D300"])
    assert engine.api.verify_formulas.call_count == 1
    assert "上限" in engine._last_action_failure_message


def test_partial_formula_scan_splits_columns_without_gaps():
    from api.sheet import FormulaVerificationResult

    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock(
        side_effect=[
            FormulaVerificationResult("partial", True),
            FormulaVerificationResult("success", False),
            FormulaVerificationResult("success", False),
        ]
    )
    assert engine._verify_formula_ranges(["B2:D2"])
    assert [call.args[2] for call in engine.api.verify_formulas.call_args_list] == [
        ["B2:D2"],
        ["B2:C2"],
        ["D2:D2"],
    ]


def test_result_does_not_claim_complete_when_later_formula_step_failed():
    from core.plan import SyncResult, PlanDocument

    outcome = SyncResult(
        OutcomeStatus.PARTIAL,
        PlanDocument("full", "full", {}, {}, ()),
        verification=({"status": "verified", "ok": False},),
    )
    assert not outcome.summary()["confirmation_complete"]


@pytest.mark.parametrize("verify", [False, True])
def test_created_field_waits_for_schema_and_is_confirmed_once(verify):
    from core.plan import CreateFieldAction

    engine = make_file_bitable_engine(SyncMode.FULL, verify_remote_writes=verify)
    field = FieldSchema("new", "Extra", FieldKind.TEXT)
    engine.api.create_field.return_value = MutationReceipt(
        "create_field", "base_v3", 1, 1, unit="field"
    )
    engine.api.list_fields.side_effect = [(), (field,)]
    action = CreateFieldAction(field_name="Extra", suggested_type=1, scope={})
    outcome = engine.execute_plan(ExecutionPlan("full", "full", {}, {}, (action,)))
    assert outcome.ok and outcome.verification[0]["status"] == "schema_confirmed"
    assert outcome.summary()["confirmed_by_unit"] == {"field": 1}
    engine.api.create_field.assert_called_once()
    assert engine.api.list_fields.call_count == 2
