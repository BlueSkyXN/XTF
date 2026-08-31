"""Pure planning, serialization, and ordered execution tests."""

from unittest.mock import Mock

import pandas as pd
import pytest

from api import A1Range, FeishuAPIError, ReadbackStatus, SheetAPI
from api.bitable_backend import (
    BitableBackend,
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    MutationOutcome,
    MutationReceipt,
    RecordReadResult,
)
from core.config import MatchStrategy, SourceType, SyncConfig, SyncMode, TargetType
from core.converter import DataConverter
from core.engine import XTFSyncEngine
from core.plan import (
    AppendRowsAction,
    ApplySheetConfigAction,
    CreateFieldAction,
    CreateRecordsAction,
    ExecutionPlan,
    OutcomeStatus,
    UpdateRecordsAction,
    WriteColumnsAction,
)


def make_remote_engine(
    *,
    mode=SyncMode.FULL,
    granularity="exact",
    backend="base_v3",
):
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(
        file_path=None,
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.BITABLE,
        source_type=SourceType.BITABLE,
        source_app_token="source_app",
        source_table_id="source_table",
        app_token="target_app",
        table_id="target_table",
        index_column="When",
        sync_mode=mode,
        datetime_index_granularity=granularity,
        datetime_index_timezone=("Asia/Shanghai" if granularity == "day" else None),
        bitable_api_backend=backend,
    )
    engine.logger = Mock()
    engine.converter = DataConverter(
        TargetType.BITABLE,
        datetime_index_granularity=granularity,
        datetime_index_timezone=("Asia/Shanghai" if granularity == "day" else None),
    )
    engine.api = Mock(spec=BitableBackend)
    engine.api.max_batch_create_size = 500
    engine.api.max_batch_update_size = 500
    return engine


def make_file_bitable_engine(mode):
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.BITABLE,
        app_token="target_app",
        table_id="target_table",
        index_column="ID",
        sync_mode=mode,
        create_missing_fields=False,
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.BITABLE)
    engine.api = Mock(spec=BitableBackend)
    engine.api.max_batch_create_size = 500
    engine.api.max_batch_update_size = 500
    engine.api.max_batch_delete_size = 500
    fields = (
        FieldSchema("id", "ID", FieldKind.NUMBER, raw_type="number"),
        FieldSchema("name", "Name", FieldKind.TEXT, raw_type="text"),
    )
    engine.api.list_fields.return_value = fields
    engine.api.list_records.return_value = read_result(
        [CanonicalRecord("existing", {"ID": 1})], fields
    )
    return engine


def make_file_sheet_engine(*, mode=SyncMode.INCREMENTAL, granularity="exact"):
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.SHEET,
        spreadsheet_token="sheet_token",
        sheet_id="sheet",
        index_column="When",
        sync_mode=mode,
        datetime_index_granularity=granularity,
        datetime_index_timezone=("Asia/Shanghai" if granularity == "day" else None),
    )
    engine.logger = Mock()
    engine.converter = DataConverter(
        TargetType.SHEET,
        datetime_index_granularity=granularity,
        datetime_index_timezone=("Asia/Shanghai" if granularity == "day" else None),
    )
    engine.api = Mock()
    engine._sheet_read_complete = True
    return engine


def read_result(records, fields):
    return RecordReadResult(
        tuple(records),
        tuple(fields),
        True,
        BitableBackendKind.BASE_V3,
    )


def test_plan_serialization_omits_mutation_payload():
    action = CreateRecordsAction(
        records=(
            CanonicalRecord(
                None,
                {"secret": "must-not-leak", "fields": {"Name": "private"}},
            ),
        ),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
        config_sources={"sync_mode": "cli"},
    )

    assert not hasattr(plan, "to_dict")
    serialized = plan.to_public().to_dict()

    assert serialized["schema_version"] == 1
    assert serialized["config_sources"] == {"sync_mode": "cli"}
    assert serialized["actions"][0]["unit"] == "record"
    assert "verification_policy" not in serialized["actions"][0]
    assert "payload" not in serialized["actions"][0]
    assert "must-not-leak" not in repr(serialized)
    assert "private" not in repr(serialized)


def test_executor_rejects_public_plan_document():
    engine = make_remote_engine()
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
    )

    with pytest.raises(TypeError, match="internal ExecutionPlan"):
        engine.execute_plan(plan.to_public())


def test_datetime_index_exact_does_not_match_different_time_same_day():
    engine = make_remote_engine()
    fields = (
        FieldSchema("when", "When", FieldKind.DATETIME, raw_type="datetime"),
        FieldSchema("name", "Name", FieldKind.TEXT, raw_type="text"),
    )
    engine.api.list_fields.side_effect = [fields, fields]
    engine.api.list_records.side_effect = [
        read_result(
            [CanonicalRecord("source", {"When": "2026-08-30 18:00:00", "Name": "new"})],
            fields,
        ),
        read_result(
            [CanonicalRecord("target", {"When": "2026-08-30 09:00:00", "Name": "old"})],
            fields,
        ),
    ]

    plan = engine.plan()

    assert [action.kind for action in plan.actions] == ["create_records"]
    assert plan.actions[0].count == 1
    serialized = repr(plan.to_public().to_dict())
    assert "source_app" not in serialized
    assert "target_app" not in serialized


def test_datetime_index_day_preserves_legacy_day_matching():
    converter = DataConverter(
        TargetType.BITABLE,
        datetime_index_granularity="day",
        datetime_index_timezone="Asia/Shanghai",
    )

    morning = converter._normalize_index_value("2026-08-30 09:00:00", 5)
    evening = converter._normalize_index_value("2026-08-30 18:00:00", 5)

    assert morning == evening == "2026-08-30"


def test_datetime_index_exact_normalizes_seconds_ms_and_timestamp():
    converter = DataConverter(TargetType.BITABLE)
    converter.datetime_index_granularity = "exact"
    timestamp = pd.Timestamp("2026-08-30 09:00:00")
    seconds = int(timestamp.timestamp())
    expected = converter._normalize_index_value(timestamp, 5)

    assert expected == converter._normalize_index_value(seconds, 5)
    assert expected == converter._normalize_index_value(seconds * 1000, 5)


def test_datetime_index_exact_normalizes_seconds_after_2050():
    converter = DataConverter(TargetType.BITABLE)
    converter.datetime_index_granularity = "exact"
    timestamp = pd.Timestamp("2051-01-01 00:00:00")
    seconds = int(timestamp.timestamp())
    milliseconds = seconds * 1000

    assert converter._normalize_index_value(seconds, 5) == str(milliseconds)
    assert converter._normalize_index_value(milliseconds, 5) == str(milliseconds)
    assert converter._normalize_index_value(timestamp, 5) == str(milliseconds)
    assert converter._force_to_timestamp(seconds, "When") == milliseconds
    assert converter._force_to_timestamp(milliseconds, "When") == milliseconds


def test_numeric_datetime_outside_supported_range_fails_closed():
    converter = DataConverter(TargetType.BITABLE)
    converter.datetime_index_granularity = "exact"
    milliseconds_1971 = 31_536_000_000

    with pytest.raises(ValueError, match="2000-01-01"):
        converter._normalize_index_value(milliseconds_1971, 5)
    assert converter._force_to_timestamp(milliseconds_1971, "When") is None


def test_bitable_v1_schema_requires_raw_type_and_multiple_match():
    source = FieldSchema("a", "Value", FieldKind.TEXT, raw_type=1)
    target = FieldSchema("b", "Value", FieldKind.TEXT, raw_type=13)

    assert not XTFSyncEngine._bitable_schemas_compatible(
        source, target, BitableBackendKind.BITABLE_V1
    )


def test_base_v3_schema_requires_write_shape_properties_match():
    source = FieldSchema(
        "a",
        "Value",
        FieldKind.TEXT,
        raw_type="text",
        raw_properties={"ui_type": "Text"},
    )
    target = FieldSchema(
        "b",
        "Value",
        FieldKind.TEXT,
        raw_type="text",
        raw_properties={"ui_type": "Phone"},
    )

    assert not XTFSyncEngine._bitable_schemas_compatible(
        source, target, BitableBackendKind.BASE_V3
    )


def test_remote_plan_marks_empty_update_as_clearing_values():
    engine = make_remote_engine()
    fields = (
        FieldSchema("when", "When", FieldKind.DATETIME, raw_type="datetime"),
        FieldSchema("name", "Name", FieldKind.TEXT, raw_type="text"),
    )
    engine.api.list_fields.side_effect = [fields, fields]
    engine.api.list_records.side_effect = [
        read_result(
            [CanonicalRecord("source", {"When": "2026-08-30 09:00:00", "Name": None})],
            fields,
        ),
        read_result(
            [CanonicalRecord("target", {"When": "2026-08-30 09:00:00", "Name": "old"})],
            fields,
        ),
    ]

    plan = engine.plan()

    assert plan.clears_values is True
    assert plan.actions[0].kind == "update_records"
    assert plan.actions[0].clears_values is True
    assert any("清空" in warning for warning in plan.warnings)


def test_execute_plan_stops_on_first_error_and_keeps_applied_prefix():
    engine = make_remote_engine()
    create_field = CreateFieldAction(
        field_name="Name",
        suggested_type=1,
        scope={"target": "bitable", "field": "Name"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(
            create_field,
            UpdateRecordsAction(
                records=(
                    CanonicalRecord("record-1", {"Name": "one"}),
                    CanonicalRecord("record-2", {"Name": "two"}),
                ),
                scope={"target": "bitable"},
            ),
            CreateRecordsAction(
                records=tuple(
                    CanonicalRecord(None, {"Name": str(index)}) for index in range(3)
                ),
                scope={"target": "bitable"},
            ),
        ),
    )
    engine._execute_action = Mock(side_effect=[True, False, True])
    engine._refresh_and_verify_created_fields = Mock(return_value=(True, ""))

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert [action.kind for action in outcome.applied] == ["create_fields"]
    assert engine._execute_action.call_count == 2
    assert outcome.error["kind"] == "mutation"
    assert outcome.error["message"] == "action failed: update_records"
    assert outcome.error["failed_action"] == "update_records"


def test_execute_plan_reports_verification_not_requested_when_disabled():
    engine = make_remote_engine()
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(
            UpdateRecordsAction(
                records=(CanonicalRecord("record-1", {"Name": "updated"}),),
                scope={"target": "bitable"},
            ),
            CreateRecordsAction(
                records=(CanonicalRecord(None, {"Name": "new"}),),
                scope={"target": "bitable"},
            ),
        ),
    )
    engine._execute_action = Mock(return_value=True)

    outcome = engine.execute_plan(plan)

    assert outcome.ok is True
    assert outcome.verification == (
        {"kind": "update_records", "status": "not_requested", "ok": True},
        {"kind": "create_records", "status": "not_requested", "ok": True},
    )
    assert all(item["status"] != "verified" for item in outcome.verification)


def test_execute_plan_stops_after_created_field_schema_mismatch():
    engine = make_remote_engine()
    create_field = CreateFieldAction(
        field_name="Name",
        suggested_type=1,
        scope={"target": "bitable", "field": "Name"},
    )
    create_records = CreateRecordsAction(
        records=(CanonicalRecord(None, {"Name": "value"}),),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(create_field, create_records),
    )
    engine._execute_action = Mock(return_value=True)
    engine.api.list_fields.return_value = (
        FieldSchema("name", "Name", FieldKind.NUMBER, raw_type="number"),
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.applied == (create_field.to_public(),)
    assert outcome.error["kind"] == "verification"
    assert engine._execute_action.call_count == 1


def test_execute_plan_keeps_auth_kind_when_created_field_refresh_is_denied():
    engine = make_remote_engine()
    create_field = CreateFieldAction(
        field_name="Name",
        suggested_type=1,
        scope={"target": "bitable", "field": "Name"},
    )
    create_records = CreateRecordsAction(
        records=(CanonicalRecord(None, {"Name": "value"}),),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(create_field, create_records),
    )
    engine._execute_action = Mock(return_value=True)
    engine._refresh_and_verify_created_fields = Mock(
        side_effect=FeishuAPIError(99991663, "denied", http_status=401)
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.applied == (create_field.to_public(),)
    assert outcome.error["kind"] == "auth"
    assert engine._execute_action.call_count == 1


def test_execute_plan_verifies_created_fields_even_without_record_actions():
    engine = make_remote_engine()
    create_field = CreateFieldAction(
        field_name="Name",
        suggested_type=1,
        scope={"target": "bitable", "field": "Name"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(create_field,),
    )
    engine._execute_action = Mock(return_value=True)
    engine._refresh_and_verify_created_fields = Mock(
        return_value=(False, "schema mismatch")
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.applied == (create_field.to_public(),)
    assert outcome.error["kind"] == "verification"
    engine._refresh_and_verify_created_fields.assert_called_once_with([create_field])


def test_bitable_readback_mismatch_is_verification_failure():
    engine = make_file_bitable_engine(SyncMode.FULL)
    engine.config.verify_remote_writes = True
    record = CanonicalRecord("record-1", {"Name": "updated"})
    action = UpdateRecordsAction(
        records=(record,),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
    )
    engine.process_typed_bitable_batches = Mock(return_value=(True, []))
    engine._verify_bitable_mutation = Mock(return_value=False)

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.error["kind"] == "verification"
    assert outcome.applied == (action.to_public(),)
    assert outcome.verification == (
        {"kind": "update_records", "status": "failed", "ok": False},
    )


@pytest.mark.parametrize(
    (
        "failure_outcome",
        "accepted_in_failure",
        "expected_count",
        "unknown",
        "expected_status",
    ),
    [
        (MutationOutcome.PARTIAL, 1, 3, False, OutcomeStatus.PARTIAL),
        (
            MutationOutcome.UNKNOWN_OUTCOME,
            0,
            2,
            True,
            OutcomeStatus.INDETERMINATE,
        ),
    ],
)
def test_bitable_batch_failure_retains_confirmed_action_prefix(
    failure_outcome, accepted_in_failure, expected_count, unknown, expected_status
):
    engine = make_file_bitable_engine(SyncMode.INCREMENTAL)
    engine.config.batch_size = 2
    accepted = MutationReceipt(
        "create",
        BitableBackendKind.BASE_V3,
        requested_count=2,
        accepted_count=2,
    )
    failed = MutationReceipt(
        "create",
        BitableBackendKind.BASE_V3,
        requested_count=2,
        accepted_count=accepted_in_failure,
        outcome=failure_outcome,
    )
    engine.api.batch_create.side_effect = [accepted, failed]
    records = [CanonicalRecord(None, {"ID": value}) for value in range(4)]
    action = CreateRecordsAction(
        records=tuple(records),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="incremental",
        effective_mode="incremental",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is expected_status
    assert len(outcome.applied) == 1
    assert outcome.applied[0].count == expected_count
    assert outcome.applied[0].scope["partial"] is True
    assert outcome.error["remote_outcome"] == failure_outcome.value
    assert outcome.error["unknown"] is unknown


def test_sheet_range_failure_retains_confirmed_prefix():
    engine = make_file_sheet_engine()
    engine.api = SheetAPI(Mock(), Mock(), start_row=1, start_column="A")
    engine.api._optimize_column_ranges = Mock(
        return_value=[
            {"range": "A2:A2", "values": [["first"]]},
            {"range": "B2:B2", "values": [["second"]]},
        ]
    )
    engine.api.batch_update_values = Mock(
        side_effect=[
            MutationReceipt(
                "batch_update",
                "sheet_v2",
                requested_count=1,
                accepted_count=1,
                actual_ranges=(A1Range.parse("sheet!A2:A2"),),
            ),
            MutationReceipt(
                "batch_update",
                "sheet_v2",
                requested_count=1,
                outcome=MutationOutcome.PARTIAL,
            ),
        ]
    )
    action = WriteColumnsAction(
        column_data={"A": ("first",), "B": ("second",)},
        column_positions={"A": 1, "B": 2},
        start_row=2,
        max_gap=0,
        header_width=2,
        scope={"target": "sheet", "columns": 2},
    )
    plan = ExecutionPlan(
        requested_mode="full",
        effective_mode="full",
        source={"type": "file"},
        target={"type": "sheet"},
        actions=(action,),
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.applied[0].count == 1
    assert outcome.applied[0].scope["partial"] is True
    assert outcome.error["remote_outcome"] == "partial"


def test_sheet_accepted_append_with_unknown_scope_is_indeterminate():
    engine = make_file_sheet_engine()
    engine.api = SheetAPI(Mock(), Mock(), start_row=1, start_column="A")
    engine.api.append_values = Mock(
        return_value=MutationReceipt(
            "append",
            "sheet_v2",
            requested_count=1,
            accepted_count=1,
            outcome=MutationOutcome.ACCEPTED,
            readback=ReadbackStatus.UNKNOWN,
        )
    )
    action = AppendRowsAction(
        values=(("value",),),
        header_width=1,
        scope={"target": "sheet", "columns": 1},
    )
    plan = ExecutionPlan(
        requested_mode="incremental",
        effective_mode="incremental",
        source={"type": "file"},
        target={"type": "sheet"},
        actions=(action,),
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.INDETERMINATE
    assert outcome.applied == (action.to_public(),)
    assert outcome.error["remote_outcome"] == "unknown_outcome"
    assert outcome.error["unknown"] is True


def test_unknown_mutation_without_confirmed_prefix_is_indeterminate():
    engine = make_file_bitable_engine(SyncMode.INCREMENTAL)
    action = CreateRecordsAction(
        records=(CanonicalRecord(None, {"ID": 1}),),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="incremental",
        effective_mode="incremental",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
    )

    def lose_response(_action):
        engine._last_action_remote_outcome = MutationOutcome.UNKNOWN_OUTCOME.value
        return engine._mark_action_failure(engine._last_action_error_kind)

    engine._execute_action = Mock(side_effect=lose_response)

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.INDETERMINATE
    assert outcome.applied == ()
    assert outcome.error["unknown"] is True


def test_execute_auth_error_keeps_auth_kind():
    engine = make_file_bitable_engine(SyncMode.INCREMENTAL)
    action = CreateRecordsAction(
        records=(CanonicalRecord(None, {"ID": 1}),),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="incremental",
        effective_mode="incremental",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
    )
    engine.api.batch_create.side_effect = FeishuAPIError(
        99991663, "denied", http_status=401
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error["kind"] == "auth"


def test_execute_missing_resource_keeps_resource_kind():
    engine = make_file_bitable_engine(SyncMode.INCREMENTAL)
    action = CreateRecordsAction(
        records=(CanonicalRecord(None, {"ID": 1}),),
        scope={"target": "bitable"},
    )
    plan = ExecutionPlan(
        requested_mode="incremental",
        effective_mode="incremental",
        source={"type": "file"},
        target={"type": "bitable"},
        actions=(action,),
    )
    engine.api.batch_create.side_effect = FeishuAPIError(
        40400, "missing", http_status=404
    )

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.FAILED
    assert outcome.error["kind"] == "resource"


def test_apply_sheet_config_is_not_reported_as_verified_without_readback():
    engine = make_file_sheet_engine()
    engine.config.verify_remote_writes = True
    action = ApplySheetConfigAction(
        frame=pd.DataFrame({"When": ["2026-08-30"]}),
        scope={"target": "sheet"},
    )
    plan = ExecutionPlan(
        requested_mode="clone",
        effective_mode="clone",
        source={"type": "file"},
        target={"type": "sheet"},
        actions=(action,),
    )
    engine._execute_action = Mock(return_value=True)

    outcome = engine.execute_plan(plan)

    assert outcome.verification == (
        {"kind": "apply_sheet_config", "status": "not_supported", "ok": True},
    )


def test_best_effort_sheet_config_failure_only_adds_warning():
    engine = make_file_sheet_engine()
    action = ApplySheetConfigAction(
        frame=pd.DataFrame({"When": ["2026-08-30"]}),
        scope={"target": "sheet"},
    )
    plan = ExecutionPlan(
        requested_mode="clone",
        effective_mode="clone",
        source={"type": "file"},
        target={"type": "sheet"},
        actions=(action,),
    )
    engine._execute_action = Mock(return_value=False)

    outcome = engine.execute_plan(plan)

    assert outcome.status is OutcomeStatus.SUCCESS
    assert outcome.applied == ()
    assert outcome.verification == (
        {"kind": "apply_sheet_config", "status": "best_effort_failed", "ok": True},
    )
    assert any("best-effort" in warning for warning in outcome.warnings)


@pytest.mark.parametrize(
    "source_value",
    [
        pd.Timestamp("2026-08-30 09:00:00"),
        1788080400,
        "2026-08-30 09:00:00",
    ],
)
def test_sheet_datetime_exact_matches_timestamp_seconds_and_string(source_value):
    engine = make_file_sheet_engine(granularity="exact")
    engine.converter.datetime_index_granularity = "exact"
    current = pd.DataFrame({"When": [1788080400000], "Name": ["existing"]})
    engine.get_current_sheet_data = Mock(return_value=current)

    plan = engine.plan(pd.DataFrame({"When": [source_value], "Name": ["same"]}))

    assert plan.actions == ()


def test_sheet_datetime_day_matches_same_day_and_rejects_duplicate_day():
    engine = make_file_sheet_engine(granularity="day")
    current = pd.DataFrame({"When": ["2026-08-30 09:00:00"], "Name": ["existing"]})
    engine.get_current_sheet_data = Mock(return_value=current)

    plan = engine.plan(
        pd.DataFrame({"When": ["2026-08-30 18:00:00"], "Name": ["same-day"]})
    )
    assert plan.actions == ()

    engine.get_current_sheet_data = Mock(
        return_value=pd.DataFrame(
            {
                "When": ["2026-08-30 09:00:00", "2026-08-30 18:00:00"],
                "Name": ["first", "second"],
            }
        )
    )
    with pytest.raises(RuntimeError, match="重复值"):
        engine.plan(pd.DataFrame({"When": ["2026-08-31 09:00:00"], "Name": ["new"]}))


@pytest.mark.parametrize("granularity", ["exact", "day"])
def test_sheet_numeric_epoch_equivalents_are_duplicate_without_target_overlap(
    granularity,
):
    engine = make_file_sheet_engine(granularity=granularity)
    engine.converter.datetime_index_granularity = granularity
    engine.get_current_sheet_data = Mock(
        return_value=pd.DataFrame({"When": [1788166800000], "Name": ["other"]})
    )

    with pytest.raises(ValueError, match="重复值"):
        engine.plan(
            pd.DataFrame(
                {
                    "When": [1788080400, 1788080400000],
                    "Name": ["seconds", "milliseconds"],
                }
            )
        )


def test_sheet_datetime_matches_seconds_and_milliseconds_after_2050():
    engine = make_file_sheet_engine(granularity="exact")
    engine.converter.datetime_index_granularity = "exact"
    seconds = int(pd.Timestamp("2051-01-01 00:00:00").timestamp())
    engine.get_current_sheet_data = Mock(
        return_value=pd.DataFrame({"When": [seconds * 1000], "Name": ["existing"]})
    )

    plan = engine.plan(pd.DataFrame({"When": [seconds], "Name": ["same"]}))

    assert plan.actions == ()


@pytest.mark.parametrize(
    ("mode", "expected_kinds", "destructive"),
    [
        (SyncMode.FULL, ["update_records", "create_records"], False),
        (SyncMode.INCREMENTAL, ["create_records"], False),
        (SyncMode.OVERWRITE, ["delete_records", "create_records"], True),
        (SyncMode.CLONE, ["delete_records", "create_records"], True),
    ],
)
def test_file_bitable_planner_covers_modes_without_mutation(
    mode, expected_kinds, destructive
):
    engine = make_file_bitable_engine(mode)

    plan = engine.plan(pd.DataFrame({"ID": [1, 2], "Name": ["old", "new"]}))

    assert [action.kind for action in plan.actions] == expected_kinds
    assert plan.destructive is destructive
    engine.api.create_field.assert_not_called()
    engine.api.batch_create.assert_not_called()
    engine.api.batch_update.assert_not_called()
    engine.api.batch_delete.assert_not_called()


def test_file_bitable_planner_uses_predicted_new_index_without_remote_match_read():
    engine = make_file_bitable_engine(SyncMode.FULL)
    engine.config.create_missing_fields = True
    name_field = FieldSchema("name", "Name", FieldKind.TEXT, raw_type="text")
    engine.api.list_fields.return_value = (name_field,)

    plan = engine.plan(pd.DataFrame({"ID": [1, 2], "Name": ["one", "two"]}))

    assert [action.kind for action in plan.actions] == [
        "create_fields",
        "create_records",
    ]
    assert plan.actions[1].count == 2
    engine.api.list_records.assert_not_called()
    engine.api.create_field.assert_not_called()


def test_file_bitable_planner_rejects_missing_fields_when_creation_is_disabled():
    engine = make_file_bitable_engine(SyncMode.FULL)
    engine.api.list_fields.return_value = (
        FieldSchema("id", "ID", FieldKind.NUMBER, raw_type="number"),
    )

    with pytest.raises(ValueError, match="create_missing_fields=false"):
        engine.plan(pd.DataFrame({"ID": [1], "Name": ["one"]}))

    engine.api.list_records.assert_not_called()
    engine.api.batch_create.assert_not_called()


@pytest.mark.parametrize("granularity", ["minute", "", "EXACTLY"])
def test_config_rejects_unknown_datetime_index_granularity(granularity):
    with pytest.raises(ValueError, match="仅支持 exact 或 day"):
        SyncConfig(
            file_path="test.xlsx",
            app_id="test_id",
            app_secret="test_secret",
            target_type=TargetType.BITABLE,
            app_token="app",
            table_id="table",
            datetime_index_granularity=granularity,
        )


def test_config_datetime_index_granularity_defaults_to_exact():
    config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.BITABLE,
        app_token="app",
        table_id="table",
    )

    assert config.datetime_index_granularity == "exact"


def test_sheet_full_without_index_is_rejected_before_remote_reads():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.SHEET,
        spreadsheet_token="sheet_token",
        sheet_id="sheet",
        sync_mode=SyncMode.FULL,
        index_column=None,
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.SHEET)
    engine.api = Mock()
    engine._sheet_read_complete = True
    with pytest.raises(ValueError, match="by_key.*index_column"):
        engine.plan(pd.DataFrame({"ID": [2]}))

    engine.api.clear_values.assert_not_called()
    engine.api.write_values.assert_not_called()


def test_incremental_append_only_bitable_creates_every_source_row_without_record_read():
    engine = make_file_bitable_engine(SyncMode.INCREMENTAL)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.BITABLE,
        app_token="target_app",
        table_id="target_table",
        sync_mode=SyncMode.INCREMENTAL,
        match_strategy=MatchStrategy.APPEND_ONLY,
        create_missing_fields=False,
    )

    plan = engine.plan(pd.DataFrame({"ID": [1, 1], "Name": ["A", "B"]}))

    assert plan.effective_mode == "incremental"
    assert [action.kind for action in plan.actions] == ["create_records"]
    assert plan.actions[0].count == 2
    engine.api.list_records.assert_not_called()


def test_incremental_append_only_sheet_does_not_read_or_clear_target():
    engine = make_file_sheet_engine(mode=SyncMode.INCREMENTAL)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.SHEET,
        spreadsheet_token="sheet_token",
        sheet_id="sheet",
        sync_mode=SyncMode.INCREMENTAL,
        match_strategy=MatchStrategy.APPEND_ONLY,
    )

    plan = engine.plan(pd.DataFrame({"Name": ["A", "B"]}))

    assert plan.effective_mode == "incremental"
    assert [action.kind for action in plan.actions] == ["append_rows"]
    engine.api.get_sheet_data_chunked.assert_not_called()
    engine.api.clear_values.assert_not_called()


def test_empty_sheet_keeps_full_mode_instead_of_implicit_clone(monkeypatch):
    engine = make_file_sheet_engine(mode=SyncMode.FULL)
    monkeypatch.setattr(
        engine,
        "get_sheet_data_with_validation",
        Mock(return_value=(pd.DataFrame(columns=["When", "Name"]), None, set())),
    )

    plan = engine.plan(pd.DataFrame({"When": ["2026-08-31"], "Name": ["A"]}))

    assert plan.requested_mode == plan.effective_mode == "full"
    assert plan.destructive is False
    assert [action.kind for action in plan.actions] == ["append_rows"]


@pytest.mark.parametrize(
    ("mode", "expected_kinds", "destructive"),
    [
        (SyncMode.FULL, ["write_range", "append_rows"], False),
        (SyncMode.INCREMENTAL, ["append_rows"], False),
        (SyncMode.OVERWRITE, ["write_range"], True),
        (
            SyncMode.CLONE,
            ["clear_range", "write_range", "apply_sheet_config"],
            True,
        ),
    ],
)
def test_file_sheet_planner_covers_modes_without_mutation(
    monkeypatch, mode, expected_kinds, destructive
):
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(
        file_path="test.xlsx",
        app_id="test_id",
        app_secret="test_secret",
        target_type=TargetType.SHEET,
        spreadsheet_token="sheet_token",
        sheet_id="sheet",
        sync_mode=mode,
        index_column="ID",
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.SHEET)
    engine.api = Mock()
    engine._sheet_read_complete = True
    current = pd.DataFrame({"ID": [1], "Name": ["old"]})
    monkeypatch.setattr(
        engine,
        "get_sheet_data_with_validation",
        Mock(return_value=(current, None, set())),
    )
    monkeypatch.setattr(engine, "get_current_sheet_data", Mock(return_value=current))
    monkeypatch.setattr(
        engine, "_build_sheet_full_range", Mock(return_value="sheet!A1:Z100")
    )

    plan = engine.plan(pd.DataFrame({"ID": [1, 2], "Name": ["new", "added"]}))

    assert [action.kind for action in plan.actions] == expected_kinds
    assert plan.destructive is destructive
    engine.api.clear_values.assert_not_called()
    engine.api.write_values.assert_not_called()
    engine.api.append_values.assert_not_called()
