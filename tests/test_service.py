#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""单轨 SyncService 装配和 fail-fast 批处理测试。"""

from unittest.mock import Mock, patch

import pandas as pd
import pytest

from api.bitable_backend import (
    BitableBackend,
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    MutationOutcome,
    MutationReceipt,
    ReadbackStatus,
    RecordReadResult,
)
from api.sheet import A1Range, FormulaVerificationResult, SheetAPI
from core.config import (
    SourceType,
    SyncMode,
    TargetType,
)
from core.converter import DataConverter
from core.runtime_config import RuntimeSheetTarget
from core.service import SyncService
from tests.conftest import attach_runtime, make_runtime_config


def make_sheet_engine(**config_overrides):
    config_data = {
        "file_path": "test.xlsx",
        "app_id": "cli_test",
        "app_secret": "test_secret",
        "spreadsheet_token": "sheet-token",
        "sheet_id": "sh1",
        "index_column": "ID",
    }
    config_data.update(config_overrides)
    engine = SyncService.__new__(SyncService)
    runtime = make_runtime_config(TargetType.SHEET, **config_data)
    attach_runtime(engine, runtime)
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.SHEET)
    target = runtime.target
    assert isinstance(target, RuntimeSheetTarget)
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    engine.api = SheetAPI(
        auth,
        Mock(),
        start_row=target.start_row,
        start_column=target.start_column,
        value_render_option=target.value_render_option,
        datetime_render_option=target.datetime_render_option,
    )
    engine._sheet_grid_cache = None
    engine._sheet_grid_cache_key = None
    engine._sheet_read_complete = True
    return engine


@patch("core.service.DataConverter")
@patch("core.service.bootstrap_runtime")
def test_service_uses_explicit_runtime_bootstrap(
    mock_bootstrap,
    mock_converter,
    sample_bitable_config,
):
    dependencies = mock_bootstrap.return_value
    dependencies.logger = Mock()
    dependencies.transport = Mock()
    dependencies.auth = Mock()
    dependencies.target = Mock(spec=BitableBackend)

    engine = SyncService(sample_bitable_config)

    mock_bootstrap.assert_called_once_with(sample_bitable_config)
    assert engine.api_client is dependencies.transport
    assert engine.auth is dependencies.auth
    assert engine.api is dependencies.target


def make_bitable_source_engine(sync_mode=SyncMode.FULL):
    engine = SyncService.__new__(SyncService)
    runtime = make_runtime_config(
        TargetType.BITABLE,
        file_path=None,
        source_type=SourceType.BITABLE.value,
        source_app_token="app_source",
        source_table_id="tbl_source",
        app_token="app_target",
        table_id="tbl_target",
        index_column="ID",
        sync_mode=sync_mode.value,
        verify_remote_writes=False,
    )
    attach_runtime(engine, runtime)
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.BITABLE)
    engine.api = Mock(spec=BitableBackend)
    engine.api.max_batch_create_size = 500
    engine.api.max_batch_update_size = 500
    return engine


def remote_copy_schemas():
    return (
        FieldSchema("fld_id", "ID", FieldKind.NUMBER, raw_type="number"),
        FieldSchema("fld_name", "Name", FieldKind.TEXT, raw_type="text"),
    )


def remote_read(records, fields=None, *, complete=True):
    return RecordReadResult(
        records=tuple(records),
        fields=tuple(fields or remote_copy_schemas()),
        complete=complete,
        backend=BitableBackendKind.BASE_V3,
    )


def test_bitable_source_full_only_writes_changed_and_missing_records():
    engine = make_bitable_source_engine()
    schemas = remote_copy_schemas()
    engine.api.list_fields.side_effect = [schemas, schemas]
    engine.api.list_records.side_effect = [
        remote_read(
            [
                CanonicalRecord("src1", {"ID": 1, "Name": "new"}),
                CanonicalRecord("src2", {"ID": 2, "Name": "added"}),
                CanonicalRecord("src4", {"ID": 4, "Name": "unchanged"}),
            ]
        ),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1, "Name": "old"}),
                CanonicalRecord("dst3", {"ID": 3, "Name": "target only"}),
                CanonicalRecord("dst4", {"ID": 4, "Name": "unchanged"}),
            ]
        ),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1}),
                CanonicalRecord("dst3", {"ID": 3}),
                CanonicalRecord("dst4", {"ID": 4}),
            ],
            fields=(schemas[0],),
        ),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1}),
                CanonicalRecord("dst3", {"ID": 3}),
                CanonicalRecord("dst4", {"ID": 4}),
            ],
            fields=(schemas[0],),
        ),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1}),
                CanonicalRecord("dst3", {"ID": 3}),
                CanonicalRecord("dst4", {"ID": 4}),
            ],
            fields=(schemas[0],),
        ),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1}),
                CanonicalRecord("dst2", {"ID": 2}),
                CanonicalRecord("dst3", {"ID": 3}),
                CanonicalRecord("dst4", {"ID": 4}),
            ],
            fields=(schemas[0],),
        ),
    ]
    updated = []
    created = []

    def batch_update(_app, _table, records):
        updated.extend(records)
        return MutationReceipt(
            "batch_update",
            BitableBackendKind.BASE_V3,
            len(records),
            accepted_count=len(records),
        )

    def batch_create(_app, _table, records):
        created.extend(records)
        return MutationReceipt(
            "batch_create",
            BitableBackendKind.BASE_V3,
            len(records),
            accepted_count=len(records),
        )

    engine.api.batch_update = batch_update
    engine.api.batch_create = batch_create

    outcome = engine.execute_plan(engine.plan())

    assert outcome.ok is True
    assert updated == [CanonicalRecord("dst1", {"Name": "new"})]
    assert created == [CanonicalRecord(None, {"ID": 2, "Name": "added"})]
    engine.api.batch_delete.assert_not_called()


def test_bitable_source_incremental_skips_existing_changed_record():
    engine = make_bitable_source_engine(SyncMode.INCREMENTAL)
    schemas = remote_copy_schemas()
    engine.api.list_fields.side_effect = [schemas, schemas]
    engine.api.list_records.side_effect = [
        remote_read(
            [
                CanonicalRecord("src1", {"ID": 1, "Name": "new"}),
                CanonicalRecord("src2", {"ID": 2, "Name": "added"}),
            ]
        ),
        remote_read([CanonicalRecord("dst1", {"ID": 1})], fields=(schemas[0],)),
        remote_read([CanonicalRecord("dst1", {"ID": 1})], fields=(schemas[0],)),
        remote_read(
            [
                CanonicalRecord("dst1", {"ID": 1}),
                CanonicalRecord("dst2", {"ID": 2}),
            ],
            fields=(schemas[0],),
        ),
    ]
    created = []

    def batch_create(_app, _table, records):
        created.extend(records)
        return MutationReceipt(
            "batch_create",
            BitableBackendKind.BASE_V3,
            len(records),
            accepted_count=len(records),
        )

    engine.api.batch_create = batch_create

    outcome = engine.execute_plan(engine.plan())

    assert outcome.ok is True
    assert created == [CanonicalRecord(None, {"ID": 2, "Name": "added"})]
    engine.api.batch_update.assert_not_called()
    engine.api.batch_delete.assert_not_called()


def test_bitable_source_duplicate_index_stops_before_write():
    engine = make_bitable_source_engine()
    schemas = remote_copy_schemas()
    engine.api.list_fields.side_effect = [schemas, schemas]
    engine.api.list_records.side_effect = [
        remote_read(
            [
                CanonicalRecord("src1", {"ID": 1, "Name": "a"}),
                CanonicalRecord("src2", {"ID": 1, "Name": "b"}),
            ]
        ),
        remote_read([]),
    ]

    with pytest.raises(RuntimeError, match="重复"):
        engine.plan()

    engine.api.batch_update.assert_not_called()
    engine.api.batch_create.assert_not_called()
    engine.api.batch_delete.assert_not_called()


def test_bitable_source_plan_rejects_dataframe_argument():
    engine = make_bitable_source_engine()

    with pytest.raises(ValueError, match="不接受本地 DataFrame"):
        engine.plan(pd.DataFrame({"ID": [1]}))


def test_typed_bitable_batches_use_backend_limit_and_stop_on_partial():
    engine = SyncService.__new__(SyncService)
    attach_runtime(
        engine,
        make_runtime_config(
            TargetType.BITABLE,
            app_token="app",
            table_id="table",
            batch_size=500,
        ),
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.BITABLE)
    engine.api = Mock(spec=BitableBackend)
    engine.api.max_batch_create_size = 200
    calls = []

    def processor(app, table, batch):
        calls.append(list(batch))
        outcome = (
            MutationOutcome.ACCEPTED if len(calls) == 1 else MutationOutcome.PARTIAL
        )
        return MutationReceipt(
            operation="batch_create",
            backend=BitableBackendKind.BASE_V3,
            requested_count=len(batch),
            accepted_count=len(batch) if outcome is MutationOutcome.ACCEPTED else 0,
            outcome=outcome,
        )

    processor.__name__ = "batch_create"
    success, receipts = engine.process_typed_bitable_batches(
        list(range(450)), processor
    )

    assert success is False
    assert [len(batch) for batch in calls] == [200, 200]
    assert len(receipts) == 2


def test_typed_bitable_update_readback_mismatch_blocks_next_phase():
    engine = SyncService.__new__(SyncService)
    attach_runtime(
        engine,
        make_runtime_config(
            TargetType.BITABLE,
            verify_remote_writes=True,
            app_token="app",
            table_id="table",
        ),
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.BITABLE)
    engine.api = Mock(spec=BitableBackend)
    requested = [CanonicalRecord("rec1", {"Name": "expected"})]
    engine.api.batch_get_records.return_value = Mock(
        complete=True,
        ignored_fields=(),
        record_not_found=(),
        fields=(),
        records=(CanonicalRecord("rec1", {"Name": "actual"}),),
    )

    assert engine._verify_bitable_mutation("update", requested, []) is True

    receipt = MutationReceipt(
        operation="batch_update",
        backend=BitableBackendKind.BASE_V3,
        requested_count=1,
        accepted_count=1,
    )
    assert engine._verify_bitable_mutation("update", requested, [receipt]) is False


def test_typed_bitable_readback_updates_receipt_status():
    engine = SyncService.__new__(SyncService)
    attach_runtime(
        engine,
        make_runtime_config(
            TargetType.BITABLE,
            verify_remote_writes=True,
            app_token="app",
            table_id="table",
        ),
    )
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.BITABLE)
    engine.api = Mock(spec=BitableBackend)
    requested = [CanonicalRecord("rec1", {"Name": "expected"})]
    engine.api.batch_get_records.return_value = Mock(
        complete=True,
        ignored_fields=(),
        record_not_found=(),
        fields=(),
        records=(CanonicalRecord("rec1", {"Name": "expected"}),),
    )
    receipts = [
        MutationReceipt(
            operation="batch_update",
            backend=BitableBackendKind.BASE_V3,
            requested_count=1,
            accepted_count=1,
        )
    ]

    assert engine._verify_bitable_mutation("update", requested, receipts) is True
    assert receipts[0].readback.value == "verified"
    assert receipts[0].verified_count == 1


def test_typed_bitable_delete_readback_requires_explicit_absence():
    engine = SyncService.__new__(SyncService)
    attach_runtime(
        engine,
        make_runtime_config(
            TargetType.BITABLE,
            verify_remote_writes=True,
            app_token="app",
            table_id="table",
        ),
    )
    engine.logger = Mock()
    engine.api = Mock(spec=BitableBackend)
    engine.api.batch_get_records.return_value = Mock(
        complete=True,
        ignored_fields=(),
        record_not_found=("rec1",),
        records=(),
    )
    receipt = MutationReceipt(
        operation="batch_delete",
        backend=BitableBackendKind.BASE_V3,
        requested_count=1,
        accepted_count=1,
    )

    assert engine._verify_bitable_mutation("delete", ["rec1"], [receipt]) is True


def test_sheet_formula_ranges_merge_adjacent_rows_and_use_managed_width():
    ranges = SyncService._merge_sheet_formula_ranges(
        [
            A1Range.parse("sh1!D2:E3"),
            A1Range.parse("sh1!F3:G4"),
            A1Range.parse("sh1!D7:E7"),
        ],
        start_col=3,
        header_width=4,
    )

    assert ranges == ["C2:F4", "C7:F7"]


def test_sheet_formula_verification_stops_on_partial_result():
    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock(
        return_value=FormulaVerificationResult("partial", False)
    )
    receipt = MutationReceipt(
        operation="write",
        backend="sheet_v2",
        requested_count=1,
        accepted_count=1,
        actual_ranges=(A1Range.parse("sh1!A2:B3"),),
    )

    assert engine._finalize_sheet_mutation(receipt, header_width=2) is False
    engine.api.verify_formulas.assert_called_once_with(
        "sheet-token",
        ["sh1"],
        ["A2:B3"],
        max_locations_per_error=20,
    )


def test_sheet_formula_verification_excludes_header_row_from_full_write():
    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock(
        return_value=FormulaVerificationResult("success", False)
    )
    receipt = MutationReceipt(
        operation="write",
        backend="sheet_v2",
        requested_count=1,
        accepted_count=1,
        actual_ranges=(A1Range.parse("sh1!A1:B3"),),
    )

    assert (
        engine._finalize_sheet_mutation(receipt, header_width=2, skip_header_row=True)
        is True
    )
    engine.api.verify_formulas.assert_called_once_with(
        "sheet-token",
        ["sh1"],
        ["A2:B3"],
        max_locations_per_error=20,
    )


def test_sheet_formula_verification_rejects_unknown_append_scope():
    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock()
    receipt = MutationReceipt(
        operation="append",
        backend="sheet_v2",
        requested_count=2,
        accepted_count=2,
        readback=ReadbackStatus.UNKNOWN,
    )

    assert engine._finalize_sheet_mutation(receipt, header_width=2) is False
    engine.api.verify_formulas.assert_not_called()


def test_sheet_formula_verification_does_not_run_for_header_only_write():
    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock()
    receipt = MutationReceipt(
        operation="write",
        backend="sheet_v2",
        requested_count=1,
        accepted_count=1,
        actual_ranges=(A1Range.parse("sh1!A1:B1"),),
    )

    assert (
        engine._finalize_sheet_mutation(receipt, header_width=2, skip_header_row=True)
        is True
    )
    engine.api.verify_formulas.assert_not_called()


def test_sheet_write_readback_mismatch_blocks_completion():
    engine = make_sheet_engine(verify_remote_writes=True)
    engine.api.get_sheet_data = Mock(return_value=[["different"]])
    receipt = MutationReceipt(
        operation="write",
        backend="sheet_v2",
        requested_count=1,
        accepted_count=1,
        actual_ranges=(A1Range.parse("sh1!A1:A1"),),
    )
    assert (
        engine._finalize_sheet_mutation(
            receipt,
            expected_ranges={"sh1!A1:A1": [["expected"]]},
            header_width=1,
        )
        is False
    )


def test_sheet_append_readback_covers_each_server_actual_chunk():
    engine = make_sheet_engine(verify_remote_writes=True)
    engine.api.append_values = Mock(
        return_value=MutationReceipt(
            operation="append",
            backend="sheet_v2",
            requested_count=3,
            accepted_count=3,
            actual_ranges=(
                A1Range.parse("sh1!A10:B11"),
                A1Range.parse("sh1!A12:B12"),
            ),
        )
    )
    engine.api.get_sheet_data = Mock(side_effect=[[[1, 2], [3, 4]], [[5, 6]]])

    assert engine._typed_sheet_append([[1, 2], [3, 4], [5, 6]], header_width=2)
    assert [call.args[1] for call in engine.api.get_sheet_data.call_args_list] == [
        "sh1!A10:B11",
        "sh1!A12:B12",
    ]


def test_wide_sheet_append_readback_uses_receipt_source_slices():
    engine = make_sheet_engine(verify_remote_writes=True)
    values = [list(range(101)), list(range(101, 202))]
    engine.api.append_values = Mock(
        return_value=MutationReceipt(
            operation="append",
            backend="sheet_v2",
            requested_count=2,
            accepted_count=2,
            unit="row",
            actual_ranges=(
                A1Range.parse("sh1!A10:CV11"),
                A1Range.parse("sh1!CW10:CW11"),
            ),
            raw_metadata={
                "source_slices": (
                    {
                        "range": "sh1!A10:CV11",
                        "row_offset": 0,
                        "col_offset": 0,
                        "row_count": 2,
                        "col_count": 100,
                    },
                    {
                        "range": "sh1!CW10:CW11",
                        "row_offset": 0,
                        "col_offset": 100,
                        "row_count": 2,
                        "col_count": 1,
                    },
                )
            },
        )
    )
    engine.api.get_sheet_data = Mock(
        side_effect=[
            [values[0][:100], values[1][:100]],
            [[values[0][100]], [values[1][100]]],
        ]
    )

    assert engine._typed_sheet_append(values, header_width=101)
    assert [call.args[1] for call in engine.api.get_sheet_data.call_args_list] == [
        "sh1!A10:CV11",
        "sh1!CW10:CW11",
    ]


def test_sheet_unknown_outcome_blocks_formula_verification():
    engine = make_sheet_engine(sheet_verify_formulas=True)
    engine.api.verify_formulas = Mock()
    receipt = MutationReceipt(
        operation="append",
        backend="sheet_v2",
        requested_count=1,
        outcome=MutationOutcome.UNKNOWN_OUTCOME,
        readback=ReadbackStatus.UNKNOWN,
    )

    assert engine._finalize_sheet_mutation(receipt, header_width=1) is False
    engine.api.verify_formulas.assert_not_called()


def test_sheet_selective_write_propagates_partial_batch_failure():
    engine = make_sheet_engine()
    engine._typed_sheet_batch_update = Mock(return_value=False)

    assert (
        engine._typed_sheet_selective_write(
            {"ID": [1, 2, 3]},
            {"ID": 1},
            start_row=2,
            max_gap=0,
            header_width=1,
        )
        is False
    )
    engine._typed_sheet_batch_update.assert_called_once()


def test_sheet_clear_readback_requires_observed_empty_cells():
    engine = make_sheet_engine(verify_remote_writes=True)
    engine.api.clear_values = Mock(
        return_value=MutationReceipt(
            operation="batch_update",
            backend="sheet_v2",
            requested_count=1,
            accepted_count=1,
            actual_ranges=(A1Range.parse("sh1!A1:B2"),),
        )
    )
    engine.api.get_sheet_data = Mock(return_value=[["still-present"]])

    assert engine._typed_sheet_clear("A1:B2") is False
    engine.api.get_sheet_data.assert_called_once_with("sheet-token", "sh1!A1:B2")


def test_sheet_clear_readback_accepts_trimmed_empty_matrix():
    engine = make_sheet_engine(verify_remote_writes=True)
    engine.api.clear_values = Mock(
        return_value=MutationReceipt(
            operation="batch_update",
            backend="sheet_v2",
            requested_count=1,
            accepted_count=1,
            actual_ranges=(A1Range.parse("sh1!A1:B2"),),
        )
    )
    engine.api.get_sheet_data = Mock(return_value=[])

    assert engine._typed_sheet_clear("A1:B2") is True


def test_get_current_sheet_data_uses_configured_window_when_metadata_fails():
    engine = make_sheet_engine(
        start_row=3,
        start_column="C",
        sheet_scan_max_rows=7,
        sheet_scan_max_cols=4,
    )
    engine._get_sheet_grid_properties = Mock(return_value=None)
    engine.api.get_sheet_data_chunked = Mock(return_value=[["ID"], [1]])

    result = engine.get_current_sheet_data()

    assert not result.empty
    assert engine._sheet_read_complete is False
    engine.api.get_sheet_data_chunked.assert_called_once_with(
        "sheet-token", "sh1", 3, 9, "C", "F"
    )


def test_formula_read_failure_restores_config_and_api_render_options():
    engine = make_sheet_engine(
        sheet_validate_results=True,
        sheet_value_render_option="ToString",
        sheet_datetime_render_option="FormattedString",
    )
    engine._get_sheet_grid_properties = Mock(return_value=(10, 3))
    engine.api.get_sheet_data_chunked = Mock(side_effect=RuntimeError("formula failed"))
    engine.get_current_sheet_data = Mock(return_value=pd.DataFrame({"ID": [1]}))

    result_df, formula_df, formula_columns = engine.get_sheet_data_with_validation()

    assert result_df.equals(pd.DataFrame({"ID": [1]}))
    assert formula_df is None
    assert formula_columns is None
    assert engine._sheet_target().value_render_option == "ToString"
    assert engine._sheet_target().datetime_render_option == "FormattedString"
    assert engine.api.value_render_option == "ToString"
    assert engine.api.datetime_render_option == "FormattedString"


def test_result_read_failure_restores_config_and_api_render_options():
    engine = make_sheet_engine(sheet_validate_results=True)
    engine._get_sheet_grid_properties = Mock(return_value=(10, 3))
    engine.api.get_sheet_data_chunked = Mock(
        side_effect=[[["ID"], [1]], RuntimeError("result failed")]
    )
    engine.get_current_sheet_data = Mock(return_value=pd.DataFrame({"ID": [1]}))

    result_df, formula_df, formula_columns = engine.get_sheet_data_with_validation()

    assert result_df.equals(pd.DataFrame({"ID": [1]}))
    assert formula_df is None
    assert formula_columns is None
    assert engine._sheet_target().value_render_option is None
    assert engine._sheet_target().datetime_render_option is None
    assert engine.api.value_render_option is None
    assert engine.api.datetime_render_option is None
