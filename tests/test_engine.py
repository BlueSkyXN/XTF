#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""同步引擎 SDK 装配和 fail-fast 批处理测试。"""

from unittest.mock import Mock, patch

import pandas as pd

from api.bitable_backend import (
    BitableBackend,
    BitableBackendKind,
    CanonicalRecord,
    MutationOutcome,
    MutationReceipt,
    ReadbackStatus,
)
from api.sheet import A1Range, FormulaVerificationResult, SheetAPI
from core.config import SelectiveSyncConfig, SyncConfig, SyncMode, TargetType
from core.converter import DataConverter
from core.engine import XTFSyncEngine


def make_sheet_engine(**config_overrides):
    config_data = {
        "file_path": "test.xlsx",
        "app_id": "cli_test",
        "app_secret": "test_secret",
        "target_type": TargetType.SHEET,
        "spreadsheet_token": "sheet-token",
        "sheet_id": "sh1",
        "index_column": "ID",
    }
    config_data.update(config_overrides)
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = SyncConfig(**config_data)
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.SHEET)
    auth = Mock()
    auth.get_auth_headers.return_value = {"Authorization": "Bearer fake"}
    engine.api = SheetAPI(
        auth,
        Mock(),
        start_row=engine.config.start_row,
        start_column=engine.config.start_column,
        value_render_option=engine.config.sheet_value_render_option,
        datetime_render_option=engine.config.sheet_datetime_render_option,
    )
    engine._sheet_grid_cache = None
    engine._sheet_grid_cache_key = None
    engine._sheet_read_complete = True
    return engine


@patch("core.engine.DataConverter")
@patch("core.engine.XTFFeishuClient")
@patch.object(XTFSyncEngine, "setup_logging")
@patch.object(XTFSyncEngine, "_init_global_controller")
def test_engine_uses_unified_sdk_client(
    mock_controller,
    mock_logging,
    mock_sdk_class,
    mock_converter,
    sample_bitable_config,
):
    sdk = mock_sdk_class.return_value
    api = sdk.bitable_backend.return_value

    engine = XTFSyncEngine(sample_bitable_config)

    mock_sdk_class.assert_called_once_with(
        sample_bitable_config.app_id,
        sample_bitable_config.app_secret,
        max_retries=sample_bitable_config.max_retries,
        rate_limit_delay=sample_bitable_config.rate_limit_delay,
    )
    assert engine.sdk is sdk
    assert engine.api_client is sdk.api_client
    assert engine.auth is sdk.auth
    assert engine.api is api
    sdk.bitable_backend.assert_called_once_with(
        backend="base_v3", user_id_type="open_id"
    )


def test_process_in_batches_stops_after_failure():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock()
    engine.config.target_type = TargetType.BITABLE
    engine.logger = Mock()
    calls = []

    def processor(app, table, batch):
        calls.append(batch)
        return len(calls) == 1

    result = engine.process_in_batches([1, 2, 3, 4, 5], 2, processor, "app", "table")

    assert result is False
    assert calls == [[1, 2], [3, 4]]


def test_typed_bitable_batches_use_backend_limit_and_stop_on_partial():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        target_type=TargetType.BITABLE,
        batch_size=500,
        app_token="app",
        table_id="table",
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
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        verify_remote_writes=True,
        app_token="app",
        table_id="table",
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
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        verify_remote_writes=True,
        app_token="app",
        table_id="table",
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
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        verify_remote_writes=True,
        app_token="app",
        table_id="table",
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
    ranges = XTFSyncEngine._merge_sheet_formula_ranges(
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


def test_sheet_selective_write_stops_after_first_failed_chunk():
    engine = make_sheet_engine()
    engine.api.write_max_rows = 1
    engine._typed_sheet_batch_update = Mock(side_effect=[True, False, True])

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
    assert engine._typed_sheet_batch_update.call_count == 2


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


def test_full_bitable_stops_before_create_when_update_fails():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        index_column="ID",
        batch_size=500,
        app_token="app",
        table_id="table",
    )
    engine.logger = Mock()
    engine.api = Mock(spec=BitableBackend)
    engine.converter = Mock()
    engine.get_all_bitable_records = Mock(
        return_value=[{"record_id": "rec1", "fields": {"ID": 1}}]
    )
    engine.get_field_types = Mock(return_value={})
    engine.converter.build_record_index.return_value = {
        "existing": {"record_id": "rec1"}
    }
    engine.converter.get_index_value_hash.side_effect = ["existing", "new"]
    engine.converter._is_empty_value.return_value = False
    engine.converter.convert_field_value_safe.side_effect = lambda _, value, __: value
    engine.converter._normalize_index_value.side_effect = lambda value, _: value
    engine.process_typed_bitable_batches = Mock(
        return_value=(
            False,
            [
                MutationReceipt(
                    operation="batch_update",
                    backend=BitableBackendKind.BASE_V3,
                    requested_count=1,
                    outcome=MutationOutcome.PARTIAL,
                )
            ],
        )
    )

    result = engine._sync_full_bitable(
        pd.DataFrame({"ID": [1, 2], "Name": ["updated", "new"]})
    )

    assert result is False
    engine.process_typed_bitable_batches.assert_called_once()


def test_clone_bitable_stops_before_create_when_delete_fails():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        index_column="ID",
        batch_size=500,
        app_token="app",
        table_id="table",
    )
    engine.logger = Mock()
    engine.api = Mock(spec=BitableBackend)
    engine.converter = Mock()
    engine.get_all_bitable_records = Mock(return_value=[{"record_id": "rec1"}])
    engine.get_field_types = Mock(return_value={})
    engine.process_typed_bitable_batches = Mock(return_value=(False, []))

    result = engine._sync_clone_bitable(pd.DataFrame({"ID": [1]}))

    assert result is False
    engine.process_typed_bitable_batches.assert_called_once()
    engine.converter.df_to_records.assert_not_called()


def test_overwrite_bitable_stops_before_create_when_delete_fails():
    engine = XTFSyncEngine.__new__(XTFSyncEngine)
    engine.config = Mock(
        index_column="ID",
        batch_size=500,
        app_token="app",
        table_id="table",
    )
    engine.logger = Mock()
    engine.api = Mock(spec=BitableBackend)
    engine.converter = Mock()
    engine.get_all_bitable_records = Mock(
        return_value=[{"record_id": "rec1", "fields": {"ID": 1}}]
    )
    engine.get_field_types = Mock(return_value={})
    engine.converter.build_record_index.return_value = {
        "existing": {"record_id": "rec1"}
    }
    engine.converter.get_index_value_hash.return_value = "existing"
    engine.process_typed_bitable_batches = Mock(return_value=(False, []))

    result = engine._sync_overwrite_bitable(pd.DataFrame({"ID": [1]}))

    assert result is False
    engine.process_typed_bitable_batches.assert_called_once()
    engine.converter.df_to_records.assert_not_called()


def test_clone_sheet_stops_before_write_when_clear_fails():
    engine = make_sheet_engine(sync_mode=SyncMode.CLONE)
    engine._build_sheet_full_range = Mock(return_value="A1:C10")
    engine._typed_sheet_clear = Mock(return_value=False)
    engine._typed_sheet_write = Mock(return_value=True)

    result = engine._sync_clone_sheet(pd.DataFrame({"ID": [1]}))

    assert result is False
    engine._typed_sheet_write.assert_not_called()


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


def test_incremental_sheet_stops_when_remote_read_is_incomplete():
    engine = make_sheet_engine(sync_mode=SyncMode.INCREMENTAL)
    engine.get_current_sheet_data = Mock(return_value=pd.DataFrame({"ID": [1]}))
    engine._sheet_read_complete = False
    engine.api.append_sheet_data = Mock(return_value=True)

    result = engine._sync_incremental_sheet(pd.DataFrame({"ID": [2]}))

    assert result is False
    engine.api.append_sheet_data.assert_not_called()


def test_overwrite_sheet_stops_when_remote_read_is_incomplete():
    engine = make_sheet_engine(sync_mode=SyncMode.OVERWRITE)
    engine.get_current_sheet_data = Mock(return_value=pd.DataFrame({"ID": [1]}))
    engine._sheet_read_complete = False
    engine.api.write_sheet_data = Mock(return_value=True)

    result = engine._sync_overwrite_sheet(pd.DataFrame({"ID": [1]}))

    assert result is False
    engine.api.write_sheet_data.assert_not_called()


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
    assert engine.config.sheet_value_render_option == "ToString"
    assert engine.config.sheet_datetime_render_option == "FormattedString"
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
    assert engine.config.sheet_value_render_option is None
    assert engine.config.sheet_datetime_render_option is None
    assert engine.api.value_render_option is None
    assert engine.api.datetime_render_option is None


def test_full_selective_sync_appends_only_effective_columns():
    engine = make_sheet_engine(
        selective_sync=SelectiveSyncConfig(
            enabled=True,
            columns=["Name"],
            auto_include_index=True,
            optimize_ranges=False,
        )
    )
    current_df = pd.DataFrame({"ID": [1], "Name": ["old"], "Manual": ["keep"]})
    local_df = pd.DataFrame(
        {"ID": [1, 2], "Name": ["updated", "new"], "Manual": ["x", "y"]}
    )
    engine.get_sheet_data_with_validation = Mock(return_value=(current_df, None, None))
    engine.get_current_sheet_data = Mock(return_value=current_df)
    engine._update_selective_columns = Mock(return_value=True)
    engine._typed_sheet_selective_write = Mock(return_value=True)

    assert engine._sync_full_sheet(local_df) is True

    call = engine._typed_sheet_selective_write.call_args
    assert set(call.args[0]) == {"ID", "Name"}
    assert "Manual" not in call.args[0]


def test_full_sheet_fails_when_protected_formula_is_index_column():
    engine = make_sheet_engine(
        sheet_protect_formulas=True,
        selective_sync=SelectiveSyncConfig(enabled=True, columns=["Name"]),
    )
    current_df = pd.DataFrame({"ID": [1], "Name": ["old"]})
    engine.get_sheet_data_with_validation = Mock(
        return_value=(current_df, current_df, {"ID"})
    )
    engine._sync_selective_columns_sheet = Mock(return_value=True)

    result = engine._sync_full_sheet(pd.DataFrame({"ID": [1], "Name": ["new"]}))

    assert result is False
    engine._sync_selective_columns_sheet.assert_not_called()


def test_full_sheet_formula_protection_does_not_clone_empty_remote_data():
    engine = make_sheet_engine(sheet_protect_formulas=True)
    engine.get_sheet_data_with_validation = Mock(
        return_value=(pd.DataFrame(), pd.DataFrame(), set())
    )
    engine.sync_clone = Mock(return_value=True)

    result = engine._sync_full_sheet(pd.DataFrame({"ID": [1], "Name": ["new"]}))

    assert result is False
    engine.sync_clone.assert_not_called()


def test_full_sheet_formula_protection_stops_when_formula_read_fails():
    engine = make_sheet_engine(sheet_protect_formulas=True)
    engine._get_sheet_grid_properties = Mock(return_value=(10, 2))
    engine.api.get_sheet_data_chunked = Mock(side_effect=RuntimeError("formula failed"))
    engine.get_current_sheet_data = Mock(
        return_value=pd.DataFrame({"ID": [1], "Formula": ["10"]})
    )
    engine.api.write_sheet_data = Mock(return_value=True)
    engine.api.append_sheet_data = Mock(return_value=True)
    engine.api.write_selective_columns = Mock(return_value=True)
    engine.sync_clone = Mock(return_value=True)

    result = engine._sync_full_sheet(pd.DataFrame({"ID": [1], "Formula": ["local"]}))

    assert result is False
    engine.api.write_sheet_data.assert_not_called()
    engine.api.append_sheet_data.assert_not_called()
    engine.api.write_selective_columns.assert_not_called()
    engine.sync_clone.assert_not_called()


def test_full_sheet_formula_protection_stops_when_result_read_fails():
    engine = make_sheet_engine(sheet_protect_formulas=True)
    engine._get_sheet_grid_properties = Mock(return_value=(10, 2))
    engine.api.get_sheet_data_chunked = Mock(
        side_effect=[
            [["ID", "Formula"], [1, "=A2*10"]],
            RuntimeError("result failed"),
        ]
    )
    engine.get_current_sheet_data = Mock(
        return_value=pd.DataFrame({"ID": [1], "Formula": ["10"]})
    )
    engine.api.write_sheet_data = Mock(return_value=True)
    engine.api.append_sheet_data = Mock(return_value=True)
    engine.api.write_selective_columns = Mock(return_value=True)
    engine.sync_clone = Mock(return_value=True)

    result = engine._sync_full_sheet(pd.DataFrame({"ID": [1], "Formula": ["local"]}))

    assert result is False
    engine.api.write_sheet_data.assert_not_called()
    engine.api.append_sheet_data.assert_not_called()
    engine.api.write_selective_columns.assert_not_called()
    engine.sync_clone.assert_not_called()


def test_full_sheet_formula_protection_never_rewrites_formula_columns():
    engine = make_sheet_engine(sheet_protect_formulas=True)
    current_df = pd.DataFrame({"ID": [1], "Formula": ["10"], "Name": ["old"]})
    formula_df = pd.DataFrame({"ID": [1], "Formula": ["=A2*10"], "Name": ["old"]})
    local_df = pd.DataFrame(
        {
            "ID": [1, 2],
            "Formula": ["local-ignored", "local-ignored"],
            "Name": ["updated", "new"],
        }
    )
    engine.get_sheet_data_with_validation = Mock(
        return_value=(current_df, formula_df, {"Formula"})
    )
    engine._typed_sheet_selective_write = Mock(return_value=True)
    engine._typed_sheet_write = Mock(return_value=True)
    engine._typed_sheet_append = Mock(return_value=True)

    assert engine._sync_full_sheet(local_df) is True

    assert engine._typed_sheet_selective_write.call_count == 2
    for call in engine._typed_sheet_selective_write.call_args_list:
        assert set(call.args[0]) == {"ID", "Name"}
        assert "Formula" not in call.args[0]
    engine._typed_sheet_write.assert_not_called()
    engine._typed_sheet_append.assert_not_called()


def test_overwrite_sheet_hashes_each_new_index_once():
    engine = make_sheet_engine(sync_mode=SyncMode.OVERWRITE)
    current_df = pd.DataFrame({"ID": [1, 2, 3], "Name": ["a", "b", "c"]})
    new_df = pd.DataFrame({"ID": [2, 4], "Name": ["new-b", "d"]})
    engine.get_current_sheet_data = Mock(return_value=current_df)
    engine._typed_sheet_write = Mock(return_value=True)
    original_hash = engine.converter.get_index_value_hash
    hash_calls = []

    def counting_hash(row, index_column, field_types=None):
        hash_calls.append(row["ID"])
        return original_hash(row, index_column, field_types)

    engine.converter.get_index_value_hash = Mock(side_effect=counting_hash)

    assert engine._sync_overwrite_sheet(new_df) is True

    assert hash_calls == [2, 4, 1, 2, 3]
    written_values = engine._typed_sheet_write.call_args.args[0]
    assert written_values[0] == ["ID", "Name"]
    assert [row[0] for row in written_values[1:]] == [1, 3, 2, 4]
