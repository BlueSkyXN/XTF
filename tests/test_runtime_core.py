"""XTF 2.0 immutable runtime, snapshot, and explicit bootstrap contracts."""

from dataclasses import FrozenInstanceError
import logging

import pandas as pd
import pytest

from api import BaseV3Backend
from api.bitable_backend import (
    BitableBackendKind,
    CanonicalRecord,
    FieldKind,
    FieldSchema,
    RecordReadResult,
)
from core.bootstrap import bootstrap_runtime
from core.config import SourceType, SyncMode, TargetType
from core.runtime_config import RuntimeBitableTarget, RuntimeSheetTarget
from core.snapshot import (
    BitableSnapshot,
    SheetSnapshot,
    SourceTable,
    content_fingerprint,
)
from tests.conftest import make_runtime_config


def test_runtime_config_is_nested_frozen_and_hides_secret(sample_bitable_config):
    runtime = sample_bitable_config

    assert isinstance(runtime.target, RuntimeBitableTarget)
    assert runtime.sync.index.column == "ID"
    assert runtime.sync.selective.columns == ()
    assert runtime.auth.app_secret not in repr(runtime)
    assert runtime.target.app_token not in repr(runtime)
    assert not hasattr(runtime, "sync_mode")
    assert not hasattr(runtime, "app_token")
    with pytest.raises(FrozenInstanceError):
        runtime.control.batch_size = 1


def test_runtime_config_preserves_target_specific_defaults(sample_sheet_config):
    runtime = sample_sheet_config

    assert isinstance(runtime.target, RuntimeSheetTarget)
    assert runtime.target.start_row == 1
    assert runtime.target.start_column == "A"
    assert runtime.target.write_max_rows == 5000
    assert runtime.target.write_max_cols == 100
    assert runtime.control.batch_size == 1000
    assert runtime.control.rate_limit_delay == 0.1


def test_runtime_config_rejects_explicit_clone_match_strategy():
    with pytest.raises(ValueError, match="省略 match_strategy"):
        make_runtime_config(
            sync_mode=SyncMode.CLONE.value,
            match_strategy="by_key",
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"batch_size": 0}, "batch_size"),
        ({"max_retries": -1}, "max_retries"),
        ({"rate_limit_delay": -0.1}, "rate_limit_delay"),
        ({"selective_sync": {"enabled": True, "columns": ["ID", "ID"]}}, "重复"),
        (
            {
                "selective_sync": {
                    "enabled": True,
                    "columns": ["ID"],
                    "max_gap_for_merge": 51,
                }
            },
            "max_gap_for_merge",
        ),
    ],
)
def test_runtime_config_rejects_invalid_control_and_selective_values(
    overrides, message
):
    with pytest.raises(ValueError, match=message):
        make_runtime_config(**overrides)


@pytest.mark.parametrize(
    "mode", [SyncMode.INCREMENTAL, SyncMode.OVERWRITE, SyncMode.CLONE]
)
def test_runtime_config_rejects_formula_protection_outside_full(mode):
    overrides = {
        "sync_mode": mode.value,
        "sheet_protect_formulas": True,
    }
    if mode is SyncMode.CLONE:
        overrides.update({"match_strategy": None, "index_column": None})

    with pytest.raises(ValueError, match="protect_formulas"):
        make_runtime_config(TargetType.SHEET, **overrides)


def test_runtime_config_rejects_invalid_bitable_source_combinations():
    with pytest.raises(ValueError, match="target_type=bitable"):
        make_runtime_config(
            TargetType.SHEET,
            source_type=SourceType.BITABLE.value,
            source_app_token="source-app",
            source_table_id="source-table",
        )

    with pytest.raises(ValueError, match="full 或 incremental"):
        make_runtime_config(
            TargetType.BITABLE,
            source_type=SourceType.BITABLE.value,
            source_app_token="source-app",
            source_table_id="source-table",
            sync_mode=SyncMode.OVERWRITE.value,
        )


def test_source_table_round_trips_without_retaining_mutable_dataframe():
    frame = pd.DataFrame({"ID": [1, 2], "Name": ["A", "B"]})

    source = SourceTable.from_dataframe(frame)
    frame.loc[0, "Name"] = "changed"

    assert source.columns == ("ID", "Name")
    assert source.rows[0] == (1, "A")
    assert source.to_dataframe().to_dict("records") == [
        {"ID": 1, "Name": "A"},
        {"ID": 2, "Name": "B"},
    ]


def test_bitable_snapshot_retains_revision_timezone_schema_and_completeness():
    result = RecordReadResult(
        records=(CanonicalRecord("rec-1", {"ID": 1}),),
        fields=(FieldSchema("id", "ID", FieldKind.NUMBER, raw_type="number"),),
        complete=True,
        backend=BitableBackendKind.BASE_V3,
        revision="rev-7",
        timezone="Asia/Shanghai",
        ignored_fields=({"field": "readonly"},),
    )

    snapshot = BitableSnapshot.from_result(result)

    assert snapshot.revision == "rev-7"
    assert snapshot.timezone == "Asia/Shanghai"
    assert snapshot.schema[0].name == "ID"
    assert snapshot.complete is True
    assert snapshot.ignored_fields == ({"field": "readonly"},)
    assert len(snapshot.fingerprint) == 64


def test_sheet_snapshot_retains_range_grid_header_mapping_and_fingerprint():
    snapshot = SheetSnapshot.from_dataframe(
        pd.DataFrame({"ID": [1], "Name": ["A"]}),
        actual_ranges=("sh1!A1:B2",),
        grid=(100, 20),
        index_mapping={"key": 0},
        formula_columns=("Formula",),
        complete=True,
    )

    assert snapshot.actual_ranges == ("sh1!A1:B2",)
    assert snapshot.grid == (100, 20)
    assert snapshot.header == ("ID", "Name")
    assert snapshot.index_mapping == (("key", 0),)
    assert snapshot.formula_columns == ("Formula",)
    assert len(snapshot.content_fingerprint) == 64


def test_content_fingerprint_is_stable_for_unordered_sets():
    first = content_fingerprint({"values": frozenset({"b", "a", "c"})})
    second = content_fingerprint({"values": {"c", "b", "a"}})

    assert first == second


def test_bootstrap_injects_logger_and_controller_without_process_global_state(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    runtime = make_runtime_config(
        enable_advanced_control=True,
        retry_initial_delay=0,
        rate_limit_delay=0,
    )

    dependencies = bootstrap_runtime(runtime)

    assert dependencies.logger.name == "XTF.service"
    log_files = list((tmp_path / "logs").glob("xtf_bitable_*.log"))
    assert log_files
    dependencies.logger.warning("token=%s", runtime.target.app_token)
    for handler in logging.getLogger("XTF").handlers:
        handler.flush()
    log_text = log_files[0].read_text(encoding="utf-8")
    assert runtime.target.app_token not in log_text
    assert "token=<redacted>" in log_text
    assert dependencies.controller is not None
    assert dependencies.transport._controller is dependencies.controller
    assert dependencies.auth.api_client is dependencies.transport
    assert isinstance(dependencies.target, BaseV3Backend)
    assert dependencies.target.api_client is dependencies.transport
    root = logging.getLogger("XTF")
    for handler in tuple(root.handlers):
        root.removeHandler(handler)
        handler.close()
