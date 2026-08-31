"""XTF 2.0 immutable runtime, snapshot, and explicit bootstrap contracts."""

from dataclasses import FrozenInstanceError

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
from core.runtime_config import RuntimeBitableTarget, RuntimeConfig
from core.snapshot import (
    BitableSnapshot,
    SheetSnapshot,
    SourceTable,
    content_fingerprint,
)


def test_runtime_config_is_nested_frozen_and_hides_secret(sample_bitable_config):
    runtime = RuntimeConfig.from_sync_config(sample_bitable_config)

    assert isinstance(runtime.target, RuntimeBitableTarget)
    assert runtime.sync.index.column == sample_bitable_config.index_column
    assert runtime.sync.selective.columns == ()
    assert sample_bitable_config.app_secret not in repr(runtime)
    assert sample_bitable_config.app_token not in repr(runtime)
    with pytest.raises(FrozenInstanceError):
        runtime.control.batch_size = 1


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


def test_bootstrap_injects_controller_without_process_global_state(
    sample_bitable_config,
):
    sample_bitable_config.enable_advanced_control = True

    dependencies = bootstrap_runtime(sample_bitable_config)

    assert dependencies.controller is not None
    assert dependencies.transport._controller is dependencies.controller
    assert dependencies.auth.api_client is dependencies.transport
    assert isinstance(dependencies.target, BaseV3Backend)
    assert dependencies.target.api_client is dependencies.transport
