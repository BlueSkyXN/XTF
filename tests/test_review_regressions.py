"""Regression coverage from the independent 2026-09-05 source review.

These tests retain the real planner and SheetAPI interface; only remote I/O
is replaced.  Coordinates are asserted against physical cells, not a compact
DataFrame that can conceal misplaced writes.
"""

from copy import deepcopy
from decimal import Decimal
from unittest.mock import Mock

import pandas as pd
import pytest

from api import FeishuAPIError, SheetAPI
from core.config import SyncMode, TargetType
from core.converter import DataConverter
from core.key_policy import KeyPolicy
from core.plan import (
    AppendRowsAction,
    OutcomeStatus,
    WriteColumnsAction,
    WriteRangeAction,
)
from core.service import SyncService
from tests.conftest import attach_runtime, make_runtime_config
from tests.test_plan import make_file_bitable_engine


def sheet_service(values, *, mode="full", **options):
    engine = SyncService.__new__(SyncService)
    runtime = make_runtime_config(
        TargetType.SHEET,
        index_column=None if options.get("match_strategy") == "append_only" else "ID",
        sync_mode=mode,
        **options,
    )
    attach_runtime(engine, runtime)
    engine.logger = Mock()
    engine.converter = DataConverter(TargetType.SHEET)
    engine.api = SheetAPI(
        Mock(),
        Mock(),
        start_row=runtime.target.start_row,
        start_column=runtime.target.start_column,
    )
    engine.api.get_sheet_grid_properties = Mock(return_value=(20, 8))
    engine.api.get_sheet_data_chunked = Mock(
        side_effect=lambda *a, **k: deepcopy(values)
    )
    engine.api.get_sheet_data = Mock(side_effect=lambda *a, **k: deepcopy(values[:1]))
    engine._sheet_grid_cache = None
    engine._sheet_grid_cache_key = None
    engine._sheet_read_complete = True
    engine._last_sheet_read_range = None
    engine._last_bitable_read_result = None
    return engine


def test_review_noncontiguous_matched_rows_are_separate_physical_patches():
    engine = sheet_service([["ID", "Name"], [1, "old1"], [], [2, "old2"]])
    plan = engine.plan(pd.DataFrame({"ID": [1, 2], "Name": ["new1", "new2"]}))
    patches = [a for a in plan.actions if isinstance(a, WriteColumnsAction)]
    assert [(a.start_row, a.column_data["Name"]) for a in patches] == [
        (2, ("new1",)),
        (4, ("new2",)),
    ]


def test_review_blank_header_column_and_start_offset_preserve_column_positions():
    engine = sheet_service(
        [["ID", None, "Name"], [1, "untouched", "old"]], start_row=3, start_column="B"
    )
    plan = engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))
    patch = next(a for a in plan.actions if isinstance(a, WriteColumnsAction))
    assert patch.start_row == 4
    assert patch.column_positions == {"ID": 2, "Name": 4}


@pytest.mark.parametrize("mode", ["full", "incremental"])
def test_review_by_key_new_rows_follow_target_header_order(mode):
    engine = sheet_service([["Name", "ID"], ["old", 1]], mode=mode)
    plan = engine.plan(pd.DataFrame({"ID": [2], "Name": ["new"]}))
    append = next(a for a in plan.actions if isinstance(a, AppendRowsAction))
    assert append.values == (("new", 2),)


@pytest.mark.parametrize("mode", ["full", "incremental"])
def test_review_empty_by_key_target_receives_header_and_first_record(mode):
    engine = sheet_service([], mode=mode)
    plan = engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))
    assert isinstance(plan.actions[0], WriteRangeAction)
    assert plan.actions[0].values == (("ID", "Name"), (1, "new"))


@pytest.mark.parametrize("mode", ["full", "incremental"])
def test_review_header_only_target_retains_its_header(mode):
    engine = sheet_service([["Name", "ID"]], mode=mode)
    plan = engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))
    assert isinstance(plan.actions[0], AppendRowsAction)
    assert plan.actions[0].values == (("new", 1),)


def test_review_append_only_read_failure_is_not_treated_as_an_empty_target():
    engine = sheet_service([], mode="incremental", match_strategy="append_only")
    error = FeishuAPIError(99991668, "read denied")
    engine.api.get_sheet_data.side_effect = error
    engine.api.get_sheet_data_chunked.side_effect = error
    with pytest.raises((RuntimeError, FeishuAPIError)):
        engine.plan(pd.DataFrame({"Name": ["new"]}))


def test_review_nonempty_data_without_header_is_rejected():
    engine = sheet_service([[], [1, "data without header"]])
    with pytest.raises((ValueError, RuntimeError)):
        engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))


def test_review_duplicate_target_non_index_headers_are_rejected():
    engine = sheet_service([["ID", "Name", "Name"], [1, "first", "second"]])
    with pytest.raises((ValueError, RuntimeError)):
        engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))


def test_review_missing_target_column_fails_while_still_planning():
    engine = sheet_service([["ID"], [1]])
    with pytest.raises((ValueError, RuntimeError)):
        engine.plan(pd.DataFrame({"ID": [1, 2], "Name": ["new1", "new2"]}))


def test_review_inserted_blank_row_invalidates_planned_physical_coordinates():
    engine = sheet_service([["ID", "Name"], [1, "old"]])
    plan = engine.plan(pd.DataFrame({"ID": [1], "Name": ["new"]}))
    engine.api.get_sheet_data_chunked.side_effect = lambda *a, **k: [
        ["ID", "Name"],
        [],
        [1, "old"],
    ]
    engine._execute_action = Mock(return_value=True)
    outcome = engine.execute_plan(plan)
    assert outcome.status is OutcomeStatus.FAILED
    engine._execute_action.assert_not_called()


@pytest.mark.parametrize("columns", [["ID", "ID"], ["ID", ""], ["ID", " "]])
def test_review_ambiguous_source_headers_fail_before_destructive_plan(columns):
    engine = sheet_service([["ID", "Name"], [1, "old"]], mode="clone")
    with pytest.raises(ValueError):
        engine.plan(pd.DataFrame([[1, "new"]], columns=columns))


def test_review_datetime_key_is_converted_to_wire_milliseconds():
    converter = DataConverter(TargetType.BITABLE)
    result = converter.convert_strict_key_value(
        "2024-01-01T00:00:00Z", "When", {"When": 5}
    )
    assert result == 1704067200000
    assert isinstance(result, int)


def test_review_text_key_uses_a_text_payload():
    converter = DataConverter(TargetType.BITABLE)
    assert converter.convert_strict_key_value(123, "ID", {"ID": 1}) == "123"


def test_review_decimal_key_does_not_round_at_decimal_context_precision():
    policy = KeyPolicy()
    value = Decimal("12345678901234567890123456789012345")
    assert policy.normalize(value, 2).value == str(value)


def test_review_fractional_key_rejects_lossy_float_payload():
    converter = DataConverter(TargetType.BITABLE)
    with pytest.raises(ValueError):
        converter.convert_strict_key_value(
            "0.12345678901234567890123456789", "ID", {"ID": 2}
        )
