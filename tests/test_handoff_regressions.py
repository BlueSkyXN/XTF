"""Cases found while comparing the parallel candidates for local handoff."""

from unittest.mock import Mock

import pytest

from api import BaseV3Backend, CanonicalRecord, MutationReceipt, RecordReadResult
from core.config import SyncMode
from core.plan import (
    CreateFieldAction,
    CreateRecordsAction,
    ExecutionPlan,
    OutcomeStatus,
)
from tests.test_api_bitable_v3 import matrix
from tests.test_plan import make_file_bitable_engine, make_remote_engine


def datetime_read(value, zone):
    page = BaseV3Backend._parse_matrix(
        matrix(
            [[value, "A"]],
            fields=("When", "Name"),
            field_ids=("when", "name"),
            types=("datetime", "text"),
            timezone=zone,
        )["data"]
    )
    return RecordReadResult(
        page.records, page.fields, True, BaseV3Backend.api_family, timezone=zone
    )


@pytest.mark.parametrize(
    "value,zone",
    [
        ("2026-03-24 10:00:00", "Asia/Shanghai"),
        ("2026-03-24 02:00:00", "UTC"),
        ("2026-03-24T10:00:00+08:00", "America/Los_Angeles"),
        ("2026-03-24T02:00:00Z", "Asia/Shanghai"),
    ],
)
def test_base_datetime_read_keeps_the_instant(value, zone):
    result = datetime_read(value, zone)
    assert result.records[0].fields["When"] == 1774317600000
    assert result.timezone == zone


@pytest.mark.parametrize("value", [None, "", 0, 1774317600000])
def test_base_datetime_read_preserves_empty_and_numeric_values(value):
    assert datetime_read(value, "Asia/Shanghai").records[0].fields["When"] == value


def test_same_instant_in_different_bases_is_not_created_again():
    engine = make_remote_engine()
    source = datetime_read("2026-03-24 10:00:00", "Asia/Shanghai")
    target = datetime_read("2026-03-24 02:00:00", "UTC")
    engine.api.list_fields.return_value = source.fields
    engine.api.list_records.side_effect = [source, target]
    plan = engine.plan()
    assert plan.actions == ()
    engine.api.batch_create.assert_not_called()
    engine.api.batch_update.assert_not_called()


def test_copy_new_record_uses_milliseconds_not_target_local_clock():
    engine = make_remote_engine()
    source = datetime_read("2026-03-24 10:00:00", "Asia/Shanghai")
    target = RecordReadResult(
        (), source.fields, True, BaseV3Backend.api_family, timezone="UTC"
    )
    engine.api.list_fields.return_value = source.fields
    engine.api.list_records.side_effect = [source, target]
    plan = engine.plan()
    assert len(plan.actions) == 1
    action = plan.actions[0]
    assert isinstance(action, CreateRecordsAction)
    # Use the real target codec as well, not just the planner's representation.
    api = BaseV3Backend(Mock())
    value = api._encode_value(source.fields[0], action.records[0].fields["When"])
    assert value == 1774317600000


def test_offset_distinguishes_both_occurrences_of_fall_back_hour():
    first = datetime_read("2026-11-01T01:30:00-07:00", "America/Los_Angeles")
    second = datetime_read("2026-11-01T01:30:00-08:00", "America/Los_Angeles")
    assert second.records[0].fields["When"] - first.records[0].fields["When"] == 3600000


@pytest.mark.parametrize("with_records", [False, True])
def test_field_visibility_timeout_reports_accepted_operation(with_records):
    engine = make_file_bitable_engine(SyncMode.FULL)
    engine.api.create_field.return_value = MutationReceipt(
        "create_field", "base_v3", 1, 1, unit="field"
    )
    engine.api.list_fields.return_value = ()
    actions = [CreateFieldAction("New", 1)]
    if with_records:
        actions.append(
            CreateRecordsAction(records=(CanonicalRecord(None, {"New": "A"}),))
        )
    outcome = engine.execute_plan(ExecutionPlan("full", "full", {}, {}, tuple(actions)))
    assert outcome.status is OutcomeStatus.PARTIAL
    assert outcome.summary()["accepted_by_unit"] == {"field": 1}
    assert outcome.summary()["confirmation_complete"] is False
    assert outcome.error["confirmation"]["status"] == "visibility_timeout"
    assert outcome.error["confirmation"]["attempts"] > 1
    assert outcome.error["accepted_units"] == 1
    assert outcome.verification[0]["ok"] is False
    assert outcome.verification[0]["status"] == "visibility_timeout"
    engine.api.create_field.assert_called_once()
    engine.api.batch_create.assert_not_called()
