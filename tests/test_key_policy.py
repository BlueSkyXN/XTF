from decimal import Decimal

import pandas as pd
import pytest

from core.key_policy import KeyPolicy


def test_large_integer_keys_remain_distinct_without_float_coercion():
    policy = KeyPolicy()

    first = policy.normalize(9007199254740992, 2)
    second = policy.normalize(9007199254740993, 2)
    text = policy.normalize("9007199254740993", 2)

    assert first is not None and second is not None and text is not None
    assert first.value == "9007199254740992"
    assert second.value == text.value == "9007199254740993"
    assert first.digest != second.digest


def test_decimal_equivalents_share_one_canonical_key():
    policy = KeyPolicy()

    assert policy.normalize(Decimal("1.00"), 2) == policy.normalize("1", 2)


def test_integer_like_float_beyond_2_to_53_fails_closed():
    policy = KeyPolicy()

    with pytest.raises(ValueError, match=r"exceeds 2\^53"):
        policy.normalize(float(9007199254740994), 2)


def test_source_empty_key_fails_but_target_empty_key_is_counted():
    policy = KeyPolicy()
    rows = [pd.Series({"ID": 1}), pd.Series({"ID": None})]

    with pytest.raises(ValueError, match="key 为空"):
        policy.build_index(
            rows,
            value_getter=lambda row: row["ID"],
            field_type=2,
            context="本地数据",
            allow_empty=False,
        )

    target = policy.build_index(
        rows,
        value_getter=lambda row: row["ID"],
        field_type=2,
        context="目标数据",
        allow_empty=True,
    )
    assert len(target.items) == 1
    assert target.empty_count == 1


def test_day_keys_use_explicit_zoneinfo_for_aware_and_epoch_values():
    policy = KeyPolicy(datetime_granularity="day", datetime_timezone="Asia/Shanghai")
    instant = pd.Timestamp("2026-08-30 23:30:00+00:00")

    assert policy.normalize_datetime(instant) == "2026-08-31"
    assert policy.normalize_datetime(int(instant.timestamp())) == "2026-08-31"


@pytest.mark.parametrize("timezone", [None, "", "Not/A_Real_Zone"])
def test_day_keys_require_valid_iana_timezone(timezone):
    with pytest.raises(ValueError, match="timezone"):
        KeyPolicy(datetime_granularity="day", datetime_timezone=timezone)
