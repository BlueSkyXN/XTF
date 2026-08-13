"""OpenAPI dynamic path encoding tests."""

from api.url import encode_a1_range, encode_path_segment


def test_encode_path_segment_keeps_plain_identifiers() -> None:
    assert encode_path_segment("tbl_123-ABC") == "tbl_123-ABC"


def test_encode_path_segment_confines_special_characters() -> None:
    assert encode_path_segment("../资源/一?x=1#frag\\tail") == (
        "..%2F%E8%B5%84%E6%BA%90%2F%E4%B8%80%3Fx%3D1%23frag%5Ctail"
    )


def test_encode_path_segment_does_not_trust_preencoded_input() -> None:
    assert encode_path_segment("abc%2Fdef") == "abc%252Fdef"


def test_encode_path_segment_accepts_empty_and_non_string_values() -> None:
    assert encode_path_segment("") == ""
    assert encode_path_segment(123) == "123"


def test_encode_a1_range_preserves_sheet_separator_only() -> None:
    assert encode_a1_range("数据表!A1:B2") == "%E6%95%B0%E6%8D%AE%E8%A1%A8!A1%3AB2"
