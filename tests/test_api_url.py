"""OpenAPI dynamic path encoding tests."""

from unittest.mock import Mock

import pytest
import requests

from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from api.sheet import SheetAPI
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


@pytest.mark.parametrize("segment", [".", ".."])
def test_encode_path_segment_rejects_dot_segments(segment: str) -> None:
    with pytest.raises(ValueError, match="路径段"):
        encode_path_segment(segment)


@pytest.mark.parametrize("segment", ["../admin", "%2e%2e", "//example.test", "a?b#c"])
def test_encoded_path_survives_request_preparation(segment: str) -> None:
    url = (
        "https://open.feishu.cn/open-apis/bitable/v1/apps/"
        f"{encode_path_segment(segment)}/tables/tbl_test/fields"
    )
    assert requests.Request("GET", url).prepare().url == url


@pytest.mark.parametrize("segment", [".", ".."])
@pytest.mark.parametrize("backend_type", [BitableV1Backend, BaseV3Backend])
@pytest.mark.parametrize("position", [0, 1])
def test_bitable_dot_segments_rejected_before_network(
    segment, backend_type, position
) -> None:
    auth, transport = Mock(), Mock()
    backend = backend_type(auth, transport)
    identifiers = ["app_test", "tbl_test"]
    identifiers[position] = segment
    with pytest.raises(ValueError, match="路径段"):
        backend.list_fields(*identifiers)
    auth.get_auth_headers.assert_not_called()
    transport.call_api.assert_not_called()


@pytest.mark.parametrize("segment", [".", ".."])
def test_sheet_dot_token_rejected_before_network(segment: str) -> None:
    auth, transport = Mock(), Mock()
    api = SheetAPI(auth, transport)
    with pytest.raises(ValueError, match="路径段"):
        api.query_sheets(segment)
    auth.get_auth_headers.assert_not_called()
    transport.call_api.assert_not_called()


def test_encode_a1_range_preserves_sheet_separator_only() -> None:
    assert encode_a1_range("数据表!A1:B2") == "%E6%95%B0%E6%8D%AE%E8%A1%A8!A1%3AB2"
