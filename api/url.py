"""Feishu OpenAPI URL construction helpers."""

from urllib.parse import quote


def encode_path_segment(value: object) -> str:
    """Encode one dynamic OpenAPI path segment without validating token shape."""
    return quote(str(value), safe="")


def encode_a1_range(value: object) -> str:
    """Encode an A1 range kept as one path suffix while preserving its separator."""
    return quote(str(value), safe="!")
