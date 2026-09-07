"""Feishu OpenAPI URL construction helpers."""

from urllib.parse import quote


def encode_path_segment(value: object) -> str:
    """Encode one dynamic OpenAPI path segment without validating token shape."""
    segment = str(value)
    # quote 保留点号；HTTP 客户端会将独立点段规范化并改变 API 路径。
    if segment in {".", ".."}:
        raise ValueError("OpenAPI 路径段不能为 '.' 或 '..'")
    return quote(segment, safe="")


def encode_a1_range(value: object) -> str:
    """Encode an A1 range kept as one path suffix while preserving its separator."""
    return quote(str(value), safe="!")
