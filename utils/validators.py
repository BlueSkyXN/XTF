#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
输入验证模块

模块概述：
    此模块提供输入验证功能，用于防止安全漏洞，包括：
    - 令牌格式验证（防止 SSRF/路径遍历攻击）
    - 文件路径验证（防止任意文件读取）

主要功能：
    1. 验证飞书 API 令牌格式
    2. 验证文件路径安全性

安全考虑：
    - 令牌只允许字母、数字、下划线和短横线
    - 文件路径检查是否包含路径遍历序列

作者: XTF Team
版本: 1.7.3+
更新日期: 2026-01-31
"""

import re
from pathlib import Path
from typing import Optional


# 令牌格式正则表达式：只允许字母、数字、下划线和短横线
TOKEN_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')


class ValidationError(ValueError):
    """验证错误异常"""
    pass


def validate_token(token: str, token_type: str = "token") -> str:
    """
    验证令牌格式，防止 SSRF 和路径遍历攻击
    
    Args:
        token: 要验证的令牌值
        token_type: 令牌类型名称（用于错误消息）
    
    Returns:
        str: 验证通过的令牌
    
    Raises:
        ValidationError: 当令牌格式无效时
    
    Examples:
        >>> validate_token("abc123_def-ghi", "app_token")
        'abc123_def-ghi'
        >>> validate_token("../../../etc", "app_token")  # 抛出 ValidationError
    """
    # 首先检查类型
    if not isinstance(token, str):
        raise ValidationError(f"无效的 {token_type}: 必须是字符串类型")
    
    # 然后检查是否为空
    if not token:
        raise ValidationError(f"无效的 {token_type}: 不能为空")
    
    # 检查是否包含路径遍历序列
    dangerous_patterns = ['..', '/', '\\', '%2e', '%2f', '%5c']
    token_lower = token.lower()
    for pattern in dangerous_patterns:
        if pattern in token_lower:
            raise ValidationError(
                f"无效的 {token_type}: 包含非法字符序列 '{pattern}'"
            )
    
    # 检查是否符合预期格式
    if not TOKEN_PATTERN.match(token):
        raise ValidationError(
            f"无效的 {token_type}: 只能包含字母、数字、下划线和短横线"
        )
    
    return token


def validate_feishu_app_token(app_token: str) -> str:
    """
    验证飞书多维表格应用令牌
    
    Args:
        app_token: 多维表格应用令牌
    
    Returns:
        str: 验证通过的令牌
    
    Raises:
        ValidationError: 当令牌格式无效时
    """
    return validate_token(app_token, "app_token")


def validate_feishu_table_id(table_id: str) -> str:
    """
    验证飞书多维表格数据表 ID
    
    Args:
        table_id: 数据表 ID
    
    Returns:
        str: 验证通过的 ID
    
    Raises:
        ValidationError: 当 ID 格式无效时
    """
    return validate_token(table_id, "table_id")


def validate_feishu_spreadsheet_token(spreadsheet_token: str) -> str:
    """
    验证飞书电子表格令牌
    
    Args:
        spreadsheet_token: 电子表格令牌
    
    Returns:
        str: 验证通过的令牌
    
    Raises:
        ValidationError: 当令牌格式无效时
    """
    return validate_token(spreadsheet_token, "spreadsheet_token")


def validate_feishu_sheet_id(sheet_id: str) -> str:
    """
    验证飞书工作表 ID
    
    Args:
        sheet_id: 工作表 ID
    
    Returns:
        str: 验证通过的 ID
    
    Raises:
        ValidationError: 当 ID 格式无效时
    """
    return validate_token(sheet_id, "sheet_id")


def validate_file_path(
    file_path: Path,
    allowed_extensions: Optional[list] = None
) -> Path:
    """
    验证文件路径的安全性
    
    检查：
    1. 路径不包含危险的遍历序列
    2. 文件扩展名在允许列表中（如果提供）
    
    Args:
        file_path: 要验证的文件路径
        allowed_extensions: 允许的文件扩展名列表（如 ['.xlsx', '.csv']）
    
    Returns:
        Path: 验证通过的路径（已解析为绝对路径）
    
    Raises:
        ValidationError: 当路径不安全或扩展名不允许时
    
    Examples:
        >>> validate_file_path(Path("data.xlsx"), [".xlsx", ".csv"])
        PosixPath('/path/to/data.xlsx')
    """
    if file_path is None:
        raise ValidationError("文件路径不能为空")
    
    # 转换为字符串进行检查
    path_str = str(file_path)
    
    # 检查是否为空路径
    if not path_str or path_str == '.':
        raise ValidationError("文件路径不能为空")
    
    # 检查路径遍历序列
    if '..' in path_str:
        raise ValidationError(
            f"不安全的文件路径: 包含路径遍历序列 '..'"
        )
    
    # 解析为绝对路径
    resolved_path = file_path.resolve()
    
    # 检查扩展名
    if allowed_extensions:
        ext = resolved_path.suffix.lower()
        if ext not in [e.lower() for e in allowed_extensions]:
            raise ValidationError(
                f"不支持的文件扩展名: {ext}，允许的扩展名: {allowed_extensions}"
            )
    
    return resolved_path
