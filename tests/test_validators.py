#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
输入验证模块测试

模块概述：
    此模块测试 utils/validators.py 中的输入验证功能，包括令牌验证
    和文件路径验证。

测试覆盖：
    令牌验证测试（TestValidateToken）：
        - 有效令牌验证
        - 空令牌拒绝
        - 路径遍历序列拒绝
        - 无效字符拒绝
    
    飞书令牌验证测试（TestFeishuTokenValidation）：
        - app_token 验证
        - table_id 验证
        - spreadsheet_token 验证
        - sheet_id 验证
    
    文件路径验证测试（TestValidateFilePath）：
        - 有效路径验证
        - 路径遍历拒绝
        - 扩展名验证

测试策略：
    - 使用 pytest 进行单元测试
    - 验证正常情况和异常情况
    - 覆盖各种安全攻击场景

依赖关系：
    测试目标：
        - utils.validators 模块的所有函数
    测试工具：
        - pytest

作者: XTF Team
版本: 1.7.3+
"""

import pytest
from pathlib import Path

from utils.validators import (
    ValidationError,
    validate_token,
    validate_feishu_app_token,
    validate_feishu_table_id,
    validate_feishu_spreadsheet_token,
    validate_feishu_sheet_id,
    validate_file_path,
)


class TestValidateToken:
    """令牌验证测试"""

    def test_valid_token(self):
        """测试有效令牌"""
        assert validate_token("abc123", "test") == "abc123"
        assert validate_token("abc-123_def", "test") == "abc-123_def"
        assert validate_token("ABC123", "test") == "ABC123"

    def test_empty_token_rejected(self):
        """测试空令牌被拒绝"""
        with pytest.raises(ValidationError, match="不能为空"):
            validate_token("", "test")

    def test_none_token_rejected(self):
        """测试 None 令牌被拒绝"""
        with pytest.raises(ValidationError, match="必须是字符串类型"):
            validate_token(None, "test")

    def test_non_string_token_rejected(self):
        """测试非字符串类型令牌被拒绝"""
        with pytest.raises(ValidationError, match="必须是字符串类型"):
            validate_token(123, "test")
        
        with pytest.raises(ValidationError, match="必须是字符串类型"):
            validate_token(["abc"], "test")

    def test_path_traversal_rejected(self):
        """测试路径遍历被拒绝"""
        # 双点路径遍历
        with pytest.raises(ValidationError, match="非法字符序列"):
            validate_token("../../../etc", "test")
        
        # 斜杠
        with pytest.raises(ValidationError, match="非法字符序列"):
            validate_token("abc/def", "test")
        
        # 反斜杠
        with pytest.raises(ValidationError, match="非法字符序列"):
            validate_token("abc\\def", "test")

    def test_url_encoded_traversal_rejected(self):
        """测试 URL 编码的路径遍历被拒绝"""
        with pytest.raises(ValidationError, match="非法字符序列"):
            validate_token("abc%2e%2edef", "test")
        
        with pytest.raises(ValidationError, match="非法字符序列"):
            validate_token("abc%2fdef", "test")

    def test_invalid_characters_rejected(self):
        """测试无效字符被拒绝"""
        with pytest.raises(ValidationError, match="只能包含"):
            validate_token("abc@def", "test")
        
        with pytest.raises(ValidationError, match="只能包含"):
            validate_token("abc def", "test")
        
        with pytest.raises(ValidationError, match="只能包含"):
            validate_token("abc?def", "test")


class TestFeishuTokenValidation:
    """飞书令牌验证测试"""

    def test_valid_app_token(self):
        """测试有效的 app_token"""
        assert validate_feishu_app_token("bascnKXXXXXXXXXX") == "bascnKXXXXXXXXXX"
        assert validate_feishu_app_token("app_token_123") == "app_token_123"

    def test_invalid_app_token_traversal(self):
        """测试 app_token 路径遍历被拒绝"""
        with pytest.raises(ValidationError):
            validate_feishu_app_token("../../../v3/admin")

    def test_valid_table_id(self):
        """测试有效的 table_id"""
        assert validate_feishu_table_id("tblXXXXXXXXXXXXXX") == "tblXXXXXXXXXXXXXX"
        assert validate_feishu_table_id("table_id_123") == "table_id_123"

    def test_invalid_table_id_traversal(self):
        """测试 table_id 路径遍历被拒绝"""
        with pytest.raises(ValidationError):
            validate_feishu_table_id("../records/batch_delete")

    def test_valid_spreadsheet_token(self):
        """测试有效的 spreadsheet_token"""
        assert validate_feishu_spreadsheet_token("shtcnXXXXXXXXXXXX") == "shtcnXXXXXXXXXXXX"

    def test_invalid_spreadsheet_token_traversal(self):
        """测试 spreadsheet_token 路径遍历被拒绝"""
        with pytest.raises(ValidationError):
            validate_feishu_spreadsheet_token("../../admin/users")

    def test_valid_sheet_id(self):
        """测试有效的 sheet_id"""
        assert validate_feishu_sheet_id("0bc123") == "0bc123"
        assert validate_feishu_sheet_id("sheet_123") == "sheet_123"

    def test_invalid_sheet_id_traversal(self):
        """测试 sheet_id 路径遍历被拒绝"""
        with pytest.raises(ValidationError):
            validate_feishu_sheet_id("../../../admin")


class TestValidateFilePath:
    """文件路径验证测试"""

    def test_valid_path(self, tmp_path):
        """测试有效路径"""
        test_file = tmp_path / "data.xlsx"
        test_file.touch()
        
        result = validate_file_path(test_file, [".xlsx", ".csv"])
        assert result.exists()

    def test_path_traversal_rejected(self, tmp_path):
        """测试路径遍历被拒绝"""
        with pytest.raises(ValidationError, match="路径遍历"):
            validate_file_path(Path("../../../etc/passwd"))

    def test_extension_validation(self, tmp_path):
        """测试扩展名验证"""
        test_file = tmp_path / "data.txt"
        test_file.touch()
        
        with pytest.raises(ValidationError, match="不支持的文件扩展名"):
            validate_file_path(test_file, [".xlsx", ".csv"])

    def test_extension_case_insensitive(self, tmp_path):
        """测试扩展名大小写不敏感"""
        test_file = tmp_path / "data.XLSX"
        test_file.touch()
        
        result = validate_file_path(test_file, [".xlsx", ".csv"])
        assert result.exists()

    def test_no_extension_validation(self, tmp_path):
        """测试不验证扩展名时的行为"""
        test_file = tmp_path / "any_file.txt"
        test_file.touch()
        
        result = validate_file_path(test_file)
        assert result.exists()
