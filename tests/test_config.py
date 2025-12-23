#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块测试
测试 core/config.py 中的配置管理功能
"""

from pathlib import Path

import pytest
import yaml

from core.config import (
    SyncConfig,
    SelectiveSyncConfig,
    ConfigManager,
    TargetType,
    SyncMode,
    FieldTypeStrategy,
    create_sample_config,
    get_target_description,
)


class TestEnums:
    """枚举类型测试"""

    def test_target_type_values(self):
        """测试目标类型枚举值"""
        assert TargetType.BITABLE.value == "bitable"
        assert TargetType.SHEET.value == "sheet"

    def test_sync_mode_values(self):
        """测试同步模式枚举值"""
        assert SyncMode.FULL.value == "full"
        assert SyncMode.INCREMENTAL.value == "incremental"
        assert SyncMode.OVERWRITE.value == "overwrite"
        assert SyncMode.CLONE.value == "clone"

    def test_field_type_strategy_values(self):
        """测试字段类型策略枚举值"""
        assert FieldTypeStrategy.RAW.value == "raw"
        assert FieldTypeStrategy.BASE.value == "base"
        assert FieldTypeStrategy.AUTO.value == "auto"
        assert FieldTypeStrategy.INTELLIGENCE.value == "intelligence"


class TestSelectiveSyncConfig:
    """选择性同步配置测试"""

    def test_default_values(self):
        """测试默认值"""
        config = SelectiveSyncConfig()
        assert config.enabled is False
        assert config.columns is None
        assert config.auto_include_index is True
        assert config.optimize_ranges is True
        assert config.max_gap_for_merge == 2
        assert config.preserve_column_order is True

    def test_custom_values(self):
        """测试自定义值"""
        config = SelectiveSyncConfig(
            enabled=True,
            columns=["A", "B", "C"],
            auto_include_index=False,
            optimize_ranges=False,
            max_gap_for_merge=5,
            preserve_column_order=False,
        )
        assert config.enabled is True
        assert config.columns == ["A", "B", "C"]
        assert config.auto_include_index is False
        assert config.optimize_ranges is False
        assert config.max_gap_for_merge == 5
        assert config.preserve_column_order is False


class TestSyncConfig:
    """同步配置测试"""

    def test_bitable_config_creation(self, sample_bitable_config):
        """测试多维表格配置创建"""
        config = sample_bitable_config
        assert config.target_type == TargetType.BITABLE
        assert config.app_token == "test_app_token"
        assert config.table_id == "test_table_id"
        assert config.batch_size == 500
        assert config.create_missing_fields is True

    def test_sheet_config_creation(self, sample_sheet_config):
        """测试电子表格配置创建"""
        config = sample_sheet_config
        assert config.target_type == TargetType.SHEET
        assert config.spreadsheet_token == "test_spreadsheet_token"
        assert config.sheet_id == "test_sheet_id"
        assert config.start_row == 1
        assert config.start_column == "A"
        assert config.batch_size == 1000

    def test_bitable_config_missing_app_token(self):
        """测试多维表格配置缺少 app_token 时的错误"""
        with pytest.raises(ValueError, match="多维表格模式需要app_token和table_id"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.BITABLE,
                table_id="test_table_id",
                # 缺少 app_token
            )

    def test_bitable_config_missing_table_id(self):
        """测试多维表格配置缺少 table_id 时的错误"""
        with pytest.raises(ValueError, match="多维表格模式需要app_token和table_id"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.BITABLE,
                app_token="test_app_token",
                # 缺少 table_id
            )

    def test_sheet_config_missing_spreadsheet_token(self):
        """测试电子表格配置缺少 spreadsheet_token 时的错误"""
        with pytest.raises(
            ValueError, match="电子表格模式需要spreadsheet_token和sheet_id"
        ):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                sheet_id="test_sheet_id",
                # 缺少 spreadsheet_token
            )

    def test_sheet_config_missing_sheet_id(self):
        """测试电子表格配置缺少 sheet_id 时的错误"""
        with pytest.raises(
            ValueError, match="电子表格模式需要spreadsheet_token和sheet_id"
        ):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                # 缺少 sheet_id
            )

    def test_string_to_enum_conversion(self):
        """测试字符串到枚举的自动转换"""
        config = SyncConfig(
            file_path="test.xlsx",
            app_id="test_id",
            app_secret="test_secret",
            target_type="bitable",  # 字符串
            sync_mode="full",  # 字符串
            field_type_strategy="base",  # 字符串
            app_token="test_app_token",
            table_id="test_table_id",
        )
        assert config.target_type == TargetType.BITABLE
        assert config.sync_mode == SyncMode.FULL
        assert config.field_type_strategy == FieldTypeStrategy.BASE

    def test_selective_sync_clone_mode_error(self):
        """测试 selective sync 与 clone 模式不兼容"""
        with pytest.raises(ValueError, match="Clone 模式不支持 selective 同步"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                sync_mode=SyncMode.CLONE,
                selective_sync=SelectiveSyncConfig(enabled=True, columns=["A", "B"]),
            )

    def test_selective_sync_enabled_without_columns(self):
        """测试启用 selective sync 但未指定列时的错误"""
        with pytest.raises(ValueError, match="启用 selective 同步时必须指定 columns"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(enabled=True, columns=None),
            )

    def test_selective_sync_duplicate_columns(self):
        """测试 selective sync 包含重复列名时的错误"""
        with pytest.raises(ValueError, match="包含重复的列名"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(
                    enabled=True, columns=["A", "B", "A"]
                ),
            )

    def test_selective_sync_empty_column_name(self):
        """测试 selective sync 包含空列名时的错误"""
        with pytest.raises(ValueError, match="不能为空字符串"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(
                    enabled=True, columns=["A", "", "B"]
                ),
            )

    def test_selective_sync_none_column(self):
        """测试 selective sync 包含 None 列名时的错误"""
        with pytest.raises(ValueError, match="不能为 None"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(
                    enabled=True, columns=["A", None, "B"]
                ),
            )

    def test_selective_sync_max_gap_negative(self):
        """测试 max_gap_for_merge 为负数时的错误"""
        with pytest.raises(ValueError, match="不能为负数"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(
                    enabled=True, columns=["A", "B"], max_gap_for_merge=-1
                ),
            )

    def test_selective_sync_max_gap_too_large(self):
        """测试 max_gap_for_merge 超过上限时的错误"""
        with pytest.raises(ValueError, match="不应超过50"):
            SyncConfig(
                file_path="test.xlsx",
                app_id="test_id",
                app_secret="test_secret",
                target_type=TargetType.SHEET,
                spreadsheet_token="test_spreadsheet_token",
                sheet_id="test_sheet_id",
                selective_sync=SelectiveSyncConfig(
                    enabled=True, columns=["A", "B"], max_gap_for_merge=100
                ),
            )


class TestConfigManager:
    """配置管理器测试"""

    def test_load_from_file(self, temp_config_file):
        """测试从文件加载配置"""
        config_data = ConfigManager.load_from_file(str(temp_config_file))
        assert config_data is not None
        assert config_data["file_path"] == "test_data.xlsx"
        assert config_data["app_id"] == "cli_test_app_id"
        assert config_data["target_type"] == "bitable"

    def test_load_from_nonexistent_file(self, tmp_path, capsys):
        """测试从不存在的文件加载配置"""
        result = ConfigManager.load_from_file(str(tmp_path / "nonexistent.yaml"))
        assert result is None
        captured = capsys.readouterr()
        assert "配置文件不存在" in captured.out

    def test_load_from_invalid_yaml_file(self, tmp_path, capsys):
        """测试从无效 YAML 文件加载配置"""
        invalid_file = tmp_path / "invalid.yaml"
        with open(invalid_file, "w") as f:
            f.write("invalid: yaml: content: [")

        result = ConfigManager.load_from_file(str(invalid_file))
        assert result is None
        captured = capsys.readouterr()
        assert "YAML配置文件格式错误" in captured.out

    def test_save_to_file(self, tmp_path, sample_config_dict):
        """测试保存配置到文件"""
        file_path = tmp_path / "saved_config.yaml"
        ConfigManager.save_to_file(sample_config_dict, str(file_path))

        assert file_path.exists()

        with open(file_path, "r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)

        assert loaded["file_path"] == sample_config_dict["file_path"]
        assert loaded["app_id"] == sample_config_dict["app_id"]


class TestCreateSampleConfig:
    """示例配置创建测试"""

    def test_create_bitable_sample_config(self, tmp_path):
        """测试创建多维表格示例配置"""
        config_file = str(tmp_path / "sample_bitable.yaml")
        result = create_sample_config(config_file, TargetType.BITABLE)

        assert result is True
        assert Path(config_file).exists()

        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        assert config["target_type"] == "bitable"
        assert "app_token" in config
        assert "table_id" in config

    def test_create_sheet_sample_config(self, tmp_path):
        """测试创建电子表格示例配置"""
        config_file = str(tmp_path / "sample_sheet.yaml")
        result = create_sample_config(config_file, TargetType.SHEET)

        assert result is True
        assert Path(config_file).exists()

        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        assert config["target_type"] == "sheet"
        assert "spreadsheet_token" in config
        assert "sheet_id" in config
        assert "selective_sync" in config

    def test_create_sample_config_file_exists(self, tmp_path, capsys):
        """测试当配置文件已存在时不覆盖"""
        config_file = tmp_path / "existing.yaml"
        config_file.write_text("existing content")

        result = create_sample_config(str(config_file), TargetType.BITABLE)

        assert result is False
        assert config_file.read_text() == "existing content"
        captured = capsys.readouterr()
        assert "已存在" in captured.out


class TestGetTargetDescription:
    """目标类型描述测试"""

    def test_bitable_description(self):
        """测试多维表格描述"""
        desc = get_target_description(TargetType.BITABLE)
        assert "多维表格" in desc

    def test_sheet_description(self):
        """测试电子表格描述"""
        desc = get_target_description(TargetType.SHEET)
        assert "电子表格" in desc
