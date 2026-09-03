import json
from pathlib import Path

from xtf_cli import VERSION, main
from xtf_cli.parser import parse_args


def test_help_and_version_have_no_banner_or_files(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    assert main(["--help"]) == 0
    help_output = capsys.readouterr()
    assert "XTF 2.0" in help_output.out
    assert "usage: XTF" in help_output.out
    assert "====" not in help_output.out
    assert help_output.err == ""
    assert list(tmp_path.iterdir()) == []
    assert VERSION == "2.0.0-rc1"

    assert main(["--version"]) == 0
    version_output = capsys.readouterr()
    assert version_output.out == "XTF 2.0.0-rc1\n"
    assert version_output.err == ""
    assert list(tmp_path.iterdir()) == []


def test_root_and_legacy_flat_invocation_are_usage_errors(capsys):
    assert main([]) == 2
    root = capsys.readouterr()
    assert "XTF_E_USAGE" in root.err
    assert "XTF sync" in root.err
    assert "usage: XTF" in root.out

    assert main(["--target-type", "bitable"]) == 2
    legacy = capsys.readouterr()
    assert "XTF_E_USAGE" in legacy.err
    assert "sync" in legacy.err


def test_json_usage_error_is_single_stdout_object(capsys):
    assert main(["--json"]) == 2
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "XTF_E_USAGE"
    assert captured.out.count("\n") == 1


def test_single_parser_exposes_all_sync_config_leaves():
    args = parse_args(["sync"])
    actual = set(vars(args))
    expected = {
        "file_path",
        "app_id",
        "app_secret",
        "target_type",
        "source_type",
        "source_app_token",
        "source_table_id",
        "excel_sheet_name",
        "app_token",
        "table_id",
        "create_missing_fields",
        "bitable_api_backend",
        "bitable_user_id_type",
        "field_type_strategy",
        "intelligence_date_confidence",
        "intelligence_choice_confidence",
        "intelligence_boolean_confidence",
        "spreadsheet_token",
        "sheet_id",
        "start_row",
        "start_column",
        "sheet_value_render_option",
        "sheet_datetime_render_option",
        "sheet_scan_max_rows",
        "sheet_scan_max_cols",
        "sheet_write_max_rows",
        "sheet_write_max_cols",
        "sheet_validate_results",
        "sheet_protect_formulas",
        "sheet_verify_formulas",
        "sheet_formula_max_locations",
        "sheet_report_column_diff",
        "sheet_diff_tolerance",
        "sync_mode",
        "index_column",
        "datetime_index_granularity",
        "verify_remote_writes",
        "batch_size",
        "rate_limit_delay",
        "max_retries",
        "enable_advanced_control",
        "retry_strategy_type",
        "retry_initial_delay",
        "retry_max_wait_time",
        "retry_multiplier",
        "retry_increment",
        "rate_limit_strategy_type",
        "rate_limit_window_size",
        "rate_limit_max_requests",
        "log_level",
        "selective_enabled",
        "column",
        "auto_include_index",
        "optimize_ranges",
        "max_gap_for_merge",
        "preserve_column_order",
    }
    assert expected <= actual
    assert args.dry_run is None
    assert args.allow_delete is None
    doctor_args = parse_args(["doctor"])
    assert expected <= set(vars(doctor_args))
    assert "dry_run" not in vars(doctor_args)
    assert "allow_delete" not in vars(doctor_args)


def test_config_init_refuses_overwrite_and_force_replaces(tmp_path, capsys):
    path = tmp_path / "custom.yaml"
    assert main(["config", "init", "--target-type", "bitable", "-o", str(path)]) == 0
    content = path.read_text(encoding="utf-8")
    assert "schema_version: 2" in content
    assert "columns: []" in content
    capsys.readouterr()

    path.write_text("sentinel\n", encoding="utf-8")
    assert main(["config", "init", "--target-type", "bitable", "-o", str(path)]) == 3
    refused = capsys.readouterr()
    assert "XTF_E_CONFIG_EXISTS" in refused.err
    assert path.read_text(encoding="utf-8") == "sentinel\n"

    assert (
        main(
            [
                "config",
                "init",
                "--target-type",
                "bitable",
                "-o",
                str(path),
                "--force",
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert "schema_version: 2" in path.read_text(encoding="utf-8")


def test_config_init_builds_selected_shape(tmp_path, capsys):
    path = tmp_path / "sheet.yaml"
    assert (
        main(
            [
                "config",
                "init",
                "--source-type",
                "file",
                "--target-type",
                "sheet",
                "-o",
                str(path),
            ]
        )
        == 0
    )
    capsys.readouterr()
    content = path.read_text(encoding="utf-8")
    assert "type: sheet" in content
    assert "scan_max_columns: 100" in content
    assert "app_secret: null" in content
    assert "columns: []" in content


def test_config_init_defaults_to_file_bitable_template(tmp_path, capsys):
    path = tmp_path / "config.yaml"

    assert main(["config", "init", "-o", str(path)]) == 0

    content = path.read_text(encoding="utf-8")
    assert "schema_version: 2" in content
    assert "type: file" in content
    assert "type: bitable" in content
    assert capsys.readouterr().err == ""


def test_explicit_missing_config_is_exit_3(tmp_path, capsys):
    missing = tmp_path / "missing.yaml"
    assert main(["config", "validate", "-c", str(missing)]) == 3
    captured = capsys.readouterr()
    assert "XTF_E_CONFIG_NOT_FOUND" in captured.err
