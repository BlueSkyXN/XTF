import json
import copy
from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from xtf_cli import main
from xtf_cli.config import TEMPLATE, resolve_config
from xtf_cli.parser import parse_args


def v2_config(**overrides):
    config = {
        "schema_version": 2,
        "auth": {"app_id": "yaml-app", "app_secret": "yaml-secret"},
        "source": {
            "type": "file",
            "file": {"path": "data.xlsx", "sheet_name": None},
        },
        "target": {
            "type": "bitable",
            "bitable": {
                "app_token": "yaml-token",
                "table_id": "yaml-table",
                "create_missing_fields": True,
            },
        },
        "sync": {"mode": "full"},
        "conversion": {"strategy": "base"},
        "control": {
            "batch_size": 500,
            "rate_limit_delay": 0.01,
            "max_retries": 3,
            "advanced": {"enabled": False},
        },
        "output": {"log_level": "INFO"},
    }
    config.update(overrides)
    return config


def write_yaml(path: Path, value):
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")


@pytest.mark.parametrize(
    "document",
    [
        {"schema_version": 2, "app_id": "old-flat"},
        v2_config(unknown=True),
        v2_config(
            source={
                "type": "file",
                "file": {"path": "data.xlsx"},
                "bitable": {"app_token": "inactive"},
            }
        ),
        v2_config(
            target={
                "type": "bitable",
                "bitable": {"app_token": "x", "table_id": "y"},
                "sheet": {"sheet_id": "inactive"},
            }
        ),
    ],
)
def test_v2_schema_rejects_flat_unknown_and_inactive_branches(
    tmp_path, document, capsys
):
    path = tmp_path / "config.yaml"
    write_yaml(path, document)
    assert main(["config", "validate", "-c", str(path)]) == 3
    captured = capsys.readouterr()
    assert "XTF_E_CONFIG_INVALID" in captured.err


@pytest.mark.parametrize(
    ("path_parts", "invalid_value"),
    [
        (("target", "bitable", "create_missing_fields"), "false"),
        (("control", "batch_size"), True),
        (("sync", "selective", "columns"), "Name"),
        (("auth", "app_id"), 123),
    ],
)
def test_v2_schema_rejects_wrong_leaf_types(
    tmp_path, capsys, path_parts, invalid_value
):
    document = v2_config()
    node = document
    for part in path_parts[:-1]:
        node = node.setdefault(part, {})
    node[path_parts[-1]] = invalid_value
    path = tmp_path / "config.yaml"
    write_yaml(path, document)

    assert main(["config", "validate", "-c", str(path)]) == 3

    assert "XTF_E_CONFIG_INVALID" in capsys.readouterr().err


def test_full_fixed_example_allows_preconfigured_inactive_settings(tmp_path, capsys):
    document = copy.deepcopy(TEMPLATE)
    document["auth"]["app_secret"] = "test-secret"
    path = tmp_path / "config.yaml"
    write_yaml(path, document)
    assert main(["config", "validate", "-c", str(path)]) == 0
    assert "Valid v2 configuration" in capsys.readouterr().out


def test_precedence_cli_then_env_then_yaml_and_target_defaults(tmp_path):
    path = tmp_path / "config.yaml"
    document = v2_config()
    document["control"]["batch_size"] = 222
    write_yaml(path, document)

    args = parse_args(
        [
            "sync",
            "-c",
            str(path),
            "--app-secret",
            "cli-secret",
            "--batch-size",
            "333",
        ]
    )
    resolved = resolve_config(args, environ={"XTF_APP_SECRET": "env-secret"})
    assert resolved.config.app_secret == "cli-secret"
    assert resolved.config.batch_size == 333
    assert resolved.sources["app_secret"] == "cli"
    assert resolved.sources["batch_size"] == "cli"
    assert resolved.config.config_sources["app_secret"] == "cli"

    args = parse_args(["sync", "-c", str(path)])
    resolved = resolve_config(args, environ={"XTF_APP_SECRET": "env-secret"})
    assert resolved.config.app_secret == "env-secret"
    assert resolved.config.batch_size == 222
    assert resolved.sources["app_secret"] == "env:XTF_APP_SECRET"
    assert resolved.sources["batch_size"].startswith("yaml:")


def test_column_repeats_replace_yaml_columns(tmp_path):
    path = tmp_path / "config.yaml"
    document = v2_config()
    document["sync"]["selective"] = {
        "enabled": False,
        "columns": ["old"],
        "auto_include_index": True,
        "optimize_ranges": True,
        "max_gap_for_merge": 2,
        "preserve_column_order": True,
    }
    document["sync"]["index"] = {"column": "ID", "datetime_granularity": "exact"}
    write_yaml(path, document)
    args = parse_args(
        ["sync", "-c", str(path), "--column", "new-a", "--column", "new-b"]
    )
    resolved = resolve_config(args, environ={})
    assert resolved.config.selective_sync.columns == ["new-a", "new-b"]
    assert resolved.config.selective_sync.enabled is True
    assert resolved.sources["selective_sync.columns"] == "cli"
    assert resolved.sources["selective_sync.enabled"] == "cli"


def test_flags_only_requires_explicit_target_type(capsys):
    assert (
        main(
            [
                "sync",
                "--app-id",
                "flag-app",
                "--app-secret",
                "flag-secret",
                "--source-type",
                "file",
                "--file",
                "data.xlsx",
            ]
        )
        == 3
    )
    captured = capsys.readouterr()
    assert "XTF_E_CONFIG_INVALID" in captured.err
    assert "--target-type" in captured.err


def test_auto_discovery_and_flags_only(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    write_yaml(config_path, v2_config())
    monkeypatch.chdir(tmp_path)
    resolved = resolve_config(parse_args(["sync"]), environ={})
    assert resolved.path == config_path
    assert resolved.config.app_id == "yaml-app"

    config_path.unlink()
    args = parse_args(
        [
            "sync",
            "--app-id",
            "flag-app",
            "--app-secret",
            "flag-secret",
            "--source-type",
            "bitable",
            "--source-app-token",
            "source-token",
            "--source-table-id",
            "source-table",
            "--target-type",
            "bitable",
            "--target-app-token",
            "target-token",
            "--target-table-id",
            "target-table",
            "--index-column",
            "ID",
        ]
    )
    resolved = resolve_config(args, environ={})
    assert resolved.path is None
    assert resolved.config.app_id == "flag-app"


def test_show_redacts_all_secrets_and_tokens_and_reports_sources(tmp_path, capsys):
    path = tmp_path / "config.yaml"
    write_yaml(path, v2_config())
    assert main(["config", "show", "-c", str(path), "--json"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "yaml-secret" not in captured.out
    assert "yaml-token" not in captured.out
    payload = json.loads(captured.out)
    values = payload["result"]["values"]
    assert values["app_secret"] == "<redacted>"
    assert values["app_token"] == "<redacted>"
    assert payload["result"]["sources"]["app_secret"].startswith("yaml:")


def test_config_validate_is_local_and_does_not_require_input_file(tmp_path, capsys):
    path = tmp_path / "config.yaml"
    write_yaml(path, v2_config())
    assert main(["config", "validate", "-c", str(path)]) == 0
    captured = capsys.readouterr()
    assert "Valid v2 configuration" in captured.out
