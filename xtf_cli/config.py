"""Strict v2 YAML loading, precedence, source tracking, and redaction."""

from __future__ import annotations

import argparse
import copy
import os
from collections.abc import Mapping, MutableMapping
from dataclasses import MISSING, dataclass, fields
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from core.config import SelectiveSyncConfig, SyncConfig

from .errors import EXIT_CONFIG, CLIError

SCHEMA_VERSION = 2
ENV_APP_SECRET = "XTF_APP_SECRET"

TEMPLATE: dict[str, Any] = {
    "schema_version": SCHEMA_VERSION,
    "auth": {"app_id": "cli_your_app_id", "app_secret": None},
    "source": {
        "type": "file",
        "file": {"path": "data.xlsx", "sheet_name": None},
    },
    "target": {
        "type": "bitable",
        "bitable": {
            "app_token": "your_app_token",
            "table_id": "your_table_id",
            "create_missing_fields": True,
            "api_backend": "base_v3",
            "user_id_type": "open_id",
        },
    },
    "sync": {
        "mode": "full",
        "index": {"column": "ID", "datetime_granularity": "exact"},
        "verify_remote_writes": False,
        "selective": {
            "enabled": False,
            "columns": [],
            "auto_include_index": True,
            "optimize_ranges": True,
            "max_gap_for_merge": 2,
            "preserve_column_order": True,
        },
    },
    "conversion": {
        "strategy": "base",
        "intelligence": {
            "date_confidence": 0.85,
            "choice_confidence": 0.9,
            "boolean_confidence": 0.95,
        },
    },
    "control": {
        "batch_size": 500,
        "rate_limit_delay": 0.01,
        "max_retries": 3,
        "advanced": {
            "enabled": False,
            "retry": {
                "strategy": "exponential_backoff",
                "initial_delay": 0.5,
                "max_wait_time": None,
                "multiplier": 2.0,
                "increment": 0.5,
            },
            "rate_limit": {
                "strategy": "fixed_wait",
                "window_size": 1.0,
                "max_requests": 10,
            },
        },
    },
    "output": {"log_level": "INFO"},
}


ROOT_KEYS = {
    "schema_version",
    "auth",
    "source",
    "target",
    "sync",
    "conversion",
    "control",
    "output",
}
SECTION_KEYS: dict[str, set[str]] = {
    "auth": {"app_id", "app_secret"},
    "source": {"type", "file", "bitable"},
    "source.file": {"path", "sheet_name"},
    "source.bitable": {"app_token", "table_id"},
    "target": {"type", "bitable", "sheet"},
    "target.bitable": {
        "app_token",
        "table_id",
        "create_missing_fields",
        "api_backend",
        "user_id_type",
    },
    "target.sheet": {
        "spreadsheet_token",
        "sheet_id",
        "start_row",
        "start_column",
        "value_render_option",
        "datetime_render_option",
        "scan_max_rows",
        "scan_max_columns",
        "write_max_rows",
        "write_max_columns",
        "validate_results",
        "protect_formulas",
        "verify_formulas",
        "formula_max_locations",
        "report_column_diff",
        "diff_tolerance",
    },
    "sync": {"mode", "index", "verify_remote_writes", "selective"},
    "sync.index": {"column", "datetime_granularity"},
    "sync.selective": {
        "enabled",
        "columns",
        "auto_include_index",
        "optimize_ranges",
        "max_gap_for_merge",
        "preserve_column_order",
    },
    "conversion": {"strategy", "intelligence"},
    "conversion.intelligence": {
        "date_confidence",
        "choice_confidence",
        "boolean_confidence",
    },
    "control": {"batch_size", "rate_limit_delay", "max_retries", "advanced"},
    "control.advanced": {"enabled", "retry", "rate_limit"},
    "control.advanced.retry": {
        "strategy",
        "initial_delay",
        "max_wait_time",
        "multiplier",
        "increment",
    },
    "control.advanced.rate_limit": {"strategy", "window_size", "max_requests"},
    "output": {"log_level"},
}

BOOLEAN_PATHS = {
    "target.bitable.create_missing_fields",
    "target.sheet.validate_results",
    "target.sheet.protect_formulas",
    "target.sheet.verify_formulas",
    "target.sheet.report_column_diff",
    "sync.verify_remote_writes",
    "sync.selective.enabled",
    "sync.selective.auto_include_index",
    "sync.selective.optimize_ranges",
    "sync.selective.preserve_column_order",
    "control.advanced.enabled",
}
INTEGER_PATHS = {
    "target.sheet.start_row",
    "target.sheet.scan_max_rows",
    "target.sheet.scan_max_columns",
    "target.sheet.write_max_rows",
    "target.sheet.write_max_columns",
    "target.sheet.formula_max_locations",
    "sync.selective.max_gap_for_merge",
    "control.batch_size",
    "control.max_retries",
    "control.advanced.rate_limit.max_requests",
}
NUMBER_PATHS = {
    "target.sheet.diff_tolerance",
    "conversion.intelligence.date_confidence",
    "conversion.intelligence.choice_confidence",
    "conversion.intelligence.boolean_confidence",
    "control.rate_limit_delay",
    "control.advanced.retry.initial_delay",
    "control.advanced.retry.max_wait_time",
    "control.advanced.retry.multiplier",
    "control.advanced.retry.increment",
    "control.advanced.rate_limit.window_size",
}
STRING_PATHS = {
    "auth.app_id",
    "auth.app_secret",
    "source.type",
    "source.file.path",
    "source.bitable.app_token",
    "source.bitable.table_id",
    "target.type",
    "target.bitable.app_token",
    "target.bitable.table_id",
    "target.bitable.api_backend",
    "target.bitable.user_id_type",
    "target.sheet.spreadsheet_token",
    "target.sheet.sheet_id",
    "target.sheet.start_column",
    "target.sheet.value_render_option",
    "target.sheet.datetime_render_option",
    "sync.mode",
    "sync.index.column",
    "sync.index.datetime_granularity",
    "conversion.strategy",
    "control.advanced.retry.strategy",
    "control.advanced.rate_limit.strategy",
    "output.log_level",
}


YAML_TO_FLAT: dict[str, str] = {
    "auth.app_id": "app_id",
    "auth.app_secret": "app_secret",
    "source.type": "source_type",
    "source.file.path": "file_path",
    "source.file.sheet_name": "excel_sheet_name",
    "source.bitable.app_token": "source_app_token",
    "source.bitable.table_id": "source_table_id",
    "target.type": "target_type",
    "target.bitable.app_token": "app_token",
    "target.bitable.table_id": "table_id",
    "target.bitable.create_missing_fields": "create_missing_fields",
    "target.bitable.api_backend": "bitable_api_backend",
    "target.bitable.user_id_type": "bitable_user_id_type",
    "target.sheet.spreadsheet_token": "spreadsheet_token",
    "target.sheet.sheet_id": "sheet_id",
    "target.sheet.start_row": "start_row",
    "target.sheet.start_column": "start_column",
    "target.sheet.value_render_option": "sheet_value_render_option",
    "target.sheet.datetime_render_option": "sheet_datetime_render_option",
    "target.sheet.scan_max_rows": "sheet_scan_max_rows",
    "target.sheet.scan_max_columns": "sheet_scan_max_cols",
    "target.sheet.write_max_rows": "sheet_write_max_rows",
    "target.sheet.write_max_columns": "sheet_write_max_cols",
    "target.sheet.validate_results": "sheet_validate_results",
    "target.sheet.protect_formulas": "sheet_protect_formulas",
    "target.sheet.verify_formulas": "sheet_verify_formulas",
    "target.sheet.formula_max_locations": "sheet_formula_max_locations",
    "target.sheet.report_column_diff": "sheet_report_column_diff",
    "target.sheet.diff_tolerance": "sheet_diff_tolerance",
    "sync.mode": "sync_mode",
    "sync.index.column": "index_column",
    "sync.index.datetime_granularity": "datetime_index_granularity",
    "sync.verify_remote_writes": "verify_remote_writes",
    "sync.selective.enabled": "selective_sync.enabled",
    "sync.selective.columns": "selective_sync.columns",
    "sync.selective.auto_include_index": "selective_sync.auto_include_index",
    "sync.selective.optimize_ranges": "selective_sync.optimize_ranges",
    "sync.selective.max_gap_for_merge": "selective_sync.max_gap_for_merge",
    "sync.selective.preserve_column_order": "selective_sync.preserve_column_order",
    "conversion.strategy": "field_type_strategy",
    "conversion.intelligence.date_confidence": "intelligence_date_confidence",
    "conversion.intelligence.choice_confidence": "intelligence_choice_confidence",
    "conversion.intelligence.boolean_confidence": "intelligence_boolean_confidence",
    "control.batch_size": "batch_size",
    "control.rate_limit_delay": "rate_limit_delay",
    "control.max_retries": "max_retries",
    "control.advanced.enabled": "enable_advanced_control",
    "control.advanced.retry.strategy": "retry_strategy_type",
    "control.advanced.retry.initial_delay": "retry_initial_delay",
    "control.advanced.retry.max_wait_time": "retry_max_wait_time",
    "control.advanced.retry.multiplier": "retry_multiplier",
    "control.advanced.retry.increment": "retry_increment",
    "control.advanced.rate_limit.strategy": "rate_limit_strategy_type",
    "control.advanced.rate_limit.window_size": "rate_limit_window_size",
    "control.advanced.rate_limit.max_requests": "rate_limit_max_requests",
    "output.log_level": "log_level",
}

CLI_TO_FLAT: dict[str, str] = {
    name: name
    for name in (
        "app_id",
        "app_secret",
        "source_type",
        "file_path",
        "excel_sheet_name",
        "source_app_token",
        "source_table_id",
        "target_type",
        "app_token",
        "table_id",
        "create_missing_fields",
        "bitable_api_backend",
        "bitable_user_id_type",
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
        "field_type_strategy",
        "intelligence_date_confidence",
        "intelligence_choice_confidence",
        "intelligence_boolean_confidence",
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
    )
}
CLI_TO_FLAT.update(
    {
        "selective_enabled": "selective_sync.enabled",
        "column": "selective_sync.columns",
        "auto_include_index": "selective_sync.auto_include_index",
        "optimize_ranges": "selective_sync.optimize_ranges",
        "max_gap_for_merge": "selective_sync.max_gap_for_merge",
        "preserve_column_order": "selective_sync.preserve_column_order",
    }
)


@dataclass(frozen=True)
class ResolvedConfig:
    config: SyncConfig
    values: dict[str, Any]
    sources: dict[str, str]
    path: Path | None


def _config_error(
    message: str, *, details: Mapping[str, Any] | None = None
) -> CLIError:
    return CLIError("XTF_E_CONFIG_INVALID", message, EXIT_CONFIG, details)


def _mapping(value: Any, path: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _config_error(f"{path} must be a mapping")
    return value


def _check_keys(value: Mapping[str, Any], allowed: set[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise _config_error(f"unknown key(s) in {path}: {', '.join(unknown)}")


def _nonempty_branch(value: Any) -> bool:
    return value not in (None, {})


def _check_leaf_types(root: Mapping[str, Any]) -> None:
    leaves = _walk_leaves(root)
    for path in sorted(BOOLEAN_PATHS):
        if path not in leaves:
            continue
        if not isinstance(leaves[path], bool):
            raise _config_error(f"{path} must be a boolean")
    for path in sorted(INTEGER_PATHS):
        if path not in leaves:
            continue
        value = leaves[path]
        if not isinstance(value, int) or isinstance(value, bool):
            raise _config_error(f"{path} must be an integer")
    for path in sorted(NUMBER_PATHS):
        if path not in leaves:
            continue
        value = leaves[path]
        if value is not None and (
            not isinstance(value, (int, float)) or isinstance(value, bool)
        ):
            raise _config_error(f"{path} must be a number or null")
    for path in sorted(STRING_PATHS):
        if path not in leaves:
            continue
        value = leaves[path]
        if value is not None and not isinstance(value, str):
            raise _config_error(f"{path} must be a string or null")
    columns = leaves.get("sync.selective.columns")
    if columns is not None and (
        not isinstance(columns, list)
        or not all(isinstance(item, str) for item in columns)
    ):
        raise _config_error("sync.selective.columns must be a list of strings")
    sheet_name = leaves.get("source.file.sheet_name")
    if sheet_name is not None and (
        isinstance(sheet_name, bool) or not isinstance(sheet_name, (str, int))
    ):
        raise _config_error("source.file.sheet_name must be a string, integer, or null")


def validate_v2_document(document: Any) -> Mapping[str, Any]:
    root = _mapping(document, "root")
    _check_keys(root, ROOT_KEYS, "root")
    schema_version = root.get("schema_version")
    if (
        not isinstance(schema_version, int)
        or isinstance(schema_version, bool)
        or schema_version != SCHEMA_VERSION
    ):
        raise _config_error(
            "schema_version must be exactly 2; legacy flat YAML is not accepted"
        )
    for required in ("auth", "source", "target"):
        if required not in root:
            raise _config_error(f"missing required section: {required}")

    for section in ROOT_KEYS - {"schema_version"}:
        if section not in root:
            continue
        section_value = _mapping(root[section], section)
        _check_keys(section_value, SECTION_KEYS[section], section)

    source = _mapping(root["source"], "source")
    source_type = source.get("type")
    if source_type not in {"file", "bitable"}:
        raise _config_error("source.type must be file or bitable")
    for branch in ("file", "bitable"):
        if branch in source and source[branch] is not None:
            branch_value = _mapping(source[branch], f"source.{branch}")
            _check_keys(
                branch_value, SECTION_KEYS[f"source.{branch}"], f"source.{branch}"
            )
    inactive_source = "bitable" if source_type == "file" else "file"
    if _nonempty_branch(source.get(inactive_source)):
        raise _config_error(
            f"inactive source.{inactive_source} branch must be absent or empty"
        )

    target = _mapping(root["target"], "target")
    target_type = target.get("type")
    if target_type not in {"bitable", "sheet"}:
        raise _config_error("target.type must be bitable or sheet")
    for branch in ("bitable", "sheet"):
        if branch in target and target[branch] is not None:
            branch_value = _mapping(target[branch], f"target.{branch}")
            _check_keys(
                branch_value, SECTION_KEYS[f"target.{branch}"], f"target.{branch}"
            )
    inactive_target = "sheet" if target_type == "bitable" else "bitable"
    if _nonempty_branch(target.get(inactive_target)):
        raise _config_error(
            f"inactive target.{inactive_target} branch must be absent or empty"
        )

    sync = root.get("sync", {})
    if sync:
        sync_map = _mapping(sync, "sync")
        if "index" in sync_map:
            index_map = _mapping(sync_map["index"], "sync.index")
            _check_keys(index_map, SECTION_KEYS["sync.index"], "sync.index")
        if "selective" in sync_map:
            selective_map = _mapping(sync_map["selective"], "sync.selective")
            _check_keys(
                selective_map,
                SECTION_KEYS["sync.selective"],
                "sync.selective",
            )

    conversion = root.get("conversion", {})
    if conversion:
        conversion_map = _mapping(conversion, "conversion")
        if (
            "intelligence" in conversion_map
            and conversion_map["intelligence"] is not None
        ):
            intelligence = _mapping(
                conversion_map["intelligence"], "conversion.intelligence"
            )
            _check_keys(
                intelligence,
                SECTION_KEYS["conversion.intelligence"],
                "conversion.intelligence",
            )

    control = root.get("control", {})
    if control:
        control_map = _mapping(control, "control")
        advanced = control_map.get("advanced")
        if advanced is not None:
            advanced_map = _mapping(advanced, "control.advanced")
            _check_keys(
                advanced_map,
                SECTION_KEYS["control.advanced"],
                "control.advanced",
            )
            for branch in ("retry", "rate_limit"):
                if branch not in advanced_map or advanced_map[branch] is None:
                    continue
                branch_map = _mapping(
                    advanced_map[branch], f"control.advanced.{branch}"
                )
                _check_keys(
                    branch_map,
                    SECTION_KEYS[f"control.advanced.{branch}"],
                    f"control.advanced.{branch}",
                )
    _check_leaf_types(root)
    return root


def read_v2_file(path: Path) -> Mapping[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            document = yaml.safe_load(stream)
    except FileNotFoundError as exc:
        raise CLIError(
            "XTF_E_CONFIG_NOT_FOUND",
            f"configuration file not found: {path}",
            EXIT_CONFIG,
        ) from exc
    except (OSError, yaml.YAMLError) as exc:
        raise _config_error(f"cannot read YAML {path}: {exc}") from exc
    return validate_v2_document(document)


def discover_config(explicit: str | None) -> Path | None:
    if explicit:
        path = Path(explicit)
        if not path.is_file():
            raise CLIError(
                "XTF_E_CONFIG_NOT_FOUND",
                f"configuration file not found: {path}",
                EXIT_CONFIG,
            )
        return path
    candidate = Path.cwd() / "config.yaml"
    return candidate if candidate.is_file() else None


def has_cli_overrides(args: argparse.Namespace) -> bool:
    return any(getattr(args, name, None) is not None for name in CLI_TO_FLAT)


def _walk_leaves(value: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    leaves: dict[str, Any] = {}
    for key, item in value.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(item, Mapping):
            leaves.update(_walk_leaves(item, path))
        else:
            leaves[path] = item
    return leaves


def _defaults(target_type: str) -> tuple[dict[str, Any], dict[str, str]]:
    values: dict[str, Any] = {
        "file_path": None,
        "app_id": "",
        "app_secret": "",
        "target_type": target_type,
    }
    for item in fields(SyncConfig):
        if item.name in values or item.name == "selective_sync":
            continue
        if item.default is not MISSING:
            default = item.default
            values[item.name] = getattr(default, "value", default)
    for item in fields(SelectiveSyncConfig):
        if item.default is not MISSING:
            values[f"selective_sync.{item.name}"] = item.default
    if target_type == "sheet":
        values["batch_size"] = 1000
        values["rate_limit_delay"] = 0.1
    sources = {name: f"default:{target_type}" for name in values}
    return values, sources


def _set(
    values: MutableMapping[str, Any],
    sources: MutableMapping[str, str],
    name: str,
    value: Any,
    source: str,
) -> None:
    if value == "none" and name in {
        "sheet_value_render_option",
        "sheet_datetime_render_option",
    }:
        value = None
    if name == "excel_sheet_name" and isinstance(value, str) and value.isdigit():
        value = int(value)
    values[name] = value
    sources[name] = source


def resolve_config(
    args: argparse.Namespace,
    *,
    require_file: bool = False,
    environ: Mapping[str, str] | None = None,
) -> ResolvedConfig:
    path = discover_config(getattr(args, "config", None))
    if require_file and path is None:
        raise CLIError(
            "XTF_E_CONFIG_NOT_FOUND",
            "no configuration file specified and ./config.yaml does not exist",
            EXIT_CONFIG,
        )
    document: Mapping[str, Any] = read_v2_file(path) if path else {}
    yaml_leaves = _walk_leaves(document) if document else {}
    raw_target_type = getattr(args, "target_type", None) or yaml_leaves.get(
        "target.type"
    )
    if raw_target_type is None:
        raise _config_error("target.type or --target-type is required")
    target_type = str(raw_target_type)
    values, sources = _defaults(target_type)
    if path:
        for yaml_path, flat_name in YAML_TO_FLAT.items():
            if yaml_path in yaml_leaves:
                _set(values, sources, flat_name, yaml_leaves[yaml_path], f"yaml:{path}")

    env = os.environ if environ is None else environ
    if env.get(ENV_APP_SECRET):
        _set(
            values, sources, "app_secret", env[ENV_APP_SECRET], f"env:{ENV_APP_SECRET}"
        )

    for cli_name, flat_name in CLI_TO_FLAT.items():
        value = getattr(args, cli_name, None)
        if value is not None:
            _set(values, sources, flat_name, value, "cli")

    if getattr(args, "column", None) is not None:
        _set(values, sources, "selective_sync.enabled", True, "cli")

    selective = SelectiveSyncConfig(
        **{
            item.name: values.pop(f"selective_sync.{item.name}")
            for item in fields(SelectiveSyncConfig)
        }
    )
    config_values = dict(values)
    config_values["selective_sync"] = selective
    try:
        config = SyncConfig(**config_values)
    except (TypeError, ValueError) as exc:
        raise _config_error(str(exc)) from exc
    if not config.app_id:
        raise _config_error("auth.app_id or --app-id is required")
    if not config.app_secret:
        raise _config_error(
            "app secret is required via --app-secret, XTF_APP_SECRET, or auth.app_secret"
        )
    config.__dict__["config_sources"] = dict(sources)
    values["selective_sync"] = {
        item.name: getattr(selective, item.name) for item in fields(SelectiveSyncConfig)
    }
    return ResolvedConfig(config=config, values=values, sources=sources, path=path)


def make_template(source_type: str, target_type: str) -> dict[str, Any]:
    if source_type == "bitable" and target_type != "bitable":
        raise _config_error("source.type=bitable requires target.type=bitable")
    template = copy.deepcopy(TEMPLATE)
    template["source"] = {"type": source_type}
    if source_type == "file":
        template["source"]["file"] = {"path": "data.xlsx", "sheet_name": None}
    else:
        template["source"]["bitable"] = {
            "app_token": "your_source_app_token",
            "table_id": "your_source_table_id",
        }
    template["target"] = {"type": target_type}
    if target_type == "bitable":
        template["target"]["bitable"] = copy.deepcopy(TEMPLATE["target"]["bitable"])
    else:
        template["target"]["sheet"] = {
            "spreadsheet_token": "your_spreadsheet_token",
            "sheet_id": "your_sheet_id",
            "start_row": 1,
            "start_column": "A",
            "value_render_option": None,
            "datetime_render_option": None,
            "scan_max_rows": 5000,
            "scan_max_columns": 100,
            "write_max_rows": 5000,
            "write_max_columns": 100,
            "validate_results": False,
            "protect_formulas": False,
            "verify_formulas": False,
            "formula_max_locations": 20,
            "report_column_diff": False,
            "diff_tolerance": 0.001,
        }
        template["control"]["batch_size"] = 1000
        template["control"]["rate_limit_delay"] = 0.1
    return template


def write_template(
    path: Path, *, force: bool, source_type: str, target_type: str
) -> None:
    if path.exists() and not force:
        raise CLIError(
            "XTF_E_CONFIG_EXISTS",
            f"refusing to overwrite existing file: {path}; use --force",
            EXIT_CONFIG,
        )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as stream:
            yaml.safe_dump(
                make_template(source_type, target_type),
                stream,
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            )
    except OSError as exc:
        raise _config_error(f"cannot write configuration template: {exc}") from exc


def is_sensitive(name: str) -> bool:
    leaf = name.rsplit(".", 1)[-1].lower()
    return "secret" in leaf or "token" in leaf


def redact_value(name: str, value: Any) -> Any:
    if is_sensitive(name) and value not in (None, ""):
        return "<redacted>"
    return value


def shown_values(resolved: ResolvedConfig) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for name, value in resolved.values.items():
        if name == "selective_sync" and isinstance(value, Mapping):
            result[name] = {
                key: redact_value(f"selective_sync.{key}", item)
                for key, item in value.items()
            }
        else:
            result[name] = redact_value(name, value)
    return result
