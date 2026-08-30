"""Single argparse surface for XTF 2.0."""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from typing import Any, NoReturn

from .errors import EXIT_USAGE, CLIError, ParserSignal
from .version import VERSION


class XTFArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        raise CLIError(
            "XTF_E_USAGE",
            f"{message}. Use 'XTF sync ...' for synchronization options.",
            EXIT_USAGE,
        )

    def exit(self, status: int = 0, message: str | None = None) -> NoReturn:
        raise ParserSignal(status, message)


def _add_output_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=argparse.SUPPRESS,
        help="emit one JSON command result on stdout",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        default=argparse.SUPPRESS,
        help="suppress informational progress",
    )


def _add_config_path(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "-c",
        "--config",
        default=None,
        help="v2 YAML path; defaults to ./config.yaml when it exists",
    )


def _bool(parser: Any, flag: str, dest: str, help_text: str) -> None:
    parser.add_argument(
        flag,
        dest=dest,
        action=argparse.BooleanOptionalAction,
        default=None,
        help=help_text,
    )


def _add_config_override_flags(parser: argparse.ArgumentParser) -> None:
    _add_config_path(parser)

    auth = parser.add_argument_group("Auth")
    auth.add_argument("--app-id")
    auth.add_argument("--app-secret")

    source = parser.add_argument_group("Source")
    source.add_argument("--source-type", choices=("file", "bitable"))
    source.add_argument("--file", dest="file_path")
    source.add_argument("--excel-sheet", dest="excel_sheet_name")
    source.add_argument("--source-app-token")
    source.add_argument("--source-table-id")

    target = parser.add_argument_group("Target")
    target.add_argument("--target-type", choices=("bitable", "sheet"))

    bitable = parser.add_argument_group("Target Bitable")
    bitable.add_argument("--target-app-token", dest="app_token")
    bitable.add_argument("--target-table-id", dest="table_id")
    _bool(
        bitable,
        "--create-missing-fields",
        "create_missing_fields",
        "create missing Bitable fields",
    )
    bitable.add_argument(
        "--bitable-backend",
        dest="bitable_api_backend",
        choices=("base_v3", "bitable_v1"),
    )
    bitable.add_argument(
        "--user-id-type",
        dest="bitable_user_id_type",
        choices=("open_id", "union_id", "user_id"),
    )

    sheet = parser.add_argument_group("Target Sheet")
    sheet.add_argument("--spreadsheet-token")
    sheet.add_argument("--sheet-id")
    sheet.add_argument("--start-row", type=int)
    sheet.add_argument("--start-column")
    sheet.add_argument(
        "--sheet-value-render-option",
        choices=("none", "ToString", "Formula", "FormattedValue", "UnformattedValue"),
    )
    sheet.add_argument(
        "--sheet-datetime-render-option", choices=("none", "FormattedString")
    )
    sheet.add_argument("--sheet-scan-max-rows", type=int)
    sheet.add_argument("--sheet-scan-max-columns", dest="sheet_scan_max_cols", type=int)
    sheet.add_argument("--sheet-write-max-rows", type=int)
    sheet.add_argument(
        "--sheet-write-max-columns", dest="sheet_write_max_cols", type=int
    )
    _bool(
        sheet,
        "--sheet-validate-results",
        "sheet_validate_results",
        "validate Sheet results",
    )
    _bool(
        sheet,
        "--sheet-protect-formulas",
        "sheet_protect_formulas",
        "protect Sheet formulas",
    )
    _bool(
        sheet,
        "--sheet-verify-formulas",
        "sheet_verify_formulas",
        "run Sheet formula verification",
    )
    sheet.add_argument("--sheet-formula-max-locations", type=int)
    _bool(
        sheet,
        "--sheet-report-column-diff",
        "sheet_report_column_diff",
        "report column differences",
    )
    sheet.add_argument("--sheet-diff-tolerance", type=float)

    sync = parser.add_argument_group("Sync")
    sync.add_argument(
        "--mode",
        dest="sync_mode",
        choices=("full", "incremental", "overwrite", "clone"),
    )
    sync.add_argument("--index-column")
    sync.add_argument("--datetime-index-granularity", choices=("exact", "day"))
    _bool(
        sync,
        "--verify-remote-writes",
        "verify_remote_writes",
        "verify remote mutations",
    )
    selective = parser.add_argument_group("Selective")
    _bool(
        selective,
        "--selective",
        "selective_enabled",
        "enable selective sync",
    )
    selective.add_argument(
        "--column",
        action="append",
        default=None,
        help="selected column; repeat to replace the YAML columns list",
    )
    _bool(
        selective,
        "--auto-include-index",
        "auto_include_index",
        "include index column",
    )
    _bool(
        selective,
        "--optimize-ranges",
        "optimize_ranges",
        "merge adjacent Sheet ranges",
    )
    selective.add_argument("--max-gap-for-merge", type=int)
    _bool(
        selective,
        "--preserve-column-order",
        "preserve_column_order",
        "preserve input column order",
    )

    conversion = parser.add_argument_group("Conversion")
    conversion.add_argument(
        "--field-type-strategy", choices=("raw", "base", "auto", "intelligence")
    )
    conversion.add_argument("--intelligence-date-confidence", type=float)
    conversion.add_argument("--intelligence-choice-confidence", type=float)
    conversion.add_argument("--intelligence-boolean-confidence", type=float)

    control = parser.add_argument_group("Control")
    control.add_argument("--batch-size", type=int)
    control.add_argument("--rate-limit-delay", type=float)
    control.add_argument("--max-retries", type=int)
    _bool(
        control,
        "--advanced-control",
        "enable_advanced_control",
        "enable advanced request control",
    )
    control.add_argument(
        "--retry-strategy-type",
        choices=("exponential_backoff", "linear_growth", "fixed_wait"),
    )
    control.add_argument("--retry-initial-delay", type=float)
    control.add_argument("--retry-max-wait-time", type=float)
    control.add_argument("--retry-multiplier", type=float)
    control.add_argument("--retry-increment", type=float)
    control.add_argument(
        "--rate-limit-strategy-type",
        choices=("fixed_wait", "sliding_window", "fixed_window"),
    )
    control.add_argument("--rate-limit-window-size", type=float)
    control.add_argument("--rate-limit-max-requests", type=int)

    output = parser.add_argument_group("Output")
    output.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"))


def _add_sync_flags(parser: argparse.ArgumentParser) -> None:
    _add_config_override_flags(parser)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=None,
        help="plan without executing",
    )
    parser.add_argument(
        "--allow-delete",
        action="store_true",
        default=None,
        help="allow a destructive requested mode or planned delete/clear action",
    )


def build_parser() -> XTFArgumentParser:
    parser = XTFArgumentParser(
        prog="XTF",
        description="XTF 2.0 - synchronize file or Bitable data to Feishu",
    )
    parser.set_defaults(json=False, quiet=False)
    parser.add_argument("--version", action="version", version=f"XTF {VERSION}")
    _add_output_flags(parser)
    subparsers = parser.add_subparsers(dest="command")

    sync = subparsers.add_parser("sync", help="plan and execute a synchronization")
    _add_output_flags(sync)
    _add_sync_flags(sync)

    config = subparsers.add_parser("config", help="manage strict v2 configuration")
    _add_output_flags(config)
    config_subparsers = config.add_subparsers(dest="config_command")

    init = config_subparsers.add_parser(
        "init", help="write a v2 configuration template"
    )
    _add_output_flags(init)
    init.add_argument("--source-type", choices=("file", "bitable"), default="file")
    init.add_argument("--target-type", choices=("bitable", "sheet"), default="bitable")
    init.add_argument("-o", "--output", default="config.yaml")
    init.add_argument("--force", action="store_true")

    validate = config_subparsers.add_parser("validate", help="validate v2 YAML locally")
    _add_output_flags(validate)
    _add_config_path(validate)

    show = config_subparsers.add_parser("show", help="show resolved values and sources")
    _add_output_flags(show)
    _add_config_path(show)

    doctor = subparsers.add_parser(
        "doctor", help="check local setup and optional remote metadata"
    )
    _add_output_flags(doctor)
    _add_config_override_flags(doctor)
    doctor.add_argument(
        "--network",
        action="store_true",
        help="perform read-only auth and resource metadata checks",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        if not getattr(args, "json", False):
            parser.print_help()
        raise CLIError(
            "XTF_E_USAGE",
            "a command is required; use 'XTF sync ...' (or config/doctor)",
            EXIT_USAGE,
        )
    if args.command == "config" and args.config_command is None:
        raise CLIError(
            "XTF_E_USAGE",
            "a config command is required: init, validate, or show",
            EXIT_USAGE,
        )
    return args
