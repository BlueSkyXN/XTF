"""Command dispatch and stdout/stderr contracts for XTF 2.0."""

from __future__ import annotations

import argparse
import importlib.util
import io
import json
import logging
import sys
from collections.abc import Mapping, Sequence
from contextlib import redirect_stdout
from dataclasses import fields, is_dataclass
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast

from .config import (
    ResolvedConfig,
    discover_config,
    has_cli_overrides,
    resolve_config,
    shown_values,
    write_template,
)
from .errors import (
    EXIT_AUTH,
    EXIT_CONFIG,
    EXIT_INPUT,
    EXIT_INDETERMINATE,
    EXIT_INTERRUPTED,
    EXIT_OK,
    EXIT_PARTIAL,
    EXIT_REMOTE,
    EXIT_RUNTIME,
    EXIT_VERIFICATION,
    CLIError,
    ParserSignal,
)
from .parser import parse_args

if TYPE_CHECKING:
    from core.plan import ExecutionPlan


class Reporter:
    def __init__(self, *, json_mode: bool, quiet: bool, command: str | None = None):
        self.json_mode = json_mode
        self.quiet = quiet
        self.command = command
        self._sensitive_values: set[str] = set()

    def add_sensitive_config(self, config: Any) -> None:
        """Register configured secrets and resource tokens for output redaction."""

        def visit(value: Any, path: str = "") -> None:
            if is_dataclass(value) and not isinstance(value, type):
                for item in fields(value):
                    child_path = f"{path}.{item.name}" if path else item.name
                    visit(getattr(value, item.name), child_path)
                return
            if isinstance(value, Mapping):
                for name, item in value.items():
                    child_path = f"{path}.{name}" if path else str(name)
                    visit(item, child_path)
                return
            if (
                isinstance(value, str)
                and value
                and any(marker in path.lower() for marker in ("secret", "token"))
            ):
                self._sensitive_values.add(value)

        visit(config)

    def redact(self, value: str) -> str:
        redacted = value
        for sensitive in sorted(self._sensitive_values, key=len, reverse=True):
            redacted = redacted.replace(sensitive, "[REDACTED]")
        return redacted

    def sanitize(self, value: Any) -> Any:
        if isinstance(value, str):
            return self.redact(value)
        if isinstance(value, Mapping):
            return {str(key): self.sanitize(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self.sanitize(item) for item in value]
        return value

    def info(self, message: str) -> None:
        if not self.quiet and not self.json_mode:
            print(self.redact(message), file=sys.stderr)

    def warning(self, message: str) -> None:
        if not self.json_mode:
            print(f"warning: {self.redact(message)}", file=sys.stderr)

    @staticmethod
    def _envelope(
        *,
        command: str | None,
        ok: bool,
        status: str,
        duration_ms: int,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        result_data = dict(result or {})
        outcome = result_data.get("outcome")
        outcome_data = dict(outcome) if isinstance(outcome, Mapping) else {}
        plan = result_data.get("plan") or outcome_data.get("plan")
        plan_data = dict(plan) if isinstance(plan, Mapping) else None
        return {
            "schema_version": 1,
            "command": command,
            "status": status,
            "ok": ok,
            "dry_run": result_data.get("dry_run"),
            "config_path": result_data.get("config_path") or result_data.get("path"),
            "source": (
                plan_data.get("source") if plan_data else result_data.get("source")
            ),
            "target": (
                plan_data.get("target") if plan_data else result_data.get("target")
            ),
            "requested_mode": (
                plan_data.get("requested_mode")
                if plan_data
                else result_data.get("requested_mode")
            ),
            "effective_mode": (
                plan_data.get("effective_mode")
                if plan_data
                else result_data.get("effective_mode")
            ),
            "plan": plan_data,
            "applied": list(outcome_data.get("applied") or []),
            "verification": list(outcome_data.get("verification") or []),
            "warnings": list(
                outcome_data.get("warnings")
                or (plan_data.get("warnings") if plan_data else [])
                or []
            ),
            "error": dict(error) if error else None,
            "duration_ms": duration_ms,
            "result": result_data,
        }

    def success(
        self,
        command: str,
        result: Mapping[str, Any],
        human: str,
        duration_ms: int,
    ) -> None:
        if self.json_mode:
            safe_result = self.sanitize(result)
            outcome = safe_result.get("outcome")
            outcome_status = (
                outcome.get("status") if isinstance(outcome, Mapping) else None
            )
            status = str(
                "planned" if safe_result.get("dry_run") else outcome_status or "success"
            )
            print(
                json.dumps(
                    self._envelope(
                        command=command,
                        ok=True,
                        status=status,
                        duration_ms=duration_ms,
                        result=safe_result,
                    ),
                    ensure_ascii=False,
                    sort_keys=True,
                )
            )
        else:
            print(self.redact(human))

    def error(self, error: CLIError, duration_ms: int) -> None:
        if self.json_mode:
            kinds = {
                1: "internal",
                2: "usage",
                3: "config",
                4: "auth",
                5: "read",
                6: "mutation",
                7: "verification",
                8: "indeterminate",
                130: "interrupt",
            }
            error_kind = kinds.get(error.exit_code, "internal")
            if error.code in {"XTF_E_RESOURCE", "XTF_E_REMOTE_RESOURCE_NOT_FOUND"}:
                error_kind = "resource"
            error_data: dict[str, Any] = {
                "kind": error_kind,
                "code": error.code,
                "message": self.redact(error.message),
            }
            result: dict[str, Any] = {}
            if error.details:
                outcome = error.details.get("outcome")
                if isinstance(outcome, Mapping):
                    result["outcome"] = dict(outcome)
                plan = error.details.get("plan")
                if isinstance(plan, Mapping):
                    result["plan"] = dict(plan)
                if "dry_run" in error.details:
                    result["dry_run"] = error.details["dry_run"]
                config_path = error.details.get("config_path")
                if config_path is not None:
                    result["config_path"] = config_path
            outcome_status = None
            outcome = result.get("outcome")
            if isinstance(outcome, Mapping):
                outcome_status = outcome.get("status")
            payload = self._envelope(
                command=self.command,
                ok=False,
                status=str(outcome_status or "error"),
                duration_ms=duration_ms,
                result=self.sanitize(result),
                error=error_data,
            )
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        else:
            print(f"{error.code}: {self.redact(error.message)}", file=sys.stderr)


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict") and callable(value.to_dict):
        result = value.to_dict()
        if isinstance(result, Mapping):
            return dict(result)
    if isinstance(value, Mapping):
        return dict(value)
    return {"value": value}


def _plan_is_destructive(plan: ExecutionPlan | Mapping[str, Any]) -> bool:
    if bool(getattr(plan, "destructive", False)):
        return True
    actions = getattr(plan, "actions", None)
    if actions is None and isinstance(plan, Mapping):
        actions = plan.get("actions", ())
    for action in actions or ():
        kind = getattr(action, "kind", None)
        destructive = getattr(action, "destructive", False)
        if isinstance(action, Mapping):
            kind = action.get("kind")
            destructive = action.get("destructive", False)
        if kind in {"delete_records", "clear_range"} or destructive:
            return True
    return False


def _load_dataframe(resolved: ResolvedConfig) -> Any:
    config = resolved.config
    source_type = config.source.type.value
    if source_type == "bitable":
        return None
    if not config.source.file_path:
        raise CLIError(
            "XTF_E_INPUT_REQUIRED",
            "source.type=file requires source.file.path or --file",
            EXIT_INPUT,
        )
    path = Path(config.source.file_path)
    if not path.is_file():
        raise CLIError(
            "XTF_E_INPUT_NOT_FOUND", f"input file not found: {path}", EXIT_INPUT
        )
    from core.reader import DataFileReader

    if not DataFileReader.is_supported(path):
        raise CLIError(
            "XTF_E_INPUT_FORMAT",
            f"unsupported input format: {path.suffix}",
            EXIT_INPUT,
        )
    kwargs: dict[str, Any] = {}
    if config.source.excel_sheet_name is not None and path.suffix.lower() in {
        ".xlsx",
        ".xls",
    }:
        kwargs["sheet_name"] = config.source.excel_sheet_name
    try:
        return DataFileReader().read_file(path, **kwargs)
    except FileNotFoundError as exc:
        raise CLIError("XTF_E_INPUT_NOT_FOUND", str(exc), EXIT_INPUT) from exc
    except (OSError, ValueError) as exc:
        raise CLIError("XTF_E_INPUT_READ", str(exc), EXIT_INPUT) from exc


def _sync(
    args: argparse.Namespace, reporter: Reporter
) -> tuple[int, dict[str, Any], str]:
    resolved = resolve_config(args)
    reporter.add_sensitive_config(resolved.config)
    dataframe = _load_dataframe(resolved)
    reporter.info("Planning synchronization...")

    from core.service import SyncService

    service = SyncService(resolved.config)
    if reporter.quiet:
        for handler in logging.getLogger("XTF").handlers:
            if isinstance(handler, logging.StreamHandler) and not isinstance(
                handler, logging.FileHandler
            ):
                handler.setLevel(logging.WARNING)
    plan_method = getattr(service, "plan", None)
    execute_method = getattr(service, "execute_plan", None)
    if not callable(plan_method) or not callable(execute_method):
        raise CLIError(
            "XTF_E_CORE_PLAN_UNAVAILABLE",
            "core planner interface is unavailable; expected SyncService.plan/execute_plan",
            EXIT_RUNTIME,
        )
    try:
        plan = plan_method(dataframe)
    except KeyboardInterrupt:
        raise
    except ValueError as exc:
        raise CLIError("XTF_E_CONFIG_INVALID", str(exc), EXIT_CONFIG) from exc
    except RuntimeError as exc:
        raise CLIError(
            "XTF_E_PLAN_INCOMPLETE",
            str(exc),
            EXIT_REMOTE,
            {"config_path": str(resolved.path) if resolved.path else None},
        ) from exc
    except Exception as exc:
        error = _normalize_exception(exc, phase="plan")
        if error.details is None:
            error.details = {
                "config_path": str(resolved.path) if resolved.path else None
            }
        raise error from exc
    to_public = getattr(plan, "to_public", None)
    public_plan = to_public() if callable(to_public) else plan
    plan_data = _as_dict(public_plan)
    config_label = str(resolved.path) if resolved.path else "flags/ENV"
    for warning in plan_data.get("warnings") or ():
        reporter.warning(str(warning))
    if args.dry_run:
        return (
            EXIT_OK,
            {
                "dry_run": True,
                "config_path": str(resolved.path) if resolved.path else None,
                "plan": plan_data,
            },
            f"Dry-run plan created; nothing executed. Config: {config_label}.",
        )

    mode = resolved.config.sync.mode.value
    if (
        mode in {"overwrite", "clone"} or _plan_is_destructive(plan)
    ) and not args.allow_delete:
        raise CLIError(
            "XTF_E_DELETE_CONFIRMATION_REQUIRED",
            "the requested mode or generated plan is destructive; rerun with --allow-delete",
            EXIT_CONFIG,
            {
                "plan": plan_data,
                "config_path": str(resolved.path) if resolved.path else None,
                "dry_run": False,
            },
        )

    reporter.info("Executing synchronization plan...")
    try:
        outcome = execute_method(plan)
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        raise _normalize_exception(exc, phase="execute") from exc
    outcome_data = _as_dict(outcome)
    ok = bool(getattr(outcome, "ok", outcome_data.get("ok", False)))
    if not ok:
        raw_status = getattr(outcome, "status", outcome_data.get("status", "failed"))
        status = str(getattr(raw_status, "value", raw_status))
        error_data = outcome_data.get("error")
        error_kind = (
            str(error_data.get("kind"))
            if isinstance(error_data, Mapping) and error_data.get("kind")
            else "mutation"
        )
        exit_codes = {
            "validation": EXIT_CONFIG,
            "auth": EXIT_AUTH,
            "resource": EXIT_REMOTE,
            "read": EXIT_REMOTE,
            "stale_snapshot": EXIT_REMOTE,
            "mutation": EXIT_PARTIAL,
            "verification": EXIT_VERIFICATION,
            "internal": EXIT_RUNTIME,
        }
        exit_code = (
            EXIT_INDETERMINATE
            if status == "indeterminate"
            else exit_codes.get(error_kind, EXIT_PARTIAL)
        )
        error_codes = {
            "validation": "XTF_E_CONFIG_INVALID",
            "auth": "XTF_E_AUTH",
            "resource": "XTF_E_RESOURCE",
            "read": "XTF_E_PLAN_INCOMPLETE",
            "stale_snapshot": "XTF_E_STALE_SNAPSHOT",
            "mutation": "XTF_E_MUTATION_REJECTED",
            "verification": "XTF_E_VERIFICATION_MISMATCH",
            "internal": "XTF_E_INTERNAL",
        }
        raise CLIError(
            (
                "XTF_E_INDETERMINATE"
                if status == "indeterminate"
                else error_codes.get(error_kind, "XTF_E_MUTATION_REJECTED")
            ),
            str(
                error_data.get("message")
                if isinstance(error_data, Mapping) and error_data.get("message")
                else error_data or status
            ),
            exit_code,
            {
                "outcome": outcome_data,
                "config_path": str(resolved.path) if resolved.path else None,
            },
        )
    return (
        EXIT_OK,
        {
            "dry_run": False,
            "config_path": str(resolved.path) if resolved.path else None,
            "outcome": outcome_data,
        },
        f"Synchronization completed. Config: {config_label}.",
    )


def _config_validate(args: argparse.Namespace) -> tuple[int, dict[str, Any], str]:
    resolved = resolve_config(args, require_file=True)
    path = str(resolved.path) if resolved.path else None
    return (
        EXIT_OK,
        {"valid": True, "schema_version": 2, "path": path},
        f"Valid v2 configuration: {path}",
    )


def _config_show(args: argparse.Namespace) -> tuple[int, dict[str, Any], str]:
    resolved = resolve_config(args, require_file=True)
    values = shown_values(resolved)
    result = {
        "schema_version": 2,
        "path": str(resolved.path) if resolved.path else None,
        "values": values,
        "sources": resolved.sources,
    }
    lines = [f"Configuration: {result['path']}"]
    for name in sorted(values):
        value = values[name]
        if name == "selective_sync" and isinstance(value, Mapping):
            for child, child_value in sorted(value.items()):
                full_name = f"selective_sync.{child}"
                lines.append(
                    f"{full_name}: {child_value!r} [{resolved.sources.get(full_name, 'unknown')}]"
                )
        else:
            lines.append(f"{name}: {value!r} [{resolved.sources.get(name, 'unknown')}]")
    return EXIT_OK, result, "\n".join(lines)


def _config_init(args: argparse.Namespace) -> tuple[int, dict[str, Any], str]:
    path = Path(args.output)
    write_template(
        path,
        force=args.force,
        source_type=args.source_type,
        target_type=args.target_type,
    )
    return (
        EXIT_OK,
        {
            "path": str(path),
            "schema_version": 2,
            "source_type": args.source_type,
            "target_type": args.target_type,
        },
        f"Created v2 configuration: {path}",
    )


def _doctor_local(
    args: argparse.Namespace,
) -> tuple[ResolvedConfig | None, list[dict[str, Any]]]:
    checks: list[dict[str, Any]] = [
        {
            "name": "python",
            "ok": sys.version_info >= (3, 10),
            "detail": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        }
    ]
    required_modules = ("pandas", "requests", "yaml")
    missing_modules = [
        name for name in required_modules if importlib.util.find_spec(name) is None
    ]
    checks.append(
        {
            "name": "dependencies",
            "ok": not missing_modules,
            "detail": (
                "available"
                if not missing_modules
                else f"missing: {', '.join(missing_modules)}"
            ),
        }
    )

    from utils.excel_reader import get_available_engines

    engines = get_available_engines()
    checks.append(
        {
            "name": "excel_engine",
            "ok": bool(engines["calamine"] or engines["openpyxl"]),
            "detail": {
                "primary": engines["primary"],
                "fallback": engines["fallback"],
            },
        }
    )
    path = discover_config(getattr(args, "config", None))
    resolved: ResolvedConfig | None = None
    if path or has_cli_overrides(args):
        resolved = resolve_config(args, require_file=bool(path))
        checks.append(
            {
                "name": "config",
                "ok": True,
                "detail": str(path) if path else "flags",
            }
        )
        source_type = resolved.config.source.type.value
        if source_type == "file":
            input_path = Path(resolved.config.source.file_path or "")
            input_exists = input_path.is_file()
            checks.append(
                {"name": "input", "ok": input_exists, "detail": str(input_path)}
            )
            if input_exists:
                from core.reader import DataFileReader

                checks.append(
                    {
                        "name": "input_format",
                        "ok": DataFileReader.is_supported(input_path),
                        "detail": input_path.suffix.lower(),
                    }
                )
    else:
        checks.append(
            {
                "name": "config",
                "ok": True,
                "detail": "not present; flags-only sync is available",
            }
        )
    return resolved, checks


def _doctor_network(resolved: ResolvedConfig) -> list[dict[str, Any]]:
    config = resolved.config
    from api import BitableBackend, SheetAPI
    from core.bootstrap import bootstrap_runtime
    from core.runtime_config import RuntimeBitableTarget, RuntimeSheetTarget

    dependencies = bootstrap_runtime(config)
    checks: list[dict[str, Any]] = []
    target_type = config.target.type.value
    if target_type == "bitable":
        target = config.target
        assert isinstance(target, RuntimeBitableTarget)
        backend = cast(BitableBackend, dependencies.target)
        target_fields = backend.list_fields(target.app_token, target.table_id)
        checks.append(
            {"name": "target_fields", "ok": True, "detail": len(target_fields)}
        )
        source_type = config.source.type.value
        if source_type == "bitable":
            source_fields = backend.list_fields(
                config.source.app_token or "", config.source.table_id or ""
            )
            checks.append(
                {"name": "source_fields", "ok": True, "detail": len(source_fields)}
            )
    else:
        target = config.target
        assert isinstance(target, RuntimeSheetTarget)
        sheet = cast(SheetAPI, dependencies.target)
        metadata = sheet.query_sheets(target.spreadsheet_token)
        found = any(item.sheet_id == target.sheet_id for item in metadata)
        if not found:
            raise CLIError(
                "XTF_E_REMOTE_RESOURCE_NOT_FOUND",
                f"sheet_id not found in spreadsheet metadata: {target.sheet_id}",
                EXIT_REMOTE,
            )
        checks.append({"name": "sheet_metadata", "ok": True, "detail": len(metadata)})
    return checks


def _doctor(
    args: argparse.Namespace, reporter: Reporter
) -> tuple[int, dict[str, Any], str]:
    resolved, checks = _doctor_local(args)
    if resolved is not None:
        reporter.add_sensitive_config(resolved.config)
    if args.network:
        if resolved is None:
            resolved = resolve_config(args)
            reporter.add_sensitive_config(resolved.config)
        reporter.info("Running read-only remote metadata checks...")
        checks.extend(_doctor_network(resolved))
    ok = all(bool(item["ok"]) for item in checks)
    if not ok:
        raise CLIError(
            "XTF_E_DOCTOR_FAILED",
            "one or more doctor checks failed",
            EXIT_INPUT,
            {"checks": checks},
        )
    config = resolved.config if resolved is not None else None
    return (
        EXIT_OK,
        {
            "network": bool(args.network),
            "checks": checks,
            "config_path": (
                str(resolved.path) if resolved is not None and resolved.path else None
            ),
            "source": (
                {"type": config.source.type.value} if config is not None else None
            ),
            "target": (
                {"type": config.target.type.value} if config is not None else None
            ),
            "requested_mode": (config.sync.mode.value if config is not None else None),
            "effective_mode": None,
        },
        (
            "Doctor checks passed. Config: "
            f"{str(resolved.path) if resolved is not None and resolved.path else 'flags/ENV or none'}."
        ),
    )


def _dispatch(
    args: argparse.Namespace, reporter: Reporter
) -> tuple[int, dict[str, Any], str]:
    if args.command == "sync":
        return _sync(args, reporter)
    if args.command == "doctor":
        return _doctor(args, reporter)
    if args.command == "config":
        if args.config_command == "init":
            return _config_init(args)
        if args.config_command == "validate":
            return _config_validate(args)
        if args.config_command == "show":
            return _config_show(args)
    raise CLIError("XTF_E_USAGE", "unsupported command", 2)


def _normalize_exception(exc: Exception, *, phase: str = "general") -> CLIError:
    if isinstance(exc, CLIError):
        return exc
    try:
        from api import FeishuAPIError, PartialBatchError

        if isinstance(exc, PartialBatchError):
            return CLIError("XTF_E_MUTATION_PARTIAL", str(exc), EXIT_PARTIAL)
        if isinstance(exc, FeishuAPIError):
            auth_codes = {99991661, 99991663, 99991664, 99991668}
            if exc.code in auth_codes or exc.http_status in {401, 403}:
                return CLIError("XTF_E_AUTH", str(exc), EXIT_AUTH)
            if exc.http_status == 404:
                return CLIError("XTF_E_RESOURCE", str(exc), EXIT_REMOTE)
            if phase == "execute":
                return CLIError("XTF_E_MUTATION_REJECTED", str(exc), EXIT_PARTIAL)
            return CLIError("XTF_E_REMOTE", str(exc), EXIT_REMOTE)
    except ImportError:
        pass
    return CLIError("XTF_E_RUNTIME", str(exc), EXIT_RUNTIME)


def main(argv: Sequence[str] | None = None) -> int:
    raw_argv = list(sys.argv[1:] if argv is None else argv)
    started = perf_counter()
    command_hint = next(
        (item for item in raw_argv if item in {"sync", "config", "doctor"}), None
    )
    reporter = Reporter(
        json_mode="--json" in raw_argv,
        quiet="--quiet" in raw_argv,
        command=command_hint,
    )
    captured = io.StringIO()

    def duration_ms() -> int:
        return max(0, round((perf_counter() - started) * 1000))

    def flush_diagnostics() -> None:
        diagnostics = reporter.redact(captured.getvalue())
        if not diagnostics:
            return
        if not reporter.quiet:
            print(diagnostics, end="", file=sys.stderr)
            return
        important = "".join(
            line
            for line in diagnostics.splitlines(keepends=True)
            if any(
                marker in line
                for marker in (" - WARNING - ", " - ERROR - ", " - CRITICAL - ")
            )
        )
        if important:
            print(important, end="", file=sys.stderr)

    try:
        args = parse_args(raw_argv)
        reporter = Reporter(
            json_mode=bool(getattr(args, "json", False)),
            quiet=bool(getattr(args, "quiet", False)),
            command=args.command,
        )
        with redirect_stdout(captured):
            exit_code, result, human = _dispatch(args, reporter)
        flush_diagnostics()
        reporter.success(args.command, result, human, duration_ms())
        return exit_code
    except ParserSignal as signal:
        if signal.message:
            print(signal.message, end="")
        return signal.status
    except KeyboardInterrupt:
        error = CLIError("XTF_E_INTERRUPTED", "interrupted by user", EXIT_INTERRUPTED)
        flush_diagnostics()
        reporter.error(error, duration_ms())
        return error.exit_code
    except Exception as exc:  # noqa: BLE001 - CLI boundary normalizes all failures.
        error = _normalize_exception(exc)
        flush_diagnostics()
        reporter.error(error, duration_ms())
        return error.exit_code


__all__ = ["main"]
