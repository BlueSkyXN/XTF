#!/usr/bin/env python3
"""Read-only live contract probes. No mutation, no implicit identity fallback.

python -m tools.probe_feishu_api --suite base_v3 --report api-probe.json
Read docs/CI_API.md for required fixture environment variables.
"""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path

from api.auth import FeishuAuth
from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from api.sheet import SheetAPI


def required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value or value.startswith("REPLACE_"):
        raise ValueError(f"missing fixture setting: {name}")
    return value


def probe_base(backend, base: str, table: str, record: str) -> dict:
    fields = backend.list_fields(base, table)
    result = backend.batch_get_records(base, table, [record])
    if (
        not fields
        or not result.complete
        or result.ignored_fields
        or result.record_not_found
        or len(result.records) != 1
        or result.records[0].record_id != record
    ):
        raise RuntimeError(
            "read-only Base probe did not return the complete fixture record"
        )
    return {"field_count": len(fields), "record_count": len(result.records)}


def formula_result_is_complete(result, minimum_formulas: int) -> bool:
    count = result.raw.get("total_formulas")
    return bool(
        result.passed
        and result.total_errors == 0
        and isinstance(count, int)
        and not isinstance(count, bool)
        and count >= minimum_formulas
    )


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--suite", choices=["base_v3", "bitable_v1", "sheet_ai"], required=True
    )
    parser.add_argument("--report", type=Path, default=Path("api-probe.json"))
    args = parser.parse_args(argv)
    report = {
        "suite": args.suite,
        "source_sha": os.getenv("GITHUB_SHA"),
        "mode": "read_only",
    }
    # Missing configuration is a distinct non-zero result, never a passing skip.
    try:
        app, secret = required_env("XTF_PROBE_APP_ID"), required_env(
            "XTF_PROBE_APP_SECRET"
        )
        if args.suite == "sheet_ai":
            token, sheet = required_env("XTF_PROBE_SPREADSHEET"), required_env(
                "XTF_PROBE_SHEET_ID"
            )
            region = required_env("XTF_PROBE_FORMULA_RANGE")
            minimum = int(required_env("XTF_PROBE_MIN_FORMULAS"))
            if minimum < 1:
                raise ValueError("minimum formulas must be positive")
        else:
            base, table = required_env("XTF_PROBE_BASE"), required_env(
                "XTF_PROBE_TABLE"
            )
            record = required_env("XTF_PROBE_RECORD")
    except (TypeError, ValueError) as exc:
        report.update(status="not_configured", detail=str(exc))
        args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
        return 2
    try:
        auth = FeishuAuth(app, secret)
        if args.suite == "sheet_ai":
            result = SheetAPI(auth).verify_formulas(
                token, sheet_ids=[sheet], ranges=[region]
            )
            if not formula_result_is_complete(result, minimum):
                raise RuntimeError("formula fixture is not completely verified")
            report["checks"] = {
                "status": result.status,
                "has_more": result.has_more,
                "total_formulas": result.raw.get("total_formulas"),
            }
        else:
            cls = BaseV3Backend if args.suite == "base_v3" else BitableV1Backend
            report["checks"] = probe_base(cls(auth), base, table, record)
        report["status"] = "passed"
        exit_code = 0
    except Exception as exc:
        # Avoid dumping server messages, credentials, row values or response bodies.
        report.update(
            status="failed",
            error_type=type(exc).__name__,
            code=getattr(exc, "code", None),
            http_status=getattr(exc, "http_status", None),
        )
        exit_code = 1
    args.report.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
