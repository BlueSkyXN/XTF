"""Explicit, isolated live UAT for source CLI or an already-built XTF binary.

Never invoked by ordinary pytest. See docs/EXECUTION_AND_UAT.md for fixture setup.
The application secret stays in the child environment, not arguments/config files.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from uuid import uuid4

from api.auth import FeishuAuth
from api.base import RetryableAPIClient
from api.bitable_v1 import BitableV1Backend
from api.bitable_v3 import BaseV3Backend
from api.sdk import FeishuResponseParser
from api.sheet import SheetAPI
from api.url import encode_path_segment
from core.verification import cells_equal, sheet_values_equal, wait_for_readback

ROOT = Path(__file__).resolve().parents[1]
SUITES = ("base_v3", "bitable_v1", "sheet")
SHEET_REGION = "A1:T200"
SCHEMA_V3 = [
    {"name": "ID", "type": "text"},
    {"name": "Name", "type": "text"},
    {"name": "Score", "type": "number"},
    {"name": "When", "type": "datetime"},
    {
        "name": "Tags",
        "type": "select",
        "multiple": True,
        "options": [{"name": "A"}, {"name": "B"}],
    },
    {"name": "Done", "type": "checkbox"},
]
SCHEMA_V1 = [
    {"field_name": "ID", "type": 1},
    {"field_name": "Name", "type": 1},
    {"field_name": "Score", "type": 2},
    {"field_name": "When", "type": 5},
    {
        "field_name": "Tags",
        "type": 4,
        "property": {"options": [{"name": "A"}, {"name": "B"}]},
    },
    {"field_name": "Done", "type": 7},
]


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value or value.startswith("REPLACE_"):
        raise ValueError(f"missing setting: {name}")
    return value


def save_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def require(check: bool, message: str) -> None:
    if not check:
        raise AssertionError(message)


def wait(check, message: str) -> None:
    observed = wait_for_readback(check, timeout=30, interval=0.5)
    require(observed.verified, message)


def validate_manifest(
    manifest: dict, suite: str, parent: str, sheet_id: str | None
) -> None:
    if (
        manifest.get("schema_version") != 1
        or manifest.get("suite") != suite
        or manifest.get("parent") != parent
        or manifest.get("region") != SHEET_REGION
        or not re.fullmatch(
            r"XTF_UAT_\d{14}_[0-9a-f]{12}", manifest.get("run_name", "")
        )
    ):
        raise ValueError("manifest does not identify this configured UAT resource")
    resource = manifest.get("resource_id")
    if not isinstance(resource, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", resource):
        raise ValueError(
            "resource ID unknown; inspect the creation result instead of guessing/deleting by name"
        )
    if suite == "sheet" and resource != sheet_id:
        raise ValueError("manifest sheet ID differs from configured UAT sheet")
    if manifest.get("state") not in {"created", "cleanup_failed", "cleaned"}:
        raise ValueError("manifest has no completed resource acquisition")


class LiveRun:
    """Small fixture harness; the sync being tested runs through the actual CLI."""

    def __init__(self, suite: str, manifest_path: Path, executable: Path | None = None):
        self.suite = suite
        self.app_id = required("XTF_UAT_APP_ID")
        self.secret = required("XTF_UAT_APP_SECRET")
        self.parent = required(
            "XTF_UAT_SPREADSHEET" if suite == "sheet" else "XTF_UAT_BASE"
        )
        self.sheet_id = required("XTF_UAT_SHEET_ID") if suite == "sheet" else None
        self.manifest_path = manifest_path
        self.executable = executable.resolve() if executable else None
        if self.executable and not self.executable.is_file():
            raise ValueError("specified XTF binary does not exist")
        self.transport = RetryableAPIClient(max_retries=0)
        self.auth = FeishuAuth(self.app_id, self.secret, api_client=self.transport)
        self.backend = (
            BaseV3Backend(self.auth)
            if suite == "base_v3"
            else BitableV1Backend(self.auth) if suite == "bitable_v1" else None
        )
        self.sheet = SheetAPI(self.auth, self.transport) if suite == "sheet" else None
        self.checks: list[str] = []
        self.manifest: dict = {}
        self.resource_id = ""

    def call(self, method: str, path: str, body=None, params=None) -> dict:
        kwargs = {
            "headers": self.auth.get_auth_headers(),
            "retry_transport": method == "GET",
        }
        if body is not None:
            kwargs["json"] = body
        if params is not None:
            kwargs["params"] = params
        response = self.transport.call_api(
            method, "https://open.feishu.cn/open-apis/" + path, **kwargs
        )
        envelope = FeishuResponseParser.parse(response)
        if method == "DELETE" and envelope.get("data") is None:
            return {}
        require(
            isinstance(envelope.get("data"), dict),
            "setup/cleanup response missing data object",
        )
        return envelope["data"]

    @property
    def table_path(self) -> str:
        parent = encode_path_segment(self.parent)
        return (
            f"base/v3/bases/{parent}/tables"
            if self.suite == "base_v3"
            else f"bitable/v1/apps/{parent}/tables"
        )

    def sheet_fixture_info(self):
        entries = self.sheet.query_sheets(self.parent)
        matches = [entry for entry in entries if entry.sheet_id == self.sheet_id]
        require(len(matches) == 1, "configured UAT sheet not found uniquely")
        entry = matches[0]
        require(
            str(entry.title or "").startswith("XTF_UAT_"),
            "UAT sheet title must start with XTF_UAT_",
        )
        grid = entry.grid_properties
        require(
            grid.get("row_count", 0) >= 200 and grid.get("column_count", 0) >= 20,
            "UAT sheet requires at least 200 rows and 20 columns",
        )
        return entry

    def acquire(self) -> None:
        if self.manifest_path.exists():
            raise ValueError(
                "manifest already exists; use another path or --cleanup-only"
            )
        name = (
            "XTF_UAT_"
            + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            + "_"
            + uuid4().hex[:12]
        )
        self.manifest = {
            "schema_version": 1,
            "suite": self.suite,
            "parent": self.parent,
            "run_name": name,
            "region": SHEET_REGION,
            "state": "creating",
            "resource_id": None,
            "source_sha": (
                os.getenv("XTF_TESTED_SOURCE_SHA") or os.getenv("GITHUB_SHA")
            ),
        }
        save_json(self.manifest_path, self.manifest)
        if self.suite == "sheet":
            info = self.sheet_fixture_info()
            # clone 清空整张工作表；不能只检查样例区域，也不能清空数据来让 setup 通过。
            original_render = self.sheet.value_render_option
            try:
                self.sheet.value_render_option = "Formula"
                values = self.sheet.get_sheet_data_chunked(
                    self.parent,
                    self.sheet_id,
                    1,
                    info.grid_properties["row_count"],
                    "A",
                    self.sheet.column_number_to_letter(
                        info.grid_properties["column_count"]
                    ),
                )
            finally:
                self.sheet.value_render_option = original_render
            require(
                sheet_values_equal([], values),
                "UAT sheet is not empty; use the previous manifest to clean its known run",
            )
            self.resource_id = self.sheet_id
            self.manifest["fixture_title"] = info.title
        else:
            body = (
                {"name": name, "fields": SCHEMA_V3}
                if self.suite == "base_v3"
                else {"table": {"name": name, "fields": SCHEMA_V1}}
            )
            created = self.call("POST", self.table_path, body)
            self.resource_id = created.get("table_id") or created.get("id") or ""
            require(
                bool(re.fullmatch(r"[A-Za-z0-9_-]+", self.resource_id)),
                "table creation returned no usable ID; creation is not retried",
            )
        self.manifest.update(resource_id=self.resource_id, state="created")
        save_json(self.manifest_path, self.manifest)
        if self.backend:
            wait(
                lambda: {
                    field.name
                    for field in self.backend.list_fields(self.parent, self.resource_id)
                }
                >= {"ID", "Name", "Score", "When", "Tags", "Done"},
                "created schema is not visible",
            )

    def table_info(self):
        params = (
            {"offset": 0, "limit": 100}
            if self.suite == "base_v3"
            else {"page_size": 100}
        )
        seen, seen_tokens = set(), set()
        match = None
        total_expected = None
        for _ in range(1000):
            data = self.call("GET", self.table_path, params=params)
            items = (
                data.get("tables", data.get("items"))
                if self.suite == "base_v3"
                else data.get("items")
            )
            require(isinstance(items, list), "table listing missing items")
            for item in items:
                require(isinstance(item, dict), "malformed table entry")
                rid = item.get("id", item.get("table_id"))
                require(
                    isinstance(rid, str) and rid and rid not in seen,
                    "table listing has invalid/duplicate IDs",
                )
                seen.add(rid)
                if rid == self.resource_id:
                    match = item
            if self.suite == "base_v3":
                total = data.get("total")
                require(
                    type(total) is int and total >= len(seen),
                    "table listing total is invalid",
                )
                require(
                    total_expected is None or total_expected == total,
                    "tables changed during listing",
                )
                total_expected = total
                if len(seen) == total:
                    return match
                require(bool(items), "empty table page before total")
                params["offset"] += len(items)
            else:
                require(
                    type(data.get("has_more")) is bool, "table listing missing has_more"
                )
                if not data["has_more"]:
                    return match
                token = data.get("page_token")
                require(
                    isinstance(token, str)
                    and token
                    and token not in seen_tokens
                    and bool(items),
                    "table pagination did not advance",
                )
                seen_tokens.add(token)
                params["page_token"] = token
        raise AssertionError("table listing exceeded finite page budget")

    def cleanup(self) -> None:
        validate_manifest(self.manifest, self.suite, self.parent, self.sheet_id)
        self.resource_id = self.manifest["resource_id"]
        if self.manifest["state"] == "cleaned":
            return
        try:
            if self.suite == "sheet":
                info = self.sheet_fixture_info()
                require(
                    info.title == self.manifest.get("fixture_title"),
                    "fixture title changed; cleanup stopped",
                )
                receipt = self.sheet.clear_values(
                    self.parent, f"{self.resource_id}!{SHEET_REGION}"
                )
                require(
                    receipt.outcome.value == "accepted",
                    "fixture clear was not fully accepted",
                )
                wait(
                    lambda: sheet_values_equal(
                        [],
                        self.sheet.get_sheet_data(
                            self.parent,
                            f"{self.resource_id}!{SHEET_REGION}",
                            value_render_option="Formula",
                        ),
                    ),
                    "fixture clear not visible",
                )
            else:
                # Only delete the exact ID created by this run. No title searches, no Base deletion.
                path = self.table_path + "/" + encode_path_segment(self.resource_id)
                info = self.table_info()
                if info is not None:
                    require(
                        info.get("name") == self.manifest["run_name"],
                        "temporary table name changed; deletion stopped",
                    )
                # After an interrupted cleanup the table may already be absent.
                # For a newly acquired run, issue DELETE even if a stale list omits it.
                if info is not None or self.manifest["state"] != "cleanup_failed":
                    self.call("DELETE", path)
                wait(
                    lambda: self.table_info() is None,
                    "temporary table deletion not visible",
                )
            self.manifest["state"] = "cleaned"
        except BaseException:
            self.manifest["state"] = "cleanup_failed"
            raise
        finally:
            save_json(self.manifest_path, self.manifest)

    def sync(
        self,
        rows: list[dict],
        columns: list[str],
        *,
        mode="full",
        selective=None,
        expected_exit=0,
    ) -> dict:
        import yaml

        with tempfile.TemporaryDirectory(prefix="xtf-uat-") as temp:
            folder = Path(temp)
            source = folder / "样例.csv"
            with source.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
            target = (
                {
                    "type": "sheet",
                    "sheet": {
                        "spreadsheet_token": self.parent,
                        "sheet_id": self.resource_id,
                        "start_row": 3,
                        "start_column": "B",
                        "protect_formulas": mode == "full",
                        "verify_formulas": False,
                    },
                }
                if self.suite == "sheet"
                else {
                    "type": "bitable",
                    "bitable": {
                        "app_token": self.parent,
                        "table_id": self.resource_id,
                        "api_backend": self.suite,
                        "create_missing_fields": True,
                    },
                }
            )
            config = {
                "schema_version": 2,
                "auth": {"app_id": self.app_id, "app_secret": None},
                "source": {"type": "file", "file": {"path": str(source)}},
                "target": target,
                "sync": {
                    "mode": mode,
                    "index": {"column": "ID"},
                    "verify_remote_writes": True,
                    "verify_timeout_seconds": 30,
                    "verify_interval_seconds": 0.5,
                },
                "control": {
                    "batch_size": 200,
                    "max_retries": 1,
                    "rate_limit_delay": 0.25,
                },
            }
            if mode != "clone":
                config["sync"]["match_strategy"] = "by_key"
            if selective:
                config["sync"]["selective"] = {"enabled": True, "columns": selective}
            config_path = folder / "config.yaml"
            config_path.write_text(
                yaml.safe_dump(config, allow_unicode=True), encoding="utf-8"
            )
            command = (
                [str(self.executable)]
                if self.executable
                else [sys.executable, str(ROOT / "XTF.py")]
            )
            command += ["sync", "--config", str(config_path), "--json"]
            if mode in {"clone", "overwrite"}:
                command.append("--allow-delete")
            env = dict(os.environ, XTF_APP_SECRET=self.secret, PYTHONUTF8="1")
            child = subprocess.run(
                command,
                cwd=folder,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
            )
            try:
                payload = json.loads(child.stdout)
            except (ValueError, TypeError) as error:
                raise AssertionError("CLI stdout is not one JSON document") from error
            # Do not persist stdout/stderr: server messages may contain fixture IDs or credentials.
            require(
                child.returncode == expected_exit,
                f"CLI exit {child.returncode}, expected {expected_exit}; code={payload.get('code', 'unknown')}",
            )
            if expected_exit == 0:
                require(payload.get("ok") is True, "CLI did not report success")
            return payload

    def records(self) -> dict:
        read = self.backend.list_records(self.parent, self.resource_id)
        require(
            read.complete and not read.ignored_fields, "UAT full read is incomplete"
        )
        by_id = {str(record.fields.get("ID")): record for record in read.records}
        require(len(by_id) == len(read.records), "UAT contains duplicate business keys")
        return by_id

    def records_when(self, predicate, message: str) -> dict:
        observed = {}

        def check():
            nonlocal observed
            observed = self.records()
            return predicate(observed)

        wait(check, message)
        return observed

    def exercise_base(self) -> None:
        rows = [
            {
                "ID": f"{i:04}",
                "Name": f" sample {i} ",
                "Score": i,
                "When": "2026-03-24T10:00:00+08:00",
                "Tags": "A,B",
                "Done": "否",
            }
            for i in range(201)
        ]
        columns = list(rows[0])
        self.sync(rows, columns)
        schema = {
            field.name: field
            for field in self.backend.list_fields(self.parent, self.resource_id)
        }
        require(schema["Tags"].multiple, "multiselect schema was not preserved")

        def all_rows_match(records):
            if set(records) != {row["ID"] for row in rows}:
                return False
            return all(
                records[row["ID"]].fields.get("Name") == row["Name"]
                and all(
                    cells_equal(
                        value, records[row["ID"]].fields.get(name), schema[name]
                    )
                    for name, value in (
                        ("Score", row["Score"]),
                        ("Tags", ["A", "B"]),
                        ("Done", False),
                        ("When", 1774317600000),
                    )
                )
                for row in rows
            )

        actual = self.records_when(all_rows_match, "201-row roundtrip mismatch")
        self.checks.append("201_record_roundtrip_text_number_date_multiselect_checkbox")
        target_record = actual["0000"].record_id
        self.sync(
            [{"ID": "0000", "Name": "updated", "Score": 999}],
            ["ID", "Name", "Score"],
            selective=["Name"],
        )
        actual = self.records_when(
            lambda found: "0000" in found
            and found["0000"].fields.get("Name") == "updated",
            "selective update not visible",
        )
        require(
            actual["0000"].record_id == target_record
            and actual["0000"].fields["Score"] == 0,
            "selective update changed an unselected field or identity",
        )
        self.checks.append("selective_update_preserves_id_and_other_fields")
        self.sync([{"ID": "0000", "Extra": "created-via-cli"}], ["ID", "Extra"])
        self.records_when(
            lambda found: "0000" in found
            and found["0000"].fields.get("Extra") == "created-via-cli",
            "new field/data not visible",
        )
        self.checks.append("new_field_created_then_records_written")
        self.sync(
            [{"ID": "0000", "Name": "not-written"}, {"ID": "0201", "Name": "new"}],
            ["ID", "Name"],
            mode="incremental",
        )
        actual = self.records_when(
            lambda found: len(found) == 202 and "0201" in found,
            "incremental append not visible",
        )
        require(
            actual["0000"].fields["Name"] == "updated",
            "incremental overwrote existing row",
        )
        self.checks.append("incremental_existing_rows_unchanged")
        # The invalid record is beyond the first write batch: planning must prevent all writes.
        invalid = [{"ID": f"bad{i}", "Score": "1"} for i in range(200)] + [
            {"ID": "bad200", "Score": "garbage"}
        ]
        self.sync(invalid, ["ID", "Score"], expected_exit=3)
        require(
            set(self.records()) == set(actual), "invalid tail allowed prefix writes"
        )
        self.checks.append("invalid_last_row_zero_writes")
        self.sync([], columns, mode="clone")
        self.records_when(
            lambda found: not found, "empty clone did not clear temporary table"
        )
        self.checks.append("empty_clone_clear_only")

    def exercise_sheet(self) -> None:
        from api.bitable_backend import MutationOutcome

        matrix = [
            ["Name", "ID", "Score"],
            ["oldA", "a", 1],
            [None, None, None],
            ["oldB", "b", 2],
        ]
        for region, values in (
            ("B3:D6", matrix),
            ("A1:A1", [[self.manifest["run_name"]]]),
        ):
            receipt = self.sheet.write_values(
                self.parent, f"{self.resource_id}!{region}", values
            )
            require(
                receipt.outcome is MutationOutcome.ACCEPTED, "Sheet fixture seed failed"
            )
            wait(
                lambda: sheet_values_equal(
                    values,
                    self.sheet.get_sheet_data(
                        self.parent,
                        f"{self.resource_id}!{region}",
                        value_render_option="UnformattedValue",
                    ),
                ),
                "Sheet seed not visible",
            )
        self.sync(
            [
                {"ID": "a", "Name": "newA", "Score": 11},
                {"ID": "b", "Name": "newB", "Score": 22},
            ],
            ["ID", "Name", "Score"],
        )
        observed = self.sheet.get_sheet_data(
            self.parent,
            f"{self.resource_id}!B3:D6",
            value_render_option="UnformattedValue",
        )
        require(
            sheet_values_equal(
                [
                    ["Name", "ID", "Score"],
                    ["newA", "a", 11],
                    [None] * 3,
                    ["newB", "b", 22],
                ],
                observed,
            ),
            "Sheet physical gap/header projection mismatch",
        )
        self.checks.append("sheet_offset_reordered_header_physical_gap")
        self.sync(
            [{"ID": "c", "Name": "newC", "Score": 33}],
            ["ID", "Name", "Score"],
            mode="incremental",
        )
        require(
            sheet_values_equal(
                [["newC", "c", 33]],
                self.sheet.get_sheet_data(
                    self.parent,
                    f"{self.resource_id}!B7:D7",
                    value_render_option="UnformattedValue",
                ),
            ),
            "Sheet append location mismatch",
        )
        require(
            self.sheet.get_sheet_data(self.parent, f"{self.resource_id}!A1:A1")
            == [[self.manifest["run_name"]]],
            "out-of-range sentinel changed",
        )
        self.checks.append("sheet_append_and_outside_sentinel")
        self.sync([], ["ID", "Name", "Score"], mode="clone")
        require(
            sheet_values_equal(
                [],
                self.sheet.get_sheet_data(
                    self.parent,
                    f"{self.resource_id}!B3:D7",
                    value_render_option="Formula",
                ),
            ),
            "empty Sheet clone did not clear values",
        )
        require(
            sheet_values_equal(
                [],
                self.sheet.get_sheet_data(
                    self.parent,
                    f"{self.resource_id}!A1:A1",
                    value_render_option="Formula",
                ),
            ),
            "whole-sheet clone did not clear the outside sentinel",
        )
        self.checks.append("sheet_empty_clone_clear_only")
        # Both a known good formula and an intentional error exercise the extension response.
        formulas = [
            [{"type": "formula", "text": "=1+1"}],
            [{"type": "formula", "text": "=1/0"}],
        ]
        receipt = self.sheet.write_values(
            self.parent, f"{self.resource_id}!G3:G4", formulas
        )
        require(
            receipt.outcome is MutationOutcome.ACCEPTED, "formula seed not accepted"
        )
        wait(
            lambda: sheet_values_equal(
                formulas,
                self.sheet.get_sheet_data(
                    self.parent,
                    f"{self.resource_id}!G3:G4",
                    value_render_option="Formula",
                ),
            ),
            "formulas not present",
        )
        wait(
            lambda: sheet_values_equal(
                [[2]],
                self.sheet.get_sheet_data(
                    self.parent,
                    f"{self.resource_id}!G3:G3",
                    value_render_option="UnformattedValue",
                ),
            ),
            "formula result is not 2",
        )
        good = self.sheet.verify_formulas(self.parent, [self.resource_id], ["G3:G3"])
        require(
            good.passed and good.raw.get("total_formulas", 0) >= 1,
            "good formula scan incomplete or no real formula",
        )
        bad = self.sheet.verify_formulas(self.parent, [self.resource_id], ["G4:G4"])
        require(
            bad.status == "errors_found" and bad.total_errors >= 1 and not bad.has_more,
            "bad formula not detected",
        )
        self.checks.append("sheet_ai_good_and_bad_formula")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", choices=SUITES, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, default=Path("uat-report.json"))
    parser.add_argument("--executable", type=Path)
    parser.add_argument("--cleanup-only", action="store_true")
    args = parser.parse_args(argv)
    report = {
        "suite": args.suite,
        "source_sha": (os.getenv("XTF_TESTED_SOURCE_SHA") or os.getenv("GITHUB_SHA")),
        "execution": "binary" if args.executable else "source",
        "status": "not_started",
        "checks": [],
    }
    run = None
    code = 1
    # Default library log messages can include HTTP details. Reports use bounded facts only.
    logging.getLogger("XTF").addHandler(logging.NullHandler())
    logging.getLogger("XTF").propagate = False
    try:
        run = LiveRun(args.suite, args.manifest, args.executable)
        if args.executable:
            report["binary_sha256"] = hashlib.sha256(
                args.executable.read_bytes()
            ).hexdigest()
        if args.cleanup_only:
            run.manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
            run.cleanup()
            report.update(status="cleanup_passed", cleanup="passed")
            code = 0
        else:
            run.acquire()
            if args.suite == "sheet":
                run.exercise_sheet()
            else:
                run.exercise_base()
            report["status"] = "passed"
            code = 0
    except (Exception, KeyboardInterrupt) as error:
        report.update(
            status="failed",
            error_type=type(error).__name__,
            code=getattr(error, "code", None),
        )
        if isinstance(error, (ValueError, AssertionError)):
            detail = str(error)
            for value in (
                os.getenv("XTF_UAT_APP_SECRET"),
                os.getenv("XTF_UAT_BASE"),
                os.getenv("XTF_UAT_SPREADSHEET"),
            ):
                if value:
                    detail = detail.replace(value, "[REDACTED]")
            report["detail"] = detail[:1000]
            if isinstance(error, ValueError) and detail.startswith("missing setting:"):
                report["status"], code = "not_configured", 2
    finally:
        if run is not None:
            report["checks"] = run.checks
            if not args.cleanup_only and run.resource_id:
                try:
                    run.cleanup()
                    report["cleanup"] = "passed"
                except (Exception, KeyboardInterrupt) as error:
                    report.update(
                        status="failed",
                        cleanup="failed",
                        cleanup_error_type=type(error).__name__,
                    )
                    code = 1
            elif not args.cleanup_only:
                report["cleanup"] = "not_acquired_or_creation_unknown"
        save_json(args.report, report)
    print(json.dumps(report, ensure_ascii=False))
    return code


if __name__ == "__main__":

    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
