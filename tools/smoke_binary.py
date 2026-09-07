"""Exercise packaged CLI and its real XLSX reader without contacting Feishu."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
from typing import Sequence

import yaml
from openpyxl import Workbook


def run_smoke(command: Sequence[str], expected_version: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="xtf-binary-smoke-") as temp:
        folder = Path(temp)
        env = dict(
            os.environ,
            PYTHONUTF8="1",
            XTF_APP_SECRET="smoke-not-real",
            HTTP_PROXY="http://127.0.0.1:9",
            HTTPS_PROXY="http://127.0.0.1:9",
            http_proxy="http://127.0.0.1:9",
            https_proxy="http://127.0.0.1:9",
            NO_PROXY="",
            no_proxy="",
        )

        def launch(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(
                [*command, *args],
                cwd=folder,
                env=env,
                text=True,
                encoding="utf-8",
                capture_output=True,
                timeout=60,
            )

        version = launch("--version")
        if version.returncode or version.stdout.strip().split()[-1] != expected_version:
            raise RuntimeError("binary version does not match build version")
        if launch("sync", "--help").returncode:
            raise RuntimeError("packaged CLI help failed")
        path = folder / "config.yaml"
        if launch(
            "config", "init", "--target-type", "sheet", "--output", str(path)
        ).returncode:
            raise RuntimeError("packaged config init failed")
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        source = folder / "中文样例.xlsx"
        book = Workbook()
        book.active.append(["ID", "名称"])
        book.save(source)
        document["source"]["file"]["path"] = str(source)
        document["sync"]["mode"] = "incremental"
        document["sync"]["match_strategy"] = "append_only"
        document["sync"]["index"]["column"] = None
        document["target"]["sheet"]["protect_formulas"] = False
        document["control"]["max_retries"] = 0
        path.write_text(yaml.safe_dump(document, allow_unicode=True), encoding="utf-8")
        result = launch("sync", "--config", str(path), "--dry-run", "--json")
        try:
            output = json.loads(result.stdout)
            plan = output["result"]["plan"]
        except (ValueError, KeyError, TypeError) as error:
            raise RuntimeError(
                "packaged XLSX reader did not produce a valid dry-run result"
            ) from error
        if (
            result.returncode
            or output.get("ok") is not True
            or plan.get("actions") != []
            or plan.get("source", {}).get("rows") != 0
        ):
            raise RuntimeError(
                "header-only XLSX must produce an empty append-only plan offline"
            )
        # The duplicate header must reach the actual file reader, not pass by grepping JSON.
        book.active.cell(1, 2, "ID")
        book.save(source)
        result = launch("sync", "--config", str(path), "--dry-run", "--json")
        try:
            output = json.loads(result.stdout)
        except ValueError as error:
            raise RuntimeError(
                "packaged reader failure must remain structured JSON"
            ) from error
        if (
            result.returncode != 3
            or output.get("ok") is not False
            or output.get("error", {}).get("code") != "XTF_E_INPUT_READ"
        ):
            raise RuntimeError(
                "duplicate XLSX headers were not rejected by the packaged reader"
            )
    return {
        "status": "passed",
        "version": expected_version,
        "checks": [
            "version",
            "help",
            "config_init",
            "real_xlsx_empty_plan",
            "real_xlsx_duplicate_header",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--expected-version", required=True)
    args = parser.parse_args()
    print(
        json.dumps(run_smoke([str(args.executable.resolve())], args.expected_version))
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
