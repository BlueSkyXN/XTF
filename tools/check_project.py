#!/usr/bin/env python3
"""Run reproducible local project checks with the current Python interpreter.

    python tools/check_project.py --runtime-only
    python tools/check_project.py --format

Missing optional development tools are reported as missing, never as passed.
"""

from __future__ import annotations

import argparse
import ast
import importlib.util
import py_compile
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_module(module: str, *args: str) -> bool | None:
    if not (ROOT / module).exists() and importlib.util.find_spec(module) is None:
        print(f"MISSING: {module}; install requirements-dev.txt", flush=True)
        return None
    command = [sys.executable, "-m", module, *args]
    print("RUN: " + " ".join(command), flush=True)
    return subprocess.run(command, cwd=ROOT, check=False).returncode == 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--runtime-only",
        action="store_true",
        help="only syntax, CLI, and pytest checks",
    )
    parser.add_argument(
        "--format", action="store_true", help="apply Black before checking"
    )
    args = parser.parse_args()
    outcomes: dict[str, bool | None] = {}
    files = [ROOT / "XTF.py"]
    for folder in ("api", "core", "utils", "xtf_cli", "tests", "tools"):
        files.extend(sorted((ROOT / folder).rglob("*.py")))
    try:
        for path in files:
            ast.parse(
                path.read_text(encoding="utf-8"),
                filename=str(path),
                feature_version=(3, 10),
            )
            py_compile.compile(str(path), doraise=True)
        outcomes["syntax (Python 3.10 grammar)"] = True
    except (SyntaxError, py_compile.PyCompileError) as error:
        print(error, file=sys.stderr)
        outcomes["syntax (Python 3.10 grammar)"] = False
    outcomes["CLI help"] = run_module("xtf_cli", "--help")
    outcomes["pytest"] = run_module("pytest", "tests/", "-q", "-m", "not integration")
    if not args.runtime_only:
        if args.format:
            outcomes["Black format"] = run_module("black", ".")
        outcomes["Ruff"] = run_module("ruff", "check", ".", "--ignore", "E501,F401")
        outcomes["Black check"] = run_module("black", "--check", ".")
        outcomes["MyPy"] = run_module(
            "mypy", "core/", "api/", "utils/", "xtf_cli/", "--ignore-missing-imports"
        )
    print("\nResults:")
    for label, status in outcomes.items():
        print(
            f"  {label}: {'MISSING' if status is None else 'PASS' if status else 'FAIL'}"
        )
    if any(value is False for value in outcomes.values()):
        return 1
    return 2 if any(value is None for value in outcomes.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
