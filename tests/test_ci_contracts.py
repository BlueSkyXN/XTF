"""Executable checks on CI status aggregation and source/build dependency wiring."""

import itertools
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "lint,test",
    itertools.product(["success", "failure", "cancelled", "skipped"], repeat=2),
)
def test_summary_requires_both_jobs_success(lint, test):
    if os.name == "nt":
        pytest.skip("the summary job is Ubuntu/bash; exercised on Linux and macOS")
    workflow = yaml.safe_load((ROOT / ".github/workflows/test.yml").read_text())
    step = workflow["jobs"]["test-summary"]["steps"][0]
    result = subprocess.run(
        ["bash", "-c", step["run"]],
        env={
            **os.environ,
            "LINT_RESULT": lint,
            "TEST_RESULT": test,
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert (result.returncode == 0) == (lint == test == "success")


def test_build_depends_on_source_tests_and_tests_packaging_dependencies():
    jobs = yaml.safe_load(
        (ROOT / ".github/workflows/multi-platform-build.yml").read_text()
    )["jobs"]
    assert jobs["source-tests"]["uses"] == "./.github/workflows/test.yml"
    assert "source-tests" in jobs["build-xtf"]["needs"]
    steps = jobs["build-xtf"]["steps"]
    tests = next(
        i for i, item in enumerate(steps) if "python -m pytest" in item.get("run", "")
    )
    build = next(
        i
        for i, item in enumerate(steps)
        if "pyinstaller --onefile" in item.get("run", "")
    )
    assert tests < build
    assert "constraints-build.txt" in "\n".join(
        item.get("run", "") for item in steps[:tests]
    )
