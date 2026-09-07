"""UAT orchestration and exact-binary publication, with no live API requests."""

import hashlib
import json
from pathlib import Path
from unittest.mock import Mock
import zipfile

import pytest
import yaml

from api.sheet import SheetMetadata
from tools import feishu_uat as uat
from tools import release_artifacts as release
from xtf_cli.config import resolve_config
from xtf_cli.parser import parse_args

SHA = "a" * 40
VERSION = "2.0.0-rc1"
NAME = "XTF_UAT_20260905080000_abcdef123456"


@pytest.fixture
def uat_env(monkeypatch):
    for name, value in {
        "XTF_UAT_APP_ID": "app",
        "XTF_UAT_APP_SECRET": "secret_test",
        "XTF_UAT_BASE": "base",
        "XTF_UAT_SPREADSHEET": "spreadsheet",
        "XTF_UAT_SHEET_ID": "sheet1",
    }.items():
        monkeypatch.setenv(name, value)


def manifest(suite="base_v3", **extra):
    result = {
        "schema_version": 1,
        "suite": suite,
        "parent": "base",
        "region": uat.SHEET_REGION,
        "run_name": NAME,
        "resource_id": "table1",
        "state": "created",
    }
    result.update(extra)
    return result


@pytest.mark.parametrize(
    "change",
    [
        {"parent": "other"},
        {"suite": "sheet"},
        {"resource_id": "../a"},
        {"resource_id": None},
        {"run_name": "Production"},
        {"region": "A1:ZZ50000"},
        {"state": "creating"},
    ],
)
def test_manifest_rejects_unrelated_resources(change):
    with pytest.raises(ValueError):
        uat.validate_manifest(manifest(**change), "base_v3", "base", None)


def test_missing_settings_are_nonzero_not_a_skipped_pass(tmp_path, monkeypatch):
    monkeypatch.delenv("XTF_UAT_APP_ID", raising=False)
    code = uat.main(
        [
            "--suite",
            "base_v3",
            "--manifest",
            str(tmp_path / "m.json"),
            "--report",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == 2
    assert json.loads((tmp_path / "r.json").read_text())["status"] == "not_configured"
    assert not (tmp_path / "m.json").exists()


@pytest.mark.parametrize("suite", ["base_v3", "bitable_v1", "sheet"])
def test_uat_launches_actual_cli_with_valid_config_and_secret_only_in_environment(
    tmp_path, monkeypatch, uat_env, suite
):
    runner = uat.LiveRun(suite, tmp_path / "m.json")
    runner.resource_id = "table1" if suite != "sheet" else "sheet1"

    def launch(command, **kwargs):
        assert command[1].endswith("XTF.py")
        assert "secret_test" not in " ".join(command)
        config_path = Path(command[command.index("--config") + 1])
        assert "secret_test" not in config_path.read_text()
        resolved = resolve_config(parse_args(command[2:]), environ=kwargs["env"])
        assert resolved.config.sync.verify_remote_writes
        assert resolved.config.auth.app_secret == "secret_test"
        return Mock(returncode=0, stdout='{"ok":true}', stderr="")

    monkeypatch.setattr(uat.subprocess, "run", launch)
    runner.sync([{"ID": "a", "Name": "x"}], ["ID", "Name"])
    runner.sync([], ["ID", "Name"], mode="clone")


def test_sheet_fixture_uses_real_typed_metadata_and_never_clears_nonempty_setup(
    tmp_path, uat_env
):
    runner = uat.LiveRun("sheet", tmp_path / "m.json")
    runner.sheet = Mock()
    runner.sheet.query_sheets.return_value = (
        SheetMetadata(
            "sheet1",
            "XTF_UAT_fixture",
            grid_properties={"row_count": 200, "column_count": 20},
        ),
    )
    runner.sheet.get_sheet_data.return_value = [["already used"]]
    with pytest.raises(AssertionError, match="not empty"):
        runner.acquire()
    runner.sheet.clear_values.assert_not_called()
    assert not runner.resource_id


def test_v3_acquire_records_id_before_schema_read_failure(tmp_path, uat_env):
    runner = uat.LiveRun("base_v3", tmp_path / "m.json")
    runner.call = Mock(return_value={"id": "table1"})
    runner.backend = Mock()
    runner.backend.list_fields.side_effect = RuntimeError("unavailable")
    with pytest.raises(RuntimeError):
        runner.acquire()
    saved = json.loads((tmp_path / "m.json").read_text())
    assert saved["resource_id"] == "table1" and saved["state"] == "created"
    assert runner.call.call_count == 1
    assert runner.call.call_args.args[2]["fields"] == uat.SCHEMA_V3


def test_cleanup_exact_table_and_read_absence(tmp_path, uat_env):
    runner = uat.LiveRun("base_v3", tmp_path / "m.json")
    runner.manifest = manifest()
    runner.call = Mock(return_value={})
    runner.table_info = Mock(side_effect=[{"id": "table1", "name": NAME}, None])
    runner.cleanup()
    runner.call.assert_called_once_with("DELETE", "base/v3/bases/base/tables/table1")
    assert json.loads((tmp_path / "m.json").read_text())["state"] == "cleaned"


def test_cleanup_cannot_hide_changed_table_identity(tmp_path, uat_env):
    runner = uat.LiveRun("base_v3", tmp_path / "m.json")
    runner.manifest = manifest()
    runner.call = Mock()
    runner.table_info = Mock(return_value={"id": "table1", "name": "changed"})
    with pytest.raises(AssertionError):
        runner.cleanup()
    runner.call.assert_not_called()
    assert runner.manifest["state"] == "cleanup_failed"


def test_main_cleans_when_scenario_fails_and_reports_failure(tmp_path, monkeypatch):
    runner = Mock(resource_id="table1", checks=["one"])
    runner.exercise_base.side_effect = AssertionError("scenario failed")
    monkeypatch.setattr(uat, "LiveRun", Mock(return_value=runner))
    code = uat.main(
        [
            "--suite",
            "base_v3",
            "--manifest",
            str(tmp_path / "m.json"),
            "--report",
            str(tmp_path / "r.json"),
        ]
    )
    assert code == 1
    runner.cleanup.assert_called_once()
    report = json.loads((tmp_path / "r.json").read_text())
    assert report["status"] == "failed" and report["cleanup"] == "passed"


def test_main_cleanup_failure_prevents_pass(tmp_path, monkeypatch):
    runner = Mock(resource_id="table1", checks=["one"])
    runner.cleanup.side_effect = RuntimeError("unavailable")
    monkeypatch.setattr(uat, "LiveRun", Mock(return_value=runner))
    assert (
        uat.main(
            [
                "--suite",
                "base_v3",
                "--manifest",
                str(tmp_path / "m.json"),
                "--report",
                str(tmp_path / "r.json"),
            ]
        )
        == 1
    )
    assert json.loads((tmp_path / "r.json").read_text())["cleanup"] == "failed"


def build_package(tmp_path, platform="linux-x64", **metadata):
    root = f"ALL-XTF-{platform}-{VERSION}"
    binary = "XTF.exe" if platform == "windows-x64" else "XTF"
    files = {
        binary: b"binary",
        "config.example.yaml": b"example",
        "README.md": b"readme",
        "QUICKSTART.md": b"quickstart",
    }
    info = {
        "source_sha": SHA,
        "run_id": "123",
        "version": VERSION,
        "target_platform": platform,
        "binary_sha256": hashlib.sha256(files[binary]).hexdigest(),
        **metadata,
    }
    files["BUILD_INFO.json"] = json.dumps(info).encode()
    files["checksums.txt"] = "".join(
        f"{hashlib.sha256(data).hexdigest()}  {name}\n" for name, data in files.items()
    ).encode()
    path = tmp_path / f"{root}.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for name, data in files.items():
            archive.writestr(f"{root}/{name}", data)
    return path


def test_exact_package_inspection_and_extraction(tmp_path):
    path = build_package(tmp_path)
    info = release.inspect_package(path, "linux-x64", SHA, "123", VERSION)
    binary = release.extract(path, tmp_path / "out", info)
    assert binary.read_bytes() == b"binary"
    assert info["binary_sha256"] == hashlib.sha256(binary.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    "metadata",
    [
        {"source_sha": "b" * 40},
        {"run_id": "124"},
        {"target_platform": "windows-x64"},
        {"version": "2.0.0"},
        {"binary_sha256": "0" * 64},
    ],
)
def test_other_build_not_publishable(tmp_path, metadata):
    path = build_package(tmp_path, **metadata)
    with pytest.raises(ValueError):
        release.inspect_package(path, "linux-x64", SHA, "123", VERSION)


@pytest.mark.parametrize(
    "name", ["../escape", "/root", "a/../escape", "a\\b", "a//b", "C:/file"]
)
def test_package_rejects_ambiguous_paths_before_extraction(tmp_path, name):
    path = tmp_path / "bad.zip"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, b"bad")
    with pytest.raises(ValueError):
        release.package_files(path)


def test_all_reports_required_and_full_bundle_not_rebuilt(tmp_path):
    reports = tmp_path / "reports"
    reports.mkdir()
    complete = {
        "201_record_roundtrip_text_number_date_multiselect_checkbox",
        "selective_update_preserves_id_and_other_fields",
        "incremental_existing_rows_unchanged",
        "invalid_last_row_zero_writes",
        "empty_clone_clear_only",
        "new_field_created_then_records_written",
        "sheet_offset_reordered_header_physical_gap",
        "sheet_append_and_outside_sentinel",
        "sheet_ai_good_and_bad_formula",
        "sheet_empty_clone_clear_only",
    }
    allfiles = {}
    for platform in release.PLATFORMS:
        path = build_package(tmp_path, platform)
        allfiles.update(release.package_files(path))
        for suite in uat.SUITES:
            (reports / f"uat-{platform}-{suite}.json").write_text(
                json.dumps(
                    {
                        "suite": suite,
                        "status": "passed",
                        "cleanup": "passed",
                        "execution": "binary",
                        "source_sha": SHA,
                        "binary_sha256": hashlib.sha256(b"binary").hexdigest(),
                        "checks": list(complete),
                    }
                )
            )
    full = tmp_path / f"FULL-XTF-{VERSION}.zip"
    with zipfile.ZipFile(full, "w") as archive:
        for name, data in allfiles.items():
            archive.writestr(f"FULL-XTF-{VERSION}/{name}", data)
    assert (
        release.verify_release(tmp_path, reports, SHA, "123", VERSION)["status"]
        == "passed"
    )
    bad = reports / "uat-linux-x64-sheet.json"
    data = json.loads(bad.read_text())
    data["cleanup"] = "failed"
    bad.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="UAT"):
        release.verify_release(tmp_path, reports, SHA, "123", VERSION)


def test_promotion_wiring_does_not_rebuild_or_publish_before_binary_suites():
    root = Path(__file__).resolve().parents[1]
    build = yaml.safe_load(
        (root / ".github/workflows/multi-platform-build.yml").read_text()
    )
    assert "publish-release" not in build["jobs"]
    promote = yaml.safe_load(
        (root / ".github/workflows/promote-release.yml").read_text()
    )
    assert set(promote["jobs"]["publish"]["needs"]) == {"candidate", "binary-uat"}
    assert promote["jobs"]["binary-uat"]["strategy"]["max-parallel"] == 1
    script = "\n".join(
        step.get("run", "") for job in promote["jobs"].values() for step in job["steps"]
    )
    assert (
        "pyinstaller" not in script
        and "release_artifacts" in script
        and "--executable" in script
    )
    assert promote["concurrency"]["cancel-in-progress"] is False


def test_independent_uat_read_waits_without_reissuing_sync(tmp_path, uat_env):
    runner = uat.LiveRun("base_v3", tmp_path / "m.json")
    runner.records = Mock(side_effect=[{}, {"a": "record"}])
    runner.sync = Mock()
    assert runner.records_when(lambda found: "a" in found, "not visible") == {
        "a": "record"
    }
    runner.sync.assert_not_called()
    assert runner.records.call_count == 2


def test_offline_packaging_harness_runs_the_actual_source_cli():
    import sys
    from tools.smoke_binary import run_smoke
    from xtf_cli.version import VERSION

    root = Path(__file__).resolve().parents[1]
    result = run_smoke([sys.executable, str(root / "XTF.py")], VERSION)
    assert (
        result["status"] == "passed"
        and "real_xlsx_duplicate_header" in result["checks"]
    )


def test_pr_tests_are_not_duplicated_by_the_build_workflow():
    root = Path(__file__).resolve().parents[1]
    tests = yaml.safe_load((root / ".github/workflows/test.yml").read_text())
    build = yaml.safe_load(
        (root / ".github/workflows/multi-platform-build.yml").read_text()
    )
    # PyYAML uses YAML 1.1: the workflow key `on` is parsed as True.
    assert "pull_request" in tests[True] and "push" not in tests[True]
    assert "push" in build[True] and "pull_request" not in build[True]
    assert "workflow_call" in tests[True]
