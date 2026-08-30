import json
from dataclasses import dataclass
from typing import ClassVar

from xtf_cli import main

BASE_FLAGS = [
    "sync",
    "--app-id",
    "app",
    "--app-secret",
    "secret",
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


@dataclass
class FakeAction:
    kind: str
    count: int = 1
    scope: str = "test"
    destructive: bool = False
    clears_values: bool = False

    def to_dict(self):
        return {
            "kind": self.kind,
            "count": self.count,
            "scope": self.scope,
            "destructive": self.destructive,
            "clears_values": self.clears_values,
        }


class FakePlan:
    def __init__(self, actions=None, warnings=None):
        self.actions = actions or [FakeAction("create_records")]
        self.destructive = any(item.destructive for item in self.actions)
        self.clears_values = any(item.clears_values for item in self.actions)
        self.warnings = warnings or []

    def to_dict(self):
        return {
            "schema_version": 1,
            "requested_mode": "full",
            "effective_mode": "full",
            "source": {"type": "bitable"},
            "target": {"type": "bitable"},
            "actions": [item.to_dict() for item in self.actions],
            "warnings": self.warnings,
        }


class FakeOutcome:
    ok = True
    status = "success"
    applied: ClassVar[list[str]] = ["create_records"]
    error = None

    def to_dict(self):
        return {"ok": True, "status": "success", "applied": self.applied}


def install_fake_engine(
    monkeypatch,
    *,
    plan=None,
    plan_exception=None,
    outcome=None,
    emit=None,
    expect_none=True,
):
    calls = {"plan": 0, "execute": 0}
    selected_plan = plan or FakePlan()

    class FakeEngine:
        def __init__(self, config):
            self.config = config

        def plan(self, df=None):
            calls["plan"] += 1
            assert (df is None) is expect_none
            if emit:
                print(emit)
            if plan_exception is not None:
                raise plan_exception
            return selected_plan

        def execute_plan(self, value):
            calls["execute"] += 1
            assert value is selected_plan
            return outcome or FakeOutcome()

    monkeypatch.setattr("core.engine.XTFSyncEngine", FakeEngine)
    return calls


def test_dry_run_calls_planner_but_never_executor(monkeypatch, capsys):
    calls = install_fake_engine(monkeypatch)
    assert main(BASE_FLAGS + ["--dry-run"]) == 0
    captured = capsys.readouterr()
    assert calls == {"plan": 1, "execute": 0}
    assert "nothing executed" in captured.out
    assert "Planning" in captured.err


def test_formal_sync_executes_plan(monkeypatch, capsys):
    calls = install_fake_engine(monkeypatch)
    assert main(BASE_FLAGS) == 0
    captured = capsys.readouterr()
    assert calls == {"plan": 1, "execute": 1}
    assert "Synchronization completed" in captured.out


def destructive_file_flags(path, mode):
    return [
        "sync",
        "--app-id",
        "app",
        "--app-secret",
        "secret",
        "--source-type",
        "file",
        "--file",
        str(path),
        "--target-type",
        "bitable",
        "--target-app-token",
        "target-token",
        "--target-table-id",
        "target-table",
        "--index-column",
        "ID",
        "--mode",
        mode,
    ]


def test_destructive_dry_run_needs_no_allow_delete_and_never_executes(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "data.csv"
    path.write_text("ID\n1\n", encoding="utf-8")
    for mode in ("clone", "overwrite"):
        plan = FakePlan([FakeAction("delete_records", destructive=True)])
        calls = install_fake_engine(monkeypatch, plan=plan, expect_none=False)
        assert main(destructive_file_flags(path, mode) + ["--dry-run"]) == 0
        capsys.readouterr()
        assert calls == {"plan": 1, "execute": 0}


def test_destructive_formal_sync_requires_allow_delete_after_plan(
    tmp_path, monkeypatch, capsys
):
    path = tmp_path / "data.csv"
    path.write_text("ID\n1\n", encoding="utf-8")
    plan = FakePlan([FakeAction("delete_records", destructive=True)])
    calls = install_fake_engine(monkeypatch, plan=plan, expect_none=False)
    flags = destructive_file_flags(path, "clone")
    assert main(flags) == 3
    captured = capsys.readouterr()
    assert "XTF_E_DELETE_CONFIRMATION_REQUIRED" in captured.err
    assert calls == {"plan": 1, "execute": 0}


def test_planned_delete_requires_allow_delete_for_formal_execution(monkeypatch, capsys):
    plan = FakePlan([FakeAction("delete_records", destructive=True)])
    calls = install_fake_engine(monkeypatch, plan=plan)
    assert main(BASE_FLAGS) == 3
    captured = capsys.readouterr()
    assert calls == {"plan": 1, "execute": 0}
    assert "XTF_E_DELETE_CONFIRMATION_REQUIRED" in captured.err

    calls = install_fake_engine(monkeypatch, plan=plan)
    assert main(BASE_FLAGS + ["--allow-delete"]) == 0
    capsys.readouterr()
    assert calls == {"plan": 1, "execute": 1}


def test_clears_values_warning_alone_does_not_require_allow_delete(monkeypatch, capsys):
    plan = FakePlan([FakeAction("update_records", clears_values=True)])
    calls = install_fake_engine(monkeypatch, plan=plan)
    assert main(BASE_FLAGS + ["--dry-run"]) == 0
    capsys.readouterr()
    assert calls == {"plan": 1, "execute": 0}


def test_json_sync_has_exactly_one_stdout_object(monkeypatch, capsys):
    calls = install_fake_engine(monkeypatch)
    assert main(BASE_FLAGS + ["--dry-run", "--json"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["result"]["dry_run"] is True
    required = {
        "schema_version",
        "command",
        "status",
        "ok",
        "dry_run",
        "config_path",
        "source",
        "target",
        "requested_mode",
        "effective_mode",
        "plan",
        "applied",
        "verification",
        "warnings",
        "error",
        "duration_ms",
    }
    assert required <= set(payload)
    assert payload["source"] == {"type": "bitable"}
    assert captured.out.count("\n") == 1
    assert calls["execute"] == 0


def test_json_error_recursively_redacts_configured_secrets_and_tokens(
    monkeypatch, capsys
):
    from api import FeishuAPIError

    install_fake_engine(
        monkeypatch,
        plan_exception=FeishuAPIError(
            500,
            "request to https://example.invalid/target-token failed with secret",
            http_status=500,
        ),
    )

    assert main(BASE_FLAGS + ["--json"]) == 5
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    serialized = json.dumps(payload, ensure_ascii=False)
    assert "target-token" not in serialized
    assert "source-token" not in serialized
    assert "secret" not in serialized
    assert "[REDACTED]" in serialized


def test_quiet_suppresses_progress(monkeypatch, capsys):
    install_fake_engine(monkeypatch)
    assert main(BASE_FLAGS + ["--dry-run", "--quiet"]) == 0
    captured = capsys.readouterr()
    assert captured.err == ""
    assert "nothing executed" in captured.out


def test_quiet_preserves_plan_warnings(monkeypatch, capsys):
    install_fake_engine(monkeypatch, plan=FakePlan(warnings=["values will be cleared"]))
    assert main(BASE_FLAGS + ["--dry-run", "--quiet"]) == 0
    captured = capsys.readouterr()
    assert "Planning" not in captured.err
    assert "values will be cleared" in captured.err


def test_quiet_preserves_core_warning_diagnostics(monkeypatch, capsys):
    install_fake_engine(
        monkeypatch,
        emit="2026-08-31 - XTF - WARNING - remote metadata warning",
    )
    assert main(BASE_FLAGS + ["--dry-run", "--quiet"]) == 0
    captured = capsys.readouterr()
    assert "remote metadata warning" in captured.err


def test_core_diagnostics_redact_configured_tokens(monkeypatch, capsys):
    install_fake_engine(
        monkeypatch,
        emit="2026-08-31 - XTF - WARNING - target-token is unavailable",
    )
    assert main(BASE_FLAGS + ["--dry-run"]) == 0
    captured = capsys.readouterr()
    assert "target-token" not in captured.err
    assert "[REDACTED]" in captured.err


def test_core_stdout_is_diagnostic_stderr_not_final_stdout(monkeypatch, capsys):
    install_fake_engine(monkeypatch, emit="core-progress")
    assert main(BASE_FLAGS + ["--dry-run"]) == 0
    captured = capsys.readouterr()
    assert "core-progress" not in captured.out
    assert captured.out.strip() == (
        "Dry-run plan created; nothing executed. Config: flags/ENV."
    )
    assert "core-progress" in captured.err


def test_doctor_offline_does_not_construct_sdk(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)

    class ForbiddenSDK:
        def __init__(self, *args, **kwargs):
            raise AssertionError("offline doctor must not construct SDK")

    monkeypatch.setattr("api.XTFFeishuClient", ForbiddenSDK)
    assert main(["doctor", "--json"]) == 0
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["result"]["network"] is False
    checks = {item["name"]: item for item in payload["result"]["checks"]}
    assert checks["python"]["ok"] is True
    assert checks["dependencies"]["ok"] is True
    assert checks["excel_engine"]["ok"] is True


def test_input_failure_uses_exit_3_and_stable_code(capsys):
    flags = [
        "sync",
        "--app-id",
        "app",
        "--app-secret",
        "secret",
        "--target-type",
        "bitable",
        "--target-app-token",
        "target-token",
        "--target-table-id",
        "target-table",
        "--file",
        "missing.xlsx",
    ]
    assert main(flags) == 3
    captured = capsys.readouterr()
    assert "XTF_E_INPUT_NOT_FOUND" in captured.err


def test_runtime_auth_remote_partial_and_interrupt_exit_codes(monkeypatch, capsys):
    install_fake_engine(monkeypatch, plan_exception=RuntimeError("broken"))
    assert main(BASE_FLAGS) == 5
    assert "XTF_E_PLAN_INCOMPLETE" in capsys.readouterr().err

    from api import FeishuAPIError

    install_fake_engine(
        monkeypatch,
        plan_exception=FeishuAPIError(99991663, "denied", http_status=401),
    )
    assert main(BASE_FLAGS) == 4
    assert "XTF_E_AUTH" in capsys.readouterr().err

    install_fake_engine(
        monkeypatch,
        plan_exception=FeishuAPIError(500, "unavailable", http_status=500),
    )
    assert main(BASE_FLAGS) == 5
    assert "XTF_E_REMOTE" in capsys.readouterr().err

    install_fake_engine(
        monkeypatch,
        plan_exception=FeishuAPIError(40400, "missing", http_status=404),
    )
    assert main(BASE_FLAGS) == 4
    assert "XTF_E_RESOURCE" in capsys.readouterr().err

    class PartialOutcome:
        ok = False
        status = "partial"
        applied: ClassVar[list[FakeAction]] = [FakeAction("create_records")]
        error: ClassVar[dict[str, str]] = {"code": "write_failed"}

        def to_dict(self):
            return {
                "ok": False,
                "status": self.status,
                "applied": [item.to_dict() for item in self.applied],
                "error": self.error,
            }

    install_fake_engine(monkeypatch, outcome=PartialOutcome())
    assert main(BASE_FLAGS) == 6
    assert "XTF_E_MUTATION_REJECTED" in capsys.readouterr().err

    class VerificationOutcome(PartialOutcome):
        applied: ClassVar[list[FakeAction]] = []
        error: ClassVar[dict[str, str]] = {
            "kind": "verification",
            "message": "readback mismatch",
        }

    install_fake_engine(monkeypatch, outcome=VerificationOutcome())
    assert main(BASE_FLAGS) == 7
    assert "XTF_E_VERIFICATION_MISMATCH" in capsys.readouterr().err

    class AuthOutcome(PartialOutcome):
        applied: ClassVar[list[FakeAction]] = []
        error: ClassVar[dict[str, str]] = {
            "kind": "auth",
            "message": "permission denied",
        }

    install_fake_engine(monkeypatch, outcome=AuthOutcome())
    assert main(BASE_FLAGS) == 4
    assert "XTF_E_AUTH" in capsys.readouterr().err

    class ResourceOutcome(PartialOutcome):
        applied: ClassVar[list[FakeAction]] = []
        error: ClassVar[dict[str, str]] = {
            "kind": "resource",
            "message": "resource missing",
        }

    install_fake_engine(monkeypatch, outcome=ResourceOutcome())
    assert main(BASE_FLAGS) == 4
    assert "XTF_E_RESOURCE" in capsys.readouterr().err

    install_fake_engine(monkeypatch, plan_exception=KeyboardInterrupt())
    assert main(BASE_FLAGS) == 130
    assert "XTF_E_INTERRUPTED" in capsys.readouterr().err
