from unittest.mock import Mock

import pytest

from api.sheet import FormulaVerificationResult
from tools.probe_feishu_api import formula_result_is_complete, main


@pytest.mark.parametrize(
    "status,more,errors,count,passed",
    [
        ("success", False, 0, 1, True),
        ("partial", True, 0, 1, False),
        ("success", False, 0, 0, False),
        ("success", False, 0, True, False),
        ("success", False, 0, None, False),
        ("success", True, 0, 1, False),
        ("errors_found", False, 1, 1, False),
    ],
)
def test_probe_checks_json_not_only_cli_exit(status, more, errors, count, passed):
    result = FormulaVerificationResult(
        status, more, total_errors=errors, raw={"total_formulas": count}
    )
    assert formula_result_is_complete(result, 1) is passed


def test_no_credentials_is_not_a_pass(tmp_path, monkeypatch):
    monkeypatch.delenv("XTF_PROBE_APP_ID", raising=False)
    auth = Mock()
    monkeypatch.setattr("tools.probe_feishu_api.FeishuAuth", auth)
    output = tmp_path / "probe.json"
    assert main(["--suite", "base_v3", "--report", str(output)]) == 2
    assert '"not_configured"' in output.read_text()
    auth.assert_not_called()
