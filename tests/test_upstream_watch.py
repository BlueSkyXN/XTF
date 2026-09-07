import json

import pytest

from tools.check_upstream_api import compare_sources


@pytest.mark.parametrize("changed", [False, True])
def test_source_changes_require_review_without_changing_baseline(tmp_path, changed):
    manifest = {
        "repository": "larksuite/cli",
        "commit": "a" * 40,
        "files": ["shortcuts/base/example.go"],
    }

    def fetch(url):
        if url.endswith("commits/main"):
            return json.dumps({"sha": "b" * 40})
        return "after\n" if changed and "/" + "b" * 40 + "/" in url else "before\n"

    status, report = compare_sources(manifest, tmp_path, fetch)
    assert status == (1 if changed else 0)
    assert report["status"] == ("changed" if changed else "unchanged")
    assert manifest["commit"] == "a" * 40


def test_source_fetch_failure_is_not_unchanged(tmp_path):
    def fail(url):
        raise OSError("offline")

    with pytest.raises(OSError):
        compare_sources(
            {"repository": "larksuite/cli", "commit": "a" * 40, "files": []},
            tmp_path,
            fail,
        )
