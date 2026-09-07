#!/usr/bin/env python3
"""Compare selected official CLI files with the reviewed revision.

Exit 0: selected files unchanged; 1: review needed; 2: comparison incomplete.
This tool never updates the reviewed commit or expected test outputs.
"""

from __future__ import annotations
import argparse
import difflib
import json
import re
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]


def fetch_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "XTF-api-contract-monitor"})
    with urlopen(request, timeout=30) as response:
        raw = response.read(8_000_001)
    if len(raw) > 8_000_000:
        raise ValueError("source file unexpectedly exceeds 8 MB")
    return raw.decode("utf-8")


def compare_sources(manifest: dict, output: Path, fetch=fetch_text) -> tuple[int, dict]:
    # Repository identity is intentionally fixed; never compare a similarly named fork.
    if manifest.get("repository") != "larksuite/cli":
        raise ValueError("unexpected upstream repository")
    baseline = manifest["commit"]
    if not re.fullmatch(r"[0-9a-f]{40}", baseline):
        raise ValueError("reviewed commit must be a full SHA")
    head = json.loads(fetch("https://api.github.com/repos/larksuite/cli/commits/main"))[
        "sha"
    ]
    if not isinstance(head, str) or not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("invalid current commit response")
    output.mkdir(parents=True, exist_ok=True)
    changed = []
    for name in manifest["files"]:
        path = Path(name)
        if path.is_absolute() or ".." in path.parts:
            raise ValueError("unexpected upstream file path")
        root = "https://raw.githubusercontent.com/larksuite/cli"
        old = fetch(f"{root}/{baseline}/{name}")
        new = old if head == baseline else fetch(f"{root}/{head}/{name}")
        if old != new:
            changed.append(name)
            diff = "".join(
                difflib.unified_diff(
                    old.splitlines(True),
                    new.splitlines(True),
                    fromfile=f"{baseline}/{name}",
                    tofile=f"{head}/{name}",
                )
            )
            (output / (name.replace("/", "__") + ".diff")).write_text(
                diff, encoding="utf-8"
            )
    report = {
        "status": "changed" if changed else "unchanged",
        "baseline": baseline,
        "head": head,
        "changed_files": changed,
        "scope": "selected_cli_files_only",
    }
    return (1 if changed else 0), report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("upstream-api-drift"))
    args = parser.parse_args(argv)
    try:
        manifest = json.loads((ROOT / "contracts/larkcli-sources.json").read_text())
        code, report = compare_sources(manifest, args.output)
    except Exception as exc:
        code, report = 2, {"status": "incomplete", "error_type": type(exc).__name__}
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
