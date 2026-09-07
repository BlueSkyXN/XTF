"""Inspect and extract already-built packages; never rebuild a release candidate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import zipfile

PLATFORMS = ("linux-x64", "linux-arm64", "windows-x64", "macos-arm64")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def package_files(path: Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        if sum(item.file_size for item in archive.infolist()) > 2 * 1024**3:
            raise ValueError("package is unexpectedly large")
        seen = set()
        files = {}
        for info in archive.infolist():
            name = PurePosixPath(info.filename)
            if (
                name.is_absolute()
                or ".." in name.parts
                or "\\" in info.filename
                or ":" in info.filename
                or str(name) in seen
                or info.filename.rstrip("/") != str(name)
                or stat.S_ISLNK(info.external_attr >> 16)
            ):
                raise ValueError("package contains ambiguous/nonregular paths")
            seen.add(str(name))
            if not info.is_dir():
                files[str(name)] = archive.read(info)
        return files


def inspect_package(
    path: Path, platform: str, source_sha: str, run_id: str, version: str
) -> dict:
    if platform not in PLATFORMS or not re.fullmatch(r"[0-9a-f]{40}", source_sha):
        raise ValueError("invalid platform/source SHA")
    root = f"ALL-XTF-{platform}-{version}"
    binary = "XTF.exe" if platform == "windows-x64" else "XTF"
    expected = {
        binary,
        "config.example.yaml",
        "README.md",
        "QUICKSTART.md",
        "BUILD_INFO.json",
        "checksums.txt",
    }
    files = package_files(path)
    if set(files) != {f"{root}/{name}" for name in expected}:
        raise ValueError("package file set/version/platform is not exact")
    checksums = {}
    for line in files[f"{root}/checksums.txt"].decode().splitlines():
        match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
        if not match or match[2] in checksums:
            raise ValueError("invalid/duplicate package checksum")
        checksums[match[2]] = match[1]
    if set(checksums) != expected - {"checksums.txt"}:
        raise ValueError("checksums must cover all packaged files")
    for name, digest in checksums.items():
        if sha256(files[f"{root}/{name}"]) != digest:
            raise ValueError("package checksum mismatch")
    info = json.loads(files[f"{root}/BUILD_INFO.json"])
    if (
        info.get("source_sha") != source_sha
        or str(info.get("run_id")) != str(run_id)
        or info.get("target_platform") != platform
        or info.get("version") != version
    ):
        raise ValueError("package build information does not match the selected run")
    binary_digest = sha256(files[f"{root}/{binary}"])
    if info.get("binary_sha256") != binary_digest:
        raise ValueError("binary differs from build information")
    return {
        "platform": platform,
        "source_sha": source_sha,
        "run_id": str(run_id),
        "version": version,
        "package": path.name,
        "package_sha256": sha256(path.read_bytes()),
        "binary_sha256": binary_digest,
        "binary_relative": f"{root}/{binary}",
    }


def extract(path: Path, destination: Path, info: dict) -> Path:
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("artifact extraction directory must be empty")
    files = package_files(path)
    if sha256(path.read_bytes()) != info["package_sha256"]:
        raise ValueError("package changed since inspection")
    for name, content in files.items():
        target = destination / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    executable = destination / info["binary_relative"]
    executable.chmod(0o755)
    return executable.resolve()


def verify_release(
    packages: Path, reports: Path, sha: str, run_id: str, version: str
) -> dict:
    result = {
        "source_sha": sha,
        "build_run_id": str(run_id),
        "version": version,
        "platforms": [],
    }
    full_expected = {}
    for platform in PLATFORMS:
        name = f"ALL-XTF-{platform}-{version}.zip"
        matches = list(packages.rglob(name))
        if len(matches) != 1:
            raise ValueError(f"required package not unique: {platform}")
        package = matches[0]
        info = inspect_package(package, platform, sha, run_id, version)
        for suite in ("base_v3", "bitable_v1", "sheet"):
            paths = list(reports.rglob(f"uat-{platform}-{suite}.json"))
            if len(paths) != 1:
                raise ValueError(f"required UAT report not unique: {platform}/{suite}")
            report = json.loads(paths[0].read_text(encoding="utf-8"))
            required_checks = (
                {
                    "sheet_offset_reordered_header_physical_gap",
                    "sheet_append_and_outside_sentinel",
                    "sheet_ai_good_and_bad_formula",
                    "sheet_empty_clone_clear_only",
                }
                if suite == "sheet"
                else {
                    "201_record_roundtrip_text_number_date_multiselect_checkbox",
                    "selective_update_preserves_id_and_other_fields",
                    "incremental_existing_rows_unchanged",
                    "invalid_last_row_zero_writes",
                    "empty_clone_clear_only",
                    "new_field_created_then_records_written",
                }
            )
            if (
                report.get("status") != "passed"
                or report.get("cleanup") != "passed"
                or report.get("execution") != "binary"
                or report.get("suite") != suite
                or report.get("source_sha") != sha
                or report.get("binary_sha256") != info["binary_sha256"]
                or not required_checks <= set(report.get("checks", []))
            ):
                raise ValueError(
                    f"UAT does not cover this exact binary: {platform}/{suite}"
                )
        result["platforms"].append(info)
        full_expected.update(
            {
                f"FULL-XTF-{version}/{name}": content
                for name, content in package_files(package).items()
            }
        )
    full_paths = list(packages.rglob(f"FULL-XTF-{version}.zip"))
    if len(full_paths) != 1 or package_files(full_paths[0]) != full_expected:
        raise ValueError(
            "full release bundle differs from the four tested platform packages"
        )
    result["full_bundle_sha256"] = sha256(full_paths[0].read_bytes())
    result["status"] = "passed"
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--platform", choices=PLATFORMS)
    parser.add_argument("--packages", type=Path, required=True)
    parser.add_argument("--reports", type=Path)
    parser.add_argument("--destination", type=Path, default=Path("candidate"))
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,79}", args.version):
        raise ValueError("invalid version")
    if args.platform:
        paths = list(args.packages.rglob(f"ALL-XTF-{args.platform}-{args.version}.zip"))
        if len(paths) != 1:
            raise ValueError("platform package not unique")
        info = inspect_package(
            paths[0], args.platform, args.source_sha, args.run_id, args.version
        )
        binary = extract(paths[0], args.destination, info)
        if os.getenv("GITHUB_OUTPUT"):
            with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
                output.write(f"executable={binary}\n")
    else:
        if args.reports is None:
            parser.error("--reports is required to verify the entire release")
        info = verify_release(
            args.packages, args.reports, args.source_sha, args.run_id, args.version
        )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(info, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(info))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
