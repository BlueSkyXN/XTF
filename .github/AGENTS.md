# .github guardrail card

`.github/` contains workflows for quality gates, coverage, PyInstaller builds, bundles, and releases.
Read this card before changing workflow commands, Python or OS matrices, artifact names, release assets, package contents, retention, or secrets usage.
Key files: `workflows/test.yml` and `workflows/multi-platform-build.yml`.

## Why this is high-risk

- Workflow commands are the source for the root validation contract.
- Test matrices define Python/OS compatibility; build matrices define distributed binary platforms.
- Builds package `config.example.yaml` as `config.yaml`; template changes affect distributed defaults.
- Release jobs upload public artifacts and use repository secrets.

## Required before changes

- Read the target workflow and compare invoked commands with root `AGENTS.md`.
- Check whether command changes require dependency updates in `requirements.txt` or `requirements-dev.txt`.
- Preserve Ruff `0.15.13` unless a repo-wide lint migration is explicitly in scope.
- For build changes, verify all three entrypoints are still handled: `XTF.py`, `lite/XTF_Sheet.py`, and `lite/XTF_Bitable.py`.
- For artifact changes, trace individual program ZIPs, per-platform `ALL-XTF-*` bundles, and `FULL-XTF-*` release bundle names.

## Do not

- Do not put real secrets in workflows; use GitHub secrets such as `CODECOV_TOKEN` or `GITHUB_TOKEN`.
- Do not skip Ruff, Black check, MyPy, syntax checks, pytest, or failure exits without documenting impact and updating root guidance.
- Do not reduce the explicit test combinations or Linux x64/ARM64, Windows x64, and macOS ARM64 build targets without documenting compatibility impact.
- Do not change artifact retention, platform labels, binary names, or release asset names without checking downstream references.
- Do not run release publishing locally.

## Validation

- Local validation can run the commands invoked by `test.yml`: Ruff, Black check, MyPy, `py_compile`, and pytest.
- Complete matrix, artifact packaging, release bundle, and release upload behavior require GitHub Actions.
