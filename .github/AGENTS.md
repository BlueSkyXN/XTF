# .github guardrail card

`.github/` contains GitHub Actions workflows for lint/typecheck/tests, coverage upload, multi-platform PyInstaller builds, bundle creation, and release uploads.
Read this card before changing workflow commands, Python or OS matrices, artifact names, release assets, package contents, or secrets usage.
Key files: `workflows/test.yml` and `workflows/multi-platform-build.yml`.

## Why This Is High-Risk

- Workflow commands are the source for the root validation contract.
- Matrix changes alter supported Python versions or platforms.
- Build jobs package `config.example.yaml` as `config.yaml`, so template changes affect distributed defaults.
- Release jobs upload public artifacts and use repository secrets.

## Required Before Changes

- Read the target workflow and compare invoked commands with root `AGENTS.md`.
- Check whether a command change requires dependency updates in `requirements.txt` or `requirements-dev.txt`.
- For build changes, verify all three entrypoints are still handled: `XTF.py`, `lite/XTF_Sheet.py`, and `lite/XTF_Bitable.py`.
- For artifact changes, trace individual program ZIPs, per-platform `ALL-XTF-*` bundles, and `FULL-XTF-*` release bundle names.

## Do Not

- Do not put real secrets in workflows; use GitHub secrets such as `CODECOV_TOKEN` or `GITHUB_TOKEN`.
- Do not skip Ruff, Black check, MyPy, syntax checks, pytest, or failure exits without documenting the impact.
- Do not reduce Python 3.10-3.13 or Linux/Windows/macOS coverage casually.
- Do not change artifact retention, platform labels, binary names, or release asset names without checking downstream references.
- Do not run release publishing locally.

## Validation

- Local validation can run the commands invoked by `test.yml`: Ruff, Black check, MyPy, py_compile, and pytest.
- Complete matrix, artifact packaging, and release upload behavior require GitHub Actions.
