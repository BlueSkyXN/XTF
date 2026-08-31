# .github guardrail card

`.github/` contains workflows for quality gates, coverage, PyInstaller builds, bundles, and releases.
Read this card before changing workflow commands, Python or OS matrices, artifact names, release assets, package contents, retention, or secrets usage.
Key files: `workflows/test.yml`, `workflows/multi-platform-build.yml`, and the manual-only `workflows/build-1.9-rollback.yml`.

## Why this is high-risk

- Workflow commands are the source for the root validation contract.
- Test matrices define Python/OS compatibility; build matrices define distributed binary platforms.
- XTF 2.0 builds ship `config.example.yaml` under that exact name; a real `config.yaml` must only come from `XTF config init`.
- Release jobs upload public artifacts and use repository secrets.

## Required before changes

- Read the target workflow and compare invoked commands with root `AGENTS.md`.
- Check whether command changes require dependency updates in `requirements.txt` or `requirements-dev.txt`.
- Preserve Ruff `0.15.13` unless a repo-wide lint migration is explicitly in scope.
- Preserve the build-only `PyInstaller==6.19.0` and `setuptools<82` pins until a separately validated packaging-tool upgrade; setuptools 82 removes the `pkg_resources` API expected by this PyInstaller runtime hook.
- Before the 1.9 rollback archive gate, keep the transitional legacy jobs usable. After that separately authorized gate, remove them and verify the single `XTF.py` matrix only.
- For XTF 2.0 artifact changes, trace the per-platform `XTF` ZIP, packaged docs/template/checksum, smoke commands, and any release bundle that consumes it.

## Do not

- Do not put real secrets in workflows; use GitHub secrets such as `CODECOV_TOKEN` or `GITHUB_TOKEN`.
- Do not skip Ruff, Black check, MyPy, syntax checks, pytest, or failure exits without documenting impact and updating root guidance.
- Do not reduce the explicit test combinations or Linux x64/ARM64, Windows x64, and macOS ARM64 build targets without documenting compatibility impact.
- Do not change artifact retention, platform labels, binary names, or release asset names without checking downstream references.
- Do not run release publishing locally.

## Validation

- Local validation can run the commands invoked by `test.yml`: Ruff, Black check, MyPy, `py_compile`, and pytest.
- Complete matrix, artifact packaging, release bundle, and release upload behavior require GitHub Actions.
