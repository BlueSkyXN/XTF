# .github guardrail card

`.github/` contains workflows for quality gates, coverage, PyInstaller builds, bundles, and releases.
Read this card before changing workflow commands, Python or OS matrices, artifact names, release assets, package contents, retention, or secrets usage.
Key files: `workflows/test.yml`, `workflows/multi-platform-build.yml`, and the manual-only `workflows/build-1.9-rollback.yml`. Current live tests and publication use `workflows/api-live-write.yml` and `workflows/promote-release.yml`; read `docs/CI_API.md`.

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
- Before this new rollback workflow exists on the default branch, dispatch the already-registered `multi-platform-build.yml` at the cutover branch with `build_1_9_rollback=true`; it calls the same local reusable workflow and skips the normal 2.0/legacy bundle jobs.
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

## Current delivery flow (2026-09-05)

PR runs Tests; push/manual build calls the same-event Tests instead of a second push test run. Do not restore release-published builds. Promotion downloads a specified successful build run, executes all three live suites through each of its four packaged binaries, and uploads the same ZIP bytes after those results pass. `tools/release_artifacts.py` checks this correspondence. `tools/smoke_binary.py` is the offline real-XLSX binary smoke, not live API validation.

Use the shared `xtf-feishu-isolated-uat` concurrency group for these explicit fixtures. Live test and cleanup instructions are in `docs/EXECUTION_AND_UAT.md`. Missing setup or a failed cleanup is not a skipped pass. No live or release action was executed when preparing this source handoff.
