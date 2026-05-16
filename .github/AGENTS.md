# .github guardrail card

`.github/` contains GitHub Actions workflows for tests, quality checks, coverage upload, and PyInstaller builds.
Before editing it, read the target workflow and root `AGENTS.md` command list.
Keep reading this card when changing CI commands, matrices, artifact names, build packaging, or secrets usage.

## Key files

- `workflows/test.yml`: lint, Black check, MyPy, syntax check, pytest matrix, coverage upload.
- `workflows/multi-platform-build.yml`: multi-platform builds for `XTF.py` and legacy `lite/` scripts.

## Local invariants

- Root validation commands mirror `test.yml`; update root `AGENTS.md` if CI commands change.
- Preserve Python 3.10-3.13 and Linux/Windows/macOS coverage unless the PR explains the reduction.
- Build workflow must continue covering main `XTF` plus `XTF-Sheet` and `XTF-Bitable`.
- Release zips copy `config.example.yaml` as `config.yaml`; template changes affect packaged defaults.

## Do not

- Do not put real secrets in workflows; use GitHub secrets such as `CODECOV_TOKEN`.
- Do not skip lint, typecheck, tests, or failure exits without documenting the impact.
- Do not change artifact retention, platform labels, or names casually.

## Validation

Use root validation commands. Workflow execution itself requires GitHub Actions; local checks only validate the commands they invoke.
