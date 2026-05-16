# XTF repository agent instructions

## Purpose

XTF is a Python CLI tool that syncs local Excel or CSV data to Feishu Bitable or Feishu Sheet. The repository contains the unified CLI entrypoint, core sync engine, Feishu API wrappers, reusable file-reading helpers, tests, documentation, legacy standalone scripts, and GitHub Actions build workflows.

## Codex Startup Behavior

- Codex is normally started from the repository root, so this file is the repo-local startup router.
- Subdirectory `AGENTS.md` files are on-demand navigation cards. They are not assumed to be loaded when Codex starts at the root.
- Before editing a path whose directory has a local `AGENTS.md`, read that file first with `cat <path>/AGENTS.md` or an equivalent read command.
- If multiple nested `AGENTS.md` files exist on the path to a target file, read them from shallow to deep before changing files.
- Subdirectory cards contain local invariants, guardrails, and focused validation notes. Root rules still apply unless the closer card is stricter.

## Directory Map

| Path | Responsibility | Local AGENTS.md | Read When |
| --- | --- | --- | --- |
| `XTF.py` | CLI entrypoint, logging setup, config loading, sync orchestration, user-visible console output. | No | Changing CLI flow, startup messages, config bootstrapping, logging, or target dispatch. |
| `core/` | Config model, sync engine, field conversion, file reader facade, retry and rate-limit strategies. | Yes | Changing config priority, target inference, sync modes, selective sync, field strategies, batching, formula protection, retry, or rate limiting. |
| `api/` | Feishu authentication, base HTTP client, Bitable API wrapper, Sheet API wrapper. | Yes | Changing token handling, headers, pagination, Feishu response parsing, retryable errors, rate limits, batch write/delete, or range validation. |
| `utils/` | Reusable helper code, currently Excel engine detection and read support. | Yes | Changing optional Excel engines, file format handling, reader fallback, import boundaries, or helper side effects. |
| `tests/` | Pytest suite, fixtures, mocks, and test conventions. | Yes | Adding or changing tests, fixtures, mocks, markers, coverage scope, or CI expectations. |
| `docs/` | User/developer documentation for architecture, config, sync, fields, Sheet behavior, and control strategies. | Yes | Editing docs, examples, config references, OpenAPI descriptions, destructive-mode warnings, or documented commands. |
| `docs/feishu-openapi-doc/` | External Feishu OpenAPI reference submodule. | Covered by `docs/AGENTS.md` | Reading API reference is fine. Do not edit it as normal repository documentation. |
| `lite/` | Legacy standalone scripts and compatibility release entrypoints. | Yes | Changing `XTF_Bitable.py`, `XTF_Sheet.py`, legacy CLI behavior, legacy config compatibility, or legacy artifact names. |
| `.github/` | GitHub Actions tests, quality gates, coverage upload, multi-platform PyInstaller build, release assets. | Yes | Changing CI commands, Python/OS matrix, build jobs, artifact names, release uploads, or secrets usage. |
| `.codex/`, `.claude/` | Local agent/tooling metadata. | No | Avoid unless the user explicitly asks for local agent configuration changes. |
| `local/` | Ignored local scratch or experimental files. | No | Do not rely on it for committed behavior; do not create rules there unless explicitly requested. |
| `logs/` | Runtime logs. | No | Generated output; do not edit, commit, or use as a stable fixture. |
| `htmlcov/`, `.coverage`, coverage caches | Coverage reports and local test artifacts. | No | Generated output; do not edit or commit. |
| `__pycache__/`, `.pytest_cache/`, `.ruff_cache/`, `build/`, `dist/` | Generated/cache/build output. | No | Ignore for implementation and commits. |
| `config.example.yaml` | Committed configuration template and packaged default config source. | No | Changing config fields, defaults, examples, or release-package defaults. Keep docs and tests aligned. |
| `config.yaml` | Local real config, gitignored. | No | Use only for explicit local manual runs. Never commit it. |
| `requirements.txt` | Runtime dependency list. | No | Changing runtime dependencies or minimum versions. |
| `requirements-dev.txt` | Test/dev dependency list. | No | Changing pytest, coverage, or mock dependencies. |

## On-Demand Read Protocol

Before editing files under a directory that has a local navigation card, read the nearest card first:

```bash
cat core/AGENTS.md
cat api/AGENTS.md
cat utils/AGENTS.md
cat tests/AGENTS.md
cat docs/AGENTS.md
cat lite/AGENTS.md
cat .github/AGENTS.md
```

Then read the target module and the smallest related tests or docs. For example, an `api/sheet.py` range change should include `api/AGENTS.md`, the relevant `api/sheet.py` functions, related `core/engine.py` call sites if behavior crosses layers, and focused tests or docs that describe the same behavior.

## Commands

Run commands from the repository root unless noted.

| Command | Purpose | Scope | Sandbox Notes |
| --- | --- | --- | --- |
| `python -m pip install -r requirements.txt` | Install runtime dependencies. | repo | Requires network and writes environment packages. |
| `python -m pip install -r requirements-dev.txt` | Install pytest/coverage/mock test dependencies. | repo | Requires network and writes environment packages. |
| `pytest tests/ -v` | Run the whole pytest suite. | tests | No Feishu service required for unit tests. |
| `pytest tests/ -v -m "not integration" --tb=short --cov=core --cov=api --cov=utils --cov-report=term` | CI-like unit test and coverage run without XML output. | tests/core/api/utils | No external Feishu service required; depends on local test dependencies. |
| `ruff check . --ignore E501,F401` | CI Ruff check. | repo | Mirrors `.github/workflows/test.yml`. |
| `black --check .` | Formatting check only. | repo | Mirrors `.github/workflows/test.yml`; does not modify files. |
| `mypy core/ api/ utils/ --ignore-missing-imports` | Type check package modules. | core/api/utils | Mirrors `.github/workflows/test.yml`. |
| `python -m py_compile XTF.py core/*.py api/*.py utils/*.py` | Syntax check mainline Python files. | mainline | Mirrors `.github/workflows/test.yml`. |
| `python -m py_compile lite/*.py` | Syntax check legacy scripts. | lite | Useful for `lite/` changes; CI build compiles through PyInstaller instead. |
| `python XTF.py --target-type bitable --config config.yaml` | Manual Bitable sync run. | runtime | Requires valid local config, Feishu credentials, network, and may write/delete remote data. Use only when explicitly intended. |
| `python XTF.py --target-type sheet --config config.yaml` | Manual Sheet sync run. | runtime | Requires valid local config, Feishu credentials, network, and may write/delete remote data. Use only when explicitly intended. |

Build behavior is defined in `.github/workflows/multi-platform-build.yml`. There is no local build script. CI installs `pyinstaller`, builds `XTF.py`, `lite/XTF_Sheet.py`, and `lite/XTF_Bitable.py`, then packages per-platform and full release ZIP artifacts. Local replication requires network, PyInstaller, platform-specific tooling, and generated `dist/`/`artifacts/` output.

There are no repository migration or code generation commands.

## Architecture Boundaries

- `XTF.py` coordinates startup and orchestration. Keep business logic in the package modules.
- `core/` owns configuration semantics and sync behavior. It may call `api/` through explicit clients but should not embed raw Feishu HTTP request details.
- `api/` owns Feishu HTTP behavior, auth headers, retryable API errors, pagination, batch operations, and Sheet range validation.
- `utils/` must stay reusable and side-effect-light. It should not import `api/` or remote sync logic.
- `tests/` validates behavior with pytest and mocks. Unit tests should not require real Feishu APIs, network, local `config.yaml`, or runtime logs.
- `docs/` must describe behavior that is implemented in code or verified from Feishu OpenAPI reference material.
- `lite/` is compatibility code. Mainline improvements should not be introduced only in legacy scripts unless the change is explicitly legacy-only.
- `.github/` commands and root validation guidance must stay aligned. If workflow commands change, update this router.

## Global Rules

- Configuration priority is CLI arguments > YAML config > intelligent inference > defaults.
- Supported targets are `bitable` and `sheet`; shared changes must consider both unless the code path is target-specific by construction.
- Sync modes are `full`, `incremental`, `overwrite`, and `clone`.
- Treat `overwrite` and `clone` as destructive remote-data modes. Any change touching them must preserve clear logging, batching semantics, deletion scope, and failure handling.
- `selective_sync` must remain incompatible with `clone`.
- Sheet formula protection depends on dual reads using `Formula` and `FormattedValue`; do not rewrite formula columns unless the intended behavior is explicit and tested.
- Bitable field type behavior is strategy-based: `raw`, `base`, `auto`, `intelligence`. Preserve conservative defaults unless a config or test change explicitly covers the behavior shift.
- Bitable batch operations and Sheet chunked writes must keep API limits, auto-splitting, and retry behavior visible in logs.
- Keep `config.example.yaml`, docs, tests, and packaging assumptions synchronized when changing config keys or defaults.
- Prefer focused tests near the changed module. Broaden validation when touching shared config, sync engine behavior, API wrappers, or CI commands.
- Use mocks for HTTP responses, retry sleeps, rate-limit timing, and Feishu error cases.

## Do Not

- Do not commit real `app_secret`, `app_token`, `spreadsheet_token`, `tenant_access_token`, local `config.yaml`, runtime logs, or copied credentials.
- Do not hand-edit or commit `logs/`, `htmlcov/`, `.coverage`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `dist/`, `build/`, or generated artifacts.
- Do not edit `docs/feishu-openapi-doc/` as normal documentation; it is an external submodule reference.
- Do not weaken validation, lint, typecheck, test workflow settings, matrix coverage, or release artifact coverage without explaining the impact.
- Do not move raw Feishu API details into `core/` or sync orchestration into `api/`.
- Do not create unit tests that require network, real Feishu state, real credentials, local logs, or test execution order.
- Do not run manual sync commands against `config.yaml` unless the user explicitly asks for a real local run and accepts remote write/delete risk.

## Validation

Use the smallest meaningful validation first, then expand based on blast radius.

Default unit-test path:

```bash
pytest tests/ -v -m "not integration" --tb=short --cov=core --cov=api --cov=utils --cov-report=term
```

Default quality path:

```bash
ruff check . --ignore E501,F401
black --check .
mypy core/ api/ utils/ --ignore-missing-imports
python -m py_compile XTF.py core/*.py api/*.py utils/*.py
```

Add `python -m py_compile lite/*.py` for legacy script changes. For docs-only edits, there is no dedicated docs build; validate referenced commands, config keys, and examples against code or CI. For `.github/` edits, local commands can validate invoked checks, but complete workflow behavior requires GitHub Actions.

Manual sync commands are not default validation because they require valid Feishu credentials, network, and may mutate remote Bitable or Sheet data. If skipped, say so explicitly in the final report.

## Done Criteria

A change is complete when:

- The relevant local navigation card was read before editing files under that subtree.
- Only intended files were changed, and unrelated dirty files remain untouched.
- Focused tests or checks were run for the changed behavior.
- Shared or cross-target changes considered both `bitable` and `sheet`.
- Destructive modes (`overwrite`, `clone`) include explicit risk assessment and validation notes if affected.
- Any skipped validation is justified with a concrete reason.
