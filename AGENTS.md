# XTF repository agent instructions

## Purpose

XTF is a Python 3.10+ CLI tool that syncs local Excel or CSV data to Feishu Bitable or Feishu Sheet. XTF 2.0 exposes one CLI, strict YAML v2, JSON/exit-code contracts, one typed sync service, internal Feishu clients, file-reading helpers, pytest coverage, documentation, and GitHub Actions workflows. It does not provide a stable Python SDK.

## Codex startup behavior

- Codex is normally started from the repository root. This file is the repo-local startup router.
- Subdirectory `AGENTS.md` files are on-demand navigation cards. They are not assumed to be loaded when Codex starts at the root.
- Before editing a path whose directory has a local `AGENTS.md`, read that file first with `cat <path>/AGENTS.md` or an equivalent read command.
- If multiple nested `AGENTS.md` files exist on the path to a target file, read them from shallow to deep before changing files.
- Root rules apply everywhere unless a closer `AGENTS.md` is stricter for its subtree.

## Directory map

| Path | Responsibility | Local AGENTS.md | Read when |
| --- | --- | --- | --- |
| `XTF.py` | Thin executable entrypoint that delegates to the XTF 2.0 CLI and exits with its status code. | No | Change the source/PyInstaller entrypoint or process-exit behavior. |
| `xtf_cli/` | XTF 2.0 command parser, strict config-v2 resolver, command dispatch, output rendering, diagnostics, and version source. | No | Change subcommands, flags, config precedence/discovery, JSON/human output, exit codes, doctor behavior, or CLI versioning. |
| `core/` | Immutable runtime config, typed sync service, plans/results, snapshots, reconciliation, target compilers, conversion, retry/rate-limit strategies, and destructive-mode safety. | Yes | Change config semantics, sync modes, plans/results, snapshots, batching, conversion, selective sync, formula protection, retry, rate limiting, or cross-target behavior. |
| `api/` | Internal typed Feishu contracts, auth, transport, pagination/batching helpers, versioned Bitable backends, and Sheet client. | Yes | Change internal exports, token handling, typed errors, retry ownership, page tokens, batch failure semantics, Bitable operations, or Sheet ranges/chunks/styles. |
| `utils/` | Side-effect-light helpers, currently Excel engine detection and smart Excel read support. | Yes | Change optional Excel engines, engine fallback, supported formats, import boundaries, helper side effects, or read error behavior. |
| `tests/` | Pytest suite for v2 config/runtime, sync behavior, typed API contracts, readers, conversion, retries, and rate limiting. | Yes | Add/change tests, fixtures, mocks, markers, coverage scope, CI assumptions, fake credentials, or test timing. |
| `docs/` | User/developer documentation for architecture, config, sync modes, field strategies, Sheet behavior, and control strategies. | Yes | Edit docs, examples, config references, CLI commands, Feishu OpenAPI claims, destructive-mode warnings, or package defaults wording. |
| `docs/feishu-openapi-doc/` | Git submodule containing external Feishu OpenAPI reference material. | Covered by `docs/AGENTS.md` | Reading API reference is fine. Do not edit as normal repo documentation unless the user explicitly asks to update the submodule/reference. |
| `.github/` | GitHub Actions quality gates, coverage upload, multi-platform PyInstaller builds, bundles, and release assets. | Yes | Change workflow commands, Python/OS matrix, build entrypoints, artifact names, release upload behavior, secrets, or retention. |
| `.codex/` | Local agent/tooling metadata. | No | Avoid unless the user explicitly asks for local Codex configuration changes. |
| `logs/` | Runtime log output. | No | Generated output; do not edit, commit, or use as stable test fixtures. |
| `htmlcov/`, `.coverage`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `build/`, `dist/`, `artifacts/` | Coverage, cache, build, and packaging output. | No | Generated output; ignore for implementation and commits. |
| `config.example.yaml` | Strict nested XTF 2.0 configuration template shipped under the same name. | No | Change only with aligned resolver/docs/tests; a real `config.yaml` is created only by `XTF config init`. |
| `config.yaml` | Local real config, gitignored. | No | Use only for explicit local manual runs. Never commit or quote real credentials. |
| `requirements.txt` | Runtime dependency list for the CLI and CI builds. | No | Change runtime dependencies or minimum versions. Keep workflows and docs aligned. |
| `requirements-dev.txt` | Test/dev dependency list for pytest coverage and mocks. | No | Change pytest, coverage, mock, lint, or typecheck dependency assumptions. |
| `.gitmodules` | Submodule pointer for `docs/feishu-openapi-doc/`. | No | Change only when intentionally changing the external OpenAPI reference source. |

## On-demand cat protocol

Before editing files under a directory with a local navigation card, read that card first:

```bash
cat core/AGENTS.md
cat api/AGENTS.md
cat utils/AGENTS.md
cat tests/AGENTS.md
cat docs/AGENTS.md
cat .github/AGENTS.md
```

Then read the target module and the smallest related tests, docs, or workflows. Examples:

- `api/sdk.py` or `api/sheet.py` contract changes: read `api/AGENTS.md`, internal exports in `api/__init__.py`, relevant `core/service.py` call sites, and focused API/service tests and docs.
- Config defaults or keys: read `core/AGENTS.md`, `core/runtime_config.py`, `xtf_cli/config.py`, `config.example.yaml`, related docs, and `tests/test_cli_config.py` plus `tests/test_runtime_core.py`.
- Workflow command changes: read `.github/AGENTS.md`, the target workflow, root command guidance here, and dependency files.

## Commands

Run commands from the repository root unless noted. The repository uses `pip` with requirements files; there is no `pyproject.toml`, Makefile, tox config, repo-local codegen command, or local build script.

| Command | Purpose | Scope | Sandbox notes |
| --- | --- | --- | --- |
| `python -m pip install -r requirements.txt` | Install runtime dependencies. | repo | Requires network and writes environment packages. |
| `python -m pip install -r requirements-dev.txt` | Install pytest/coverage/mock test dependencies. | repo | Requires network and writes environment packages. |
| `pytest tests/ -v` | Run the full pytest suite. | tests | No Feishu service required for unit tests. |
| `pytest tests/ -v -m "not integration" --tb=short --cov=core --cov=api --cov=utils --cov=xtf_cli --cov-report=term` | CI-like unit test and coverage run without XML output. | tests/core/api/utils/xtf_cli | No external Feishu service required; depends on installed test dependencies. |
| `ruff check . --ignore E501,F401` | Ruff check used by CI. | repo | CI pins Ruff `0.15.13`; local `ruff` must be installed. |
| `black --check .` | Formatting check only. | repo | Mirrors `.github/workflows/test.yml`; does not modify files. |
| `mypy core/ api/ utils/ xtf_cli/ --ignore-missing-imports` | Type check package modules. | core/api/utils/xtf_cli | Mirrors `.github/workflows/test.yml`; `mypy` must be installed. |
| `python -m py_compile XTF.py core/*.py api/*.py utils/*.py xtf_cli/*.py` | Syntax check mainline Python files. | mainline | Mirrors `.github/workflows/test.yml`. |
| `python XTF.py sync --config config.yaml --dry-run` | Build an exact read-only sync plan. | runtime | May use Feishu read APIs but must not call any mutation endpoint. |
| `python XTF.py sync --config config.yaml` | Execute the configured sync. | runtime | Requires valid credentials/network and may write remote data; `overwrite`/`clone` additionally require `--allow-delete`. Use only when explicitly intended. |

If the local machine lacks `python`, run the same module command with `python3` and report the substitution. Keep committed workflow/documentation command changes aligned with the actual CI commands.

## CI and build notes

- `.github/workflows/test.yml` pins Ruff `0.15.13`, runs Ruff, Black check, MyPy, and `py_compile` across `core/`, `api/`, `utils/`, and `xtf_cli/` on Python 3.11, then runs pytest with matching coverage on Ubuntu 22.04/Python 3.10-3.13, Ubuntu 24.04/Python 3.11-3.13, Windows/Python 3.10-3.12, and macOS ARM/Python 3.11-3.13.
- The target XTF 2.0 workflow builds only `XTF.py` for Linux x64/ARM64, Windows x64, and macOS ARM64. The 1.9 four-platform rollback gate has been satisfied on `codex/xtf-cli-v2`; the transitional `build-xtf-sheet` and `build-xtf-bitable` legacy jobs have been removed from `multi-platform-build.yml`. `.github/workflows/build-1.9-rollback.yml` remains the only source of legacy `XTF-Sheet` / `XTF-Bitable` binaries, rebuilt from the pinned 1.9 source commit for rollback purposes only.
- The XTF 2.0 artifact ships `config.example.yaml`, `README.md`, `QUICKSTART.md`, and `checksums.txt`; it must not contain an auto-discoverable real `config.yaml`.
- Build jobs pin `PyInstaller==6.19.0` with `setuptools<82`; change that pair only with a binary startup smoke because setuptools 82 removes the `pkg_resources` API used by this PyInstaller runtime hook.
- `.github/workflows/build-1.9-rollback.yml` is manual/reusable only and checks out exact commit `a22ac8119d33625cbcadbfb18cc2a36538f69b7e`. Before that new workflow exists on the default branch, the authorized gate is dispatched through the existing `multi-platform-build.yml` at the cutover ref with input `build_1_9_rollback=true`; either dispatch is an external GitHub write requiring separate authorization.
- Complete PyInstaller builds, platform bundles, release bundles, and release uploads require GitHub Actions. Do not treat them as default local validation.

## Architecture boundaries

- `XTF.py` is a thin executable wrapper. Keep parsing, configuration resolution, command dispatch, rendering, and exit-code mapping in `xtf_cli/`.
- `core/` owns sync planning and execution. A dry-run may perform remote reads but must never execute field, record, range, style, or validation mutations.
- `core/` owns configuration semantics and sync behavior. It may call `api/` through explicit clients but must not embed raw Feishu HTTP request details.
- `api/` owns internal typed Feishu responses/errors, auth headers, HTTP retry behavior, pagination, batch operations, and Sheet range/style/validation calls. These constructors and `api.__all__` are repository-internal contracts, not a stable Python SDK.
- `utils/` must stay reusable and side-effect-light. It must not import remote sync clients or trigger network behavior.
- `tests/` validates behavior with pytest and mocks. Unit tests must not require real Feishu APIs, network, local `config.yaml`, runtime logs, or test execution order.
- `docs/` must describe behavior implemented in code or verified from Feishu OpenAPI reference material.
- The `lite/` directory has been removed. `.github/workflows/build-1.9-rollback.yml` rebuilds legacy `XTF-Sheet` / `XTF-Bitable` binaries from the pinned 1.9 source commit for rollback purposes only; it is not a product surface and must not receive mainline improvements.
- `.github/` is the source of CI command truth. If workflow commands change, update this root router.

## Global rules

- Configuration is flags-first. `app_secret` priority is CLI > `XTF_APP_SECRET` > YAML; every other value uses CLI > YAML > target-specific defaults. An unqualified command may discover `./config.yaml`, but an explicit missing `--config` path must fail.
- Treat the XTF 2.0 command/config/output contract as public: main-program examples must use `python XTF.py <subcommand>`, never the removed root-level flat invocation. `sync` owns sync flags; `config init` is the only command that creates a main-program configuration.
- Main-program user docs and templates use nested YAML v2 paths such as `sync.selective`; flattened resolver names are internal details and must not be presented as supported YAML. The removed `lite/` directory and the 1.9 rollback workflow outputs are rollback inputs, not a documented 2.0 surface.
- Describe `--dry-run` precisely: it performs zero Feishu mutations, but normal runtime initialization can create local logs. A local check or GitHub Actions build proves code/build contracts only, not a remote synchronization, Release, deployment, or business UAT.
- When a change affects CLI flags, v2 schema/defaults, plan/outcome serialization, or destructive behavior, keep `config.example.yaml`, `README.md`, `docs/CONFIG.md`, `docs/SYNC.md`, relevant architecture docs, parser/config tests, and packaging assumptions aligned as applicable.
- Supported targets are `bitable` and `sheet`; shared changes must consider both unless the code path is target-specific by construction.
- Sync modes are `full`, `incremental`, `overwrite`, and `clone`.
- Matching is explicit: `full`/`overwrite` require `by_key`; `incremental`
  accepts `by_key` or `append_only`; `clone` omits `match_strategy` and always
  uses replace-all. `by_key` requires a non-empty index, while `append_only`
  forbids index/selective configuration. Never infer clone from an empty target
  or missing Sheet index.
- DATETIME key granularity defaults to `exact` UTC milliseconds. `day` requires
  an explicit valid IANA timezone and must not use the host timezone implicitly.
- Treat `overwrite` and `clone` as destructive remote-data modes. Preserve clear logging, batching semantics, deletion scope, failure handling, and user-facing risk wording when touching them.
- `sync.selective.enabled` is incompatible with `clone`; keep the underlying `selective_sync` column validation, duplicate checks, and `max_gap_for_merge` bounds intact.
- Sheet formula protection is valid only for `full` with a non-empty `index_column`; it depends on dual reads using `Formula` and `FormattedValue`, and enabling it also enables result validation. A failed or incomplete formula/result read must stop the write, and protected formula columns must not be rewritten.
- Bitable field type behavior is strategy-based: `raw`, `base`, `auto`, `intelligence`. Preserve conservative defaults unless a config, docs, and test change explicitly covers the behavior shift.
- Transport owns request exceptions and HTTP 429/5xx retry. Bitable business retry handles retryable Feishu business codes returned in parsed responses; do not multiply the two retry budgets.
- Pagination must reject missing or repeated continuation tokens instead of returning an incomplete result. Multi-batch operations stop at the first failed batch and must expose the already-applied prefix; do not report partial remote mutation as full success.
- Bitable batch operations and Sheet chunked writes must keep API limits, idempotency where implemented, auto-splitting, retry behavior, and progress/failure visibility in logs.
- Excel `.xlsx`/`.xls` are the stable primary formats. CSV support is experimental and should not be documented or treated as production-equivalent.
- Keep `config.example.yaml`, docs, tests, and packaging assumptions synchronized when changing config keys, defaults, examples, or supported flags.
- Prefer focused tests near the changed module. Broaden validation when touching shared config, sync engine behavior, API wrappers, destructive modes, or CI commands.
- Use mocks for HTTP responses, retry sleeps, rate-limit timing, Feishu errors, and destructive remote operations.

## Do not

- Do not commit real `app_secret`, `app_token`, `spreadsheet_token`, `tenant_access_token`, local `config.yaml`, runtime logs, copied credentials, production identifiers, or private Feishu URLs.
- Do not hand-edit or commit `logs/`, `htmlcov/`, `.coverage`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `dist/`, `build/`, `artifacts/`, or other generated output.
- Do not edit `docs/feishu-openapi-doc/` as normal documentation; it is an external submodule reference.
- Do not weaken validation, lint, typecheck, test workflow settings, matrix coverage, or release artifact coverage without explaining the impact and updating the router/cards.
- Do not move raw Feishu API details into `core/` or sync orchestration into `api/`.
- Do not silently break CLI/YAML/JSON/exit-code contracts, typed error metadata, pagination completeness, or partial-batch failure reporting. Python imports and direct typed-client construction are internal and may change with aligned callers/tests.
- Do not create unit tests that require network, real Feishu state, real credentials, local logs, or test execution order.
- Do not run manual sync commands against `config.yaml` unless the user explicitly asks for a real local run and accepts remote write/delete risk.
- Do not publish releases, upload release assets, or alter GitHub secrets from a local session.

## Validation

Use the smallest meaningful validation first, then expand based on blast radius.

Default unit-test path:

```bash
pytest tests/ -v -m "not integration" --tb=short --cov=core --cov=api --cov=utils --cov=xtf_cli --cov-report=term
```

Default quality path:

```bash
ruff check . --ignore E501,F401
black --check .
mypy core/ api/ utils/ xtf_cli/ --ignore-missing-imports
python -m py_compile XTF.py core/*.py api/*.py utils/*.py xtf_cli/*.py
```

For docs-only edits, there is no dedicated docs build; validate referenced commands, config keys, examples, and OpenAPI claims against code, workflows, `config.example.yaml`, or `docs/feishu-openapi-doc/`.

For `.github/` edits, local commands can validate invoked checks, but complete matrix, artifact packaging, release bundle, and release upload behavior require GitHub Actions.

Manual sync commands are not default validation because they require valid Feishu credentials, network, and may mutate remote Bitable or Sheet data. If skipped, say so explicitly in the final report.

## Done criteria

A change is complete when:

- The relevant local navigation card was read before editing files under that subtree.
- Only intended files were changed, and unrelated dirty files remain untouched.
- Focused tests or checks were run for the changed behavior.
- Shared or cross-target changes considered both `bitable` and `sheet`.
- Destructive modes (`overwrite`, `clone`) include explicit risk assessment and validation notes if affected.
- Config, docs, tests, and packaging assumptions stay aligned when config keys/defaults/examples change.
- Any skipped validation is justified with a concrete reason.
