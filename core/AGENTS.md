# core navigation card

`core/` owns XTF configuration semantics and sync behavior.
Read before modifying config parsing, sync modes, conversion, reader dispatch, retry/rate-limit, selective sync, or Sheet formula protection.
Key files: `config.py`, `engine.py`, `converter.py`, `reader.py`, `control.py`; focused tests: `test_config.py`, `test_converter.py`, `test_reader.py`, `test_control.py`.

## Local Invariants

- Config priority: CLI args > YAML config > target inference > defaults.
- Keep target-specific required-field checks for Bitable and Sheet.
- `selective_sync.enabled` is invalid with `SyncMode.CLONE`; preserve column and `max_gap_for_merge` checks.
- `overwrite` and `clone` delete remote data; keep scope, batching, logs, and failures explicit.
- Sheet formula protection relies on `Formula` and `FormattedValue` dual reads.
- Field strategy behavior stays progressive: `raw`, `base`, `auto`, `intelligence`.
- Shared engine changes must cover both `TargetType.BITABLE` and `TargetType.SHEET`.

## Local Rules

- Keep Feishu HTTP details in `api/`; `core/engine.py` should use API client methods.
- Keep low-level file helpers in `utils/` unless the code is sync orchestration.
- Config key/default changes may require `config.example.yaml`, docs, tests, and root rule updates; batch/retry/rate-limit tests should mock time and requests.

## Do Not

- Do not bypass required-field validation, duplicate checks, range limits, chunk splitting, retry, or rate-limit controls.
- Do not downgrade destructive-mode logging or error handling.
- Do not add Bitable-only behavior to shared paths without guarding Sheet behavior.

## Validation

- `pytest tests/test_config.py -v` for config, target inference, CLI/YAML merge, and validation.
- `pytest tests/test_converter.py -v` for field type, conversion, index, and range helpers.
- `pytest tests/test_reader.py -v` for reader changes.
- `pytest tests/test_control.py -v` for retry/rate-limit changes.
- Run root CI-like pytest and quality checks for shared `engine.py` or config-default changes.
