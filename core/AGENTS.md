# core navigation card

`core/` owns config, conversion, sync orchestration, reader dispatch, and request-control strategies.
Read before changing config/inference, sync modes, selective or destructive sync, partial failures, retries/rate limits, or Sheet formula protection.
Key files: `config.py`, `engine.py`, `converter.py`, `reader.py`, `control.py`; matching tests use the same module names.

## Local invariants

- Preserve config priority and target-specific required fields. `selective_sync.enabled` is invalid with `clone`; keep column/duplicate checks and `max_gap_for_merge <= 50`.
- `overwrite` and `clone` delete remote data. Keep scope, batching, logs, and failure paths explicit.
- Multi-batch mutations stop on first failure. Never create/write after a prerequisite update/delete/clear fails or report an applied prefix as full success.
- Formula protection is limited to `full` with `index_column`; failed/incomplete `Formula` or `FormattedValue` reads stop writes. Never rewrite a protected formula column or formula-backed index.
- Field strategies remain `raw`, `base`, `auto`, `intelligence`; shared engine changes must cover both targets.

## Local rules

- Keep raw HTTP behavior in `api/`; `engine.py` consumes `XTFFeishuClient` and target-client contracts.
- Config key/default changes usually require `config.example.yaml`, docs, tests, and root updates.

## Do not

- Do not bypass validation, range/chunk limits, retry/rate control, or destructive-mode diagnostics.
- Do not fall back from an incomplete remote read to a destructive or whole-sheet write.
- Do not add Bitable-only behavior to shared paths without guarding Sheet behavior.

## Validation

- Use `pytest tests/test_config.py -v`, `test_converter.py`, `test_reader.py`, or `test_control.py` for the matching module.
- `pytest tests/test_engine.py -v` for sync ordering, partial failures, selective sync, destructive modes, SDK assembly, or formula protection.
- Run root CI-like checks for shared engine, defaults, destructive modes, or cross-target changes.
