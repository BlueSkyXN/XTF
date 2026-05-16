# tests navigation card

`tests/` contains the pytest suite for config, conversion, reading, retry/rate control, and base HTTP client behavior.
Before editing it, read `tests/README.md` and `tests/conftest.py`.
Keep reading this card when adding tests, fixtures, mocks, markers, or coverage expectations.

## Key files

- `conftest.py`: shared configs, DataFrames, temp files.
- `test_config.py`, `test_converter.py`, `test_reader.py`, `test_control.py`, `test_api_base.py`: module-specific coverage.

## Local invariants

- Test files use `test_*.py`; test names should describe behavior and condition.
- Reuse existing fixtures before adding new ones.
- Mock HTTP calls, time sleeps, retries, and Feishu responses.
- Tests requiring real services must be marked `integration`; default CI excludes them.

## Do not

- Do not depend on real `config.yaml`, network, test order, local logs, or remote Feishu state.
- Do not use real credentials; keep fake values such as `test_app_secret`.

## Validation

Use root validation commands. For test-only changes, run the touched test file first, then `pytest tests/ -v -m "not integration" --tb=short`.
