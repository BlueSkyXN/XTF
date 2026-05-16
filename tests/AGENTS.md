# tests navigation card

`tests/` contains the pytest suite for config, conversion, reading, retry/rate control, and base HTTP client behavior.
Read this card before adding or changing tests, fixtures, mocks, markers, coverage expectations, or CI test assumptions.
Key files: `tests/README.md`, `tests/conftest.py`, `test_config.py`, `test_converter.py`, `test_reader.py`, `test_control.py`, and `test_api_base.py`.

## Local Invariants

- Test files use `test_*.py`; test names should describe the behavior and condition.
- Reuse existing fixtures in `conftest.py` before adding new fixture shapes.
- Unit tests must mock HTTP calls, Feishu responses, retry sleeps, time-sensitive rate limiting, and destructive remote operations.
- Tests requiring real services must be explicitly marked `integration`; the default CI command excludes integration tests.
- Fake credentials such as `test_app_secret` are fine; real credentials are not.

## Local Rules

- Keep tests close to the module they protect: config in `test_config.py`, conversion in `test_converter.py`, reader in `test_reader.py`, control in `test_control.py`, base HTTP in `test_api_base.py`.
- When a bug touches destructive modes, include assertions for deletion scope or no-delete behavior where practical.
- When changing docs or config examples based on tests, keep example values fake and safe.

## Do Not

- Do not depend on local `config.yaml`, network, test order, local logs, `htmlcov/`, or remote Feishu state.
- Do not add slow real sleeps; mock or reduce timing delays.
- Do not relax CI expectations without updating `.github/AGENTS.md` and explaining impact.

## Validation

- For test-only edits, run the touched test file first.
- Then run `pytest tests/ -v -m "not integration" --tb=short` for broader regression coverage.
- Use the root coverage command when changing coverage-relevant shared behavior.
