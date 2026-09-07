# tests navigation card

`tests/` contains the pytest suite for v2 config/runtime, sync ordering, conversion, file reading, typed API contracts, and retry/rate control.
Read this card before adding or changing tests, fixtures, mocks, markers, coverage expectations, or CI test assumptions.
Key files: `tests/README.md`, `tests/conftest.py`, and the matching `test_<module>.py`.

## Local invariants

- Test files use `test_*.py`; test names should describe the behavior and condition.
- Reuse fixtures in `conftest.py` before adding new fixture shapes.
- Unit tests must mock HTTP calls, Feishu responses, retry sleeps, time-sensitive rate limiting, and destructive remote operations.
- Tests requiring real services must be explicitly marked `integration`; the default CI command excludes integration tests.
- Fake credentials such as `test_app_secret` are fine. Real credentials are not.

## Local rules

- Use `test_api_sdk.py` for shared typed contracts, target API files for wrappers, and `test_service.py` for orchestration/order.
- For mutation failures, assert both the applied prefix and that later update/create/delete/clear/write calls did not run.
- For pagination, assert missing/repeated-token failure rather than accepting truncated results; for retries, assert transport and business budgets do not multiply.
- For destructive-mode bugs, assert exact deletion/clear scope or no-delete behavior where practical.
- When tests drive docs or config examples, keep example values fake and safe.

## Do not

- Do not depend on local `config.yaml`, network, test order, local logs, `htmlcov/`, or remote Feishu state.
- Do not add slow real sleeps; mock or reduce timing delays.
- Do not relax CI expectations without updating `.github/AGENTS.md` and explaining impact.

## Validation

- For test-only edits, run the touched test file first.
- Then run `pytest tests/ -v -m "not integration" --tb=short` for broader regression coverage.
- Use the root coverage command when changing coverage-relevant shared behavior.
