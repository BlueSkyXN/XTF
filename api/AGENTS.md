# api navigation card

`api/` owns public Feishu SDK contracts, auth/transport, and Bitable/Sheet wrappers.
Read before changing exports, constructors, tokens, retries, pagination/batching, mutations, or Sheet ranges/styles/chunks.
Key files: `__init__.py`, `sdk.py`, `auth.py`, `base.py`, `bitable.py`, `sheet.py`; tests: `tests/test_api_*.py`.

## Local invariants

- Never log full `Authorization`, `app_secret`, `app_token`, `spreadsheet_token`, or tenant tokens.
- `api.__all__`, `XTFFeishuClient`, typed errors, and direct target-client construction are compatibility surfaces.
- `FeishuAuth` owns tenant access token retrieval, caching, and refresh; clients created by one `XTFFeishuClient` share its auth and transport.
- Transport owns request exceptions and HTTP 429/5xx retry; Bitable owns retryable business codes. Do not multiply budgets.
- Typed errors preserve HTTP status, business code/message, `log_id`, retryability, and pacing metadata.
- Pagination must honor `page_token` and reject missing/repeated cursors; batch helpers stop at first failure and report the already-applied prefix.
- Bitable create uses one unique `client_token` per logical batch and reuses it across that batch's retries.
- Sheet methods that accept A1 ranges validate them before calls; oversized reads/writes preserve splitting.

## Local rules

- Keep endpoints/parsing in `api/`; reuse shared parser, paginator, and batch contracts.
- Unit tests use mocked `requests` responses. Real Feishu integration is outside the default unit path and must be explicitly marked.

## Do not

- Do not test unit behavior against real Feishu APIs.
- Do not hide mutations, flatten typed errors, return truncated pages, or continue batches after failure.
- Do not change boolean/tuple return contracts used by `core/engine.py` without updating callers and compatibility tests.

## Validation

- Run matching `pytest tests/test_api_<module>.py -v`: `sdk` for contracts, `base` for transport, and target files for wrappers.
- Run `pytest tests/test_engine.py -v` plus root CI-like checks when a contract used by sync orchestration changes.
