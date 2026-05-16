# api navigation card

`api/` owns Feishu authentication and HTTP wrappers for Bitable and Sheet.
Read this card before modifying token handling, auth headers, pagination, retryable errors, rate limits, batch writes/deletes, Sheet ranges, styles, validations, or chunked upload.
Key files: `auth.py`, `base.py`, `bitable.py`, `sheet.py`; focused coverage currently starts in `tests/test_api_base.py`.

## Local Invariants

- Logs must redact secrets and tokens; never log full `Authorization`, `app_secret`, `app_token`, `spreadsheet_token`, or tenant tokens.
- `FeishuAuth` owns tenant access token retrieval and refresh. Keep auth concerns out of `core/`.
- `RetryableAPIClient` must preserve predictable behavior for HTTP 429, 5xx, request exceptions, and retry counts.
- Bitable pagination must honor `page_token` and guard repeated tokens or loops.
- Bitable batch create should keep idempotency via `client_token`.
- Sheet read/write/append/clear/style/validation methods validate ranges before remote calls.
- Chunked Sheet upload and append must preserve auto-splitting behavior for oversized responses or writes.

## Local Rules

- Keep endpoint construction and Feishu response parsing inside `api/`.
- Use mocked `requests` responses in unit tests. Real Feishu integration belongs outside the default unit test path and must be explicitly marked.
- If a Feishu OpenAPI behavior is uncertain, check `docs/feishu-openapi-doc/` or official docs before encoding assumptions.

## Do Not

- Do not test unit behavior against real Feishu APIs.
- Do not hide delete, clear, or overwrite calls behind no-log helper paths.
- Do not swallow Feishu business error codes that callers need for retry, fallback, or user-visible diagnostics.

## Validation

- `pytest tests/test_api_base.py -v` for base HTTP client, retry, and rate limiter changes.
- Run root CI-like pytest and quality checks when changing auth, Bitable, Sheet, pagination, delete, or chunk behavior because callers in `core/engine.py` may be affected.
