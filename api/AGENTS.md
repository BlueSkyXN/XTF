# api navigation card

`api/` owns Feishu authentication and HTTP wrappers for Bitable and Sheet.
Before editing it, read the target API module and `docs/CONTROL.md`.
Keep reading this card when a change touches token handling, pagination, retry, rate limits, batch write/delete, or Sheet ranges.

## Key files

- `auth.py`: `tenant_access_token` acquisition, cache, refresh.
- `base.py`: `RateLimiter` and `RetryableAPIClient`.
- `bitable.py`: fields, record search pagination, batch create/update/delete.
- `sheet.py`: spreadsheet metadata, range read/write/append/clear/style.
- `tests/test_api_base.py`: covered retry/client behavior.

## Local invariants

- Logs must redact secrets and tokens.
- Bitable pagination must honor `page_token` and guard against repeated tokens.
- Batch create should keep idempotency via `client_token`.
- Sheet operations must validate ranges before read/write/clear/style calls.
- Retry behavior for 429, 5xx, request exceptions, and known Feishu business codes must stay predictable.

## Do not

- Do not test against real Feishu APIs in unit tests.
- Do not log full `Authorization`, `app_secret`, `app_token`, or `spreadsheet_token`.
- Do not hide delete operations behind no-log helper paths.

## Validation

Use root validation commands. For focused base-client changes, start with `pytest tests/test_api_base.py -v`.
