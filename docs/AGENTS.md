# docs guardrail card

`docs/` is the user and developer reference for architecture, config, sync modes, field types, Sheet behavior, and retry/rate-limit control.
Read this card before editing documentation, examples, config references, Feishu API descriptions, risk warnings, or command snippets.
Key files: `README.md`, `ARCH.md`, `CONFIG.md`, `SYNC.md`, `FIELD_TYPES.md`, `SHEET.md`, `CONTROL.md`; `feishu-openapi-doc/` is an external submodule reference.

## Why This Needs Guardrails

- Docs describe behavior that can trigger remote writes or deletes in Feishu.
- Config examples can become packaged defaults through CI release artifacts.
- Feishu API assumptions can drift; unsupported claims can mislead implementation work.

## Required Before Changes

- Verify commands, CLI flags, config keys, defaults, sync modes, and error behavior against code, CI, or `config.example.yaml`.
- Verify Feishu API claims against `docs/feishu-openapi-doc/` or official OpenAPI material when relevant.
- For `overwrite`, `clone`, credentials, tokens, rate limits, and formula protection, keep risk wording explicit.

## Do Not

- Do not invent APIs, commands, limits, performance numbers, or config keys.
- Do not edit `docs/feishu-openapi-doc/` as normal docs; it is a submodule and should change only when explicitly requested.
- Do not paste real `config.yaml`, logs, tokens, app IDs, secrets, or production identifiers.
- Do not let examples contradict `config.example.yaml`.

## Validation

There is no dedicated docs build. For docs-only changes, validate referenced commands and config names against source files or workflows. Run root tests when a documentation edit accompanies behavior changes.
