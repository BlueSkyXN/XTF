# docs guardrail card

`docs/` covers architecture, config, sync modes, field strategies, Sheet behavior, and request control.
Read this card before editing documentation, examples, config references, Feishu API descriptions, risk warnings, or command snippets.
Key files: `README.md`, `ARCH.md`, `CONFIG.md`, `SYNC.md`, `FIELD_TYPES.md`, `SHEET.md`, `CONTROL.md`; `feishu-openapi-doc/` is an external submodule reference.

## Why this needs guardrails

- Docs describe behavior that can trigger remote writes or deletes in Feishu.
- CI packages `config.example.yaml` as `config.yaml`; examples can become distributed defaults.
- Feishu API assumptions drift; unsupported claims can mislead implementation.

## Required before changes

- Verify commands, CLI flags, config keys, defaults, sync modes, and error behavior against code, CI, or `config.example.yaml`.
- Verify Feishu API claims against `docs/feishu-openapi-doc/` or official OpenAPI material when relevant.
- For SDK/API docs, verify exports, compatibility, typed errors, pagination, retry ownership, and partial batches against `api/` and focused tests.
- Keep risk wording explicit for `overwrite`, `clone`, credentials, tokens, rate limits, formula protection, and CSV's experimental status.

## Do not

- Do not invent APIs, commands, limits, performance numbers, or config keys.
- Do not edit `docs/feishu-openapi-doc/` as normal docs; it is a submodule and should change only when explicitly requested.
- Do not paste real `config.yaml`, logs, tokens, app IDs, secrets, private URLs, or production identifiers.
- Do not let examples contradict `config.example.yaml`.
- Do not describe a partial batch, incomplete pagination read, local check, or GitHub Actions build as a verified remote synchronization result.

## Validation

There is no dedicated docs build. For docs-only changes, validate referenced commands, config names, examples, and API claims against source files, workflows, `config.example.yaml`, or OpenAPI reference material. Run root tests when docs change alongside behavior.
