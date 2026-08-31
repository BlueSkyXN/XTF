# docs guardrail card

`docs/` covers architecture, config, sync modes, field strategies, Sheet behavior, and request control.
Read this card before editing documentation, examples, config references, Feishu API descriptions, risk warnings, or command snippets.
Key files: `README.md`, `ARCH.md`, `CONFIG.md`, `SYNC.md`, `FIELD_TYPES.md`, `SHEET.md`, `CONTROL.md`; `feishu-openapi-doc/` is an external submodule reference.

## Why this needs guardrails

- Docs describe behavior that can trigger remote writes or deletes in Feishu.
- The XTF 2.0 package ships `config.example.yaml`; a real `config.yaml` is created only by `XTF config init`.
- Feishu API assumptions drift; unsupported claims can mislead implementation.

## Required before changes

- Verify commands, CLI flags, config keys, defaults, sync modes, and error behavior against code, CI, or `config.example.yaml`.
- Verify Feishu API claims against `docs/feishu-openapi-doc/` or official OpenAPI material when relevant.
- For internal API docs, verify exports, typed errors, pagination, retry ownership, and partial batches against `api/` and focused tests; do not present them as a stable Python SDK.
- Keep risk wording explicit for `overwrite`, `clone`, credentials, tokens, rate limits, formula protection, and CSV's experimental status.
- Document the main program only as `python3 XTF.py <subcommand>` (or packaged `XTF <subcommand>`). The removed root-level flat invocation is not an example or compatibility path; flag fragments must identify the subcommand that owns them.
- Use nested v2 YAML names in user documentation, for example `sync.selective` and `target.sheet.protect_formulas`. Internal flattened resolver names are not user-facing YAML keys. The temporarily retained `lite/` files are rollback material, not an XTF 2.0 product surface.
- State dry-run precisely: it may make read-only Feishu API calls and initialize local logs, but it must make zero Feishu mutations. Local tests and CI builds validate code/build contracts only; they never establish real synchronization, Release, deployment, or business UAT.
- When a public CLI/config/safety contract changes, update the smallest relevant combination of `README.md`, `docs/README.md`, `CONFIG.md`, `SYNC.md`, `ARCH.md`, `config.example.yaml`, and focused tests rather than leaving one route stale.

## Do not

- Do not invent APIs, commands, limits, performance numbers, or config keys.
- Do not edit `docs/feishu-openapi-doc/` as normal docs; it is a submodule and should change only when explicitly requested.
- Do not paste real `config.yaml`, logs, tokens, app IDs, secrets, private URLs, or production identifiers.
- Do not let examples contradict `config.example.yaml`.
- Do not describe a partial batch, incomplete pagination read, local check, or GitHub Actions build as a verified remote synchronization result.

## Validation

There is no dedicated docs build. For docs-only changes, validate referenced commands, config names, examples, and API claims against source files, workflows, `config.example.yaml`, or OpenAPI reference material. Run root tests when docs change alongside behavior.
