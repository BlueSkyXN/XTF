# docs guardrail card

`docs/` is the user and developer reference for architecture, config, sync, fields, Sheet behavior, and control strategies.
Before editing it, read `docs/README.md` and the topic document being changed.
Keep reading this card when updating commands, config fields, Feishu API descriptions, risk warnings, or examples.

## Key files

- `ARCH.md`, `CONFIG.md`, `SYNC.md`, `FIELD_TYPES.md`, `SHEET.md`, `CONTROL.md`.
- `feishu-openapi-doc/`: git submodule with external Feishu OpenAPI reference.

## Local invariants

- Commands, defaults, CLI flags, config keys, and error codes must match code, CI, or cited OpenAPI docs.
- Example YAML must stay consistent with `config.example.yaml`.
- `overwrite`, `clone`, credentials, tokens, rate limits, and formula protection need explicit risk wording.

## Do not

- Do not invent APIs, commands, performance numbers, or Feishu limits.
- Do not edit `docs/feishu-openapi-doc/` as normal docs; update it only as a submodule when explicitly requested.
- Do not paste real `config.yaml`, logs, or tokens.

## Validation

Use root validation commands. Documentation-only changes have no dedicated build command in this repository.
