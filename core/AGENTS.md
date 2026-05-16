# core navigation card

`core/` is the configuration and sync behavior domain for XTF.
Before editing it, read the target module plus the matching docs under `docs/`.
Keep reading this card when a change touches sync modes, config validation, conversion, chunking, retry, or Sheet formula protection.

## Key files

- `config.py`: `SyncConfig`, CLI/YAML merge, target inference, validation.
- `engine.py`: `XTFSyncEngine`, Bitable/Sheet dispatch, `full`/`incremental`/`overwrite`/`clone`.
- `converter.py`: field type detection, conversion, index building.
- `reader.py`: Excel/CSV read path used by the CLI.
- `control.py`: retry and rate-limit strategies.

## Local invariants

- Keep config priority as CLI > YAML > inference > defaults.
- `selective_sync` remains incompatible with `clone`.
- `overwrite` and `clone` are destructive remote-data modes; preserve clear logging, batching, and failure semantics.
- Sheet formula protection depends on Formula and FormattedValue dual reads; do not overwrite protected formula columns.
- Field strategies stay progressive: `raw`, `base`, `auto`, `intelligence`.

## Do not

- Do not bypass required-field validation, duplicate-column checks, range limits, retry, rate limiting, or chunk splitting.
- Do not move Feishu HTTP details into `core/`; that belongs in `api/`.

## Validation

Use root validation commands. For focused changes, start with the matching `tests/test_config.py`, `tests/test_converter.py`, `tests/test_reader.py`, or `tests/test_control.py`.
