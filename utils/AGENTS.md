# utils navigation card

`utils/` contains reusable helper code, currently focused on Excel engine detection and smart Excel read support.
Read this card before changing optional reader engines, supported formats, engine fallback, error handling, helper import boundaries, or helper side effects.
Key files: `utils/excel_reader.py`, `core/reader.py`, and `tests/test_reader.py`.

## Local invariants

- Preserve Calamine-first, OpenPyXL-fallback behavior; `requirements.txt` currently installs both engines, while helper imports still need diagnosable failure behavior.
- Excel `.xlsx` and `.xls` are stable primary formats; CSV remains experimental and must not be presented as production-equivalent.
- Helpers must have no Feishu, network, remote logging, or sync side effects.
- `DataFileReader` owns format dispatch and UTF-8-to-GBK CSV fallback; `utils/excel_reader.py` owns Excel engine selection.
- File-not-found, unsupported-format, empty-file, encoding, sheet-selection, and engine errors should remain diagnosable to callers.

## Local rules

- Keep reusable file-reading helpers independent from sync orchestration.
- If helper behavior changes, check whether `core/reader.py`, `README.md`, `docs/CONFIG.md`, or `docs/SYNC.md` need aligned wording.
- Prefer deterministic tests with temp files and fixtures.

## Do not

- Do not import `api/`, `core/engine.py`, or remote sync clients into utility helpers.
- Do not silently fall back in ways that hide unsupported formats, corrupted inputs, or missing optional engines.

## Validation

- `pytest tests/test_reader.py -v` for focused read-path changes.
- Run root quality checks if imports, public helper signatures, or dependency assumptions change.
