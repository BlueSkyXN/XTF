# utils navigation card

`utils/` contains reusable helper code, currently focused on Excel engine detection and read support.
Read this card before changing optional reader engines, supported file formats, engine fallback, error handling, or helper import boundaries.
Key files: `utils/excel_reader.py`, `core/reader.py`, and `tests/test_reader.py`.

## Local Invariants

- Preserve Calamine-first, OpenPyXL-fallback behavior when optional dependencies are available.
- Excel `.xlsx` and `.xls` are primary supported formats; CSV remains experimental and must not be presented as equivalent to Excel.
- Helpers must have no Feishu, network, logging-to-remote, or sync side effects.
- File-not-found, unsupported-format, empty-file, encoding, and engine errors should remain diagnosable to callers.

## Local Rules

- Keep reusable file-reading helpers independent from sync orchestration.
- If helper behavior changes, check whether `core/reader.py`, `README.md`, `docs/CONFIG.md`, or `docs/SYNC.md` need aligned wording.
- Prefer deterministic tests with temp files and fixtures.

## Do Not

- Do not import `api/`, `core/engine.py`, or remote sync clients into utility helpers.
- Do not silently fall back in ways that hide unsupported formats or corrupted inputs.

## Validation

- `pytest tests/test_reader.py -v` for focused read-path changes.
- Run root quality checks if imports, public helper signatures, or dependency assumptions change.
