# utils navigation card

`utils/` contains reusable helpers, currently focused on Excel engine detection and read support.
Before editing it, read `utils/excel_reader.py`, `core/reader.py`, and `tests/test_reader.py`.
Keep reading this card when a change affects input file formats, optional reader engines, or fallback behavior.

## Key files

- `excel_reader.py`: Calamine/OpenPyXL helper functions and engine information.
- `core/reader.py`: primary DataFrame reading path used by the CLI.
- `tests/test_reader.py`: file-format and edge-case coverage.

## Local invariants

- Preserve Calamine-first, OpenPyXL-fallback behavior when optional dependencies are present.
- CSV remains experimental; do not present it as equivalent to Excel.
- Helpers should have no Feishu/network side effects.

## Do not

- Do not import `api/` or `core/engine.py` into utility helpers.
- Do not swallow file-not-found, unsupported-format, or encoding errors.

## Validation

Use root validation commands. For focused read-path changes, start with `pytest tests/test_reader.py -v`.
