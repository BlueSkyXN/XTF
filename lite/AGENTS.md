# lite navigation card

`lite/` contains legacy standalone scripts kept for compatibility and release artifacts.
Before editing it, read `lite/README.md` and `.github/workflows/multi-platform-build.yml`.
Keep reading this card when changing `XTF_Bitable.py`, `XTF_Sheet.py`, legacy CLI behavior, or legacy artifact names.

## Key files

- `XTF_Bitable.py`: standalone legacy Bitable sync script.
- `XTF_Sheet.py`: standalone legacy Sheet sync script.
- `README.md`: legacy usage notes.

## Local invariants

- Preserve existing CLI flags, config fields, and standalone execution behavior unless the PR explicitly breaks compatibility.
- CI still builds `XTF-Sheet` and `XTF-Bitable`; artifact names matter.
- Security or destructive-sync fixes may need to be mirrored between mainline and both legacy scripts.

## Do not

- Do not add new mainline features here first.
- Do not remove legacy scripts or change binary names without updating workflow and release notes.

## Validation

Use root validation commands. For focused legacy syntax checks, run `python -m py_compile lite/*.py`.
