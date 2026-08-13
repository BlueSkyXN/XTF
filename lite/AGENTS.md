# lite navigation card

`lite/` contains legacy standalone scripts kept for compatibility and release artifacts.
Read this card before changing `XTF_Bitable.py`, `XTF_Sheet.py`, legacy CLI behavior, legacy config compatibility, or legacy artifact names.
Key files: `XTF_Bitable.py`, `XTF_Sheet.py`, `README.md`, and `.github/workflows/multi-platform-build.yml`.

## Local invariants

- Preserve standalone execution behavior unless the user explicitly requests a breaking legacy change.
- CI still builds `XTF-Sheet` and `XTF-Bitable`; binary names, artifact paths, and release bundle names matter.
- Security, credential redaction, destructive-sync, and config-template fixes require an explicit parity check across mainline and both legacy scripts; mirror only where the legacy contract is affected.
- Legacy scripts share user-facing concepts with `XTF.py`, but new mainline features should not be introduced here first by default.
- Despite their standalone distribution role, both scripts currently import `utils/excel_reader.py`; keep the PyInstaller hidden-import/package path aligned with that dependency.

## Local rules

- Check the build workflow before renaming files, changing CLI entrypoints, or modifying runtime dependencies.
- Keep examples and docs consistent with legacy behavior, not only the mainline CLI.
- Prefer syntax and smoke-level validation for legacy-only edits unless behavior changes justify broader tests.

## Do not

- Do not remove legacy scripts or change `XTF-Sheet` / `XTF-Bitable` artifact names without updating workflows and release notes.
- Do not add features only to legacy scripts when the same behavior belongs in mainline.
- Do not add `core/` or new package dependencies to standalone scripts without confirming PyInstaller packaging and compatibility intent.

## Validation

- `python -m py_compile lite/*.py` for focused legacy syntax checks.
- Run root CI-like quality and pytest commands if a legacy fix is mirrored into mainline modules.
