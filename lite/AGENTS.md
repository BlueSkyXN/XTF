# lite navigation card

`lite/` contains legacy standalone scripts kept for compatibility and release artifacts.
Read this card before changing `XTF_Bitable.py`, `XTF_Sheet.py`, legacy CLI behavior, legacy config compatibility, or legacy artifact names.
Key files: `XTF_Bitable.py`, `XTF_Sheet.py`, `README.md`, and `.github/workflows/multi-platform-build.yml`.

## Local Invariants

- Preserve existing standalone execution behavior unless the user explicitly requests a breaking legacy change.
- CI still builds `XTF-Sheet` and `XTF-Bitable`; binary names, artifact paths, and release bundle names matter.
- Security, credential redaction, destructive-sync, and config-template fixes may need to be mirrored between mainline and both legacy scripts.
- Legacy scripts share user-facing concepts with `XTF.py`, but new mainline features should not be introduced here first by default.

## Local Rules

- Check the build workflow before renaming files, changing CLI entrypoints, or modifying runtime dependencies.
- Keep examples and docs consistent with legacy behavior, not only the mainline CLI.
- Prefer syntax and smoke-level validation for legacy-only edits unless behavior changes justify broader tests.

## Do Not

- Do not remove legacy scripts or change `XTF-Sheet` / `XTF-Bitable` artifact names without updating workflow and release notes.
- Do not add features only to legacy scripts when the same behavior belongs in mainline.
- Do not assume `core/` helpers are available inside standalone scripts unless the script already imports and packages them.

## Validation

- `python -m py_compile lite/*.py` for focused legacy syntax checks.
- Run root CI-like quality and pytest commands if a legacy fix is mirrored into mainline modules.
