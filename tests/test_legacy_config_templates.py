"""Legacy artifact templates remain compatible with their archived entrypoints."""

from pathlib import Path

import yaml

from lite.XTF_Bitable import SyncConfig as BitableSyncConfig
from lite.XTF_Sheet import SyncConfig as SheetSyncConfig

REPO_ROOT = Path(__file__).resolve().parents[1]


def _load(name: str):
    with (REPO_ROOT / "lite" / name).open("r", encoding="utf-8") as stream:
        return yaml.safe_load(stream)


def test_bitable_legacy_template_matches_bitable_config():
    config = BitableSyncConfig(**_load("config.bitable.example.yaml"))

    assert config.app_token == "your_app_token"
    assert config.table_id == "your_table_id"


def test_sheet_legacy_template_matches_sheet_config():
    config = SheetSyncConfig(**_load("config.sheet.example.yaml"))

    assert config.spreadsheet_token == "your_spreadsheet_token"
    assert config.sheet_id == "your_sheet_id"
