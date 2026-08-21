import pytest

import reademail


@pytest.fixture(autouse=True)
def disable_sheet_backups_by_default(monkeypatch):
    """Evita que las pruebas ajenas al respaldo escriban artefactos locales."""
    monkeypatch.setattr(reademail, "SHEET_BACKUP_ENABLED", False)
