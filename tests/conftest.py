import pytest

import reademail


@pytest.fixture(autouse=True)
def disable_sheet_backups_by_default(monkeypatch):
    """Evita que las pruebas ajenas al respaldo escriban artefactos locales."""
    monkeypatch.setattr(reademail, "SHEET_BACKUP_ENABLED", False)


@pytest.fixture(autouse=True)
def modo_real_por_defecto(monkeypatch):
    """Aisla la suite del .env del desarrollador.

    MODO_PRUEBAS y COMPRAS_EMAIL se leen del .env al importar el modulo, asi
    que una maquina con MODO_PRUEBAS=true y COMPRAS_EMAIL definido desviaba a
    la ruta A5 pruebas que esperaban el rechazo A4. Cada test que necesite la
    ruta de pruebas fija estos valores explicitamente.
    """
    monkeypatch.setattr(reademail, "MODO_PRUEBAS", False)
    monkeypatch.setattr(reademail, "COMPRAS_EMAIL", "")
