"""
Tests de comportamiento del guardia DRY_RUN.

DRY_RUN es la proteccion critica antes de conectar modulos nuevos a
reademail.py. Estos tests congelan su comportamiento actual.
"""

import dataclasses

import pytest

import app.config.settings as settings_module


@pytest.fixture(autouse=True)
def no_dotenv(monkeypatch):
    """Evita que load_dotenv lea un .env real durante los tests."""
    monkeypatch.setattr(settings_module, "load_dotenv", lambda: None)


def test_dry_run_true_por_defecto_sin_variables(monkeypatch):
    monkeypatch.delenv("DRY_RUN", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)

    settings = settings_module.load_settings()

    assert settings.app_env == "development"
    assert settings.dry_run is True


def test_dry_run_false_solo_con_valor_explicito(monkeypatch):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("DRY_RUN", "false")

    settings = settings_module.load_settings()

    assert settings.dry_run is False


@pytest.mark.parametrize("valor", ["banana", "verdadero", "0.5", "  ", "FALSE_"])
def test_valores_raros_caen_en_seguro_fuera_de_produccion(monkeypatch, valor):
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.setenv("DRY_RUN", valor)

    settings = settings_module.load_settings()

    # env_bool: valor no reconocido -> default; fuera de produccion default=True
    assert settings.dry_run is True


@pytest.mark.parametrize("valor", ["true", "TRUE", "1", "yes", "y", "si", "sí"])
def test_valores_afirmativos_activan_dry_run(monkeypatch, valor):
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DRY_RUN", valor)

    settings = settings_module.load_settings()

    assert settings.dry_run is True


def test_en_produccion_valor_raro_cae_en_default_false(monkeypatch):
    """
    Documenta comportamiento ACTUAL: en produccion, un valor no reconocido
    de DRY_RUN cae en el default False. Si algun dia se decide que valores
    raros deben ser seguros (True) tambien en produccion, este test debe
    actualizarse junto con env_bool.
    """
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("DRY_RUN", "banana")

    settings = settings_module.load_settings()

    assert settings.dry_run is False


def test_settings_es_inmutable(monkeypatch):
    monkeypatch.delenv("DRY_RUN", raising=False)
    monkeypatch.delenv("APP_ENV", raising=False)

    settings = settings_module.load_settings()

    with pytest.raises(dataclasses.FrozenInstanceError):
        settings.dry_run = False
