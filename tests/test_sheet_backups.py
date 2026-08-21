import json
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import reademail


def _sheets_service(*responses, titles):
    service = MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": title}} for title in titles]
    }
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": values} for values in responses
    ]
    return service


def _enable_backups(monkeypatch, backup_dir, keep_days=30):
    monkeypatch.setattr(reademail, "SHEET_BACKUP_ENABLED", True)
    monkeypatch.setattr(reademail, "SHEET_BACKUP_DIR", str(backup_dir))
    monkeypatch.setattr(reademail, "SHEET_BACKUP_KEEP_DAYS", keep_days)


def test_primer_admin_lookup_respalda_crudos_y_segundo_no_duplica(
    monkeypatch,
    tmp_path,
    caplog,
):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-prueba")
    _enable_backups(monkeypatch, tmp_path / "backups")
    administrativas = [["Nit", "Proveedor"], [900123456.0, "ACME S.A.S."]]
    caja_menor = [["Nit", "Nombre"], [None, "Caja Uno"]]
    service = _sheets_service(
        administrativas,
        caja_menor,
        administrativas,
        caja_menor,
        titles=reademail.ADMIN_SHEET_TABS,
    )
    writes = []
    original_writer = reademail._write_sheet_backup_file

    def record_write(target, values):
        writes.append(target)
        original_writer(target, values)

    monkeypatch.setattr(reademail, "_write_sheet_backup_file", record_write)

    first_lookup = reademail.load_admin_lookup(service)
    second_lookup = reademail.load_admin_lookup(service)

    day_dir = tmp_path / "backups" / datetime.now().date().isoformat()
    assert first_lookup == second_lookup
    assert json.loads((day_dir / "Administrativas.json").read_text(encoding="utf-8")) == administrativas
    assert json.loads((day_dir / "CajaMenor.json").read_text(encoding="utf-8")) == caja_menor
    assert writes == [
        day_dir / "Administrativas.json",
        day_dir / "CajaMenor.json",
    ]
    assert caplog.text.count("💾 Respaldo de hojas guardado en") == 1


def test_registered_entities_respalda_clientes_y_terceros(monkeypatch, tmp_path):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-prueba")
    _enable_backups(monkeypatch, tmp_path / "backups")
    clientes = [["NIT", "Nombre"], ["900123456", "Cliente Uno"]]
    terceros = [["NIT", "Nombre"], ["901234567", "Tercero Uno"]]
    service = _sheets_service(
        clientes,
        terceros,
        titles=reademail.KNOWN_ENTITY_TABS,
    )

    lookup = reademail.load_registered_entities(service)

    day_dir = tmp_path / "backups" / datetime.now().date().isoformat()
    assert "900123456" in lookup.registered_nits
    assert "901234567" in lookup.registered_nits
    assert json.loads((day_dir / "Clientes.json").read_text(encoding="utf-8")) == clientes
    assert json.loads((day_dir / "Terceros.json").read_text(encoding="utf-8")) == terceros


def test_fallo_de_escritura_no_impide_cargar_la_clasificacion(
    monkeypatch,
    tmp_path,
    caplog,
):
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-prueba")
    _enable_backups(monkeypatch, tmp_path / "backups")
    service = _sheets_service(
        [["Nit", "Proveedor"], ["900123456", "ACME"]],
        [],
        titles=reademail.ADMIN_SHEET_TABS,
    )
    monkeypatch.setattr(
        reademail,
        "_write_sheet_backup_file",
        lambda target, values: (_ for _ in ()).throw(OSError("disco lleno")),
    )

    lookup = reademail.load_admin_lookup(service)

    assert lookup.admin_nits == {"900123456"}
    assert lookup.admin_names == {"acme"}
    assert "No pude guardar el respaldo de la hoja Administrativas: disco lleno" in caplog.text


def test_limpieza_borra_directorio_viejo_y_conserva_reciente(monkeypatch, tmp_path):
    backup_root = tmp_path / "backups"
    _enable_backups(monkeypatch, backup_root, keep_days=30)
    today = datetime.now().date()
    old_dir = backup_root / (today - timedelta(days=31)).isoformat()
    recent_dir = backup_root / (today - timedelta(days=30)).isoformat()
    unrelated_dir = backup_root / "manual"
    old_dir.mkdir(parents=True)
    recent_dir.mkdir()
    unrelated_dir.mkdir()

    reademail._backup_sheet_tabs({"Clientes": [["NIT"], ["900123456"]]})

    assert not old_dir.exists()
    assert recent_dir.is_dir()
    assert unrelated_dir.is_dir()
    assert (backup_root / today.isoformat() / "Clientes.json").is_file()
