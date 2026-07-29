import logging
from unittest.mock import MagicMock, call

import pytest

import reademail
from reademail import (
    AdminLookup,
    UnifiedFile,
    auto_fill_nit_from_subject,
    extract_nit_and_name_from_dian_subject,
    is_administrativa_by_subject,
    load_admin_lookup,
    should_auto_fill_admin_nit,
    write_nit_to_admin_sheet,
)


def _sheets_service_with_values(*responses):
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": values} for values in responses
    ]
    return service


def test_load_admin_lookup_combina_administrativas_y_caja_menor(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = _sheets_service_with_values(
        [["NIT", "Nombre"], ["900.123.456-7", "ACME S.A.S."], ["", "Sin NIT Admin"]],
        [["NIT", "Nombre"], ["800765432", "Proveedor Caja"], ["", "Sin NIT Caja"]],
    )

    lookup = load_admin_lookup(service)

    assert lookup == AdminLookup(
        admin_nits={"9001234567", "800765432"},
        admin_names={"acme sas", "proveedor caja", "sin nit admin", "sin nit caja"},
        admin_rows_sin_nit={
            "sin nit admin": ("Administrativas", 3),
            "sin nit caja": ("CajaMenor", 3),
        },
    )
    assert service.spreadsheets.return_value.values.return_value.get.call_args_list == [
        call(spreadsheetId="sheet-123", range="Administrativas!A:B"),
        call(spreadsheetId="sheet-123", range="CajaMenor!A:B"),
    ]


def test_load_admin_lookup_tolera_una_hoja_inaccesible(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = MagicMock()
    execute = service.spreadsheets.return_value.values.return_value.get.return_value.execute
    execute.side_effect = [RuntimeError("sin hoja"), {"values": [["901234567", "Caja Uno"]]}]

    lookup = load_admin_lookup(service)

    assert lookup.admin_nits == {"901234567"}
    assert lookup.admin_names == {"caja uno"}
    assert lookup.admin_rows_sin_nit == {}


@pytest.mark.parametrize(
    ("subject", "admin_nits", "admin_names"),
    [
        ("8991234567;PROVEEDOR;FAC;01", {"123456"}, set()),
        ("901234567;PROVEEDOR;FAC;01;0123", {"567012"}, set()),
        ("Factura ACMEDICA SAS", set(), {"acme"}),
        ("Distribuidora Rioseco Ltda", set(), {"rios"}),
        ("ACMES ASOCIADOS", set(), {"acmesas"}),
    ],
)
def test_is_administrativa_by_subject_rechaza_falsos_positivos(
    subject, admin_nits, admin_names
):
    assert is_administrativa_by_subject(subject, admin_nits, admin_names) is False


@pytest.mark.parametrize(
    ("subject", "admin_nits", "admin_names"),
    [
        ("900123456;OTRO PROVEEDOR;FAC;01", {"900123456"}, set()),
        ("900123456;ACME SAS;FAC;01", set(), {"acme sas"}),
        ("Factura mensual - ACME S.A.S.", set(), {"acme s.a.s."}),
        ("Cobro ACME LTDA. abril", set(), {"acme ltda."}),
        ("Factura mensual - ACME S.A.S.", set(), {"acme sas"}),
        ("Factura mensual - ACME SAS", set(), {"acme s.a.s."}),
    ],
)
def test_is_administrativa_by_subject_acepta_coincidencia_exacta(
    subject, admin_nits, admin_names
):
    assert is_administrativa_by_subject(subject, admin_nits, admin_names) is True


def test_is_administrativa_by_subject_no_busca_en_cuerpo():
    nits = {"900123456"}
    names = {"acme sas"}

    assert is_administrativa_by_subject("Factura mensual", nits, names) is False


def test_process_message_clasifica_por_nombre_administrativo_en_asunto(monkeypatch):
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@example.com"},
            {"name": "Subject", "value": "Factura mensual - ACME S.A.S."},
        ]
    }
    labels = []
    state = {}
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda current, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"payload": payload, "snippet": "sin coincidencias"},
    )
    monkeypatch.setattr(reademail, "collect_attachments", lambda current_payload: [{}])
    monkeypatch.setattr(
        reademail,
        "build_unified_files",
        lambda service, message_id, attachments: (
            [UnifiedFile("factura.pdf", "application/pdf", b"", "test")],
            [],
            [],
        ),
    )
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: labels.append((label, archive)),
    )

    reademail.process_message(
        object(),
        object(),
        "message-1",
        [],
        admin_lookup=AdminLookup(set(), {"acme sas"}, {}),
    )

    assert labels == [(reademail.LABEL_ADMIN_NAME, reademail.ARCHIVE_ADMIN)]


def test_extract_nit_and_name_from_dian_subject():
    assert extract_nit_and_name_from_dian_subject(
        "900123456;ACME SAS;FAC;01;ACME SAS"
    ) == ("900123456", "ACME SAS")
    assert extract_nit_and_name_from_dian_subject("123456;EMPRESA;FAC") == ("123456", "EMPRESA")
    assert extract_nit_and_name_from_dian_subject("Factura normal") == (None, None)


def test_should_auto_fill_admin_nit_aplica_guardias():
    rows = {"acme sas": ("Administrativas", 2)}

    assert should_auto_fill_admin_nit("900123456", "acme sas", rows, True, False, False) is True
    assert should_auto_fill_admin_nit("900123456", "acme sas", rows, False, False, False) is False
    assert should_auto_fill_admin_nit("900123456", "acme sas", rows, True, True, False) is False
    assert should_auto_fill_admin_nit("900123456", "acme sas", rows, True, False, True) is False
    assert should_auto_fill_admin_nit("12345", "acme sas", rows, True, False, False) is False
    assert should_auto_fill_admin_nit("900123456", "nombre con nit", rows, True, False, False) is False


def test_write_nit_to_admin_sheet_actualiza_rango_y_valor(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = MagicMock()

    result = write_nit_to_admin_sheet(service, "CajaMenor", 7, "900123456")

    assert result is True
    service.spreadsheets.return_value.values.return_value.update.assert_called_once_with(
        spreadsheetId="sheet-123",
        range="CajaMenor!A7",
        valueInputOption="RAW",
        body={"values": [["900123456"]]},
    )


def test_write_nit_to_admin_sheet_devuelve_false_si_falla(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.update.side_effect = RuntimeError("sin permisos")

    assert write_nit_to_admin_sheet(service, "Administrativas", 4, "900123456") is False


def test_auto_fill_nit_respeta_candado_y_actualiza_lookup(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": []
    }
    lookup = AdminLookup(set(), {"acme sas"}, {"acme sas": ("Administrativas", 2)})

    result = auto_fill_nit_from_subject(
        service,
        "900123456;ACME SAS;FAC;01;ACME SAS",
        lookup,
        enabled=True,
        modo_pruebas=False,
        dry_run=False,
    )

    assert result is True
    assert lookup.admin_nits == {"900123456"}
    assert lookup.admin_rows_sin_nit == {}
    service.spreadsheets.return_value.values.return_value.get.assert_called_once_with(
        spreadsheetId="sheet-123",
        range="Administrativas!A2",
    )


def test_auto_fill_nit_no_sobrescribe_celda_con_valor(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {
        "values": [["800999888"]]
    }
    lookup = AdminLookup(set(), {"acme sas"}, {"acme sas": ("Administrativas", 2)})

    result = auto_fill_nit_from_subject(
        service,
        "900123456;ACME SAS;FAC;01;ACME SAS",
        lookup,
        enabled=True,
        modo_pruebas=False,
        dry_run=False,
    )

    assert result is False
    service.spreadsheets.return_value.values.return_value.update.assert_not_called()


def test_auto_fill_nit_en_pruebas_solo_loguea(caplog):
    caplog.set_level(logging.INFO)
    lookup = AdminLookup(set(), {"acme sas"}, {"acme sas": ("CajaMenor", 5)})

    result = auto_fill_nit_from_subject(
        object(),
        "900123456;ACME SAS;FAC;01;ACME SAS",
        lookup,
        enabled=True,
        modo_pruebas=True,
        dry_run=False,
    )

    assert result is False
    assert "🧪 habría escrito NIT 900123456 en CajaMenor!A5 (nombre=ACME SAS)" in caplog.text


def test_auto_fill_flag_apagado_no_llama_api_sheets():
    class SheetsServiceQueFalla:
        def spreadsheets(self):
            raise AssertionError("No debe llamar la API de Sheets")

    lookup = AdminLookup(set(), {"acme sas"}, {"acme sas": ("Administrativas", 2)})

    assert auto_fill_nit_from_subject(
        SheetsServiceQueFalla(),
        "900123456;ACME SAS;FAC;01;ACME SAS",
        lookup,
        enabled=False,
        modo_pruebas=False,
        dry_run=False,
    ) is False


def test_load_admin_lookup_no_habilita_escritura_en_fila_uno_sin_encabezado(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-123")
    service = _sheets_service_with_values(
        [["", "Rótulo especial"], ["", "ACME SAS"]],
        [],
    )

    lookup = load_admin_lookup(service)

    assert "rotulo especial" not in lookup.admin_rows_sin_nit
    assert lookup.admin_rows_sin_nit["acme sas"] == ("Administrativas", 2)
