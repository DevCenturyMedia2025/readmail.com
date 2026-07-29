from unittest.mock import MagicMock, call

import reademail
from reademail import (
    AdminLookup,
    UnifiedFile,
    is_administrativa_by_subject,
    load_admin_lookup,
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
        [["NIT", "Nombre"], ["900.123.456-7", "ACME SAS"]],
        [["NIT", "Nombre"], ["800765432", "Proveedor Caja"]],
    )

    lookup = load_admin_lookup(service)

    assert lookup == AdminLookup(
        admin_nits={"9001234567", "800765432"},
        admin_names={"acmesas", "proveedorcaja"},
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
    assert lookup.admin_names == {"cajauno"}


def test_is_administrativa_by_subject_coincide_por_nit_o_nombre():
    nits = {"900123456"}
    names = {"acmesas"}

    assert is_administrativa_by_subject("900123456;OTRO PROVEEDOR;FAC;01", nits, names) is True
    assert is_administrativa_by_subject("Factura mensual - ACME S.A.S.", nits, names) is True


def test_is_administrativa_by_subject_no_busca_en_cuerpo():
    nits = {"900123456"}
    names = {"acmesas"}

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
        admin_lookup=AdminLookup(set(), {"acmesas"}),
    )

    assert labels == [(reademail.LABEL_ADMIN_NAME, reademail.ARCHIVE_ADMIN)]
