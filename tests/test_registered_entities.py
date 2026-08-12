import logging
from unittest.mock import MagicMock, call

import pytest

import reademail
from reademail import is_registered_entity_by_subject, load_registered_entities


def _sheets_service_with_values(*responses, titles):
    service = MagicMock()
    service.spreadsheets.return_value.get.return_value.execute.return_value = {
        "sheets": [{"properties": {"title": title}} for title in titles]
    }
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.side_effect = [
        {"values": values} for values in responses
    ]
    return service


def test_load_registered_entities_combina_clientes_y_terceros_con_parser_robusto(
    monkeypatch,
    caplog,
):
    caplog.set_level(logging.INFO)
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-ficticio")
    clientes_title = "\ufeffClientes "
    terceros_title = "Tercéros\u200b"
    service = _sheets_service_with_values(
        [
            ["\ufeff ID ", " NÍT ", "Nombre\u200b", " CORREO ", " ESTADO "],
            ["CLI-001", "900.123.456-7", "Cliente Ejemplo S.A.S.\u200b", "cliente@example.test", "ACTIVO"],
            ["CLI-002", "901.000.000-1", "Cliente Inactivo", "inactivo@example.test", "INACTIVO"],
        ],
        [
            [" Estado ", "Correo electrónico", "NOMBRE", "ID", "Nit"],
            ["", "tercero@example.test", "Tercero Ágil LTDA.\u200b", "TER-001", 860009999.0],
        ],
        titles=[clientes_title, terceros_title],
    )

    lookup = load_registered_entities(service)

    assert lookup.registered_nits == {"900123456", "9001234567", "860009999"}
    assert lookup.registered_names == {"cliente ejemplo sas", "tercero agil ltda"}
    assert lookup.registered_docs == {
        "9001234567": {"carpeta": "", "rut": "", "camara": "", "bancaria": ""},
        "860009999": {"carpeta": "", "rut": "", "camara": "", "bancaria": ""},
    }
    assert service.spreadsheets.return_value.values.return_value.get.call_args_list == [
        call(
            spreadsheetId="sheet-ficticio",
            range=f"'{clientes_title}'!A:M",
            valueRenderOption="UNFORMATTED_VALUE",
        ),
        call(
            spreadsheetId="sheet-ficticio",
            range=f"'{terceros_title}'!A:M",
            valueRenderOption="UNFORMATTED_VALUE",
        ),
    ]
    assert f"📄 {clientes_title}: 1 NIT, 1 nombres, 0 con papelería completa" in caplog.text
    assert f"📄 {terceros_title}: 1 NIT, 1 nombres, 0 con papelería completa" in caplog.text


def test_load_registered_entities_usa_posiciones_reales_sin_encabezado(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-ficticio")
    service = _sheets_service_with_values(
        [
            ["9001234567", "901.555.222-4", "Cliente Sin Encabezado", "cliente@example.test", "activo"],
            ["9001234567", "800.000.001-1", "No Usa ID Como Nit", "otro@example.test", "INACTIVO"],
        ],
        [],
        titles=["Clientes", "Terceros"],
    )

    lookup = load_registered_entities(service)

    assert lookup.registered_nits == {"901555222", "9015552224"}
    assert lookup.registered_names == {"cliente sin encabezado"}


def test_load_registered_entities_reconoce_encabezado_nit_cliente(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-ficticio")
    service = _sheets_service_with_values(
        [
            ["ID", "NIT Cliente", "Nombre", "Correo", "Estado"],
            ["CLI-001", "900.456.789-1", "Cliente Con Alias", "alias@example.test", "ACTIVO"],
        ],
        [],
        titles=["Clientes", "Terceros"],
    )

    lookup = load_registered_entities(service)

    assert lookup.registered_nits == {"900456789", "9004567891"}
    assert lookup.registered_names == {"cliente con alias"}


def test_load_registered_entities_aplica_fallback_por_campo(monkeypatch):
    monkeypatch.setattr(reademail, "SHEET_ID", "sheet-ficticio")
    service = _sheets_service_with_values(
        [
            ["ID", "Documento fiscal", "Nombre", "Correo", "Estado"],
            ["CLI-001", "901.234.567-8", "Cliente Con Fallback", "fallback@example.test", "ACTIVO"],
        ],
        [],
        titles=["Clientes", "Terceros"],
    )

    lookup = load_registered_entities(service)

    assert lookup.registered_nits == {"901234567", "9012345678"}
    assert lookup.registered_names == {"cliente con fallback"}


def test_registered_docs_detecta_encabezados_y_cuenta_papeleria_completa():
    registered_nits = set()
    registered_names = set()
    registered_docs = {}
    values = [
        [
            "ID",
            "NIT",
            "Nombre",
            "Correo",
            "Estado",
            "ID Cámara de Comercio",
            "ID Certificación bancaria",
            "ID carpeta Drive",
            "ID RUT",
        ],
        [
            "CLI-001",
            "900.123.456-7",
            "Entidad Completa",
            "completa@example.test",
            "ACTIVO",
            "camara-1",
            "bancaria-1",
            "carpeta-1",
            "rut-1",
        ],
        [
            "CLI-002",
            "901.234.567-8",
            "Entidad Incompleta",
            "incompleta@example.test",
            "ACTIVO",
            "camara-2",
            "",
            "carpeta-2",
            "rut-2",
        ],
    ]

    counts = reademail._registered_lookup_from_values(
        values,
        registered_nits,
        registered_names,
        registered_docs,
    )

    assert counts == (2, 2, 1)
    assert registered_docs == {
        "9001234567": {
            "carpeta": "carpeta-1",
            "rut": "rut-1",
            "camara": "camara-1",
            "bancaria": "bancaria-1",
        },
        "9012345678": {
            "carpeta": "carpeta-2",
            "rut": "rut-2",
            "camara": "camara-2",
            "bancaria": "",
        },
    }


def test_registered_docs_sin_encabezado_usa_fallback_posicional():
    registered_docs = {}
    values = [
        [
            "CLI-001",
            "900.123.456-7",
            "Entidad Sin Encabezado",
            "entidad@example.test",
            "ACTIVO",
            "",
            "",
            "",
            "",
            "carpeta-posicional",
            "rut-posicional",
            "camara-posicional",
            "bancaria-posicional",
        ]
    ]

    counts = reademail._registered_lookup_from_values(values, set(), set(), registered_docs)

    assert counts == (1, 1, 1)
    assert registered_docs["9001234567"] == {
        "carpeta": "carpeta-posicional",
        "rut": "rut-posicional",
        "camara": "camara-posicional",
        "bancaria": "bancaria-posicional",
    }


@pytest.mark.parametrize(
    ("subject", "registered_nits", "registered_names"),
    [
        ("860009999;PROVEEDOR FICTICIO;FAC;01", {"860009999"}, set()),
        ("Factura mensual - TERCERO ÁGIL LTDA.", set(), {"tercero agil ltda"}),
        ("Factura mensual - ACME S.A.S.", set(), {"acme sas"}),
    ],
)
def test_is_registered_entity_by_subject_detecta_nit_o_nombre_de_terceros(
    subject,
    registered_nits,
    registered_names,
):
    assert is_registered_entity_by_subject(
        subject,
        registered_nits,
        registered_names,
    ) is True


@pytest.mark.parametrize(
    ("subject", "registered_nits", "registered_names"),
    [
        ("Factura de entidad nueva", {"860009999"}, {"tercero agil ltda"}),
        ("18600099990;OTRA ENTIDAD;FAC;01", {"860009999"}, set()),
        ("Factura Tercero Agilidad SAS", set(), {"tercero agil"}),
    ],
)
def test_is_registered_entity_by_subject_rechaza_entidad_no_registrada(
    subject,
    registered_nits,
    registered_names,
):
    assert is_registered_entity_by_subject(
        subject,
        registered_nits,
        registered_names,
    ) is False


def test_known_entity_tabs_son_clientes_y_terceros():
    assert reademail.KNOWN_ENTITY_TABS == ("Clientes", "Terceros")
