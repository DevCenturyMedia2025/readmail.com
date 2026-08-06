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
            ["\ufeffNombre", "NIT"],
            ["Cliente Ejemplo S.A.S.\u200b", "900.123.456-7"],
        ],
        [
            ["NIT", "Razón Social"],
            [860009999.0, "Tercero Ágil LTDA.\u200b"],
        ],
        titles=[clientes_title, terceros_title],
    )

    registered_nits, registered_names = load_registered_entities(service)

    assert registered_nits == {"900123456", "9001234567", "860009999"}
    assert registered_names == {"cliente ejemplo sas", "tercero agil ltda"}
    assert service.spreadsheets.return_value.values.return_value.get.call_args_list == [
        call(
            spreadsheetId="sheet-ficticio",
            range=f"'{clientes_title}'!A:B",
            valueRenderOption="UNFORMATTED_VALUE",
        ),
        call(
            spreadsheetId="sheet-ficticio",
            range=f"'{terceros_title}'!A:B",
            valueRenderOption="UNFORMATTED_VALUE",
        ),
    ]
    assert f"📄 {clientes_title}: 1 NIT, 1 nombres" in caplog.text
    assert f"📄 {terceros_title}: 1 NIT, 1 nombres" in caplog.text


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
