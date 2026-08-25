"""
Paridad entre reademail.py y app/services/email_responses.py.

Los textos de respuesta viven duplicados: el monolito es el que corre en
produccion y el modulo es su version extraida, todavia sin conectar.
Mientras la duplicacion exista, estas pruebas la vigilan: si alguien edita
un lado y no el otro, fallan aqui en vez de descubrirse el dia que se
conecte el modulo y los textos retrocedan silenciosamente.

Si estas pruebas fallan, la correccion NO es relajarlas: es aplicar el
mismo cambio en los dos archivos.
"""

import pytest

import reademail
from app.services import email_responses

CASOS_RECHAZO = [
    pytest.param(
        "RAD-20260825-000001",
        "FACTURA ELECTRONICA",
        [reademail.MISSING_ORDER_MESSAGE, reademail.MISSING_OK_COMPRAS_MESSAGE],
        "Proveedor Ejemplo SAS",
        id="fe-faltan-orden-y-ok",
    ),
    pytest.param(
        "RAD-20260825-000002",
        "CUENTA DE COBRO",
        ["Cuenta de cobro incompleta. Faltan: cédula, RUT, certificado bancario."],
        "Proveedor Ejemplo SAS",
        id="cuenta-de-cobro-incompleta",
    ),
    pytest.param(
        "RAD-20260825-000003",
        "FACTURA ELECTRONICA",
        [],
        None,
        id="sin-motivos-y-sin-cliente",
    ),
]

CASOS_APROBACION = [
    pytest.param("RAD-20260825-000004", "FACTURA ELECTRONICA", "Proveedor Ejemplo SAS", 3, 1, id="fe"),
    pytest.param("RAD-20260825-000005", "CUENTA DE COBRO", "No identificado", 5, 0, id="cuenta-de-cobro"),
]


@pytest.mark.parametrize(("radicado", "invoice_type", "reasons", "client_name"), CASOS_RECHAZO)
def test_rechazo_identico_en_monolito_y_modulo(radicado, invoice_type, reasons, client_name):
    esperado = reademail.build_rejected_email(radicado, invoice_type, reasons, client_name)
    obtenido = email_responses.build_rejected_email(radicado, invoice_type, reasons, client_name)

    assert obtenido[0] == esperado[0], "El asunto del rechazo se desincronizo"
    assert obtenido[1] == esperado[1], "El cuerpo del rechazo se desincronizo"


@pytest.mark.parametrize(
    ("radicado", "invoice_type", "client_name", "pdf_count", "xml_count"),
    CASOS_APROBACION,
)
def test_aprobacion_identica_en_monolito_y_modulo(
    radicado,
    invoice_type,
    client_name,
    pdf_count,
    xml_count,
):
    esperado = reademail.build_approved_email(radicado, invoice_type, client_name, pdf_count, xml_count)
    obtenido = email_responses.build_approved_email(radicado, invoice_type, client_name, pdf_count, xml_count)

    assert obtenido[0] == esperado[0], "El asunto de la aprobacion se desincronizo"
    assert obtenido[1] == esperado[1], "El cuerpo de la aprobacion se desincronizo"


@pytest.mark.parametrize(
    "nombre_constante",
    ["MISSING_ORDER_MESSAGE", "MISSING_OK_COMPRAS_MESSAGE", "SELECTABLE_TEXT_NOTICE"],
)
def test_constantes_de_texto_identicas(nombre_constante):
    assert getattr(email_responses, nombre_constante) == getattr(reademail, nombre_constante)
