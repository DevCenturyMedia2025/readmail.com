"""
Tests del matching de clientes (app/services/client_matching.py).

Todo con catalogos de prueba en memoria (listas de ClientRecord
inventadas), sin llamadas a Google Sheets. Se congela el comportamiento
actual: umbrales de similitud, stopwords y heuristicas de ordenes.
"""

from app.models import ClientMatchResult, ClientRecord
from app.services.client_matching import (
    CLIENT_MATCH_STOPWORDS,
    client_lookup_catalog,
    client_name_tokens,
    client_records_from_values,
    client_similarity,
    extract_client_field_values,
    extract_order_client_raw,
    find_contact_email_by_nit,
    find_client_by_name_in_text,
    find_client_by_nit,
    find_client_by_nit_in_text,
    find_client_in_text,
    first_client_field_value,
    identify_client,
    identify_client_from_fields,
    match_client_raw_to_catalog,
    normalize_client_match_value,
)
from app.utils.text import normalize_alnum, normalize_nit


def record(name, nit=None, active=True, raw_row=None, contact_email=None):
    return ClientRecord(
        name=name,
        normalized_name=normalize_alnum(name),
        nit=nit,
        normalized_nit=normalize_nit(nit) if nit else None,
        contact_email=contact_email,
        active=active,
        raw_row=raw_row or {},
    )


CATALOGO = [
    record("ACME Corp", nit="900.123.456-7"),
    record("TGI Transportadora de Gas", nit="900134459"),
    record("Compañía Eléctrica del Norte", nit="800.555.111"),
    record("Cliente Inactivo SAS", nit="999888777", active=False),
]


# ------------------------------------------------------------
# Tokens y similitud
# ------------------------------------------------------------
def test_tokens_filtran_stopwords_y_cortos():
    tokens = client_name_tokens("TGI Transportadora de Gas SAS Ltda")
    assert tokens == {"tgi", "transportadora", "gas"}


def test_tokens_solo_stopwords_queda_vacio():
    assert client_name_tokens("de la sas ltda y el") == set()


def test_similitud_igualdad_exacta():
    rec = record("ACME Corp")
    assert client_similarity("acme corp", rec) == 1000 + len("acmecorp")


def test_similitud_substring():
    rec = record("ACME Corp")
    score = client_similarity("Factura para ACME Corp Colombia", rec)
    assert score == 700 + len("acmecorp")


def test_similitud_por_tokens_comunes():
    rec = record("TGI Transportadora de Gas")
    score = client_similarity("Transportadora Gas Internacional", rec)
    assert score > 0


def test_similitud_sin_tokens_comunes_es_cero():
    rec = record("ACME Corp")
    assert client_similarity("Compañía Eléctrica del Norte", rec) == 0


def test_stopwords_no_generan_falsos_positivos():
    rec = record("Comercializadora de la Costa SAS")
    assert client_similarity("Servicios de la Ltda", rec) == 0


def test_normalize_client_match_value():
    assert normalize_client_match_value("Compañía Eléctrica S.A.S.") == "companiaelectricasas"


# ------------------------------------------------------------
# Matching contra el catalogo por nombre
# ------------------------------------------------------------
def test_match_raw_igualdad_exacta():
    rec = match_client_raw_to_catalog("ACME CORP", CATALOGO)
    assert rec is not None and rec.name == "ACME Corp"


def test_match_raw_nombre_dentro_del_valor():
    rec = match_client_raw_to_catalog("Cliente ACME Corp sucursal Bogotá", CATALOGO)
    assert rec is not None and rec.name == "ACME Corp"


def test_match_raw_valor_dentro_del_nombre():
    rec = match_client_raw_to_catalog("Transportadora de Gas", CATALOGO)
    assert rec is not None and rec.name == "TGI Transportadora de Gas"


def test_match_raw_inactivo_no_matchea():
    assert match_client_raw_to_catalog("Cliente Inactivo SAS", CATALOGO) is None


def test_match_raw_vacio():
    assert match_client_raw_to_catalog("", CATALOGO) is None


def test_find_client_in_text_con_tildes():
    rec = find_client_in_text("factura de la COMPAÑÍA ELÉCTRICA DEL NORTE por energía", CATALOGO)
    assert rec is not None and rec.name == "Compañía Eléctrica del Norte"


def test_find_client_in_text_sin_cliente():
    assert find_client_in_text("texto sin ninguna coincidencia posible", CATALOGO) is None


def test_find_client_in_text_gana_el_nombre_mas_largo():
    catalogo = [record("ACME"), record("ACME Corp Colombia")]
    rec = find_client_in_text("pago a acme corp colombia", catalogo)
    assert rec is not None and rec.name == "ACME Corp Colombia"


def test_find_client_by_name_in_text_texto_vacio():
    assert find_client_by_name_in_text("", CATALOGO) is None


# ------------------------------------------------------------
# Matching por NIT
# ------------------------------------------------------------
def test_nit_exacto_con_puntos_y_guion():
    rec = find_client_by_nit("900.123.456-7", CATALOGO)
    assert rec is not None and rec.name == "ACME Corp"


def test_nit_inactivo_no_matchea():
    assert find_client_by_nit("999888777", CATALOGO) is None


def test_nit_vacio():
    assert find_client_by_nit("", CATALOGO) is None


def test_find_contact_email_by_nit_existente():
    catalogo = [record("ACME Corp", nit="900.123.456-7", contact_email="contacto@acme.test")]
    assert find_contact_email_by_nit("9001234567", catalogo) == "contacto@acme.test"


def test_find_contact_email_by_nit_inexistente():
    catalogo = [record("ACME Corp", nit="900123456")]
    assert find_contact_email_by_nit("111222333", catalogo) is None


def test_find_contact_email_by_nit_inactivo():
    catalogo = [record("ACME Corp", nit="900123456", active=False, contact_email="contacto@acme.test")]
    assert find_contact_email_by_nit("900123456", catalogo) is None


def test_nit_dentro_de_texto():
    resultado = find_client_by_nit_in_text("Factura NIT: 900.134.459 valor $1.000", CATALOGO)
    assert resultado is not None
    nit, rec = resultado
    assert nit == "900134459"
    assert rec.name == "TGI Transportadora de Gas"


def test_nit_en_texto_gana_el_mas_largo():
    catalogo = [record("Corto", nit="123456"), record("Largo", nit="12345678")]
    resultado = find_client_by_nit_in_text("documento 12345678", catalogo)
    assert resultado is not None and resultado[1].name == "Largo"


def test_nit_en_texto_sin_digitos():
    assert find_client_by_nit_in_text("sin numeros aqui", CATALOGO) is None


# ------------------------------------------------------------
# Filtro de catalogo por rango
# ------------------------------------------------------------
def test_lookup_filtra_por_rango_clientes():
    catalogo = [
        record("Solo Proveedor", raw_row={"__range": "Proveedores!A2:D"}),
        record("Solo Cliente", raw_row={"__range": "Clientes!A2:D"}),
    ]
    resultado = client_lookup_catalog(catalogo)
    assert [r.name for r in resultado] == ["Solo Cliente"]


def test_lookup_sin_rango_clientes_devuelve_todo():
    catalogo = [record("Uno"), record("Dos")]
    assert client_lookup_catalog(catalogo) == catalogo


def test_client_records_from_values_pobla_email_contacto():
    catalogo = client_records_from_values(
        [
            ["cliente", "nit", "estado", "email contacto"],
            ["ACME Corp", "900.123.456-7", "activo", "contacto@acme.test"],
        ],
        sheet_range="Clientes!A:D",
    )

    assert len(catalogo) == 1
    assert catalogo[0].contact_email == "contacto@acme.test"
    assert find_contact_email_by_nit("9001234567", catalogo) == "contacto@acme.test"


def test_client_records_from_values_email_malformado_es_none():
    catalogo = client_records_from_values(
        [
            ["cliente", "nit", "estado", "email contacto"],
            ["ACME Corp", "900.123.456-7", "activo", "no-es-email"],
        ],
        sheet_range="Clientes!A:D",
    )

    assert catalogo[0].contact_email is None
    assert find_contact_email_by_nit("9001234567", catalogo) is None


# ------------------------------------------------------------
# Extraccion del cliente en ordenes de compra
# ------------------------------------------------------------
def test_orden_cliente_en_la_misma_linea():
    texto = "Orden de compra 123\nCliente: ACME Corp Sucursal\nProducto: Pauta"
    assert extract_order_client_raw(texto) == "ACME Corp Sucursal"


def test_orden_cliente_corta_en_campo_siguiente():
    texto = "CLIENTE: ACME Corp NIT: 900123456"
    assert extract_order_client_raw(texto) == "ACME Corp"


def test_orden_cliente_en_linea_siguiente():
    texto = "CLIENTE:\nACME Corp Sucursal\nPRODUCTO: Pauta"
    assert extract_order_client_raw(texto) == "ACME Corp Sucursal"


def test_orden_tabla_no_cliente_producto_nit():
    texto = "No: CLIENTE: PRODUCTO: NIT:\n33071\nTGI TRANSPORTADORA DE GAS\nCONTRATO 551008471 2026\n900134459"
    assert extract_order_client_raw(texto) == "TGI TRANSPORTADORA DE GAS"


def test_orden_valor_solo_numerico_no_es_util():
    texto = "Cliente: 12345678"
    assert extract_order_client_raw(texto) is None


def test_orden_sin_cliente():
    assert extract_order_client_raw("Factura sin bloque de cliente") is None


def test_orden_texto_vacio():
    assert extract_order_client_raw("") is None


# ------------------------------------------------------------
# identify_client / campos de cliente
# ------------------------------------------------------------
def test_identify_client_por_substring():
    rec = identify_client(["pago recibido de acme corp"], CATALOGO)
    assert rec is not None and rec.name == "ACME Corp"


def test_identify_client_por_tokens():
    rec = identify_client(["Transportadora Gas Internacional"], CATALOGO)
    assert rec is not None and rec.name == "TGI Transportadora de Gas"


def test_identify_client_sin_match():
    assert identify_client(["texto irrelevante"], CATALOGO) is None


def test_identify_client_catalogo_vacio():
    assert identify_client(["acme corp"], []) is None


def test_extract_client_field_values_linea_y_tabla():
    texto = "No: CLIENTE: PRODUCTO: NIT:\n33071\nTGI TRANSPORTADORA DE GAS\n900134459\nCliente: ACME Corp Sucursal"
    values = extract_client_field_values(texto)
    assert "TGI TRANSPORTADORA DE GAS" in values
    assert "ACME Corp Sucursal" in values


def test_extract_client_field_values_vacio():
    assert extract_client_field_values("") == []


def test_identify_client_from_fields():
    texto = "Orden 55\nCliente: TGI Transportadora de Gas\nValor: 1000"
    rec = identify_client_from_fields([texto], CATALOGO)
    assert rec is not None and rec.name == "TGI Transportadora de Gas"


def test_identify_client_from_fields_sin_campos():
    assert identify_client_from_fields(["sin bloque de campos"], CATALOGO) is None


def test_first_client_field_value():
    textos = ["sin campos aqui", "Cliente: ACME Corp Sucursal"]
    assert first_client_field_value(textos) == "ACME Corp Sucursal"


def test_first_client_field_value_sin_valores():
    assert first_client_field_value(["nada", "tampoco"]) is None


# ------------------------------------------------------------
# Modelos
# ------------------------------------------------------------
def test_client_record_defaults():
    rec = ClientRecord(name="X", normalized_name="x")
    assert rec.nit is None
    assert rec.normalized_nit is None
    assert rec.contact_email is None
    assert rec.active is True
    assert rec.raw_row == {}


def test_client_match_result_defaults():
    res = ClientMatchResult()
    assert res.record is None
    assert res.raw is None
    assert res.source == ""


def test_stopwords_congeladas():
    assert CLIENT_MATCH_STOPWORDS == {
        "de", "del", "la", "las", "los", "el", "y", "sa", "sas", "s", "a",
        "esp", "e", "p", "cia", "ltda", "inc",
    }
