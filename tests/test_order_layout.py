"""
Lectura de los campos de una orden de compra por coordenadas.

Los PDF se generan aqui mismo con datos ficticios: ninguna orden real entra
al repositorio. El layout replica el de las ordenes que llegan por correo,
con las etiquetas a la izquierda y sus valores a la derecha en la misma fila.
"""

import pytest

import reademail
from reademail import ClientRecord, UnifiedFile

pymupdf = pytest.importorskip("pymupdf")


def _pdf_con_filas(filas, paginas=None):
    """Construye un PDF colocando texto en coordenadas exactas.

    filas: lista de (x, y, texto). paginas: lista de listas de filas.
    """
    doc = pymupdf.open()
    for contenido in paginas if paginas is not None else [filas]:
        page = doc.new_page()
        for x, y, texto in contenido:
            page.insert_text((x, y), texto, fontsize=10)
    datos = doc.tobytes()
    doc.close()
    return datos


ORDEN_TIPICA = [
    (70, 60, "PROVEEDORA DE SERVICIOS SAS"),
    (70, 75, "NIT:"),
    (140, 75, "800111222-3"),
    (70, 140, "CLIENTE:"),
    (140, 140, "DISTRIBUIDORA EJEMPLO SAS"),
    (70, 158, "NIT:"),
    (140, 158, "900555444-1"),
    (70, 176, "No:"),
    (140, 176, "OC-4501"),
    (70, 194, "PRODUCTO:"),
    (140, 194, "Papeleria institucional"),
    (70, 212, "FECHA:"),
    (140, 212, "2026-08-25"),
]


def _cliente(nombre, clave, nit):
    """normalized_nit es un campo plano del dataclass: hay que calcularlo."""
    return ClientRecord(
        nombre,
        clave,
        nit=nit,
        normalized_nit=reademail.normalize_nit(nit),
    )


def _catalogo():
    return [
        _cliente("DISTRIBUIDORA EJEMPLO SAS", "distribuidoraejemplosas", "900555444-1"),
        _cliente("OTRO CLIENTE SAS", "otroclientesas", "901000000-2"),
    ]


def _orden(nombre, data, texto=""):
    return UnifiedFile(nombre, "application/pdf", data, "test", texto)


# --------------------------------------------------------------------------
# extract_order_fields_by_layout
# --------------------------------------------------------------------------
def test_lee_el_nombre_del_cliente_no_una_etiqueta_ni_el_nit():
    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(ORDEN_TIPICA))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"
    assert "NIT" not in campos["cliente"]
    assert "CLIENTE" not in campos["cliente"]
    assert campos["cliente"] != campos.get("nit")


def test_lee_los_demas_campos_de_la_fila():
    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(ORDEN_TIPICA))

    assert campos["no"] == "OC-4501"
    assert campos["producto"] == "Papeleria institucional"
    assert campos["fecha"] == "2026-08-25"


def test_con_dos_nit_toma_el_de_la_columna_del_cliente():
    """El NIT del emisor esta arriba; el del cliente comparte columna con CLIENTE."""
    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(ORDEN_TIPICA))

    assert campos["nit"] == "900555444-1", "tomo el NIT del emisor en vez del cliente"
    assert campos["nit"] != "800111222-3"


def test_con_dos_nit_en_columnas_distintas_gana_el_mas_cercano_a_cliente():
    filas = [
        (400, 60, "NIT:"),
        (450, 60, "800111222-3"),
        (70, 140, "CLIENTE:"),
        (140, 140, "DISTRIBUIDORA EJEMPLO SAS"),
        (70, 158, "NIT:"),
        (140, 158, "900555444-1"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["nit"] == "900555444-1"


def test_pdf_sin_capa_de_texto_devuelve_vacio():
    doc = pymupdf.open()
    doc.new_page()
    vacio = doc.tobytes()
    doc.close()

    assert reademail.extract_order_fields_by_layout(vacio) == {}


@pytest.mark.parametrize(
    "datos",
    [b"no soy un pdf", b"", b"%PDF-1.4 truncado", None],
    ids=["basura", "vacio", "truncado", "none"],
)
def test_entradas_invalidas_devuelven_vacio_sin_lanzar(datos):
    assert reademail.extract_order_fields_by_layout(datos) == {}


def test_etiqueta_sin_valor_a_la_derecha_se_omite():
    filas = [
        (70, 140, "CLIENTE:"),
        (70, 158, "NIT:"),
        (140, 158, "900555444-1"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert "cliente" not in campos
    assert campos["nit"] == "900555444-1"


def test_ignora_texto_a_la_izquierda_de_la_etiqueta():
    # La columna de la izquierda va bien separada: si el texto queda pegado a la
    # etiqueta, pymupdf los une en una sola palabra y deja de ser este caso.
    filas = [
        (20, 140, "IZQ"),
        (70, 140, "CLIENTE:"),
        (140, 140, "DISTRIBUIDORA EJEMPLO SAS"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"
    assert "IZQUIERDA" not in campos["cliente"]


def test_ignora_filas_desalineadas():
    filas = [
        (70, 140, "CLIENTE:"),
        (140, 140, "DISTRIBUIDORA EJEMPLO SAS"),
        (140, 175, "OTRA FILA LEJANA"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"
    assert "LEJANA" not in campos["cliente"]


def test_orden_de_varias_paginas_toma_la_primera_aparicion():
    paginas = [
        [(70, 140, "CLIENTE:"), (140, 140, "DISTRIBUIDORA EJEMPLO SAS")],
        [(70, 140, "CLIENTE:"), (140, 140, "OTRO CLIENTE SAS")],
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(None, paginas=paginas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"


def test_campo_de_una_pagina_posterior_tambien_se_lee():
    paginas = [
        [(70, 140, "CLIENTE:"), (140, 140, "DISTRIBUIDORA EJEMPLO SAS")],
        [(70, 140, "No:"), (140, 140, "OC-9999")],
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(None, paginas=paginas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"
    assert campos["no"] == "OC-9999"


def test_layout_de_dos_columnas_no_traga_la_etiqueta_vecina():
    """CLIENTE a la izquierda y FECHA a la derecha, en la misma fila."""
    filas = [
        (70, 140, "CLIENTE:"),
        (140, 140, "DISTRIBUIDORA EJEMPLO SAS"),
        (350, 140, "FECHA:"),
        (420, 140, "2026-08-25"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"
    assert "FECHA" not in campos["cliente"]
    assert "2026-08-25" not in campos["cliente"]
    assert campos["fecha"] == "2026-08-25"


def test_nombre_que_contiene_la_palabra_no_no_se_trunca():
    """'No' es una etiqueta buscada, pero sin dos puntos es parte del nombre."""
    filas = [(70, 140, "CLIENTE:"), (140, 140, "EMPRESA NO REGISTRADA SAS")]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "EMPRESA NO REGISTRADA SAS"


def test_dos_nit_sin_cliente_no_devuelve_ninguno():
    """Sin CLIENTE no se puede saber cuál NIT es el del cliente: mejor ninguno."""
    filas = [
        (70, 75, "NIT:"),
        (140, 75, "800111222-3"),
        (70, 158, "NIT:"),
        (140, 158, "900555444-1"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert "nit" not in campos, "devolvio un NIT ambiguo; podria ser el del emisor"


def test_un_solo_nit_sin_cliente_si_se_devuelve():
    filas = [(70, 158, "NIT:"), (140, 158, "900555444-1")]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["nit"] == "900555444-1"


def test_dos_bloques_cliente_toma_el_primero():
    filas = [
        (70, 100, "CLIENTE:"),
        (140, 100, "PRIMERO SAS"),
        (70, 300, "CLIENTE:"),
        (140, 300, "SEGUNDO SAS"),
    ]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "PRIMERO SAS"


def test_etiqueta_sin_dos_puntos_tambien_se_reconoce():
    filas = [(70, 140, "CLIENTE"), (140, 140, "DISTRIBUIDORA EJEMPLO SAS")]

    campos = reademail.extract_order_fields_by_layout(_pdf_con_filas(filas))

    assert campos["cliente"] == "DISTRIBUIDORA EJEMPLO SAS"


# --------------------------------------------------------------------------
# identify_client_in_order_pdfs
# --------------------------------------------------------------------------
def test_identifica_cliente_por_layout(monkeypatch):
    monkeypatch.setattr(reademail, "is_order_file", lambda f: True)
    pdf = _orden("orden de compra.pdf", _pdf_con_filas(ORDEN_TIPICA))

    resultado = reademail.identify_client_in_order_pdfs([pdf], _catalogo())

    assert resultado.record is not None
    assert resultado.record.name == "DISTRIBUIDORA EJEMPLO SAS"
    assert resultado.source == "ORDER_BLOCK"
    assert resultado.raw == "DISTRIBUIDORA EJEMPLO SAS"


def test_identifica_por_nit_cuando_el_nombre_no_coincide(monkeypatch):
    monkeypatch.setattr(reademail, "is_order_file", lambda f: True)
    filas = [
        (70, 140, "CLIENTE:"),
        (140, 140, "RAZON SOCIAL QUE NO ESTA EN EL CATALOGO"),
        (70, 158, "NIT:"),
        (140, 158, "900555444-1"),
    ]
    pdf = _orden("orden de compra.pdf", _pdf_con_filas(filas))

    resultado = reademail.identify_client_in_order_pdfs([pdf], _catalogo())

    assert resultado.record is not None
    assert resultado.record.name == "DISTRIBUIDORA EJEMPLO SAS"
    assert resultado.raw == "900555444-1"


def test_cae_a_los_metodos_actuales_si_el_layout_no_resuelve(monkeypatch):
    """Con layout vacio debe seguir el camino de siempre: texto extraido."""
    monkeypatch.setattr(reademail, "is_order_file", lambda f: True)
    monkeypatch.setattr(reademail, "extract_order_fields_by_layout", lambda data: {})
    pdf = _orden(
        "orden de compra.pdf",
        b"no soy un pdf",
        texto="CLIENTE: DISTRIBUIDORA EJEMPLO SAS\nNIT: 900555444-1",
    )

    resultado = reademail.identify_client_in_order_pdfs([pdf], _catalogo())

    assert resultado.record is not None
    assert resultado.record.name == "DISTRIBUIDORA EJEMPLO SAS"


def test_pdf_ilegible_no_rompe_y_usa_el_texto_extraido(monkeypatch):
    monkeypatch.setattr(reademail, "is_order_file", lambda f: True)
    pdf = _orden(
        "orden de compra.pdf",
        b"no soy un pdf",
        texto="CLIENTE: DISTRIBUIDORA EJEMPLO SAS",
    )

    resultado = reademail.identify_client_in_order_pdfs([pdf], _catalogo())

    assert resultado.record is not None
    assert resultado.record.name == "DISTRIBUIDORA EJEMPLO SAS"


def test_layout_no_confunde_al_emisor_con_el_cliente(monkeypatch):
    """El emisor esta en el catalogo; el cliente tambien. Debe ganar el cliente."""
    monkeypatch.setattr(reademail, "is_order_file", lambda f: True)
    catalogo = _catalogo() + [
        _cliente("PROVEEDORA DE SERVICIOS SAS", "proveedoradeserviciossas", "800111222-3")
    ]
    pdf = _orden("orden de compra.pdf", _pdf_con_filas(ORDEN_TIPICA))

    resultado = reademail.identify_client_in_order_pdfs([pdf], catalogo)

    assert resultado.record.name == "DISTRIBUIDORA EJEMPLO SAS"
    assert resultado.record.name != "PROVEEDORA DE SERVICIOS SAS"
