"""
Resolucion de columnas del catalogo de clientes por encabezado.

La hoja real usa ID | Nit | Nombre | Correo | Estado. Sin "nombre" entre los
alias de "cliente", la resolucion caia al fallback posicional y cargaba la
columna ID como nombre del cliente, lo que hacia imposible identificar al
cliente por nombre.

Cada caso se ejecuta contra reademail.py y contra app/services/client_matching.py,
que mantienen copias de esta logica: si alguien arregla una y no la otra, falla.
Datos de catalogo inventados.
"""

import pytest

import reademail
from app.services import client_matching

RESOLUTORES = [
    pytest.param(reademail._resolve_column_indexes, id="monolito"),
    pytest.param(client_matching._resolve_column_indexes, id="modulo"),
]

CONSTRUCTORES = [
    pytest.param(reademail._client_records_from_values, id="monolito"),
    pytest.param(client_matching.client_records_from_values, id="modulo"),
]

HOJA_REAL = ["ID", "Nit", "Nombre", "Correo", "Estado"]
FILAS_REALES = [
    ["CLI-20260804-AAAA111111", "900555444-1", "DISTRIBUIDORA EJEMPLO SAS", "pagos@ejemplo.test", "Activo"],
    ["CLI-20260804-BBBB222222", "901000000-2", "OTRO CLIENTE SAS", "otro@ejemplo.test", "Activo"],
]


# --------------------------------------------------------------------------
# El formato de la hoja real
# --------------------------------------------------------------------------
@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_hoja_real_resuelve_nombre_en_la_columna_2(resolver):
    indices = resolver(HOJA_REAL)

    assert indices["cliente"] == 2, "cayo al fallback posicional y tomaria la columna ID"
    assert indices["cliente"] != 0


@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_hoja_real_reconoce_nit_estado_y_correo(resolver):
    indices = resolver(HOJA_REAL)

    assert indices["nit"] == 1
    assert indices["estado"] == 4
    assert indices["email"] == 3


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_hoja_real_carga_nombres_y_no_identificadores(construir):
    catalogo = construir([HOJA_REAL] + FILAS_REALES, sheet_range="Clientes!A:Z")

    nombres = [registro.name for registro in catalogo]
    assert nombres == ["DISTRIBUIDORA EJEMPLO SAS", "OTRO CLIENTE SAS"]
    for nombre in nombres:
        assert not nombre.startswith("CLI-"), "cargo el ID como nombre del cliente"


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_hoja_real_permite_identificar_al_cliente_por_nombre(construir):
    """El sintoma que motivo el fix: sin esto toda FE caia en revision manual."""
    catalogo = construir([HOJA_REAL] + FILAS_REALES, sheet_range="Clientes!A:Z")

    encontrado = reademail.match_client_raw_to_catalog("DISTRIBUIDORA EJEMPLO SAS", catalogo)

    assert encontrado is not None
    assert encontrado.name == "DISTRIBUIDORA EJEMPLO SAS"


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_hoja_real_conserva_nit_y_correo_por_fila(construir):
    catalogo = construir([HOJA_REAL] + FILAS_REALES, sheet_range="Clientes!A:Z")

    assert catalogo[0].nit == "900555444-1"
    assert catalogo[0].contact_email == "pagos@ejemplo.test"
    assert catalogo[0].active is True


# --------------------------------------------------------------------------
# Los formatos que ya funcionaban
# --------------------------------------------------------------------------
@pytest.mark.parametrize("resolver", RESOLUTORES)
@pytest.mark.parametrize(
    ("encabezado", "esperado"),
    [
        (["Cliente", "Nit", "Estado"], 0),
        (["Razón social", "Nit", "Estado"], 0),
        (["Razon social", "Nit", "Estado"], 0),
        (["Nombre cliente", "Nit", "Estado"], 0),
        (["Empresa", "Nit", "Estado"], 0),
        (["Proveedor", "Nit", "Estado"], 0),
        (["Nit", "Cliente", "Estado"], 1),
        # Los alias nuevos van en una posicion que el fallback NO elegiria solo,
        # para que el caso falle si se quita el alias.
        (["Nit", "Columna rara", "Nombre"], 2),
        (["Nit", "Columna rara", "Nombre del cliente"], 2),
        (["Nit", "Columna rara", "Razon"], 2),
        (["Nombre", "Nit", "Estado"], 0),
    ],
)
def test_alias_de_cliente_siguen_funcionando(resolver, encabezado, esperado):
    assert resolver(encabezado)["cliente"] == esperado


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_formato_clasico_cliente_nit_estado_sin_cambios(construir):
    catalogo = construir(
        [
            ["cliente", "nit", "estado"],
            ["ACME Corp", "900.123.456-7", "activo"],
            ["Inactiva SAS", "900.999.999-9", "inactivo"],
        ],
        sheet_range="Clientes!A:C",
    )

    assert [r.name for r in catalogo] == ["ACME Corp", "Inactiva SAS"]
    assert catalogo[0].active is True
    assert catalogo[1].active is False


# --------------------------------------------------------------------------
# Fallback posicional
# --------------------------------------------------------------------------
@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_sin_encabezado_reconocible_usa_el_fallback_posicional(resolver):
    indices = resolver(["Columna A", "Columna B", "Columna C"])

    assert indices["cliente"] == 0
    assert indices["nit"] == 1


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_valores_sin_fila_de_encabezado_se_leen_por_posicion(construir):
    catalogo = construir(
        [
            ["ACME Corp", "900.123.456-7"],
            ["Beta SAS", "901.000.000-2"],
        ],
        sheet_range="Clientes!A:B",
    )

    assert [r.name for r in catalogo] == ["ACME Corp", "Beta SAS"]


@pytest.mark.parametrize("construir", CONSTRUCTORES)
def test_primera_fila_con_estado_activo_se_confunde_con_encabezado(construir):
    """Defecto PREEXISTENTE, congelado aqui para que no pase inadvertido.

    'activo' es alias del campo estado, asi que una hoja SIN fila de
    encabezado pierde su primer registro. No lo introduce ni lo corrige este
    cambio: el comportamiento es identico antes y despues.
    """
    catalogo = construir(
        [
            ["ACME Corp", "900.123.456-7", "activo"],
            ["Beta SAS", "901.000.000-2", "activo"],
        ],
        sheet_range="Clientes!A:C",
    )

    assert [r.name for r in catalogo] == ["Beta SAS"], "cambio el comportamiento preexistente"


@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_el_fallback_no_reutiliza_una_columna_ya_asignada(resolver):
    """Con 'Nit' en la columna 0, el cliente no puede caer tambien en la 0."""
    indices = resolver(["Nit", "Columna rara"])

    assert indices["nit"] == 0
    assert indices["cliente"] != 0
    assert indices["cliente"] == 1


@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_el_fallback_del_nit_no_pisa_la_columna_del_cliente(resolver):
    """Con el cliente en la columna 1, el fallback del NIT debe saltar a la 2."""
    indices = resolver(["Columna rara", "Cliente", "Otra"])

    assert indices["cliente"] == 1
    assert indices["nit"] != indices["cliente"]
    assert indices["nit"] == 2


@pytest.mark.parametrize("resolver", RESOLUTORES)
def test_encabezado_reconocible_gana_sobre_la_posicion_cero(resolver):
    indices = resolver(["Identificador", "Consecutivo", "Cliente"])

    assert indices["cliente"] == 2


# --------------------------------------------------------------------------
# Paridad entre las dos copias
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "encabezado",
    [
        HOJA_REAL,
        ["Cliente", "Nit", "Estado"],
        ["Nit", "Nombre"],
        ["Columna A", "Columna B"],
        ["Identificador", "Consecutivo", "Cliente"],
        [],
    ],
    ids=["hoja-real", "clasico", "nit-nombre", "sin-alias", "cliente-al-final", "vacio"],
)
def test_las_dos_copias_resuelven_igual(encabezado):
    assert reademail._resolve_column_indexes(encabezado) == client_matching._resolve_column_indexes(encabezado)


def test_los_alias_son_identicos_en_las_dos_copias():
    assert reademail._header_aliases() == client_matching._header_aliases()
