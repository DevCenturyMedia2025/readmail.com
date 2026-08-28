"""
Identificacion flexible del cliente y orden de evaluacion en factura electronica.

Dos reglas nuevas, verificadas de punta a punta con catalogos y PDF inventados:

1. Estar en la hoja Clientes da un nombre uniforme entre facturas, pero NO
   estar ya no frena la radicacion: si la orden trae un nombre legible se usa
   tal cual. Solo se frena cuando no hay ningun nombre.
2. La comprobacion del cliente corre DESPUES de la de orden y OK. Antes, una
   factura sin orden no tenia de donde leer el cliente y caia en revision
   manual sin llegar nunca al reenvio a Compras, que es quien resuelve
   justamente la falta de orden.
"""

import base64
import time

import pytest

import reademail
from reademail import UnifiedFile

HOJA_CLIENTES = [
    ["ID", "Nit", "Nombre", "Correo", "Estado"],
    ["CLI-20260804-AAAA111111", "", "DISTRIBUIDORA EJEMPLO SAS", "pagos@ejemplo.test", "Activo"],
]


def _catalogo():
    return reademail._client_records_from_values(HOJA_CLIENTES, sheet_range="Clientes!A:Z")


def _pdf(nombre, texto=""):
    return UnifiedFile(nombre, "application/pdf", b"", "test", texto)


XML = UnifiedFile("factura.xml", "application/xml", b"<Invoice />", "test")
OK_COMPRAS = _pdf("ok.pdf", "OK DE COMPRAS")
ORDEN_EN_CATALOGO = _pdf("orden de compra.pdf", "CLIENTE: DISTRIBUIDORA EJEMPLO SAS\nORDEN DE COMPRA No 4501")
ORDEN_FUERA_CATALOGO = _pdf("orden de compra.pdf", "CLIENTE: EMPRESA FUERA DEL CATALOGO SAS\nORDEN DE COMPRA No 4501")
ORDEN_SIN_CLIENTE = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")


class _Ejecutable:
    def execute(self):
        return {}


class _Mensajes:
    def modify(self, **kwargs):
        return _Ejecutable()


class _Usuarios:
    def messages(self):
        return _Mensajes()


class _Gmail:
    def users(self):
        return _Usuarios()


def _correr(monkeypatch, archivos, *, catalogo=None, modo_pruebas=False, compras_email=""):
    estado = {}
    llamadas = {"labels": [], "replies": [], "forwards": [], "new_emails": []}
    cuerpo = "Cuerpo original de la factura."
    payload = {
        "mimeType": "text/plain",
        "body": {"data": base64.urlsafe_b64encode(cuerpo.encode("utf-8")).decode("ascii")},
        "headers": [
            {"name": "From", "value": "proveedor@ejemplo.test"},
            {"name": "To", "value": "facturas@ejemplo.test"},
            {"name": "Date", "value": "Tue, 25 Aug 2026 10:00:00 -0500"},
            {"name": "Subject", "value": "Factura de venta"},
        ],
    }

    monkeypatch.setattr(reademail, "MODO_PRUEBAS", modo_pruebas)
    monkeypatch.setattr(reademail, "COMPRAS_EMAIL", compras_email)
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: estado)
    monkeypatch.setattr(reademail, "save_state", lambda actual, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda servicio, message_id: {
            "payload": payload,
            "snippet": "",
            "internalDate": str(int(time.time() * 1000)),
        },
    )
    monkeypatch.setattr(
        reademail,
        "collect_attachments",
        lambda p: [{"attachmentId": "a1", "filename": "factura.pdf", "mimeType": "application/pdf"}],
    )
    monkeypatch.setattr(reademail, "build_unified_files", lambda s, m, a: (list(archivos), [], []))
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *a, **k: None)
    monkeypatch.setattr(reademail, "gmail_download_attachment_bytes", lambda s, m, a: b"BYTES")
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda s, mid, nombre, archive=False: llamadas["labels"].append(nombre),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *a: llamadas["replies"].append(a))
    monkeypatch.setattr(reademail, "send_new_email", lambda *a: llamadas["new_emails"].append(a))
    monkeypatch.setattr(
        reademail, "send_forward_with_attachments", lambda *a: llamadas["forwards"].append(a)
    )
    monkeypatch.setattr(reademail, "send_whatsapp_alert", lambda m, cooldown_key=None: None)

    reademail.process_message(
        _Gmail(),
        object(),
        "mensaje-1",
        _catalogo() if catalogo is None else catalogo,
        None,
        reademail.AdminLookup(set(), set(), {}),
        reademail.RegisteredLookup(set(), set(), {}),
    )
    llamadas["estado"] = estado
    return llamadas


def _cuerpo_respuesta(llamadas):
    return llamadas["replies"][0][4]


# --------------------------------------------------------------------------
# Cambio 2: no estar en la hoja no frena la factura
# --------------------------------------------------------------------------
def test_cliente_en_catalogo_usa_el_nombre_del_catalogo(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_EN_CATALOGO, OK_COMPRAS, XML])

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]
    assert "Cliente: DISTRIBUIDORA EJEMPLO SAS" in _cuerpo_respuesta(llamadas)


def test_cliente_fuera_del_catalogo_continua_y_usa_el_nombre_de_la_orden(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_FUERA_CATALOGO, OK_COMPRAS, XML])

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]
    assert reademail.LABEL_REVIEW_NAME not in llamadas["labels"]
    assert len(llamadas["replies"]) == 1
    assert "Cliente: EMPRESA FUERA DEL CATALOGO SAS" in _cuerpo_respuesta(llamadas)


def test_catalogo_vacio_no_impide_radicar(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_FUERA_CATALOGO, OK_COMPRAS, XML], catalogo=[])

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]
    assert "Cliente: EMPRESA FUERA DEL CATALOGO SAS" in _cuerpo_respuesta(llamadas)


def test_orden_sin_cliente_legible_va_a_revision_sin_responder(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_SIN_CLIENTE, OK_COMPRAS, XML])

    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert llamadas["replies"] == []
    assert llamadas["forwards"] == []


def test_orden_sin_cliente_va_a_revision_tambien_en_modo_pruebas(monkeypatch):
    """El reenvio a Compras resuelve la falta de orden, no la de cliente."""
    llamadas = _correr(
        monkeypatch,
        [ORDEN_SIN_CLIENTE, OK_COMPRAS, XML],
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert llamadas["forwards"] == []
    assert llamadas["replies"] == []


@pytest.mark.parametrize(
    ("crudo", "esperado"),
    [
        ("  DISTRIBUIDORA EJEMPLO SAS  ", "DISTRIBUIDORA EJEMPLO SAS"),
        ("DISTRIBUIDORA   EJEMPLO   SAS", "DISTRIBUIDORA EJEMPLO SAS"),
        (":DISTRIBUIDORA EJEMPLO SAS.", "DISTRIBUIDORA EJEMPLO SAS"),
        ("DISTRIBUIDORA EJEMPLO SAS,", "DISTRIBUIDORA EJEMPLO SAS"),
        ("", ""),
    ],
)
def test_limpieza_del_nombre_leido_de_la_orden(crudo, esperado):
    assert reademail.clean_client_display_name(crudo) == esperado


# --------------------------------------------------------------------------
# Cambio 3: orden y OK se evaluan ANTES que el cliente
# --------------------------------------------------------------------------
def test_sin_orden_en_pruebas_se_reenvia_a_compras(monkeypatch):
    """Antes caia en revision manual y nunca llegaba a Compras."""
    llamadas = _correr(
        monkeypatch,
        [OK_COMPRAS, XML],
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert len(llamadas["forwards"]) == 1
    assert llamadas["replies"] == []


def test_sin_orden_en_modo_real_se_rechaza_con_el_motivo_de_orden(monkeypatch):
    llamadas = _correr(monkeypatch, [OK_COMPRAS, XML])

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1
    assert reademail.MISSING_ORDER_MESSAGE in _cuerpo_respuesta(llamadas)


def test_sin_orden_ni_ok_en_pruebas_se_reenvia_a_compras(monkeypatch):
    llamadas = _correr(
        monkeypatch,
        [_pdf("factura.pdf"), XML],
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert len(llamadas["forwards"]) == 1


def test_sin_orden_ni_ok_en_modo_real_se_rechaza_con_ambos_motivos(monkeypatch):
    llamadas = _correr(monkeypatch, [_pdf("factura.pdf"), XML])

    cuerpo = _cuerpo_respuesta(llamadas)
    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert reademail.MISSING_ORDER_MESSAGE in cuerpo
    assert reademail.MISSING_OK_COMPRAS_MESSAGE in cuerpo


def test_sin_ok_con_orden_en_modo_real_se_rechaza(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_EN_CATALOGO, XML])

    cuerpo = _cuerpo_respuesta(llamadas)
    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert reademail.MISSING_OK_COMPRAS_MESSAGE in cuerpo
    assert reademail.MISSING_ORDER_MESSAGE not in cuerpo


def test_sin_ok_con_orden_en_pruebas_se_reenvia(monkeypatch):
    llamadas = _correr(
        monkeypatch,
        [ORDEN_EN_CATALOGO, XML],
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert len(llamadas["forwards"]) == 1


def test_el_rechazo_conserva_el_nombre_del_cliente_cuando_se_pudo_leer(monkeypatch):
    """Reordenar no debe degradar el correo de rechazo."""
    llamadas = _correr(monkeypatch, [ORDEN_EN_CATALOGO, XML])

    assert "Cliente identificado: DISTRIBUIDORA EJEMPLO SAS" in _cuerpo_respuesta(llamadas)


def test_factura_completa_se_aprueba_con_el_cliente_correcto(monkeypatch):
    llamadas = _correr(monkeypatch, [ORDEN_EN_CATALOGO, OK_COMPRAS, XML])

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]
    assert "Cliente: DISTRIBUIDORA EJEMPLO SAS" in _cuerpo_respuesta(llamadas)


# --------------------------------------------------------------------------
# La cuenta de cobro no cambia
# --------------------------------------------------------------------------
def _paquete_cuenta_cobro():
    """Paquete completo: los 5 documentos y el OK de compras sobre la orden."""
    return [
        _pdf("cuenta de cobro.pdf", "CUENTA DE COBRO"),
        _pdf("cedula.pdf", "REPUBLICA DE COLOMBIA CEDULA DE CIUDADANIA"),
        _pdf("rut.pdf", "Registro Unico Tributario RUT DIAN"),
        _pdf("certificado bancario.pdf", "bancolombia certifica cuenta de ahorros nro de producto"),
        _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501 aprobado por compras"),
    ]


def test_cuenta_de_cobro_completa_sigue_aprobando(monkeypatch):
    llamadas = _correr(monkeypatch, _paquete_cuenta_cobro())

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]
    assert len(llamadas["replies"]) == 1


def test_cuenta_de_cobro_incompleta_sigue_rechazando(monkeypatch):
    llamadas = _correr(monkeypatch, _paquete_cuenta_cobro()[:3])

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1


def test_cuenta_de_cobro_no_la_captura_el_reenvio_a_compras(monkeypatch):
    llamadas = _correr(
        monkeypatch,
        _paquete_cuenta_cobro()[:3],
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert llamadas["forwards"] == []


def test_cuenta_de_cobro_sin_cliente_no_va_a_revision_por_cliente(monkeypatch):
    """La regla del cliente solo aplica a factura electronica."""
    llamadas = _correr(monkeypatch, _paquete_cuenta_cobro(), catalogo=[])

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]


# --------------------------------------------------------------------------
# La cuenta de cobro tambien exige el OK de compras
# --------------------------------------------------------------------------
def test_cuenta_de_cobro_sin_ok_de_compras_se_rechaza(monkeypatch):
    paquete = _paquete_cuenta_cobro()
    paquete[-1] = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")

    llamadas = _correr(monkeypatch, paquete)

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1


def test_cuenta_de_cobro_acepta_el_ok_en_un_archivo_aparte(monkeypatch):
    paquete = _paquete_cuenta_cobro()
    paquete[-1] = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")
    paquete.append(_pdf("ok compras.pdf", ""))

    llamadas = _correr(monkeypatch, paquete)

    assert llamadas["labels"] == [reademail.LABEL_APPROVED_NAME]


def test_cuenta_de_cobro_sin_ok_se_reenvia_a_compras_en_pruebas(monkeypatch):
    """El OK es interno: en pruebas se le pide a Compras, no al proveedor."""
    paquete = _paquete_cuenta_cobro()
    paquete[-1] = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")

    llamadas = _correr(
        monkeypatch,
        paquete,
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert len(llamadas["forwards"]) == 1
    assert llamadas["labels"] == [reademail.LABEL_REVIEW_NAME]
    assert llamadas["replies"] == []


def test_cuenta_de_cobro_sin_ok_se_rechaza_sin_compras_email(monkeypatch):
    """Sin buzon de Compras el reenvio no ocurre y se cae al rechazo normal."""
    paquete = _paquete_cuenta_cobro()
    paquete[-1] = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")

    llamadas = _correr(monkeypatch, paquete, modo_pruebas=True, compras_email="")

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1
    assert llamadas["forwards"] == []


def test_cuenta_de_cobro_incompleta_no_se_reenvia_ni_en_pruebas(monkeypatch):
    """El reenvio cubre el OK, no el paquete: eso lo arma el proveedor."""
    paquete = _paquete_cuenta_cobro()[:3]

    llamadas = _correr(
        monkeypatch,
        paquete,
        modo_pruebas=True,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert llamadas["forwards"] == []
    assert len(llamadas["replies"]) == 1


# --------------------------------------------------------------------------
# El reenvio a Compras es exclusivo del modo pruebas
# --------------------------------------------------------------------------
def _cc_sin_ok():
    paquete = _paquete_cuenta_cobro()
    paquete[-1] = _pdf("orden de compra.pdf", "ORDEN DE COMPRA No 4501")
    return paquete


@pytest.mark.parametrize(
    "archivos",
    [
        [ORDEN_EN_CATALOGO, XML],
        [OK_COMPRAS, XML],
        [_pdf("factura.pdf", "factura de venta"), XML],
        _cc_sin_ok(),
        _paquete_cuenta_cobro()[:3],
    ],
    ids=[
        "fe_sin_ok",
        "fe_sin_orden",
        "fe_sin_orden_ni_ok",
        "cuenta_cobro_sin_ok",
        "cuenta_cobro_incompleta",
    ],
)
def test_modo_real_nunca_reenvia_a_compras(monkeypatch, archivos):
    """Fuera de MODO_PRUEBAS solo se rechaza, en las dos ramas.

    Se define COMPRAS_EMAIL a proposito: el buzon configurado no debe bastar
    para que el reenvio ocurra; la unica llave es MODO_PRUEBAS.
    """
    llamadas = _correr(
        monkeypatch,
        archivos,
        modo_pruebas=False,
        compras_email="compras@ejemplo.test",
    )

    assert llamadas["forwards"] == []
    assert llamadas["new_emails"] == []
    assert llamadas["labels"] == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1
