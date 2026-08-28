"""Ciclo completo del reenvio a Compras y su respuesta.

El reenvio sale como conversacion aparte, a proposito: asi el proveedor no
puede quedar incluido si alguien en Compras responde a todos. El vinculo con la
factura original se guarda en el estado, y cuando Compras contesta ambos
correos quedan en REVISION MANUAL, con la conversacion y los archivos
completos, para que una persona radique. Nada se aprueba automaticamente.
"""

import base64
import time

import pytest

import reademail
from reademail import UnifiedFile


def _pdf(nombre, texto=""):
    return UnifiedFile(nombre, "application/pdf", b"", "test", texto)


XML = UnifiedFile("factura.xml", "application/xml", b"<Invoice />", "test")
ORDEN = _pdf("orden de compra.pdf", "CLIENTE: EJEMPLO SAS\nORDEN DE COMPRA No 4501")
OK_COMPRAS = _pdf("ok compras.pdf", "OK DE COMPRAS")
HILO_COMPRAS = "hilo-compras-1"


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


def _correr(
    monkeypatch,
    archivos,
    *,
    estado,
    message_id,
    asunto="Factura de venta",
    remitente="proveedor@ejemplo.test",
    thread_id=None,
    modo_pruebas=True,
    compras_email="compras@ejemplo.test",
    thread_id_reenvio=HILO_COMPRAS,
):
    llamadas = {"labels": [], "replies": [], "forwards": [], "new_emails": []}
    cuerpo = "Cuerpo original."
    payload = {
        "mimeType": "text/plain",
        "body": {"data": base64.urlsafe_b64encode(cuerpo.encode("utf-8")).decode("ascii")},
        "headers": [
            {"name": "From", "value": remitente},
            {"name": "To", "value": "facturas@ejemplo.test"},
            {"name": "Date", "value": "Fri, 28 Aug 2026 10:00:00 -0500"},
            {"name": "Subject", "value": asunto},
        ],
    }
    mensaje = {
        "payload": payload,
        "snippet": "",
        "internalDate": str(int(time.time() * 1000)),
    }
    if thread_id:
        mensaje["threadId"] = thread_id

    def _reenviar(*args):
        llamadas["forwards"].append(args)
        return {"threadId": thread_id_reenvio} if thread_id_reenvio else None

    monkeypatch.setattr(reademail, "MODO_PRUEBAS", modo_pruebas)
    monkeypatch.setattr(reademail, "COMPRAS_EMAIL", compras_email)
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: estado)
    monkeypatch.setattr(reademail, "save_state", lambda actual, account_id=None: None)
    monkeypatch.setattr(reademail, "safe_get_message_full", lambda s, mid: mensaje)
    monkeypatch.setattr(
        reademail,
        "collect_attachments",
        lambda p: [{"attachmentId": "a1", "filename": "f.pdf", "mimeType": "application/pdf"}],
    )
    monkeypatch.setattr(reademail, "build_unified_files", lambda s, m, a: (list(archivos), [], []))
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *a, **k: None)
    monkeypatch.setattr(reademail, "gmail_download_attachment_bytes", lambda s, m, a: b"BYTES")
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda s, mid, nombre, archive=False: llamadas["labels"].append((mid, nombre)),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *a: llamadas["replies"].append(a))
    monkeypatch.setattr(
        reademail,
        "send_new_email",
        lambda *a: (llamadas["new_emails"].append(a), {"threadId": thread_id_reenvio})[1],
    )
    monkeypatch.setattr(reademail, "send_forward_with_attachments", _reenviar)
    monkeypatch.setattr(reademail, "send_whatsapp_alert", lambda m, cooldown_key=None: None)

    reademail.process_message(
        _Gmail(),
        object(),
        message_id,
        [],
        None,
        reademail.AdminLookup(set(), set(), {}),
        reademail.RegisteredLookup(set(), set(), {}),
    )
    return llamadas


def _reenviar_factura(monkeypatch, estado):
    """Paso 1: la factura llega sin OK y se reenvia a Compras."""
    return _correr(monkeypatch, [ORDEN, XML], estado=estado, message_id="factura-1")


def test_el_reenvio_deja_la_factura_en_revision_manual(monkeypatch):
    estado = {}
    llamadas = _reenviar_factura(monkeypatch, estado)

    assert len(llamadas["forwards"]) == 1
    assert llamadas["labels"] == [("factura-1", reademail.LABEL_REVIEW_NAME)]
    assert llamadas["replies"] == []


def test_el_reenvio_guarda_el_vinculo_con_la_factura(monkeypatch):
    estado = {}
    _reenviar_factura(monkeypatch, estado)

    vinculo = estado["compras_forwards"][HILO_COMPRAS]
    assert vinculo["original_message_id"] == "factura-1"
    assert vinculo["radicado"].startswith("RAD-")


def test_la_respuesta_de_compras_lleva_ambos_correos_a_revision(monkeypatch):
    estado = {}
    _reenviar_factura(monkeypatch, estado)

    llamadas = _correr(
        monkeypatch,
        [OK_COMPRAS],
        estado=estado,
        message_id="respuesta-compras",
        asunto="Re: Falta documentacion - Factura de venta",
        remitente="compras@ejemplo.test",
        thread_id=HILO_COMPRAS,
    )

    assert llamadas["labels"] == [
        ("respuesta-compras", reademail.LABEL_REVIEW_NAME),
        ("factura-1", reademail.LABEL_REVIEW_NAME),
    ]
    assert llamadas["replies"] == []
    assert llamadas["forwards"] == []


def test_la_respuesta_de_compras_no_se_trata_como_factura_nueva(monkeypatch):
    """Sin el vinculo, un paquete completo de Compras se habria aprobado."""
    estado = {}
    _reenviar_factura(monkeypatch, estado)

    llamadas = _correr(
        monkeypatch,
        [ORDEN, OK_COMPRAS, XML],
        estado=estado,
        message_id="respuesta-compras",
        asunto="Re: Falta documentacion - Factura de venta",
        remitente="compras@ejemplo.test",
        thread_id=HILO_COMPRAS,
    )

    etiquetas = [nombre for _, nombre in llamadas["labels"]]
    assert reademail.LABEL_APPROVED_NAME not in etiquetas
    assert llamadas["replies"] == []


def test_se_reconoce_por_radicado_si_se_pierde_el_hilo(monkeypatch):
    """Respaldo para el caso en que alguien reenvie el correo a mano."""
    estado = {}
    _reenviar_factura(monkeypatch, estado)
    radicado = estado["compras_forwards"][HILO_COMPRAS]["radicado"]

    llamadas = _correr(
        monkeypatch,
        [OK_COMPRAS],
        estado=estado,
        message_id="respuesta-suelta",
        asunto=f"RV: Falta documentacion (ID: {radicado})",
        remitente="otra.persona@ejemplo.test",
        thread_id="hilo-distinto",
    )

    assert ("factura-1", reademail.LABEL_REVIEW_NAME) in llamadas["labels"]
    assert llamadas["replies"] == []


def test_un_correo_ajeno_no_se_confunde_con_la_respuesta(monkeypatch):
    """Una factura nueva de otro proveedor sigue su flujo normal."""
    estado = {}
    _reenviar_factura(monkeypatch, estado)

    llamadas = _correr(
        monkeypatch,
        [_pdf("factura.pdf", "factura de venta"), XML],
        estado=estado,
        message_id="factura-2",
        asunto="Factura de otro proveedor",
        remitente="otro@ejemplo.test",
        thread_id="hilo-ajeno",
        modo_pruebas=False,
        compras_email="",
    )

    etiquetas = [nombre for _, nombre in llamadas["labels"]]
    assert etiquetas == [reademail.LABEL_REJECTED_NAME]
    assert len(llamadas["replies"]) == 1


def test_sin_hilo_devuelto_el_vinculo_se_guarda_por_radicado(monkeypatch):
    """Si Gmail no devuelve el hilo, el radicado sigue sirviendo de llave."""
    estado = {}
    _correr(
        monkeypatch,
        [ORDEN, XML],
        estado=estado,
        message_id="factura-1",
        thread_id_reenvio=None,
    )

    claves = list(estado["compras_forwards"])
    assert len(claves) == 1
    assert claves[0].startswith("RAD-")


# --------------------------------------------------------------------------
# Instrucciones a Compras sobre como devolver el soporte
# --------------------------------------------------------------------------
def _texto(invoice_type="FACTURA ELECTRONICA", faltantes=None):
    return reademail.build_compras_request_text(
        radicado="RAD-20260828-000009",
        invoice_type=invoice_type,
        sender_email="proveedor@ejemplo.test",
        missing_documents=faltantes if faltantes is not None else ["OK de compras"],
    )


def test_pide_responder_ese_mismo_correo():
    texto = _texto()
    assert "Responda a ESTE MISMO correo" in texto
    assert "IMPORTANTE — no cree un correo nuevo." in texto


def test_advierte_que_un_correo_nuevo_pierde_el_vinculo():
    texto = _texto()
    assert "no queda vinculado a nada" in texto
    assert "la radicación se queda detenida" in texto


def test_ofrece_el_radicado_como_salida_si_usan_otro_correo():
    """La deteccion por asunto existe: el texto no debe ocultarla."""
    texto = _texto()
    assert "conserve el identificador RAD-20260828-000009 en el asunto" in texto


def test_identifica_de_que_documento_se_trata():
    assert "una factura electrónica" in _texto()
    assert "una cuenta de cobro" in _texto(invoice_type="CUENTA DE COBRO")


def test_nombra_el_proveedor_y_los_documentos_pendientes():
    texto = _texto(faltantes=["orden de compra", "OK de compras"])
    assert "Proveedor: proveedor@ejemplo.test" in texto
    assert "- orden de compra" in texto
    assert "- OK de compras" in texto


def test_pide_pdf_con_texto_seleccionable():
    assert "texto seleccionable" in _texto()


def test_avisa_que_no_se_le_responde_al_proveedor():
    assert "REVISIÓN MANUAL" in _texto()
    assert "sin responderle al proveedor" in _texto()
