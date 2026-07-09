"""
Tests de los correos de respuesta (app/services/email_responses.py).

Congelan los textos EXACTOS (asuntos, tildes, saltos de linea) que
reciben los proveedores. Si un test falla tras editar el modulo, el
texto cambio y hay que decidirlo conscientemente.
"""

from app.services.email_responses import build_approved_email, build_rejected_email


def test_rechazo_asunto_exacto():
    subject, _ = build_rejected_email("RAD-20260708-000001", "FACTURA ELECTRONICA", ["motivo x"], "ACME")
    assert subject == "RECHAZADO - facturacion no radicada (ID: RAD-20260708-000001)"


def test_rechazo_cuerpo_completo_exacto():
    _, body = build_rejected_email(
        radicado="RAD-1",
        invoice_type="CUENTA DE COBRO",
        reasons=["Falta RUT.", "Falta orden de compra."],
        client_name="ACME SAS",
    )
    esperado = (
        "Hola,\n\n"
        "Recibimos tu correo, pero no fue posible radicarlo.\n\n"
        "ID interno: RAD-1\n"
        "Cliente identificado: ACME SAS\n"
        "Clasificacion detectada: CUENTA DE COBRO\n\n"
        "Motivos del rechazo:\n"
        "  - Falta RUT.\n"
        "  - Falta orden de compra.\n\n"
        "Por favor revisa que la documentacion este completa y vuelve a enviar.\n\n"
        "Gracias,\n"
        "Equipo de Facturacion\n"
    )
    assert body == esperado


def test_rechazo_sin_cliente_usa_no_identificado():
    _, body = build_rejected_email("RAD-1", "FACTURA ELECTRONICA", ["motivo"], None)
    assert "Cliente identificado: No identificado\n" in body


def test_rechazo_sin_motivos_usa_texto_por_defecto():
    _, body = build_rejected_email("RAD-1", "FACTURA ELECTRONICA", [], "ACME")
    assert "  - Documentacion incompleta o no identificada.\n" in body


def test_aprobado_asunto_exacto():
    subject, _ = build_approved_email("RAD-9", "FACTURA ELECTRONICA", "ACME", 3, 1)
    assert subject == "APROBADO - facturacion recibida correctamente (ID: RAD-9)"


def test_aprobado_cuerpo_completo_exacto():
    _, body = build_approved_email(
        radicado="RAD-9",
        invoice_type="FACTURA ELECTRONICA",
        client_name="ACME SAS",
        pdf_count=3,
        xml_count=1,
    )
    esperado = (
        "Hola,\n\n"
        "Confirmamos que tu correo fue recibido y validado correctamente.\n\n"
        "ID interno: RAD-9\n"
        "Cliente: ACME SAS\n"
        "Clasificacion: FACTURA ELECTRONICA\n"
        "PDF detectados: 3\n"
        "XML detectados: 1\n\n"
        "Tu radicacion queda en proceso interno.\n\n"
        "Gracias,\n"
        "Equipo de Facturacion\n"
    )
    assert body == esperado
