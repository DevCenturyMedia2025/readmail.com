"""
Construccion de correos de respuesta de ReadMail.

Modulo paralelo a build_rejected_email y build_approved_email de
reademail.py. Los asuntos, cuerpos, tildes y saltos de linea se
preservan EXACTOS: son los textos que reciben los proveedores.

send_reply_email y create_raw_email NO viven aqui (tocan Gmail/MIME);
se extraeran en una fase posterior con mocks.
Todavia no esta conectado a reademail.py.

⚠️ Mientras siga duplicado, cualquier cambio de texto debe aplicarse en
AMBOS lados. tests/test_email_responses_parity.py compara la salida de
las dos versiones y falla si se desincronizan.
"""

from typing import List, Optional, Tuple

# Espejo de las constantes de reademail.py. Deben coincidir palabra por palabra.
MISSING_ORDER_MESSAGE = (
    "Falta la ORDEN DE COMPRA. Adjunte el PDF de la orden que le envió el área de Compras. "
    "El archivo debe tener un nombre claro (por ejemplo: orden de compra.pdf) y ser un PDF con "
    "texto seleccionable, no una fotografía ni un escaneo de imagen."
)
MISSING_OK_COMPRAS_MESSAGE = (
    "Falta el OK DE COMPRAS. Solicítelo al área de Compras y adjúntelo como PDF. "
    "El documento debe contener la frase «OK de compras» en texto seleccionable, "
    "no como imagen escaneada."
)
SELECTABLE_TEXT_NOTICE = (
    "Recuerde: todos los documentos deben enviarse en PDF con texto seleccionable. "
    "Los archivos escaneados como imagen no pueden ser leídos y se darán por faltantes."
)


def build_rejected_email(
    radicado: str,
    invoice_type: str,
    reasons: List[str],
    client_name: Optional[str],
) -> Tuple[str, str]:
    subject = f"RECHAZADO - facturación no radicada (ID: {radicado})"
    reasons_lines = (
        "\n".join(f"  - {r}" for r in reasons)
        if reasons
        else "  - Documentación incompleta o no identificada."
    )
    body = (
        "Hola,\n\n"
        "Recibimos tu correo, pero no fue posible radicarlo.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente identificado: {client_name or 'No identificado'}\n"
        f"Clasificación detectada: {invoice_type}\n\n"
        "Motivos del rechazo:\n"
        f"{reasons_lines}\n\n"
        "Por favor revisa que la documentación esté completa y vuelve a enviar.\n\n"
        f"{SELECTABLE_TEXT_NOTICE}\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


def build_approved_email(
    radicado: str,
    invoice_type: str,
    client_name: str,
    pdf_count: int,
    xml_count: int,
) -> Tuple[str, str]:
    # El asunto no lleva tilde en "facturacion": se copia tal cual del monolito.
    subject = f"APROBADO - facturacion recibida correctamente (ID: {radicado})"
    body = (
        "Hola,\n\n"
        "Confirmamos que tu correo fue recibido y validado correctamente.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente: {client_name}\n"
        f"Clasificación: {invoice_type}\n"
        f"PDF detectados: {pdf_count}\n"
        f"XML detectados: {xml_count}\n\n"
        "Tu radicación queda en proceso interno.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body
