"""
Construccion de correos de respuesta de ReadMail.

Modulo paralelo a build_rejected_email y build_approved_email de
reademail.py. Los asuntos, cuerpos, tildes y saltos de linea se
preservan EXACTOS: son los textos que reciben los proveedores.

send_reply_email y create_raw_email NO viven aqui (tocan Gmail/MIME);
se extraeran en una fase posterior con mocks.
Todavia no esta conectado a reademail.py.
"""

from typing import List, Optional, Tuple


def build_rejected_email(
    radicado: str,
    invoice_type: str,
    reasons: List[str],
    client_name: Optional[str],
) -> Tuple[str, str]:
    subject = f"RECHAZADO - facturacion no radicada (ID: {radicado})"
    reasons_lines = (
        "\n".join(f"  - {r}" for r in reasons)
        if reasons
        else "  - Documentacion incompleta o no identificada."
    )
    body = (
        "Hola,\n\n"
        "Recibimos tu correo, pero no fue posible radicarlo.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente identificado: {client_name or 'No identificado'}\n"
        f"Clasificacion detectada: {invoice_type}\n\n"
        "Motivos del rechazo:\n"
        f"{reasons_lines}\n\n"
        "Por favor revisa que la documentacion este completa y vuelve a enviar.\n\n"
        "Gracias,\n"
        "Equipo de Facturacion\n"
    )
    return subject, body


def build_approved_email(
    radicado: str,
    invoice_type: str,
    client_name: str,
    pdf_count: int,
    xml_count: int,
) -> Tuple[str, str]:
    subject = f"APROBADO - facturacion recibida correctamente (ID: {radicado})"
    body = (
        "Hola,\n\n"
        "Confirmamos que tu correo fue recibido y validado correctamente.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente: {client_name}\n"
        f"Clasificacion: {invoice_type}\n"
        f"PDF detectados: {pdf_count}\n"
        f"XML detectados: {xml_count}\n\n"
        "Tu radicacion queda en proceso interno.\n\n"
        "Gracias,\n"
        "Equipo de Facturacion\n"
    )
    return subject, body
