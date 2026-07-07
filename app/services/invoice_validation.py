"""
Reglas puras de validacion de facturas.

Este modulo espeja la logica de clasificacion y validacion minima de
documentos que hoy vive en reademail.py (classify_invoice_type,
validate_electronic_invoice_minimum, validate_pdf_minimum,
format_missing_documents). Los defaults numericos coinciden con los
defaults actuales (MIN_PDF_FE=3, MIN_XML_FE=1, MIN_PDF_CC=4) para no
cambiar comportamiento. Todavia no esta conectado a reademail.py.
"""

from typing import Dict, List, Optional

MIN_PDF_FE_DEFAULT = 3
MIN_XML_FE_DEFAULT = 1
MIN_PDF_CC_DEFAULT = 4

DOCUMENT_LABELS_DEFAULT: Dict[str, str] = {
    "cuenta_cobro": "cuenta de cobro",
    "cedula": "cédula",
    "rut": "RUT",
    "certificado_bancario": "certificado bancario",
    "orden_compra": "orden de compra",
    "aprobado_compras": "aprobado de compras",
}


def classify_invoice_type(xml_count: int, min_xml_fe: int = MIN_XML_FE_DEFAULT) -> str:
    return "FACTURA ELECTRONICA" if xml_count >= min_xml_fe else "CUENTA DE COBRO"


def validate_electronic_invoice_minimum(
    pdf_count: int,
    xml_count: int,
    min_pdf_fe: int = MIN_PDF_FE_DEFAULT,
    min_xml_fe: int = MIN_XML_FE_DEFAULT,
) -> List[str]:
    errors = []
    if pdf_count < min_pdf_fe:
        errors.append(
            "Factura electrónica: archivos incompletos, revisa tus documentos y que estén completos."
        )
    if xml_count < min_xml_fe:
        missing_xml = min_xml_fe - xml_count
        errors.append(
            f"Factura electrónica: falta {missing_xml} XML para completar el mínimo requerido de {min_xml_fe}."
        )
    return errors


def validate_pdf_minimum(
    invoice_type: str,
    pdf_count: int,
    min_pdf_cc: int = MIN_PDF_CC_DEFAULT,
) -> Optional[str]:
    if invoice_type == "CUENTA DE COBRO" and pdf_count < min_pdf_cc:
        return "Cuenta de cobro: archivos incompletos, revisa tus documentos y que estén completos."
    return None


def format_missing_documents(
    doc_types: List[str],
    labels: Optional[Dict[str, str]] = None,
) -> List[str]:
    labels = labels if labels is not None else DOCUMENT_LABELS_DEFAULT
    return [labels.get(doc_type, str(doc_type).replace("_", " ")) for doc_type in doc_types]
