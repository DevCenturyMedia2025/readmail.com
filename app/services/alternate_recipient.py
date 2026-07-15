"""
Deteccion de proveedores tecnologicos y destinatarios alternos.

Este modulo contiene reglas puras para identificar correos automaticos de
facturacion electronica DIAN y extraer el correo real del emisor desde XML.
"""

import re
import xml.etree.ElementTree as ET
from typing import Optional

from app.gmail.parsing import is_no_reply_sender


DIAN_SUBJECT_RE = re.compile(r"^\s*(\d{9,10});.+;.+;\d{2};")
CDATA_RE = re.compile(rb"<!\[CDATA\[(.*?)\]\]>", re.DOTALL)
EMAIL_RE = re.compile(r"^[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+$")
BLOCKED_SUPPLIER_DOMAINS = {"century-media.net"}


def is_tech_provider(subject: str, sender_email: str, has_xml: bool) -> bool:
    if DIAN_SUBJECT_RE.search(subject or ""):
        return True
    return bool(has_xml and is_no_reply_sender(sender_email or ""))


def extract_supplier_email(xml_bytes: bytes) -> Optional[str]:
    xml_content = _extract_embedded_xml(xml_bytes or b"")
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return None

    supplier_party = _first_descendant(root, "AccountingSupplierParty")
    if supplier_party is None:
        return None

    for electronic_mail in supplier_party.iter():
        if _local_name(electronic_mail.tag) != "ElectronicMail":
            continue
        email = (electronic_mail.text or "").strip()
        if not EMAIL_RE.fullmatch(email):
            continue
        domain = email.rsplit("@", 1)[1].lower()
        if domain in BLOCKED_SUPPLIER_DOMAINS:
            return None
        return email

    return None


def extract_nit_from_dian_subject(subject: str) -> Optional[str]:
    match = DIAN_SUBJECT_RE.search(subject or "")
    if not match:
        return None
    return match.group(1)


def _extract_embedded_xml(xml_bytes: bytes) -> bytes:
    match = CDATA_RE.search(xml_bytes)
    if not match:
        return xml_bytes
    return match.group(1).strip()


def _first_descendant(root: ET.Element, local_name: str) -> Optional[ET.Element]:
    for element in root.iter():
        if _local_name(element.tag) == local_name:
            return element
    return None


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[1]
    if ":" in tag:
        return tag.rsplit(":", 1)[1]
    return tag
