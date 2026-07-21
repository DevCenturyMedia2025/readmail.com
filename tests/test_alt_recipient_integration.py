from app.models import ClientRecord
from app.utils.text import normalize_alnum, normalize_nit
from reademail import decide_rejection_recipient


DIAN_SUBJECT = "123456789;ACME S.A.S.;PRUE0001;01;ACME S.A.S."


def _record(name, nit, contact_email=None):
    return ClientRecord(
        name=name,
        normalized_name=normalize_alnum(name),
        nit=nit,
        normalized_nit=normalize_nit(nit),
        contact_email=contact_email,
    )


def _invoice_with_supplier_email(email):
    return f"""<?xml version="1.0" encoding="UTF-8"?>
    <Invoice xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
             xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
      <cac:AccountingSupplierParty>
        <cac:Party><cac:Contact><cbc:ElectronicMail>{email}</cbc:ElectronicMail></cac:Contact></cac:Party>
      </cac:AccountingSupplierParty>
    </Invoice>
    """.encode("utf-8")


def test_decide_rejection_recipient_flag_off_sin_desvio():
    result = decide_rejection_recipient(
        sender_email="notificacion@dian.gov.co",
        enabled=False,
        xml_bytes=_invoice_with_supplier_email("proveedor@example.com"),
        subject=DIAN_SUBJECT,
        catalog=[],
        fallback_email="fallback@century-media.net",
    )

    assert result == (None, "deshabilitado", False)


def test_decide_rejection_recipient_flag_on_no_reply_xml():
    result = decide_rejection_recipient(
        sender_email="notificacion@dian.gov.co",
        enabled=True,
        xml_bytes=_invoice_with_supplier_email("proveedor@example.com"),
        subject=DIAN_SUBJECT,
        catalog=[_record("ACME", "123456789", "sheet@acme.test")],
        fallback_email="fallback@century-media.net",
    )

    assert result == ("proveedor@example.com", "xml", True)


def test_decide_rejection_recipient_flag_on_remitente_normal_asunto_dian_desvia():
    result = decide_rejection_recipient(
        sender_email="juan@empresa.com",
        enabled=True,
        xml_bytes=_invoice_with_supplier_email("proveedor@example.com"),
        subject=DIAN_SUBJECT,
        catalog=[],
        fallback_email="fallback@century-media.net",
    )

    assert result == ("proveedor@example.com", "xml", True)


def test_decide_rejection_recipient_flag_on_remitente_normal_asunto_normal_sin_desvio():
    result = decide_rejection_recipient(
        sender_email="juan@empresa.com",
        enabled=True,
        xml_bytes=_invoice_with_supplier_email("proveedor@example.com"),
        subject="Factura marzo",
        catalog=[],
        fallback_email="fallback@century-media.net",
    )

    assert result == (None, "remitente_normal", False)


def test_decide_rejection_recipient_flag_on_no_reply_asunto_normal_desvia():
    result = decide_rejection_recipient(
        sender_email="notificacion@dian.gov.co",
        enabled=True,
        xml_bytes=_invoice_with_supplier_email("proveedor@example.com"),
        subject="Factura marzo",
        catalog=[],
        fallback_email="fallback@century-media.net",
    )

    assert result == ("proveedor@example.com", "xml", True)


def test_decide_rejection_recipient_flag_on_sin_datos():
    result = decide_rejection_recipient(
        sender_email="notificacion@dian.gov.co",
        enabled=True,
        xml_bytes=None,
        subject="Factura marzo",
        catalog=[],
        fallback_email="",
    )

    assert result == (None, "remitente_normal", False)
