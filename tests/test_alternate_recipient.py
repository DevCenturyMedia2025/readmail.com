from pathlib import Path

from app.models import ClientRecord
from app.services.alternate_recipient import (
    extract_nit_from_dian_subject,
    extract_supplier_email,
    is_tech_provider,
    resolve_alternate_recipient,
)
from app.utils.text import normalize_alnum, normalize_nit


FIXTURES_DIR = Path(__file__).parent / "fixtures"
DIAN_SUBJECT = "123456789;ACME S.A.S.;PRUE0001;01;ACME S.A.S."


def _record(name, nit, contact_email=None, active=True):
    return ClientRecord(
        name=name,
        normalized_name=normalize_alnum(name),
        nit=nit,
        normalized_nit=normalize_nit(nit),
        contact_email=contact_email,
        active=active,
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


XML_WITHOUT_SUPPLIER_EMAIL = b"""<?xml version="1.0" encoding="UTF-8"?>
<Invoice xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
         xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
  <cac:AccountingSupplierParty>
    <cac:Party><cac:Contact /></cac:Party>
  </cac:AccountingSupplierParty>
</Invoice>
"""


def test_is_tech_provider_true_for_real_dian_subject_and_extracts_nit():
    assert is_tech_provider(DIAN_SUBJECT, "juan@empresa.com", has_xml=False) is True
    assert extract_nit_from_dian_subject(DIAN_SUBJECT) == "123456789"


def test_is_tech_provider_false_for_normal_subject_and_sender():
    assert is_tech_provider("Factura marzo", "juan@empresa.com", has_xml=True) is False
    assert extract_nit_from_dian_subject("Factura marzo") is None


def test_is_tech_provider_true_for_no_reply_sender_with_xml():
    assert is_tech_provider("Factura marzo", "notificaciones@int.lafactura.co", has_xml=True) is True


def test_extract_supplier_email_from_attached_document_cdata():
    xml_bytes = (FIXTURES_DIR / "attached_document_sample.xml").read_bytes()

    assert extract_supplier_email(xml_bytes) == "proveedor@example.com"


def test_extract_supplier_email_ignores_customer_and_signature_emails():
    xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
    <Invoice xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
             xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2"
             xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
      <cac:AccountingCustomerParty>
        <cac:Party><cac:Contact><cbc:ElectronicMail>cliente@century-media.net</cbc:ElectronicMail></cac:Contact></cac:Party>
      </cac:AccountingCustomerParty>
      <ds:Signature><ds:Object><cbc:ElectronicMail>certificador@andesscd.com.co</cbc:ElectronicMail></ds:Object></ds:Signature>
    </Invoice>
    """

    assert extract_supplier_email(xml_bytes) is None


def test_extract_supplier_email_blocks_own_domain_to_avoid_loops():
    xml_bytes = b"""<?xml version="1.0" encoding="UTF-8"?>
    <Invoice xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
             xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
      <cac:AccountingSupplierParty>
        <cac:Party><cac:Contact><cbc:ElectronicMail>facturas@century-media.net</cbc:ElectronicMail></cac:Contact></cac:Party>
      </cac:AccountingSupplierParty>
    </Invoice>
    """

    assert extract_supplier_email(xml_bytes) is None


def test_extract_supplier_email_returns_none_for_malformed_xml():
    assert extract_supplier_email(b"<Invoice><broken></Invoice>") is None


def test_resolve_alternate_recipient_xml_gana_sobre_sheet():
    catalog = [_record("ACME", "123456789", "sheet@acme.test")]

    result = resolve_alternate_recipient(
        _invoice_with_supplier_email("xml@proveedor.test"),
        DIAN_SUBJECT,
        catalog,
        "fallback@century-media.net",
    )

    assert result == ("xml@proveedor.test", "xml")


def test_resolve_alternate_recipient_xml_sin_email_cae_a_sheet():
    catalog = [_record("ACME", "123456789", "sheet@acme.test")]

    result = resolve_alternate_recipient(XML_WITHOUT_SUPPLIER_EMAIL, DIAN_SUBJECT, catalog, "")

    assert result == ("sheet@acme.test", "sheet")


def test_resolve_alternate_recipient_sheet_sin_xml():
    catalog = [_record("ACME", "123456789", "sheet@acme.test")]

    result = resolve_alternate_recipient(None, DIAN_SUBJECT, catalog, "")

    assert result == ("sheet@acme.test", "sheet")


def test_resolve_alternate_recipient_sin_xml_ni_sheet_cae_a_fallback():
    result = resolve_alternate_recipient(None, DIAN_SUBJECT, [], "fallback@century-media.net")

    assert result == ("fallback@century-media.net", "fallback")


def test_resolve_alternate_recipient_sheet_dominio_interno_cae_a_fallback():
    catalog = [_record("ACME", "123456789", "gestion@century-media.net")]

    result = resolve_alternate_recipient(None, DIAN_SUBJECT, catalog, "fallback@century-media.net")

    assert result == ("fallback@century-media.net", "fallback")


def test_resolve_alternate_recipient_todo_vacio():
    assert resolve_alternate_recipient(None, "Factura marzo", [], "") == (None, "sin_destinatario")
