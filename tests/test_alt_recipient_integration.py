import pytest

from app.models import ClientMatchResult, ClientRecord
from app.utils.text import normalize_alnum, normalize_nit
import reademail
from reademail import UnifiedFile, decide_rejection_recipient


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

    assert result == (None, "sin_destinatario", False)


def _run_rejected_message(
    monkeypatch,
    sender,
    subject,
    enabled,
    with_xml=False,
    send_error=None,
    attachment_size=0,
    label_error=None,
    approved=False,
):
    payload = {
        "headers": [
            {"name": "From", "value": sender},
            {"name": "Subject", "value": subject},
        ]
    }
    files = [UnifiedFile("factura.pdf", "application/pdf", b"", "test")]
    if with_xml:
        files.append(UnifiedFile("factura.xml", "application/xml", _invoice_with_supplier_email("proveedor@example.com"), "test"))

    attachment = {"attachmentId": "attachment-1", "filename": "factura.pdf"} if attachment_size else {}
    state = {}
    saved_states = []
    calls = {"reply": [], "new": [], "forward": [], "state": state, "saved_states": saved_states}

    def record_reply(*args):
        calls["reply"].append(args)
        if send_error:
            raise send_error

    def record_label(*args, **kwargs):
        if label_error:
            raise label_error

    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", enabled)
    monkeypatch.setattr(reademail, "ALT_FALLBACK_EMAIL", "fallback@example.com")
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda current, account_id=None: saved_states.append(current.copy()))
    monkeypatch.setattr(reademail, "safe_get_message_full", lambda service, message_id: {"payload": payload, "snippet": ""})
    monkeypatch.setattr(reademail, "collect_attachments", lambda payload: [attachment])
    monkeypatch.setattr(reademail, "build_unified_files", lambda service, message_id, attachments: (files, [], []))
    monkeypatch.setattr(reademail, "gmail_download_attachment_bytes", lambda *args: b"x" * attachment_size)
    monkeypatch.setattr(reademail, "apply_single_status_label", lambda *args, **kwargs: None)
    monkeypatch.setattr(reademail, "add_status_label", record_label)
    monkeypatch.setattr(reademail, "send_reply_email", record_reply)
    monkeypatch.setattr(reademail, "send_new_email", lambda *args: calls["new"].append(args))
    monkeypatch.setattr(reademail, "send_forward_with_attachments", lambda *args: calls["forward"].append(args))
    if approved:
        client = _record("ACME", "123456789", "contacto@acme.test")
        monkeypatch.setattr(reademail, "validate_electronic_invoice_minimum", lambda *args: [])
        monkeypatch.setattr(reademail, "detect_order", lambda pdfs: True)
        monkeypatch.setattr(reademail, "identify_client_in_order_pdfs", lambda pdfs, catalog: ClientMatchResult(record=client))
        monkeypatch.setattr(reademail, "detect_ok_compras", lambda pdfs: True)

    reademail.process_message(object(), object(), "message-1", [])
    return calls


def test_no_reply_sin_xml_asunto_normal_flag_off_omite_respuesta(monkeypatch):
    calls = _run_rejected_message(monkeypatch, "noreply@example.com", "Factura marzo", enabled=False)

    assert calls["reply"] == []
    assert calls["new"] == []
    assert calls["forward"] == []


def test_humano_asunto_dian_flag_off_responde_al_hilo(monkeypatch):
    calls = _run_rejected_message(monkeypatch, "juan@empresa.com", DIAN_SUBJECT, enabled=False)

    assert len(calls["reply"]) == 1
    assert calls["new"] == []
    assert calls["forward"] == []


def test_humano_asunto_dian_flag_on_desvia(monkeypatch):
    calls = _run_rejected_message(monkeypatch, "juan@empresa.com", DIAN_SUBJECT, enabled=True, with_xml=True)

    assert calls["reply"] == []
    assert len(calls["new"]) == 1


def test_no_reply_sin_xml_flag_on_usa_flujo_de_desvio(monkeypatch):
    calls = _run_rejected_message(monkeypatch, "noreply@example.com", "Factura marzo", enabled=True)

    assert calls["reply"] == []
    assert len(calls["new"]) == 1


@pytest.mark.parametrize(
    ("sender", "subject", "with_xml", "expected_replies"),
    [
        ("noreply@example.com", DIAN_SUBJECT, True, 0),
        ("noreply@example.com", "Factura marzo", False, 0),
        ("juan@empresa.com", DIAN_SUBJECT, True, 1),
        ("juan@empresa.com", DIAN_SUBJECT, False, 1),
        ("juan@empresa.com", "Factura marzo", True, 1),
    ],
)
def test_flag_off_conserva_los_cinco_escenarios_historicos(
    monkeypatch, sender, subject, with_xml, expected_replies
):
    calls = _run_rejected_message(monkeypatch, sender, subject, enabled=False, with_xml=with_xml)

    assert len(calls["reply"]) == expected_replies
    assert calls["new"] == []
    assert calls["forward"] == []


def test_fallo_de_envio_marca_replied_y_no_deja_reintento(monkeypatch, caplog):
    calls = _run_rejected_message(
        monkeypatch,
        "juan@empresa.com",
        "Factura marzo",
        enabled=False,
        send_error=RuntimeError("SMTP caído"),
    )

    assert calls["state"]["replied_message_ids"] == ["message-1"]
    assert calls["state"]["processed_message_ids"] == ["message-1"]
    assert calls["saved_states"][-1]["replied_message_ids"] == ["message-1"]
    assert "❌ Falló envío de rechazo a juan@empresa.com: SMTP caído" in caplog.text

    reademail.process_message(object(), object(), "message-1", [])
    assert len(calls["reply"]) == 1


def test_adjuntos_mayores_a_20_mb_envian_solo_texto_con_nota(monkeypatch, caplog):
    calls = _run_rejected_message(
        monkeypatch,
        "juan@empresa.com",
        DIAN_SUBJECT,
        enabled=True,
        with_xml=True,
        attachment_size=20 * 1024 * 1024 + 1,
    )

    assert calls["forward"] == []
    assert len(calls["new"]) == 1
    assert "La factura no se reenvió por superar el tamaño permitido." in calls["new"][0][3]
    assert "Adjuntos de rechazo superan 20 MB" in caplog.text


def test_fallo_de_etiquetado_fallback_no_reenvia_en_segundo_ciclo(monkeypatch, caplog):
    calls = _run_rejected_message(
        monkeypatch,
        "noreply@example.com",
        "Factura marzo",
        enabled=True,
        label_error=RuntimeError("Gmail labels caído"),
    )

    assert len(calls["new"]) == 1
    assert calls["state"]["replied_message_ids"] == ["message-1"]
    assert "❌ Falló etiquetado: Gmail labels caído" in caplog.text

    reademail.process_message(object(), object(), "message-1", [])
    assert len(calls["new"]) == 1


def test_fallo_de_envio_de_aprobacion_no_responde_en_segundo_ciclo(monkeypatch, caplog):
    calls = _run_rejected_message(
        monkeypatch,
        "juan@empresa.com",
        "Factura marzo",
        enabled=False,
        with_xml=True,
        send_error=RuntimeError("SMTP caído"),
        approved=True,
    )

    assert len(calls["reply"]) == 1
    assert calls["state"]["replied_message_ids"] == ["message-1"]
    assert calls["state"]["processed_message_ids"] == ["message-1"]
    assert "❌ Falló envío de aprobación a juan@empresa.com: SMTP caído" in caplog.text

    reademail.process_message(object(), object(), "message-1", [])
    assert len(calls["reply"]) == 1
