import base64

import pytest

import reademail
from reademail import ClientMatchResult, ClientRecord, UnifiedFile


class _Executable:
    def execute(self):
        return {}


class _Messages:
    def __init__(self, modifications):
        self.modifications = modifications

    def modify(self, **kwargs):
        self.modifications.append(kwargs)
        return _Executable()


class _Users:
    def __init__(self, modifications):
        self._messages = _Messages(modifications)

    def messages(self):
        return self._messages


class _GmailService:
    def __init__(self, modifications):
        self._users = _Users(modifications)

    def users(self):
        return self._users


def _pdf(name, text=""):
    return UnifiedFile(name, "application/pdf", b"%PDF-fake", "test", text)


def _run_fe(
    monkeypatch,
    pdfs,
    *,
    client_identified=True,
    modo_pruebas=False,
    compras_email="",
    forward_error=None,
    attachment_size=32,
):
    state = {}
    calls = {
        "labels": [],
        "replies": [],
        "forwards": [],
        "new_emails": [],
        "modifications": [],
        "saved_states": [],
    }
    gmail_service = _GmailService(calls["modifications"])
    body_text = "Cuerpo original de la factura."
    payload = {
        "mimeType": "text/plain",
        "body": {
            "data": base64.urlsafe_b64encode(body_text.encode("utf-8")).decode("ascii")
        },
        "headers": [
            {"name": "From", "value": "proveedor@example.test"},
            {"name": "To", "value": "facturas@example.test"},
            {"name": "Date", "value": "Thu, 20 Aug 2026 10:00:00 -0500"},
            {"name": "Subject", "value": "Factura Cliente Demo"},
        ],
    }
    attachments = [
        {
            "attachmentId": "attachment-1",
            "filename": "factura-original.pdf",
            "mimeType": "application/pdf",
        }
    ]
    xml = UnifiedFile("factura.xml", "application/xml", b"<Invoice />", "test")
    client = ClientRecord("Cliente Demo", "clientedemo") if client_identified else None

    def record_forward(*args):
        calls["forwards"].append(args)
        if forward_error:
            raise forward_error

    monkeypatch.setattr(reademail, "MODO_PRUEBAS", modo_pruebas)
    monkeypatch.setattr(reademail, "COMPRAS_EMAIL", compras_email)
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(
        reademail,
        "save_state",
        lambda current, account_id=None: calls["saved_states"].append(current.copy()),
    )
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"payload": payload, "snippet": ""},
    )
    monkeypatch.setattr(reademail, "collect_attachments", lambda current_payload: attachments)
    monkeypatch.setattr(
        reademail,
        "build_unified_files",
        lambda service, message_id, current_attachments: ([*pdfs, xml], [], []),
    )
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        reademail,
        "identify_client_in_order_pdfs",
        lambda current_pdfs, catalog: ClientMatchResult(record=client),
    )
    monkeypatch.setattr(reademail, "identify_client_from_fields", lambda *args: client)
    monkeypatch.setattr(reademail, "identify_client", lambda *args, **kwargs: client)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: calls["labels"].append(
            (label, archive)
        ),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *args: calls["replies"].append(args))
    monkeypatch.setattr(reademail, "send_forward_with_attachments", record_forward)
    monkeypatch.setattr(
        reademail,
        "send_new_email",
        lambda *args: calls["new_emails"].append(args),
    )
    monkeypatch.setattr(
        reademail,
        "gmail_download_attachment_bytes",
        lambda *args: b"x" * attachment_size,
    )

    catalog = [client] if client else []
    reademail.process_message(gmail_service, object(), "message-fe", catalog)
    calls["state"] = state
    calls["gmail_service"] = gmail_service
    return calls


def test_fe_con_un_solo_pdf_orden_y_ok_se_aprueba_sin_validar_minimo(monkeypatch):
    combined_pdf = _pdf("orden de compra.pdf", "Cliente Demo. OK compras para radicar.")

    calls = _run_fe(monkeypatch, [combined_pdf])

    assert calls["labels"] == [(reademail.LABEL_APPROVED_NAME, reademail.ARCHIVE_APPROVED)]
    assert len(calls["replies"]) == 1


@pytest.mark.parametrize(
    ("pdf", "expected_reason", "unexpected_reason"),
    [
        (
            _pdf("factura.pdf", "OK compras"),
            "No se detectó orden de compra en nombre ni texto de los PDF.",
            "No se detectó OK de compras dentro de los PDF.",
        ),
        (
            _pdf("orden de compra.pdf"),
            "No se detectó OK de compras dentro de los PDF.",
            "No se detectó orden de compra en nombre ni texto de los PDF.",
        ),
    ],
)
def test_fe_real_rechaza_solo_por_orden_u_ok(
    monkeypatch,
    pdf,
    expected_reason,
    unexpected_reason,
):
    calls = _run_fe(monkeypatch, [pdf], modo_pruebas=False)

    assert calls["labels"] == [(reademail.LABEL_REJECTED_NAME, reademail.ARCHIVE_REJECTED)]
    assert len(calls["replies"]) == 1
    rejection_body = calls["replies"][0][4]
    assert expected_reason in rejection_body
    assert unexpected_reason not in rejection_body
    assert "Factura electrónica: archivos incompletos" not in rejection_body
    assert "No se logró identificar el cliente" not in rejection_body


@pytest.mark.parametrize(
    ("pdf", "expected_missing"),
    [
        (_pdf("factura.pdf", "OK compras"), "orden de compra"),
        (_pdf("orden de compra.pdf"), "OK de compras"),
    ],
)
def test_fe_pruebas_reenvia_faltante_a_compras(monkeypatch, caplog, pdf, expected_missing):
    caplog.set_level("INFO")
    calls = _run_fe(
        monkeypatch,
        [pdf],
        modo_pruebas=True,
        compras_email="compras@example.test",
    )

    assert calls["labels"] == []
    assert calls["replies"] == []
    assert calls["new_emails"] == []
    assert len(calls["forwards"]) == 1
    _, destination, subject, body, forwarded_attachments = calls["forwards"][0]
    assert destination == "compras@example.test"
    assert subject.startswith("Falta documentación - Factura Cliente Demo (ID: ")
    assert f"- {expected_missing}" in body
    assert "Por favor responder este correo adjuntando el archivo requerido." in body
    assert "---------- Mensaje original ----------" in body
    assert "Cuerpo original de la factura." in body
    assert forwarded_attachments == [
        {
            "filename": "factura-original.pdf",
            "mime_type": "application/pdf",
            "data": b"x" * 32,
        }
    ]
    assert calls["modifications"] == [
        {
            "userId": "me",
            "id": "message-fe",
            "body": {"addLabelIds": ["UNREAD"]},
        }
    ]
    assert calls["state"]["replied_message_ids"] == ["message-fe"]
    assert calls["state"]["processed_message_ids"] == ["message-fe"]
    assert "📨 Reenviado a Compras (compras@example.test) por falta de:" in caplog.text


def test_fe_sin_cliente_va_a_revision_manual_sin_responder(monkeypatch):
    combined_pdf = _pdf("orden de compra.pdf", "OK compras")

    calls = _run_fe(monkeypatch, [combined_pdf], client_identified=False)

    assert calls["labels"] == [(reademail.LABEL_REVIEW_NAME, reademail.ARCHIVE_REVIEW)]
    assert calls["replies"] == []
    assert calls["forwards"] == []
    assert calls["state"]["processed_message_ids"] == ["message-fe"]
    assert "replied_message_ids" not in calls["state"]


def test_fe_pruebas_sin_compras_email_rechaza_y_loguea(monkeypatch, caplog):
    missing_ok_pdf = _pdf("orden de compra.pdf")

    calls = _run_fe(monkeypatch, [missing_ok_pdf], modo_pruebas=True, compras_email="")

    assert calls["labels"] == [(reademail.LABEL_REJECTED_NAME, reademail.ARCHIVE_REJECTED)]
    assert len(calls["replies"]) == 1
    assert calls["forwards"] == []
    assert "MODO_PRUEBAS activo pero COMPRAS_EMAIL está vacío" in caplog.text


def test_fallo_de_reenvio_a_compras_marca_estado_y_no_reintenta(monkeypatch, caplog):
    missing_ok_pdf = _pdf("orden de compra.pdf")
    calls = _run_fe(
        monkeypatch,
        [missing_ok_pdf],
        modo_pruebas=True,
        compras_email="compras@example.test",
        forward_error=RuntimeError("Gmail caído"),
    )

    assert len(calls["forwards"]) == 1
    assert calls["state"]["replied_message_ids"] == ["message-fe"]
    assert calls["state"]["processed_message_ids"] == ["message-fe"]
    assert "❌ Falló reenvío a Compras" in caplog.text

    reademail.process_message(calls["gmail_service"], object(), "message-fe", [])
    assert len(calls["forwards"]) == 1
