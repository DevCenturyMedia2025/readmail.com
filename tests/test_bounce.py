import base64

import pytest

import reademail
from reademail import extract_bounce_info, find_message_id_by_radicado, is_bounce_message


def _payload(from_header="proveedor@example.com", subject="Factura FV-123", body="", mime_type="text/plain"):
    encoded_body = base64.urlsafe_b64encode(body.encode("utf-8")).decode("ascii")
    return {
        "mimeType": mime_type,
        "headers": [
            {"name": "From", "value": from_header},
            {"name": "Subject", "value": subject},
        ],
        "body": {"data": encoded_body},
    }


@pytest.mark.parametrize(
    ("from_header", "subject"),
    [
        ("MAILER-DAEMON@example.com", "Delivery report"),
        ("postmaster@example.com", "Delivery report"),
        ("mailer@example.com", "Undelivered Mail Returned to Sender"),
        ("mailer@example.com", "Delivery incomplete"),
    ],
)
def test_is_bounce_message_detecta_remitente_o_asunto(from_header, subject):
    payload = _payload(from_header, subject)

    assert is_bounce_message(payload, from_header, subject) is True


def test_is_bounce_message_detecta_multipart_report():
    payload = _payload(mime_type="multipart/report")

    assert is_bounce_message(payload, "servidor@example.com", "Informe") is True


@pytest.mark.parametrize(
    ("from_header", "subject"),
    [
        ("proveedor@example.com", "Envío de documentos"),
        ("facturacion@proveedor.com", "Factura electrónica FV-123"),
    ],
)
def test_is_bounce_message_no_genera_falsos_positivos(from_header, subject):
    payload = _payload(from_header, subject)

    assert is_bounce_message(payload, from_header, subject) is False


def test_extract_bounce_info_obtiene_radicado_y_destinatario_fallido():
    body = (
        "No fue posible entregar el mensaje RAD-20260722-000003\n"
        "Final-Recipient: rfc822; info@proveedor.com"
    )
    payload = _payload("mailer-daemon@example.com", "Delivery failed", body)

    info = extract_bounce_info({"payload": payload}, payload, body)

    assert info == {
        "radicado": "RAD-20260722-000003",
        "failed_recipient": "info@proveedor.com",
    }


def test_extract_bounce_info_sin_radicado_devuelve_none():
    payload = _payload("mailer-daemon@example.com", "Delivery failed", "No such user")

    info = extract_bounce_info({"payload": payload}, payload, "No such user")

    assert info["radicado"] is None


def test_find_message_id_by_radicado_hace_busqueda_inversa():
    state = {"message_radicados": {"msg1": "RAD-20260722-000001"}}

    assert find_message_id_by_radicado(state, "RAD-20260722-000001") == "msg1"
    assert find_message_id_by_radicado(state, "RAD-20260722-999999") is None


def test_process_message_rebote_etiqueta_original_antes_del_filtro_de_adjuntos(monkeypatch):
    bounce_body = (
        "Delivery Status Notification RAD-20260722-000003\n"
        "Final-Recipient: rfc822; info@proveedor.com"
    )
    payload = _payload("MAILER-DAEMON@example.com", "Undelivered Mail", bounce_body)
    state = {"message_radicados": {"msg-original": "RAD-20260722-000003"}}
    status_labels = []
    whatsapp = []

    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda state, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"payload": payload, "snippet": bounce_body},
    )
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: status_labels.append(
            (message_id, label, archive)
        ),
    )
    monkeypatch.setattr(
        reademail,
        "send_whatsapp_alert",
        lambda message, cooldown_key=None: whatsapp.append((message, cooldown_key)),
    )

    reademail.process_message(object(), object(), "msg-rebote", [], account_id="cuenta@example.com")

    assert status_labels == [
        ("msg-original", reademail.LABEL_REVIEW_NAME, False),
        ("msg-rebote", reademail.LABEL_REVIEW_NAME, reademail.ARCHIVE_REVIEW),
    ]
    assert state["processed_message_ids"] == ["msg-rebote"]
    assert whatsapp[0][0].startswith("[Rebote] El rechazo RAD-20260722-000003 rebotó")
    assert "Cuenta: cuenta@example.com. Factura movida a Revisión Manual." in whatsapp[0][0]
    assert whatsapp[0][1] == "rebote:cuenta@example.com:RAD-20260722-000003"


def test_cooldown_key_separa_rebotes_y_silencia_repetido(monkeypatch):
    calls = []
    monkeypatch.setattr(reademail, "WHATSAPP_ALERT_ENABLED", True)
    monkeypatch.setattr(reademail, "WHATSAPP_PHONE", "000")
    monkeypatch.setattr(reademail, "WHATSAPP_APIKEY", "test")
    monkeypatch.setattr(reademail, "WHATSAPP_COOLDOWN_MIN", 15)
    monkeypatch.setattr(reademail, "_WHATSAPP_ALERT_CACHE", {})
    monkeypatch.setattr(reademail.time, "time", lambda: 1_000)
    monkeypatch.setattr(
        reademail.urllib.request,
        "urlopen",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    reademail.send_whatsapp_alert("[Rebote] Primer rebote", cooldown_key="rebote:cuenta:rad-1")
    reademail.send_whatsapp_alert("[Rebote] Segundo rebote", cooldown_key="rebote:cuenta:rad-2")
    reademail.send_whatsapp_alert("[Rebote] Primer rebote repetido", cooldown_key="rebote:cuenta:rad-1")

    assert len(calls) == 2
