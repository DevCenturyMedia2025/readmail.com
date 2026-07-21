import base64
from email import policy
from email.parser import BytesParser

from reademail import create_forward_email


def _decode_message(raw_message):
    decoded = base64.urlsafe_b64decode(raw_message.encode("utf-8"))
    return BytesParser(policy=policy.default).parsebytes(decoded)


def test_create_forward_email_con_adjunto():
    raw_message = create_forward_email(
        "proveedor@example.com",
        "Rechazo de facturación - RAD-123",
        "La factura fue rechazada.",
        [
            {
                "filename": "factura_test.pdf",
                "mime_type": "application/pdf",
                "data": b"%PDF-fake",
            }
        ],
    )

    message = _decode_message(raw_message)
    parts = list(message.iter_parts())

    assert message.is_multipart()
    assert message.get_content_type() == "multipart/mixed"
    assert parts[0].get_content().strip() == "La factura fue rechazada."
    assert parts[1].get_content_disposition() == "attachment"
    assert parts[1].get_filename() == "factura_test.pdf"
    assert parts[1].get_payload(decode=True) == b"%PDF-fake"


def test_create_forward_email_sin_adjuntos():
    raw_message = create_forward_email(
        "proveedor@example.com",
        "Rechazo de facturación - RAD-123",
        "La factura fue rechazada.",
        [],
    )

    message = _decode_message(raw_message)
    parts = list(message.iter_parts())

    assert message.is_multipart()
    assert message.get_content_type() == "multipart/mixed"
    assert len(parts) == 1
    assert parts[0].get_content().strip() == "La factura fue rechazada."
