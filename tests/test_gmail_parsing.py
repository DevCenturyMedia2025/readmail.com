import base64

from app.gmail.parsing import (
    decode_body,
    extract_plain_text,
    extract_sender_email,
    get_header,
    html_to_text,
    is_no_reply_sender,
)


def _b64(text: str) -> str:
    return base64.urlsafe_b64encode(text.encode("utf-8")).decode("utf-8")


def test_decode_body_valid_base64():
    assert decode_body(_b64("hola mundo")) == "hola mundo"


def test_decode_body_handles_missing_padding():
    encoded = _b64("x").rstrip("=")
    assert decode_body(encoded) == "x"


def test_decode_body_empty_input():
    assert decode_body(None) == ""
    assert decode_body("") == ""


def test_decode_body_unrecoverable_input_returns_empty():
    assert decode_body("\x00\x01") == ""


def test_get_header_case_insensitive():
    payload = {"headers": [{"name": "Subject", "value": "Hola"}]}
    assert get_header(payload, "subject") == "Hola"


def test_get_header_missing_returns_empty():
    assert get_header({"headers": []}, "subject") == ""
    assert get_header({}, "subject") == ""


def test_html_to_text_strips_tags_and_scripts():
    html_value = "<html><body><script>evil()</script><p>Hola</p><br>Mundo</body></html>"
    assert html_to_text(html_value) == "Hola\nMundo"


def test_html_to_text_empty():
    assert html_to_text("") == ""


def test_extract_plain_text_prefers_plain_part():
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/plain", "body": {"data": _b64("texto plano")}},
            {"mimeType": "text/html", "body": {"data": _b64("<p>html</p>")}},
        ],
    }
    assert extract_plain_text(payload) == "texto plano"


def test_extract_plain_text_falls_back_to_html():
    payload = {
        "mimeType": "multipart/alternative",
        "parts": [
            {"mimeType": "text/html", "body": {"data": _b64("<p>solo html</p>")}},
        ],
    }
    assert extract_plain_text(payload) == "solo html"


def test_extract_plain_text_empty_payload():
    assert extract_plain_text({}) == ""
    assert extract_plain_text(None) == ""


def test_extract_sender_email_parses_display_name():
    assert extract_sender_email("Juan Perez <juan@example.com>") == "juan@example.com"


def test_extract_sender_email_empty_returns_none():
    assert extract_sender_email("") is None


def test_is_no_reply_sender_true_cases():
    assert is_no_reply_sender("no-reply@example.com") is True
    assert is_no_reply_sender("notifications@example.com") is True
    assert is_no_reply_sender("notificaciones@int.lafactura.co") is True
    assert is_no_reply_sender("avisos@example.com") is True
    assert is_no_reply_sender("alertas@example.com") is True


def test_is_no_reply_sender_false_case():
    assert is_no_reply_sender("juan@example.com") is False
    assert is_no_reply_sender("juan@empresa.com") is False
