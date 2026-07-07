"""
Parsing puro de mensajes Gmail.

Este modulo espeja funciones de reademail.py (lineas ~273-350):
decode_body, get_header, html_to_text, extract_plain_text,
extract_sender_email, is_no_reply_sender.

Son funciones sin efectos secundarios: no llaman a gmail_service, no
leen archivos ni credenciales. Todavia no esta conectado a
reademail.py.
"""

import base64
import html
import re
from email.utils import parseaddr
from typing import Dict, Optional

from app.utils.text import ensure_list


def decode_body(data: Optional[str]) -> str:
    if not data:
        return ""
    try:
        missing_padding = len(data) % 4
        if missing_padding:
            data += "=" * (4 - missing_padding)
        decoded_bytes = base64.urlsafe_b64decode(data)
        return decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def get_header(payload: Dict, name: str) -> str:
    target = name.lower()
    for header in ensure_list(payload.get("headers")):
        if (header.get("name", "") or "").lower() == target:
            return header.get("value", "") or ""
    return ""


def html_to_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p\s*>", "\n", value)
    value = re.sub(r"(?i)</div\s*>", "\n", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"[ \t\r\f\v]+", " ", value)
    value = re.sub(r"\n\s+", "\n", value)
    return value.strip()


def extract_plain_text(payload: Dict) -> str:
    if not payload:
        return ""

    body = payload.get("body", {}) or {}
    mime_type = payload.get("mimeType", "")
    if body.get("data") and mime_type == "text/html":
        return html_to_text(decode_body(body.get("data")))
    if body.get("data") and (mime_type == "text/plain" or not payload.get("parts")):
        return decode_body(body.get("data"))

    html_fallback = ""
    for part in ensure_list(payload.get("parts")):
        part_mime_type = part.get("mimeType", "")
        if part_mime_type == "text/plain":
            txt = decode_body((part.get("body", {}) or {}).get("data"))
            if txt:
                return txt
        if part_mime_type == "text/html":
            txt = html_to_text(decode_body((part.get("body", {}) or {}).get("data")))
            if txt and not html_fallback:
                html_fallback = txt
            continue
        nested = extract_plain_text(part)
        if nested:
            return nested
    return html_fallback


def extract_sender_email(from_header: str) -> Optional[str]:
    _, email_addr = parseaddr(from_header or "")
    return email_addr or None


_NO_REPLY_RE = re.compile(
    r"(no.?reply|noreply|bounce|mailer-daemon|postmaster|notifications?@|donotreply|do-not-reply)",
    re.IGNORECASE,
)


def is_no_reply_sender(email: str) -> bool:
    return bool(_NO_REPLY_RE.search(email or ""))
