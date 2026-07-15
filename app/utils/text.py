"""
Utilidades de texto para ReadMail.

Este modulo replica helpers puros que hoy viven en reademail.py.
Por ahora no cambia el comportamiento del sistema principal.
"""

import re
import unicodedata
from typing import List


EMAIL_RE = re.compile(r"^[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+$")


def strip_accents(value: str) -> str:
    if not value:
        return ""
    return "".join(
        ch
        for ch in unicodedata.normalize("NFKD", value)
        if not unicodedata.combining(ch)
    )


def normalize_text(value: str) -> str:
    value = strip_accents(value or "")
    value = value.lower()
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_alnum(value: str) -> str:
    value = strip_accents(value or "")
    value = value.lower()
    return re.sub(r"[^a-z0-9]", "", value)


def normalize_nit(value: str) -> str:
    return re.sub(r"\D", "", value or "")


def ensure_list(value) -> List:
    return value if isinstance(value, list) else []
