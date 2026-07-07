from app.utils.text import (
    ensure_list,
    normalize_alnum,
    normalize_nit,
    normalize_text,
    strip_accents,
)


def test_strip_accents_removes_spanish_accents():
    assert strip_accents("áéíóúñÁÉÍÓÚÑ") == "aeiounAEIOUN"


def test_normalize_text_lowercases_removes_accents_and_extra_spaces():
    assert normalize_text("  Factura   Electrónica  Ñandú  ") == "factura electronica nandu"


def test_normalize_alnum_keeps_only_letters_and_numbers():
    assert normalize_alnum("Nit: 900.123-456 Ñ") == "nit900123456n"


def test_normalize_nit_keeps_only_digits():
    assert normalize_nit("NIT 900.123.456-7") == "9001234567"


def test_ensure_list_returns_list_or_empty_list():
    existing = ["a", "b"]

    assert ensure_list(existing) is existing
    assert ensure_list(None) == []
    assert ensure_list("texto") == []
    assert ensure_list({"a": 1}) == []
