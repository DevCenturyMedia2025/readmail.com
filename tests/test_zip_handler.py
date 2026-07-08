"""
Tests del manejo seguro de ZIP (app/services/zip_handler.py).

Todos los ZIPs se construyen en memoria con zipfile + io.BytesIO.
Los limites se prueban con valores pequenos gracias a los parametros.
"""

import io
import zipfile
from unittest.mock import patch

from app.models import UnifiedFile
from app.services.zip_handler import (
    analyze_zip_bytes,
    extract_zip_files,
    is_ignored_zip_member,
    is_safe_zip_member,
    is_zip_attachment,
)


def build_zip(entries):
    """Construye un ZIP en memoria: entries = [(nombre, bytes), ...]."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in entries:
            zf.writestr(name, data)
    return buffer.getvalue()


def test_is_zip_attachment_por_extension():
    assert is_zip_attachment({"filename": "docs.ZIP", "mimeType": ""}) is True


def test_is_zip_attachment_por_mimetype():
    assert is_zip_attachment({"filename": "adjunto", "mimeType": "application/x-zip-compressed"}) is True


def test_is_zip_attachment_negativo():
    assert is_zip_attachment({"filename": "factura.pdf", "mimeType": "application/pdf"}) is False


def test_ruta_normal_es_segura():
    assert is_safe_zip_member("carpeta/factura.pdf") is True


def test_ruta_vacia_no_es_segura():
    assert is_safe_zip_member("") is False


def test_zip_slip_no_es_seguro():
    assert is_safe_zip_member("../fuera.txt") is False
    assert is_safe_zip_member("carpeta/../../fuera.txt") is False


def test_ruta_absoluta_no_es_segura():
    assert is_safe_zip_member("/etc/passwd") is False


def test_backslash_se_normaliza():
    assert is_safe_zip_member("..\\fuera.txt") is False


def test_macosx_se_ignora():
    assert is_ignored_zip_member("__MACOSX/factura.pdf") is True


def test_appledouble_se_ignora():
    assert is_ignored_zip_member("carpeta/._factura.pdf") is True


def test_ds_store_se_ignora():
    assert is_ignored_zip_member("carpeta/.DS_Store") is True


def test_archivo_normal_no_se_ignora():
    assert is_ignored_zip_member("carpeta/factura.pdf") is False


def test_zip_valido_simple():
    data = build_zip([("factura.pdf", b"%PDF-fake"), ("factura.xml", b"<xml/>")])
    result = analyze_zip_bytes("docs.zip", data)

    assert result["ok"] is True
    assert result["error"] is None
    assert result["pdf_count"] == 1
    assert result["xml_count"] == 1
    assert len(result["files"]) == 2


def test_zip_none_es_vacio():
    result = analyze_zip_bytes("docs.zip", None)
    assert result["ok"] is False
    assert result["error"] == "ZIP vacio"


def test_zip_corrupto():
    result = analyze_zip_bytes("docs.zip", b"esto no es un zip")
    assert result["ok"] is False
    assert result["error"] == "ZIP corrupto o invalido"


def test_zip_excede_bytes_maximos():
    data = build_zip([("factura.pdf", b"x" * 100)])
    result = analyze_zip_bytes("docs.zip", data, max_zip_bytes=10)
    assert result["ok"] is False
    assert result["error"].startswith("ZIP excede MAX_ZIP_BYTES")


def test_zip_con_demasiados_archivos():
    data = build_zip([(f"f{i}.pdf", b"x") for i in range(5)])
    result = analyze_zip_bytes("docs.zip", data, max_zip_files=3)
    assert result["ok"] is False
    assert result["error"].startswith("ZIP tiene demasiados archivos")


def test_zip_con_ruta_insegura():
    data = build_zip([("../fuera.txt", b"x")])
    result = analyze_zip_bytes("docs.zip", data)
    assert result["ok"] is False
    assert result["error"] == "Ruta insegura dentro del ZIP: ../fuera.txt"


def test_archivo_interno_demasiado_grande():
    data = build_zip([("grande.pdf", b"x" * 100)])
    result = analyze_zip_bytes("docs.zip", data, max_zip_single_file=50)
    assert result["ok"] is False
    assert result["error"] == "Archivo dentro del ZIP demasiado grande: grande.pdf"


def test_total_descomprimido_excedido():
    data = build_zip([("a.pdf", b"x" * 40), ("b.pdf", b"x" * 40)])
    result = analyze_zip_bytes("docs.zip", data, max_zip_total_uncompressed=60, max_zip_single_file=50)
    assert result["ok"] is False
    assert result["error"] == "ZIP excede el tamano total descomprimido permitido"


def test_zip_protegido_con_contrasena():
    data = build_zip([("secreto.pdf", b"x")])

    real_zipfile = zipfile.ZipFile
    real_info = zipfile.ZipInfo

    class FakeEncryptedZipFile:
        def __init__(self, file_obj):
            self._zip = real_zipfile(file_obj)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            self._zip.close()
            return False

        def infolist(self):
            info = real_info("secreto.pdf")
            info.file_size = 1
            info.flag_bits = 0x1
            return [info]

    with patch("app.services.zip_handler.zipfile.ZipFile", FakeEncryptedZipFile):
        result = analyze_zip_bytes("docs.zip", data)

    assert result["ok"] is False
    assert result["error"] == "ZIP protegido con contrasena"


def test_miembros_ignorados_no_cuentan():
    data = build_zip([
        ("__MACOSX/._factura.pdf", b"x"),
        (".DS_Store", b"x"),
        ("factura.pdf", b"%PDF"),
    ])
    result = analyze_zip_bytes("docs.zip", data)
    assert result["ok"] is True
    assert result["pdf_count"] == 1
    assert len(result["files"]) == 1


def test_zip_anidado_dentro_del_limite():
    inner = build_zip([("interna.pdf", b"%PDF")])
    outer = build_zip([("interno.zip", inner), ("externa.xml", b"<xml/>")])
    result = analyze_zip_bytes("docs.zip", outer)

    assert result["ok"] is True
    assert result["pdf_count"] == 1
    assert result["xml_count"] == 1
    nested_entries = [f for f in result["files"] if f["is_zip"]]
    assert len(nested_entries) == 1
    assert nested_entries[0]["nested"]["ok"] is True


def test_zip_anidado_excede_nesting():
    inner = build_zip([("interna.pdf", b"%PDF")])
    outer = build_zip([("interno.zip", inner)])
    result = analyze_zip_bytes("docs.zip", outer, max_zip_nesting=1)
    assert result["ok"] is False
    assert result["error"] == "ZIP anidado excede MAX_ZIP_NESTING en interno.zip"


def test_error_de_zip_anidado_se_propaga():
    outer = build_zip([("interno.zip", b"no es un zip")])
    result = analyze_zip_bytes("docs.zip", outer)
    assert result["ok"] is False
    assert result["error"] == "interno.zip: ZIP corrupto o invalido"


def test_extraccion_simple():
    data = build_zip([("factura.pdf", b"%PDF"), ("factura.xml", b"<xml/>")])
    result = extract_zip_files("docs.zip", data)

    assert result["ok"] is True
    files = result["files"]
    assert len(files) == 2
    by_name = {f.name: f for f in files}
    assert by_name["factura.pdf"].mime_type == "application/pdf"
    assert by_name["factura.pdf"].data == b"%PDF"
    assert by_name["factura.pdf"].source == "zip:docs.zip"
    assert by_name["factura.xml"].mime_type == "application/xml"


def test_extraccion_de_zip_anidado_con_prefijo():
    inner = build_zip([("interna.pdf", b"%PDF")])
    outer = build_zip([("interno.zip", inner)])
    result = extract_zip_files("docs.zip", outer)

    assert result["ok"] is True
    assert len(result["files"]) == 1
    assert result["files"][0].name == "interno.zip/interna.pdf"


def test_extraccion_rechaza_zip_invalido():
    result = extract_zip_files("docs.zip", b"no es un zip")
    assert result["ok"] is False
    assert result["error"] == "ZIP corrupto o invalido"
    assert result["files"] == []


def test_extraccion_asigna_mimes_de_imagen_y_generico():
    data = build_zip([("foto.JPG", b"img"), ("logo.png", b"img"), ("otro.bin", b"raw")])
    result = extract_zip_files("docs.zip", data)

    by_name = {f.name: f for f in result["files"]}
    assert by_name["foto.JPG"].mime_type == "image/jpeg"
    assert by_name["logo.png"].mime_type == "image/png"
    assert by_name["otro.bin"].mime_type == "application/octet-stream"


def test_unified_file_lower_name():
    uf = UnifiedFile(name="Carpeta/FACTURA.PDF", mime_type="application/pdf", data=b"", source="zip:x")
    assert uf.lower_name == "carpeta/factura.pdf"
