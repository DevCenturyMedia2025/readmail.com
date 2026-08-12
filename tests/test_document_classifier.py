"""
Tests de la clasificacion documental (app/services/document_classifier.py).

Los textos de prueba imitan muestras reales (nombres de archivo y texto
extraido) sin I/O. Se congela el comportamiento actual del clasificador.
"""

from app.models import UnifiedFile
from app.services.document_classifier import (
    CUENTA_COBRO_REQUIRED_DOCS,
    classify_document_type,
    classify_document_type_with_method,
    contains_note_credit_text,
    detect_ok_compras,
    detect_order,
    is_note_credit_by_filename,
    is_note_credit_by_text,
    is_order_file,
    validate_cuenta_cobro_package,
)


def pdf(name, text=""):
    return UnifiedFile(name=name, mime_type="application/pdf", data=b"", source="test", extracted_text=text)


def image(name, text=""):
    return UnifiedFile(name=name, mime_type="image/jpeg", data=b"", source="test", extracted_text=text)


def test_nota_credito_con_tilde():
    assert contains_note_credit_text("Adjunto la Nota Credito No. 123") is True


def test_nota_de_credito():
    assert contains_note_credit_text("nota de credito por devolucion") is True


def test_credit_note_en_ingles():
    assert contains_note_credit_text("Please find the Credit Note attached") is True


def test_texto_sin_nota_credito():
    assert contains_note_credit_text("factura electronica adjunta") is False


def test_texto_vacio_no_es_nota_credito():
    assert contains_note_credit_text("") is False


def test_nota_y_credito_separados_no_cuentan():
    assert contains_note_credit_text("nota importante: pago a credito") is False


def test_nota_credito_por_nombre_de_archivo():
    files = [pdf("factura.pdf"), pdf("NOTA CREDITO 456.pdf")]
    assert is_note_credit_by_filename(files) is True
    assert is_note_credit_by_text(files) is False


def test_nota_credito_por_texto_extraido():
    files = [pdf("documento.pdf", text="se emite nota credito por ajuste")]
    assert is_note_credit_by_filename(files) is False
    assert is_note_credit_by_text(files) is True


def test_detect_order_por_frase_completa():
    files = [pdf("adjunto.pdf", text="Se anexa la orden de compra correspondiente")]
    assert detect_order(files) is False


def test_detect_order_por_numero():
    files = [pdf("OC-2024-001.pdf", text="OC: 2024-001")]
    assert detect_order(files) is True


def test_detect_order_negativo():
    files = [pdf("factura.pdf", text="factura electronica de venta")]
    assert detect_order(files) is False


def test_is_order_file_por_clasificacion():
    file_obj = pdf("orden de compra 55.pdf", text="subtotal 100 autorizado por gerencia")
    assert is_order_file(file_obj) is True


def test_ok_compras_default():
    files = [pdf("correo.pdf", text="El area da el visto bueno para radicacion")]
    assert detect_ok_compras(files) is True


def test_ok_compras_ignora_tildes_y_espacios():
    files = [pdf("correo.pdf", text="APROBACION   DE   COMPRAS confirmada")]
    assert detect_ok_compras(files) is True


def test_ok_compras_negativo():
    files = [pdf("correo.pdf", text="pendiente de revision por el area")]
    assert detect_ok_compras(files) is False


def test_ok_compras_con_patrones_inyectados():
    files = [pdf("correo.pdf", text="sello especial interno")]
    assert detect_ok_compras(files, patterns=["sello especial interno"]) is True


def test_clasifica_cuenta_cobro_por_nombre():
    file_obj = pdf("cuenta de cobro enero.pdf")
    tipo, metodo = classify_document_type_with_method(file_obj)
    assert tipo == "cuenta_cobro"
    assert metodo == "nombre"


def test_clasifica_rut_por_contenido():
    file_obj = pdf(
        "documento1.pdf",
        text="registro unico tributario DIAN NIT 900123456 actividad economica 7310 formulario del registro muisca",
    )
    tipo, metodo = classify_document_type_with_method(file_obj)
    assert tipo == "rut"
    assert metodo == "contenido"


def test_clasifica_cedula_en_imagen():
    file_obj = image(
        "escaneo.jpg",
        text="republica de colombia cedula de ciudadania fecha de nacimiento lugar de expedicion",
    )
    assert classify_document_type(file_obj) == "cedula"


def test_rut_no_clasifica_en_imagen():
    file_obj = image("escaneo.jpg", text="registro unico tributario DIAN NIT actividad economica")
    assert classify_document_type(file_obj) != "rut"


def test_certificado_bancario_por_contenido():
    file_obj = pdf(
        "cert.pdf",
        text="certificado bancario bancolombia certifica cuenta de ahorros tipo de producto nro de producto",
    )
    assert classify_document_type(file_obj) == "certificado_bancario"


def test_documento_ambiguo_devuelve_none():
    file_obj = pdf("varios.pdf", text="texto generico sin palabras clave de ningun documento")
    tipo, metodo = classify_document_type_with_method(file_obj)
    assert tipo is None
    assert metodo == "desconocido"


def paquete_completo():
    return [
        pdf("cuenta de cobro enero.pdf", text="debe a la suma de un millon por concepto de servicios"),
        image("cedula.jpg", text="republica de colombia cedula de ciudadania fecha de nacimiento"),
        pdf("rut.pdf", text="registro unico tributario DIAN NIT actividad economica muisca"),
        pdf("certificado bancario.pdf", text="bancolombia certifica cuenta de ahorros nro de producto"),
        pdf("orden de compra 10.pdf", text="subtotal autorizado por elaborado por century media"),
    ]


def test_paquete_completo():
    result = validate_cuenta_cobro_package(paquete_completo())
    assert result["estado"] == "completo"
    assert result["mensaje"] == "Recibido archivos completos"
    assert result["faltantes"] == []
    assert result["desconocidos"] == []
    assert set(result["identificados"].keys()) == set(CUENTA_COBRO_REQUIRED_DOCS)


def test_paquete_cuenta_cobro_acepta_orden_por_nombre_sin_texto():
    files = paquete_completo()
    files[-1] = pdf("orden de compra.pdf")

    result = validate_cuenta_cobro_package(files)

    assert result["estado"] == "completo"
    assert result["faltantes"] == []
    assert result["identificados"]["orden_compra"] == ["orden de compra.pdf (nombre)"]


def test_paquete_incompleto():
    files = paquete_completo()[:2]
    result = validate_cuenta_cobro_package(files)
    assert result["estado"] == "incompleto"
    assert result["mensaje"] == "Faltan documentos obligatorios"
    assert "rut" in result["faltantes"]
    assert "certificado_bancario" in result["faltantes"]
    assert "orden_compra" in result["faltantes"]


def test_paquete_completo_con_un_desconocido():
    files = paquete_completo()[:4]
    files.append(pdf("misterioso.pdf", text="texto que no clasifica en nada"))
    result = validate_cuenta_cobro_package(files)
    assert result["estado"] == "completo_con_desconocido"
    assert result["mensaje"] == "Recibido archivos completos con un documento no identificado"
    assert result["desconocidos"] == ["misterioso.pdf"]


def test_paquete_detecta_duplicados():
    files = paquete_completo()
    files.append(pdf("cuenta de cobro febrero.pdf", text="debe a la suma de dos millones"))
    result = validate_cuenta_cobro_package(files)
    assert "cuenta_cobro" in result["duplicados"]
    assert len(result["duplicados"]["cuenta_cobro"]) == 2


def test_paquete_ignora_archivos_no_pdf_ni_imagen():
    files = paquete_completo()
    files.append(UnifiedFile(name="datos.xml", mime_type="application/xml", data=b"", source="test"))
    result = validate_cuenta_cobro_package(files)
    assert result["estado"] == "completo"
    assert "datos.xml" not in result["desconocidos"]
