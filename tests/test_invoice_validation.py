from app.services.invoice_validation import (
    classify_invoice_type,
    format_missing_documents,
    validate_electronic_invoice_minimum,
    validate_pdf_minimum,
)


def test_classify_invoice_type_electronic_when_has_xml():
    assert classify_invoice_type(1) == "FACTURA ELECTRONICA"
    assert classify_invoice_type(5) == "FACTURA ELECTRONICA"


def test_classify_invoice_type_cuenta_cobro_when_no_xml():
    assert classify_invoice_type(0) == "CUENTA DE COBRO"


def test_validate_electronic_invoice_minimum_ok():
    assert validate_electronic_invoice_minimum(pdf_count=3, xml_count=1) == []


def test_validate_electronic_invoice_minimum_missing_pdf_and_xml():
    errors = validate_electronic_invoice_minimum(pdf_count=1, xml_count=0)
    assert len(errors) == 2
    assert "archivos incompletos" in errors[0]
    assert "falta 1 XML" in errors[1]


def test_validate_pdf_minimum_cuenta_cobro_incomplete():
    msg = validate_pdf_minimum("CUENTA DE COBRO", pdf_count=2)
    assert msg is not None
    assert "incompletos" in msg


def test_validate_pdf_minimum_cuenta_cobro_ok():
    assert validate_pdf_minimum("CUENTA DE COBRO", pdf_count=4) is None


def test_validate_pdf_minimum_not_applicable_to_electronic():
    assert validate_pdf_minimum("FACTURA ELECTRONICA", pdf_count=0) is None


def test_format_missing_documents_known_labels():
    result = format_missing_documents(["rut", "cedula"])
    assert result == ["RUT", "cédula"]


def test_format_missing_documents_unknown_label_falls_back_to_readable_text():
    result = format_missing_documents(["algo_raro"])
    assert result == ["algo raro"]
