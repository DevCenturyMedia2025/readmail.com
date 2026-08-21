import pytest

import reademail
from app.services import document_classifier
from reademail import ClientMatchResult, ClientRecord, UnifiedFile


ORDER_TEXT_DETECTORS = (
    reademail.contains_purchase_order_reference,
    document_classifier.contains_purchase_order_reference,
)
OK_TEXT_DETECTORS = (
    reademail.contains_ok_compras_text,
    document_classifier.contains_ok_compras_text,
)
ORDER_FILE_DETECTORS = (
    reademail.detect_order,
    document_classifier.detect_order,
)
OK_FILE_DETECTORS = (
    reademail.detect_ok_compras,
    document_classifier.detect_ok_compras,
)

OK_COMPRAS_NEGATED_MATRIX = (
    "pendiente ok compras",
    "no tiene ok compras",
    "falta ok compras",
    "sin visto bueno compras",
    "Aun no tenemos el ok compras",
    "todavia no hay ok de compras",
    "el ok compras esta pendiente",
    "el ok de compras aun no llega",
    "el ok de compras sigue en espera",
    "queda pendiente el ok de compras",
    "El ok de compras no ha llegado",
    "Falta la orden, el ok de compras y el soporte de pago",
    "Pendiente la orden y el ok de compras",
    "No cuenta con orden y ok de compras",
    "Requiere orden, ok de compras y RUT actualizado",
    "No tenemos la orden ni el ok de compras",
    "No adjuntan orden ni aprobado por compras",
)

OK_COMPRAS_APPROVED_MATRIX = (
    "OK compras",
    "ok de compras",
    "aprobado por compras",
    "Aprobado compras, no tiene observaciones",
    "Cuenta con visto bueno y no requiere firma adicional",
    "Aprobado por compras aunque falta el sello del cliente",
    "OK de compras, no requiere orden adicional",
    "Aprobado por compras pero sin fecha",
    "Revisada la factura y sus soportes. OK compras para radicar.",
)

OK_COMPRAS_NEGATED_ENUMERATIONS = (
    "Falta la orden, el ok de compras y el soporte de pago",
    "Pendiente la orden y el ok de compras",
    "No cuenta con orden y ok de compras",
    "Requiere orden, ok de compras y RUT actualizado",
)

OK_COMPRAS_LONG_PREFIX_NEGATIONS = (
    "Pendiente que el area de compras nos envie el ok de compras",
    "Falta que el proveedor adjunte el documento con el ok de compras",
    "Requiere que se anexe nuevamente el soporte con el ok de compras",
)

OK_COMPRAS_POST_NEGATION_VARIANTS = (
    "el ok de compras aun esta pendiente",
    "el ok de compras se encuentra pendiente",
)


@pytest.mark.parametrize("detector", ORDER_FILE_DETECTORS)
@pytest.mark.parametrize(
    "filename",
    [
        "orden de compra.pdf",
        "orden.pdf",
        "OC-4501234.pdf",
        "orden compra 123.pdf",
        "O.C..pdf",
    ],
)
def test_presencia_detecta_adjunto_de_orden_sin_texto(detector, filename):
    assert detector([UnifiedFile(filename, "application/pdf", b"", "test")]) is True


@pytest.mark.parametrize("detector", ORDER_FILE_DETECTORS)
@pytest.mark.parametrize(
    "filename",
    [
        "orden de servicio.pdf",
        "orden de trabajo.pdf",
        "ordenador.pdf",
        "factura.pdf",
    ],
)
def test_presencia_no_confunde_otros_adjuntos_con_orden(detector, filename):
    assert detector([UnifiedFile(filename, "application/pdf", b"", "test")]) is False


@pytest.mark.parametrize("detector", OK_FILE_DETECTORS)
@pytest.mark.parametrize(
    "filename",
    [
        "ok compras.pdf",
        "OK DE COMPRAS.pdf",
        "visto bueno.pdf",
        "visto bueno de compras.pdf",
        "vobo compras.pdf",
        "aprobado compras.pdf",
        "aprobado por compras.pdf",
        "aprobacion compras.pdf",
        "autorizado por compras.pdf",
        "vb compras.pdf",
        "vb de compras.pdf",
        "ok-compras.pdf",
        "ok-de-compras.pdf",
        "aprobado-por-compras.pdf",
        "ok compras No 4501.pdf",
        "OK-DE-COMPRAS-No-123.pdf",
        "aprobado por compras - No 7.pdf",
    ],
)
def test_presencia_detecta_adjunto_de_ok_sin_texto(detector, filename):
    assert detector([UnifiedFile(filename, "application/pdf", b"", "test")]) is True


@pytest.mark.parametrize("detector", OK_FILE_DETECTORS)
@pytest.mark.parametrize(
    "filename",
    ["Anexo VB Ltda.pdf", "VB-2024-001.pdf", "no-ok-compras.pdf"],
)
def test_presencia_no_acepta_nombre_negado_o_vb_suelto(detector, filename):
    assert detector([UnifiedFile(filename, "application/pdf", b"", "test")]) is False


@pytest.mark.parametrize("detector", ORDER_TEXT_DETECTORS)
@pytest.mark.parametrize(
    "text",
    [
        "Este documento no reemplaza la orden de compra",
        "El presente documento no constituye orden de compra",
        "Este documento no requiere orden",
        "Sin orden de compra asociada",
        "Documento equivalente. No aplica orden.",
        "ordenador.pdf",
        "orden de servicio.pdf",
        "orden de trabajo interna",
        "Su orden fue despachada",
        "orden",
    ],
)
def test_auditoria_no_detecta_orden_sin_referencia_valida(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", ORDER_TEXT_DETECTORS)
@pytest.mark.parametrize(
    "text",
    [
        "Orden de compra No 4501234",
        "OC-4501234",
        "Orden No. 33071",
        "orden de compra 12345",
    ],
)
def test_auditoria_detecta_orden_con_identificador(detector, text):
    assert detector(text) is True


@pytest.mark.parametrize("detector", ORDER_TEXT_DETECTORS)
def test_contexto_negativo_descarta_orden_aunque_tenga_identificador(detector):
    assert detector("Este documento no reemplaza la orden de compra No 4501234") is False


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize("text", OK_COMPRAS_NEGATED_MATRIX)
def test_auditoria_no_detecta_ok_negado_o_pendiente(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize("text", OK_COMPRAS_LONG_PREFIX_NEGATIONS)
def test_auditoria_mantiene_negacion_lejana_en_la_misma_clausula(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize("text", OK_COMPRAS_POST_NEGATION_VARIANTS)
def test_auditoria_detecta_variantes_pospuestas_de_pendiente(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize("text", OK_COMPRAS_APPROVED_MATRIX)
def test_matriz_detecta_ok_real(detector, text):
    assert detector(text) is True


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize(
    "text",
    [
        "aprobado compras",
        "Aprobado de compras",
        "aprobación de compras",
        "APROBACION COMPRAS",
        "visto bueno compras",
        "VISTO BUENO DE COMPRAS",
        "vobo compras",
        "VoBo de Compras",
        "autorizado por compras",
        "aprobada compras",
        "aprobado para radicar",
        "autorizado para radicar",
        "cuenta con visto bueno",
        "recibida a satisfaccion",
        "visto bueno para radicación",
    ],
)
def test_auditoria_detecta_variantes_ok_existentes(detector, text):
    assert detector(text) is True


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize(
    "text",
    [
        "el ok de compras queda pendiente",
        "el ok de compras sigue pendiente",
        "texto sin ninguna frase de aprobación",
    ],
)
def test_auditoria_no_detecta_estados_pendientes_adicionales(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", OK_FILE_DETECTORS)
def test_correo_impreso_en_pdf_con_ok_compras_se_detecta(detector):
    printed_email = UnifiedFile(
        "correo impreso.pdf",
        "application/pdf",
        b"",
        "test",
        "Revisada la factura y sus soportes. OK compras para radicar.",
    )

    assert detector([printed_email]) is True


@pytest.mark.parametrize("detector", ORDER_FILE_DETECTORS)
def test_control_de_presencia_no_confunde_frase_negativa_con_orden(detector):
    file_obj = UnifiedFile(
        name="factura.pdf",
        mime_type="application/pdf",
        data=b"",
        source="test",
        extracted_text="Este documento no reemplaza la orden de compra No 4501234",
    )

    assert detector([file_obj]) is False


@pytest.mark.parametrize(
    "text",
    [
        "Orden de compra OC-123. No aplica IVA. OK COMPRAS",
        "Orden de compra OC-123. No requiere anticipo. OK COMPRAS",
        "Orden de compra OC-123. Sin retencion. OK COMPRAS",
        "Orden de compra OC-123. Falta contabilizar IVA. OK COMPRAS",
        "Orden de compra OC-123. Pendiente de pago. OK COMPRAS",
        "Orden de compra OC-123. No tiene retencion. OK COMPRAS",
        "Orden de compra OC-123. No cuenta con descuento. OK COMPRAS",
        "Orden de compra OC-123. Requiere factura. OK COMPRAS",
        "Orden de compra OC-123. Documento equivalente. OK COMPRAS",
    ],
)
def test_proximidad_entre_frases_no_veta_documento_ni_ok(text):
    file_obj = UnifiedFile("documento.pdf", "application/pdf", b"", "test", text)

    assert reademail.detect_order([file_obj]) is True
    assert document_classifier.detect_order([file_obj]) is True
    assert reademail.detect_ok_compras([file_obj]) is True
    assert document_classifier.detect_ok_compras([file_obj]) is True


def _run_electronic_invoice(monkeypatch, extra_pdfs):
    state = {}
    labels = []
    replies = []
    pdf = UnifiedFile(
        "factura.pdf",
        "application/pdf",
        b"",
        "test",
        extracted_text=(
            "Este documento no reemplaza la orden de compra No 4501234. "
            "Pendiente OK COMPRAS."
        ),
    )
    xml = UnifiedFile("factura.xml", "application/xml", b"<Invoice />", "test")
    client = ClientRecord("Cliente Demo", "clientedemo")
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@example.test"},
            {"name": "Subject", "value": "Factura Cliente Demo"},
        ]
    }

    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda current, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"payload": payload, "snippet": ""},
    )
    monkeypatch.setattr(reademail, "collect_attachments", lambda current_payload: [])
    monkeypatch.setattr(
        reademail,
        "build_unified_files",
        lambda service, message_id, attachments: ([pdf, *extra_pdfs, xml], [], []),
    )
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *args, **kwargs: None)
    monkeypatch.setattr(reademail, "validate_electronic_invoice_minimum", lambda *args: [])
    monkeypatch.setattr(
        reademail,
        "identify_client_in_order_pdfs",
        lambda pdfs, catalog: (
            ClientMatchResult(record=client)
            if reademail.detect_order(pdfs)
            else ClientMatchResult()
        ),
    )
    monkeypatch.setattr(reademail, "identify_client_from_fields", lambda *args: client)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: labels.append(label),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *args: replies.append(args))

    reademail.process_message(object(), object(), "message-auditoria", [])
    return labels, replies


def test_factura_con_orden_negada_y_ok_pendiente_no_se_aprueba(monkeypatch, capsys):
    labels, replies = _run_electronic_invoice(monkeypatch, [])

    output = capsys.readouterr().out
    assert labels == [reademail.LABEL_REJECTED_NAME]
    assert reademail.LABEL_APPROVED_NAME not in labels
    assert len(replies) == 1
    assert "No se detectó orden de compra en nombre ni texto de los PDF." in output
    assert "No se detectó OK de compras dentro de los PDF." in output


def test_factura_con_adjuntos_de_orden_y_ok_se_aprueba(monkeypatch):
    order_pdf = UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test")
    ok_pdf = UnifiedFile("ok compras.pdf", "application/pdf", b"", "test")

    labels, replies = _run_electronic_invoice(monkeypatch, [order_pdf, ok_pdf])

    assert labels == [reademail.LABEL_APPROVED_NAME]
    assert len(replies) == 1


@pytest.mark.parametrize("filename", ["Anexo VB Ltda.pdf", "VB-2024-001.pdf"])
def test_factura_con_vb_suelto_no_se_aprueba(monkeypatch, filename):
    order_pdf = UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test")
    misleading_pdf = UnifiedFile(filename, "application/pdf", b"", "test")

    labels, replies = _run_electronic_invoice(monkeypatch, [order_pdf, misleading_pdf])

    assert labels == [reademail.LABEL_REJECTED_NAME]
    assert reademail.LABEL_APPROVED_NAME not in labels
    assert len(replies) == 1


@pytest.mark.parametrize("text", OK_COMPRAS_NEGATED_ENUMERATIONS)
def test_factura_con_enumeracion_negada_se_rechaza(monkeypatch, text):
    order_pdf = UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test")
    status_pdf = UnifiedFile(
        "estado de documentos.pdf",
        "application/pdf",
        b"",
        "test",
        text,
    )

    labels, replies = _run_electronic_invoice(monkeypatch, [order_pdf, status_pdf])

    assert labels == [reademail.LABEL_REJECTED_NAME]
    assert reademail.LABEL_APPROVED_NAME not in labels
    assert len(replies) == 1


def test_factura_con_ok_que_sigue_en_espera_se_rechaza(monkeypatch):
    order_pdf = UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test")
    status_pdf = UnifiedFile(
        "estado de documentos.pdf",
        "application/pdf",
        b"",
        "test",
        "el ok de compras sigue en espera",
    )

    labels, replies = _run_electronic_invoice(monkeypatch, [order_pdf, status_pdf])

    assert labels == [reademail.LABEL_REJECTED_NAME]
    assert reademail.LABEL_APPROVED_NAME not in labels
    assert len(replies) == 1


def test_factura_con_xml_orden_y_pdf_con_ok_de_compras_se_aprueba(monkeypatch):
    order_pdf = UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test")
    printed_email = UnifiedFile(
        "correo impreso.pdf",
        "application/pdf",
        b"",
        "test",
        "OK DE COMPRAS",
    )

    labels, replies = _run_electronic_invoice(monkeypatch, [order_pdf, printed_email])

    assert labels == [reademail.LABEL_APPROVED_NAME]
    assert len(replies) == 1


def test_cuenta_cobro_con_orden_sin_texto_se_aprueba(monkeypatch):
    state = {}
    labels = []
    files = [
        UnifiedFile("cuenta de cobro.pdf", "application/pdf", b"", "test"),
        UnifiedFile(
            "cedula.jpg",
            "image/jpeg",
            b"",
            "test",
            "republica de colombia cedula de ciudadania fecha de nacimiento",
        ),
        UnifiedFile("rut.pdf", "application/pdf", b"", "test"),
        UnifiedFile("certificado bancario.pdf", "application/pdf", b"", "test"),
        UnifiedFile("orden de compra.pdf", "application/pdf", b"", "test"),
    ]
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@example.test"},
            {"name": "Subject", "value": "Cuenta de cobro"},
        ]
    }

    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda current, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"payload": payload, "snippet": ""},
    )
    monkeypatch.setattr(reademail, "collect_attachments", lambda current_payload: [])
    monkeypatch.setattr(
        reademail,
        "build_unified_files",
        lambda service, message_id, attachments: (files, [], []),
    )
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: labels.append(label),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *args: None)

    reademail.process_message(object(), object(), "message-cuenta-cobro", [])

    assert labels == [reademail.LABEL_APPROVED_NAME]
