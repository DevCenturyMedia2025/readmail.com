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
        "visto bueno.pdf",
        "vobo compras.pdf",
        "aprobado compras.pdf",
        "aprobacion compras.pdf",
    ],
)
def test_presencia_detecta_adjunto_de_ok_sin_texto(detector, filename):
    assert detector([UnifiedFile(filename, "application/pdf", b"", "test")]) is True


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
@pytest.mark.parametrize(
    "text",
    [
        "pendiente ok compras",
        "no tiene ok compras",
        "falta ok compras",
        "sin visto bueno compras",
    ],
)
def test_auditoria_no_detecta_ok_negado_o_pendiente(detector, text):
    assert detector(text) is False


@pytest.mark.parametrize("detector", OK_TEXT_DETECTORS)
@pytest.mark.parametrize(
    "text",
    [
        "OK COMPRAS",
        "Aprobado compras",
        "Visto bueno compras",
        "aprobación de compras",
    ],
)
def test_auditoria_detecta_ok_real(detector, text):
    assert detector(text) is True


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
