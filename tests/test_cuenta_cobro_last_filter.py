import reademail
from reademail import UnifiedFile


def _run_message(monkeypatch, files):
    state = {}
    calls = {
        "labels": [],
        "replies": [],
        "saved_states": [],
        "cuenta_validation": [],
        "electronic_validation": [],
    }
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@example.test"},
            {"name": "Subject", "value": "Documentos para radicacion"},
        ]
    }

    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(
        reademail,
        "save_state",
        lambda current, account_id=None: calls["saved_states"].append(current.copy()),
    )
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
        lambda service, message_id, label, archive=False: calls["labels"].append(
            (label, archive)
        ),
    )
    monkeypatch.setattr(
        reademail,
        "send_reply_email",
        lambda *args: calls["replies"].append(args),
    )

    def validate_cuenta(files_to_validate):
        calls["cuenta_validation"].append(files_to_validate)
        return {
            "estado": "incompleto",
            "faltantes": ["rut"],
            "identificados": {"cuenta_cobro": ["cuenta de cobro.pdf (nombre)"]},
        }

    def validate_electronic(pdf_count, xml_count):
        calls["electronic_validation"].append((pdf_count, xml_count))
        return ["Factura electronica invalida para la prueba."]

    monkeypatch.setattr(reademail, "validate_cuenta_cobro_package", validate_cuenta)
    monkeypatch.setattr(reademail, "validate_electronic_invoice_minimum", validate_electronic)

    reademail.process_message(object(), object(), "message-filter", [])
    calls["state"] = state
    return calls


def test_sin_xml_pdf_declara_cuenta_cobro_sigue_a_validacion(monkeypatch):
    pdf = UnifiedFile(
        "cuenta de cobro agosto.pdf",
        "application/pdf",
        b"",
        "test",
    )

    calls = _run_message(monkeypatch, [pdf])

    assert calls["cuenta_validation"] == [[pdf]]
    assert calls["electronic_validation"] == []
    assert calls["labels"][0] == (reademail.LABEL_REJECTED_NAME, reademail.ARCHIVE_REJECTED)
    assert len(calls["replies"]) == 1


def test_sin_xml_y_sin_pdf_cuenta_cobro_va_a_revision_sin_responder(
    monkeypatch,
    capsys,
):
    pdf = UnifiedFile("soporte.pdf", "application/pdf", b"", "test")

    calls = _run_message(monkeypatch, [pdf])

    radicado = calls["state"]["message_radicados"]["message-filter"]
    assert calls["cuenta_validation"] == []
    assert calls["electronic_validation"] == []
    assert calls["labels"] == [(reademail.LABEL_REVIEW_NAME, reademail.ARCHIVE_REVIEW)]
    assert calls["replies"] == []
    assert calls["state"]["processed_message_ids"] == ["message-filter"]
    assert calls["saved_states"][-1]["processed_message_ids"] == ["message-filter"]
    assert (
        f"🟨 REVISIÓN MANUAL | {radicado} | "
        "sin XML y ningún PDF declara ser cuenta de cobro"
    ) in capsys.readouterr().out


def test_con_xml_conserva_flujo_de_factura_electronica(monkeypatch):
    pdf = UnifiedFile("factura.pdf", "application/pdf", b"", "test")
    xml = UnifiedFile("factura.xml", "application/xml", b"<Invoice />", "test")

    calls = _run_message(monkeypatch, [pdf, xml])

    assert calls["cuenta_validation"] == []
    assert calls["electronic_validation"] == [(1, 1)]
    assert calls["labels"][0] == (reademail.LABEL_REJECTED_NAME, reademail.ARCHIVE_REJECTED)
    assert all(label != reademail.LABEL_REVIEW_NAME for label, _ in calls["labels"])
    assert len(calls["replies"]) == 1
