"""Medicion en seco de la regla "entidad no registrada -> REVISION MANUAL".

El log de simulacion debe emitirse sin alterar etiquetas, respuestas ni ruta.
"""
import base64

import pytest

import reademail
from reademail import RegisteredLookup


SUBJECT = "Factura electronica de PROVEEDOR DEMO SAS NIT 900123456"


def _payload(subject):
    body = "Cuerpo del correo de prueba con orden de compra OC-123."
    return {
        "headers": [
            {"name": "Subject", "value": subject},
            {"name": "From", "value": "Proveedor Demo <proveedor@example.test>"},
            {"name": "Date", "value": "Mon, 1 Jan 2035 10:00:00 -0500"},
            {"name": "To", "value": "facturacion@example.test"},
        ],
        "mimeType": "text/plain",
        "body": {"data": base64.urlsafe_b64encode(body.encode()).decode()},
        "parts": [],
    }


def _run(monkeypatch, registered_lookup, subject=SUBJECT):
    """Ejecuta process_message capturando etiquetas y respuestas."""
    labels = []
    replies = []
    msg = {"internalDate": "1900000000000", "payload": _payload(subject), "snippet": ""}

    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: {})
    monkeypatch.setattr(reademail, "save_state", lambda state, account_id=None: None)
    monkeypatch.setattr(reademail, "safe_get_message_full", lambda svc, mid: msg)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda svc, mid, label, archive=None: labels.append(label),
    )
    monkeypatch.setattr(
        reademail,
        "send_reply_email",
        lambda svc, msg_, to, subj, body: replies.append(to),
    )
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *a, **k: None)
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "ALT_RECIPIENT_ENABLED", False)
    monkeypatch.setattr(reademail, "collect_attachments", lambda payload: [])
    monkeypatch.setattr(reademail, "build_unified_files", lambda svc, mid, att: ([], [], []))

    reademail.process_message(
        object(),
        object(),
        "message-simulacion",
        [],
        None,
        reademail.AdminLookup(set(), set(), {}),
        registered_lookup,
    )
    return labels, replies


def test_entidad_registrada_no_emite_log_de_simulacion(monkeypatch, capsys):
    lookup = RegisteredLookup(
        registered_nits={"900123456"},
        registered_names={"proveedor demo sas"},
        registered_docs={},
    )

    labels, replies = _run(monkeypatch, lookup)

    assert "[SIMULACIÓN]" not in capsys.readouterr().out
    assert labels == [reademail.LABEL_REVIEW_NAME]
    assert replies == []


def test_entidad_no_registrada_emite_log_de_simulacion(monkeypatch, capsys):
    lookup = RegisteredLookup(set(), set(), {})

    labels, replies = _run(monkeypatch, lookup)

    salida = capsys.readouterr().out
    assert "🔎 [SIMULACIÓN] Entidad no registrada — con la regla nueva iría a " in salida
    assert f"REVISIÓN MANUAL | asunto={SUBJECT[:60]} |" in salida
    # El flujo no cambia: sigue cayendo en revision manual por "sin PDF", no por la regla.
    assert labels == [reademail.LABEL_REVIEW_NAME]
    assert replies == []


def test_flujo_identico_con_y_sin_entidad_registrada(monkeypatch, capsys):
    """El unico delta entre ambos casos es la linea de simulacion."""
    labels_reg, replies_reg = _run(
        monkeypatch,
        RegisteredLookup({"900123456"}, {"proveedor demo sas"}, {}),
    )
    salida_reg = [
        line for line in capsys.readouterr().out.splitlines() if "[SIMULACIÓN]" not in line
    ]

    labels_no_reg, replies_no_reg = _run(monkeypatch, RegisteredLookup(set(), set(), {}))
    salida_no_reg = [
        line for line in capsys.readouterr().out.splitlines() if "[SIMULACIÓN]" not in line
    ]

    assert labels_reg == labels_no_reg
    assert replies_reg == replies_no_reg
    assert salida_reg == salida_no_reg


def test_registered_lookup_omitido_no_rompe_llamadas_existentes(monkeypatch, capsys):
    """Las llamadas previas (sin el parametro) siguen funcionando."""
    labels, replies = _run(monkeypatch, None)

    # Sin lookup se asume "no registrada": se mide, pero no se altera la ruta.
    assert "[SIMULACIÓN]" in capsys.readouterr().out
    assert labels == [reademail.LABEL_REVIEW_NAME]
    assert replies == []
