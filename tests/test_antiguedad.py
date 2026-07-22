import pytest

import reademail
from reademail import es_correo_antiguo


DIA_MS = 24 * 60 * 60 * 1000
AHORA_MS = 1_800_000_000_000


@pytest.mark.parametrize(
    ("internal_date_ms", "expected"),
    [
        (AHORA_MS, False),
        (AHORA_MS - 6 * DIA_MS, True),
        (AHORA_MS - 5 * DIA_MS, False),
        (None, False),
        ("", False),
    ],
)
def test_es_correo_antiguo(internal_date_ms, expected):
    assert es_correo_antiguo(internal_date_ms, AHORA_MS, 5) is expected


def test_exactamente_cinco_dias_no_es_antiguo_por_ser_limite_estricto():
    assert es_correo_antiguo(AHORA_MS - 5 * DIA_MS, AHORA_MS, 5) is False


def test_internal_date_invalido_no_se_considera_antiguo():
    assert es_correo_antiguo("fecha-invalida", AHORA_MS, 5) is False


def test_process_message_antiguo_va_a_revision_manual_sin_responder(monkeypatch, capsys):
    state = {}
    labels = []
    replies = []
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", True)
    monkeypatch.setattr(reademail, "MAX_DIAS_ANTIGUEDAD", 5)
    monkeypatch.setattr(reademail, "MODO_PRUEBAS", False)
    monkeypatch.setattr(reademail.time, "time", lambda: AHORA_MS / 1000)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "save_state", lambda state, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"internalDate": str(AHORA_MS - 6 * DIA_MS), "payload": {}},
    )
    monkeypatch.setattr(reademail, "apply_single_status_label", lambda *args, **kwargs: labels.append((args, kwargs)))
    monkeypatch.setattr(reademail, "send_reply_email", lambda *args: replies.append(args))

    reademail.process_message(object(), object(), "message-1", [])

    assert len(labels) == 1
    assert labels[0][0][2] == reademail.LABEL_REVIEW_NAME
    assert replies == []
    assert state["processed_message_ids"] == ["message-1"]
    assert "Correo con más de 5 días -> REVISIÓN MANUAL, no se responde" in capsys.readouterr().out


@pytest.mark.parametrize(("enabled", "modo_pruebas"), [(False, False), (True, True)])
def test_filtro_se_ignora_si_esta_deshabilitado_o_en_modo_pruebas(monkeypatch, enabled, modo_pruebas):
    labels = []
    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", enabled)
    monkeypatch.setattr(reademail, "MODO_PRUEBAS", modo_pruebas)
    monkeypatch.setattr(reademail.time, "time", lambda: AHORA_MS / 1000)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: {})
    monkeypatch.setattr(reademail, "save_state", lambda state, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda service, message_id: {"internalDate": str(AHORA_MS - 6 * DIA_MS), "payload": {}},
    )
    monkeypatch.setattr(reademail, "apply_single_status_label", lambda *args, **kwargs: labels.append(args))

    reademail.process_message(object(), object(), "message-1", [])

    assert labels == []
