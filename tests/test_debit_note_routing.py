import reademail
from reademail import UnifiedFile


def test_pdf_nota_debito_va_a_etiqueta_nota_credito_sin_responder(
    monkeypatch,
    capsys,
):
    state = {}
    labels = []
    replies = []
    saved_states = []
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@example.test"},
            {"name": "Subject", "value": "Documento contable adjunto"},
        ]
    }
    debit_note = UnifiedFile(
        "nota de debito.pdf",
        "application/pdf",
        b"",
        "test",
    )

    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(
        reademail,
        "save_state",
        lambda current, account_id=None: saved_states.append(current.copy()),
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
        lambda service, message_id, attachments: ([debit_note], [], []),
    )
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda service, message_id, label, archive=False: labels.append((label, archive)),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *args: replies.append(args))

    reademail.process_message(object(), object(), "message-debit-note", [])

    assert labels == [(reademail.LABEL_NOTE_CREDIT_NAME, reademail.ARCHIVE_NOTE_CREDIT)]
    assert replies == []
    assert state["processed_message_ids"] == ["message-debit-note"]
    assert saved_states[-1]["processed_message_ids"] == ["message-debit-note"]
    assert "NOTA DE CREDITO por nombre" in capsys.readouterr().out
