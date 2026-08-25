"""
Etiqueta propia para NOTA DE DÉBITO.

Antes crédito y débito compartian la etiqueta NOTA DE CRÉDITO. Ahora cada una
tiene la suya, con las mismas tres barreras de deteccion y en la misma
posicion del flujo. Datos inventados.
"""

import time

import pytest

import reademail
from reademail import UnifiedFile


def _pdf(nombre, texto=""):
    return UnifiedFile(nombre, "application/pdf", b"", "test", texto)


class _Ejecutable:
    def __init__(self, registro):
        self.registro = registro

    def execute(self):
        return {}


class _Mensajes:
    def __init__(self, registro):
        self.registro = registro

    def modify(self, **kwargs):
        self.registro.append(kwargs)
        return _Ejecutable(self.registro)


class _Usuarios:
    def __init__(self, registro):
        self._mensajes = _Mensajes(registro)

    def messages(self):
        return self._mensajes


class _Gmail:
    def __init__(self, registro):
        self._usuarios = _Usuarios(registro)

    def users(self):
        return self._usuarios


def _correr(monkeypatch, archivos, asunto="Documento contable adjunto"):
    estado = {}
    llamadas = {"labels": [], "replies": [], "modificaciones": []}
    payload = {
        "headers": [
            {"name": "From", "value": "proveedor@ejemplo.test"},
            {"name": "Subject", "value": asunto},
        ]
    }

    monkeypatch.setattr(reademail, "LIMITE_ANTIGUEDAD_ENABLED", False)
    monkeypatch.setattr(reademail, "ONLY_WITH_ATTACHMENTS", False)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: estado)
    monkeypatch.setattr(reademail, "save_state", lambda actual, account_id=None: None)
    monkeypatch.setattr(
        reademail,
        "safe_get_message_full",
        lambda s, m: {"payload": payload, "snippet": "", "internalDate": str(int(time.time() * 1000))},
    )
    monkeypatch.setattr(reademail, "collect_attachments", lambda p: [])
    monkeypatch.setattr(reademail, "build_unified_files", lambda s, m, a: (list(archivos), [], []))
    monkeypatch.setattr(reademail, "auto_fill_nit_from_subject", lambda *a, **k: None)
    monkeypatch.setattr(
        reademail,
        "apply_single_status_label",
        lambda s, mid, nombre, archive=False: llamadas["labels"].append((nombre, archive)),
    )
    monkeypatch.setattr(reademail, "send_reply_email", lambda *a: llamadas["replies"].append(a))

    reademail.process_message(_Gmail(llamadas["modificaciones"]), object(), "mensaje-1", [])
    llamadas["estado"] = estado
    return llamadas


# --------------------------------------------------------------------------
# Deteccion separada
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "texto",
    [
        "nota de credito",
        "NOTA DE CRÉDITO",
        "Nota de Crédito No 5",
        "notas de credito",
        "credit note",
        "CREDIT NOTES",
    ],
)
def test_detecta_nota_de_credito(texto):
    assert reademail.contains_credit_note_text(texto) is True
    assert reademail.contains_debit_note_text(texto) is False


@pytest.mark.parametrize(
    "texto",
    [
        "nota de debito",
        "NOTA DE DÉBITO",
        "Nota de Débito No 5",
        "notas de debito",
        "debit note",
        "DEBIT NOTES",
    ],
)
def test_detecta_nota_de_debito(texto):
    assert reademail.contains_debit_note_text(texto) is True
    assert reademail.contains_credit_note_text(texto) is False


@pytest.mark.parametrize(
    "texto",
    [
        "debito automatico",
        "débito automático",
        "tarjeta debito",
        "tarjeta débito",
        "nota interna",
        "credito rotativo",
        "pago con debito",
        "",
    ],
)
def test_falsos_positivos_siguen_en_false(texto):
    assert reademail.contains_credit_note_text(texto) is False
    assert reademail.contains_debit_note_text(texto) is False
    assert reademail.contains_credit_or_debit_note_text(texto) is False


def test_el_alias_historico_cubre_las_dos():
    assert reademail.contains_credit_or_debit_note_text("nota de credito") is True
    assert reademail.contains_credit_or_debit_note_text("nota de debito") is True
    assert reademail.contains_note_credit_text("nota de debito") is True


# --------------------------------------------------------------------------
# Enrutamiento por las tres barreras
# --------------------------------------------------------------------------
def test_credito_por_texto_del_correo(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("factura.pdf")], asunto="Nota de credito 5")

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_CREDIT_NAME, reademail.ARCHIVE_NOTE_CREDIT)]
    assert llamadas["replies"] == []
    assert llamadas["estado"]["processed_message_ids"] == ["mensaje-1"]
    assert "NOTA DE CRÉDITO por correo" in capsys.readouterr().out


def test_debito_por_texto_del_correo(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("factura.pdf")], asunto="Nota de debito 5")

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_DEBIT_NAME, reademail.ARCHIVE_NOTE_DEBIT)]
    assert llamadas["replies"] == []
    assert llamadas["estado"]["processed_message_ids"] == ["mensaje-1"]
    assert "NOTA DE DÉBITO por correo" in capsys.readouterr().out


def test_credito_por_nombre_de_pdf(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("nota de credito.pdf")])

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_CREDIT_NAME, reademail.ARCHIVE_NOTE_CREDIT)]
    assert llamadas["replies"] == []
    assert "NOTA DE CRÉDITO por nombre" in capsys.readouterr().out


def test_debito_por_nombre_de_pdf(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("nota de debito.pdf")])

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_DEBIT_NAME, reademail.ARCHIVE_NOTE_DEBIT)]
    assert llamadas["replies"] == []
    assert "NOTA DE DÉBITO por nombre" in capsys.readouterr().out


def test_credito_por_texto_de_pdf(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("documento.pdf", "NOTA DE CREDITO No 5")])

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_CREDIT_NAME, reademail.ARCHIVE_NOTE_CREDIT)]
    assert llamadas["replies"] == []
    assert "NOTA DE CRÉDITO por texto" in capsys.readouterr().out


def test_debito_por_texto_de_pdf(monkeypatch, capsys):
    llamadas = _correr(monkeypatch, [_pdf("documento.pdf", "NOTA DE DEBITO No 5")])

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_DEBIT_NAME, reademail.ARCHIVE_NOTE_DEBIT)]
    assert llamadas["replies"] == []
    assert "NOTA DE DÉBITO por texto" in capsys.readouterr().out


def test_credito_tiene_precedencia_sobre_debito(monkeypatch):
    """Caso raro: el correo declara las dos. Gana crédito, por ser el frecuente."""
    llamadas = _correr(
        monkeypatch,
        [_pdf("factura.pdf")],
        asunto="Nota de credito y nota de debito del mismo mes",
    )

    assert llamadas["labels"] == [(reademail.LABEL_NOTE_CREDIT_NAME, reademail.ARCHIVE_NOTE_CREDIT)]


def test_debito_automatico_no_dispara_ninguna_nota(monkeypatch):
    llamadas = _correr(
        monkeypatch,
        [_pdf("factura.pdf", "El pago se hace por debito automatico")],
        asunto="Factura de venta",
    )

    etiquetas = [nombre for nombre, _ in llamadas["labels"]]
    assert reademail.LABEL_NOTE_CREDIT_NAME not in etiquetas
    assert reademail.LABEL_NOTE_DEBIT_NAME not in etiquetas


# --------------------------------------------------------------------------
# Una sola etiqueta de estado
# --------------------------------------------------------------------------
def test_la_etiqueta_de_debito_esta_registrada_como_estado(monkeypatch):
    creadas = []
    monkeypatch.setattr(reademail, "ensure_label_exists", lambda s, nombre: creadas.append(nombre) or f"id-{nombre}")

    ids = reademail.ensure_status_labels(object())

    assert reademail.LABEL_NOTE_DEBIT_NAME in creadas, "no se crearia en Gmail si no existe"
    assert reademail.LABEL_NOTE_DEBIT_NAME in ids


def test_aplicar_nota_debito_remueve_las_demas_etiquetas(monkeypatch):
    monkeypatch.setattr(reademail, "ensure_label_exists", lambda s, nombre: f"id-{nombre}")
    registro = []

    reademail.apply_single_status_label(_Gmail(registro), "mensaje-1", reademail.LABEL_NOTE_DEBIT_NAME)

    cuerpo = registro[0]["body"]
    assert cuerpo["addLabelIds"] == [f"id-{reademail.LABEL_NOTE_DEBIT_NAME}"]
    for otra in (
        reademail.LABEL_NOTE_CREDIT_NAME,
        reademail.LABEL_APPROVED_NAME,
        reademail.LABEL_REJECTED_NAME,
        reademail.LABEL_REVIEW_NAME,
        reademail.LABEL_ADMIN_NAME,
    ):
        assert f"id-{otra}" in cuerpo["removeLabelIds"]
    assert f"id-{reademail.LABEL_NOTE_DEBIT_NAME}" not in cuerpo["removeLabelIds"]


def test_aplicar_otra_etiqueta_remueve_la_de_debito(monkeypatch):
    monkeypatch.setattr(reademail, "ensure_label_exists", lambda s, nombre: f"id-{nombre}")
    registro = []

    reademail.apply_single_status_label(_Gmail(registro), "mensaje-1", reademail.LABEL_APPROVED_NAME)

    assert f"id-{reademail.LABEL_NOTE_DEBIT_NAME}" in registro[0]["body"]["removeLabelIds"]
