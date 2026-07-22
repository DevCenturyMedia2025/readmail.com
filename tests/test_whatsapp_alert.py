import urllib.parse

import reademail
from reademail import send_whatsapp_alert, should_send_whatsapp


def test_should_send_whatsapp_respeta_key_y_cooldown():
    cache = {}

    assert should_send_whatsapp(cache, "loop", 1_000, 15) is True
    assert should_send_whatsapp(cache, "loop", 1_100, 15) is False
    assert should_send_whatsapp(cache, "token", 1_100, 15) is True
    assert should_send_whatsapp(cache, "loop", 1_901, 15) is True


def _configure_whatsapp(monkeypatch, enabled=True):
    monkeypatch.setattr(reademail, "WHATSAPP_ALERT_ENABLED", enabled)
    monkeypatch.setattr(reademail, "WHATSAPP_PHONE", "000")
    monkeypatch.setattr(reademail, "WHATSAPP_APIKEY", "test")
    monkeypatch.setattr(reademail, "WHATSAPP_COOLDOWN_MIN", 15)
    monkeypatch.setattr(reademail, "_WHATSAPP_ALERT_CACHE", {})


def test_whatsapp_deshabilitado_no_llama_urllib(monkeypatch):
    calls = []
    _configure_whatsapp(monkeypatch, enabled=False)
    monkeypatch.setattr(reademail.urllib.request, "urlopen", lambda *args, **kwargs: calls.append((args, kwargs)))

    send_whatsapp_alert("[Loop Pub/Sub] Error de prueba")

    assert calls == []


def test_url_codifica_texto_con_espacios_acentos_prefijo_y_area(monkeypatch):
    calls = []
    _configure_whatsapp(monkeypatch)
    monkeypatch.setattr(reademail.urllib.request, "urlopen", lambda *args, **kwargs: calls.append((args, kwargs)))
    mensaje = "[Procesar correo] Falló conexión técnica"

    send_whatsapp_alert(mensaje)

    assert len(calls) == 1
    url = calls[0][0][0]
    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    assert query["phone"] == ["000"]
    assert query["apikey"] == ["test"]
    assert query["text"] == [f"🚨 ReadMail: {mensaje}"]
    assert "%20" in url
    assert "%C3%B3" in url
    assert calls[0][1]["timeout"] == 10


def test_excepcion_de_urllib_no_se_propaga(monkeypatch, caplog):
    _configure_whatsapp(monkeypatch)

    def fail(*args, **kwargs):
        raise OSError("sin red")

    monkeypatch.setattr(reademail.urllib.request, "urlopen", fail)

    send_whatsapp_alert("[Loop Pub/Sub] Servicio caído")

    assert "⚠️ No se pudo enviar alerta WhatsApp: sin red" in caplog.text


def test_mensaje_incluye_prefijo_y_area_entre_corchetes(monkeypatch):
    calls = []
    _configure_whatsapp(monkeypatch)
    monkeypatch.setattr(reademail.urllib.request, "urlopen", lambda url, timeout: calls.append(url))

    send_whatsapp_alert("[Token] El token de cuenta@example.com se venció")

    texto = urllib.parse.parse_qs(urllib.parse.urlparse(calls[0]).query)["text"][0]
    assert texto.startswith("🚨 ReadMail: [Token]")
