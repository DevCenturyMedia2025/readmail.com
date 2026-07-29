import json
from types import SimpleNamespace

import reademail
from reademail import fetch_labeled_message_ids, fetch_new_message_ids, resolve_effective_watch_labels


class FakeHistoryList:
    def __init__(self, response):
        self.response = response
        self.requests = []

    def list(self, **kwargs):
        self.requests.append(kwargs)
        return self

    def execute(self):
        return self.response


class FakeGmailService:
    def __init__(self, response):
        self.history_list = FakeHistoryList(response)

    def users(self):
        return self

    def history(self):
        return self.history_list


class FakeMessageList:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.requests = []

    def list(self, **kwargs):
        self.requests.append(kwargs)
        return self

    def execute(self):
        if self.error:
            raise self.error
        return self.response


class FakeMessagesService:
    def __init__(self, response=None, error=None):
        self.message_list = FakeMessageList(response, error)

    def users(self):
        return self

    def messages(self):
        return self.message_list


def test_resolve_effective_watch_labels_modo_pruebas_usa_solo_etiqueta():
    assert resolve_effective_watch_labels(True, "pruebas", ["INBOX", "IMPORTANT"]) == ["pruebas"]


def test_resolve_effective_watch_labels_modo_normal_preserva_config():
    labels = ["INBOX", "IMPORTANT"]

    assert resolve_effective_watch_labels(False, "pruebas", labels) == labels


def test_fetch_incluye_label_added_solo_para_etiqueta_vigilada():
    service = FakeGmailService(
        {
            "historyId": "200",
            "history": [
                {
                    "labelsAdded": [
                        {"message": {"id": "con-pruebas"}, "labelIds": ["Label_pruebas"]},
                        {"message": {"id": "otra-etiqueta"}, "labelIds": ["Label_otra"]},
                    ]
                }
            ],
        }
    )

    message_ids, latest = fetch_new_message_ids(
        service,
        "100",
        label_id="Label_pruebas",
        include_label_added=True,
    )

    assert message_ids == {"con-pruebas"}
    assert latest == "200"
    assert service.history_list.requests[0]["historyTypes"] == ["messageAdded", "labelAdded"]


def test_fetch_ignora_label_added_si_no_esta_habilitado():
    service = FakeGmailService(
        {
            "history": [
                {
                    "messagesAdded": [{"message": {"id": "nuevo"}}],
                    "labelsAdded": [{"message": {"id": "etiquetado"}, "labelIds": ["Label_pruebas"]}],
                }
            ]
        }
    )

    message_ids, _ = fetch_new_message_ids(
        service,
        "100",
        label_id="Label_pruebas",
        include_label_added=False,
    )

    assert message_ids == {"nuevo"}
    assert service.history_list.requests[0]["historyTypes"] == ["messageAdded"]


def test_fetch_deduplica_id_presente_en_message_added_y_label_added():
    service = FakeGmailService(
        {
            "history": [
                {
                    "messagesAdded": [{"message": {"id": "mismo-id"}}],
                    "labelsAdded": [{"message": {"id": "mismo-id"}, "labelIds": ["Label_pruebas"]}],
                }
            ]
        }
    )

    message_ids, _ = fetch_new_message_ids(
        service,
        "100",
        label_id="Label_pruebas",
        include_label_added=True,
    )

    assert message_ids == {"mismo-id"}


def test_fetch_labeled_message_ids_devuelve_ids():
    service = FakeMessagesService({"messages": [{"id": "m-1"}, {"id": "m-2"}, {}]})

    message_ids = fetch_labeled_message_ids(service, "Label_pruebas", max_results=10)

    assert message_ids == ["m-1", "m-2"]
    assert service.message_list.requests == [
        {"userId": "me", "labelIds": ["Label_pruebas"], "maxResults": 10}
    ]


def test_fetch_labeled_message_ids_si_api_falla_devuelve_lista_vacia(caplog):
    service = FakeMessagesService(error=RuntimeError("Gmail caído"))

    assert fetch_labeled_message_ids(service, "Label_pruebas") == []
    assert "No se pudieron buscar correos con la etiqueta Label_pruebas: Gmail caído" in caplog.text


class FakeSubscriber:
    def __init__(self, email_address="cuenta@example.com"):
        data = json.dumps({"historyId": "200", "emailAddress": email_address}).encode("utf-8")
        received = SimpleNamespace(ack_id="ack-1", message=SimpleNamespace(data=data))
        self.response = SimpleNamespace(received_messages=[received])
        self.pull_count = 0

    def pull(self, request):
        self.pull_count += 1
        if self.pull_count > 1:
            raise KeyboardInterrupt
        return self.response

    def modify_ack_deadline(self, request):
        return None

    def acknowledge(self, request):
        return None


def _run_single_event_listener(monkeypatch, modo_pruebas, labeled_ids, processed_ids):
    subscriber = FakeSubscriber()
    state = {"last_history_id": "100", "processed_message_ids": list(processed_ids)}
    processed_calls = []
    labeled_calls = []

    monkeypatch.setattr(reademail, "MODO_PRUEBAS", modo_pruebas)
    monkeypatch.setattr(reademail.pubsub_v1, "SubscriberClient", lambda transport: subscriber)
    monkeypatch.setattr(reademail, "ensure_gmail_watch", lambda *args, **kwargs: None)
    monkeypatch.setattr(reademail, "load_state", lambda account_id=None: state)
    monkeypatch.setattr(reademail, "update_last_history_id", lambda *args, **kwargs: None)
    monkeypatch.setattr(reademail, "resolve_modo_pruebas_label_id", lambda *args: "Label_pruebas")
    monkeypatch.setattr(reademail, "fetch_new_message_ids", lambda *args, **kwargs: (set(), "200"))
    monkeypatch.setattr(reademail, "load_client_catalog", lambda service: [])
    monkeypatch.setattr(reademail, "load_admin_lookup", lambda service: reademail.AdminLookup(set(), set(), {}))

    def fetch_labeled(*args, **kwargs):
        labeled_calls.append((args, kwargs))
        return labeled_ids

    monkeypatch.setattr(reademail, "fetch_labeled_message_ids", fetch_labeled)
    monkeypatch.setattr(
        reademail,
        "process_message",
        lambda gmail, sheets, message_id, catalog, account_id, admin_lookup: processed_calls.append(message_id),
    )
    accounts = {
        "cuenta@example.com": {
            "gmail_service": object(),
            "sheets_service": object(),
            "catalog_data": [],
            "admin_lookup": reademail.AdminLookup(set(), set(), {}),
            "account_id": "cuenta@example.com",
        }
    }

    reademail.listen_pubsub(accounts)
    return processed_calls, labeled_calls


def test_modo_pruebas_excluye_ids_ya_procesados(monkeypatch, capsys):
    processed_calls, labeled_calls = _run_single_event_listener(
        monkeypatch,
        modo_pruebas=True,
        labeled_ids=["procesado", "pendiente"],
        processed_ids={"procesado"},
    )

    assert processed_calls == ["pendiente"]
    assert len(labeled_calls) == 1
    assert "🧪 MODO PRUEBAS: 1 correo(s) con etiqueta pruebas por procesar" in capsys.readouterr().out


def test_modo_produccion_no_invoca_busqueda_directa_por_etiqueta(monkeypatch):
    processed_calls, labeled_calls = _run_single_event_listener(
        monkeypatch,
        modo_pruebas=False,
        labeled_ids=["no-debe-consultarse"],
        processed_ids=set(),
    )

    assert labeled_calls == []
    assert processed_calls == []
