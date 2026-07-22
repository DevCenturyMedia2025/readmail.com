from reademail import fetch_new_message_ids, resolve_effective_watch_labels


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
