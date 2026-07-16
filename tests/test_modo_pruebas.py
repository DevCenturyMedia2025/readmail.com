from reademail import resolve_effective_watch_labels


def test_resolve_effective_watch_labels_modo_pruebas_usa_solo_etiqueta():
    assert resolve_effective_watch_labels(True, "pruebas", ["INBOX", "IMPORTANT"]) == ["pruebas"]


def test_resolve_effective_watch_labels_modo_normal_preserva_config():
    labels = ["INBOX", "IMPORTANT"]

    assert resolve_effective_watch_labels(False, "pruebas", labels) == labels
