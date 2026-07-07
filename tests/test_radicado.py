from app.services.radicado import get_or_create_radicado


def test_get_or_create_radicado_reuses_existing_for_same_message():
    state = {"message_radicados": {"m1": "RAD-20260101-000001"}}
    assert get_or_create_radicado("m1", state) == "RAD-20260101-000001"


def test_get_or_create_radicado_generates_new_with_default_format():
    state = {}
    radicado = get_or_create_radicado("m1", state)
    assert radicado.startswith("RAD-")
    assert state["radicado_counter"] == 1
    assert state["message_radicados"]["m1"] == radicado


def test_get_or_create_radicado_increments_counter_same_day():
    state = {}
    r1 = get_or_create_radicado("m1", state)
    r2 = get_or_create_radicado("m2", state)
    assert state["radicado_counter"] == 2
    assert r1 != r2


def test_get_or_create_radicado_resets_daily_when_date_changes():
    state = {"radicado_counter": 5, "radicado_date": "20200101"}
    radicado = get_or_create_radicado("m1", state, radicado_reset_daily=True)
    assert state["radicado_counter"] == 1
    assert radicado.endswith("-000001")


def test_get_or_create_radicado_no_reset_when_daily_disabled():
    state = {"radicado_counter": 5, "radicado_date": "20200101"}
    get_or_create_radicado("m1", state, radicado_reset_daily=False)
    assert state["radicado_counter"] == 6


def test_get_or_create_radicado_respects_custom_prefix_and_pad():
    state = {}
    radicado = get_or_create_radicado(
        "m1", state, radicado_prefix="FAC", radicado_pad=3, radicado_reset_daily=False
    )
    assert radicado == "FAC-001"


def test_get_or_create_radicado_trims_map_when_limit_exceeded():
    state = {}
    for i in range(5):
        get_or_create_radicado(f"m{i}", state, radicado_map_limit=3, radicado_reset_daily=False)
    mappings = state["message_radicados"]
    assert len(mappings) == 3
    assert set(mappings.keys()) == {"m2", "m3", "m4"}
