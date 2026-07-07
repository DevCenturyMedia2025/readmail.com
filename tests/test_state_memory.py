from app.services.state_memory import (
    state_add_processed,
    state_get_processed_set,
    state_has_replied,
    state_mark_replied,
    today_yyyymmdd,
)


def test_today_yyyymmdd_format():
    value = today_yyyymmdd()
    assert len(value) == 8
    assert value.isdigit()


def test_state_get_processed_set_empty_state():
    assert state_get_processed_set({}) == set()


def test_state_get_processed_set_filters_none():
    state = {"processed_message_ids": ["a", None, "b"]}
    assert state_get_processed_set(state) == {"a", "b"}


def test_state_add_processed_appends_and_dedupes():
    state = {}
    state_add_processed(state, "m1")
    state_add_processed(state, "m1")
    state_add_processed(state, "m2")
    assert state["processed_message_ids"] == ["m1", "m2"]


def test_state_add_processed_respects_limit_trimming_oldest():
    state = {}
    for i in range(5):
        state_add_processed(state, f"m{i}", processed_cache_limit=3)
    assert state["processed_message_ids"] == ["m2", "m3", "m4"]


def test_state_has_replied_true_and_false():
    state = {"replied_message_ids": ["m1"]}
    assert state_has_replied(state, "m1") is True
    assert state_has_replied(state, "m2") is False


def test_state_mark_replied_appends_and_dedupes():
    state = {}
    state_mark_replied(state, "m1")
    state_mark_replied(state, "m1")
    assert state["replied_message_ids"] == ["m1"]


def test_state_mark_replied_respects_limit_trimming_oldest():
    state = {}
    for i in range(4):
        state_mark_replied(state, f"m{i}", processed_cache_limit=2)
    assert state["replied_message_ids"] == ["m2", "m3"]
