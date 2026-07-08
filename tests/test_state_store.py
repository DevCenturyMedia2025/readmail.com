import json
from pathlib import Path

from app.services.state_store import (
    load_state,
    save_state,
    state_file_for_account,
)


def test_state_file_for_account_global_when_no_email(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    result = state_file_for_account(None, state_file, accounts_dir)
    assert result == str(state_file)


def test_state_file_for_account_global_when_empty_string(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    result = state_file_for_account("", state_file, accounts_dir)
    assert result == str(state_file)


def test_state_file_for_account_per_account_when_email(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    email = "test@example.com"
    result = state_file_for_account(email, state_file, accounts_dir)
    assert result == str(accounts_dir / email / "gmail_watch_state.json")


def test_load_state_returns_empty_dict_when_file_missing(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    assert load_state(None, state_file, accounts_dir) == {}


def test_load_state_returns_empty_dict_when_json_corrupted(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    state_file.write_text("{ invalid json }")
    assert load_state(None, state_file, accounts_dir) == {}


def test_load_state_returns_empty_dict_when_json_not_dict(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    state_file.write_text(json.dumps([1, 2, 3]))
    assert load_state(None, state_file, accounts_dir) == {}


def test_load_state_reads_valid_dict(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    expected = {"processed_message_ids": ["m1", "m2"], "radicado_counter": 5}
    state_file.write_text(json.dumps(expected, ensure_ascii=False, indent=2))
    assert load_state(None, state_file, accounts_dir) == expected


def test_save_state_creates_directories(tmp_path):
    state_file = tmp_path / "state" / "subdir" / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    state = {"key": "value"}
    save_state(state, None, state_file, accounts_dir)
    assert state_file.exists()
    assert json.loads(state_file.read_text()) == state


def test_save_state_writes_per_account(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    email = "test@example.com"
    state = {"count": 42}
    save_state(state, email, state_file, accounts_dir)
    expected_path = accounts_dir / email / "gmail_watch_state.json"
    assert expected_path.exists()
    assert json.loads(expected_path.read_text()) == state


def test_save_load_roundtrip(tmp_path):
    state_file = tmp_path / "gmail_watch_state.json"
    accounts_dir = tmp_path / "accounts"
    original = {
        "processed_message_ids": ["m1"],
        "replied_message_ids": ["m2"],
        "radicado_counter": 1,
        "radicado_date": "20260107",
        "message_radicados": {"m1": "RAD-20260107-000001"},
    }
    save_state(original, None, state_file, accounts_dir)
    loaded = load_state(None, state_file, accounts_dir)
    assert loaded == original
