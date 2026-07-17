from reademail import build_token_alert_email, should_send_token_alert


def test_build_token_alert_email_includes_account_and_command():
    failed_account = "facturacion@example.com"

    subject, body = build_token_alert_email(failed_account)

    assert failed_account in subject
    assert f"python reademail.py --authorize-account {failed_account}" in body
    assert failed_account in body


def test_should_send_token_alert_when_never_sent():
    assert should_send_token_alert(None, now=1_000, cooldown_hours=12) is True


def test_should_send_token_alert_when_cooldown_elapsed():
    now = 100_000
    last_sent = now - (12 * 60 * 60)

    assert should_send_token_alert(last_sent, now=now, cooldown_hours=12) is True


def test_should_not_send_token_alert_inside_cooldown():
    now = 100_000
    last_sent = now - (11 * 60 * 60)

    assert should_send_token_alert(last_sent, now=now, cooldown_hours=12) is False
