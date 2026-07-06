import app.config.settings as settings_module


def test_load_settings_from_environment(monkeypatch):
    monkeypatch.setattr(settings_module, "load_dotenv", lambda: None)

    monkeypatch.setenv("APP_ENV", "staging")
    monkeypatch.setenv("DRY_RUN", "true")
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("PUBSUB_SUBSCRIPTION", "test-subscription")
    monkeypatch.setenv("PUBSUB_TOPIC_FULL", "projects/test-project/topics/test-topic")
    monkeypatch.setenv("GMAIL_ACCOUNTS", "one@example.com,two@example.com")
    monkeypatch.setenv("GMAIL_LABEL_IDS", "INBOX,IMPORTANT")
    monkeypatch.setenv("ACCOUNTS_DIR", "accounts")

    settings = settings_module.load_settings()

    assert settings.app_env == "staging"
    assert settings.dry_run is True
    assert settings.gcp_project_id == "test-project"
    assert settings.pubsub_subscription_id == "test-subscription"
    assert settings.pubsub_topic_full == "projects/test-project/topics/test-topic"
    assert settings.gmail_accounts == ["one@example.com", "two@example.com"]
    assert settings.gmail_label_ids == ["INBOX", "IMPORTANT"]


def test_dry_run_defaults_to_true_outside_production(monkeypatch):
    monkeypatch.setattr(settings_module, "load_dotenv", lambda: None)

    monkeypatch.delenv("DRY_RUN", raising=False)
    monkeypatch.setenv("APP_ENV", "staging")

    settings = settings_module.load_settings()

    assert settings.dry_run is True


def test_dry_run_defaults_to_false_in_production(monkeypatch):
    monkeypatch.setattr(settings_module, "load_dotenv", lambda: None)

    monkeypatch.delenv("DRY_RUN", raising=False)
    monkeypatch.setenv("APP_ENV", "production")

    settings = settings_module.load_settings()

    assert settings.dry_run is False
