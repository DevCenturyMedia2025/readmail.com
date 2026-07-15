"""
Modulo de configuracion centralizada para ReadMail.

Este archivo prepara la nueva arquitectura, pero todavia no reemplaza
la configuracion actual de reademail.py.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parents[2]


def env_first(*names: str, default: Optional[str] = None) -> str:
    """
    Devuelve el primer valor de entorno disponible entre varios nombres.
    Sirve para mantener compatibilidad con variables antiguas y nuevas.
    """
    for name in names:
        value = os.environ.get(name)
        if value is not None and str(value).strip() != "":
            return value
    return default if default is not None else ""


def env_bool(name: str, default: bool = False) -> bool:
    """
    Convierte una variable de entorno a booleano.
    Acepta valores comunes como true, 1, yes, y.
    """
    value = os.environ.get(name)
    if value is None:
        return default

    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "y", "si", "sí"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    return default


def env_int(name: str, default: int = 0) -> int:
    """
    Convierte una variable de entorno a entero.
    Si viene vacia o invalida, devuelve el default.
    """
    value = os.environ.get(name)
    if value is None or str(value).strip() == "":
        return default

    try:
        return int(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    app_env: str
    dry_run: bool
    base_dir: Path

    gcp_project_id: str
    pubsub_subscription_id: str
    pubsub_topic_full: str
    pubsub_pull_max: int
    pubsub_ack_deadline_seconds: int

    gmail_label_ids: List[str]
    gmail_accounts: List[str]
    accounts_dir: Path
    gmail_watch_state_file: Path

    archive_on_status_legacy: bool
    archive_approved: bool
    archive_rejected: bool
    archive_admin: bool
    archive_note_credit: bool
    archive_review: bool

    alt_recipient_enabled: bool
    alt_fallback_email: str


def load_settings() -> Settings:
    """
    Carga configuracion desde .env y variables del sistema.

    Por ahora este modulo es preparatorio. No debe modificar el comportamiento
    actual hasta que reademail.py lo importe explicitamente en una rama posterior.
    """
    load_dotenv()

    app_env = env_first("APP_ENV", default="development").lower()
    dry_run = env_bool("DRY_RUN", default=(app_env != "production"))

    accounts_dir = Path(env_first("ACCOUNTS_DIR", default=str(BASE_DIR / "accounts")))
    gmail_watch_state_file = Path(
        env_first(
            "GMAIL_WATCH_STATE_FILE",
            default=str(BASE_DIR / "gmail_watch_state.json"),
        )
    )

    gmail_accounts_raw = env_first("GMAIL_ACCOUNTS", default="")
    gmail_accounts = [item.strip() for item in gmail_accounts_raw.split(",") if item.strip()]

    gmail_label_ids_raw = env_first("GMAIL_LABEL_IDS", default="INBOX")
    gmail_label_ids = [item.strip() for item in gmail_label_ids_raw.split(",") if item.strip()]

    archive_on_status_legacy = env_bool("ARCHIVE_ON_STATUS", default=True)

    return Settings(
        app_env=app_env,
        dry_run=dry_run,
        base_dir=BASE_DIR,

        gcp_project_id=env_first("GCP_PROJECT_ID"),
        pubsub_subscription_id=env_first("PUBSUB_SUBSCRIPTION", "PUBSUB_SUBSCRIPTION_ID"),
        pubsub_topic_full=env_first("PUBSUB_TOPIC_FULL"),
        pubsub_pull_max=env_int("PUBSUB_PULL_MAX", default=10),
        pubsub_ack_deadline_seconds=env_int("PUBSUB_ACK_DEADLINE_SECONDS", default=600),

        gmail_label_ids=gmail_label_ids,
        gmail_accounts=gmail_accounts,
        accounts_dir=accounts_dir,
        gmail_watch_state_file=gmail_watch_state_file,

        archive_on_status_legacy=archive_on_status_legacy,
        archive_approved=env_bool("ARCHIVE_APPROVED", default=archive_on_status_legacy),
        archive_rejected=env_bool("ARCHIVE_REJECTED", default=archive_on_status_legacy),
        archive_admin=env_bool("ARCHIVE_ADMIN", default=True),
        archive_note_credit=env_bool("ARCHIVE_NOTE_CREDIT", default=True),
        archive_review=env_bool("ARCHIVE_REVIEW", default=False),

        alt_recipient_enabled=env_bool("ALT_RECIPIENT_ENABLED", default=False),
        alt_fallback_email=env_first("ALT_FALLBACK_EMAIL", default=""),
    )
