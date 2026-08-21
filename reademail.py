# -*- coding: utf-8 -*-
"""
Programa de facturación Century - versión ajustada al nuevo flujo

Flujo implementado:
1. Lee asunto y cuerpo.
2. Si detecta NIT explícito y está en lista blanca del Google Sheet -> etiqueta ADMINISTRATIVA.
3. Si hay ZIP, extrae ZIP y ZIP dentro de ZIP (con límites anti zip-bomb) y unifica PDFs/XML.
4. Si no queda al menos 1 PDF unificado -> etiqueta REVISION MANUAL.
5. Barrera doble de Nota de Crédito:
   - nombre del PDF
   - texto interno del PDF
   Si detecta cualquiera -> etiqueta NOTA DE CREDITO.
6. Si existe XML unificado -> FACTURA ELECTRONICA; si no -> CUENTA DE COBRO.
7. Valida mínimos:
   - Electrónica: mínimo 2 PDF
   - Cuenta de cobro: mínimo 4 PDF
8. Validación de documentos:
   - Tiene PDF de orden de compra
   - Identifica cliente
   - Tiene PDF o sello de OK de compras
9. Si todo cumple -> APROBADO y notifica.
10. Si falla -> RECHAZADO y notifica el motivo específico.

No usa OCR. Lee texto digital del PDF.
Si el PDF es escaneado como imagen sin capa de texto, varias validaciones podrían no detectar contenido.
"""

# region 📦 Imports y dependencias
import base64
import html
import io
import json
import logging
import math
import os
import os.path
import re
import shutil
import time
import urllib.parse
import urllib.request
import zipfile
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import parseaddr
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Tuple

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import pubsub_v1

from app.services.alternate_recipient import is_tech_provider, resolve_alternate_recipient

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    from PyPDF2 import PdfReader  # type: ignore


# endregion
# region 🔐 Scopes de Google
# ============================================================
# SCOPES
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    # Este scope de escritura EXIGE re-autorizar todas las cuentas OAuth existentes.
    "https://www.googleapis.com/auth/spreadsheets",
]


# endregion
# region ⚙️ Configuración y variables de entorno
# ============================================================
# ENV / CONFIG
# Compatibilidad: este bloque admite tanto las variables NUEVAS
# como las del .env anterior para que no tengas que rehacerlo.
# ============================================================
def env_first(*names: str, default: str = "") -> str:
    for name in names:
        value = os.environ.get(name)
        if value is not None and str(value).strip() != "":
            return str(value).strip()
    return default


def env_bool(*names: str, default: bool = False) -> bool:
    value = env_first(*names, default="")
    if value == "":
        return default
    return value.lower() in ("1", "true", "yes", "y", "si")


def env_int(*names: str, default: int = 0) -> int:
    value = env_first(*names, default="")
    if value == "":
        return default
    try:
        return int(value)
    except Exception:
        return default


GCP_PROJECT_ID = env_first("GCP_PROJECT_ID")
PUBSUB_SUBSCRIPTION_ID = env_first("PUBSUB_SUBSCRIPTION", "PUBSUB_SUBSCRIPTION_ID")
PUBSUB_TOPIC_FULL = env_first("PUBSUB_TOPIC_FULL")
WATCH_LABEL_IDS = [x.strip() for x in env_first("GMAIL_LABEL_IDS", default="INBOX").split(",") if x.strip()]
GMAIL_SYSTEM_LABEL_IDS = {"INBOX", "SENT", "DRAFT", "TRASH", "SPAM", "STARRED", "IMPORTANT", "UNREAD"}
MODO_PRUEBAS = env_bool("MODO_PRUEBAS", default=False)
ETIQUETA_PRUEBAS = env_first("ETIQUETA_PRUEBAS", default="pruebas")
COMPRAS_EMAIL = env_first("COMPRAS_EMAIL", default="")
DRY_RUN = env_bool("DRY_RUN", default=False)
AUTO_FILL_NIT_ENABLED = env_bool("AUTO_FILL_NIT_ENABLED", default=False)
LIMITE_ANTIGUEDAD_ENABLED = env_bool("LIMITE_ANTIGUEDAD_ENABLED", default=True)
MAX_DIAS_ANTIGUEDAD = env_int("MAX_DIAS_ANTIGUEDAD", default=5)
WHATSAPP_ALERT_ENABLED = env_bool("WHATSAPP_ALERT_ENABLED", default=False)
WHATSAPP_PHONE = env_first("WHATSAPP_PHONE", default="")
WHATSAPP_APIKEY = env_first("WHATSAPP_APIKEY", default="")
WHATSAPP_COOLDOWN_MIN = env_int("WHATSAPP_COOLDOWN_MIN", default=15)

_WHATSAPP_ALERT_CACHE: Dict[str, float] = {}

SHEET_ID = env_first("CLIENT_SHEET_ID")
SHEET_RANGE = env_first("CLIENT_SHEET_RANGE", default="Clientes!A:Z")
CLIENT_LOOKUP_RANGE = env_first("CLIENT_LOOKUP_RANGE", default="Clientes!A:Z")
SHEET_BACKUP_ENABLED = env_bool("SHEET_BACKUP_ENABLED", default=True)
SHEET_BACKUP_DIR = env_first("SHEET_BACKUP_DIR", default="backups")
SHEET_BACKUP_KEEP_DAYS = env_int("SHEET_BACKUP_KEEP_DAYS", default=30)
ADMIN_SHEET_TABS = ("Administrativas", "CajaMenor")
KNOWN_ENTITY_TABS = ("Clientes", "Terceros")
ADMIN_NIT_HEADER_LABELS = {"nit", "nit cliente", "tax id", "identificacion", "identificación"}
ADMIN_NAME_HEADER_LABELS = {
    "nombre",
    "nombre proveedor",
    "nombre tercero",
    "razon social",
    "razón social",
    "cliente",
    "empresa",
    "proveedor",
    "proverdor",
    "tercero",
    "beneficiario",
    "entidad",
}
ACTIVE_VALUES = {x.strip().lower() for x in env_first("ACTIVE_VALUES", default="activo,active,si,yes,1,true").split(",") if x.strip()}

# Etiquetas nuevas con fallback al .env viejo
LABEL_ADMIN_NAME = env_first("LABEL_ADMIN_NAME", default="ADMINISTRATIVA")
LABEL_REVIEW_NAME = env_first("LABEL_REVIEW_NAME", default="REVISIÓN MANUAL")
LABEL_NOTE_CREDIT_NAME = env_first("LABEL_NOTE_CREDIT_NAME", default="NOTA DE CRÉDITO")
LABEL_APPROVED_NAME = env_first("LABEL_APPROVED_NAME", "LABEL_ACCEPTED_NAME", default="APROBADOS")
LABEL_REJECTED_NAME = env_first("LABEL_REJECTED_NAME", default="RECHAZADOS")

ALT_RECIPIENT_ENABLED = env_bool("ALT_RECIPIENT_ENABLED", default=False)
ALT_FALLBACK_EMAIL = env_first("ALT_FALLBACK_EMAIL", default="")

# Archivo / mover de inbox
# Si existe ARCHIVE_ON_STATUS del .env viejo, lo reutilizamos para aprobado y rechazado.
ARCHIVE_ON_STATUS_LEGACY = env_bool("ARCHIVE_ON_STATUS", default=True)
ARCHIVE_APPROVED = env_bool("ARCHIVE_APPROVED", default=ARCHIVE_ON_STATUS_LEGACY)
ARCHIVE_REJECTED = env_bool("ARCHIVE_REJECTED", default=ARCHIVE_ON_STATUS_LEGACY)
ARCHIVE_ADMIN = env_bool("ARCHIVE_ADMIN", default=True)
ARCHIVE_NOTE_CREDIT = env_bool("ARCHIVE_NOTE_CREDIT", default=True)
ARCHIVE_REVIEW = env_bool("ARCHIVE_REVIEW", default=False)

# Compatibilidad con el .env anterior
ONLY_WITH_ATTACHMENTS = env_bool("ONLY_WITH_ATTACHMENTS", "ONLY_WITH_ATTACHMENTS", default=True)
PROCESSED_CACHE_LIMIT = env_int("PROCESSED_CACHE_LIMIT", default=3000)
RADICADO_PREFIX = env_first("RADICADO_PREFIX", default="RAD")
RADICADO_PAD = env_int("RADICADO_PAD", default=6)
RADICADO_RESET_DAILY = env_bool("RADICADO_RESET_DAILY", default=True)
RADICADO_MAP_LIMIT = env_int("RADICADO_MAP_LIMIT", default=10000)

PUBSUB_PULL_MAX = env_int("PUBSUB_PULL_MAX", default=10)
PUBSUB_ACK_DEADLINE_SECONDS = env_int("PUBSUB_ACK_DEADLINE_SECONDS", default=600)
IDLE_SLEEP_SEC = float(env_first("IDLE_SLEEP_SEC", default="1.0"))
WATCH_RENEW_WINDOW_MS = env_int("WATCH_RENEW_WINDOW_MS", default=60 * 60 * 1000)
TOKEN_ALERT_EMAIL = env_first("TOKEN_ALERT_EMAIL", default="")
INTERACTIVE_AUTH = env_bool("INTERACTIVE_AUTH", default=False)
TOKEN_ALERT_COOLDOWN_HOURS = env_int("TOKEN_ALERT_COOLDOWN_HOURS", default=12)

# Cuenta de cobro: validación documental por tipos.
# - Si existe REQUIRED_PDF_COUNT del .env viejo, lo tomamos como fallback SOLO para cuenta de cobro.
MIN_PDF_CC = env_int("MIN_PDF_CUENTA_COBRO", "REQUIRED_PDF_COUNT", default=4)
INCOMPLETE_FILES_MESSAGE = (
    "Se identificaron archivos incompletos. Agradecemos revisar y confirmar que la documentación "
    "esté completa antes de realizar el envío."
)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
STATE_FILE = env_first("GMAIL_WATCH_STATE_FILE", default=os.path.join(_BASE_DIR, "gmail_watch_state.json"))

# Multi-cuenta
GMAIL_ACCOUNTS_RAW = env_first("GMAIL_ACCOUNTS", default="")
ACCOUNTS_DIR = env_first("ACCOUNTS_DIR", default=os.path.join(_BASE_DIR, "accounts"))
GMAIL_ACCOUNTS: List[str] = [a.strip() for a in GMAIL_ACCOUNTS_RAW.split(",") if a.strip()]

# ZIP safety
MAX_ZIP_BYTES = env_int("MAX_ZIP_BYTES", default=25 * 1024 * 1024)
MAX_ZIP_FILES = env_int("MAX_ZIP_FILES", default=250)
MAX_ZIP_TOTAL_UNCOMPRESSED = env_int("MAX_ZIP_TOTAL_UNCOMPRESSED", default=150 * 1024 * 1024)
MAX_ZIP_SINGLE_FILE = env_int("MAX_ZIP_SINGLE_FILE", default=25 * 1024 * 1024)
MAX_ZIP_NESTING = env_int("MAX_ZIP_NESTING", default=2)

# Detection patterns
OK_COMPRAS_PATTERNS = [
    x.strip() for x in env_first(
        "OK_COMPRAS_PATTERNS",
        default=(
            "ok compras,aprobado compras,aprobada compras,visto bueno compras,vb compras,vobo compras,"
            "aprobacion compras,autorizado compras,visto bueno para radicacion,aprobado para radicar,"
            "autorizado para radicar,cuenta con visto bueno,recibida a satisfaccion"
        ),
    ).split(",") if x.strip()
]

PURCHASE_ORDER_FILENAME_REGEXES = (
    re.compile(r"\borden\s+(?:de\s+)?compra\b"),
    re.compile(r"\borden\b"),
    re.compile(r"(?<![a-z0-9])oc(?=$|[\s_.-])"),
    re.compile(r"(?<![a-z0-9])o\s*\.\s*c\s*\.?(?![a-z0-9])"),
)
PURCHASE_ORDER_FILENAME_EXCLUSIONS = re.compile(r"\borden\s+de\s+(?:servicio|trabajo)\b")
PURCHASE_ORDER_HEADER_REGEX = re.compile(
    r"^(?:orden\s+(?:de\s+)?compra|orden\s+(?:n(?:o|ro|umero)\.?|#)\b|"
    r"o\s*\.\s*c\s*\.?|oc(?:\b|(?=[\s_.-])))"
)

OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX = r"(?:\s+(?:de|por|del|por\s+el\s+area\s+de))?"
OK_COMPRAS_WITH_PURCHASES_REGEX = re.compile(
    rf"^(?P<term>.+?){OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras$"
)
OK_COMPRAS_CLAUSE_SEPARATOR_REGEX = re.compile(
    r"[.!?;:\n]+|\b(?:pero|aunque)\b",
    re.IGNORECASE,
)
OK_COMPRAS_POST_NEGATIVE_REGEX = re.compile(
    r"^\s+(?:(?:aun\s+)?esta\s+pendiente|aun\s+no\s+llega|no\s+ha\s+llegado|"
    r"queda\s+pendiente|sigue(?:\s+pendiente|\s+en\s+espera)|"
    r"se\s+encuentra\s+pendiente)\b"
)
OK_COMPRAS_FILENAME_REGEXES = (
    re.compile(rf"\bok{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras\b"),
    re.compile(rf"\bvisto\s+bueno(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvo\s*bo(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvobo(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvb{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras\b"),
    re.compile(rf"\baprob(?:ado|ada|acion){OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras\b"),
    re.compile(rf"\bautorizado{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras\b"),
)

OK_COMPRAS_NEGATIVE_REGEXES = [
    re.compile(pattern)
    for pattern in (
        r"\bpendiente(?:\s+de)?\b",
        r"\bsin\b",
        r"\bfalta\b",
        r"\bno\s+tiene\b",
        r"\bno\s+cuenta\s+con\b",
        r"\brequiere\b",
        r"\baun\s+no\b",
        r"\btodavia\s+no\b",
        r"\bno\s+hay\b",
        r"\besta\s+pendiente\b",
        r"\bqueda\s+pendiente\b",
        r"\bno\s+llega\b",
        r"\bno\s+ha\s+llegado\b",
        r"\bno\s+tenemos\b",
        r"\bno\s+adjuntan\b",
        r"\bni\b",
        r"\ben\s+espera\b",
    )
]
OK_COMPRAS_FILENAME_NEGATIVE_REGEXES = (
    *OK_COMPRAS_NEGATIVE_REGEXES,
    re.compile(
        r"\bno\s+(?=(?:ok|visto\s+bueno|vo\s*bo|vobo|vb|"
        r"aprob(?:ado|ada|acion)|autorizado)\b)"
    ),
)

NIT_REGEX = re.compile(r"\bnit\b\s*[:#\-]?\s*([0-9][0-9.\-]{5,20})", re.IGNORECASE)

# endregion
# region 🗂️ Modelos de datos
# ============================================================
# DATA MODELS
# ============================================================
@dataclass
class ClientRecord:
    name: str
    normalized_name: str
    nit: Optional[str] = None
    normalized_nit: Optional[str] = None
    contact_email: Optional[str] = None
    active: bool = True
    raw_row: Dict[str, str] = field(default_factory=dict)


@dataclass
class ClientMatchResult:
    record: Optional[ClientRecord] = None
    raw: Optional[str] = None
    source: str = ""


class AdminLookup(NamedTuple):
    admin_nits: Set[str]
    admin_names: Set[str]
    admin_rows_sin_nit: Dict[str, Tuple[str, int]]


class RegisteredLookup(NamedTuple):
    registered_nits: Set[str]
    registered_names: Set[str]
    registered_docs: Dict[str, Dict[str, str]]


@dataclass
class UnifiedFile:
    name: str
    mime_type: str
    data: bytes
    source: str
    extracted_text: str = ""

    @property
    def lower_name(self) -> str:
        return self.name.lower()

    @property
    def is_pdf(self) -> bool:
        return self.lower_name.endswith(".pdf") or self.mime_type == "application/pdf"

    @property
    def is_xml(self) -> bool:
        return self.lower_name.endswith(".xml") or self.mime_type in ("application/xml", "text/xml")

    @property
    def is_image(self) -> bool:
        return self.lower_name.endswith((".jpg", ".jpeg", ".png")) or self.mime_type in ("image/jpeg", "image/png")


# endregion
# region 🔧 Utilidades básicas de texto
# ============================================================
# BASIC HELPERS
# ============================================================
def strip_accents(value: str) -> str:
    if not value:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch))


def normalize_text(value: str) -> str:
    value = strip_accents(value or "")
    value = value.lower()
    value = re.sub(r"\s+", " ", value).strip()
    return value


def normalize_alnum(value: str) -> str:
    value = strip_accents(value or "")
    value = value.lower()
    return re.sub(r"[^a-z0-9]", "", value)


def normalize_nit(value: str) -> str:
    return re.sub(r"\D", "", value or "")


EMAIL_RE = re.compile(r"^[^@\s<>]+@[^@\s<>]+\.[^@\s<>]+$")


def ensure_list(value) -> List:
    return value if isinstance(value, list) else []


def decode_body(data: Optional[str]) -> str:
    if not data:
        return ""
    try:
        missing_padding = len(data) % 4
        if missing_padding:
            data += "=" * (4 - missing_padding)
        decoded_bytes = base64.urlsafe_b64decode(data)
        return decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def get_header(payload: Dict, name: str) -> str:
    target = name.lower()
    for header in ensure_list(payload.get("headers")):
        if (header.get("name", "") or "").lower() == target:
            return header.get("value", "") or ""
    return ""


def html_to_text(value: str) -> str:
    if not value:
        return ""
    value = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", value)
    value = re.sub(r"(?i)<br\s*/?>", "\n", value)
    value = re.sub(r"(?i)</p\s*>", "\n", value)
    value = re.sub(r"(?i)</div\s*>", "\n", value)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    value = re.sub(r"[ \t\r\f\v]+", " ", value)
    value = re.sub(r"\n\s+", "\n", value)
    return value.strip()


def extract_plain_text(payload: Dict) -> str:
    if not payload:
        return ""

    body = payload.get("body", {}) or {}
    mime_type = payload.get("mimeType", "")
    if body.get("data") and mime_type == "text/html":
        return html_to_text(decode_body(body.get("data")))
    if body.get("data") and (mime_type == "text/plain" or not payload.get("parts")):
        return decode_body(body.get("data"))

    html_fallback = ""
    for part in ensure_list(payload.get("parts")):
        part_mime_type = part.get("mimeType", "")
        if part_mime_type == "text/plain":
            txt = decode_body((part.get("body", {}) or {}).get("data"))
            if txt:
                return txt
        if part_mime_type == "text/html":
            txt = html_to_text(decode_body((part.get("body", {}) or {}).get("data")))
            if txt and not html_fallback:
                html_fallback = txt
            continue
        nested = extract_plain_text(part)
        if nested:
            return nested
    return html_fallback


def extract_sender_email(from_header: str) -> Optional[str]:
    _, email = parseaddr(from_header or "")
    return email or None


_NO_REPLY_RE = re.compile(
    r"(no.?reply|noreply|bounce|mailer-daemon|postmaster|notifications?@|notificacion(?:es)?@|avisos?@|alertas?@|donotreply|do-not-reply)",
    re.IGNORECASE,
)

def is_no_reply_sender(email: str) -> bool:
    return bool(_NO_REPLY_RE.search(email or ""))


def create_raw_email(to_email: str, subject: str, body: str, extra_headers: Optional[Dict[str, str]] = None) -> str:
    msg = MIMEText(body, _charset="utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject
    for name, value in (extra_headers or {}).items():
        if value:
            msg[name] = value
    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")


def create_forward_email(to_email: str, subject: str, body: str, attachments: List[Dict[str, object]]) -> str:
    msg = MIMEMultipart("mixed")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, _charset="utf-8"))

    for attachment in attachments:
        filename = str(attachment.get("filename") or "adjunto")
        mime_type = str(attachment.get("mime_type") or "application/octet-stream")
        if "/" not in mime_type:
            mime_type = "application/octet-stream"
        maintype, subtype = mime_type.split("/", 1)
        if not maintype or not subtype:
            maintype, subtype = "application", "octet-stream"

        part = MIMEBase(maintype, subtype)
        part.set_payload(attachment.get("data") or b"")
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)

    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


# endregion
# region 💾 Estado y persistencia (JSON)
# ============================================================
# STATE
# ============================================================
def _state_file_for_account(account_id: Optional[str]) -> str:
    if not account_id:
        return STATE_FILE
    return os.path.join(ACCOUNTS_DIR, account_id, "gmail_watch_state.json")


def load_state(account_id: Optional[str] = None) -> Dict:
    path = _state_file_for_account(account_id)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state: Dict, account_id: Optional[str] = None) -> None:
    path = _state_file_for_account(account_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def state_get_processed_set(state: Dict) -> Set[str]:
    arr = state.get("processed_message_ids") or []
    return {str(x) for x in arr if x is not None}


def state_add_processed(state: Dict, message_id: str) -> None:
    arr = state.get("processed_message_ids") or []
    if not isinstance(arr, list):
        arr = []
    mid = str(message_id)
    if mid not in arr:
        arr.append(mid)
    if len(arr) > PROCESSED_CACHE_LIMIT:
        arr = arr[-PROCESSED_CACHE_LIMIT:]
    state["processed_message_ids"] = arr


def state_has_replied(state: Dict, message_id: str) -> bool:
    arr = state.get("replied_message_ids") or []
    return str(message_id) in {str(x) for x in arr}


def state_mark_replied(state: Dict, message_id: str) -> None:
    arr = state.get("replied_message_ids") or []
    if not isinstance(arr, list):
        arr = []
    mid = str(message_id)
    if mid not in arr:
        arr.append(mid)
    if len(arr) > PROCESSED_CACHE_LIMIT:
        arr = arr[-PROCESSED_CACHE_LIMIT:]
    state["replied_message_ids"] = arr


def get_or_create_radicado(message_id: str, state: Dict) -> str:
    mappings = state.get("message_radicados") or {}
    if not isinstance(mappings, dict):
        mappings = {}

    mid = str(message_id)
    if mid in mappings:
        return mappings[mid]

    today = today_yyyymmdd()
    last_date = str(state.get("radicado_date") or "")
    counter = int(state.get("radicado_counter") or 0)

    if RADICADO_RESET_DAILY and last_date != today:
        counter = 0

    counter += 1
    state["radicado_counter"] = counter
    state["radicado_date"] = today

    if RADICADO_RESET_DAILY:
        radicado = f"{RADICADO_PREFIX}-{today}-{counter:0{RADICADO_PAD}d}"
    else:
        radicado = f"{RADICADO_PREFIX}-{counter:0{RADICADO_PAD}d}"

    mappings[mid] = radicado
    if len(mappings) > RADICADO_MAP_LIMIT:
        keys = list(mappings.keys())[-RADICADO_MAP_LIMIT:]
        mappings = {k: mappings[k] for k in keys}

    state["message_radicados"] = mappings
    return radicado


# endregion
# region 🔑 OAuth y servicios de Google
# ============================================================
# OAUTH / SERVICES
# ============================================================
class TokenAuthError(RuntimeError):
    def __init__(self, account_email: str):
        self.account_email = account_email
        super().__init__(f"Token OAuth inválido para {account_email}")


def get_oauth_creds(
    account_dir: Optional[str] = None,
    account_email: Optional[str] = None,
    interactive_auth: Optional[bool] = None,
) -> Credentials:
    base = account_dir if account_dir else _BASE_DIR
    failed_account = account_email or os.path.basename(os.path.normpath(base)) or "cuenta unica"
    allow_interactive = INTERACTIVE_AUTH if interactive_auth is None else interactive_auth
    token_path = os.path.join(base, "token.json")
    # credentials.json: buscar en el directorio de la cuenta; si no existe, usar el del raíz (client secret compartido)
    creds_path = os.path.join(base, "credentials.json")
    if not os.path.exists(creds_path):
        creds_path = os.path.join(_BASE_DIR, "credentials.json")

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                try:
                    os.remove(token_path)
                except FileNotFoundError:
                    pass
                creds = None

        if not creds or not creds.valid:
            if not allow_interactive:
                raise TokenAuthError(failed_account)
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)

        os.makedirs(base, exist_ok=True)
        with open(token_path, "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return creds


# endregion
# region 📊 Google Sheets: catálogo de clientes y lista blanca
# ============================================================
# SHEETS: CLIENTS + NIT WHITELIST
# ============================================================
def _header_aliases() -> Dict[str, Set[str]]:
    return {
        "cliente": {"cliente", "razon social", "razón social", "nombre cliente", "client", "empresa", "proveedor", "proverdor"},
        "nit": {"nit", "nit cliente", "tax id"},
        "estado": {"estado", "activo", "status"},
        "email": {"email", "correo", "email contacto", "correo contacto", "correo electronico", "correo electrónico", "email de contacto"},
    }


def _resolve_column_indexes(headers: List[str]) -> Dict[str, Optional[int]]:
    aliases = _header_aliases()
    normalized_headers = [normalize_text(h) for h in headers]
    resolved: Dict[str, Optional[int]] = {"cliente": None, "nit": None, "estado": None, "email": None}

    for logical_name, options in aliases.items():
        for idx, header in enumerate(normalized_headers):
            if header in {normalize_text(o) for o in options}:
                resolved[logical_name] = idx
                break

    if resolved["cliente"] is None and headers:
        resolved["cliente"] = 0
    if resolved["nit"] is None and len(headers) > 1:
        resolved["nit"] = 1
    return resolved


def _looks_like_header_row(row: List[str]) -> bool:
    aliases = _header_aliases()
    header_tokens = {normalize_text(option) for options in aliases.values() for option in options}
    normalized = [normalize_text(cell) for cell in row]
    return any(cell in header_tokens for cell in normalized)


def _client_records_from_values(values: List[List[str]], sheet_range: str) -> List[ClientRecord]:
    if not values:
        return []

    first_row = [str(x).strip() for x in values[0]]
    has_header = _looks_like_header_row(first_row)
    headers = first_row if has_header else ["cliente", "nit", "estado"]
    indexes = _resolve_column_indexes(headers)
    data_rows = values[1:] if has_header else values

    catalog: List[ClientRecord] = []
    for row in data_rows:
        if not row:
            continue

        cliente = ""
        nit = ""
        estado = ""
        contact_email = ""

        if indexes.get("cliente") is not None and indexes["cliente"] < len(row):
            cliente = str(row[indexes["cliente"]]).strip()
        if indexes.get("nit") is not None and indexes["nit"] < len(row):
            nit = str(row[indexes["nit"]]).strip()
        if indexes.get("estado") is not None and indexes["estado"] < len(row):
            estado = str(row[indexes["estado"]]).strip()
        if indexes.get("email") is not None and indexes["email"] < len(row):
            candidate_email = str(row[indexes["email"]]).strip()
            if EMAIL_RE.fullmatch(candidate_email):
                contact_email = candidate_email

        if not cliente and not nit:
            continue

        active = True
        if estado:
            active = normalize_text(estado) in ACTIVE_VALUES

        record = ClientRecord(
            name=cliente or nit,
            normalized_name=normalize_alnum(cliente or nit),
            nit=nit or None,
            normalized_nit=normalize_nit(nit) or None,
            contact_email=contact_email or None,
            active=active,
            raw_row={
                **{headers[i] if i < len(headers) else str(i): str(v) for i, v in enumerate(row)},
                "__range": sheet_range,
            },
        )
        catalog.append(record)

    print(
        f"📄 Catálogo rango {sheet_range}: {len(catalog)} registros | "
        f"encabezado={'sí' if has_header else 'no'} | "
        f"muestra={[record.name for record in catalog[:2]]}"
    )
    return catalog


def _load_client_catalog_range(sheets_service, sheet_range: str) -> List[ClientRecord]:
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=sheet_range,
    ).execute()
    values = result.get("values", []) or []
    if not values:
        print(f"⚠️ La hoja/rango no tiene datos: {sheet_range}")
        return []

    return _client_records_from_values(values, sheet_range)


def load_client_catalog(sheets_service) -> List[ClientRecord]:
    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Se seguirá sin catálogo/lista blanca.")
        return []

    ranges = []
    for sheet_range in [SHEET_RANGE, CLIENT_LOOKUP_RANGE]:
        if sheet_range and sheet_range not in ranges:
            ranges.append(sheet_range)

    catalog: List[ClientRecord] = []
    seen = set()
    for sheet_range in ranges:
        try:
            records = _load_client_catalog_range(sheets_service, sheet_range)
        except Exception as e:
            print(f"⚠️ No pude cargar catálogo desde {sheet_range}: {e}")
            continue
        for record in records:
            key = (record.normalized_name, record.normalized_nit or "")
            if key in seen:
                continue
            seen.add(key)
            catalog.append(record)

    print(f"✅ Catálogo/lista blanca cargado: {len(catalog)} registros desde {', '.join(ranges)}")
    return catalog


def _strip_invisible_characters(value: str) -> str:
    """Elimina caracteres Unicode de formato invisibles sin alterar el texto visible."""
    return "".join(character for character in str(value or "") if unicodedata.category(character) != "Cf")


def normalize_admin_name(value: str) -> str:
    """
    Normaliza nombres administrativos conservando espacios y signos.

    Solo elimina puntos precedidos por una letra para hacer equivalentes las
    siglas puntuadas y no puntuadas: S.A.S. -> sas y LTDA. -> ltda.
    """
    normalized = normalize_text(_strip_invisible_characters(value))
    return re.sub(r"(?<=[a-z])\.(?=[a-z]|$|[^a-z0-9])", "", normalized)


def _normalize_admin_nit_cell(value) -> str:
    if value is None or isinstance(value, bool):
        return ""

    if isinstance(value, int):
        raw_value = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return ""
        raw_value = str(int(value))
    else:
        raw_value = str(value).strip()
        decimal_match = re.fullmatch(r"([+-]?\d+)\.0+", raw_value)
        if decimal_match:
            raw_value = decimal_match.group(1)

    normalized_nit = normalize_nit(raw_value)
    if len(normalized_nit) < 6:
        return ""

    alnum_characters = re.findall(r"[a-z0-9]", normalize_text(raw_value))
    if not alnum_characters:
        return ""
    numeric_ratio = sum(character.isdigit() for character in alnum_characters) / len(alnum_characters)
    return normalized_nit if numeric_ratio >= 0.70 else ""


def _normalize_admin_name_cell(value) -> str:
    if value is None or isinstance(value, (bool, int, float)):
        return ""
    normalized_name = normalize_admin_name(str(value))
    if not re.search(r"[a-z]", normalized_name):
        return ""
    return normalized_name if len(normalize_alnum(normalized_name)) >= 4 else ""


def _admin_nit_index_forms(normalized_nit: str) -> Set[str]:
    """
    Genera las claves exactas de búsqueda para un NIT leído de la hoja.

    Nueve dígitos se conservan tal cual. Diez dígitos se indexan completos y
    también sin el último dígito, tratado como posible DV. Las demás longitudes
    válidas conservan únicamente su forma completa.
    """
    forms = {normalized_nit}
    if len(normalized_nit) == 10:
        forms.add(normalized_nit[:9])
    return forms


def _looks_like_admin_header_row(row: List[object]) -> bool:
    normalized_cells = {
        normalize_text(_strip_invisible_characters(str(value)))
        for value in row
        if value is not None
    }
    return bool(normalized_cells & (ADMIN_NIT_HEADER_LABELS | ADMIN_NAME_HEADER_LABELS))


def _admin_lookup_from_values(
    values: List[List[object]],
    tab: str,
    admin_nits: Set[str],
    admin_names: Set[str],
    admin_rows_sin_nit: Dict[str, Tuple[str, int]],
) -> Tuple[int, int]:
    if not values:
        return 0, 0

    has_header = _looks_like_admin_header_row(values[0])
    detected_nits: Set[str] = set()
    detected_names: Set[str] = set()
    for row_number, row in enumerate(values, start=1):
        if has_header and row_number == 1:
            continue
        if not row:
            continue

        nit = ""
        nit_cell_index: Optional[int] = None
        for cell_index, cell_value in enumerate(row):
            candidate_nit = _normalize_admin_nit_cell(cell_value)
            if candidate_nit:
                nit = candidate_nit
                nit_cell_index = cell_index
                break

        name = ""
        for cell_index, cell_value in enumerate(row):
            if cell_index == nit_cell_index:
                continue
            candidate_name = _normalize_admin_name_cell(cell_value)
            if candidate_name:
                name = candidate_name
                break

        if nit:
            detected_nits.add(nit)
            admin_nits.update(_admin_nit_index_forms(nit))
        if name:
            detected_names.add(name)
            admin_names.add(name)
            first_cell_is_empty = not row or row[0] is None or not str(row[0]).strip()
            # Auto-fill escribe en A: solo se registra una fila si A está realmente vacía.
            if not nit and first_cell_is_empty and row_number > 1:
                admin_rows_sin_nit.setdefault(name, (tab, row_number))
    return len(detected_nits), len(detected_names)


def _normalize_sheet_title(title: str) -> str:
    """Normaliza un título solo para localizarlo, sin modificar su forma real."""
    return strip_accents(_strip_invisible_characters(title).strip()).lower()


def _quote_sheet_title(title: str) -> str:
    """Cita un título para notación A1 y duplica las comillas simples internas."""
    escaped_title = str(title).replace("'", "''")
    return f"'{escaped_title}'"


def resolve_sheet_titles(sheets_service, spreadsheet_id: str) -> Dict[str, str]:
    """Devuelve títulos reales de pestañas indexados sin espacios extremos, caso ni acentos."""
    try:
        result = sheets_service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            fields="sheets.properties.title",
        ).execute()
        resolved_titles: Dict[str, str] = {}
        for sheet in (result or {}).get("sheets", []) or []:
            title = (sheet.get("properties", {}) or {}).get("title")
            if not isinstance(title, str):
                continue
            normalized_title = _normalize_sheet_title(title)
            if normalized_title:
                resolved_titles[normalized_title] = title
        return resolved_titles
    except Exception as error:
        logger.warning("⚠️ No pude resolver los títulos de las pestañas: %s", error)
        return {}


def _write_sheet_backup_file(target: Path, values: List[List[object]]) -> None:
    temporary = target.with_name(f"{target.name}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as backup_file:
            json.dump(values, backup_file, ensure_ascii=False, indent=2)
            backup_file.write("\n")
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def _cleanup_old_sheet_backups(root: Path, today) -> None:
    keep_days = max(0, SHEET_BACKUP_KEEP_DAYS)
    if not root.exists():
        return

    for candidate in root.iterdir():
        if not candidate.is_dir() or candidate.is_symlink():
            continue
        try:
            backup_date = datetime.strptime(candidate.name, "%Y-%m-%d").date()
        except ValueError:
            continue
        if (today - backup_date).days <= keep_days:
            continue
        try:
            shutil.rmtree(candidate)
        except Exception as error:
            logger.warning(
                "⚠️ No pude eliminar el respaldo antiguo %s: %s",
                candidate,
                error,
            )


def _backup_sheet_tabs(raw_tabs: Dict[str, List[List[object]]]) -> None:
    if not SHEET_BACKUP_ENABLED:
        return

    today = datetime.now().date()
    root = Path(SHEET_BACKUP_DIR)
    day_directory = root / today.isoformat()
    created = False

    try:
        day_directory.mkdir(parents=True, exist_ok=True)
    except Exception as error:
        logger.warning("⚠️ No pude preparar el respaldo de hojas en %s: %s", day_directory, error)
        return

    for tab, values in raw_tabs.items():
        safe_tab = re.sub(r'[<>:"/\\|?*]', "_", str(tab)).strip(" .") or "pestana"
        target = day_directory / f"{safe_tab}.json"
        if target.exists():
            continue
        try:
            _write_sheet_backup_file(target, values)
            created = True
        except Exception as error:
            logger.warning("⚠️ No pude guardar el respaldo de la hoja %s: %s", tab, error)

    try:
        _cleanup_old_sheet_backups(root, today)
    except Exception as error:
        logger.warning("⚠️ No pude limpiar respaldos antiguos de hojas: %s", error)

    if created:
        logger.info("💾 Respaldo de hojas guardado en %s", day_directory.as_posix())


def load_admin_lookup(sheets_service) -> AdminLookup:
    admin_nits: Set[str] = set()
    admin_names: Set[str] = set()
    admin_rows_sin_nit: Dict[str, Tuple[str, int]] = {}

    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Se seguirá sin listas Administrativas/CajaMenor.")
        return AdminLookup(admin_nits, admin_names, admin_rows_sin_nit)

    resolved_titles = resolve_sheet_titles(sheets_service, SHEET_ID)
    raw_tabs: Dict[str, List[List[object]]] = {}
    for expected_tab in ADMIN_SHEET_TABS:
        real_title = resolved_titles.get(_normalize_sheet_title(expected_tab))
        if not real_title:
            logger.warning(
                "⚠️ No encontré la pestaña parecida a '%s' en el spreadsheet",
                expected_tab,
            )
            continue

        sheet_range = f"{_quote_sheet_title(real_title)}!A:B"
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=sheet_range,
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
            values = result.get("values", []) or []
            raw_tabs[expected_tab] = values
            tab_nits: Set[str] = set()
            tab_names: Set[str] = set()
            tab_rows_sin_nit: Dict[str, Tuple[str, int]] = {}
            tab_nit_count, tab_name_count = _admin_lookup_from_values(
                values,
                real_title,
                tab_nits,
                tab_names,
                tab_rows_sin_nit,
            )
            admin_nits.update(tab_nits)
            admin_names.update(tab_names)
            for name, location in tab_rows_sin_nit.items():
                admin_rows_sin_nit.setdefault(name, location)
            logger.info("📄 %s: %d NIT, %d nombres", real_title, tab_nit_count, tab_name_count)
        except Exception as error:
            logger.warning("⚠️ No pude leer la hoja %s: %s", real_title, error)

    try:
        _backup_sheet_tabs(raw_tabs)
    except Exception as error:
        logger.warning(
            "⚠️ Falló el respaldo de hojas administrativas; se continúa: %s",
            error,
        )

    print(
        f"✅ Listas administrativas cargadas: {len(admin_nits)} NIT y "
        f"{len(admin_names)} nombres desde {', '.join(ADMIN_SHEET_TABS)}"
    )
    return AdminLookup(admin_nits, admin_names, admin_rows_sin_nit)


def _registered_entity_header_indexes(row: List[object]) -> Dict[str, Optional[int]]:
    aliases = {
        "nit": ADMIN_NIT_HEADER_LABELS,
        "nombre": {"nombre", "razon social"},
        "correo": {"correo", "correo electronico", "email"},
        "estado": {"estado", "status"},
        "carpeta": {"carpeta", "carpeta drive", "id carpeta", "id carpeta drive"},
        "rut": {"rut", "id rut"},
        "camara": {"camara de comercio", "id camara de comercio"},
        "bancaria": {"certificacion bancaria", "id certificacion bancaria"},
    }
    normalized_aliases = {
        field: {normalize_text(alias) for alias in field_aliases}
        for field, field_aliases in aliases.items()
    }
    indexes: Dict[str, Optional[int]] = {
        "nit": None,
        "nombre": None,
        "correo": None,
        "estado": None,
        "carpeta": None,
        "rut": None,
        "camara": None,
        "bancaria": None,
    }

    for index, value in enumerate(row):
        raw_header = "" if value is None else str(value)
        normalized_header = normalize_text(_strip_invisible_characters(raw_header))
        for field, field_aliases in normalized_aliases.items():
            if indexes[field] is None and normalized_header in field_aliases:
                indexes[field] = index
    return indexes


def _merge_registered_docs(
    registered_docs: Dict[str, Dict[str, str]],
    nit: str,
    new_docs: Dict[str, str],
) -> Dict[str, str]:
    fields = ("carpeta", "rut", "camara", "bancaria")
    nit_forms = _admin_nit_index_forms(nit)
    ordered_forms = [nit, *sorted(nit_forms - {nit})]
    existing_records = [
        registered_docs[nit_form]
        for nit_form in ordered_forms
        if nit_form in registered_docs
    ]
    merged_docs = existing_records[0] if existing_records else {field: "" for field in fields}

    for existing_docs in existing_records:
        for field in fields:
            if not merged_docs.get(field) and existing_docs.get(field):
                merged_docs[field] = existing_docs[field]
    for field in fields:
        if new_docs.get(field):
            merged_docs[field] = new_docs[field]
        else:
            merged_docs.setdefault(field, "")

    for nit_form in ordered_forms:
        registered_docs[nit_form] = merged_docs
    return merged_docs


def _registered_lookup_from_values(
    values: List[List[object]],
    registered_nits: Set[str],
    registered_names: Set[str],
    registered_docs: Dict[str, Dict[str, str]],
) -> Tuple[int, int, int]:
    if not values:
        return 0, 0, 0

    header_indexes = _registered_entity_header_indexes(values[0])
    has_header = any(index is not None for index in header_indexes.values())
    default_indexes = {
        "nit": 1,
        "nombre": 2,
        "correo": 3,
        "estado": 4,
        "carpeta": 9,
        "rut": 10,
        "camara": 11,
        "bancaria": 12,
    }
    indexes = (
        {
            field: detected_index if detected_index is not None else default_indexes[field]
            for field, detected_index in header_indexes.items()
        }
        if has_header
        else default_indexes
    )
    data_rows = values[1:] if has_header else values
    detected_nits: Set[str] = set()
    detected_names: Set[str] = set()

    for row in data_rows:
        if not row:
            continue

        estado_index = indexes["estado"]
        estado = ""
        if estado_index is not None and estado_index < len(row):
            raw_estado = "" if row[estado_index] is None else str(row[estado_index])
            estado = _strip_invisible_characters(raw_estado).strip()
        if estado and normalize_text(estado) not in ACTIVE_VALUES:
            continue

        nit = ""
        nit_index = indexes["nit"]
        if nit_index is not None and nit_index < len(row):
            nit = _normalize_admin_nit_cell(row[nit_index])

        name = ""
        name_index = indexes["nombre"]
        if name_index is not None and name_index < len(row):
            name = _normalize_admin_name_cell(row[name_index])

        if nit:
            detected_nits.add(nit)
            registered_nits.update(_admin_nit_index_forms(nit))
            row_docs = {
                field: (
                    _strip_invisible_characters(str(row[indexes[field]])).strip()
                    if indexes[field] is not None
                    and indexes[field] < len(row)
                    and row[indexes[field]] is not None
                    else ""
                )
                for field in ("carpeta", "rut", "camara", "bancaria")
            }
            _merge_registered_docs(registered_docs, nit, row_docs)
        if name:
            detected_names.add(name)
            registered_names.add(name)

    unique_doc_records = {
        id(registered_docs[nit]): registered_docs[nit]
        for nit in detected_nits
    }.values()
    complete_docs_count = sum(
        all(docs[field] for field in ("rut", "camara", "bancaria"))
        for docs in unique_doc_records
    )
    return len(detected_nits), len(detected_names), complete_docs_count


def load_registered_entities(sheets_service) -> RegisteredLookup:
    registered_nits: Set[str] = set()
    registered_names: Set[str] = set()
    registered_docs: Dict[str, Dict[str, str]] = {}

    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Se seguirá sin listas Clientes/Terceros.")
        return RegisteredLookup(registered_nits, registered_names, registered_docs)

    resolved_titles = resolve_sheet_titles(sheets_service, SHEET_ID)
    raw_tabs: Dict[str, List[List[object]]] = {}
    for expected_tab in KNOWN_ENTITY_TABS:
        real_title = resolved_titles.get(_normalize_sheet_title(expected_tab))
        if not real_title:
            logger.warning(
                "⚠️ No encontré la pestaña parecida a '%s' en el spreadsheet",
                expected_tab,
            )
            continue

        sheet_range = f"{_quote_sheet_title(real_title)}!A:M"
        try:
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=SHEET_ID,
                range=sheet_range,
                valueRenderOption="UNFORMATTED_VALUE",
            ).execute()
            values = result.get("values", []) or []
            raw_tabs[expected_tab] = values
            tab_nits: Set[str] = set()
            tab_names: Set[str] = set()
            tab_docs: Dict[str, Dict[str, str]] = {}
            tab_nit_count, tab_name_count, tab_complete_docs_count = _registered_lookup_from_values(
                values,
                tab_nits,
                tab_names,
                tab_docs,
            )
            registered_nits.update(tab_nits)
            registered_names.update(tab_names)
            merged_tab_records: Set[int] = set()
            for nit, docs in tab_docs.items():
                if id(docs) in merged_tab_records:
                    continue
                _merge_registered_docs(registered_docs, nit, docs)
                merged_tab_records.add(id(docs))
            logger.info(
                "📄 %s: %d NIT, %d nombres, %d con papelería completa",
                real_title,
                tab_nit_count,
                tab_name_count,
                tab_complete_docs_count,
            )
        except Exception as error:
            logger.warning("⚠️ No pude leer la hoja %s: %s", real_title, error)

    try:
        _backup_sheet_tabs(raw_tabs)
    except Exception as error:
        logger.warning(
            "⚠️ Falló el respaldo de hojas registradas; se continúa: %s",
            error,
        )

    print(
        f"✅ Entidades registradas cargadas: {len(registered_nits)} NIT y "
        f"{len(registered_names)} nombres desde {', '.join(KNOWN_ENTITY_TABS)}"
    )
    return RegisteredLookup(registered_nits, registered_names, registered_docs)


def is_administrativa_by_subject(subject: str, admin_nits: Set[str], admin_names: Set[str]) -> bool:
    subject_nits = {
        normalize_nit(sequence)
        for sequence in re.findall(r"\d+", subject or "")
        if len(normalize_nit(sequence)) >= 6
    }
    normalized_admin_nits = {
        normalize_nit(nit)
        for nit in admin_nits
        if len(normalize_nit(nit)) >= 6
    }
    if subject_nits & normalized_admin_nits:
        return True

    normalized_subject = normalize_admin_name(subject)
    for admin_name in admin_names:
        normalized_name = normalize_admin_name(admin_name)
        if len(normalize_alnum(normalized_name)) < 4:
            continue
        if re.search(rf"(?<!\w){re.escape(normalized_name)}(?!\w)", normalized_subject):
            return True
    return False


def is_registered_entity_by_subject(
    subject: str,
    registered_nits: Set[str],
    registered_names: Set[str],
) -> bool:
    return is_administrativa_by_subject(subject, registered_nits, registered_names)


def extract_nit_and_name_from_dian_subject(subject: str) -> Tuple[Optional[str], Optional[str]]:
    fields = (subject or "").split(";")
    if len(fields) < 3:
        return None, None
    raw_nit = fields[0].strip()
    nit = normalize_nit(raw_nit)
    name = fields[1].strip()
    if len(nit) < 6 or not re.fullmatch(r"[0-9.\-\s]+", raw_nit) or not name:
        return None, None
    return nit, name


def should_auto_fill_admin_nit(
    nit: Optional[str],
    normalized_name: str,
    admin_rows_sin_nit: Dict[str, Tuple[str, int]],
    enabled: bool,
    modo_pruebas: bool,
    dry_run: bool,
) -> bool:
    normalized_nit = normalize_nit(nit or "")
    return bool(
        enabled
        and not modo_pruebas
        and not dry_run
        and len(normalized_nit) >= 6
        and normalized_name in admin_rows_sin_nit
    )


def _admin_nit_cell_is_empty(sheets_service, tab: str, row_number: int) -> bool:
    cell_range = f"{_quote_sheet_title(tab)}!A{row_number}"
    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=cell_range,
        ).execute()
        values = result.get("values", []) or []
        return not values or not values[0] or not str(values[0][0]).strip()
    except Exception as error:
        logger.error("❌ No pude verificar el candado de NIT en %s: %s", cell_range, error)
        return False


def write_nit_to_admin_sheet(sheets_service, tab: str, row_number: int, nit: str) -> bool:
    cell_range = f"{_quote_sheet_title(tab)}!A{row_number}"
    try:
        sheets_service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=cell_range,
            valueInputOption="RAW",
            body={"values": [[nit]]},
        ).execute()
        return True
    except Exception as error:
        logger.error("❌ No pude escribir el NIT en %s: %s", cell_range, error)
        return False


def auto_fill_nit_from_subject(
    sheets_service,
    subject: str,
    admin_lookup: AdminLookup,
    enabled: Optional[bool] = None,
    modo_pruebas: Optional[bool] = None,
    dry_run: Optional[bool] = None,
) -> bool:
    enabled = AUTO_FILL_NIT_ENABLED if enabled is None else enabled
    modo_pruebas = MODO_PRUEBAS if modo_pruebas is None else modo_pruebas
    dry_run = DRY_RUN if dry_run is None else dry_run

    nit, name = extract_nit_and_name_from_dian_subject(subject)
    normalized_name = normalize_admin_name(name or "")
    normalized_nit = normalize_nit(nit or "")
    location = admin_lookup.admin_rows_sin_nit.get(normalized_name)
    is_candidate = bool(enabled and len(normalized_nit) >= 6 and location)

    if is_candidate and (modo_pruebas or dry_run):
        tab, row_number = location
        logger.info(
            "🧪 habría escrito NIT %s en %s!A%s (nombre=%s)",
            normalized_nit,
            tab,
            row_number,
            name,
        )
        return False

    if not should_auto_fill_admin_nit(
        normalized_nit,
        normalized_name,
        admin_lookup.admin_rows_sin_nit,
        enabled,
        modo_pruebas,
        dry_run,
    ):
        return False

    tab, row_number = location
    if not _admin_nit_cell_is_empty(sheets_service, tab, row_number):
        logger.warning("🔒 No se sobrescribe %s!A%s porque ya contiene un NIT.", tab, row_number)
        admin_lookup.admin_rows_sin_nit.pop(normalized_name, None)
        return False

    if not write_nit_to_admin_sheet(sheets_service, tab, row_number, normalized_nit):
        return False

    admin_lookup.admin_rows_sin_nit.pop(normalized_name, None)
    admin_lookup.admin_nits.add(normalized_nit)
    logger.info(
        "✍️ NIT %s auto-llenado en %s!A%s (nombre=%s)",
        normalized_nit,
        tab,
        row_number,
        name,
    )
    return True


def find_client_by_nit(nit: str, catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    normalized = normalize_nit(nit)
    if not normalized:
        return None
    for record in catalog:
        if not record.active:
            continue
        if record.normalized_nit and record.normalized_nit == normalized:
            return record
    return None


def find_client_by_nit_in_text(text: str, catalog: List[ClientRecord]) -> Optional[Tuple[str, ClientRecord]]:
    if not text or not catalog:
        return None

    digits = normalize_nit(text)
    if not digits:
        return None

    best: Optional[Tuple[int, str, ClientRecord]] = None
    for record in catalog:
        if not record.active:
            continue
        nit = record.normalized_nit or ""
        if len(nit) < 6:
            continue
        if nit in digits:
            score = len(nit)
            if best is None or score > best[0]:
                best = (score, nit, record)

    if not best:
        return None
    return best[1], best[2]


def find_client_by_name_in_text(text: str, catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    if not text or not catalog:
        return None

    return find_client_in_text(text, catalog)


def client_lookup_catalog(catalog: List[ClientRecord]) -> List[ClientRecord]:
    clients = [
        record for record in catalog
        if "clientes" in normalize_text(record.raw_row.get("__range", ""))
    ]
    return clients or catalog


CLIENT_MATCH_STOPWORDS = {
    "de", "del", "la", "las", "los", "el", "y", "sa", "sas", "s", "a", "esp", "e", "s", "p", "cia", "ltda", "inc",
}


def client_name_tokens(value: str) -> Set[str]:
    normalized = normalize_text(value)
    tokens = re.findall(r"[a-z0-9]+", normalized)
    return {token for token in tokens if len(token) >= 3 and token not in CLIENT_MATCH_STOPWORDS}


def client_similarity(candidate: str, record: ClientRecord) -> int:
    candidate_norm = normalize_alnum(candidate)
    record_norm = record.normalized_name
    if not candidate_norm or not record_norm:
        return 0

    if candidate_norm == record_norm:
        return 1000 + len(record_norm)
    if record_norm in candidate_norm or candidate_norm in record_norm:
        return 700 + min(len(candidate_norm), len(record_norm))

    candidate_tokens = client_name_tokens(candidate)
    record_tokens = client_name_tokens(record.name)
    if not candidate_tokens or not record_tokens:
        return 0

    common = candidate_tokens & record_tokens
    if not common:
        return 0

    coverage_candidate = len(common) / len(candidate_tokens)
    coverage_record = len(common) / len(record_tokens)

    if coverage_candidate >= 0.80 or (len(common) >= 2 and coverage_candidate >= 0.60 and coverage_record >= 0.40):
        return int((coverage_candidate + coverage_record) * 100) + sum(len(token) for token in common)

    return 0


def normalize_client_match_value(value: str) -> str:
    return normalize_alnum(value)


def match_client_raw_to_catalog(raw_client: str, catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    raw_norm = normalize_client_match_value(raw_client)
    if not raw_norm:
        return None

    best: Optional[Tuple[int, ClientRecord]] = None
    for record in catalog:
        if not record.active:
            continue
        client_norm = normalize_client_match_value(record.name)
        if not client_norm or len(client_norm) < 4:
            continue

        score = 0
        if client_norm == raw_norm:
            score = 1000 + len(client_norm)
        elif client_norm in raw_norm:
            score = 800 + len(client_norm)
        elif raw_norm in client_norm:
            score = 650 + len(raw_norm)

        if score and (best is None or score > best[0]):
            best = (score, record)

    return best[1] if best else None


def find_client_in_text(text: str, catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    text_norm = normalize_client_match_value(text)
    if not text_norm:
        return None

    best: Optional[Tuple[int, ClientRecord]] = None
    for record in catalog:
        if not record.active:
            continue
        client_norm = normalize_client_match_value(record.name)
        if not client_norm or len(client_norm) < 4:
            continue
        if client_norm in text_norm:
            score = len(client_norm)
            if best is None or score > best[0]:
                best = (score, record)

    return best[1] if best else None


def _clean_order_client_value(value: str) -> str:
    value = re.sub(r"\s+", " ", value or "").strip(" :-")
    value = re.split(
        r"\b(producto|nit|no\s*ppto|fecha|orden|medio|contacto|mail|cel|ciudad|razon\s+social|razón\s+social|proveedor|documento)\b\s*:?",
        value,
        flags=re.IGNORECASE,
    )[0].strip(" :-")
    return value


def _is_useful_order_client_value(value: str) -> bool:
    normalized = normalize_text(value)
    alnum = normalize_alnum(value)
    if len(alnum) < 4:
        return False
    if normalized in {"cliente", "nombre del cliente"}:
        return False
    if normalize_nit(value) == value:
        return False
    return True


def extract_order_client_raw(text: str) -> Optional[str]:
    if not text:
        return None

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    for idx, line in enumerate(lines):
        match = re.search(r"\bcliente\b\s*[:\-]\s*(.+)$", line, re.IGNORECASE)
        if match:
            value = _clean_order_client_value(match.group(1))
            if _is_useful_order_client_value(value):
                return value

        if re.fullmatch(r"cliente\s*[:\-]?", line, re.IGNORECASE):
            for next_line in lines[idx + 1 : idx + 8]:
                value = _clean_order_client_value(next_line)
                normalized = normalize_alnum(value)
                if any(token in normalized for token in ("producto", "nit", "fecha", "noppto", "orden")):
                    continue
                if _is_useful_order_client_value(value):
                    return value

    for idx, line in enumerate(lines):
        if "noclienteproductonit" not in normalize_alnum(line):
            continue
        for next_line in lines[idx + 1 : idx + 8]:
            value = _clean_order_client_value(next_line)
            normalized = normalize_alnum(value)
            if any(token in normalized for token in ("nopptofecha", "cliente", "producto", "nit", "fecha")):
                continue
            if _is_useful_order_client_value(value):
                return value

    return None


def identify_client(candidate_texts: List[str], catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    if not catalog:
        return None

    haystack = " ".join(candidate_texts)
    haystack_norm = normalize_alnum(haystack)
    if not haystack_norm:
        return None

    best: Optional[Tuple[int, ClientRecord]] = None
    for record in catalog:
        if not record.active:
            continue
        score = 0
        token = record.normalized_name
        if token and len(token) >= 4 and token in haystack_norm:
            score = 600 + len(token)
        else:
            score = client_similarity(haystack, record)
        if score and (best is None or score > best[0]):
            best = (score, record)

    return best[1] if best else None


def extract_client_field_values(text: str) -> List[str]:
    if not text:
        return []

    values = []
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    stop_fields = r"(producto|nit|no\s*ppto|fecha|orden|medio|contacto|mail|cel|ciudad|razon\s+social|razón\s+social|proveedor|documento)"

    # PDFs de órdenes pueden extraer la tabla como:
    # No: CLIENTE: PRODUCTO: NIT:
    # 33071
    # TGI TRANSPORTADORA DE GAS
    # CONTRATO 551008471 2026
    # 900134459
    for idx, line in enumerate(lines):
        normalized_line = normalize_alnum(line)
        if "noclienteproductonit" not in normalized_line:
            continue
        for next_line in lines[idx + 1 : idx + 6]:
            candidate = re.sub(r"\s+", " ", next_line).strip(" :-")
            normalized_candidate = normalize_alnum(candidate)
            if not candidate or normalize_nit(candidate) == candidate:
                continue
            if any(token in normalized_candidate for token in ("nopptofecha", "cliente", "producto", "nit", "fecha")):
                continue
            if len(normalize_alnum(candidate)) >= 6:
                values.append(candidate)
                break

    for idx, line in enumerate(lines):
        value = ""
        match = re.search(r"\bcliente\b\s*[:\-]\s*(.+)$", line, re.IGNORECASE)
        if match:
            value = match.group(1).strip()

        # Algunos extractores de PDF dejan "CLIENTE:" en una línea y el valor en una línea posterior.
        if not value and re.fullmatch(r"cliente\s*[:\-]?", line, re.IGNORECASE) and idx + 1 < len(lines):
            for next_line in lines[idx + 1 : idx + 6]:
                candidate = re.sub(r"\s+", " ", next_line).strip(" :-")
                normalized_candidate = normalize_alnum(candidate)
                if not candidate or normalize_nit(candidate) == candidate:
                    continue
                if any(token in normalized_candidate for token in ("producto", "nit", "fecha", "noppto", "orden")):
                    continue
                value = candidate
                break

        value = re.split(rf"\b{stop_fields}\b\s*:?", value, flags=re.IGNORECASE)[0].strip()
        value = re.sub(r"\s+", " ", value).strip(" :-")
        normalized_value = normalize_text(value)
        alnum_value = normalize_alnum(value)
        if len(alnum_value) >= 6 and normalized_value not in {"cliente", "nombre del cliente"}:
            values.append(value)

    return values


def identify_client_from_fields(candidate_texts: List[str], catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    field_values: List[str] = []
    for text in candidate_texts:
        field_values.extend(extract_client_field_values(text))

    return identify_client(field_values, catalog) if field_values else None


def first_client_field_value(candidate_texts: List[str]) -> Optional[str]:
    for text in candidate_texts:
        values = extract_client_field_values(text)
        if values:
            return values[0]
    return None


# endregion
# region 🏷️ Etiquetas de Gmail
# ============================================================
# LABELS
# ============================================================
def get_label_id_by_name(gmail_service, label_name: str) -> Optional[str]:
    resp = gmail_service.users().labels().list(userId="me").execute()
    for lb in ensure_list(resp.get("labels")):
        if (lb.get("name", "") or "").strip().lower() == label_name.strip().lower():
            return lb.get("id")
    return None


def ensure_label_exists(gmail_service, label_name: str) -> Optional[str]:
    lid = get_label_id_by_name(gmail_service, label_name)
    if lid:
        return lid
    created = gmail_service.users().labels().create(
        userId="me",
        body={
            "name": label_name,
            "labelListVisibility": "labelShow",
            "messageListVisibility": "show",
            "type": "user",
        },
    ).execute()
    return created.get("id")


def ensure_status_labels(gmail_service) -> Dict[str, Optional[str]]:
    label_ids = {}
    for name in [LABEL_ADMIN_NAME, LABEL_REVIEW_NAME, LABEL_NOTE_CREDIT_NAME, LABEL_APPROVED_NAME, LABEL_REJECTED_NAME]:
        label_ids[name] = ensure_label_exists(gmail_service, name)
    return label_ids


def apply_single_status_label(gmail_service, message_id: str, label_name: str, archive: bool = False) -> None:
    label_ids = ensure_status_labels(gmail_service)

    add_ids = [label_ids[label_name]] if label_ids.get(label_name) else []
    remove_ids = [lid for name, lid in label_ids.items() if lid and name != label_name]
    if archive:
        remove_ids.append("INBOX")

    gmail_service.users().messages().modify(
        userId="me",
        id=message_id,
        body={
            "addLabelIds": list(dict.fromkeys(add_ids)),
            "removeLabelIds": list(dict.fromkeys(remove_ids)),
        },
    ).execute()


# endregion
# region 👀 Gmail Watch e historial
# ============================================================
# GMAIL WATCH / HISTORY
# ============================================================
def resolve_effective_watch_labels(
    modo_pruebas: bool,
    etiqueta_pruebas: str,
    gmail_label_ids: List[str],
) -> List[str]:
    if modo_pruebas:
        return [etiqueta_pruebas]
    return gmail_label_ids


def resolve_watch_label_ids(gmail_service, label_names: List[str]) -> List[str]:
    """Convierte nombres de etiquetas a IDs. Las etiquetas del sistema (INBOX, etc.) se usan tal cual."""
    if all(name.upper() in GMAIL_SYSTEM_LABEL_IDS for name in label_names):
        return label_names
    resp = gmail_service.users().labels().list(userId="me").execute()
    label_map = {lb.get("name", "").lower(): lb.get("id") for lb in ensure_list(resp.get("labels"))}
    resolved = []
    for name in label_names:
        if name.upper() in GMAIL_SYSTEM_LABEL_IDS:
            resolved.append(name)
        else:
            lid = label_map.get(name.lower())
            if lid:
                resolved.append(lid)
                print(f"🏷️ Etiqueta watch '{name}' → ID={lid}")
            else:
                print(f"⚠️ Etiqueta de watch no encontrada en Gmail: '{name}'. Se ignorará.")
    return resolved


def resolve_modo_pruebas_label_id(gmail_service, account_id: Optional[str] = None) -> str:
    resolved = resolve_watch_label_ids(gmail_service, [ETIQUETA_PRUEBAS])
    if not resolved:
        account = account_id or "cuenta unica"
        raise RuntimeError(
            f"MODO_PRUEBAS activo pero la etiqueta '{ETIQUETA_PRUEBAS}' no existe en la cuenta {account}"
        )
    return resolved[0]


def ensure_gmail_watch(gmail_service, account_id: Optional[str] = None) -> Dict:
    if not GCP_PROJECT_ID or not PUBSUB_TOPIC_FULL or not PUBSUB_SUBSCRIPTION_ID:
        raise RuntimeError("Faltan env vars: GCP_PROJECT_ID, PUBSUB_TOPIC_FULL, PUBSUB_SUBSCRIPTION.")

    state = load_state(account_id)
    now_ms = int(time.time() * 1000)
    expiration = int(state.get("watch_expiration_ms", 0))

    if expiration and (expiration - now_ms) > WATCH_RENEW_WINDOW_MS:
        return state

    effective_label_names = resolve_effective_watch_labels(MODO_PRUEBAS, ETIQUETA_PRUEBAS, WATCH_LABEL_IDS)
    watch_label_ids = resolve_watch_label_ids(gmail_service, effective_label_names)
    if MODO_PRUEBAS and not watch_label_ids:
        account = account_id or "cuenta unica"
        raise RuntimeError(
            f"MODO_PRUEBAS activo pero la etiqueta '{ETIQUETA_PRUEBAS}' no existe en la cuenta {account}"
        )
    body = {
        "topicName": PUBSUB_TOPIC_FULL,
        "labelIds": watch_label_ids if watch_label_ids else ["INBOX"],
        "labelFilterBehavior": "INCLUDE",
    }
    resp = gmail_service.users().watch(userId="me", body=body).execute()

    last_history = state.get("last_history_id") or resp.get("historyId")
    state.update(
        {
            "watch_started_at_ms": now_ms,
            "watch_expiration_ms": int(resp.get("expiration", 0)),
            "last_history_id": str(last_history) if last_history else None,
        }
    )
    save_state(state, account_id)
    label = f" ({account_id})" if account_id else ""
    print(f"✅ Watch activo{label}. last_history_id={state.get('last_history_id')}")
    return state


def fetch_new_message_ids(
    gmail_service,
    start_history_id: str,
    label_id: Optional[str] = None,
    include_label_added: bool = False,
) -> Tuple[Set[str], Optional[str]]:
    message_ids: Set[str] = set()
    page_token = None
    latest_history_id: Optional[str] = None

    while True:
        history_types = ["messageAdded", "labelAdded"] if include_label_added else ["messageAdded"]
        request = {
            "userId": "me",
            "startHistoryId": start_history_id,
            "historyTypes": history_types,
            "pageToken": page_token,
        }
        if label_id:
            request["labelId"] = label_id
        resp = gmail_service.users().history().list(**request).execute()

        for history in ensure_list(resp.get("history")):
            for added in ensure_list(history.get("messagesAdded")):
                mid = (added.get("message") or {}).get("id")
                if mid:
                    message_ids.add(mid)
            if include_label_added and label_id:
                for added in ensure_list(history.get("labelsAdded")):
                    if label_id not in ensure_list(added.get("labelIds")):
                        continue
                    mid = (added.get("message") or {}).get("id")
                    if mid:
                        message_ids.add(mid)

        page_token = resp.get("nextPageToken")
        if resp.get("historyId"):
            latest_history_id = str(resp.get("historyId"))

        if not page_token:
            break

    return message_ids, latest_history_id


def fetch_labeled_message_ids(gmail_service, label_id: str, max_results: int = 25) -> List[str]:
    try:
        resp = gmail_service.users().messages().list(
            userId="me",
            labelIds=[label_id],
            maxResults=max_results,
        ).execute()
        return [item["id"] for item in ensure_list(resp.get("messages")) if item.get("id")]
    except Exception as error:
        logger.warning("No se pudieron buscar correos con la etiqueta %s: %s", label_id, error)
        return []


def update_last_history_id(latest_history_id: Optional[str], account_id: Optional[str] = None) -> None:
    if not latest_history_id:
        return
    state = load_state(account_id)
    state["last_history_id"] = str(latest_history_id)
    save_state(state, account_id)


# endregion
# region 📎 Adjuntos, ZIP y extracción de PDF
# ============================================================
# ATTACHMENTS / ZIP / PDF TEXT
# ============================================================
def collect_attachments(payload: Dict) -> List[Dict[str, Optional[str]]]:
    items: List[Dict[str, Optional[str]]] = []
    if not payload:
        return items

    for part in ensure_list(payload.get("parts")):
        filename = (part.get("filename") or "").strip()
        mime_type = (part.get("mimeType") or "").strip()
        body = part.get("body", {}) or {}
        attachment_id = body.get("attachmentId")
        if filename:
            items.append({"filename": filename, "mimeType": mime_type, "attachmentId": attachment_id})
        items.extend(collect_attachments(part))

    return items


def gmail_download_attachment_bytes(gmail_service, message_id: str, attachment_id: str) -> bytes:
    att = gmail_service.users().messages().attachments().get(
        userId="me",
        messageId=message_id,
        id=attachment_id,
    ).execute()
    data = att.get("data", "") or ""
    return base64.urlsafe_b64decode(data.encode("utf-8"))


def is_pdf_attachment(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".pdf") or mt == "application/pdf"


def is_xml_attachment(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".xml") or mt in ("application/xml", "text/xml")


def is_zip_attachment(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".zip") or mt in ("application/zip", "application/x-zip-compressed")


def _is_safe_zip_member(name: str) -> bool:
    if not name:
        return False
    n = name.replace("\\", "/")
    if n.startswith("/") or n.startswith("../") or "/../" in n:
        return False
    return True


def _is_ignored_zip_member(name: str) -> bool:
    normalized = (name or "").replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return True
    return "__MACOSX" in parts or any(part.startswith("._") for part in parts) or parts[-1] == ".DS_Store"


def analyze_zip_bytes(zip_filename: str, zip_bytes: bytes, depth: int = 1) -> Dict[str, object]:
    out = {
        "zip_filename": zip_filename,
        "ok": True,
        "error": None,
        "files": [],
        "pdf_count": 0,
        "xml_count": 0,
        "image_count": 0,
        "total_uncompressed": 0,
    }

    try:
        if zip_bytes is None:
            out["ok"] = False
            out["error"] = "ZIP vacío"
            return out

        if len(zip_bytes) > MAX_ZIP_BYTES:
            out["ok"] = False
            out["error"] = f"ZIP excede MAX_ZIP_BYTES ({len(zip_bytes)} > {MAX_ZIP_BYTES})"
            return out

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            infos = zf.infolist()
            if len(infos) > MAX_ZIP_FILES:
                out["ok"] = False
                out["error"] = f"ZIP tiene demasiados archivos ({len(infos)} > {MAX_ZIP_FILES})"
                return out

            total = 0
            pdf_count = 0
            xml_count = 0
            image_count = 0
            files = []

            for info in infos:
                if info.is_dir():
                    continue
                if _is_ignored_zip_member(info.filename):
                    continue
                if info.flag_bits & 0x1:
                    out["ok"] = False
                    out["error"] = "ZIP protegido con contraseña"
                    return out

                name = info.filename
                if not _is_safe_zip_member(name):
                    out["ok"] = False
                    out["error"] = f"Ruta insegura dentro del ZIP: {name}"
                    return out

                size = int(getattr(info, "file_size", 0) or 0)
                if size > MAX_ZIP_SINGLE_FILE:
                    out["ok"] = False
                    out["error"] = f"Archivo dentro del ZIP demasiado grande: {name}"
                    return out

                lower = name.lower()
                is_pdf = lower.endswith(".pdf")
                is_xml = lower.endswith(".xml")
                is_image = lower.endswith((".jpg", ".jpeg", ".png"))
                is_zip = lower.endswith(".zip")
                entry = {
                    "name": name,
                    "size": size,
                    "is_pdf": is_pdf,
                    "is_xml": is_xml,
                    "is_image": is_image,
                    "is_zip": is_zip,
                }
                files.append(entry)

                if is_zip:
                    if depth >= MAX_ZIP_NESTING:
                        out["ok"] = False
                        out["error"] = f"ZIP anidado excede MAX_ZIP_NESTING en {name}"
                        return out
                    nested_bytes = zf.read(name)
                    nested_analysis = analyze_zip_bytes(f"{zip_filename}/{name}", nested_bytes, depth=depth + 1)
                    if not nested_analysis.get("ok"):
                        out["ok"] = False
                        out["error"] = f"{name}: {nested_analysis.get('error')}"
                        return out
                    entry["nested"] = nested_analysis
                    pdf_count += int(nested_analysis.get("pdf_count") or 0)
                    xml_count += int(nested_analysis.get("xml_count") or 0)
                    image_count += int(nested_analysis.get("image_count") or 0)
                    total += int(nested_analysis.get("total_uncompressed") or 0)
                    if total > MAX_ZIP_TOTAL_UNCOMPRESSED:
                        out["ok"] = False
                        out["error"] = "ZIP excede el tamaño total descomprimido permitido"
                        return out
                    continue

                total += size
                if total > MAX_ZIP_TOTAL_UNCOMPRESSED:
                    out["ok"] = False
                    out["error"] = "ZIP excede el tamaño total descomprimido permitido"
                    return out
                if is_pdf:
                    pdf_count += 1
                if is_xml:
                    xml_count += 1
                if is_image:
                    image_count += 1

            out["files"] = files
            out["pdf_count"] = pdf_count
            out["xml_count"] = xml_count
            out["image_count"] = image_count
            out["total_uncompressed"] = total
            return out

    except zipfile.BadZipFile:
        out["ok"] = False
        out["error"] = "ZIP corrupto o inválido"
        return out
    except Exception as e:
        out["ok"] = False
        out["error"] = f"Error leyendo ZIP: {e}"
        return out


def extract_zip_files(zip_filename: str, zip_bytes: bytes) -> Dict[str, object]:
    analysis = analyze_zip_bytes(zip_filename, zip_bytes)
    if not analysis.get("ok"):
        return {"ok": False, "error": analysis.get("error"), "files": []}

    output: List[UnifiedFile] = []

    def _extract_level(current_bytes: bytes, node: Dict[str, object], prefix: str = "") -> None:
        with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
            for file_info in ensure_list(node.get("files")):
                name = (file_info.get("name") or "").strip()
                if not name or not _is_safe_zip_member(name):
                    continue
                if _is_ignored_zip_member(name):
                    continue
                raw = zf.read(name)
                normalized = name.replace("\\", "/")
                full_name = "/".join(part for part in [prefix, normalized] if part)

                if file_info.get("is_zip"):
                    nested = file_info.get("nested")
                    if nested:
                        _extract_level(raw, nested, full_name)
                    continue

                mime = "application/octet-stream"
                lower = normalized.lower()
                if lower.endswith(".pdf"):
                    mime = "application/pdf"
                elif lower.endswith(".xml"):
                    mime = "application/xml"
                elif lower.endswith((".jpg", ".jpeg")):
                    mime = "image/jpeg"
                elif lower.endswith(".png"):
                    mime = "image/png"

                output.append(UnifiedFile(name=full_name, mime_type=mime, data=raw, source=f"zip:{zip_filename}"))

    try:
        _extract_level(zip_bytes, analysis)
        return {"ok": True, "error": None, "files": output, "analysis": analysis}
    except Exception as e:
        return {"ok": False, "error": str(e), "files": [], "analysis": analysis}


def extract_pdf_text(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception:
                continue
        return "\n".join(pages).strip()
    except Exception:
        return ""


def build_unified_files(gmail_service, message_id: str, attachments: List[Dict[str, Optional[str]]]) -> Tuple[List[UnifiedFile], List[str], List[Dict[str, object]]]:
    unified: List[UnifiedFile] = []
    zip_errors: List[str] = []
    zip_analyses: List[Dict[str, object]] = []

    for att in attachments:
        filename = (att.get("filename") or "").strip()
        attachment_id = att.get("attachmentId")
        if not filename or not attachment_id:
            continue

        try:
            content = gmail_download_attachment_bytes(gmail_service, message_id, attachment_id)
        except Exception as e:
            zip_errors.append(f"No pude descargar adjunto {filename}: {e}")
            continue

        mime_type = (att.get("mimeType") or "application/octet-stream").strip()

        if is_zip_attachment(att):
            extracted = extract_zip_files(filename, content)
            analysis = extracted.get("analysis") or {"zip_filename": filename, "ok": extracted.get("ok", False), "error": extracted.get("error")}
            zip_analyses.append(analysis)
            if not extracted.get("ok"):
                zip_errors.append(f"ZIP inválido {filename}: {extracted.get('error')}")
                continue
            for item in extracted.get("files") or []:
                if item.is_pdf or item.is_xml or item.is_image:
                    unified.append(item)
            print(
                f"📦 ZIP leído: {filename} | "
                f"pdf={analysis.get('pdf_count')} | "
                f"xml={analysis.get('xml_count')} | "
                f"img={analysis.get('image_count')}"
            )
            for item in extracted.get("files") or []:
                if item.is_pdf or item.is_xml or item.is_image:
                    print(f"   ↳ {item.name}")
            continue

        file_obj = UnifiedFile(name=filename, mime_type=mime_type, data=content, source="direct")
        if file_obj.is_pdf or file_obj.is_xml or file_obj.is_image:
            unified.append(file_obj)

    for item in unified:
        if item.is_pdf and not item.extracted_text:
            item.extracted_text = extract_pdf_text(item.data)

    return unified, zip_errors, zip_analyses


# endregion
# region 📋 Reglas de negocio (clasificación y validación)
# ============================================================
# BUSINESS RULES
# ============================================================
def extract_nit_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    match = NIT_REGEX.search(text)
    if not match:
        return None
    nit = normalize_nit(match.group(1))
    return nit or None


def contains_credit_or_debit_note_text(text: str) -> bool:
    """Detecta notas de crédito o débito en español e inglés."""
    normalized_text = normalize_text(text)
    if not normalized_text:
        return False
    return bool(
        re.search(r"\bnota\s+(?:de\s+)?(?:credito|debito)\b", normalized_text)
        or re.search(r"\b(?:credit|debit)\s+note\b", normalized_text)
    )


# Alias retrocompatible para llamadores e imports existentes.
contains_note_credit_text = contains_credit_or_debit_note_text


def is_note_credit_by_filename(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        if contains_credit_or_debit_note_text(pdf.name):
            return True
    return False


def is_note_credit_by_text(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        if contains_credit_or_debit_note_text(pdf.extracted_text):
            return True
    return False


def filename_declares_purchase_order(filename: str) -> bool:
    """Indica si el nombre identifica un PDF de orden, sin exigir número/código."""
    stem = os.path.splitext(os.path.basename(filename or ""))[0]
    normalized_name = normalize_text(stem.replace("_", " "))
    if PURCHASE_ORDER_FILENAME_EXCLUSIONS.search(normalized_name):
        return False
    return any(pattern.search(normalized_name) for pattern in PURCHASE_ORDER_FILENAME_REGEXES)


def contains_purchase_order_reference(text: str) -> bool:
    """Reconoce una orden declarada como título/encabezado del propio documento.

    Solo revisa las primeras líneas no vacías y exige que la declaración comience
    la línea. Así, una mención narrativa dentro del texto de una factura no cuenta.
    """
    header_lines = [line.strip() for line in (text or "").splitlines() if line.strip()][:8]
    for line in header_lines:
        normalized_line = normalize_text(line)
        if PURCHASE_ORDER_FILENAME_EXCLUSIONS.search(normalized_line):
            continue
        if PURCHASE_ORDER_HEADER_REGEX.search(normalized_line):
            return True
    return False


def is_purchase_order_document(file_obj: UnifiedFile) -> bool:
    """Valida presencia por nombre del adjunto o encabezado del propio PDF."""
    return filename_declares_purchase_order(file_obj.name) or contains_purchase_order_reference(
        file_obj.extracted_text
    )


def detect_order(pdfs: List[UnifiedFile]) -> bool:
    return any(is_purchase_order_document(pdf) for pdf in pdfs)


def is_order_file(file_obj: UnifiedFile) -> bool:
    if classify_document_type(file_obj) == "orden_compra":
        return True
    return is_purchase_order_document(file_obj)


def identify_client_in_order_pdfs(pdfs: List[UnifiedFile], catalog: List[ClientRecord]) -> ClientMatchResult:
    order_files = []
    for pdf in pdfs:
        if is_order_file(pdf):
            order_files.append(pdf)

    logger.info(f"🔎 Clientes en orden: catálogo={len(catalog)} | ordenes_detectadas={len(order_files)}")
    if not order_files:
        logger.info("🔎 Clientes en orden: no se detectó PDF de orden de compra.")
        return ClientMatchResult(source="ORDER_BLOCK")

    for pdf in order_files:
        order_text = f"{pdf.name}\n{pdf.extracted_text}"
        logger.info(f"🔎 Orden evaluada: {pdf.name}")

        raw_client = extract_order_client_raw(order_text)
        logger.info(f"🔎 Cliente crudo extraído desde orden: {raw_client or 'No encontrado'}")
        if not raw_client:
            continue

        record = match_client_raw_to_catalog(raw_client, catalog)
        if record:
            logger.info(f"✅ Cliente final del catálogo: {record.name} | fuente=ORDER_BLOCK | archivo={pdf.name}")
            return ClientMatchResult(record=record, raw=raw_client, source="ORDER_BLOCK")

        record = find_client_in_text(order_text, catalog)
        if record:
            logger.info(f"✅ Cliente encontrado dentro de orden: {record.name} | fuente=ORDER_BLOCK | archivo={pdf.name}")
            return ClientMatchResult(record=record, raw=raw_client, source="ORDER_BLOCK")

        logger.info(f"⚠️ Cliente crudo sin match en catálogo: {raw_client} | archivo={pdf.name}")
        return ClientMatchResult(raw=raw_client, source="ORDER_BLOCK")

    for pdf in order_files:
        order_text = f"{pdf.name}\n{pdf.extracted_text}"
        record = find_client_in_text(order_text, catalog)
        if record:
            logger.info(f"✅ Cliente encontrado dentro de orden: {record.name} | fuente=ORDER_BLOCK | archivo={pdf.name}")
            return ClientMatchResult(record=record, source="ORDER_BLOCK")

    logger.info("⚠️ No se encontró ningún cliente estructurado en la orden.")
    return ClientMatchResult(source="ORDER_BLOCK")


def _compile_ok_compras_pattern(normalized_pattern: str) -> re.Pattern:
    purchases_match = OK_COMPRAS_WITH_PURCHASES_REGEX.fullmatch(normalized_pattern)
    if purchases_match:
        term = re.escape(purchases_match.group("term"))
        return re.compile(
            rf"(?<!\w){term}{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras(?!\w)"
        )
    return re.compile(rf"(?<!\w){re.escape(normalized_pattern)}(?!\w)")


def _has_ok_compras_negative_before(normalized_clause: str, match_start: int) -> bool:
    before_match = normalized_clause[:match_start]
    return any(exclusion.search(before_match) for exclusion in OK_COMPRAS_NEGATIVE_REGEXES)


def contains_ok_compras_text(text: str, patterns: Optional[List[str]] = None) -> bool:
    """Detecta una aprobación considerando el prefijo de su cláusula.

    Las fronteras son puntuación fuerte, salto de línea, ``pero`` y ``aunque``.
    Una negación previa veta el término sin importar la distancia dentro de la
    cláusula; después solo vetan estados pendientes de aparición inmediata.
    """
    configured_patterns = patterns if patterns is not None else OK_COMPRAS_PATTERNS
    approval_patterns = [
        _compile_ok_compras_pattern(normalize_text(pattern))
        for pattern in configured_patterns
        if pattern
    ]
    for raw_clause in OK_COMPRAS_CLAUSE_SEPARATOR_REGEX.split(text or ""):
        normalized_clause = normalize_text(raw_clause)
        for pattern in approval_patterns:
            for match in pattern.finditer(normalized_clause):
                if _has_ok_compras_negative_before(normalized_clause, match.start()):
                    continue
                if OK_COMPRAS_POST_NEGATIVE_REGEX.match(normalized_clause[match.end():]):
                    continue
                return True
    return False


def filename_declares_ok_compras(filename: str) -> bool:
    """Indica si el nombre identifica un adjunto de OK/visto bueno de compras."""
    stem = os.path.splitext(os.path.basename(filename or ""))[0]
    normalized_name = normalize_text(stem.replace("_", " ").replace("-", " "))
    if any(exclusion.search(normalized_name) for exclusion in OK_COMPRAS_FILENAME_NEGATIVE_REGEXES):
        return False
    return any(pattern.search(normalized_name) for pattern in OK_COMPRAS_FILENAME_REGEXES)


def is_ok_compras_document(file_obj: UnifiedFile) -> bool:
    """Valida el adjunto por nombre y conserva como respaldo el sello en su texto."""
    return filename_declares_ok_compras(file_obj.name) or contains_ok_compras_text(
        file_obj.extracted_text
    )


def detect_ok_compras(pdfs: List[UnifiedFile]) -> bool:
    return any(is_ok_compras_document(pdf) for pdf in pdfs)


CUENTA_COBRO_REQUIRED_DOCS = [
    "cuenta_cobro",
    "cedula",
    "rut",
    "certificado_bancario",
    "orden_compra",
]

DOCUMENT_CLASSIFIERS = {
    "cuenta_cobro": {
        "required_any": ("cuenta cobro", "cuenta de cobro", "cuenta_cobro", "cta"),
        "alternate_all": (("debe a", "la suma de"),),
        "support": ("debe a", "la suma de", "por concepto de", "por favor consignar"),
        "min_support": 0,
        "allow_images": False,
    },
    "cedula": {
        "required_any": ("cedula de ciudadania", "cedula", "identificacion personal", "documento de identidad"),
        "alternate_all": (("republica de colombia", "identificacion"),),
        "support": ("republica de colombia", "fecha de nacimiento", "lugar de expedicion", "indice derecho"),
        "min_support": 0,
        "allow_images": True,
    },
    "rut": {
        "required_any": ("registro unico tributario", "rut"),
        "alternate_all": (("dian", "nit", "actividad economica"),),
        "support": (
            "dian",
            "nit",
            "numero de identificacion tributaria",
            "responsabilidades calidades y atributos",
            "tipo de contribuyente",
            "actividad economica",
            "regimen simplificado",
            "formulario del registro",
            "muisca",
        ),
        "min_support": 0,
        "allow_images": False,
    },
    "certificado_bancario": {
        "required_any": ("certificado bancario", "certificado_bancario", "certificado-bancario", "cert bancario", "c b", "certifica", "firma autorizada"),
        "alternate_all": (("certifica", "cuenta de ahorros"),),
        "support": (
            "c b",
            "certificado bancario",
            "cuenta de ahorros",
            "banco",
            "saldo o cupo disponible",
            "tipo de producto",
            "nro de producto",
            "davivienda",
            "bancolombia",
            "banco de bogota",
        ),
        "min_support": 1,
        "allow_images": False,
    },
    "orden_compra": {
        "required_any": ("orden de internet", "orden de compra", "orden no", "orden nro", "orden numero"),
        "alternate_all": (("subtotal", "autorizado por"),),
        "support": (
            "valor neto",
            "cpm costo x click",
            "subtotal",
            "elaborado por",
            "autorizado por",
            "no se recibe factura",
            "century media",
        ),
        "min_support": 0,
        "allow_images": False,
    },
    "aprobado_compras": {
        "required_any": ("aprobado", "aprobacion", "aprobado por"),
        "alternate_all": (),
        "support": ("vo bo", "vobo", "visto bueno", "autorizado", "jefe de compras", "gerente", "firma de aprobacion"),
        "min_support": 1,
        "allow_images": False,
    },
}

DOCUMENT_LABELS = {
    "cuenta_cobro": "cuenta de cobro",
    "cedula": "cédula",
    "rut": "RUT",
    "certificado_bancario": "certificado bancario",
    "orden_compra": "orden de compra",
    "aprobado_compras": "aprobado de compras",
}


def _contains_document_keyword(text: str, keyword: str) -> bool:
    normalized = normalize_text(text)
    if not normalized:
        return False
    normalized_keyword = normalize_text(keyword)
    if normalized_keyword == "rut":
        return bool(re.search(r"\brut\b", normalized))
    if normalized_keyword == "cta":
        return bool(re.search(r"\bcta\b", normalized))
    if re.search(rf"\b{re.escape(normalized_keyword)}\b", normalized):
        return True
    return normalize_alnum(normalized_keyword) in normalize_alnum(normalized)


def _document_classifier_score(sample: str, config: Dict[str, object]) -> int:
    required = tuple(str(x) for x in config.get("required_any", ()))
    alternate_groups = tuple(config.get("alternate_all", ()) or ())
    support = tuple(str(x) for x in config.get("support", ()))
    min_support = int(config.get("min_support") or 0)

    required_hits = sum(1 for keyword in required if _contains_document_keyword(sample, keyword))
    alternate_hit = any(
        all(_contains_document_keyword(sample, str(keyword)) for keyword in group)
        for group in alternate_groups
    )
    if required_hits < 1 and not alternate_hit:
        return 0

    support_hits = sum(1 for keyword in support if _contains_document_keyword(sample, keyword))
    if not alternate_hit and support_hits < min_support:
        return 0

    return (required_hits * 10) + (10 if alternate_hit else 0) + support_hits


def _classify_document_sample(sample: str, file_obj: UnifiedFile) -> Optional[str]:
    matches: List[Tuple[int, str]] = []

    for doc_type, config in DOCUMENT_CLASSIFIERS.items():
        allow_images = bool(config.get("allow_images"))
        if not file_obj.is_pdf and not (allow_images and file_obj.is_image):
            continue
        score = _document_classifier_score(sample, config)
        if score:
            matches.append((score, doc_type))

    if not matches:
        return None

    matches.sort(reverse=True)
    if len(matches) > 1 and matches[0][0] == matches[1][0]:
        return None
    return matches[0][1]


def classify_document_type(file_obj: UnifiedFile) -> Optional[str]:
    by_name = _classify_document_sample(file_obj.name, file_obj)
    if by_name:
        return by_name
    return _classify_document_sample(file_obj.extracted_text, file_obj)


def classify_document_type_with_method(file_obj: UnifiedFile) -> Tuple[Optional[str], str]:
    by_name = _classify_document_sample(file_obj.name, file_obj)
    if by_name:
        return by_name, "nombre"

    by_content = _classify_document_sample(file_obj.extracted_text, file_obj)
    if by_content:
        return by_content, "contenido"

    return None, "desconocido"


def validate_cuenta_cobro_package(files: List[UnifiedFile]) -> Dict[str, object]:
    identified: Dict[str, List[str]] = {doc_type: [] for doc_type in CUENTA_COBRO_REQUIRED_DOCS}
    unknown: List[str] = []

    for file_obj in files:
        if not (file_obj.is_pdf or file_obj.is_image):
            continue

        doc_type, method = classify_document_type_with_method(file_obj)
        if not doc_type:
            unknown.append(file_obj.name)
            continue
        identified.setdefault(doc_type, [])
        identified[doc_type].append(f"{file_obj.name} ({method})")

    faltantes = [doc_type for doc_type in CUENTA_COBRO_REQUIRED_DOCS if not identified[doc_type]]
    duplicados = {doc_type: names for doc_type, names in identified.items() if len(names) > 1}
    identificados = {doc_type: names for doc_type, names in identified.items() if names}
    identified_required_count = sum(1 for doc_type in CUENTA_COBRO_REQUIRED_DOCS if identified[doc_type])

    complete = not faltantes
    complete_with_unknown = bool(faltantes) and len(unknown) == 1 and identified_required_count >= 4
    estado = "completo" if complete else "completo_con_desconocido" if complete_with_unknown else "incompleto"
    mensaje = (
        "Recibido archivos completos"
        if complete
        else "Recibido archivos completos con un documento no identificado"
        if complete_with_unknown
        else "Faltan documentos obligatorios"
    )
    return {
        "estado": estado,
        "mensaje": mensaje,
        "identificados": identificados,
        "faltantes": faltantes,
        "duplicados": duplicados,
        "desconocidos": unknown,
    }


def classify_invoice_type(xml_count: int) -> str:
    return "FACTURA ELECTRONICA" if xml_count >= 1 else "CUENTA DE COBRO"


def validate_pdf_minimum(invoice_type: str, pdf_count: int) -> Optional[str]:
    if invoice_type == "CUENTA DE COBRO" and pdf_count < MIN_PDF_CC:
        return "Cuenta de cobro: archivos incompletos, revisa tus documentos y que estén completos."
    return None


def format_missing_documents(doc_types: List[str]) -> List[str]:
    return [DOCUMENT_LABELS.get(doc_type, str(doc_type).replace("_", " ")) for doc_type in doc_types]


# endregion
# region ✉️ Construcción de respuestas
# ============================================================
# RESPONSES
# ============================================================
def build_rejected_email(radicado: str, invoice_type: str, reasons: List[str], client_name: Optional[str]) -> Tuple[str, str]:
    subject = f"RECHAZADO - facturacion no radicada (ID: {radicado})"
    reasons_lines = "\n".join(f"  - {r}" for r in reasons) if reasons else "  - Documentación incompleta o no identificada."
    body = (
        "Hola,\n\n"
        "Recibimos tu correo, pero no fue posible radicarlo.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente identificado: {client_name or 'No identificado'}\n"
        f"Clasificación detectada: {invoice_type}\n\n"
        "Motivos del rechazo:\n"
        f"{reasons_lines}\n\n"
        "Por favor revisa que la documentación esté completa y vuelve a enviar.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


def build_approved_email(radicado: str, invoice_type: str, client_name: str, pdf_count: int, xml_count: int) -> Tuple[str, str]:
    subject = f"APROBADO - facturacion recibida correctamente (ID: {radicado})"
    body = (
        "Hola,\n\n"
        "Confirmamos que tu correo fue recibido y validado correctamente.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente: {client_name}\n"
        f"Clasificación: {invoice_type}\n"
        f"PDF detectados: {pdf_count}\n"
        f"XML detectados: {xml_count}\n\n"
        "Tu radicación queda en proceso interno.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


def build_forward_body(
    rejection_text: str,
    from_header: str,
    fecha: str,
    asunto: str,
    para: str,
    body_text: str,
) -> str:
    original_text = body_text if body_text else "(el correo original no tenía texto)"
    return (
        f"{rejection_text}\n\n"
        "---------- Mensaje original ----------\n"
        f"De: {from_header}\n"
        f"Fecha: {fecha}\n"
        f"Asunto: {asunto}\n"
        f"Para: {para}\n\n"
        f"{original_text}\n"
    )


def send_reply_email(gmail_service, original_msg: Dict, to_email: str, subject: str, body: str) -> None:
    original_payload = original_msg.get("payload", {}) or {}
    original_subject = get_header(original_payload, "Subject")
    original_message_id = get_header(original_payload, "Message-ID")
    original_references = get_header(original_payload, "References")
    reply_subject = original_subject if original_subject.lower().startswith("re:") else f"Re: {original_subject or subject}"
    references = " ".join(x for x in [original_references, original_message_id] if x).strip()
    extra_headers = {
        "In-Reply-To": original_message_id,
        "References": references,
    }

    payload = {"raw": create_raw_email(to_email, reply_subject, body, extra_headers=extra_headers)}
    thread_id = original_msg.get("threadId")
    if thread_id:
        payload["threadId"] = thread_id
    gmail_service.users().messages().send(userId="me", body=payload).execute()


def send_new_email(gmail_service, to_email: str, subject: str, body: str) -> None:
    payload = {"raw": create_raw_email(to_email, subject, body)}
    gmail_service.users().messages().send(userId="me", body=payload).execute()


def send_forward_with_attachments(
    gmail_service,
    to_email: str,
    subject: str,
    body: str,
    attachments: List[Dict[str, object]],
) -> None:
    payload = {"raw": create_forward_email(to_email, subject, body, attachments)}
    gmail_service.users().messages().send(userId="me", body=payload).execute()


def build_token_alert_email(failed_account: str) -> Tuple[str, str]:
    subject = f"⚠️ Token vencido: {failed_account} — requiere reactivación"
    body = (
        "Hola,\n\n"
        f"El token OAuth de la cuenta {failed_account} se venció o dejó de ser válido.\n"
        "Esa cuenta ya no está procesando correos hasta que se reactive el acceso.\n\n"
        "Para reactivarla:\n"
        "En el equipo autorizado, con el entorno del proyecto:\n"
        f"1) Ejecutar: python reademail.py --authorize-account {failed_account}\n"
        f"2) Iniciar sesión con {failed_account} en el navegador y aceptar permisos\n"
        f"3) Copiar el token.json generado al servidor (accounts/{failed_account}/token.json) y reiniciar el servicio\n\n"
        "Este aviso se genera automáticamente para evitar que el servicio quede bloqueado esperando autorización interactiva.\n"
    )
    return subject, body


def should_send_token_alert(last_sent_ts: Optional[float], now: float, cooldown_hours: int) -> bool:
    if not last_sent_ts:
        return True
    try:
        last_sent = float(last_sent_ts)
    except (TypeError, ValueError):
        return True
    return (now - last_sent) >= cooldown_hours * 60 * 60


def should_send_whatsapp(cache: Dict[str, float], mensaje_key: str, ahora: float, cooldown_min: int) -> bool:
    last_sent = cache.get(mensaje_key)
    if last_sent is not None and (ahora - last_sent) < cooldown_min * 60:
        return False
    cache[mensaje_key] = ahora
    return True


def send_whatsapp_alert(mensaje: str, cooldown_key: Optional[str] = None) -> None:
    try:
        if not WHATSAPP_ALERT_ENABLED or not WHATSAPP_PHONE or not WHATSAPP_APIKEY:
            logger.debug("Alerta WhatsApp deshabilitada o sin configuración completa")
            return

        area = mensaje.partition("]")[0].lstrip("[")
        mensaje_key = cooldown_key or {
            "Loop Pub/Sub": "loop",
            "Procesar correo": "procesar",
            "Token": "token",
            "Configuración": "config",
            "Rebote": "rebote",
        }.get(area, area.lower() or "general")
        if not should_send_whatsapp(
            _WHATSAPP_ALERT_CACHE,
            mensaje_key,
            time.time(),
            WHATSAPP_COOLDOWN_MIN,
        ):
            logger.debug("Alerta WhatsApp omitida por cooldown: %s", mensaje_key)
            return

        texto = f"🚨 ReadMail: {mensaje}"[:250]
        url = (
            "https://api.callmebot.com/whatsapp.php"
            f"?phone={urllib.parse.quote(WHATSAPP_PHONE)}"
            f"&text={urllib.parse.quote(texto)}"
            f"&apikey={urllib.parse.quote(WHATSAPP_APIKEY)}"
        )
        urllib.request.urlopen(url, timeout=10)
    except Exception as error:
        logger.warning("⚠️ No se pudo enviar alerta WhatsApp: %s", error)


def send_token_alert(gmail_service, to_email: str, failed_account: str) -> None:
    send_whatsapp_alert(f"[Token] El token de {failed_account} se venció, requiere reactivación")
    if not to_email:
        print(f"⚠️ TOKEN_ALERT_EMAIL vacío; no se envía alerta de token para {failed_account}")
        return
    subject, body = build_token_alert_email(failed_account)
    payload = {"raw": create_raw_email(to_email, subject, body)}
    gmail_service.users().messages().send(userId="me", body=payload).execute()


def decide_rejection_recipient(
    sender_email: str,
    enabled: bool,
    xml_bytes: Optional[bytes],
    subject: str,
    catalog: List[ClientRecord],
    fallback_email: str,
) -> Tuple[Optional[str], str, bool]:
    if not enabled:
        return None, "deshabilitado", False
    has_xml = bool(xml_bytes)
    if not (is_tech_provider(subject, sender_email, has_xml) or is_no_reply_sender(sender_email)):
        return None, "remitente_normal", False

    email, source = resolve_alternate_recipient(xml_bytes, subject, catalog, fallback_email)
    return email, source, bool(email)


# endregion
# region 🔄 Procesamiento de mensajes
# ============================================================
# MESSAGE PROCESSING
# ============================================================
def es_correo_antiguo(internal_date_ms, ahora_ms, max_dias) -> bool:
    if internal_date_ms is None or internal_date_ms == "":
        return False
    try:
        antiguedad_ms = float(ahora_ms) - float(internal_date_ms)
        limite_ms = float(max_dias) * 24 * 60 * 60 * 1000
    except (TypeError, ValueError):
        return False
    return antiguedad_ms > limite_ms


def is_bounce_message(payload: Dict, from_header: str, subject: str) -> bool:
    sender_text = normalize_text(from_header)
    subject_text = normalize_text(subject)
    sender_patterns = ("mailer-daemon", "postmaster", "mail delivery")
    subject_patterns = (
        "delivery status notification",
        "undelivered mail",
        "mail delivery failed",
        "returned to sender",
        "failure notice",
        "no such user",
        "delivery incomplete",
    )
    if any(pattern in sender_text for pattern in sender_patterns):
        return True
    if any(pattern in subject_text for pattern in subject_patterns):
        return True

    mime_type = normalize_text(str((payload or {}).get("mimeType") or ""))
    content_type = normalize_text(get_header(payload or {}, "Content-Type"))
    return mime_type.startswith("multipart/report") or (
        "multipart/report" in content_type and "report-type=delivery-status" in content_type
    )


def _extract_bounce_parts_text(part: Dict) -> str:
    texts = [decode_body(((part or {}).get("body") or {}).get("data"))]
    texts.extend(str(header.get("value") or "") for header in ensure_list((part or {}).get("headers")))
    texts.extend(_extract_bounce_parts_text(child) for child in ensure_list((part or {}).get("parts")))
    return "\n".join(text for text in texts if text)


def extract_bounce_info(msg: Dict, payload: Dict, body_text: str) -> Dict[str, Optional[str]]:
    bounce_text = "\n".join(
        text
        for text in (
            get_header(payload, "Subject"),
            body_text,
            str((msg or {}).get("snippet") or ""),
            _extract_bounce_parts_text(payload),
        )
        if text
    )
    radicado_match = re.search(r"RAD-\d{8}-\d{6}", bounce_text, re.IGNORECASE)
    recipient_match = re.search(
        r"(?:Final-Recipient|Original-Recipient|To)\s*:\s*(?:rfc822;\s*)?<?([^\s<>;]+@[^\s<>;]+)>?",
        bounce_text,
        re.IGNORECASE,
    )
    return {
        "radicado": radicado_match.group(0).upper() if radicado_match else None,
        "failed_recipient": recipient_match.group(1).rstrip(".,") if recipient_match else None,
    }


def find_message_id_by_radicado(state: Dict, radicado: Optional[str]) -> Optional[str]:
    if not radicado:
        return None
    mappings = state.get("message_radicados") or {}
    if not isinstance(mappings, dict):
        return None
    return next((str(message_id) for message_id, value in mappings.items() if value == radicado), None)


def safe_get_message_full(gmail_service, message_id: str) -> Optional[Dict]:
    try:
        return gmail_service.users().messages().get(userId="me", id=message_id, format="full").execute()
    except HttpError as e:
        if getattr(e, "resp", None) is not None and e.resp.status == 404:
            print(f"⚠️ Gmail 404: messageId {message_id} ya no existe. SKIP.")
            return None
        raise


def process_message(
    gmail_service,
    sheets_service,
    message_id: str,
    catalog: List[ClientRecord],
    account_id: Optional[str] = None,
    admin_lookup: Optional[AdminLookup] = None,
    registered_lookup: Optional[RegisteredLookup] = None,
) -> None:
    state = load_state(account_id)

    if state_has_replied(state, message_id):
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    if message_id in state_get_processed_set(state):
        return

    radicado = get_or_create_radicado(message_id, state)
    save_state(state, account_id)

    msg = safe_get_message_full(gmail_service, message_id)
    if not msg:
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    payload = msg.get("payload", {}) or {}
    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    body_text = extract_plain_text(payload)
    snippet = msg.get("snippet", "") or ""

    if is_bounce_message(payload, from_header, subject):
        bounce_info = extract_bounce_info(msg, payload, body_text)
        bounce_radicado = bounce_info["radicado"]
        failed_recipient = bounce_info["failed_recipient"]
        original_message_id = find_message_id_by_radicado(state, bounce_radicado)
        if original_message_id:
            try:
                apply_single_status_label(gmail_service, original_message_id, LABEL_REVIEW_NAME)
            except Exception as error:
                logger.error("❌ Falló re-etiquetado del original: %s", error)
        try:
            apply_single_status_label(
                gmail_service,
                message_id,
                LABEL_REVIEW_NAME,
                archive=ARCHIVE_REVIEW,
            )
        except Exception as error:
            logger.error("❌ Falló etiquetado del rebote para Revisión Manual: %s", error)
        cuenta_afectada = account_id or "cuenta única"
        send_whatsapp_alert(
            f"[Rebote] El rechazo {bounce_radicado or 'desconocido'} rebotó — "
            f"el correo {failed_recipient or 'destino'} no recibió el mensaje. "
            f"Cuenta: {cuenta_afectada}. "
            "Factura movida a Revisión Manual.",
            cooldown_key=(
                f"rebote:{account_id or 'cuenta_unica'}:{bounce_radicado or message_id}"
            ),
        )
        print(
            f"↩️ REBOTE | cuenta={account_id} | radicado={bounce_radicado} | "
            f"destino_fallido={failed_recipient} -> Revisión Manual"
        )
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    ahora_ms = int(time.time() * 1000)
    if (
        LIMITE_ANTIGUEDAD_ENABLED
        and not MODO_PRUEBAS
        and es_correo_antiguo(msg.get("internalDate"), ahora_ms, MAX_DIAS_ANTIGUEDAD)
    ):
        apply_single_status_label(gmail_service, message_id, LABEL_REVIEW_NAME, archive=ARCHIVE_REVIEW)
        print(
            f"🕒 Correo con más de {MAX_DIAS_ANTIGUEDAD} días -> "
            f"REVISIÓN MANUAL, no se responde | {radicado}"
        )
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    sender_email = extract_sender_email(from_header)

    if not sender_email:
        print(f"⚠️ No pude extraer email del remitente. From: {from_header}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    attachments = collect_attachments(payload)
    if ONLY_WITH_ATTACHMENTS and not attachments:
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    combined_email_text = f"{subject}\n{body_text}\n{snippet}"

    # 1) Descargar adjuntos y abrir ZIP/ZIP anidados desde el inicio.
    unified_files, zip_errors, zip_analyses = build_unified_files(gmail_service, message_id, attachments)
    pdfs = [f for f in unified_files if f.is_pdf]
    xmls = [f for f in unified_files if f.is_xml]
    images = [f for f in unified_files if f.is_image]

    # 2) Ruta administrativa por NIT/nombre del catálogo o por asunto en hojas administrativas
    current_admin_lookup = admin_lookup or AdminLookup(set(), set(), {})
    auto_fill_nit_from_subject(sheets_service, subject, current_admin_lookup)

    nit = extract_nit_from_text(combined_email_text)
    matched_nit_client = find_client_by_nit(nit, catalog) if nit else None
    if not matched_nit_client:
        matched_nit = find_client_by_nit_in_text(combined_email_text, catalog)
        if matched_nit:
            nit, matched_nit_client = matched_nit
    matched_admin_subject = is_administrativa_by_subject(
        subject,
        current_admin_lookup.admin_nits,
        current_admin_lookup.admin_names,
    )
    if matched_nit_client or matched_admin_subject:
        apply_single_status_label(gmail_service, message_id, LABEL_ADMIN_NAME, archive=ARCHIVE_ADMIN)
        matched_name = matched_nit_client.name if matched_nit_client else "asunto administrativo"
        print(f"🟦 ADMINISTRATIVA | {radicado} | NIT={nit} | cliente={matched_name}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    # La búsqueda por nombre en texto del correo se eliminó porque generaba falsos positivos:
    # proveedores que mencionan el nombre del cliente en el asunto/cuerpo eran marcados como
    # ADMINISTRATIVA en lugar de pasar por el flujo de validación de facturas.

    # 2.b) MEDICIÓN EN SECO de la regla "entidad no registrada -> REVISIÓN MANUAL".
    # Solo observa y registra: no etiqueta, no responde y no altera la ruta. Sirve para
    # dimensionar el volumen que caería en revisión manual antes de conectar la regla.
    current_registered_lookup = registered_lookup or RegisteredLookup(set(), set(), {})
    is_registered = is_registered_entity_by_subject(
        subject,
        current_registered_lookup.registered_nits,
        current_registered_lookup.registered_names,
    )
    if not is_registered:
        print(
            f"🔎 [SIMULACIÓN] Entidad no registrada — con la regla nueva iría a "
            f"REVISIÓN MANUAL | asunto={subject[:60]} | {radicado}"
        )

    # 3) Nota de crédito: texto del correo
    if contains_credit_or_debit_note_text(combined_email_text):
        apply_single_status_label(gmail_service, message_id, LABEL_NOTE_CREDIT_NAME, archive=ARCHIVE_NOTE_CREDIT)
        print(f"🟪 NOTA DE CREDITO por correo | {radicado}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    invoice_type = classify_invoice_type(len(xmls))

    if invoice_type != "CUENTA DE COBRO" and zip_errors:
        apply_single_status_label(
            gmail_service,
            message_id,
            LABEL_REVIEW_NAME,
            archive=ARCHIVE_REVIEW,
        )
        logger.warning(
            "🟨 REVISIÓN MANUAL | %s | no se pudo leer el paquete: %s",
            radicado,
            zip_errors,
        )
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    # 4) Si no hay al menos 1 PDF -> revisión manual
    if len(pdfs) < 1:
        apply_single_status_label(gmail_service, message_id, LABEL_REVIEW_NAME, archive=ARCHIVE_REVIEW)
        print(f"🟨 REVISION MANUAL | {radicado} | sin PDF unificado | ZIP errors={zip_errors}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    # 5) Nota de crédito: nombre del PDF
    if is_note_credit_by_filename(pdfs):
        apply_single_status_label(gmail_service, message_id, LABEL_NOTE_CREDIT_NAME, archive=ARCHIVE_NOTE_CREDIT)
        print(f"🟪 NOTA DE CREDITO por nombre | {radicado}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    # 6) Nota de crédito: texto del PDF
    if is_note_credit_by_text(pdfs):
        apply_single_status_label(gmail_service, message_id, LABEL_NOTE_CREDIT_NAME, archive=ARCHIVE_NOTE_CREDIT)
        print(f"🟪 NOTA DE CREDITO por texto | {radicado}")
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    if invoice_type == "CUENTA DE COBRO" and not any(
        classify_document_type(file_obj) == "cuenta_cobro"
        for file_obj in pdfs + images
    ):
        apply_single_status_label(
            gmail_service,
            message_id,
            LABEL_REVIEW_NAME,
            archive=ARCHIVE_REVIEW,
        )
        print(
            f"🟨 REVISIÓN MANUAL | {radicado} | "
            "sin XML y ningún PDF declara ser cuenta de cobro"
        )
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    reasons: List[str] = []
    cuenta_cobro_validation: Optional[Dict[str, object]] = None
    if invoice_type == "CUENTA DE COBRO":
        cuenta_cobro_validation = validate_cuenta_cobro_package(pdfs + images)
        validation_status = str(cuenta_cobro_validation.get("estado") or "")

        if validation_status == "incompleto":
            faltantes = cuenta_cobro_validation.get("faltantes") or []
            reasons.append("Cuenta de cobro incompleta. Faltan: " + ", ".join(str(x) for x in faltantes) + ".")
        print("📦 Validación cuenta de cobro:", json.dumps(cuenta_cobro_validation, ensure_ascii=False))
        if zip_errors:
            reasons.extend(zip_errors)

    has_order = bool(cuenta_cobro_validation and "orden_compra" in (cuenta_cobro_validation.get("identificados") or {}))
    if invoice_type != "CUENTA DE COBRO":
        has_order = detect_order(pdfs)
    client_catalog_only = client_lookup_catalog(catalog)
    client_candidate_texts = [subject, body_text, snippet] + [f.name for f in pdfs] + [f.extracted_text for f in pdfs]
    order_client_result = identify_client_in_order_pdfs(pdfs, client_catalog_only) if invoice_type != "CUENTA DE COBRO" else ClientMatchResult()
    if order_client_result.record:
        client_match = order_client_result.record
        logger.info("✅ Fuente final cliente: ORDER_BLOCK")
    elif order_client_result.raw:
        client_match = None
        logger.info("⚠️ Fuente final cliente: ORDER_BLOCK sin match de catálogo")
    elif invoice_type != "CUENTA DE COBRO" and has_order:
        client_match = None
        logger.info("⚠️ Fuente final cliente: ORDER_BLOCK sin cliente detectado")
    else:
        client_match = identify_client_from_fields(client_candidate_texts, client_catalog_only) or identify_client(
            candidate_texts=client_candidate_texts,
            catalog=client_catalog_only,
        )
        logger.info(f"🔎 Fuente final cliente: {'FALLBACK' if client_match else 'SIN_CLIENTE'}")
    if invoice_type != "CUENTA DE COBRO" and not client_match:
        apply_single_status_label(
            gmail_service,
            message_id,
            LABEL_REVIEW_NAME,
            archive=ARCHIVE_REVIEW,
        )
        logger.info(
            "🟨 REVISIÓN MANUAL | %s | factura electrónica sin cliente identificado; "
            "no se responde al proveedor",
            radicado,
        )
        state_add_processed(state, message_id)
        save_state(state, account_id)
        return

    has_ok_compras = detect_ok_compras(pdfs)
    missing_purchase_documents: List[str] = []
    if invoice_type != "CUENTA DE COBRO":
        if not has_order:
            reasons.append("No se detectó orden de compra en nombre ni texto de los PDF.")
            missing_purchase_documents.append("orden de compra")
        if not has_ok_compras:
            reasons.append("No se detectó OK de compras dentro de los PDF.")
            missing_purchase_documents.append("OK de compras")

    if reasons and invoice_type != "CUENTA DE COBRO" and MODO_PRUEBAS:
        if COMPRAS_EMAIL:
            faltantes = " y ".join(missing_purchase_documents)
            request_text = (
                f"Falta documentación para completar la radicación {radicado}:\n"
                + "\n".join(f"- {item}" for item in missing_purchase_documents)
                + "\n\nPor favor responder este correo adjuntando el archivo requerido."
            )
            forward_body = build_forward_body(
                rejection_text=request_text,
                from_header=from_header,
                fecha=get_header(payload, "Date"),
                asunto=subject,
                para=get_header(payload, "To"),
                body_text=body_text,
            )
            forward_subject = f"Falta documentación - {subject} (ID: {radicado})"
            original_attachments: List[Dict[str, object]] = []
            for attachment in attachments:
                attachment_id = attachment.get("attachmentId")
                if not attachment_id:
                    continue
                filename = str(attachment.get("filename") or "adjunto")
                try:
                    attachment_data = gmail_download_attachment_bytes(
                        gmail_service,
                        message_id,
                        attachment_id,
                    )
                except Exception as error:
                    logger.warning(
                        "No pude descargar adjunto original %s para Compras: %s",
                        filename,
                        error,
                    )
                    continue
                original_attachments.append(
                    {
                        "filename": filename,
                        "mime_type": attachment.get("mimeType") or "application/octet-stream",
                        "data": attachment_data,
                    }
                )

            try:
                attachments_size = sum(len(item["data"]) for item in original_attachments)
                if attachments_size > 20 * 1024 * 1024:
                    forward_body += "\nLa factura no se reenvió por superar el tamaño permitido.\n"
                    logger.warning(
                        "Adjuntos para Compras superan 20 MB; se envía solo texto | %s",
                        radicado,
                    )
                    send_new_email(
                        gmail_service,
                        COMPRAS_EMAIL,
                        forward_subject,
                        forward_body,
                    )
                elif original_attachments:
                    send_forward_with_attachments(
                        gmail_service,
                        COMPRAS_EMAIL,
                        forward_subject,
                        forward_body,
                        original_attachments,
                    )
                else:
                    send_new_email(
                        gmail_service,
                        COMPRAS_EMAIL,
                        forward_subject,
                        forward_body,
                    )
                logger.info(
                    "📨 Reenviado a Compras (%s) por falta de: %s | %s",
                    COMPRAS_EMAIL,
                    faltantes,
                    radicado,
                )
            except Exception as error:
                logger.error(
                    "❌ Falló reenvío a Compras (%s) por falta de: %s | %s: %s",
                    COMPRAS_EMAIL,
                    faltantes,
                    radicado,
                    error,
                )
                send_whatsapp_alert(
                    f"[Reenvío Compras] Falló el reenvío de {radicado} "
                    f"a {COMPRAS_EMAIL}: {error}"
                )

            try:
                gmail_service.users().messages().modify(
                    userId="me",
                    id=message_id,
                    body={"addLabelIds": ["UNREAD"]},
                ).execute()
            except Exception as error:
                logger.error("❌ Falló marcado como no leído | %s: %s", radicado, error)

            state_mark_replied(state, message_id)
            state_add_processed(state, message_id)
            save_state(state, account_id)
            return

        logger.warning(
            "MODO_PRUEBAS activo pero COMPRAS_EMAIL está vacío; "
            "se aplica rechazo normal | %s",
            radicado,
        )

    if reasons:
        apply_single_status_label(gmail_service, message_id, LABEL_REJECTED_NAME, archive=ARCHIVE_REJECTED)
        subject_reply, body_reply = build_rejected_email(
            radicado=radicado,
            invoice_type=invoice_type,
            reasons=reasons,
            client_name=client_match.name if client_match else None,
        )

        def attempt_rejection_send(destination: str, send_operation) -> bool:
            try:
                send_operation()
                return True
            except Exception as error:
                logger.error("❌ Falló envío de rechazo a %s: %s", destination, error)
                return False

        if not ALT_RECIPIENT_ENABLED:
            if is_no_reply_sender(sender_email):
                print(f"⏭️ Remitente es no-reply, se omite respuesta | {radicado} | {sender_email}")
            else:
                print(f"✉️ Respondiendo rechazo en el mismo hilo | {radicado} | to={sender_email}")
                sent = attempt_rejection_send(
                    sender_email,
                    lambda: send_reply_email(gmail_service, msg, sender_email, subject_reply, body_reply),
                )
                if sent:
                    print(f"✅ Respuesta de rechazo enviada | {radicado}")
        else:
            tech_provider = is_tech_provider(subject, sender_email, bool(xmls)) or is_no_reply_sender(sender_email)
            if tech_provider:
                xml_bytes = xmls[0].data if xmls else None
                alt_email, alt_source, should_redirect = decide_rejection_recipient(
                    sender_email=sender_email,
                    enabled=ALT_RECIPIENT_ENABLED,
                    xml_bytes=xml_bytes,
                    subject=subject,
                    catalog=catalog,
                    fallback_email=ALT_FALLBACK_EMAIL,
                )
                if should_redirect and alt_email:
                    body_new = build_forward_body(
                        rejection_text=body_reply,
                        from_header=from_header,
                        fecha=get_header(payload, "Date"),
                        asunto=subject,
                        para=get_header(payload, "To"),
                        body_text=body_text,
                    )
                    original_attachments: List[Dict[str, object]] = []
                    for attachment in attachments:
                        attachment_id = attachment.get("attachmentId")
                        if not attachment_id:
                            continue
                        filename = str(attachment.get("filename") or "adjunto")
                        try:
                            attachment_data = gmail_download_attachment_bytes(
                                gmail_service,
                                message_id,
                                attachment_id,
                            )
                        except Exception as e:
                            logger.warning("No pude descargar adjunto original %s para reenviarlo: %s", filename, e)
                            continue
                        original_attachments.append(
                            {
                                "filename": filename,
                                "mime_type": attachment.get("mimeType") or "application/octet-stream",
                                "data": attachment_data,
                            }
                        )

                    forward_subject = f"Rechazo de facturación - {subject} (ID: {radicado})"
                    attachments_size = sum(len(item["data"]) for item in original_attachments)
                    if attachments_size > 20 * 1024 * 1024:
                        body_new += "\nLa factura no se reenvió por superar el tamaño permitido.\n"
                        logger.warning(
                            "Adjuntos de rechazo superan 20 MB; se envía solo texto a %s | %s",
                            alt_email,
                            radicado,
                        )
                        sent = attempt_rejection_send(
                            alt_email,
                            lambda: send_new_email(gmail_service, alt_email, forward_subject, body_new),
                        )
                    elif original_attachments:
                        sent = attempt_rejection_send(
                            alt_email,
                            lambda: send_forward_with_attachments(
                                gmail_service,
                                alt_email,
                                forward_subject,
                                body_new,
                                original_attachments,
                            ),
                        )
                    else:
                        sent = attempt_rejection_send(
                            alt_email,
                            lambda: send_new_email(gmail_service, alt_email, forward_subject, body_new),
                        )
                    if alt_source == "fallback":
                        try:
                            apply_single_status_label(
                                gmail_service,
                                message_id,
                                LABEL_REVIEW_NAME,
                                archive=ARCHIVE_REVIEW,
                            )
                        except Exception as error:
                            logger.error("❌ Falló etiquetado: %s", error)
                    if sent:
                        print(f"✉️ Rechazo desviado a {alt_email} (fuente={alt_source}) | {radicado}")
                else:
                    print(f"⏭️ Remitente es no-reply, se omite respuesta | {radicado} | {sender_email}")
            else:
                print(f"✉️ Respondiendo rechazo en el mismo hilo | {radicado} | to={sender_email}")
                sent = attempt_rejection_send(
                    sender_email,
                    lambda: send_reply_email(gmail_service, msg, sender_email, subject_reply, body_reply),
                )
                if sent:
                    print(f"✅ Respuesta de rechazo enviada | {radicado}")
        state_mark_replied(state, message_id)
        state_add_processed(state, message_id)
        save_state(state, account_id)

        print("\n" + "=" * 80)
        print(f"🟥 RECHAZADO | {radicado}")
        print(f"From: {from_header}")
        print(f"Subject: {subject}")
        print(f"Tipo: {invoice_type}")
        print(f"PDF: {len(pdfs)} | XML: {len(xmls)}")
        print("Motivos:")
        for reason in reasons:
            print(f" - {reason}")
        print("=" * 80)
        return

    apply_single_status_label(gmail_service, message_id, LABEL_APPROVED_NAME, archive=ARCHIVE_APPROVED)
    approved_subject, approved_body = build_approved_email(
        radicado=radicado,
        invoice_type=invoice_type,
        client_name=client_match.name if client_match else "No identificado",
        pdf_count=len(pdfs),
        xml_count=len(xmls),
    )
    if is_no_reply_sender(sender_email):
        print(f"⏭️ Remitente es no-reply, se omite respuesta | {radicado} | {sender_email}")
    else:
        print(f"✉️ Respondiendo aprobación en el mismo hilo | {radicado} | to={sender_email}")
        try:
            send_reply_email(gmail_service, msg, sender_email, approved_subject, approved_body)
            print(f"✅ Respuesta de aprobación enviada | {radicado}")
        except Exception as error:
            logger.error("❌ Falló envío de aprobación a %s: %s", sender_email, error)
    state_mark_replied(state, message_id)
    state_add_processed(state, message_id)
    save_state(state, account_id)

    print("\n" + "=" * 80)
    print(f"🟩 APROBADO | {radicado}")
    print(f"From: {from_header}")
    print(f"Subject: {subject}")
    print(f"Cliente: {client_match.name if client_match else 'No identificado'}")
    print(f"Tipo: {invoice_type}")
    print(f"PDF: {len(pdfs)} | XML: {len(xmls)}")
    if zip_analyses:
        for analysis in zip_analyses:
            print(f"ZIP: {analysis.get('zip_filename')} | ok={analysis.get('ok')} | pdf={analysis.get('pdf_count')} | xml={analysis.get('xml_count')} | err={analysis.get('error')}")
    print("=" * 80)


# endregion
# region 👂 Loop de Pub/Sub
# ============================================================
# PUBSUB LOOP
# ============================================================
def listen_pubsub(accounts: Dict[str, Dict]) -> None:
    """
    accounts: dict con clave = email en minúsculas, valor = {
        "gmail_service", "sheets_service", "catalog_data", "admin_lookup",
        "registered_lookup", "account_id"
    }
    """
    subscriber = pubsub_v1.SubscriberClient(transport="rest")
    subscription_path = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    account_list = ", ".join(accounts.keys()) or "(cuenta única)"
    print(f"👂 Escuchando Pub/Sub (PULL/REST): {subscription_path}")
    print(f"   Cuentas activas: {account_list}")

    backoff = 1

    def refresh_catalog_for(acc: Dict) -> None:
        try:
            acc["catalog_data"] = load_client_catalog(acc["sheets_service"])
            acc["admin_lookup"] = load_admin_lookup(acc["sheets_service"])
            acc["registered_lookup"] = load_registered_entities(acc["sheets_service"])
            print(f"🔄 Catálogo actualizado ({acc['account_id'] or 'cuenta única'}): {len(acc['catalog_data'])} registros")
        except Exception as e:
            print(f"⚠️ No pude refrescar el catálogo: {e}")

    while True:
        try:
            for acc in accounts.values():
                ensure_gmail_watch(acc["gmail_service"], account_id=acc["account_id"])

            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": PUBSUB_PULL_MAX,
                }
            )

            if not response.received_messages:
                time.sleep(IDLE_SLEEP_SEC)
                backoff = 1
                continue

            for rm in response.received_messages:
                ack_id = rm.ack_id
                should_ack = True
                try:
                    subscriber.modify_ack_deadline(
                        request={
                            "subscription": subscription_path,
                            "ack_ids": [ack_id],
                            "ack_deadline_seconds": PUBSUB_ACK_DEADLINE_SECONDS,
                        }
                    )
                except Exception as e:
                    print(f"⚠️ No pude extender ack deadline: {e}")

                try:
                    raw = rm.message.data.decode("utf-8")
                    event_payload = json.loads(raw)
                    history_id = str(event_payload.get("historyId", "")).strip()
                    email_address = (event_payload.get("emailAddress", "") or "").lower()

                    if not history_id:
                        print("⚠️ Evento sin historyId. Lo ignoro.")
                        continue

                    acc = accounts.get(email_address)
                    if acc is None:
                        print(f"⚠️ Evento para cuenta no configurada: {email_address}. Ignorando.")
                        continue

                    account_id = acc["account_id"]
                    acc_gmail = acc["gmail_service"]
                    acc_sheets = acc["sheets_service"]

                    state = load_state(account_id)
                    last_history = str(state.get("last_history_id") or "").strip()

                    if not last_history:
                        update_last_history_id(history_id, account_id)
                        print(f"🔧 Inicialicé last_history_id={history_id} ({email_address})")
                        continue

                    try:
                        if MODO_PRUEBAS:
                            history_label_id = resolve_modo_pruebas_label_id(acc_gmail, account_id)
                            _, latest_history = fetch_new_message_ids(
                                acc_gmail,
                                last_history,
                                label_id=history_label_id,
                                include_label_added=True,
                            )
                            labeled_ids = fetch_labeled_message_ids(acc_gmail, history_label_id)
                            processed_ids = state_get_processed_set(state)
                            new_ids = [mid for mid in labeled_ids if mid not in processed_ids]
                            print(
                                f"🧪 MODO PRUEBAS: {len(new_ids)} correo(s) con etiqueta pruebas por procesar"
                            )
                        else:
                            new_ids, latest_history = fetch_new_message_ids(
                                acc_gmail,
                                last_history,
                                label_id=None,
                                include_label_added=False,
                            )
                    except HttpError as he:
                        if getattr(he, "resp", None) is not None and he.resp.status in (400, 404):
                            update_last_history_id(history_id, account_id)
                            print(f"⚠️ HistoryId viejo/inválido. Reseteado a {history_id} ({email_address})")
                            continue
                        raise

                    if latest_history:
                        update_last_history_id(latest_history, account_id)

                    if not new_ids:
                        print(f"🔔 Evento ({email_address}) historyId={history_id} sin mensajes nuevos")
                    else:
                        print(f"🔔 Evento ({email_address}) historyId={history_id} -> {len(new_ids)} mensaje(s)")
                        refresh_catalog_for(acc)
                        for mid in new_ids:
                            process_message(
                                acc_gmail,
                                acc_sheets,
                                mid,
                                acc["catalog_data"],
                                account_id,
                                acc["admin_lookup"],
                                acc.get("registered_lookup"),
                            )

                except Exception as e:
                    should_ack = False
                    print(f"❌ Error procesando evento Pub/Sub: {e}")
                    send_whatsapp_alert(f"[Procesar correo] Falló el procesamiento de un correo: {e}")
                finally:
                    if not should_ack:
                        print("↩️ Evento no confirmado; Pub/Sub lo reintentará.")
                        continue
                    try:
                        subscriber.acknowledge(
                            request={
                                "subscription": subscription_path,
                                "ack_ids": [ack_id],
                            }
                        )
                    except Exception as e:
                        print(f"⚠️ No pude ACK: {e}")

            backoff = 1

        except KeyboardInterrupt:
            print("🛑 Listener detenido.")
            break
        except Exception as e:
            print(f"❌ Error en loop Pub/Sub: {e}")
            send_whatsapp_alert(f"[Loop Pub/Sub] El programa tuvo un error y reintentará: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


# endregion
# region 🔓 Autorización de cuentas
# ============================================================
# AUTHORIZE ACCOUNT
# ============================================================
def authorize_account(email: str) -> None:
    account_dir = os.path.join(ACCOUNTS_DIR, email)
    os.makedirs(account_dir, exist_ok=True)
    print(f"🔐 Iniciando flujo OAuth para: {email}")
    print(f"📁 Directorio: {account_dir}")
    creds = get_oauth_creds(account_dir, account_email=email, interactive_auth=True)
    svc = build("gmail", "v1", credentials=creds)
    profile = svc.users().getProfile(userId="me").execute()
    actual_email = profile.get("emailAddress", "")
    if actual_email.lower() != email.lower():
        print(f"⚠️  ADVERTENCIA: autenticaste como {actual_email}, no como {email}.")
        print("   El token se guardó de todas formas. Usa el email real al configurar GMAIL_ACCOUNTS.")
    else:
        print(f"✅ Cuenta autorizada correctamente: {actual_email}")
    print()
    print("Para añadir esta cuenta al sistema, edita el archivo .env:")
    print(f"  GMAIL_ACCOUNTS=...,{email}")
    print("  ACCOUNTS_DIR=accounts")
    print("Luego reinicia el proceso.")


# endregion
# region 🚀 Main
# ============================================================
# MAIN
# ============================================================
def alert_failed_token_accounts(failed_accounts: List[Tuple[str, Optional[str]]], accounts: Dict[str, Dict]) -> None:
    if not failed_accounts:
        return
    if not accounts:
        for failed_account, _ in failed_accounts:
            print(f"⚠️ Token inválido para {failed_account}; no hay cuenta cargada para enviar alerta.")
        return

    notifier = next(iter(accounts.values()))["gmail_service"]
    now = time.time()
    for failed_account, account_id in failed_accounts:
        state = load_state(account_id)
        last_sent = state.get("token_alert_sent_at")
        if not should_send_token_alert(last_sent, now, TOKEN_ALERT_COOLDOWN_HOURS):
            print(f"⏳ Alerta de token omitida por cooldown para {failed_account}")
            continue
        try:
            send_token_alert(notifier, TOKEN_ALERT_EMAIL, failed_account)
        except Exception as e:
            print(f"⚠️ No se pudo enviar alerta de token para {failed_account}: {e}")
            continue
        if TOKEN_ALERT_EMAIL:
            state["token_alert_sent_at"] = now
            save_state(state, account_id)
            print(f"📧 Alerta de token enviada para {failed_account} a {TOKEN_ALERT_EMAIL}")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Sistema de facturación BTL")
    parser.add_argument(
        "--authorize-account",
        metavar="EMAIL",
        help="Autoriza una cuenta Gmail nueva via OAuth interactivo y genera su token.json",
    )
    args = parser.parse_args()

    if args.authorize_account:
        authorize_account(args.authorize_account)
        return

    missing_config = [
        name
        for name, value in (
            ("GCP_PROJECT_ID", GCP_PROJECT_ID),
            ("PUBSUB_SUBSCRIPTION", PUBSUB_SUBSCRIPTION_ID),
            ("PUBSUB_TOPIC_FULL", PUBSUB_TOPIC_FULL),
        )
        if not value
    ]
    if missing_config:
        missing_names = ", ".join(missing_config)
        send_whatsapp_alert(
            f"[Configuración] Falta {missing_names} en el .env, el programa no puede iniciar"
        )
        raise RuntimeError("Faltan env vars: GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION, PUBSUB_TOPIC_FULL.")

    if MODO_PRUEBAS:
        print(
            f"🧪 MODO PRUEBAS ACTIVO: solo se procesan correos con etiqueta '{ETIQUETA_PRUEBAS}'. "
            "Bandeja de entrada IGNORADA."
        )

    if GMAIL_ACCOUNTS:
        # --- Modo multi-cuenta ---
        accounts: Dict[str, Dict] = {}
        failed_token_accounts: List[Tuple[str, Optional[str]]] = []
        for email in GMAIL_ACCOUNTS:
            email_lc = email.lower()
            account_dir = os.path.join(ACCOUNTS_DIR, email)
            print(f"🔑 Cargando cuenta: {email}")
            try:
                creds = get_oauth_creds(account_dir, account_email=email_lc)
            except TokenAuthError as e:
                print(f"⚠️ {e}. Se continúa con las demás cuentas.")
                failed_token_accounts.append((e.account_email, email_lc))
                continue
            gmail_svc = build("gmail", "v1", credentials=creds)
            sheets_svc = build("sheets", "v4", credentials=creds)
            profile = gmail_svc.users().getProfile(userId="me").execute()
            print(f"   ✅ Autenticado como: {profile.get('emailAddress')}")
            print(f"   📁 State: {_state_file_for_account(email_lc)}")
            catalog = load_client_catalog(sheets_svc)
            admin_lookup = load_admin_lookup(sheets_svc)
            registered_lookup = load_registered_entities(sheets_svc)
            ensure_status_labels(gmail_svc)
            print("   Etiquetas verificadas/creadas")
            ensure_gmail_watch(gmail_svc, account_id=email_lc)
            accounts[email_lc] = {
                "gmail_service": gmail_svc,
                "sheets_service": sheets_svc,
                "catalog_data": catalog,
                "admin_lookup": admin_lookup,
                "registered_lookup": registered_lookup,
                "account_id": email_lc,
            }
        alert_failed_token_accounts(failed_token_accounts, accounts)
        if not accounts:
            print("⚠️ No hay cuentas Gmail cargadas; no se inicia el listener.")
            return
        listen_pubsub(accounts)
    else:
        # --- Modo cuenta única (retrocompatibilidad) ---
        failed_token_accounts: List[Tuple[str, Optional[str]]] = []
        accounts: Dict[str, Dict] = {}
        try:
            creds = get_oauth_creds(account_email="cuenta unica")
        except TokenAuthError as e:
            print(f"⚠️ {e}. No se inicia el listener de cuenta única.")
            failed_token_accounts.append((e.account_email, None))
            alert_failed_token_accounts(failed_token_accounts, accounts)
            return
        gmail_service = build("gmail", "v1", credentials=creds)
        sheets_service = build("sheets", "v4", credentials=creds)
        profile = gmail_service.users().getProfile(userId="me").execute()
        email_lc = (profile.get("emailAddress") or "").lower()
        print("✅ Autenticado como:", profile.get("emailAddress"))
        print("🗂️ STATE_FILE:", STATE_FILE)
        client_catalog = load_client_catalog(sheets_service)
        admin_lookup = load_admin_lookup(sheets_service)
        registered_lookup = load_registered_entities(sheets_service)
        ensure_status_labels(gmail_service)
        print("Etiquetas verificadas/creadas")
        ensure_gmail_watch(gmail_service)
        accounts = {
            email_lc: {
                "gmail_service": gmail_service,
                "sheets_service": sheets_service,
                "catalog_data": client_catalog,
                "admin_lookup": admin_lookup,
                "registered_lookup": registered_lookup,
                "account_id": None,
            }
        }
        listen_pubsub(accounts)


if __name__ == "__main__":
    main()
# endregion
