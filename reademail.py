# -*- coding: utf-8 -*-
"""
Programa de facturación BTL - versión ajustada al nuevo flujo

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
8. Validación de contenido:
   - Tiene orden (nombre o texto)
   - Identifica cliente
   - Tiene OK de compras (texto del PDF)
9. Si todo cumple -> APROBADO y notifica.
10. Si falla -> RECHAZADO y notifica el motivo específico.

No usa OCR. Lee texto digital del PDF.
Si el PDF es escaneado como imagen sin capa de texto, varias validaciones podrían no detectar contenido.
"""

import base64
import io
import json
import os
import os.path
import re
import time
import zipfile
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime
from email.mime.text import MIMEText
from email.utils import parseaddr
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
load_dotenv()

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.cloud import pubsub_v1

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    from PyPDF2 import PdfReader  # type: ignore


# ============================================================
# SCOPES
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


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

SHEET_ID = env_first("CLIENT_SHEET_ID")
SHEET_RANGE = env_first("CLIENT_SHEET_RANGE", default="Clientes!A:Z")
ACTIVE_VALUES = {x.strip().lower() for x in env_first("ACTIVE_VALUES", default="activo,active,si,yes,1,true").split(",") if x.strip()}

# Etiquetas nuevas con fallback al .env viejo
LABEL_ADMIN_NAME = env_first("LABEL_ADMIN_NAME", default="ADMINISTRATIVA")
LABEL_REVIEW_NAME = env_first("LABEL_REVIEW_NAME", default="REVISIÓN MANUAL")
LABEL_NOTE_CREDIT_NAME = env_first("LABEL_NOTE_CREDIT_NAME", default="NOTA DE CRÉDITO")
LABEL_APPROVED_NAME = env_first("LABEL_APPROVED_NAME", "LABEL_ACCEPTED_NAME", default="APROBADOS")
LABEL_REJECTED_NAME = env_first("LABEL_REJECTED_NAME", default="RECHAZADOS")

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

# PDFs mínimos:
# - Nuevo flujo: electrónica=2, cuenta de cobro=4
# - Si existe REQUIRED_PDF_COUNT del .env viejo, lo tomamos como fallback SOLO para cuenta de cobro.
MIN_PDF_FE = env_int("MIN_PDF_FACTURA_ELECTRONICA", default=2)
MIN_PDF_CC = env_int("MIN_PDF_CUENTA_COBRO", "REQUIRED_PDF_COUNT", default=4)

_BASE_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
STATE_FILE = env_first("GMAIL_WATCH_STATE_FILE", default=os.path.join(_BASE_DIR, "gmail_watch_state.json"))

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
        default="ok compras,aprobado compras,aprobada compras,visto bueno compras,vb compras,vobo compras,aprobacion compras,aprobación compras",
    ).split(",") if x.strip()
]

ORDER_REGEXES = [
    re.compile(r"\borden\s+de\s+compra\b", re.IGNORECASE),
    re.compile(r"\borden\b", re.IGNORECASE),
    re.compile(r"\boc\b\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
    re.compile(r"\bop\b\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
    re.compile(r"\borden\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
]

NIT_REGEX = re.compile(r"\bnit\b\s*[:#\-]?\s*([0-9][0-9.\-]{5,20})", re.IGNORECASE)

# ============================================================
# DATA MODELS
# ============================================================
@dataclass
class ClientRecord:
    name: str
    normalized_name: str
    nit: Optional[str] = None
    normalized_nit: Optional[str] = None
    active: bool = True
    raw_row: Dict[str, str] = field(default_factory=dict)


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


def extract_plain_text(payload: Dict) -> str:
    if not payload:
        return ""

    body = payload.get("body", {}) or {}
    if body.get("data") and (payload.get("mimeType") == "text/plain" or not payload.get("parts")):
        return decode_body(body.get("data"))

    for part in ensure_list(payload.get("parts")):
        mime_type = part.get("mimeType", "")
        if mime_type == "text/plain":
            txt = decode_body((part.get("body", {}) or {}).get("data"))
            if txt:
                return txt
        nested = extract_plain_text(part)
        if nested:
            return nested
    return ""


def extract_sender_email(from_header: str) -> Optional[str]:
    _, email = parseaddr(from_header or "")
    return email or None


def create_raw_email(to_email: str, subject: str, body: str) -> str:
    msg = MIMEText(body, _charset="utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject
    return base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")


def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


# ============================================================
# STATE
# ============================================================
def load_state() -> Dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_state(state: Dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
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


# ============================================================
# OAUTH / SERVICES
# ============================================================
def get_oauth_creds() -> Credentials:
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError:
                try:
                    os.remove("token.json")
                except FileNotFoundError:
                    pass
                creds = None

        if not creds or not creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)

        with open("token.json", "w", encoding="utf-8") as token:
            token.write(creds.to_json())

    return creds


# ============================================================
# SHEETS: CLIENTS + NIT WHITELIST
# ============================================================
def _header_aliases() -> Dict[str, Set[str]]:
    return {
        "cliente": {"cliente", "razon social", "razón social", "nombre cliente", "client", "empresa"},
        "nit": {"nit", "nit cliente", "tax id"},
        "estado": {"estado", "activo", "status"},
    }


def _resolve_column_indexes(headers: List[str]) -> Dict[str, Optional[int]]:
    aliases = _header_aliases()
    normalized_headers = [normalize_text(h) for h in headers]
    resolved: Dict[str, Optional[int]] = {"cliente": None, "nit": None, "estado": None}

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


def load_client_catalog(sheets_service) -> List[ClientRecord]:
    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Se seguirá sin catálogo/lista blanca.")
        return []

    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=SHEET_RANGE,
    ).execute()

    values = result.get("values", []) or []
    if not values:
        print("⚠️ La hoja no tiene datos.")
        return []

    headers = [str(x).strip() for x in values[0]]
    indexes = _resolve_column_indexes(headers)

    catalog: List[ClientRecord] = []
    for row in values[1:]:
        if not row:
            continue

        cliente = ""
        nit = ""
        estado = ""

        if indexes.get("cliente") is not None and indexes["cliente"] < len(row):
            cliente = str(row[indexes["cliente"]]).strip()
        if indexes.get("nit") is not None and indexes["nit"] < len(row):
            nit = str(row[indexes["nit"]]).strip()
        if indexes.get("estado") is not None and indexes["estado"] < len(row):
            estado = str(row[indexes["estado"]]).strip()

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
            active=active,
            raw_row={headers[i] if i < len(headers) else str(i): str(v) for i, v in enumerate(row)},
        )
        catalog.append(record)

    print(f"✅ Catálogo/lista blanca cargado: {len(catalog)} registros")
    return catalog


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
        token = record.normalized_name
        if not token or len(token) < 4:
            continue
        if token in haystack_norm:
            score = len(token)
            if best is None or score > best[0]:
                best = (score, record)

    return best[1] if best else None


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


def apply_single_status_label(gmail_service, message_id: str, label_name: str, archive: bool = False) -> None:
    label_ids = {}
    for name in [LABEL_ADMIN_NAME, LABEL_REVIEW_NAME, LABEL_NOTE_CREDIT_NAME, LABEL_APPROVED_NAME, LABEL_REJECTED_NAME]:
        label_ids[name] = ensure_label_exists(gmail_service, name)

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


# ============================================================
# GMAIL WATCH / HISTORY
# ============================================================
def ensure_gmail_watch(gmail_service) -> Dict:
    if not GCP_PROJECT_ID or not PUBSUB_TOPIC_FULL or not PUBSUB_SUBSCRIPTION_ID:
        raise RuntimeError("Faltan env vars: GCP_PROJECT_ID, PUBSUB_TOPIC_FULL, PUBSUB_SUBSCRIPTION.")

    state = load_state()
    now_ms = int(time.time() * 1000)
    expiration = int(state.get("watch_expiration_ms", 0))

    if expiration and (expiration - now_ms) > WATCH_RENEW_WINDOW_MS:
        return state

    body = {
        "topicName": PUBSUB_TOPIC_FULL,
        "labelIds": WATCH_LABEL_IDS,
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
    save_state(state)
    print(f"✅ Watch activo. last_history_id={state.get('last_history_id')}")
    return state


def fetch_new_message_ids(gmail_service, start_history_id: str) -> Tuple[Set[str], Optional[str]]:
    message_ids: Set[str] = set()
    page_token = None
    latest_history_id: Optional[str] = None

    while True:
        resp = gmail_service.users().history().list(
            userId="me",
            startHistoryId=start_history_id,
            historyTypes=["messageAdded"],
            pageToken=page_token,
        ).execute()

        for history in ensure_list(resp.get("history")):
            for added in ensure_list(history.get("messagesAdded")):
                mid = (added.get("message") or {}).get("id")
                if mid:
                    message_ids.add(mid)

        page_token = resp.get("nextPageToken")
        if resp.get("historyId"):
            latest_history_id = str(resp.get("historyId"))

        if not page_token:
            break

    return message_ids, latest_history_id


def update_last_history_id(latest_history_id: Optional[str]) -> None:
    if not latest_history_id:
        return
    state = load_state()
    state["last_history_id"] = str(latest_history_id)
    save_state(state)


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


def analyze_zip_bytes(zip_filename: str, zip_bytes: bytes, depth: int = 1) -> Dict[str, object]:
    out = {
        "zip_filename": zip_filename,
        "ok": True,
        "error": None,
        "files": [],
        "pdf_count": 0,
        "xml_count": 0,
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
            files = []

            for info in infos:
                if info.is_dir():
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
                is_zip = lower.endswith(".zip")
                entry = {
                    "name": name,
                    "size": size,
                    "is_pdf": is_pdf,
                    "is_xml": is_xml,
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

            out["files"] = files
            out["pdf_count"] = pdf_count
            out["xml_count"] = xml_count
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
                if item.is_pdf or item.is_xml:
                    unified.append(item)
            continue

        file_obj = UnifiedFile(name=filename, mime_type=mime_type, data=content, source="direct")
        if file_obj.is_pdf or file_obj.is_xml:
            unified.append(file_obj)

    for item in unified:
        if item.is_pdf and not item.extracted_text:
            item.extracted_text = extract_pdf_text(item.data)

    return unified, zip_errors, zip_analyses


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


def is_note_credit_by_filename(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        normalized_name = normalize_alnum(pdf.name)
        if "notadecredito" in normalized_name or "creditnote" in normalized_name:
            return True
    return False


def is_note_credit_by_text(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        normalized_text = normalize_alnum(pdf.extracted_text)
        if "notadecredito" in normalized_text or "creditnote" in normalized_text:
            return True
    return False


def detect_order(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        sample = f"{pdf.name}\n{pdf.extracted_text}"
        for pattern in ORDER_REGEXES:
            if pattern.search(sample):
                return True
    return False


def detect_ok_compras(pdfs: List[UnifiedFile]) -> bool:
    normalized_patterns = [normalize_alnum(x) for x in OK_COMPRAS_PATTERNS if x.strip()]
    for pdf in pdfs:
        text = normalize_alnum(pdf.extracted_text)
        if any(p and p in text for p in normalized_patterns):
            return True
    return False


def classify_invoice_type(xml_count: int) -> str:
    return "FACTURA ELECTRONICA" if xml_count >= 1 else "CUENTA DE COBRO"


def validate_pdf_minimum(invoice_type: str, pdf_count: int) -> Optional[str]:
    if invoice_type == "FACTURA ELECTRONICA" and pdf_count < MIN_PDF_FE:
        return f"Factura electrónica incompleta: requiere mínimo {MIN_PDF_FE} PDF y llegaron {pdf_count}."
    if invoice_type == "CUENTA DE COBRO" and pdf_count < MIN_PDF_CC:
        return f"Cuenta de cobro incompleta: requiere mínimo {MIN_PDF_CC} PDF y llegaron {pdf_count}."
    return None


# ============================================================
# RESPONSES
# ============================================================
def build_rejected_email(radicado: str, invoice_type: str, reasons: List[str], client_name: Optional[str]) -> Tuple[str, str]:
    subject = f"RECHAZADO – facturación no radicada (ID: {radicado})"
    body = (
        "Hola,\n\n"
        "Recibimos tu correo, pero no fue posible radicarlo.\n\n"
        f"ID interno: {radicado}\n"
        f"Cliente identificado: {client_name or 'No identificado'}\n"
        f"Clasificación detectada: {invoice_type}\n\n"
        "Motivos del rechazo:\n"
        + "".join(f"- {reason}\n" for reason in reasons)
        + "\nCorrige y reenvía el correo con los soportes completos.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


def build_approved_email(radicado: str, invoice_type: str, client_name: str, pdf_count: int, xml_count: int) -> Tuple[str, str]:
    subject = f"APROBADO – facturación recibida correctamente (ID: {radicado})"
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


def send_reply_email(gmail_service, original_msg: Dict, to_email: str, subject: str, body: str) -> None:
    payload = {"raw": create_raw_email(to_email, subject, body)}
    thread_id = original_msg.get("threadId")
    if thread_id:
        payload["threadId"] = thread_id
    gmail_service.users().messages().send(userId="me", body=payload).execute()


# ============================================================
# MESSAGE PROCESSING
# ============================================================
def safe_get_message_full(gmail_service, message_id: str) -> Optional[Dict]:
    try:
        return gmail_service.users().messages().get(userId="me", id=message_id, format="full").execute()
    except HttpError as e:
        if getattr(e, "resp", None) is not None and e.resp.status == 404:
            print(f"⚠️ Gmail 404: messageId {message_id} ya no existe. SKIP.")
            return None
        raise


def process_message(gmail_service, message_id: str, catalog: List[ClientRecord]) -> None:
    state = load_state()

    if state_has_replied(state, message_id):
        state_add_processed(state, message_id)
        save_state(state)
        return

    if message_id in state_get_processed_set(state):
        return

    radicado = get_or_create_radicado(message_id, state)
    save_state(state)

    msg = safe_get_message_full(gmail_service, message_id)
    if not msg:
        state_add_processed(state, message_id)
        save_state(state)
        return

    payload = msg.get("payload", {}) or {}
    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    body_text = extract_plain_text(payload)
    snippet = msg.get("snippet", "") or ""
    sender_email = extract_sender_email(from_header)

    if not sender_email:
        print(f"⚠️ No pude extraer email del remitente. From: {from_header}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    attachments = collect_attachments(payload)
    if ONLY_WITH_ATTACHMENTS and not attachments:
        state_add_processed(state, message_id)
        save_state(state)
        return

    combined_email_text = f"{subject}\n{body_text}\n{snippet}"

    # 1) Ruta administrativa por NIT en lista blanca
    nit = extract_nit_from_text(combined_email_text)
    matched_nit_client = find_client_by_nit(nit, catalog) if nit else None
    if matched_nit_client:
        apply_single_status_label(gmail_service, message_id, LABEL_ADMIN_NAME, archive=ARCHIVE_ADMIN)
        print(f"🟦 ADMINISTRATIVA | {radicado} | NIT={nit} | cliente={matched_nit_client.name}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    # 2) Unificar PDF/XML desde adjuntos directos + ZIP
    unified_files, zip_errors, zip_analyses = build_unified_files(gmail_service, message_id, attachments)
    pdfs = [f for f in unified_files if f.is_pdf]
    xmls = [f for f in unified_files if f.is_xml]

    # 3) Si no hay al menos 1 PDF -> revisión manual
    if len(pdfs) < 1:
        apply_single_status_label(gmail_service, message_id, LABEL_REVIEW_NAME, archive=ARCHIVE_REVIEW)
        print(f"🟨 REVISION MANUAL | {radicado} | sin PDF unificado | ZIP errors={zip_errors}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    # 4) Nota de crédito: nombre del PDF
    if is_note_credit_by_filename(pdfs):
        apply_single_status_label(gmail_service, message_id, LABEL_NOTE_CREDIT_NAME, archive=ARCHIVE_NOTE_CREDIT)
        print(f"🟪 NOTA DE CREDITO por nombre | {radicado}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    # 5) Nota de crédito: texto del PDF
    if is_note_credit_by_text(pdfs):
        apply_single_status_label(gmail_service, message_id, LABEL_NOTE_CREDIT_NAME, archive=ARCHIVE_NOTE_CREDIT)
        print(f"🟪 NOTA DE CREDITO por texto | {radicado}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    invoice_type = classify_invoice_type(len(xmls))

    reasons: List[str] = []
    minimum_error = validate_pdf_minimum(invoice_type, len(pdfs))
    if minimum_error:
        reasons.append(minimum_error)

    if zip_errors:
        reasons.extend(zip_errors)

    has_order = detect_order(pdfs)
    if not has_order:
        reasons.append("No se detectó orden de compra en nombre ni texto de los PDF.")

    client_match = identify_client(
        candidate_texts=[subject, body_text, snippet] + [f.name for f in pdfs] + [f.extracted_text for f in pdfs],
        catalog=catalog,
    )
    if not client_match:
        reasons.append("No se logró identificar el cliente con el contenido del correo o los PDF.")

    has_ok_compras = detect_ok_compras(pdfs)
    if not has_ok_compras:
        reasons.append("No se detectó OK de compras dentro de los PDF.")

    if reasons:
        apply_single_status_label(gmail_service, message_id, LABEL_REJECTED_NAME, archive=ARCHIVE_REJECTED)
        subject_reply, body_reply = build_rejected_email(
            radicado=radicado,
            invoice_type=invoice_type,
            reasons=reasons,
            client_name=client_match.name if client_match else None,
        )
        send_reply_email(gmail_service, msg, sender_email, subject_reply, body_reply)
        state_mark_replied(state, message_id)
        state_add_processed(state, message_id)
        save_state(state)

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
        client_name=client_match.name,
        pdf_count=len(pdfs),
        xml_count=len(xmls),
    )
    send_reply_email(gmail_service, msg, sender_email, approved_subject, approved_body)
    state_mark_replied(state, message_id)
    state_add_processed(state, message_id)
    save_state(state)

    print("\n" + "=" * 80)
    print(f"🟩 APROBADO | {radicado}")
    print(f"From: {from_header}")
    print(f"Subject: {subject}")
    print(f"Cliente: {client_match.name}")
    print(f"Tipo: {invoice_type}")
    print(f"PDF: {len(pdfs)} | XML: {len(xmls)}")
    if zip_analyses:
        for analysis in zip_analyses:
            print(f"ZIP: {analysis.get('zip_filename')} | ok={analysis.get('ok')} | pdf={analysis.get('pdf_count')} | xml={analysis.get('xml_count')} | err={analysis.get('error')}")
    print("=" * 80)


# ============================================================
# PUBSUB LOOP
# ============================================================
def listen_pubsub(gmail_service, sheets_service, client_catalog: List[ClientRecord]) -> None:
    subscriber = pubsub_v1.SubscriberClient(transport="rest")
    subscription_path = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    print(f"👂 Escuchando Pub/Sub (PULL/REST): {subscription_path}")

    catalog_data = client_catalog or []
    backoff = 1

    def refresh_catalog() -> None:
        nonlocal catalog_data
        try:
            catalog_data = load_client_catalog(sheets_service)
            print(f"🔄 Catálogo actualizado: {len(catalog_data)} registros")
        except Exception as e:
            print(f"⚠️ No pude refrescar el catálogo: {e}")

    while True:
        try:
            ensure_gmail_watch(gmail_service)

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
                    email_address = event_payload.get("emailAddress", "")

                    if not history_id:
                        print("⚠️ Evento sin historyId. Lo ignoro.")
                        continue

                    state = load_state()
                    last_history = str(state.get("last_history_id") or "").strip()

                    if not last_history:
                        update_last_history_id(history_id)
                        print(f"🔧 Inicialicé last_history_id={history_id} (primer evento)")
                        continue

                    try:
                        new_ids, latest_history = fetch_new_message_ids(gmail_service, last_history)
                    except HttpError as he:
                        if getattr(he, "resp", None) is not None and he.resp.status in (400, 404):
                            update_last_history_id(history_id)
                            print(f"⚠️ HistoryId viejo/inválido. Reseteado a {history_id}")
                            continue
                        raise

                    if latest_history:
                        update_last_history_id(latest_history)

                    if not new_ids:
                        print(f"🔔 Evento ({email_address}) historyId={history_id} sin mensajes nuevos")
                    else:
                        print(f"🔔 Evento ({email_address}) historyId={history_id} -> {len(new_ids)} mensaje(s)")
                        refresh_catalog()
                        for mid in new_ids:
                            process_message(gmail_service, mid, catalog_data)

                except Exception as e:
                    print(f"❌ Error procesando evento Pub/Sub: {e}")
                finally:
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
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    if not GCP_PROJECT_ID or not PUBSUB_SUBSCRIPTION_ID or not PUBSUB_TOPIC_FULL:
        raise RuntimeError("Faltan env vars: GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION, PUBSUB_TOPIC_FULL.")

    creds = get_oauth_creds()
    gmail_service = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    profile = gmail_service.users().getProfile(userId="me").execute()
    print("✅ Autenticado como:", profile.get("emailAddress"))
    print("🗂️ STATE_FILE:", STATE_FILE)

    client_catalog = load_client_catalog(sheets_service)
    ensure_gmail_watch(gmail_service)
    listen_pubsub(gmail_service, sheets_service, client_catalog)


if __name__ == "__main__":
    main()
