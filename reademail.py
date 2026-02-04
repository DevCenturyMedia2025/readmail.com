# -*- coding: utf-8 -*-
"""
Gmail push listener: Gmail Watch -> Pub/Sub (PULL/REST) -> History -> Messages

Incluye:
- ✅ Radicado secuencial estable por correo (idempotente por Gmail messageId)
- ✅ Respuesta automática por correo (APROBADO / RECHAZADO)
- ✅ Validaciones comunicado 2026:
  - Recepción: L–V 9:00 a.m. a 5:00 p.m. (Bogotá)
  - Cierre mensual 2026 (si llega después => RECHAZADO)
  - No links (http/https/www) => RECHAZADO
  - Adjuntos obligatorios y reglas por tipo de factura
- ✅ Formato obligatorio en ASUNTO o CUERPO (case-insensitive):
  - CLIENTE: <nombre>
  - COBRO: CONTADO | CREDITO | ANTICIPO
  - FACTURA: NORMAL | ELECTRONICA
- ✅ Parser tolerante “pegado”:
  - "Cliente: Ecopetrol Cobro: Credito Factura: Normal" ✅
  - "CLIENTE: Ecopetrol | COBRO: CREDITO | FACTURA: NORMAL" ✅
- ✅ Reglas de adjuntos:
  - ELECTRONICA: (PDF + XML) O (2 PDFs). Solo PDF/XML permitidos
  - NORMAL: SOLO PDF (XML prohibido)
- ✅ Regla base PDFs:
  - NORMAL: mínimo REQUIRED_PDF_COUNT PDFs
  - ELECTRONICA: mínimo 1 PDF (porque puede venir 1 PDF + 1 XML)
- ✅ Robustez: ACK SIEMPRE, manejo 404, dedupe state, watch renew

✅ Drive:
- 📁 Guarda contenido del correo en Google Drive en la ruta “del diagrama”:
  DriveRoot/
    Clientes/<CLIENTE>/<COBRO>/YYYY/MM/<RADICADO>/{email.eml,email.txt,metadata.json,adjuntos/*}   (APROBADO)
    Clientes/<CLIENTE>/Rechazado/YYYY/MM/<RADICADO>/{...}                                       (RECHAZADO con cliente)
    Cliente_no_identificado/YYYY/MM/<RADICADO>/{...}                                            (RECHAZADO sin cliente)

Requisitos:
- credentials.json (OAuth desktop)
- token.json (se genera)
- Pub/Sub pull REST requiere ADC:
    gcloud auth application-default login
- .env mínimo:
    GCP_PROJECT_ID=...
    PUBSUB_SUBSCRIPTION=...
    PUBSUB_TOPIC_FULL=projects/.../topics/...
    CLIENT_SHEET_ID=...
- .env Drive (opcional):
    DRIVE_ROOT_FOLDER_ID=...        # si no lo pones, crea/usa carpeta por DRIVE_ROOT_FOLDER_NAME
    DRIVE_ROOT_FOLDER_NAME=Facturacion2026
    DRIVE_USE_SHARED_DRIVE=false
    DRIVE_SHARED_DRIVE_ID=...

⚠️ IMPORTANTE:
- Para Drive estable (sin líos de permisos con carpetas existentes),
  se usa scope https://www.googleapis.com/auth/drive (NO drive.file).
  Si cambiaste scopes, borra token.json y vuelve a autenticar.
"""

import base64
import json
import os
import os.path
import re
import time
from typing import Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
load_dotenv()

from datetime import datetime, timezone, timedelta
from email.mime.text import MIMEText
from email.utils import parseaddr

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaInMemoryUpload

from google.cloud import pubsub_v1


# ============================================================
# SCOPES
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",  # ✅ drive full
]

# ============================================================
# CONFIG (ENV)
# ============================================================
SHEET_ID = os.environ.get("CLIENT_SHEET_ID", "").strip() or "14x7UflRW7P9qIHy65biueQUQjn03WBhV7T6l454VUmQ"
SHEET_RANGE = os.environ.get("CLIENT_SHEET_RANGE", "Clientes!A:B").strip()

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip()
PUBSUB_SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION", "").strip()
PUBSUB_TOPIC_FULL = os.environ.get("PUBSUB_TOPIC_FULL", "").strip()

WATCH_LABEL_IDS = [x.strip() for x in os.environ.get("GMAIL_LABEL_IDS", "INBOX").split(",") if x.strip()]
STATE_FILE = os.environ.get("GMAIL_WATCH_STATE_FILE", "gmail_watch_state.json")

PUBSUB_PULL_MAX = int(os.environ.get("PUBSUB_PULL_MAX", "10"))
IDLE_SLEEP_SEC = float(os.environ.get("IDLE_SLEEP_SEC", "1.0"))
WATCH_RENEW_WINDOW_MS = int(os.environ.get("WATCH_RENEW_WINDOW_MS", str(60 * 60 * 1000)))  # 1h

REQUIRED_PDF_COUNT = int(os.environ.get("REQUIRED_PDF_COUNT", "5"))

ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS = os.environ.get("ONLY_WITH_ATTACHMENTS", "true").lower() in (
    "1", "true", "yes", "y", "si"
)

KEYWORDS_FILTER = [
    k.strip().lower()
    for k in os.environ.get("KEYWORDS_FILTER", "").split(",")
    if k.strip()
]

PROCESSED_CACHE_LIMIT = int(os.environ.get("PROCESSED_CACHE_LIMIT", "2000"))

# ✅ RADICADO
RADICADO_PREFIX = os.environ.get("RADICADO_PREFIX", "RAD").strip()
RADICADO_RESET_DAILY = os.environ.get("RADICADO_RESET_DAILY", "true").lower() in ("1", "true", "yes", "y", "si")
RADICADO_PAD = int(os.environ.get("RADICADO_PAD", "6"))
RADICADO_MAP_LIMIT = int(os.environ.get("RADICADO_MAP_LIMIT", "5000"))

# ✅ Reglas comunicado 2026
TZ_BOGOTA = timezone(timedelta(hours=-5))
RECEPTION_START_HOUR = 9
RECEPTION_END_HOUR = 17

CLOSING_2026 = {
    1: 28, 2: 25, 3: 27, 4: 28, 5: 27, 6: 24,
    7: 29, 8: 27, 9: 28, 10: 28, 11: 26, 12: 14
}

# ✅ Drive
DRIVE_ROOT_FOLDER_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID", "1ICV3tQy_yea0LxNdlCLOZaTAnJoVbMJk").strip()
DRIVE_ROOT_FOLDER_NAME = os.environ.get("DRIVE_ROOT_FOLDER_NAME", "Facturacion2026").strip()
DRIVE_USE_SHARED_DRIVE = os.environ.get("DRIVE_USE_SHARED_DRIVE", "false").lower() in ("1", "true", "yes", "y", "si")
DRIVE_SHARED_DRIVE_ID = os.environ.get("DRIVE_SHARED_DRIVE_ID", "").strip()

DRIVE_DEDUPE_ATTACHMENTS = os.environ.get("DRIVE_DEDUPE_ATTACHMENTS", "true").lower() in ("1", "true", "yes", "y", "si")


# ============================================================
# STATE
# ============================================================
def load_state() -> Dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def save_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def _as_set(arr) -> Set[str]:
    if not isinstance(arr, list):
        return set()
    return set(str(x) for x in arr)

def state_get_processed_set(state: Dict) -> Set[str]:
    return _as_set(state.get("processed_message_ids") or [])

def state_add_processed(state: Dict, message_id: str) -> None:
    s = state_get_processed_set(state)
    s.add(str(message_id))
    if len(s) > PROCESSED_CACHE_LIMIT:
        s = set(list(s)[-PROCESSED_CACHE_LIMIT:])
    state["processed_message_ids"] = list(s)

def state_has_replied(state: Dict, message_id: str) -> bool:
    return str(message_id) in _as_set(state.get("replied_message_ids") or [])

def state_mark_replied(state: Dict, message_id: str) -> None:
    s = _as_set(state.get("replied_message_ids") or [])
    s.add(str(message_id))
    state["replied_message_ids"] = list(s)


# ============================================================
# RADICADO
# ============================================================
def _today_yyyymmdd() -> str:
    return time.strftime("%Y%m%d")

def _get_radicado_map(state: Dict) -> Dict[str, str]:
    m = state.get("message_radicados") or {}
    return m if isinstance(m, dict) else {}

def _set_radicado_map(state: Dict, m: Dict[str, str]) -> None:
    state["message_radicados"] = m

def get_or_create_radicado(message_id: str, state: Dict) -> str:
    mid = str(message_id)
    m = _get_radicado_map(state)

    if mid in m:
        return m[mid]

    today = _today_yyyymmdd()
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

    m[mid] = radicado

    if len(m) > RADICADO_MAP_LIMIT:
        keys = list(m.keys())[-RADICADO_MAP_LIMIT:]
        m = {k: m[k] for k in keys}

    _set_radicado_map(state, m)
    return radicado


# ============================================================
# TEXT UTIL
# ============================================================
def _normalize_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())

def _decode_body(data: Optional[str]) -> str:
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

def extract_plain_text(payload: Dict) -> str:
    if not payload:
        return ""
    body = payload.get("body", {}) or {}
    data = body.get("data")
    if data:
        return _decode_body(data)

    for part in payload.get("parts", []) or []:
        mime_type = part.get("mimeType", "")
        if mime_type == "text/plain":
            return _decode_body((part.get("body", {}) or {}).get("data"))
        nested = extract_plain_text(part)
        if nested:
            return nested
    return ""

def get_header(payload: Dict, name: str) -> str:
    target = name.lower()
    for header in payload.get("headers", []) or []:
        if (header.get("name", "") or "").lower() == target:
            return header.get("value", "") or ""
    return ""

def passes_keyword_filter(searchable_text: str) -> bool:
    if not KEYWORDS_FILTER:
        return True
    low = (searchable_text or "").lower()
    return any(k in low for k in KEYWORDS_FILTER)

def contains_forbidden_links(text: str) -> bool:
    if not text:
        return False
    return bool(re.search(r"(https?://|www\.)", text, re.IGNORECASE))


# ============================================================
# TIME RULES
# ============================================================
def gmail_internaldate_to_dt_bogota(msg: Dict) -> Optional[datetime]:
    try:
        ms = int(msg.get("internalDate", 0))
        if not ms:
            return None
        dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt_utc.astimezone(TZ_BOGOTA)
    except Exception:
        return None

def is_within_receiving_window(dt: datetime) -> bool:
    if not dt:
        return True
    weekday = dt.weekday()  # 0=Mon..6=Sun
    if weekday > 4:
        return False
    hour, minute = dt.hour, dt.minute
    if hour < RECEPTION_START_HOUR:
        return False
    if hour > RECEPTION_END_HOUR:
        return False
    if hour == RECEPTION_END_HOUR and minute > 0:
        return False
    return True

def is_after_monthly_closing_2026(dt: datetime) -> bool:
    if not dt:
        return False
    if dt.year != 2026:
        return False
    close_day = CLOSING_2026.get(dt.month)
    if not close_day:
        return False
    closing_date = datetime(dt.year, dt.month, close_day, 23, 59, 59, tzinfo=dt.tzinfo)
    return dt > closing_date


# ============================================================
# ATTACHMENTS
# ============================================================
def _collect_attachments(payload: Dict) -> List[Dict[str, Optional[str]]]:
    attachments: List[Dict[str, Optional[str]]] = []
    if not payload:
        return attachments

    for part in payload.get("parts", []) or []:
        filename = (part.get("filename") or "").strip()
        mime_type = (part.get("mimeType") or "").strip()
        body = part.get("body", {}) or {}
        attachment_id = body.get("attachmentId")

        if filename:
            attachments.append({"filename": filename, "mimeType": mime_type, "attachmentId": attachment_id})

        attachments.extend(_collect_attachments(part))

    return attachments

def _is_pdf(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return mt == "application/pdf" or fn.endswith(".pdf")

def _is_xml(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".xml") or mt in ("application/xml", "text/xml")

def has_any_attachment(payload: Dict) -> bool:
    return len(_collect_attachments(payload)) > 0

def validate_required_pdfs(payload: Dict, required_count: int) -> Dict[str, object]:
    atts = _collect_attachments(payload)
    pdfs = [a for a in atts if _is_pdf(a)]
    pdf_names = [a.get("filename") or "(sin nombre)" for a in pdfs]
    pdf_count = len(pdfs)
    missing = max(0, required_count - pdf_count)

    return {
        "ok": pdf_count >= required_count,
        "pdf_count": pdf_count,
        "missing": missing,
        "pdf_filenames": pdf_names,
        "all_attachments": atts,
    }


# ============================================================
# CAMPOS OBLIGATORIOS EN ASUNTO/CUERPO
# ============================================================
def parse_radicacion_fields(subject: str, body_text: str) -> Dict[str, Optional[str]]:
    """
    Lee CLIENTE / COBRO / FACTURA en asunto o cuerpo, aunque venga “pegado”.
    Extrae el valor de cada campo hasta que aparezca la siguiente etiqueta.
    """
    haystack = f"{subject or ''}\n{body_text or ''}"
    hs = re.sub(r"\s+", " ", haystack).strip()

    def pick_label(label: str) -> Optional[str]:
        pattern = rf"{label}\s*:\s*(.+?)(?=\s+(CLIENTE|COBRO|FACTURA)\s*:|$)"
        m = re.search(pattern, hs, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    cliente = pick_label("CLIENTE")
    cobro   = pick_label("COBRO")
    factura = pick_label("FACTURA")

    if cliente:
        cliente = cliente.strip(" |;-")

    if cobro:
        cobro = cobro.upper().replace("CRÉDITO", "CREDITO").strip(" |;-")
        cobro = re.sub(r"[^A-Z]", "", cobro)
        if cobro not in {"CONTADO", "CREDITO", "ANTICIPO"}:
            cobro = None

    if factura:
        factura = factura.upper().replace("ELECTRÓNICA", "ELECTRONICA").strip(" |;-")
        factura = re.sub(r"[^A-Z]", "", factura)
        if factura not in {"NORMAL", "ELECTRONICA"}:
            factura = None

    return {"cliente": cliente, "cobro": cobro, "factura": factura}

def validate_required_radicacion_fields(fields: Dict[str, Optional[str]]) -> List[str]:
    missing = []
    if not fields.get("cliente"):
        missing.append("CLIENTE")
    if not fields.get("cobro"):
        missing.append("COBRO (CONTADO|CREDITO|ANTICIPO)")
    if not fields.get("factura"):
        missing.append("FACTURA (NORMAL|ELECTRONICA)")
    return missing

def validate_invoice_type_attachments(factura_type: str, attachments: List[Dict[str, Optional[str]]]) -> List[str]:
    """
    Reglas duras (según tu ajuste):
    - ELECTRONICA: OK si (>=1 PDF y >=1 XML) OR (>=2 PDFs). Solo PDF/XML permitidos.
    - NORMAL: solo PDF (si trae XML u otro tipo => error).
    """
    errors: List[str] = []
    ft = (factura_type or "").upper().strip()

    pdf_count = sum(1 for a in attachments if _is_pdf(a))
    xml_count = sum(1 for a in attachments if _is_xml(a))

    if ft == "ELECTRONICA":
        # Solo permitidos PDF/XML
        for a in attachments:
            if not (_is_pdf(a) or _is_xml(a)):
                errors.append(f"Adjunto no permitido para FACTURA ELECTRÓNICA: {a.get('filename')}")

        ok = (pdf_count >= 1 and xml_count >= 1) or (pdf_count >= 2)
        if not ok:
            errors.append("FACTURA ELECTRÓNICA debe traer (PDF + XML) o (2 PDFs).")

    else:  # NORMAL
        for a in attachments:
            if not _is_pdf(a):
                errors.append(f"Adjunto no permitido (FACTURA NORMAL solo PDF): {a.get('filename')}")
        if xml_count > 0:
            errors.append("FACTURA NORMAL no debe incluir XML (solo PDF).")

    return errors


# ============================================================
# SHEETS: CLIENT CATALOG
# ============================================================
def load_client_catalog(sheets_service) -> List[Dict[str, Optional[str]]]:
    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Catálogo deshabilitado.")
        return []

    try:
        result = sheets_service.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=SHEET_RANGE
        ).execute()
    except HttpError as error:
        raise RuntimeError(f"No pude leer Sheets. Error: {error}") from error

    values = result.get("values", []) or []
    catalog: List[Dict[str, Optional[str]]] = []

    for row in values:
        if not row:
            continue
        name = (row[0] or "").strip()
        if not name or name.lower() == "cliente":
            continue
        status = (row[1] if len(row) > 1 else "").strip().lower()
        if status not in {"activo", "active"}:
            continue
        catalog.append({"name": name, "normalized": _normalize_text(name)})

    return catalog

def find_client_exact_or_normalized(cliente_field: str, catalog: List[Dict[str, Optional[str]]]) -> Optional[Dict[str, Optional[str]]]:
    if not cliente_field:
        return None
    n = _normalize_text(cliente_field)
    for c in catalog:
        if c.get("normalized") == n:
            return c
    for c in catalog:
        cn = c.get("normalized") or ""
        if cn and (cn in n or n in cn):
            return c
    return None


# ============================================================
# OAUTH
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
# WATCH
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

    last_h = state.get("last_history_id") or resp.get("historyId")

    new_state = dict(state)
    new_state.update({
        "watch_started_at_ms": now_ms,
        "watch_expiration_ms": int(resp.get("expiration", 0)),
        "last_history_id": str(last_h) if last_h else None,
    })

    save_state(new_state)

    print(
        f"✅ Watch activo. Expira(ms): {new_state.get('watch_expiration_ms')} | "
        f"last_history_id: {new_state.get('last_history_id')}"
    )
    return new_state


# ============================================================
# HISTORY
# ============================================================
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

        for h in resp.get("history", []) or []:
            for added in h.get("messagesAdded", []) or []:
                mid = (added.get("message") or {}).get("id")
                if mid:
                    message_ids.add(mid)

        page_token = resp.get("nextPageToken")
        if resp.get("historyId"):
            latest_history_id = str(resp.get("historyId"))

        if page_token:
            continue
        break

    return message_ids, latest_history_id

def update_last_history_id(latest_history_id: Optional[str]) -> None:
    if not latest_history_id:
        return
    st = load_state()
    st["last_history_id"] = str(latest_history_id)
    save_state(st)


# ============================================================
# EMAIL REPLY (APROBADO / RECHAZADO)
# ============================================================
def _extract_sender_email(from_header: str) -> Optional[str]:
    _, email = parseaddr(from_header or "")
    return email or None

def _create_raw_email(to_email: str, subject: str, body: str) -> str:
    msg = MIMEText(body, _charset="utf-8")
    msg["To"] = to_email
    msg["Subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return raw

def send_reply_email(gmail_service, original_msg: Dict, to_email: str, subject: str, body: str) -> None:
    thread_id = original_msg.get("threadId")
    raw = _create_raw_email(to_email, subject, body)

    send_body = {"raw": raw}
    if thread_id:
        send_body["threadId"] = thread_id

    gmail_service.users().messages().send(userId="me", body=send_body).execute()

def build_rejected_email(radicado: str, fields: Dict[str, Optional[str]], reasons: List[str]) -> Tuple[str, str]:
    subject = f"RECHAZADO – No fue posible radicar tu facturación (ID: {radicado})"
    cliente = fields.get("cliente") or "NO IDENTIFICADO"
    cobro = fields.get("cobro") or "NO INFORMADO"
    factura = fields.get("factura") or "NO INFORMADO"

    body = (
        "Hola,\n\n"
        "Recibimos tu correo de facturación, pero NO fue posible radicarlo porque está incompleto o no cumple el formato.\n\n"
        f"ID interno (radicado): {radicado}\n"
        "Estado: RECHAZADO\n"
        f"Cliente: {cliente}\n"
        f"Tipo de cobro: {cobro} (CONTADO / CREDITO / ANTICIPO)\n"
        f"Tipo de factura: {factura} (NORMAL / ELECTRONICA)\n\n"
        "Motivos del rechazo:\n"
        + "".join([f"- {r}\n" for r in reasons]) +
        "\nQué debes corregir y reenviar (en un solo correo):\n"
        "1) En asunto o cuerpo indicar: CLIENTE + COBRO + FACTURA.\n"
        "2) Adjuntar soportes:\n"
        "   - NORMAL: PDFs\n"
        "   - ELECTRONICA: (PDF + XML) o (2 PDFs)\n"
        "3) Sin links: no http/https/www.\n\n"
        "Ejemplo válido:\n"
        "CLIENTE: ACME SAS | COBRO: CREDITO | FACTURA: ELECTRONICA\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body

def build_approved_email(radicado: str, fields: Dict[str, Optional[str]], pdf_count: int) -> Tuple[str, str]:
    subject = f"APROBADO – Facturación recibida y radicada correctamente (ID: {radicado})"
    cliente = fields.get("cliente") or "NO IDENTIFICADO"
    cobro = fields.get("cobro") or "NO INFORMADO"
    factura = fields.get("factura") or "NO INFORMADO"

    body = (
        "Hola,\n\n"
        "✅ Confirmamos que tu correo de facturación fue recibido y validado correctamente.\n\n"
        f"ID interno (radicado): {radicado}\n"
        "Estado: APROBADO / RECIBIDO OK\n"
        f"Cliente: {cliente}\n"
        f"Tipo de cobro: {cobro} (CONTADO / CREDITO / ANTICIPO)\n"
        f"Tipo de factura: {factura} (NORMAL / ELECTRONICA)\n"
        f"Adjuntos validados: {pdf_count} PDF(s)\n\n"
        "Tu solicitud queda en proceso según los tiempos internos de revisión y pago.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


# ============================================================
# DRIVE (carpetas + subida de contenido)
# ============================================================
def _drive_list_kwargs():
    if DRIVE_USE_SHARED_DRIVE:
        return {
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
            "corpora": "drive",
            "driveId": DRIVE_SHARED_DRIVE_ID,
        }
    return {}

def _drive_create_kwargs():
    if DRIVE_USE_SHARED_DRIVE:
        return {"supportsAllDrives": True}
    return {}

def drive_file_exists(drive_service, parent_id: str, filename: str) -> bool:
    safe_name = filename.replace("'", "\\'")
    q = (
        f"'{parent_id}' in parents "
        "and trashed=false "
        f"and name='{safe_name}'"
    )
    res = drive_service.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=1,
        **_drive_list_kwargs()
    ).execute()
    return bool(res.get("files"))

def drive_find_folder(drive_service, parent_id: str, name: str) -> Optional[str]:
    safe_name = name.replace("'", "\\'")
    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{safe_name}' "
        f"and '{parent_id}' in parents "
        "and trashed=false"
    )
    res = drive_service.files().list(
        q=q,
        fields="files(id,name)",
        pageSize=10,
        **_drive_list_kwargs()
    ).execute()
    files = res.get("files", [])
    return files[0]["id"] if files else None

def drive_get_or_create_folder(drive_service, parent_id: str, name: str) -> str:
    fid = drive_find_folder(drive_service, parent_id, name)
    if fid:
        return fid
    meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    folder = drive_service.files().create(body=meta, fields="id", **_drive_create_kwargs()).execute()
    return folder["id"]

def drive_create_root_if_needed(drive_service) -> Optional[str]:
    """
    Si DRIVE_ROOT_FOLDER_ID viene, lo valida. Si no hay acceso, crea/usa root por nombre.
    """
    if DRIVE_ROOT_FOLDER_ID:
        try:
            drive_service.files().get(
                fileId=DRIVE_ROOT_FOLDER_ID,
                fields="id,name",
                **_drive_create_kwargs()
            ).execute()
            return DRIVE_ROOT_FOLDER_ID
        except Exception as e:
            print(f"⚠️ Sin acceso a DRIVE_ROOT_FOLDER_ID={DRIVE_ROOT_FOLDER_ID}. Crearé uno nuevo. Error: {e}")

    safe_name = DRIVE_ROOT_FOLDER_NAME.replace("'", "\\'")
    q = (
        "mimeType='application/vnd.google-apps.folder' "
        f"and name='{safe_name}' "
        "and trashed=false"
    )
    res = drive_service.files().list(q=q, fields="files(id,name)", pageSize=10, **_drive_list_kwargs()).execute()
    files = res.get("files", [])
    if files:
        return files[0]["id"]

    meta = {"name": DRIVE_ROOT_FOLDER_NAME, "mimeType": "application/vnd.google-apps.folder"}
    folder = drive_service.files().create(body=meta, fields="id,webViewLink", **_drive_create_kwargs()).execute()
    print(f"📁 Drive root creado: {DRIVE_ROOT_FOLDER_NAME} (id={folder.get('id')})")
    return folder.get("id")

def drive_upload_bytes(drive_service, parent_id: str, filename: str, data: bytes, mime_type: str) -> str:
    media = MediaInMemoryUpload(data, mimetype=mime_type, resumable=False)
    meta = {"name": filename, "parents": [parent_id]}
    f = drive_service.files().create(
        body=meta,
        media_body=media,
        fields="id",
        **_drive_create_kwargs()
    ).execute()
    return f["id"]

def gmail_download_attachment_bytes(gmail_service, message_id: str, attachment_id: str) -> bytes:
    att = gmail_service.users().messages().attachments().get(
        userId="me", messageId=message_id, id=attachment_id
    ).execute()
    data = att.get("data", "") or ""
    return base64.urlsafe_b64decode(data.encode("utf-8"))

def drive_build_radicado_folder(
    drive_service,
    root_id: str,
    estado: str,  # "APROBADO"|"RECHAZADO"
    cliente: str,
    cobro: Optional[str],
    radicado: str,
    received_dt: Optional[datetime],
    cliente_identificado: bool,
) -> str:
    """
    Ruta “del diagrama”:
      root/Clientes/<CLIENTE>/<COBRO>/YYYY/MM/<RADICADO>                     (APROBADO)
      root/Clientes/<CLIENTE>/Rechazado/YYYY/MM/<RADICADO>                   (RECHAZADO con cliente)
      root/Cliente_no_identificado/YYYY/MM/<RADICADO>                        (RECHAZADO sin cliente)
    """
    if not received_dt:
        received_dt = datetime.now(TZ_BOGOTA)

    yyyy = f"{received_dt.year:04d}"
    mm = f"{received_dt.month:02d}"

    if estado.upper() == "APROBADO":
        base = drive_get_or_create_folder(drive_service, root_id, "Clientes")
        cliente_folder = (cliente or "NO_IDENTIFICADO").strip() or "NO_IDENTIFICADO"
        base = drive_get_or_create_folder(drive_service, base, cliente_folder)

        cobro_folder = (cobro or "NO_INFORMADO").upper()
        base = drive_get_or_create_folder(drive_service, base, cobro_folder)
    else:
        if cliente_identificado and (cliente or "").strip():
            base = drive_get_or_create_folder(drive_service, root_id, "Clientes")
            base = drive_get_or_create_folder(drive_service, base, cliente.strip())
            base = drive_get_or_create_folder(drive_service, base, "Rechazado")
        else:
            base = drive_get_or_create_folder(drive_service, root_id, "Cliente_no_identificado")

    base = drive_get_or_create_folder(drive_service, base, yyyy)
    base = drive_get_or_create_folder(drive_service, base, mm)
    base = drive_get_or_create_folder(drive_service, base, radicado)

    drive_get_or_create_folder(drive_service, base, "adjuntos")
    return base

def store_email_to_drive(
    gmail_service,
    drive_service,
    root_id: str,
    message_id: str,
    msg_full: Dict,
    radicado: str,
    estado: str,
    cliente: str,
    cobro: Optional[str],
    fields: Dict[str, Optional[str]],
    subject: str,
    body_text: str,
    received_dt: Optional[datetime],
    reasons: Optional[List[str]],
    attachments: List[Dict[str, Optional[str]]],
) -> None:
    """
    Sube:
      - email.eml (raw)
      - email.txt
      - metadata.json
      - adjuntos/*
    """
    cliente_identificado = bool((cliente or "").strip()) and (cliente.strip() not in ("NO_IDENTIFICADO", "NO IDENTIFICADO"))

    rad_folder_id = drive_build_radicado_folder(
        drive_service=drive_service,
        root_id=root_id,
        estado=estado,
        cliente=cliente,
        cobro=cobro,
        radicado=radicado,
        received_dt=received_dt,
        cliente_identificado=cliente_identificado,
    )

    # Idempotencia: si ya existe metadata.json, no duplicamos
    try:
        if drive_file_exists(drive_service, rad_folder_id, "metadata.json"):
            print(f"ℹ️ Drive: ya existe metadata.json para {radicado}, no duplico subida.")
            return
    except Exception as e:
        print(f"⚠️ Drive: no pude verificar metadata.json (continuo). Error: {e}")

    # 1) email.eml
    try:
        raw_msg = gmail_service.users().messages().get(userId="me", id=message_id, format="raw").execute()
        raw_b64 = raw_msg.get("raw", "") or ""
        eml_bytes = base64.urlsafe_b64decode(raw_b64.encode("utf-8"))
        drive_upload_bytes(drive_service, rad_folder_id, "email.eml", eml_bytes, "message/rfc822")
    except Exception as e:
        print(f"⚠️ No pude subir email.eml ({radicado}): {e}")

    # 2) email.txt
    try:
        drive_upload_bytes(
            drive_service, rad_folder_id, "email.txt",
            (body_text or "").encode("utf-8"), "text/plain"
        )
    except Exception as e:
        print(f"⚠️ No pude subir email.txt ({radicado}): {e}")

    # 3) metadata.json
    try:
        metadata = {
            "radicado": radicado,
            "estado": estado,
            "cliente": fields.get("cliente"),
            "cobro": fields.get("cobro"),
            "factura": fields.get("factura"),
            "message_id": message_id,
            "thread_id": msg_full.get("threadId"),
            "subject": subject,
            "from": get_header((msg_full.get("payload") or {}), "From"),
            "received_dt_bogota": received_dt.isoformat() if received_dt else None,
            "rejection_reasons": reasons or [],
            "attachments": [{"filename": a.get("filename"), "mimeType": a.get("mimeType")} for a in attachments],
        }
        drive_upload_bytes(
            drive_service,
            rad_folder_id,
            "metadata.json",
            json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8"),
            "application/json"
        )
    except Exception as e:
        print(f"⚠️ No pude subir metadata.json ({radicado}): {e}")

    # 4) adjuntos/*
    try:
        adj_folder_id = drive_find_folder(drive_service, rad_folder_id, "adjuntos")
        if not adj_folder_id:
            adj_folder_id = drive_get_or_create_folder(drive_service, rad_folder_id, "adjuntos")

        for a in attachments:
            filename = (a.get("filename") or "").strip()
            att_id = a.get("attachmentId")
            if not filename or not att_id:
                continue

            if DRIVE_DEDUPE_ATTACHMENTS:
                try:
                    if drive_file_exists(drive_service, adj_folder_id, filename):
                        continue
                except Exception:
                    pass

            try:
                content = gmail_download_attachment_bytes(gmail_service, message_id, att_id)
                mt = (a.get("mimeType") or "application/octet-stream").strip()
                drive_upload_bytes(drive_service, adj_folder_id, filename, content, mt)
            except Exception as e:
                print(f"⚠️ No pude subir adjunto a Drive ({filename}): {e}")

    except Exception as e:
        print(f"⚠️ Drive: fallo subiendo adjuntos ({radicado}): {e}")


# ============================================================
# MESSAGE PROCESSING
# ============================================================
def safe_get_message_full(gmail_service, message_id: str) -> Optional[Dict]:
    try:
        return gmail_service.users().messages().get(
            userId="me",
            id=message_id,
            format="full"
        ).execute()
    except HttpError as e:
        if getattr(e, "resp", None) is not None and e.resp.status == 404:
            print(f"⚠️ Gmail 404: messageId {message_id} ya no existe. SKIP.")
            return None
        raise

def process_message(gmail_service, drive_service, message_id: str, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    state = load_state()

    # dedupe
    if message_id in state_get_processed_set(state):
        return

    already_replied = state_has_replied(state, message_id)

    radicado = get_or_create_radicado(message_id, state)
    save_state(state)

    msg = safe_get_message_full(gmail_service, message_id)
    if not msg:
        state_add_processed(state, message_id)
        save_state(state)
        return

    payload = msg.get("payload", {}) or {}
    snippet = msg.get("snippet", "") or ""

    received_dt = gmail_internaldate_to_dt_bogota(msg)

    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    body_text = extract_plain_text(payload)

    to_email = _extract_sender_email(from_header)
    if not to_email:
        print(f"⚠️ No pude extraer email del remitente. From: {from_header}")
        state_add_processed(state, message_id)
        save_state(state)
        return

    searchable_text = f"{subject}\n{from_header}\n{body_text}\n{snippet}"
    attachments = _collect_attachments(payload)

    # Drive root
    root_id = None
    if drive_service:
        try:
            root_id = drive_create_root_if_needed(drive_service)
        except Exception as e:
            print(f"⚠️ Drive deshabilitado por error creando/validando root: {e}")
            root_id = None

    # ruido sin adjuntos
    if ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS and not has_any_attachment(payload):
        state_add_processed(state, message_id)
        save_state(state)
        return

    # links prohibidos
    if contains_forbidden_links(searchable_text):
        reasons = ["El correo contiene enlaces (http/https/www). Deben adjuntar los archivos (sin links)."]

        if not already_replied:
            subj, body = build_rejected_email(radicado, {}, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente="NO_IDENTIFICADO", cobro=None, fields={}, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # horario
    if received_dt and not is_within_receiving_window(received_dt):
        reasons = [f"Fuera de horario de recepción (L–V 9:00 a.m. a 5:00 p.m.). Llegó: {received_dt.isoformat()}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, {}, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente="NO_IDENTIFICADO", cobro=None, fields={}, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # cierre mensual 2026
    if received_dt and is_after_monthly_closing_2026(received_dt):
        reasons = [f"Llegó después de la fecha de cierre del mes (calendario 2026). Llegó: {received_dt.date().isoformat()}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, {}, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente="NO_IDENTIFICADO", cobro=None, fields={}, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # keywords opcional
    if not passes_keyword_filter(searchable_text):
        state_add_processed(state, message_id)
        save_state(state)
        return

    # campos obligatorios
    fields = parse_radicacion_fields(subject, body_text)
    missing_fields = validate_required_radicacion_fields(fields)
    if missing_fields:
        reasons = ["Falta información obligatoria en ASUNTO o CUERPO.", f"Faltantes: {', '.join(missing_fields)}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente=fields.get("cliente") or "NO_IDENTIFICADO", cobro=fields.get("cobro"),
                fields=fields, subject=subject, body_text=body_text, received_dt=received_dt,
                reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # validar cliente con catálogo
    client_obj = find_client_exact_or_normalized(fields["cliente"], client_catalog) if client_catalog else None
    if client_catalog and not client_obj:
        reasons = ["CLIENTE no existe en el catálogo o no está activo.", f"CLIENTE declarado: {fields.get('cliente')}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente=fields.get("cliente") or "NO_IDENTIFICADO", cobro=fields.get("cobro"),
                fields=fields, subject=subject, body_text=body_text, received_dt=received_dt,
                reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # validar adjuntos por tipo factura
    invoice_attach_errors = validate_invoice_type_attachments(fields["factura"], attachments)
    if invoice_attach_errors:
        reasons = ["Adjuntos no cumplen el tipo de FACTURA declarado."] + invoice_attach_errors

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente=client_obj["name"] if client_obj else (fields.get("cliente") or "NO_IDENTIFICADO"),
                cobro=fields.get("cobro"), fields=fields, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # ✅ validar PDFs mínimos (dinámico por tipo)
    required = REQUIRED_PDF_COUNT
    if (fields.get("factura") or "").upper() == "ELECTRONICA":
        required = 1  # porque puede ser 1 PDF + 1 XML (o 2 PDFs)

    pdf_validation = validate_required_pdfs(payload, required_count=required)
    if not pdf_validation["ok"]:
        reasons = [
            f"PDF incompletos. Llegaron {pdf_validation['pdf_count']} / {required} (faltan {pdf_validation['missing']}).",
            f"PDFs detectados: {', '.join(pdf_validation['pdf_filenames']) if pdf_validation['pdf_filenames'] else '(ninguno)'}"
        ]

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="RECHAZADO",
                cliente=client_obj["name"] if client_obj else (fields.get("cliente") or "NO_IDENTIFICADO"),
                cobro=fields.get("cobro"), fields=fields, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # ✅ APROBADO
    if not already_replied:
        subj, body = build_approved_email(radicado, fields, pdf_validation["pdf_count"])
        send_reply_email(gmail_service, msg, to_email, subj, body)
        state_mark_replied(state, message_id)
        save_state(state)

    if root_id:
        cliente_name = client_obj["name"] if client_obj else (fields.get("cliente") or "NO_IDENTIFICADO")
        store_email_to_drive(
            gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
            message_id=message_id, msg_full=msg, radicado=radicado, estado="APROBADO",
            cliente=cliente_name, cobro=fields.get("cobro"), fields=fields, subject=subject,
            body_text=body_text, received_dt=received_dt, reasons=None, attachments=attachments
        )

    print("\n" + "=" * 70)
    print(f"🆕 Procesado → Message ID: {message_id}")
    print(f"🧾 Radicado: {radicado}")
    if received_dt:
        print(f"🕒 Recibido (Bogotá): {received_dt.isoformat()}")
    print(f"From: {from_header or '(sin From)'}")
    print(f"Subject: {subject or snippet or '(sin subject)'}")
    print(f"CLIENTE: {client_obj['name'] if client_obj else fields['cliente']}")
    print(f"COBRO: {fields['cobro']}")
    print(f"FACTURA: {fields['factura']}")
    print("✅ Estado: APROBADO")
    print(f"Adjuntos OK: {pdf_validation['pdf_count']} PDFs detectados.")
    print("=" * 70)

    state_add_processed(state, message_id)
    save_state(state)


# ============================================================
# PUBSUB LISTENER (PULL/REST) - ACK SIEMPRE
# ============================================================
def listen_pubsub(gmail_service, drive_service, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    subscriber = pubsub_v1.SubscriberClient(transport="rest")
    subscription_path = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    print(f"👂 Escuchando Pub/Sub (PULL/REST): {subscription_path}")

    backoff = 1

    while True:
        try:
            ensure_gmail_watch(gmail_service)

            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": PUBSUB_PULL_MAX}
            )

            if not response.received_messages:
                time.sleep(IDLE_SLEEP_SEC)
                backoff = 1
                continue

            for rm in response.received_messages:
                ack_id = rm.ack_id

                try:
                    raw = rm.message.data.decode("utf-8")
                    payload = json.loads(raw)

                    history_id = str(payload.get("historyId", "")).strip()
                    email_addr = payload.get("emailAddress", "")

                    if not history_id:
                        print("⚠️ Evento sin historyId (lo ignoro).")
                        continue

                    state = load_state()
                    last_history = str(state.get("last_history_id") or "").strip()

                    if not last_history:
                        update_last_history_id(history_id)
                        print(f"🔧 Inicialicé last_history_id={history_id} (primer evento).")
                        continue

                    new_ids, latest_history = fetch_new_message_ids(gmail_service, last_history)

                    if latest_history:
                        update_last_history_id(latest_history)

                    if not new_ids:
                        print(f"🔔 Evento ({email_addr}) historyId={history_id} → sin messageAdded nuevos (normal).")
                    else:
                        print(f"🔔 Evento ({email_addr}) historyId={history_id} → {len(new_ids)} mensaje(s) nuevo(s)")
                        for mid in new_ids:
                            process_message(gmail_service, drive_service, mid, client_catalog)

                except Exception as e:
                    print(f"❌ Error procesando evento Pub/Sub: {e}")

                finally:
                    try:
                        subscriber.acknowledge(
                            request={"subscription": subscription_path, "ack_ids": [ack_id]}
                        )
                    except Exception as e:
                        print(f"⚠️ No pude ACK (Pub/Sub reintentará): {e}")

            backoff = 1

        except KeyboardInterrupt:
            print("🛑 Listener detenido.")
            break
        except Exception as e:
            print(f"❌ Error en loop PULL/REST: {e}")
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


# ============================================================
# MAIN
# ============================================================
def main():
    if not GCP_PROJECT_ID or not PUBSUB_SUBSCRIPTION_ID or not PUBSUB_TOPIC_FULL:
        raise RuntimeError("Faltan env vars: GCP_PROJECT_ID, PUBSUB_SUBSCRIPTION, PUBSUB_TOPIC_FULL.")

    creds = get_oauth_creds()

    gmail_service = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

    profile = gmail_service.users().getProfile(userId="me").execute()
    print("✅ Autenticado como:", profile.get("emailAddress"))

    client_catalog = load_client_catalog(sheets_service)
    print(f"✅ Catálogo cargado: {len(client_catalog)} clientes activos")

    ensure_gmail_watch(gmail_service)
    listen_pubsub(gmail_service, drive_service, client_catalog)


if __name__ == "__main__":
    main()
