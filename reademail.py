# -*- coding: utf-8 -*-
"""
Gmail push listener: Gmail Watch -> Pub/Sub (PULL/REST) -> History -> Messages

Incluye:
- ✅ Radicado secuencial estable por correo (idempotente por Gmail messageId)
- ✅ Validaciones del comunicado 2026:
  - Recepción: L–V 9:00 a.m. a 5:00 p.m.
  - Cierre mensual 2026 (si llega después => RECHAZADO)
  - No links (http/https/www) => RECHAZADO
  - Adjuntos obligatorios (y validación por tipo de factura)
- ✅ Formato obligatorio en ASUNTO o CUERPO:
  - CLIENTE: <nombre>
  - COBRO: CONTADO | CREDITO | ANTICIPO
  - FACTURA: NORMAL | ELECTRONICA
- ✅ Reglas de adjuntos según FACTURA:
  - ELECTRONICA: PDF + XML (obligatorios), y NO otros tipos
  - NORMAL: SOLO PDF (XML prohibido)
- ✅ Reglas base:
  - Mínimo REQUIRED_PDF_COUNT PDFs
  - (opcional) solo procesar si hay adjuntos
  - (opcional) filtro keywords
- ✅ Robustez:
  - ACK SIEMPRE por cada evento Pub/Sub
  - Manejo 404 en messages.get => SKIP
  - Dedupe por state file
  - Watch auto-renew

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
Opcionales:
    CLIENT_SHEET_RANGE=Clientes!A:B
    GMAIL_LABEL_IDS=INBOX
    REQUIRED_PDF_COUNT=5
    ONLY_WITH_ATTACHMENTS=true
    KEYWORDS_FILTER=factura,cuenta de cobro,soportes
    RADICADO_PREFIX=RAD
    RADICADO_RESET_DAILY=true
    RADICADO_PAD=6
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

from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from google.cloud import pubsub_v1

from datetime import datetime, timezone, timedelta


# ============================================================
# SCOPES
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]

# ============================================================
# CONFIG (ENV)
# ============================================================
SHEET_ID = os.environ.get("CLIENT_SHEET_ID", "14x7UflRW7P9qIHy65biueQUQjn03WBhV7T6l454VUmQ").strip()
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

def state_get_processed_set(state: Dict) -> Set[str]:
    arr = state.get("processed_message_ids") or []
    if not isinstance(arr, list):
        return set()
    return set(str(x) for x in arr)

def state_add_processed(state: Dict, message_id: str) -> None:
    s = state_get_processed_set(state)
    s.add(str(message_id))
    if len(s) > PROCESSED_CACHE_LIMIT:
        s = set(list(s)[-PROCESSED_CACHE_LIMIT:])
    state["processed_message_ids"] = list(s)


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
# ✅ NUEVO: CAMPOS OBLIGATORIOS EN ASUNTO/CUERPO
# ============================================================
def parse_radicacion_fields(subject: str, body_text: str) -> Dict[str, Optional[str]]:
    """
    Busca en asunto o cuerpo:
      CLIENTE: <texto>
      COBRO: CONTADO | CREDITO | CRÉDITO | ANTICIPO
      FACTURA: NORMAL | ELECTRONICA | ELECTRÓNICA
    Captura hasta salto de línea o |.
    """
    haystack = f"{subject or ''}\n{body_text or ''}"

    def pick(pattern: str) -> Optional[str]:
        m = re.search(pattern, haystack, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    cliente = pick(r"CLIENTE\s*:\s*([^\n\|]+)")
    cobro = pick(r"COBRO\s*:\s*(CONTADO|CREDITO|CRÉDITO|ANTICIPO)")
    factura = pick(r"FACTURA\s*:\s*(NORMAL|ELECTRONICA|ELECTRÓNICA)")

    if cobro:
        cobro = cobro.upper().replace("CRÉDITO", "CREDITO")
    if factura:
        factura = factura.upper().replace("ELECTRÓNICA", "ELECTRONICA")

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
    Reglas duras:
    - ELECTRONICA: debe tener >=1 PDF y >=1 XML. Solo PDF/XML permitidos.
    - NORMAL: solo PDF (si trae XML => error).
    """
    errors = []
    has_pdf = any(_is_pdf(a) for a in attachments)
    has_xml = any(_is_xml(a) for a in attachments)

    if factura_type == "ELECTRONICA":
        # no permitir otros tipos
        for a in attachments:
            if not (_is_pdf(a) or _is_xml(a)):
                errors.append(f"Adjunto no permitido para FACTURA ELECTRÓNICA: {a.get('filename')}")
        if not has_pdf:
            errors.append("FACTURA ELECTRÓNICA requiere PDF (representación gráfica).")
        if not has_xml:
            errors.append("FACTURA ELECTRÓNICA requiere XML.")
    else:  # NORMAL
        for a in attachments:
            if not _is_pdf(a):
                errors.append(f"Adjunto no permitido (FACTURA NORMAL solo PDF): {a.get('filename')}")
        if has_xml:
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
    """
    Valida CLIENTE declarado contra catálogo activo.
    Matching permisivo por normalizado.
    """
    if not cliente_field:
        return None
    n = _normalize_text(cliente_field)
    for c in catalog:
        if c.get("normalized") == n:
            return c
    # Permite "contiene" (por si ponen ACME SAS y en catálogo ACME S.A.S)
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

def reject_print(radicado: str, subject: str, reason_lines: List[str], extra: Optional[List[str]] = None) -> None:
    print("\n" + "=" * 70)
    print(f"🧾 Radicado: {radicado}")
    print("🚫 Estado: RECHAZADO")
    for r in reason_lines:
        print(f"Motivo: {r}")
    if extra:
        for x in extra:
            print(x)
    print(f"Subject: {subject or '(sin subject)'}")
    print("=" * 70)

def process_message(gmail_service, message_id: str, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    state = load_state()
    processed = state_get_processed_set(state)
    if message_id in processed:
        return

    radicado = get_or_create_radicado(message_id, state)
    save_state(state)

    msg = safe_get_message_full(gmail_service, message_id)
    if not msg:
        state_add_processed(state, message_id)
        save_state(state)
        return

    payload = msg.get("payload", {}) or {}
    snippet = msg.get("snippet", "") or ""

    # Fecha/hora real de llegada
    received_dt = gmail_internaldate_to_dt_bogota(msg)

    # Headers/body
    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    body_text = extract_plain_text(payload)

    searchable_text = f"{subject}\n{from_header}\n{body_text}\n{snippet}"

    # Filtro adjuntos (ruido)
    if ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS and not has_any_attachment(payload):
        state_add_processed(state, message_id)
        save_state(state)
        return

    # No links
    if contains_forbidden_links(searchable_text):
        reject_print(
            radicado, subject,
            ["El correo contiene enlaces (http/https/www). Deben adjuntar los archivos (sin links)."]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Horario
    if received_dt and not is_within_receiving_window(received_dt):
        reject_print(
            radicado, subject,
            [f"Fuera de horario de recepción (L–V 9:00 a.m. a 5:00 p.m.). Llegó: {received_dt.isoformat()}"]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Cierre mensual 2026
    if received_dt and is_after_monthly_closing_2026(received_dt):
        reject_print(
            radicado, subject,
            [f"Llegó después de la fecha de cierre del mes (calendario 2026). Llegó: {received_dt.date().isoformat()}"]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Keywords opcional
    if not passes_keyword_filter(searchable_text):
        state_add_processed(state, message_id)
        save_state(state)
        return

    attachments = _collect_attachments(payload)

    # ✅ NUEVO: campos obligatorios del comunicado (asunto o cuerpo)
    fields = parse_radicacion_fields(subject, body_text)
    missing_fields = validate_required_radicacion_fields(fields)
    if missing_fields:
        reject_print(
            radicado, subject,
            ["Falta información obligatoria en ASUNTO o CUERPO."],
            [f"Faltantes: {', '.join(missing_fields)}",
             "Ejemplo: RADICACIÓN | CLIENTE: ACME SAS | COBRO: CREDITO | FACTURA: ELECTRONICA"]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Validar CLIENTE declarado contra catálogo activo
    client_obj = find_client_exact_or_normalized(fields["cliente"], client_catalog) if client_catalog else None
    if client_catalog and not client_obj:
        reject_print(
            radicado, subject,
            ["CLIENTE no existe en el catálogo o no está activo."],
            [f"CLIENTE declarado: {fields.get('cliente')}"]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Validación adjuntos por tipo de factura declarado
    invoice_attach_errors = validate_invoice_type_attachments(fields["factura"], attachments)
    if invoice_attach_errors:
        reject_print(
            radicado, subject,
            ["Adjuntos no cumplen el tipo de FACTURA declarado."],
            [" - " + e for e in invoice_attach_errors]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # Validación PDFs mínimos (tu regla base)
    pdf_validation = validate_required_pdfs(payload, required_count=REQUIRED_PDF_COUNT)
    if not pdf_validation["ok"]:
        reject_print(
            radicado, subject,
            [f"PDF incompletos. Llegaron {pdf_validation['pdf_count']} / {REQUIRED_PDF_COUNT} (faltan {pdf_validation['missing']})."],
            [f"PDFs: {pdf_validation['pdf_filenames']}"]
        )
        state_add_processed(state, message_id)
        save_state(state)
        return

    # ✅ ACEPTADO
    cobro = fields["cobro"].lower()  # contado|credito|anticipo
    factura = fields["factura"]      # NORMAL|ELECTRONICA

    print("\n" + "=" * 70)
    print(f"🆕 Procesado → Message ID: {message_id}")
    print(f"🧾 Radicado: {radicado}")
    if received_dt:
        print(f"🕒 Recibido (Bogotá): {received_dt.isoformat()}")
    print(f"From: {from_header or '(sin From)'}")
    print(f"Subject: {subject or snippet or '(sin subject)'}")
    print(f"CLIENTE: {client_obj['name'] if client_obj else fields['cliente']}")
    print(f"COBRO: {cobro}")
    print(f"FACTURA: {factura}")
    print("✅ Estado: ACEPTADO")
    print(f"Adjuntos OK: {pdf_validation['pdf_count']} PDFs detectados.")
    if factura == "ELECTRONICA":
        print("XML detectado:", any(_is_xml(a) for a in attachments))
    print("PDFs:", pdf_validation["pdf_filenames"])
    print("=" * 70)

    state_add_processed(state, message_id)
    save_state(state)


# ============================================================
# PUBSUB LISTENER (PULL/REST) - ACK SIEMPRE
# ============================================================
def listen_pubsub(gmail_service, client_catalog: List[Dict[str, Optional[str]]]) -> None:
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
                            process_message(gmail_service, mid, client_catalog)

                except Exception as e:
                    print(f"❌ Error procesando evento Pub/Sub: {e}")

                finally:
                    # ✅ ACK SIEMPRE (evita reentregas infinitas)
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
    creds = get_oauth_creds()

    gmail_service = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    profile = gmail_service.users().getProfile(userId="me").execute()
    print("✅ Autenticado como:", profile.get("emailAddress"))

    client_catalog = load_client_catalog(sheets_service)
    print(f"✅ Catálogo cargado: {len(client_catalog)} clientes activos")

    ensure_gmail_watch(gmail_service)
    listen_pubsub(gmail_service, client_catalog)


if __name__ == "__main__":
    main()
