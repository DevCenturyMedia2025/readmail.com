# -*- coding: utf-8 -*-
"""
Listener Gmail (push) + Pub/Sub (pull REST) + procesamiento de correos
+ Validación: mínimo N PDFs
+ Filtro: solo correos con adjuntos (evitar ruido)
+ Catálogo de clientes desde Google Sheets
+ Watch auto-renew
+ Robustez:
  - ACK SIEMPRE por cada evento Pub/Sub (evita redelivery infinito)
  - Manejo de 404 en messages.get (mensaje ya no existe) => SKIP
  - Dedupe de mensajes ya procesados (state)
+ ✅ Radicado secuencial estable por correo (idempotente por messageId)

Requisitos:
- credentials.json (OAuth desktop)
- token.json (se genera)
- Pub/Sub pull REST requiere ADC:
    gcloud auth application-default login
- .env con:
    GCP_PROJECT_ID=...
    PUBSUB_SUBSCRIPTION=...
    PUBSUB_TOPIC_FULL=projects/.../topics/...
    CLIENT_SHEET_ID=...
  opcionales:
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

# -------------------------
# Carga variables desde .env
# -------------------------
load_dotenv()

# -------------------------
# Google Auth / APIs
# -------------------------
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# -------------------------
# Pub/Sub client (REST)
# -------------------------
from google.cloud import pubsub_v1


# ============================================================
# SCOPES (PERMISOS) - OAuth
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


# ============================================================
# CONFIG GMAIL / SHEETS
# ============================================================
SHEET_ID = os.environ.get("CLIENT_SHEET_ID", "14x7UflRW7P9qIHy65biueQUQjn03WBhV7T6l454VUmQ").strip()
SHEET_RANGE = os.environ.get("CLIENT_SHEET_RANGE", "Clientes!A:B").strip()

# ============================================================
# CONFIG PUBSUB
# ============================================================
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "").strip()
PUBSUB_SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION", "").strip()
PUBSUB_TOPIC_FULL = os.environ.get("PUBSUB_TOPIC_FULL", "").strip()

# Labels que Gmail observa para mandar eventos (por defecto: INBOX)
WATCH_LABEL_IDS = [x.strip() for x in os.environ.get("GMAIL_LABEL_IDS", "INBOX").split(",") if x.strip()]

# Archivo para persistir estado
STATE_FILE = os.environ.get("GMAIL_WATCH_STATE_FILE", "gmail_watch_state.json")

# Pull tuning
PUBSUB_PULL_MAX = int(os.environ.get("PUBSUB_PULL_MAX", "10"))
IDLE_SLEEP_SEC = float(os.environ.get("IDLE_SLEEP_SEC", "1.0"))
WATCH_RENEW_WINDOW_MS = int(os.environ.get("WATCH_RENEW_WINDOW_MS", str(60 * 60 * 1000)))  # 1h

# ============================================================
# REQUERIMIENTOS DEL NEGOCIO
# ============================================================
REQUIRED_PDF_COUNT = int(os.environ.get("REQUIRED_PDF_COUNT", "5"))

ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS = os.environ.get("ONLY_WITH_ATTACHMENTS", "true").lower() in (
    "1", "true", "yes", "y", "si"
)

KEYWORDS_FILTER = [
    k.strip().lower()
    for k in os.environ.get("KEYWORDS_FILTER", "").split(",")
    if k.strip()
]

# ============================================================
# DEDUPE / CACHE
# ============================================================
PROCESSED_CACHE_LIMIT = int(os.environ.get("PROCESSED_CACHE_LIMIT", "2000"))

# ============================================================
# ✅ RADICADO SECUENCIAL
# ============================================================
RADICADO_PREFIX = os.environ.get("RADICADO_PREFIX", "RAD").strip()
RADICADO_RESET_DAILY = os.environ.get("RADICADO_RESET_DAILY", "true").lower() in ("1", "true", "yes", "y", "si")
RADICADO_PAD = int(os.environ.get("RADICADO_PAD", "6"))
RADICADO_MAP_LIMIT = int(os.environ.get("RADICADO_MAP_LIMIT", "5000"))


# ============================================================
# STATE HELPERS
# ============================================================
def load_state() -> Dict:
    """Lee STATE_FILE y retorna dict (o vacío si no existe/está corrupto)."""
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state: Dict) -> None:
    """Guarda STATE_FILE."""
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
# ✅ RADICADO HELPERS
# ============================================================
def _today_yyyymmdd() -> str:
    return time.strftime("%Y%m%d")


def _get_radicado_map(state: Dict) -> Dict[str, str]:
    m = state.get("message_radicados") or {}
    return m if isinstance(m, dict) else {}


def _set_radicado_map(state: Dict, m: Dict[str, str]) -> None:
    state["message_radicados"] = m


def get_or_create_radicado(message_id: str, state: Dict) -> str:
    """
    Crea un radicado secuencial y lo asocia al message_id.
    Si el message_id ya tiene radicado, devuelve el mismo (idempotente).
    Formato:
      - daily reset: RAD-YYYYMMDD-000001
      - no reset:    RAD-000001
    """
    mid = str(message_id)
    m = _get_radicado_map(state)

    # Si ya existe => estable
    if mid in m:
        return m[mid]

    today = _today_yyyymmdd()
    last_date = str(state.get("radicado_date") or "")
    counter = int(state.get("radicado_counter") or 0)

    # Reset diario opcional
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

    # recorte para que no crezca infinito
    if len(m) > RADICADO_MAP_LIMIT:
        keys = list(m.keys())[-RADICADO_MAP_LIMIT:]
        m = {k: m[k] for k in keys}

    _set_radicado_map(state, m)
    return radicado


# ============================================================
# UTILIDADES DE TEXTO
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


# ============================================================
# ADJUNTOS: extraer / validar
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


def has_any_attachment(payload: Dict) -> bool:
    atts = _collect_attachments(payload)
    return len(atts) > 0


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
# SHEETS: CATÁLOGO CLIENTES
# ============================================================
def load_client_catalog(sheets_service) -> List[Dict[str, Optional[str]]]:
    if not SHEET_ID:
        print("⚠️ CLIENT_SHEET_ID vacío. Catálogo de clientes deshabilitado.")
        return []

    try:
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
            .execute()
        )
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


def find_client(text: str, catalog: List[Dict[str, Optional[str]]]) -> Optional[Dict[str, Optional[str]]]:
    normalized_text = _normalize_text(text)
    for client in catalog:
        n = client.get("normalized")
        if n and n in normalized_text:
            return client
    return None


def determine_payment_type(text: str) -> Optional[str]:
    t = (text or "").lower()

    contado_patterns = [
        r"\bcontado\b",
        r"\bcontra\s*entrega\b",
        r"\binmediato\b",
        r"\bde\s*una\b",
        r"\bmismo\s*d[ií]a\b",
    ]

    credito_patterns = [
        r"\bcredito\b", r"\bcrédito\b",
        r"\bplazo\b",
        r"\bnet\s*30\b", r"\bnet\s*45\b", r"\bnet\s*60\b",
        r"\b30\s*d[ií]as\b", r"\b45\s*d[ií]as\b", r"\b60\s*d[ií]as\b", r"\b90\s*d[ií]as\b",
        r"\ba\s*\d{2}\s*d[ií]as\b",
    ]

    for p in contado_patterns:
        if re.search(p, t):
            return "contado"

    for p in credito_patterns:
        if re.search(p, t):
            return "credito"

    return None


# ============================================================
# OAUTH: Gmail/Sheets
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
# GMAIL WATCH
# ============================================================
def ensure_gmail_watch(gmail_service) -> Dict:
    if not GCP_PROJECT_ID or not PUBSUB_TOPIC_FULL or not PUBSUB_SUBSCRIPTION_ID:
        raise RuntimeError(
            "Faltan env vars: GCP_PROJECT_ID, PUBSUB_TOPIC_FULL y PUBSUB_SUBSCRIPTION."
        )

    state = load_state()
    now_ms = int(time.time() * 1000)
    expiration = int(state.get("watch_expiration_ms", 0))

    # Si expira en más de WATCH_RENEW_WINDOW_MS, no renovamos
    if expiration and (expiration - now_ms) > WATCH_RENEW_WINDOW_MS:
        return state

    body = {
        "topicName": PUBSUB_TOPIC_FULL,
        "labelIds": WATCH_LABEL_IDS,
        "labelFilterBehavior": "INCLUDE",
    }

    resp = gmail_service.users().watch(userId="me", body=body).execute()

    # mantener last_history_id si ya existía; si no, usar historyId del watch
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
# HISTORY -> messageIds
# ============================================================
def fetch_new_message_ids(gmail_service, start_history_id: str) -> Tuple[Set[str], Optional[str]]:
    """
    Retorna:
      - set(message_ids) agregados (messageAdded)
      - latest_history_id (para actualizar last_history_id)
    """
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
# PROCESAR MENSAJE (con 404-skip + radicado + dedupe)
# ============================================================
def safe_get_message_full(gmail_service, message_id: str) -> Optional[Dict]:
    try:
        return gmail_service.users().messages().get(
            userId="me",
            id=message_id,
            format="full"
        ).execute()
    except HttpError as e:
        # 404: mensaje ya no existe en este buzón (borrado/movido)
        if getattr(e, "resp", None) is not None and e.resp.status == 404:
            print(f"⚠️ Gmail 404: messageId {message_id} ya no existe. SKIP.")
            return None
        raise


def process_message(gmail_service, message_id: str, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    # state y dedupe
    state = load_state()
    processed = state_get_processed_set(state)
    if message_id in processed:
        return

    # ✅ radicado estable por message_id
    radicado = get_or_create_radicado(message_id, state)
    save_state(state)

    msg = safe_get_message_full(gmail_service, message_id)
    if not msg:
        # 404 => lo marcamos procesado para no insistir
        state_add_processed(state, message_id)
        save_state(state)
        return

    payload = msg.get("payload", {}) or {}
    snippet = msg.get("snippet", "") or ""

    # ✅ filtro "solo adjuntos"
    if ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS and not has_any_attachment(payload):
        state_add_processed(state, message_id)
        save_state(state)
        return

    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    date_header = get_header(payload, "Date")
    body_text = extract_plain_text(payload)

    searchable_text = f"{subject}\n{from_header}\n{body_text}\n{snippet}"

    # ✅ filtro por keywords opcional
    if not passes_keyword_filter(searchable_text):
        state_add_processed(state, message_id)
        save_state(state)
        return

    client = find_client(searchable_text, client_catalog) if client_catalog else None
    payment_type = determine_payment_type(searchable_text)
    pdf_validation = validate_required_pdfs(payload, required_count=REQUIRED_PDF_COUNT)

    print("\n" + "=" * 70)
    print(f"🆕 Procesado → Message ID: {message_id}")
    print(f"🧾 Radicado: {radicado}")
    print(f"From: {from_header or '(sin From)'}")
    print(f"Date: {date_header or '(sin Date)'}")
    print(f"Subject: {subject or snippet or '(sin subject)'}")
    print(f"Cliente: {client['name'] if client else 'no identificado (o no activo)'}")
    print(f"Tipo: {payment_type or 'indeterminado'}")

    if not pdf_validation["ok"]:
        print("🚫 Estado: RECHAZADO")
        print(
            f"Motivo: PDFs incompletos. Llegaron {pdf_validation['pdf_count']} / "
            f"{REQUIRED_PDF_COUNT} (faltan {pdf_validation['missing']})."
        )
        print("PDFs detectados:", pdf_validation["pdf_filenames"])
    else:
        print("✅ Estado: ACEPTADO")
        print(f"Adjuntos OK: {pdf_validation['pdf_count']} PDFs detectados.")
        print("PDFs:", pdf_validation["pdf_filenames"])

    print("=" * 70)

    # ✅ marcar procesado
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
            # renovar watch periódicamente (barato)
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

                    # Si es primer arranque y no hay last_history_id, inicializamos y ya
                    if not last_history:
                        update_last_history_id(history_id)
                        print(f"🔧 Inicialicé last_history_id={history_id} (primer evento).")
                        continue

                    new_ids, latest_history = fetch_new_message_ids(gmail_service, last_history)

                    # actualizamos last_history_id siempre al latest (si viene)
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

    # sanity check: quién es "me"
    profile = gmail_service.users().getProfile(userId="me").execute()
    print("✅ Autenticado como:", profile.get("emailAddress"))

    client_catalog = load_client_catalog(sheets_service)
    print(f"✅ Catálogo cargado: {len(client_catalog)} clientes activos")

    ensure_gmail_watch(gmail_service)
    listen_pubsub(gmail_service, client_catalog)


if __name__ == "__main__":
    main()
