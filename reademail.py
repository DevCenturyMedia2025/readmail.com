# -*- coding: utf-8 -*-
"""
Listener Gmail (push) + Pub/Sub (pull) + procesamiento de correos
+ Validación: deben llegar mínimo 5 PDFs
+ FILTRO: solo procesar correos que tengan adjuntos (para evitar newsletters / ruido)

FLUJO GENERAL:
1) OAuth: Gmail + Google Sheets (token.json / credentials.json)
2) Lee catálogo de clientes activos desde Sheets (Clientes!A:B)
3) Crea/renueva watch de Gmail (users.watch) hacia Pub/Sub (topic)
4) Escucha Pub/Sub con PULL/REST (sin streaming gRPC para evitar errores SSL)
5) Por cada evento:
   - Lee historyId
   - Consulta Gmail History desde last_history_id
   - Obtiene messageIds nuevos
   - Por cada mensaje:
       a) (nuevo) si NO tiene adjuntos → lo ignoramos
       b) si tiene adjuntos → valida PDFs mínimos
       c) imprime resultado (y luego tú conectas: responder correo + Drive)
"""

import base64
import json
import os
import os.path
import re
import time
from typing import Dict, List, Optional, Set

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
# Pub/Sub client
# (usa Application Default Credentials:
#  gcloud auth application-default login)
# -------------------------
from google.cloud import pubsub_v1


# ============================================================
# SCOPES (PERMISOS) - OAuth
# ============================================================
# gmail.readonly: solo lectura (NO puedes marcar leído, mover labels, etc.)
# spreadsheets.readonly: solo lectura del catálogo de clientes
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets.readonly",
]


# ============================================================
# CONFIG GMAIL / SHEETS
# ============================================================

# Consulta fallback (en este diseño "push" no dependemos de esto,
# pero sirve si luego agregas un modo fallback por polling)
GMAIL_QUERY = os.environ.get("GMAIL_QUERY", "is:unread")

# Máximo de mensajes en algunas lógicas (no se usa directamente en push)
MAX_MESSAGES = int(os.environ.get("MAX_EMAILS", "10"))

# Google Sheet donde está tu catálogo de clientes.
SHEET_ID = os.environ.get(
    "CLIENT_SHEET_ID",
    "14x7UflRW7P9qIHy65biueQUQjn03WBhV7T6l454VUmQ",
)

# Rango en tu spreadsheet donde está la tabla (A=cliente, B=estado)
SHEET_RANGE = os.environ.get("CLIENT_SHEET_RANGE", "Clientes!A:B")


# ============================================================
# CONFIG PUBSUB
# ============================================================

# Proyecto GCP donde vive Pub/Sub
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# Nombre de la suscripción Pub/Sub (solo el ID, no la ruta completa)
PUBSUB_SUBSCRIPTION_ID = os.environ.get("PUBSUB_SUBSCRIPTION")

# Topic completo (ruta) donde Gmail envía notificaciones
PUBSUB_TOPIC_FULL = os.environ.get(
    "PUBSUB_TOPIC_FULL",
    f"projects/{GCP_PROJECT_ID}/topics/gmail-inbox-events" if GCP_PROJECT_ID else "",
)

# Labels que Gmail observa para mandar eventos (por defecto: INBOX)
WATCH_LABEL_IDS = os.environ.get("GMAIL_LABEL_IDS", "INBOX").split(",")

# Archivo para persistir:
# - watch_expiration_ms: expiración del watch
# - last_history_id: el último historyId procesado
STATE_FILE = os.environ.get("GMAIL_WATCH_STATE_FILE", "gmail_watch_state.json")


# ============================================================
# REQUERIMIENTOS DEL NEGOCIO
# ============================================================

# Cantidad mínima de PDFs requeridos en un correo "válido"
REQUIRED_PDF_COUNT = int(os.environ.get("REQUIRED_PDF_COUNT", "5"))

# 🔥 NUEVO: si está en "true", solo procesamos mensajes con adjuntos
ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS = os.environ.get(
    "ONLY_WITH_ATTACHMENTS", "true"
).lower() in ("1", "true", "yes", "y", "si")


# (Opcional) Filtro extra por palabras clave (súper útil)
# Ej: SOLO procesar si el subject/cuerpo contiene “factura” o “cuenta de cobro”
# Si no quieres esto, déjalo vacío en .env
KEYWORDS_FILTER = [
    k.strip().lower()
    for k in os.environ.get("KEYWORDS_FILTER", "").split(",")
    if k.strip()
]
# Ej .env:
# KEYWORDS_FILTER=factura,cuenta de cobro,cobro,soportes


# ============================================================
# UTILIDADES DE TEXTO
# ============================================================

def _normalize_text(value: str) -> str:
    """
    Normaliza texto para matching “permisivo”:
    - lower()
    - elimina todo lo que no sea a-z o 0-9
    Ej: "ACME S.A.S" -> "acmesas"
    """
    return re.sub(r"[^a-z0-9]", "", (value or "").lower())


def _decode_body(data: Optional[str]) -> str:
    """
    Decodifica base64 URL-safe que Gmail entrega en body.data.
    Corrige padding si hace falta.
    """
    if not data:
        return ""
    try:
        # Gmail puede mandar base64 sin padding correcto
        missing_padding = len(data) % 4
        if missing_padding:
            data += "=" * (4 - missing_padding)

        decoded_bytes = base64.urlsafe_b64decode(data)
        return decoded_bytes.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def extract_plain_text(payload: Dict) -> str:
    """
    Extrae texto plano (text/plain) del payload:
    - Si payload.body.data existe: decodifica y retorna
    - Si no: recorre parts buscando text/plain
    - Si hay multiparts anidados: recursión
    """
    if not payload:
        return ""

    body = payload.get("body", {})
    data = body.get("data")
    if data:
        return _decode_body(data)

    for part in payload.get("parts", []) or []:
        mime_type = part.get("mimeType", "")

        if mime_type == "text/plain":
            return _decode_body(part.get("body", {}).get("data"))

        nested = extract_plain_text(part)
        if nested:
            return nested

    return ""


def get_header(payload: Dict, name: str) -> str:
    """
    Busca y retorna el valor de un header específico (Subject/From/Date/etc)
    desde payload.headers
    """
    target = name.lower()
    for header in payload.get("headers", []):
        if header.get("name", "").lower() == target:
            return header.get("value", "")
    return ""


# ============================================================
# ADJUNTOS: extraer / validar
# ============================================================

def _collect_attachments(payload: Dict) -> List[Dict[str, Optional[str]]]:
    """
    Recorre recursivamente payload/parts y recolecta adjuntos.
    Un adjunto común viene como:
      - part.filename != ""
      - part.body.attachmentId (no siempre, a veces inline)

    Retorna lista de dicts:
      { filename, mimeType, attachmentId }
    """
    attachments: List[Dict[str, Optional[str]]] = []
    if not payload:
        return attachments

    for part in payload.get("parts", []) or []:
        filename = (part.get("filename") or "").strip()
        mime_type = (part.get("mimeType") or "").strip()
        body = part.get("body", {}) or {}
        attachment_id = body.get("attachmentId")

        # Si el filename existe, lo consideramos adjunto/inline con nombre
        if filename:
            attachments.append(
                {"filename": filename, "mimeType": mime_type, "attachmentId": attachment_id}
            )

        # Buscar adjuntos en subpartes (recursivo)
        attachments.extend(_collect_attachments(part))

    return attachments


def _is_pdf(att: Dict[str, Optional[str]]) -> bool:
    """
    Identifica PDFs por:
    - mimeType = application/pdf
    - o filename termina en .pdf
    """
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return mt == "application/pdf" or fn.endswith(".pdf")


def has_any_attachment(payload: Dict) -> bool:
    """
    Retorna True si el correo tiene al menos 1 adjunto con filename.
    Ojo: esto no cuenta contenido inline sin filename.
    """
    atts = _collect_attachments(payload)
    return len(atts) > 0


def validate_required_pdfs(payload: Dict, required_count: int) -> Dict[str, object]:
    """
    Valida que el correo tenga al menos N PDFs.

    Retorna:
      ok: bool
      pdf_count: int
      missing: int
      pdf_filenames: List[str]
      all_attachments: List[Dict]
    """
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
    """
    Lee la hoja y construye catálogo de clientes activos.
    Espera:
      A: Cliente
      B: Estado (Activo/active)

    Retorna:
      [{ name, normalized }]
    """
    try:
        result = (
            sheets_service.spreadsheets()
            .values()
            .get(spreadsheetId=SHEET_ID, range=SHEET_RANGE)
            .execute()
        )
    except HttpError as error:
        raise RuntimeError(f"No pude leer Sheets. Error: {error}") from error

    values = result.get("values", [])
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
    """
    Busca coincidencia del nombre normalizado del cliente dentro del texto normalizado del correo.
    """
    normalized_text = _normalize_text(text)

    for client in catalog:
        n = client.get("normalized")
        if n and n in normalized_text:
            return client

    return None


def determine_payment_type(text: str) -> Optional[str]:
    """
    Detecta "contado" o "credito" usando patrones.
    Retorna None si no encuentra nada.
    """
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


def passes_keyword_filter(searchable_text: str) -> bool:
    """
    Si KEYWORDS_FILTER está vacío => siempre pasa.
    Si tiene palabras => el correo debe contener al menos 1 keyword.
    """
    if not KEYWORDS_FILTER:
        return True

    low = (searchable_text or "").lower()
    return any(k in low for k in KEYWORDS_FILTER)


# ============================================================
# OAUTH: Gmail/Sheets
# ============================================================

def get_oauth_creds() -> Credentials:
    """
    Autenticación OAuth:
    - Usa token.json si existe
    - Refresca si está expirado
    - Si falla refresh: borra token.json y pide login por navegador
    """
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
# STATE: watch / historyId
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


# ============================================================
# GMAIL WATCH
# ============================================================

def ensure_gmail_watch(gmail_service) -> Dict:
    """
    Crea o renueva el watch de Gmail.
    - Si ya existe y expira en > 1h: no hace nada.
    - Si no existe o expira pronto: crea watch nuevo.

    Gmail envía eventos a Pub/Sub. Esos eventos solo traen historyId.
    """
    if not GCP_PROJECT_ID or not PUBSUB_TOPIC_FULL or not PUBSUB_SUBSCRIPTION_ID:
        raise RuntimeError(
            "Faltan env vars: GCP_PROJECT_ID, PUBSUB_TOPIC_FULL (o GCP_PROJECT_ID) y PUBSUB_SUBSCRIPTION."
        )

    state = load_state()
    now_ms = int(time.time() * 1000)
    expiration = int(state.get("watch_expiration_ms", 0))

    # Si expira en más de 1 hora, mantenemos
    if expiration and (expiration - now_ms) > (60 * 60 * 1000):
        return state

    body = {
        "topicName": PUBSUB_TOPIC_FULL,
        "labelIds": [x.strip() for x in WATCH_LABEL_IDS if x.strip()],
        "labelFilterBehavior": "INCLUDE",
    }

    resp = gmail_service.users().watch(userId="me", body=body).execute()

    new_state = {
        "watch_started_at_ms": now_ms,
        "watch_expiration_ms": int(resp.get("expiration", 0)),
        # mantenemos last_history_id si ya lo teníamos (para no perder continuidad)
        "last_history_id": state.get("last_history_id") or resp.get("historyId"),
    }

    save_state(new_state)

    print(
        f"✅ Watch activo. Expira(ms): {new_state['watch_expiration_ms']} | "
        f"historyId base: {new_state['last_history_id']}"
    )

    return new_state


# ============================================================
# HISTORY -> messageIds
# ============================================================

def fetch_new_message_ids(gmail_service, start_history_id: str) -> Set[str]:
    """
    Consulta Gmail History y retorna messageIds agregados desde startHistoryId.
    - Filtra historyTypes=["messageAdded"]
    - Maneja paginación
    - Actualiza last_history_id al final
    """
    message_ids: Set[str] = set()
    page_token = None

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
        if page_token:
            continue

        latest = resp.get("historyId")
        if latest:
            st = load_state()
            st["last_history_id"] = str(latest)
            save_state(st)

        break

    return message_ids


# ============================================================
# PROCESAR MENSAJE
# ============================================================

def process_message(gmail_service, message_id: str, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    """
    Procesa un correo:
    - descarga mensaje full
    - (nuevo) filtro: si no hay adjuntos -> ignorar
    - extrae headers, body
    - (opcional) filtro por keywords
    - identifica cliente / tipo de pago
    - valida PDFs mínimos
    - imprime resultado
    """
    msg = gmail_service.users().messages().get(
        userId="me",
        id=message_id,
        format="full"
    ).execute()

    payload = msg.get("payload", {})
    snippet = msg.get("snippet", "")

    # ✅ NUEVO: filtro "solo adjuntos"
    if ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS:
        if not has_any_attachment(payload):
            # Si quieres ver qué se está ignorando, descomenta el print:
            # subject_tmp = get_header(payload, "Subject")
            # print(f"⏭️ Ignorado (sin adjuntos): {subject_tmp or snippet or message_id}")
            return

    subject = get_header(payload, "Subject")
    from_header = get_header(payload, "From")
    date_header = get_header(payload, "Date")
    body_text = extract_plain_text(payload)

    searchable_text = f"{subject}\n{from_header}\n{body_text}\n{snippet}"

    # ✅ Opcional: filtro por keywords (si KEYWORDS_FILTER no está vacío)
    if not passes_keyword_filter(searchable_text):
        # print(f"⏭️ Ignorado (sin keywords): {subject or snippet or message_id}")
        return

    client = find_client(searchable_text, client_catalog) if client_catalog else None
    payment_type = determine_payment_type(searchable_text)

    pdf_validation = validate_required_pdfs(payload, required_count=REQUIRED_PDF_COUNT)

    print("\n" + "=" * 70)
    print(f"🆕 Nuevo evento → Message ID: {message_id}")
    print(f"From: {from_header or '(sin From)'}")
    print(f"Date: {date_header or '(sin Date)'}")
    print(f"Subject: {subject or snippet or '(sin subject)'}")
    print(f"Cliente: {client['name'] if client else 'no identificado (o no activo)'}")
    print(f"Tipo: {payment_type or 'indeterminado'}")

    if not pdf_validation["ok"]:
        print("🚫 Estado: RECHAZADO")
        print(
            f"Motivo: PDF incompletos. Llegaron {pdf_validation['pdf_count']} / "
            f"{REQUIRED_PDF_COUNT} (faltan {pdf_validation['missing']})."
        )
        print("PDFs detectados:", pdf_validation["pdf_filenames"])
    else:
        print("✅ Estado: ACEPTADO")
        print(f"Adjuntos OK: {pdf_validation['pdf_count']} PDFs detectados.")
        print("PDFs:", pdf_validation["pdf_filenames"])

    print("=" * 70)


# ============================================================
# PUBSUB LISTENER (PULL/REST)
# ============================================================

def listen_pubsub(gmail_service, client_catalog: List[Dict[str, Optional[str]]]) -> None:
    """
    Listener Pub/Sub por PULL usando REST (sin streaming gRPC).
    - Hace pull cada ~1s si no hay mensajes
    - ACK al final de procesar
    - Backoff exponencial si hay error de red
    """
    subscriber = pubsub_v1.SubscriberClient(transport="rest")
    subscription_path = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    print(f"👂 Escuchando Pub/Sub (PULL/REST): {subscription_path}")

    backoff = 1

    while True:
        try:
            response = subscriber.pull(
                request={"subscription": subscription_path, "max_messages": 10}
            )

            # Si no hay mensajes, dormimos 1s y seguimos
            if not response.received_messages:
                time.sleep(1)
                backoff = 1
                continue

            ack_ids = []

            # Procesamos cada evento Pub/Sub
            for rm in response.received_messages:
                ack_ids.append(rm.ack_id)

                raw = rm.message.data.decode("utf-8")
                payload = json.loads(raw)

                history_id = str(payload.get("historyId", "")).strip()
                email_addr = payload.get("emailAddress", "")

                if not history_id:
                    # Sin historyId no podemos consultar Gmail History
                    print("⚠️ Evento sin historyId (lo ignoro).")
                    continue

                state = load_state()
                start_history = str(state.get("last_history_id") or history_id)

                new_ids = fetch_new_message_ids(gmail_service, start_history)

                if not new_ids:
                    print(f"🔔 Evento ({email_addr}) historyId={history_id} → sin messageAdded nuevos (normal).")
                else:
                    print(f"🔔 Evento ({email_addr}) historyId={history_id} → {len(new_ids)} mensaje(s) nuevo(s)")
                    for mid in new_ids:
                        process_message(gmail_service, mid, client_catalog)

            # ACK: confirmamos a Pub/Sub que ya procesamos esos eventos
            subscriber.acknowledge(
                request={"subscription": subscription_path, "ack_ids": ack_ids}
            )

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
    """
    Orquestador:
    - OAuth Gmail/Sheets
    - Construye services
    - Carga catálogo de clientes
    - Asegura watch
    - Escucha Pub/Sub
    """
    creds = get_oauth_creds()

    gmail_service = build("gmail", "v1", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    client_catalog = load_client_catalog(sheets_service)
    print(f"✅ Catálogo cargado: {len(client_catalog)} clientes activos")

    ensure_gmail_watch(gmail_service)

    listen_pubsub(gmail_service, client_catalog)


if __name__ == "__main__":
    main()
