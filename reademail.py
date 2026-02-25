# -*- coding: utf-8 -*-
"""
Gmail push listener: Gmail Watch -> Pub/Sub (PULL/REST) -> History -> Messages

Incluye:
- ✅ Radicado secuencial estable por correo (idempotente por Gmail messageId)
- ✅ Respuesta automática por correo (APROBADO / RECHAZADO)
- ✅ Etiquetado automático:
  - Facturación Aceptada
  - Facturación Rechazada
  - (Opcional) archivar (remover INBOX) para simular “mover”
- ✅ Validaciones del comunicado 2026:
  - Recepción: L–V 9:00 a.m. a 6:00 p.m. (Bogotá)
  - Cierre mensual 2026 (si llega después => RECHAZADO)
  - Adjuntos obligatorios y reglas por tipo de factura
- ✅ Formato obligatorio en ASUNTO o CUERPO:
  - CLIENTE: <nombre>
  - COBRO: CONTADO | CREDITO | ANTICIPO
  - FACTURA: CUENTA DE COBRO | ELECTRONICA
- ✅ Reglas de adjuntos según FACTURA (AJUSTADO):
  - ELECTRONICA: requisito mínimo:
        (2 PDFs) o (1 PDF + 1 XML) sumando adjuntos directos y ZIP (incluyendo ZIP dentro de ZIP).
    Adjuntos extra (ej. XLS, imágenes) no bloquean la radicación.
  - CUENTA DE COBRO: SOLO PDF (excepto ZIP como contenedor), y mínimo REQUIRED_PDF_COUNT PDFs
- ✅ ZIP:
  - Si llega ZIP, se inspecciona y se cuentan PDFs/XML adentro (con límites anti-zipbomb).
  - Si Drive está activo, también se suben los archivos extraídos.
- ✅ Robustez:
  - ACK SIEMPRE por cada evento Pub/Sub
  - Manejo 404 en messages.get => SKIP
  - Dedupe por state file (processed/replied)
  - Watch auto-renew

✅ Drive (NUEVA ESTRUCTURA PEDIDA):
CARPETA_MADRE
├── clientes
│   └── <cliente>
│       ├── anticipo
│       │   └── <año>
│       │       └── <mes>
│       │           └── <radicado>
│       ├── credito
│       │   └── <año>
│       │       └── <mes>
│       │           └── <radicado>
│       ├── contado
│       │   └── <año>
│       │       └── <mes>
│       │           └── <radicado>
│       └── rechazado
│           └── <año>
│               └── <mes>
│                   └── <radicado>
└── rechazados
    └── rechazados sin identificar
        └── <año>
            └── <mes>
                └── <radicado>

Dentro de <radicado> se sube:
  - email.eml
  - email.txt
  - metadata.json
  - adjuntos/*
  - adjuntos/_extraidos/<zipname>/*   (si hay ZIP)

⚠️ NOTA SCOPES DRIVE:
- Para crear/buscar carpetas existentes de forma estable se usa scope DRIVE FULL:
  https://www.googleapis.com/auth/drive
- Si vienes usando drive.file y cambiaste scopes: borra token.json y re-autentica.
"""

import base64
import json
import os
import os.path
import re
import time
import io
import zipfile
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
# SCOPES (IMPORTANTE: para enviar correo, necesitas gmail.send)
# ============================================================
SCOPES = [
    "https://www.googleapis.com/auth/gmail.modify",  # ✅ labels/archivar
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",    # ✅ responder
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",         # ✅ Drive full (carpetas existentes)
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
RECEPTION_END_HOUR = 18

CLOSING_2026 = {
    1: 28, 2: 25, 3: 27, 4: 28, 5: 27, 6: 24,
    7: 29, 8: 27, 9: 28, 10: 28, 11: 26, 12: 14
}

# ✅ LABELS (mover a etiquetas)
LABEL_ACCEPTED_NAME = os.environ.get("LABEL_ACCEPTED_NAME", "Facturación Aceptada").strip()
LABEL_REJECTED_NAME = os.environ.get("LABEL_REJECTED_NAME", "Facturación Rechazada").strip()
ARCHIVE_ON_STATUS = os.environ.get("ARCHIVE_ON_STATUS", "true").lower() in ("1", "true", "yes", "y", "si")

# ✅ DRIVE (CARPETA_MADRE)
DRIVE_ROOT_FOLDER_ID = os.environ.get("DRIVE_ROOT_FOLDER_ID", "").strip()  # recomendado: ID real de CARPETA_MADRE
DRIVE_ROOT_FOLDER_NAME = os.environ.get("DRIVE_ROOT_FOLDER_NAME", "Facturacion2026").strip()
DRIVE_USE_SHARED_DRIVE = os.environ.get("DRIVE_USE_SHARED_DRIVE", "false").lower() in ("1", "true", "yes", "y", "si")
DRIVE_SHARED_DRIVE_ID = os.environ.get("DRIVE_SHARED_DRIVE_ID", "").strip()
DRIVE_DEDUPE_ATTACHMENTS = os.environ.get("DRIVE_DEDUPE_ATTACHMENTS", "true").lower() in ("1", "true", "yes", "y", "si")

# ✅ ZIP SAFETY (anti zip-bomb)
MAX_ZIP_BYTES = int(os.environ.get("MAX_ZIP_BYTES", str(25 * 1024 * 1024)))  # 25MB zip adjunto
MAX_ZIP_FILES = int(os.environ.get("MAX_ZIP_FILES", "200"))                 # max entradas
MAX_ZIP_TOTAL_UNCOMPRESSED = int(os.environ.get("MAX_ZIP_TOTAL_UNCOMPRESSED", str(150 * 1024 * 1024)))  # 150MB
MAX_ZIP_SINGLE_FILE = int(os.environ.get("MAX_ZIP_SINGLE_FILE", str(25 * 1024 * 1024)))  # 25MB por archivo
MAX_ZIP_NESTING = int(os.environ.get("MAX_ZIP_NESTING", "2"))  # Profundidad permitida (2 => ZIP dentro de ZIP)


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

# (Opcional) Ya NO se usa, pero lo dejo por si lo quieres después.
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
# ATTACHMENTS + ZIP
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

def _is_zip(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".zip") or mt in ("application/zip", "application/x-zip-compressed")

def has_invoice_attachments(attachments: List[Dict[str, Optional[str]]]) -> bool:
    return any(_is_pdf(a) or _is_xml(a) or _is_zip(a) for a in (attachments or []))

def has_any_attachment(payload: Dict) -> bool:
    return len(_collect_attachments(payload)) > 0

def gmail_download_attachment_bytes(gmail_service, message_id: str, attachment_id: str) -> bytes:
    att = gmail_service.users().messages().attachments().get(
        userId="me", messageId=message_id, id=attachment_id
    ).execute()
    data = att.get("data", "") or ""
    return base64.urlsafe_b64decode(data.encode("utf-8"))

def _is_safe_zip_member(name: str) -> bool:
    if not name:
        return False
    n = name.replace("\\", "/")
    if n.startswith("/") or n.startswith("../") or "/../" in n:
        return False
    return True

def analyze_zip_bytes(zip_filename: str, zip_bytes: bytes, _depth: int = 1) -> Dict[str, object]:
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
            files = []
            pdf_count = 0
            xml_count = 0

            for info in infos:
                if info.is_dir():
                    continue

                if info.flag_bits & 0x1:
                    out["ok"] = False
                    out["error"] = "ZIP encriptado/protegido con contraseña (no permitido)."
                    return out

                name = info.filename
                if not _is_safe_zip_member(name):
                    out["ok"] = False
                    out["error"] = f"ZIP contiene ruta insegura: {name}"
                    return out

                size = int(getattr(info, "file_size", 0) or 0)
                if size > MAX_ZIP_SINGLE_FILE:
                    out["ok"] = False
                    out["error"] = f"Archivo dentro del ZIP demasiado grande: {name} ({size} > {MAX_ZIP_SINGLE_FILE})"
                    return out

                lower = name.lower()
                is_pdf = lower.endswith(".pdf")
                is_xml = lower.endswith(".xml")
                is_nested_zip = lower.endswith(".zip")

                entry = {
                    "name": name,
                    "size": size,
                    "is_pdf": is_pdf,
                    "is_xml": is_xml,
                    "is_zip": is_nested_zip,
                }
                files.append(entry)

                if is_nested_zip:
                    if _depth >= MAX_ZIP_NESTING:
                        out["ok"] = False
                        out["error"] = (
                            f"ZIP anidado excede profundidad permitida (MAX_ZIP_NESTING={MAX_ZIP_NESTING}). "
                            f"Archivo: {name}"
                        )
                        return out
                    nested_bytes = zf.read(name)
                    nested_analysis = analyze_zip_bytes(
                        f"{zip_filename}/{name}",
                        nested_bytes,
                        _depth=_depth + 1
                    )
                    if not nested_analysis.get("ok"):
                        out["ok"] = False
                        nested_error = nested_analysis.get("error") or "Error en ZIP anidado"
                        out["error"] = f"{name}: {nested_error}"
                        return out
                    entry["nested"] = nested_analysis
                    pdf_count += int(nested_analysis.get("pdf_count") or 0)
                    xml_count += int(nested_analysis.get("xml_count") or 0)
                    total += int(nested_analysis.get("total_uncompressed") or 0)
                    if total > MAX_ZIP_TOTAL_UNCOMPRESSED:
                        out["ok"] = False
                        out["error"] = f"ZIP excede total descomprimido ({total} > {MAX_ZIP_TOTAL_UNCOMPRESSED})"
                        return out
                    continue

                total += size
                if total > MAX_ZIP_TOTAL_UNCOMPRESSED:
                    out["ok"] = False
                    out["error"] = f"ZIP excede total descomprimido ({total} > {MAX_ZIP_TOTAL_UNCOMPRESSED})"
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

    files_out: List[Dict[str, object]] = []

    def _extract_level(current_bytes: bytes, node: Dict[str, object], prefix: str = "") -> None:
        with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
            for f in node.get("files", []) or []:
                name = (f.get("name") or "").strip()
                if not name or not _is_safe_zip_member(name):
                    continue
                try:
                    raw = zf.read(name)
                except KeyError:
                    continue

                normalized = name.replace("\\", "/")
                full_name = "/".join(part for part in [prefix, normalized] if part)

                if f.get("is_zip"):
                    nested = f.get("nested")
                    if nested:
                        _extract_level(raw, nested, full_name or normalized)
                    continue

                mime = "application/octet-stream"
                lower = normalized.lower()
                if lower.endswith(".pdf"):
                    mime = "application/pdf"
                elif lower.endswith(".xml"):
                    mime = "application/xml"

                files_out.append({"name": full_name or normalized, "bytes": raw, "mime": mime})

    try:
        _extract_level(zip_bytes, analysis, "")
        return {"ok": True, "error": None, "files": files_out}
    except Exception as e:
        return {"ok": False, "error": str(e), "files": []}

def analyze_zip_attachments(gmail_service, message_id: str, attachments: List[Dict[str, Optional[str]]]) -> List[Dict[str, object]]:
    results = []
    for a in attachments:
        if not _is_zip(a):
            continue
        fn = (a.get("filename") or "").strip() or "archivo.zip"
        att_id = a.get("attachmentId")
        if not att_id:
            results.append({
                "zip_filename": fn, "ok": False, "error": "ZIP sin attachmentId",
                "files": [], "pdf_count": 0, "xml_count": 0, "total_uncompressed": 0
            })
            continue
        try:
            zb = gmail_download_attachment_bytes(gmail_service, message_id, att_id)
            results.append(analyze_zip_bytes(fn, zb))
        except Exception as e:
            results.append({
                "zip_filename": fn, "ok": False, "error": f"No pude descargar ZIP: {e}",
                "files": [], "pdf_count": 0, "xml_count": 0, "total_uncompressed": 0
            })
    return results

def _zip_counts(zip_analysis: List[Dict[str, object]]) -> Dict[str, int]:
    pdf_zip = 0
    xml_zip = 0
    for z in zip_analysis or []:
        if not z.get("ok"):
            continue
        pdf_zip += int(z.get("pdf_count") or 0)
        xml_zip += int(z.get("xml_count") or 0)
    return {"pdf_zip": pdf_zip, "xml_zip": xml_zip}

def _iter_zip_leaf_files(analysis: Dict[str, object]) -> List[Dict[str, object]]:
    flat: List[Dict[str, object]] = []

    def _walk(node: Dict[str, object], prefix: str) -> None:
        for f in node.get("files", []) or []:
            name = (f.get("name") or "").strip()
            if not name:
                continue
            normalized = name.replace("\\", "/")
            full_name = "/".join(part for part in [prefix, normalized] if part)
            if f.get("is_zip"):
                nested = f.get("nested")
                if nested:
                    _walk(nested, full_name)
                continue
            flat.append({
                "name": full_name or normalized,
                "is_pdf": bool(f.get("is_pdf")),
                "is_xml": bool(f.get("is_xml")),
            })

    _walk(analysis or {}, "")
    return flat

def validate_required_pdfs(payload: Dict, required_count: int, zip_analysis: Optional[List[Dict[str, object]]] = None) -> Dict[str, object]:
    atts = _collect_attachments(payload)
    pdfs_direct = [a for a in atts if _is_pdf(a)]
    pdf_names = [a.get("filename") or "(sin nombre)" for a in pdfs_direct]

    counts = _zip_counts(zip_analysis or [])
    pdf_count_zip = counts["pdf_zip"]

    pdf_count_direct = len(pdfs_direct)
    pdf_total = pdf_count_direct + pdf_count_zip
    missing = max(0, required_count - pdf_total)

    return {
        "ok": pdf_total >= required_count,
        "pdf_count": pdf_total,
        "missing": missing,
        "pdf_filenames": pdf_names,
        "pdf_count_direct": pdf_count_direct,
        "pdf_count_zip": pdf_count_zip,
        "all_attachments": atts,
    }


# ============================================================
# CAMPOS OBLIGATORIOS EN ASUNTO/CUERPO
# ============================================================
def parse_radicacion_fields(subject: str, body_text: str) -> Dict[str, Optional[str]]:
    haystack = f"{subject or ''}\n{body_text or ''}"

    def pick(pattern: str) -> Optional[str]:
        m = re.search(pattern, haystack, flags=re.IGNORECASE)
        return m.group(1).strip() if m else None

    cliente = pick(r"CLIENTE\s*:\s*([^\n\|]+)")
    cobro = pick(r"COBRO\s*:\s*(CONTADO|CREDITO|CRÉDITO|ANTICIPO)")
    factura = pick(r"FACTURA\s*:\s*(CUENTA\s+DE\s+COBRO|CUENTADECOBRO|ELECTRONICA|ELECTRÓNICA)")

    if cobro:
        cobro = cobro.upper().replace("CRÉDITO", "CREDITO")
    if factura:
        factura = factura.upper().replace("ELECTRÓNICA", "ELECTRONICA")
        factura_compact = factura.replace(" ", "")
        if factura_compact == "CUENTADECOBRO":
            factura = "CUENTA DE COBRO"

    return {"cliente": cliente, "cobro": cobro, "factura": factura}

def validate_required_radicacion_fields(fields: Dict[str, Optional[str]]) -> List[str]:
    missing = []
    if not fields.get("cliente"):
        missing.append("CLIENTE")
    if not fields.get("cobro"):
        missing.append("COBRO (CONTADO|CREDITO|ANTICIPO)")
    if not fields.get("factura"):
        missing.append("FACTURA (CUENTA DE COBRO|ELECTRONICA)")
    return missing

def validate_invoice_type_attachments(
    factura_type: str,
    attachments: List[Dict[str, Optional[str]]],
    zip_analysis: Optional[List[Dict[str, object]]] = None
) -> List[str]:
    errors = []

    pdf_direct = sum(1 for a in attachments if _is_pdf(a))
    xml_direct = sum(1 for a in attachments if _is_xml(a))

    counts = _zip_counts(zip_analysis or [])
    pdf_zip = counts["pdf_zip"]
    xml_zip = counts["xml_zip"]

    pdf_total = pdf_direct + pdf_zip
    xml_total = xml_direct + xml_zip

    # 1) Validar tipos directos (permitimos ZIP)
    for a in attachments:
        if _is_zip(a):
            continue
        if factura_type == "CUENTA DE COBRO":
            if not _is_pdf(a):
                errors.append(f"Adjunto no permitido (FACTURA CUENTA DE COBRO solo PDF): {a.get('filename')}")

    # 2) Validar contenido del ZIP (si se pudo leer)
    for z in zip_analysis or []:
        if not z.get("ok"):
            errors.append(f"ZIP no se pudo procesar ({z.get('zip_filename')}): {z.get('error')}")
            continue
        for f in _iter_zip_leaf_files(z):
            name = f.get("name") or ""
            is_pdf = bool(f.get("is_pdf"))
            is_xml = bool(f.get("is_xml"))
            if factura_type == "CUENTA DE COBRO":
                if not is_pdf:
                    errors.append(
                        f"Archivo no permitido dentro del ZIP para CUENTA DE COBRO (solo PDF): "
                        f"{z.get('zip_filename')} → {name}"
                    )

    if errors:
        return errors

    # 3) Reglas específicas por tipo
    if factura_type == "ELECTRONICA":
        has_two_pdfs = pdf_total >= 2
        has_pdf_xml_mix = (pdf_total >= 1) and (xml_total >= 1)
        if not (has_two_pdfs or has_pdf_xml_mix):
            errors.append(
                "FACTURA ELECTRÓNICA requiere mínimo (2 PDFs) o (1 PDF + 1 XML) "
                "sumando adjuntos directos y ZIP (incluyendo ZIP dentro de ZIP)."
            )
            errors.append(
                f"Detectados → PDFs: {pdf_total} (directos {pdf_direct}, ZIP {pdf_zip}) | "
                f"XML: {xml_total} (directos {xml_direct}, ZIP {xml_zip})."
            )
        return errors

    if factura_type == "CUENTA DE COBRO":
        if xml_total > 0:
            errors.append("FACTURA CUENTA DE COBRO no debe incluir XML (solo PDF).")
        return errors

    errors.append(f"Tipo de FACTURA desconocido: {factura_type}")

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
# LABELS (crear / buscar / aplicar)
# ============================================================
def get_label_id_by_name(gmail_service, label_name: str) -> Optional[str]:
    try:
        resp = gmail_service.users().labels().list(userId="me").execute()
        for lb in resp.get("labels", []) or []:
            if (lb.get("name", "") or "").strip().lower() == label_name.strip().lower():
                return lb.get("id")
        return None
    except HttpError as e:
        print(f"⚠️ Error listando labels: {e}")
        return None

def ensure_label_exists(gmail_service, label_name: str) -> Optional[str]:
    lid = get_label_id_by_name(gmail_service, label_name)
    if lid:
        return lid
    try:
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
    except HttpError as e:
        print(f"⚠️ No pude crear la etiqueta '{label_name}': {e}")
        return None

def apply_status_labels(gmail_service, message_id: str, status: str) -> None:
    accepted_id = ensure_label_exists(gmail_service, LABEL_ACCEPTED_NAME)
    rejected_id = ensure_label_exists(gmail_service, LABEL_REJECTED_NAME)

    add_ids = []
    remove_ids = []

    if status == "accepted":
        if accepted_id:
            add_ids.append(accepted_id)
        if rejected_id:
            remove_ids.append(rejected_id)
    elif status == "rejected":
        if rejected_id:
            add_ids.append(rejected_id)
        if accepted_id:
            remove_ids.append(accepted_id)
    else:
        return

    if ARCHIVE_ON_STATUS:
        remove_ids.append("INBOX")  # archiva (sale de recibidos)

    add_ids = list(dict.fromkeys(add_ids))
    remove_ids = list(dict.fromkeys(remove_ids))

    try:
        gmail_service.users().messages().modify(
            userId="me",
            id=message_id,
            body={"addLabelIds": add_ids, "removeLabelIds": remove_ids}
        ).execute()
        print(f"🏷️ Etiquetas aplicadas ({status}) a messageId={message_id}")
    except HttpError as e:
        print(f"⚠️ No pude aplicar etiquetas a {message_id}: {e}")


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
        f"Tipo de factura: {factura} (CUENTA DE COBRO / ELECTRONICA)\n\n"
        "Motivos del rechazo:\n"
        + "".join([f"- {r}\n" for r in reasons]) +
        "\nQué debes corregir y reenviar (en un solo correo):\n"
        "1) En asunto o cuerpo indicar: CLIENTE + COBRO + FACTURA.\n"
        "2) Adjuntar soportes en PDF (y XML si aplica para ELECTRONICA).\n\n"
        "Ejemplo válido:\n"
        "CLIENTE: ACME SAS | COBRO: CREDITO | FACTURA: ELECTRONICA\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body

def build_approved_email(radicado: str, fields: Dict[str, Optional[str]], pdf_total: int) -> Tuple[str, str]:
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
        f"Tipo de factura: {factura} (CUENTA DE COBRO / ELECTRONICA)\n"
        f"PDFs detectados (directo + ZIP): {pdf_total}\n\n"
        "Tu solicitud queda en proceso según los tiempos internos de revisión y pago.\n\n"
        "Gracias,\n"
        "Equipo de Facturación\n"
    )
    return subject, body


# ============================================================
# DRIVE (carpetas + subida de contenido) - NUEVA ESTRUCTURA
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
    q = f"'{parent_id}' in parents and trashed=false and name='{safe_name}'"
    res = drive_service.files().list(
        q=q, fields="files(id,name)", pageSize=1, **_drive_list_kwargs()
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
        q=q, fields="files(id,name)", pageSize=10, **_drive_list_kwargs()
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
    if DRIVE_ROOT_FOLDER_ID:
        try:
            drive_service.files().get(
                fileId=DRIVE_ROOT_FOLDER_ID, fields="id,name", **_drive_create_kwargs()
            ).execute()
            return DRIVE_ROOT_FOLDER_ID
        except Exception as e:
            print(f"⚠️ Sin acceso a DRIVE_ROOT_FOLDER_ID={DRIVE_ROOT_FOLDER_ID}. Intentaré por nombre. Error: {e}")

    safe_name = DRIVE_ROOT_FOLDER_NAME.replace("'", "\\'")
    q = "mimeType='application/vnd.google-apps.folder' and trashed=false and name='%s'" % safe_name
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
        body=meta, media_body=media, fields="id", **_drive_create_kwargs()
    ).execute()
    return f["id"]

def _cobro_to_folder_name(cobro: Optional[str]) -> str:
    c = (cobro or "").upper().strip()
    if c == "CONTADO":
        return "contado"
    if c == "CREDITO":
        return "credito"
    if c == "ANTICIPO":
        return "anticipo"
    return "contado"

def drive_build_radicado_folder_new_structure(
    drive_service,
    root_id: str,
    estado: str,
    cliente_name: Optional[str],
    cobro: Optional[str],
    radicado: str,
    received_dt: Optional[datetime],
    cliente_identificado: bool,
) -> str:
    if not received_dt:
        received_dt = datetime.now(TZ_BOGOTA)

    yyyy = f"{received_dt.year:04d}"
    mm = f"{received_dt.month:02d}"

    if estado == "accepted":
        base = drive_get_or_create_folder(drive_service, root_id, "clientes")
        base = drive_get_or_create_folder(drive_service, base, (cliente_name or "NO_IDENTIFICADO").strip() or "NO_IDENTIFICADO")
        base = drive_get_or_create_folder(drive_service, base, _cobro_to_folder_name(cobro))
    else:
        if cliente_identificado and (cliente_name or "").strip():
            base = drive_get_or_create_folder(drive_service, root_id, "clientes")
            base = drive_get_or_create_folder(drive_service, base, cliente_name.strip())
            base = drive_get_or_create_folder(drive_service, base, "rechazado")
        else:
            base = drive_get_or_create_folder(drive_service, root_id, "rechazados")
            base = drive_get_or_create_folder(drive_service, base, "rechazados sin identificar")

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
    cliente_name: Optional[str],
    cobro: Optional[str],
    fields: Dict[str, Optional[str]],
    subject: str,
    body_text: str,
    received_dt: Optional[datetime],
    reasons: Optional[List[str]],
    attachments: List[Dict[str, Optional[str]]],
    zip_analysis: Optional[List[Dict[str, object]]] = None,
) -> None:
    if not drive_service or not root_id:
        return

    cliente_identificado = bool((cliente_name or "").strip()) and (cliente_name.strip().lower() not in {"no_identificado", "no identificado"})

    rad_folder_id = drive_build_radicado_folder_new_structure(
        drive_service=drive_service,
        root_id=root_id,
        estado=estado,
        cliente_name=cliente_name,
        cobro=cobro,
        radicado=radicado,
        received_dt=received_dt,
        cliente_identificado=cliente_identificado,
    )

    try:
        if drive_file_exists(drive_service, rad_folder_id, "metadata.json"):
            print(f"ℹ️ Drive: ya existe metadata.json para {radicado}, no duplico subida.")
            return
    except Exception as e:
        print(f"⚠️ Drive: no pude verificar metadata.json (continuo). Error: {e}")

    try:
        raw_msg = gmail_service.users().messages().get(userId="me", id=message_id, format="raw").execute()
        raw_b64 = raw_msg.get("raw", "") or ""
        eml_bytes = base64.urlsafe_b64decode(raw_b64.encode("utf-8"))
        drive_upload_bytes(drive_service, rad_folder_id, "email.eml", eml_bytes, "message/rfc822")
    except Exception as e:
        print(f"⚠️ No pude subir email.eml ({radicado}): {e}")

    try:
        drive_upload_bytes(
            drive_service, rad_folder_id, "email.txt",
            (body_text or "").encode("utf-8"), "text/plain"
        )
    except Exception as e:
        print(f"⚠️ No pude subir email.txt ({radicado}): {e}")

    try:
        payload = (msg_full.get("payload") or {})
        metadata = {
            "radicado": radicado,
            "estado": estado,
            "cliente": fields.get("cliente"),
            "cliente_resuelto": cliente_name,
            "cobro": fields.get("cobro"),
            "factura": fields.get("factura"),
            "message_id": message_id,
            "thread_id": msg_full.get("threadId"),
            "subject": subject,
            "from": get_header(payload, "From"),
            "received_dt_bogota": received_dt.isoformat() if received_dt else None,
            "rejection_reasons": reasons or [],
            "attachments": [{"filename": a.get("filename"), "mimeType": a.get("mimeType")} for a in attachments],
            "zip_analysis": zip_analysis or [],
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

        zip_atts = [a for a in attachments if _is_zip(a)]
        if zip_atts:
            extra_root = drive_get_or_create_folder(drive_service, adj_folder_id, "_extraidos")

            for a in zip_atts:
                fn = (a.get("filename") or "archivo.zip").strip()
                att_id = a.get("attachmentId")
                if not att_id:
                    continue

                try:
                    zb = gmail_download_attachment_bytes(gmail_service, message_id, att_id)
                    extracted = extract_zip_files(fn, zb)
                    if not extracted.get("ok"):
                        continue

                    zip_folder = drive_get_or_create_folder(drive_service, extra_root, fn)

                    for f in extracted.get("files", []) or []:
                        name = (f.get("name") or "").replace("\\", "/")
                        data = f.get("bytes") or b""
                        mime = f.get("mime") or "application/octet-stream"

                        parts = [p for p in name.split("/") if p]
                        if not parts:
                            continue

                        current_parent = zip_folder
                        for folder_part in parts[:-1]:
                            current_parent = drive_get_or_create_folder(drive_service, current_parent, folder_part)

                        final_name = parts[-1]
                        if DRIVE_DEDUPE_ATTACHMENTS:
                            try:
                                if drive_file_exists(drive_service, current_parent, final_name):
                                    continue
                            except Exception:
                                pass

                        drive_upload_bytes(drive_service, current_parent, final_name, data, mime)

                except Exception as e:
                    print(f"⚠️ Drive: fallo subiendo extraídos ZIP ({fn}): {e}")

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

    if ONLY_PROCESS_EMAILS_WITH_ATTACHMENTS and not has_any_attachment(payload):
        state_add_processed(state, message_id)
        save_state(state)
        return

    attachments = _collect_attachments(payload)

    zip_analysis = analyze_zip_attachments(gmail_service, message_id, attachments) if any(_is_zip(a) for a in attachments) else []

    root_id = None
    if drive_service:
        try:
            root_id = drive_create_root_if_needed(drive_service)
        except Exception as e:
            print(f"⚠️ Drive deshabilitado por error creando/validando root: {e}")
            root_id = None

    # -------------------------
    # RECHAZO: horario
    # -------------------------
    if received_dt and not is_within_receiving_window(received_dt):
        apply_status_labels(gmail_service, message_id, "rejected")
        reasons = [f"Fuera de horario de recepción (L–V 9:00 a.m. a 6:00 p.m.). Llegó: {received_dt.isoformat()}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, {}, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                cliente_name=None, cobro=None, fields={}, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments, zip_analysis=zip_analysis
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    # -------------------------
    # RECHAZO: cierre mensual 2026
    # -------------------------
    if received_dt and is_after_monthly_closing_2026(received_dt):
        apply_status_labels(gmail_service, message_id, "rejected")
        reasons = [f"Llegó después de la fecha de cierre del mes (calendario 2026). Llegó: {received_dt.date().isoformat()}"]

        if not already_replied:
            subj, body = build_rejected_email(radicado, {}, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                cliente_name=None, cobro=None, fields={}, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments, zip_analysis=zip_analysis
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    if not passes_keyword_filter(searchable_text):
        state_add_processed(state, message_id)
        save_state(state)
        return

    fields = parse_radicacion_fields(subject, body_text)
    missing_fields = validate_required_radicacion_fields(fields)
    if missing_fields:
        apply_status_labels(gmail_service, message_id, "rejected")
        reasons = [
            "Falta información obligatoria en ASUNTO o CUERPO.",
            f"Faltantes: {', '.join(missing_fields)}"
        ]

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        cliente_guess = (fields.get("cliente") or "").strip() or None
        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                cliente_name=cliente_guess, cobro=fields.get("cobro"), fields=fields, subject=subject,
                body_text=body_text, received_dt=received_dt, reasons=reasons,
                attachments=attachments, zip_analysis=zip_analysis
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    client_obj = find_client_exact_or_normalized(fields["cliente"], client_catalog) if client_catalog else None
    if client_catalog and not client_obj:
        apply_status_labels(gmail_service, message_id, "rejected")
        reasons = [
            "CLIENTE no existe en el catálogo o no está activo.",
            f"CLIENTE declarado: {fields.get('cliente')}"
        ]

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                cliente_name=None, cobro=fields.get("cobro"), fields=fields, subject=subject,
                body_text=body_text, received_dt=received_dt, reasons=reasons,
                attachments=attachments, zip_analysis=zip_analysis
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    invoice_attach_errors = validate_invoice_type_attachments(fields["factura"], attachments, zip_analysis=zip_analysis)
    if invoice_attach_errors:
        apply_status_labels(gmail_service, message_id, "rejected")
        reasons = ["Adjuntos no cumplen el tipo de FACTURA declarado."] + invoice_attach_errors

        if not already_replied:
            subj, body = build_rejected_email(radicado, fields, reasons)
            send_reply_email(gmail_service, msg, to_email, subj, body)
            state_mark_replied(state, message_id)
            save_state(state)

        if root_id:
            store_email_to_drive(
                gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                cliente_name=client_obj["name"] if client_obj else None,
                cobro=fields.get("cobro"), fields=fields, subject=subject, body_text=body_text,
                received_dt=received_dt, reasons=reasons, attachments=attachments, zip_analysis=zip_analysis
            )

        state_add_processed(state, message_id)
        save_state(state)
        return

    pdf_validation = validate_required_pdfs(payload, required_count=REQUIRED_PDF_COUNT, zip_analysis=zip_analysis)

    if fields["factura"] == "CUENTA DE COBRO":
        if not pdf_validation["ok"]:
            apply_status_labels(gmail_service, message_id, "rejected")
            reasons = [
                f"PDF incompletos. Llegaron {pdf_validation['pdf_count']} / {REQUIRED_PDF_COUNT} (faltan {pdf_validation['missing']}).",
                f"PDFs directos: {pdf_validation['pdf_count_direct']} | PDFs en ZIP: {pdf_validation['pdf_count_zip']}",
                f"PDFs directos detectados: {', '.join(pdf_validation['pdf_filenames']) if pdf_validation['pdf_filenames'] else '(ninguno)'}"
            ]

            if not already_replied:
                subj, body = build_rejected_email(radicado, fields, reasons)
                send_reply_email(gmail_service, msg, to_email, subj, body)
                state_mark_replied(state, message_id)
                save_state(state)

            if root_id:
                store_email_to_drive(
                    gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
                    message_id=message_id, msg_full=msg, radicado=radicado, estado="rejected",
                    cliente_name=client_obj["name"] if client_obj else None,
                    cobro=fields.get("cobro"), fields=fields, subject=subject, body_text=body_text,
                    received_dt=received_dt, reasons=reasons, attachments=attachments, zip_analysis=zip_analysis
                )

            state_add_processed(state, message_id)
            save_state(state)
            return

    apply_status_labels(gmail_service, message_id, "accepted")
    if not already_replied:
        subj, body = build_approved_email(radicado, fields, pdf_validation["pdf_count"])
        send_reply_email(gmail_service, msg, to_email, subj, body)
        state_mark_replied(state, message_id)
        save_state(state)

    if root_id:
        store_email_to_drive(
            gmail_service=gmail_service, drive_service=drive_service, root_id=root_id,
            message_id=message_id, msg_full=msg, radicado=radicado, estado="accepted",
            cliente_name=client_obj["name"] if client_obj else (fields.get("cliente") or None),
            cobro=fields.get("cobro"), fields=fields, subject=subject, body_text=body_text,
            received_dt=received_dt, reasons=None, attachments=attachments, zip_analysis=zip_analysis
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
    print("✅ Estado: ACEPTADO")
    print(f"PDFs detectados total (directo+ZIP): {pdf_validation['pdf_count']}")
    if zip_analysis:
        for z in zip_analysis:
            print(f"📦 ZIP: {z.get('zip_filename')} | ok={z.get('ok')} | pdf={z.get('pdf_count')} | xml={z.get('xml_count')} | err={z.get('error')}")
    print("=" * 70)

    state_add_processed(state, message_id)
    save_state(state)


# ============================================================
# PUBSUB LISTENER (PULL/REST) - ACK SIEMPRE
# ============================================================
def listen_pubsub(
    gmail_service,
    drive_service,
    sheets_service,
    client_catalog: List[Dict[str, Optional[str]]]
) -> None:
    subscriber = pubsub_v1.SubscriberClient(transport="rest")
    subscription_path = f"projects/{GCP_PROJECT_ID}/subscriptions/{PUBSUB_SUBSCRIPTION_ID}"
    print(f"👂 Escuchando Pub/Sub (PULL/REST): {subscription_path}")

    backoff = 1
    catalog_data = client_catalog or []

    def refresh_catalog() -> None:
        nonlocal catalog_data
        try:
            updated = load_client_catalog(sheets_service)
            catalog_data = updated
            print(f"🔄 Catálogo de clientes actualizado: {len(catalog_data)} clientes activos")
        except Exception as e:
            print(f"⚠️ No pude refrescar el catálogo de clientes: {e}")

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
                        refresh_catalog()
                        for mid in new_ids:
                            process_message(gmail_service, drive_service, mid, catalog_data)

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
    listen_pubsub(gmail_service, drive_service, sheets_service, client_catalog)


if __name__ == "__main__":
    main()
