"""
Matching de clientes contra el catalogo para ReadMail.

Modulo paralelo a las funciones de matching de reademail.py. Extrae SOLO
la logica pura: recibe un catalogo ya cargado (lista de ClientRecord) y
nunca llama a Google Sheets ni a Gmail.

Quedan fuera deliberadamente (van en fases posteriores):
- load_client_catalog y helpers (tocan la API de Sheets).
- identify_client_in_order_pdfs (orquestacion con logging y
  document_classifier.is_order_file).

Los umbrales de similitud, stopwords, regex y defaults se preservan
EXACTOS. Todavia no esta conectado a reademail.py.
"""

import re
from typing import List, Optional, Set, Tuple

from app.models import ClientRecord
from app.utils.text import EMAIL_RE, normalize_alnum, normalize_nit, normalize_text


CLIENT_MATCH_STOPWORDS = {
    "de", "del", "la", "las", "los", "el", "y", "sa", "sas", "s", "a", "esp", "e", "s", "p", "cia", "ltda", "inc",
}

ACTIVE_VALUES_DEFAULT = {"activo", "active", "si", "yes", "1", "true"}


def _header_aliases() -> dict[str, Set[str]]:
    return {
        "cliente": {
            "cliente", "razon social", "razón social", "nombre cliente", "client",
            "empresa", "proveedor", "proverdor",
            "nombre", "nombre del cliente", "razon",
        },
        "nit": {"nit", "nit cliente", "tax id"},
        "estado": {"estado", "activo", "status"},
        "email": {"email", "correo", "email contacto", "correo contacto", "correo electronico", "correo electrónico", "email de contacto"},
    }


def _resolve_column_indexes(headers: List[str]) -> dict[str, Optional[int]]:
    aliases = _header_aliases()
    normalized_headers = [normalize_text(header) for header in headers]
    resolved: dict[str, Optional[int]] = {"cliente": None, "nit": None, "estado": None, "email": None}

    for logical_name, options in aliases.items():
        normalized_options = {normalize_text(option) for option in options}
        for idx, header in enumerate(normalized_headers):
            if header in normalized_options:
                resolved[logical_name] = idx
                break

    # Fallback posicional: solo para columnas que ningún encabezado identificó.
    # Nunca reutiliza una columna ya asignada a otro campo; si no, una hoja como
    # ID|Nit|Nombre sin alias para "Nombre" cargaría los ID como nombre.
    ocupadas = {idx for idx in resolved.values() if idx is not None}
    if resolved["cliente"] is None and headers:
        resolved["cliente"] = next(
            (idx for idx in range(len(headers)) if idx not in ocupadas), 0
        )
        ocupadas.add(resolved["cliente"])
    if resolved["nit"] is None and len(headers) > 1:
        resolved["nit"] = next(
            (idx for idx in range(1, len(headers)) if idx not in ocupadas), 1
        )
    return resolved


def _looks_like_header_row(row: List[str]) -> bool:
    header_tokens = {
        normalize_text(option)
        for options in _header_aliases().values()
        for option in options
    }
    normalized = [normalize_text(cell) for cell in row]
    return any(cell in header_tokens for cell in normalized)


def client_records_from_values(
    values: List[List[str]],
    sheet_range: str = "",
    active_values: Optional[Set[str]] = None,
) -> List[ClientRecord]:
    if not values:
        return []

    first_row = [str(value).strip() for value in values[0]]
    has_header = _looks_like_header_row(first_row)
    headers = first_row if has_header else ["cliente", "nit", "estado"]
    indexes = _resolve_column_indexes(headers)
    data_rows = values[1:] if has_header else values
    active_values = active_values if active_values is not None else ACTIVE_VALUES_DEFAULT

    catalog: List[ClientRecord] = []
    for row in data_rows:
        if not row:
            continue

        cliente = _cell(row, indexes.get("cliente"))
        nit = _cell(row, indexes.get("nit"))
        estado = _cell(row, indexes.get("estado"))
        candidate_email = _cell(row, indexes.get("email"))
        contact_email = candidate_email if EMAIL_RE.fullmatch(candidate_email) else ""

        if not cliente and not nit:
            continue

        active = True
        if estado:
            active = normalize_text(estado) in active_values

        catalog.append(
            ClientRecord(
                name=cliente or nit,
                normalized_name=normalize_alnum(cliente or nit),
                nit=nit or None,
                normalized_nit=normalize_nit(nit) or None,
                contact_email=contact_email or None,
                active=active,
                raw_row={
                    **{headers[i] if i < len(headers) else str(i): str(value) for i, value in enumerate(row)},
                    "__range": sheet_range,
                },
            )
        )

    return catalog


def _cell(row: List[str], index: Optional[int]) -> str:
    if index is None or index >= len(row):
        return ""
    return str(row[index]).strip()


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


def find_client_by_name_in_text(text: str, catalog: List[ClientRecord]) -> Optional[ClientRecord]:
    if not text or not catalog:
        return None

    return find_client_in_text(text, catalog)


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


def find_contact_email_by_nit(nit: str, catalog: List[ClientRecord]) -> Optional[str]:
    record = find_client_by_nit(nit, catalog)
    if record is None:
        return None
    return record.contact_email


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


def client_lookup_catalog(catalog: List[ClientRecord]) -> List[ClientRecord]:
    clients = [
        record for record in catalog
        if "clientes" in normalize_text(record.raw_row.get("__range", ""))
    ]
    return clients or catalog


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
