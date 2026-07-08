"""
Clasificacion documental de ReadMail.

Modulo paralelo a las funciones de clasificacion de reademail.py:
nota credito, ordenes de compra, ok de compras, clasificacion por tipo
de documento y validacion del paquete de cuenta de cobro.

Los patrones y textos se preservan EXACTOS. Los patrones de OK compras
se reciben por parametro con el mismo default del original.
identify_client_in_order_pdfs queda para el bloque de client matching.
format_missing_documents y DOCUMENT_LABELS viven en invoice_validation.
Todavia no esta conectado a reademail.py.
"""

import re
from typing import Dict, List, Optional, Tuple

from app.models import UnifiedFile
from app.utils.text import normalize_alnum, normalize_text


DEFAULT_OK_COMPRAS_PATTERNS: List[str] = [
    x.strip()
    for x in (
        "ok compras,aprobado compras,aprobada compras,visto bueno compras,vb compras,vobo compras,"
        "aprobacion compras,aprobación compras,aprobacion de compras,aprobación de compras,"
        "visto bueno para radicacion,visto bueno para radicación,aprobado para radicar,"
        "autorizado para radicar,cuenta con visto bueno,recibida a satisfaccion,recibida a satisfacción"
    ).split(",")
    if x.strip()
]

ORDER_REGEXES = [
    re.compile(r"\borden\s+de\s+compra\b", re.IGNORECASE),
    re.compile(r"\borden\s+n(?:o|ro|umero|úmero)?\.?\s*[a-z0-9\-.]+", re.IGNORECASE),
    re.compile(r"\borden\b", re.IGNORECASE),
    re.compile(r"\boc\b\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
    re.compile(r"\bop\b\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
    re.compile(r"\borden\s*[:#\-]?\s*[a-z0-9\-.]+", re.IGNORECASE),
]

NIT_REGEX = re.compile(r"\bnit\b\s*[:#\-]?\s*([0-9][0-9.\-]{5,20})", re.IGNORECASE)

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
        "required_any": (
            "certificado bancario",
            "certificado_bancario",
            "certificado-bancario",
            "cert bancario",
            "c b",
            "certifica",
            "firma autorizada",
        ),
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


def contains_note_credit_text(text: str) -> bool:
    normalized_text = normalize_text(text)
    if not normalized_text:
        return False
    return bool(
        re.search(r"\bnota\s+(?:de\s+)?credito\b", normalized_text)
        or re.search(r"\bcredit\s+note\b", normalized_text)
    )


def is_note_credit_by_filename(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        if contains_note_credit_text(pdf.name):
            return True
    return False


def is_note_credit_by_text(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        if contains_note_credit_text(pdf.extracted_text):
            return True
    return False


def detect_order(pdfs: List[UnifiedFile]) -> bool:
    for pdf in pdfs:
        sample = f"{pdf.name}\n{pdf.extracted_text}"
        for pattern in ORDER_REGEXES:
            if pattern.search(sample):
                return True
    return False


def is_order_file(file_obj: UnifiedFile) -> bool:
    if classify_document_type(file_obj) == "orden_compra":
        return True
    sample = f"{file_obj.name}\n{file_obj.extracted_text}"
    return any(pattern.search(sample) for pattern in ORDER_REGEXES)


def detect_ok_compras(pdfs: List[UnifiedFile], patterns: Optional[List[str]] = None) -> bool:
    patterns = patterns if patterns is not None else DEFAULT_OK_COMPRAS_PATTERNS
    normalized_patterns = [normalize_alnum(x) for x in patterns if x.strip()]
    for pdf in pdfs:
        text = normalize_alnum(pdf.extracted_text)
        if any(p and p in text for p in normalized_patterns):
            return True
    return False


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
        all(_contains_document_keyword(sample, str(keyword)) for keyword in group) for group in alternate_groups
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
    estado = (
        "completo"
        if complete
        else "completo_con_desconocido"
        if complete_with_unknown
        else "incompleto"
    )
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
