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

import os
import re
from typing import Dict, List, Optional, Tuple

from app.models import UnifiedFile
from app.utils.text import normalize_alnum, normalize_text


DEFAULT_OK_COMPRAS_PATTERNS: List[str] = [
    x.strip()
    for x in (
        "ok compras,aprobado compras,aprobada compras,visto bueno compras,vb compras,vobo compras,"
        "aprobacion compras,autorizado compras,visto bueno para radicacion,aprobado para radicar,"
        "autorizado para radicar,cuenta con visto bueno,recibida a satisfaccion"
    ).split(",")
    if x.strip()
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
OK_COMPRAS_FILENAME_REGEXES = (
    re.compile(rf"\bok{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras\b"),
    re.compile(rf"\bvisto\s+bueno(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvo\s*bo(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvobo(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
    re.compile(rf"\bvb(?:{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras)?\b"),
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
        r"\ben\s+espera\b",
    )
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


def _compile_ok_compras_pattern(normalized_pattern: str) -> re.Pattern:
    purchases_match = OK_COMPRAS_WITH_PURCHASES_REGEX.fullmatch(normalized_pattern)
    if purchases_match:
        term = re.escape(purchases_match.group("term"))
        return re.compile(
            rf"(?<!\w){term}{OK_COMPRAS_OPTIONAL_CONNECTOR_REGEX}\s+compras(?!\w)"
        )
    return re.compile(rf"(?<!\w){re.escape(normalized_pattern)}(?!\w)")


def contains_ok_compras_text(text: str, patterns: Optional[List[str]] = None) -> bool:
    """Detecta una aprobación sin negaciones en la oración completa.

    Cada oración se delimita por puntuación fuerte o salto de línea. Una
    negación anterior o posterior al término veta la aprobación de esa oración.
    """
    configured_patterns = patterns if patterns is not None else DEFAULT_OK_COMPRAS_PATTERNS
    approval_patterns = [
        _compile_ok_compras_pattern(normalize_text(pattern))
        for pattern in configured_patterns
        if pattern
    ]
    for raw_sentence in re.split(r"[.!?;:\n]+", text or ""):
        normalized_sentence = normalize_text(raw_sentence)
        if any(exclusion.search(normalized_sentence) for exclusion in OK_COMPRAS_NEGATIVE_REGEXES):
            continue
        if any(pattern.search(normalized_sentence) for pattern in approval_patterns):
            return True
    return False


def filename_declares_ok_compras(filename: str) -> bool:
    """Indica si el nombre identifica un adjunto de OK/visto bueno de compras."""
    stem = os.path.splitext(os.path.basename(filename or ""))[0]
    normalized_name = normalize_text(stem.replace("_", " "))
    if any(exclusion.search(normalized_name) for exclusion in OK_COMPRAS_NEGATIVE_REGEXES):
        return False
    return any(pattern.search(normalized_name) for pattern in OK_COMPRAS_FILENAME_REGEXES)


def is_ok_compras_document(file_obj: UnifiedFile) -> bool:
    """Valida el adjunto por nombre y conserva como respaldo el sello en su texto."""
    return filename_declares_ok_compras(file_obj.name) or contains_ok_compras_text(
        file_obj.extracted_text
    )


def detect_ok_compras(pdfs: List[UnifiedFile], patterns: Optional[List[str]] = None) -> bool:
    return any(
        filename_declares_ok_compras(pdf.name)
        or contains_ok_compras_text(pdf.extracted_text, patterns)
        for pdf in pdfs
    )


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
