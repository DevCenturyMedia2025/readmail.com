#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Clasificador y estampador de documentos.

Uso:
    python3 clasificador_docs.py <archivo_o_carpeta> [carpeta_salida]
"""

import argparse
import os
import re
import shutil
import sys
import tempfile
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

try:
    from PIL import Image
except Exception:  # pragma: no cover
    Image = None

try:
    import pytesseract
except Exception:  # pragma: no cover
    pytesseract = None

if pytesseract is not None and not shutil.which("tesseract"):
    local_tesseract = Path.home() / "AppData" / "Local" / "Programs" / "Tesseract-OCR" / "tesseract.exe"
    if local_tesseract.exists():
        pytesseract.pytesseract.tesseract_cmd = str(local_tesseract)


SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png"}

DOCUMENT_TYPES: Dict[str, Dict[str, object]] = {
    "CUENTA_COBRO": {
        "label": "CUENTA DE COBRO",
        "color": (21, 101, 192),
        "required_any": ("CUENTA COBRO", "CUENTA DE COBRO", "CUENTA_COBRO"),
        "alternate_all": (("DEBE A", "LA SUMA DE"),),
        "support": ("POR CONCEPTO DE", "POR FAVOR CONSIGNAR"),
        "min_support": 0,
    },
    "ORDEN_COMPRA": {
        "label": "ORDEN DE COMPRA",
        "color": (230, 81, 0),
        "required_any": ("ORDEN DE INTERNET", "ORDEN DE COMPRA", "ORDEN NO", "ORDEN NRO", "ORDEN NUMERO"),
        "alternate_all": (("SUBTOTAL", "AUTORIZADO POR"),),
        "support": ("VALOR NETO", "CPM COSTO X CLICK", "ELABORADO POR", "NO SE RECIBE FACTURA", "CENTURY MEDIA"),
        "min_support": 0,
    },
    "RUT": {
        "label": "RUT - DIAN",
        "color": (46, 125, 50),
        "required_any": ("REGISTRO UNICO TRIBUTARIO", "RUT"),
        "alternate_all": (("DIAN", "NIT", "ACTIVIDAD ECONOMICA"),),
        "support": (
            "NUMERO DE IDENTIFICACION TRIBUTARIA",
            "RESPONSABILIDADES CALIDADES Y ATRIBUTOS",
            "TIPO DE CONTRIBUYENTE",
            "REGIMEN SIMPLIFICADO",
            "FORMULARIO DEL REGISTRO",
            "MUISCA",
        ),
        "min_support": 0,
    },
    "CERTIFICADO_BANCARIO": {
        "label": "CERTIFICADO BANCARIO",
        "color": (183, 28, 28),
        "required_any": ("CERTIFICADO BANCARIO", "CERTIFICA", "FIRMA AUTORIZADA"),
        "alternate_all": (("CERTIFICA", "CUENTA DE AHORROS"),),
        "support": (
            "BANCO",
            "SALDO O CUPO DISPONIBLE",
            "TIPO DE PRODUCTO",
            "NRO DE PRODUCTO",
            "DAVIVIENDA",
            "BANCOLOMBIA",
            "BANCO DE BOGOTA",
        ),
        "min_support": 1,
    },
    "CEDULA": {
        "label": "CEDULA DE CIUDADANIA",
        "color": (106, 27, 154),
        "required_any": ("CEDULA DE CIUDADANIA", "CEDULA", "IDENTIFICACION PERSONAL"),
        "alternate_all": (("REPUBLICA DE COLOMBIA", "IDENTIFICACION"),),
        "support": ("FECHA DE NACIMIENTO", "LUGAR DE EXPEDICION", "INDICE DERECHO"),
        "min_support": 0,
    },
    "APROBADO_COMPRAS": {
        "label": "APROBADO DE COMPRAS",
        "color": (55, 71, 79),
        "required_any": ("APROBADO", "APROBACION", "APROBADO POR"),
        "alternate_all": (),
        "support": ("VO BO", "VO.BO.", "VISTO BUENO", "AUTORIZADO", "JEFE DE COMPRAS", "GERENTE", "FIRMA DE APROBACION"),
        "min_support": 1,
    },
}

UNKNOWN_TYPE = {
    "id": "DESCONOCIDO",
    "label": "DOCUMENTO SIN CLASIFICAR",
    "color": (120, 120, 120),
}


def strip_accents(value: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", value or "") if not unicodedata.combining(ch))


def normalize_text(value: str) -> str:
    value = strip_accents(value or "").upper()
    value = value.replace("_", " ")
    value = re.sub(r"[^A-Z0-9]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def contains_keyword(text: str, keyword: str) -> bool:
    text_norm = normalize_text(text)
    keyword_norm = normalize_text(keyword)
    if not text_norm or not keyword_norm:
        return False
    if keyword_norm == "RUT":
        return bool(re.search(r"\bRUT\b", text_norm))
    return keyword_norm in text_norm


def extract_text_from_pdf(path: Path) -> str:
    if fitz is None:
        raise RuntimeError("Falta PyMuPDF. Instala con: pip install pymupdf")
    text_parts: List[str] = []
    try:
        with fitz.open(path) as doc:
            for page in doc:
                text_parts.append(page.get_text("text") or "")
    except Exception as exc:
        print(f"  [!] Error extrayendo texto PDF {path.name}: {exc}")
    return "\n".join(text_parts)


def ocr_image(image) -> str:
    if pytesseract is None:
        return ""
    try:
        return pytesseract.image_to_string(image, lang="spa")
    except Exception:
        try:
            return pytesseract.image_to_string(image)
        except Exception as exc:
            print(f"  [!] Error OCR: {exc}")
            return ""


def extract_text_from_image(path: Path) -> str:
    if Image is None:
        print("  [!] Falta Pillow. Instala con: pip install Pillow")
        return ""
    try:
        with Image.open(path) as img:
            return ocr_image(img)
    except Exception as exc:
        print(f"  [!] Error leyendo imagen {path.name}: {exc}")
        return ""


def extract_text_from_pdf_ocr(path: Path) -> str:
    if fitz is None:
        raise RuntimeError("Falta PyMuPDF. Instala con: pip install pymupdf")
    if pytesseract is None:
        return ""
    text_parts: List[str] = []
    try:
        with fitz.open(path) as doc:
            for page in doc:
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                if Image is None:
                    return ""
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                text_parts.append(ocr_image(img))
    except Exception as exc:
        print(f"  [!] Error OCR PDF {path.name}: {exc}")
    return "\n".join(text_parts)


def get_text(path: Path) -> str:
    ext = path.suffix.lower()
    filename_text = path.stem.replace("_", " ")
    if ext == ".pdf":
        text = extract_text_from_pdf(path)
        if len(normalize_text(text)) < 50:
            ocr_text = extract_text_from_pdf_ocr(path)
            if ocr_text:
                text = f"{text}\n{ocr_text}"
        return f"{filename_text}\n{text}"
    if ext in {".jpg", ".jpeg", ".png"}:
        return f"{filename_text}\n{extract_text_from_image(path)}"
    return filename_text


def classifier_score(text: str, config: Dict[str, object]) -> int:
    required_any = tuple(str(x) for x in config.get("required_any", ()))
    alternate_all = tuple(config.get("alternate_all", ()) or ())
    support = tuple(str(x) for x in config.get("support", ()))
    min_support = int(config.get("min_support") or 0)

    required_hits = sum(1 for keyword in required_any if contains_keyword(text, keyword))
    alternate_hit = any(all(contains_keyword(text, str(keyword)) for keyword in group) for group in alternate_all)
    if required_hits < 1 and not alternate_hit:
        return 0

    support_hits = sum(1 for keyword in support if contains_keyword(text, keyword))
    if not alternate_hit and support_hits < min_support:
        return 0
    return (required_hits * 10) + (10 if alternate_hit else 0) + support_hits


def classify_document(text: str) -> Dict[str, object]:
    matches: List[Tuple[int, str, Dict[str, object]]] = []
    for doc_id, config in DOCUMENT_TYPES.items():
        score = classifier_score(text, config)
        if score:
            doc_type = dict(config)
            doc_type["id"] = doc_id
            matches.append((score, doc_id, doc_type))

    if not matches:
        return dict(UNKNOWN_TYPE)

    matches.sort(reverse=True, key=lambda item: item[0])
    if len(matches) > 1 and matches[0][0] == matches[1][0]:
        return dict(UNKNOWN_TYPE)
    return matches[0][2]


def stamp_pdf(input_path: Path, output_path: Path, doc_type: Dict[str, object]) -> None:
    if fitz is None:
        raise RuntimeError("Falta PyMuPDF. Instala con: pip install pymupdf")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with fitz.open(input_path) as doc:
        r, g, b = doc_type["color"]  # type: ignore[index]
        color_rgb = (r / 255, g / 255, b / 255)
        label = str(doc_type["label"])

        for page in doc:
            rect = page.rect
            stamp_w = min(220, rect.width - 20)
            stamp_h = 28
            x1 = rect.width - stamp_w - 10
            y1 = 10
            stamp_rect = fitz.Rect(x1, y1, rect.width - 10, y1 + stamp_h)
            page.draw_rect(stamp_rect, color=color_rgb, fill=color_rgb)
            page.insert_text(
                fitz.Point(x1 + 7, y1 + 18),
                label,
                fontsize=8,
                color=(1, 1, 1),
                fontname="helv",
            )
        doc.save(output_path)


def image_to_temp_pdf(input_path: Path) -> Path:
    if Image is None:
        raise RuntimeError("Falta Pillow. Instala con: pip install Pillow")
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    tmp.close()
    temp_path = Path(tmp.name)
    with Image.open(input_path) as img:
        img.convert("RGB").save(temp_path, "PDF")
    return temp_path


def process_file(input_path: Path, output_dir: Path) -> str:
    print(f"\nProcesando: {input_path.name}")
    text = get_text(input_path)
    doc_type = classify_document(text)
    print(f"  Tipo detectado: {doc_type['label']}")

    output_name = f"{input_path.stem}__[{doc_type['id']}].pdf"
    output_path = output_dir / output_name

    if input_path.suffix.lower() == ".pdf":
        stamp_pdf(input_path, output_path, doc_type)
    else:
        temp_pdf = image_to_temp_pdf(input_path)
        try:
            stamp_pdf(temp_pdf, output_path, doc_type)
        finally:
            try:
                os.remove(temp_pdf)
            except OSError:
                pass

    print(f"  Guardado en: {output_path}")
    return str(doc_type["id"])


def collect_files(input_arg: Path) -> List[Path]:
    if input_arg.is_file():
        return [input_arg] if input_arg.suffix.lower() in SUPPORTED_EXTS else []
    if input_arg.is_dir():
        return sorted(p for p in input_arg.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS)
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description="Clasifica y estampa documentos PDF/JPG/PNG.")
    parser.add_argument("entrada", help="Archivo o carpeta de entrada")
    parser.add_argument("salida", nargs="?", default="documentos_clasificados", help="Carpeta de salida")
    args = parser.parse_args()

    input_arg = Path(args.entrada)
    output_dir = Path(args.salida)
    files = collect_files(input_arg)

    if not files:
        print(f"No se encontraron archivos soportados en: {input_arg}")
        return 1

    print(f"Documentos a procesar: {len(files)}")
    results = {}
    for file_path in files:
        try:
            results[file_path.name] = process_file(file_path, output_dir)
        except Exception as exc:
            results[file_path.name] = "ERROR"
            print(f"  [!] Error procesando {file_path.name}: {exc}")

    print("\nResumen de clasificacion")
    print("=" * 60)
    for filename, doc_id in results.items():
        print(f"{filename:50s} -> {doc_id}")
    print("=" * 60)
    print(f"Archivos guardados en: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
