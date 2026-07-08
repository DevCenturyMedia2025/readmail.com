"""
Manejo seguro de archivos ZIP para ReadMail.

Modulo paralelo a las funciones ZIP de reademail.py (analyze_zip_bytes,
extract_zip_files y helpers). Diferencias deliberadas:

- Los limites se reciben por parametro (con los mismos defaults del original),
  lo que permite pruebas con valores pequenos.

El comportamiento, mensajes de error y defaults se preservan EXACTOS.
Todavia no esta conectado a reademail.py.
"""

import io
import zipfile
from typing import Dict, List, Optional

from app.models import UnifiedFile
from app.utils.text import ensure_list


DEFAULT_MAX_ZIP_BYTES = 25 * 1024 * 1024
DEFAULT_MAX_ZIP_FILES = 250
DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED = 150 * 1024 * 1024
DEFAULT_MAX_ZIP_SINGLE_FILE = 25 * 1024 * 1024
DEFAULT_MAX_ZIP_NESTING = 2


def is_zip_attachment(att: Dict[str, Optional[str]]) -> bool:
    fn = (att.get("filename") or "").lower()
    mt = (att.get("mimeType") or "").lower()
    return fn.endswith(".zip") or mt in ("application/zip", "application/x-zip-compressed")


def is_safe_zip_member(name: str) -> bool:
    if not name:
        return False
    n = name.replace("\\", "/")
    if n.startswith("/") or n.startswith("../") or "/../" in n:
        return False
    return True


def is_ignored_zip_member(name: str) -> bool:
    normalized = (name or "").replace("\\", "/")
    parts = [part for part in normalized.split("/") if part]
    if not parts:
        return True
    return "__MACOSX" in parts or any(part.startswith("._") for part in parts) or parts[-1] == ".DS_Store"


def analyze_zip_bytes(
    zip_filename: str,
    zip_bytes: bytes,
    depth: int = 1,
    max_zip_bytes: int = DEFAULT_MAX_ZIP_BYTES,
    max_zip_files: int = DEFAULT_MAX_ZIP_FILES,
    max_zip_total_uncompressed: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED,
    max_zip_single_file: int = DEFAULT_MAX_ZIP_SINGLE_FILE,
    max_zip_nesting: int = DEFAULT_MAX_ZIP_NESTING,
) -> Dict[str, object]:
    out = {
        "zip_filename": zip_filename,
        "ok": True,
        "error": None,
        "files": [],
        "pdf_count": 0,
        "xml_count": 0,
        "image_count": 0,
        "total_uncompressed": 0,
    }

    try:
        if zip_bytes is None:
            out["ok"] = False
            out["error"] = "ZIP vacio"
            return out

        if len(zip_bytes) > max_zip_bytes:
            out["ok"] = False
            out["error"] = f"ZIP excede MAX_ZIP_BYTES ({len(zip_bytes)} > {max_zip_bytes})"
            return out

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            infos = zf.infolist()
            if len(infos) > max_zip_files:
                out["ok"] = False
                out["error"] = f"ZIP tiene demasiados archivos ({len(infos)} > {max_zip_files})"
                return out

            total = 0
            pdf_count = 0
            xml_count = 0
            image_count = 0
            files = []

            for info in infos:
                if info.is_dir():
                    continue
                if is_ignored_zip_member(info.filename):
                    continue
                if info.flag_bits & 0x1:
                    out["ok"] = False
                    out["error"] = "ZIP protegido con contrasena"
                    return out

                name = info.filename
                if not is_safe_zip_member(name):
                    out["ok"] = False
                    out["error"] = f"Ruta insegura dentro del ZIP: {name}"
                    return out

                size = int(getattr(info, "file_size", 0) or 0)
                if size > max_zip_single_file:
                    out["ok"] = False
                    out["error"] = f"Archivo dentro del ZIP demasiado grande: {name}"
                    return out

                lower = name.lower()
                is_pdf = lower.endswith(".pdf")
                is_xml = lower.endswith(".xml")
                is_image = lower.endswith((".jpg", ".jpeg", ".png"))
                is_zip = lower.endswith(".zip")
                entry = {
                    "name": name,
                    "size": size,
                    "is_pdf": is_pdf,
                    "is_xml": is_xml,
                    "is_image": is_image,
                    "is_zip": is_zip,
                }
                files.append(entry)

                if is_zip:
                    if depth >= max_zip_nesting:
                        out["ok"] = False
                        out["error"] = f"ZIP anidado excede MAX_ZIP_NESTING en {name}"
                        return out
                    nested_bytes = zf.read(name)
                    nested_analysis = analyze_zip_bytes(
                        f"{zip_filename}/{name}",
                        nested_bytes,
                        depth=depth + 1,
                        max_zip_bytes=max_zip_bytes,
                        max_zip_files=max_zip_files,
                        max_zip_total_uncompressed=max_zip_total_uncompressed,
                        max_zip_single_file=max_zip_single_file,
                        max_zip_nesting=max_zip_nesting,
                    )
                    if not nested_analysis.get("ok"):
                        out["ok"] = False
                        out["error"] = f"{name}: {nested_analysis.get('error')}"
                        return out
                    entry["nested"] = nested_analysis
                    pdf_count += int(nested_analysis.get("pdf_count") or 0)
                    xml_count += int(nested_analysis.get("xml_count") or 0)
                    image_count += int(nested_analysis.get("image_count") or 0)
                    total += int(nested_analysis.get("total_uncompressed") or 0)
                    if total > max_zip_total_uncompressed:
                        out["ok"] = False
                        out["error"] = "ZIP excede el tamano total descomprimido permitido"
                        return out
                    continue

                total += size
                if total > max_zip_total_uncompressed:
                    out["ok"] = False
                    out["error"] = "ZIP excede el tamano total descomprimido permitido"
                    return out
                if is_pdf:
                    pdf_count += 1
                if is_xml:
                    xml_count += 1
                if is_image:
                    image_count += 1

            out["files"] = files
            out["pdf_count"] = pdf_count
            out["xml_count"] = xml_count
            out["image_count"] = image_count
            out["total_uncompressed"] = total
            return out

    except zipfile.BadZipFile:
        out["ok"] = False
        out["error"] = "ZIP corrupto o invalido"
        return out
    except Exception as e:
        out["ok"] = False
        out["error"] = f"Error leyendo ZIP: {e}"
        return out


def extract_zip_files(
    zip_filename: str,
    zip_bytes: bytes,
    max_zip_bytes: int = DEFAULT_MAX_ZIP_BYTES,
    max_zip_files: int = DEFAULT_MAX_ZIP_FILES,
    max_zip_total_uncompressed: int = DEFAULT_MAX_ZIP_TOTAL_UNCOMPRESSED,
    max_zip_single_file: int = DEFAULT_MAX_ZIP_SINGLE_FILE,
    max_zip_nesting: int = DEFAULT_MAX_ZIP_NESTING,
) -> Dict[str, object]:
    analysis = analyze_zip_bytes(
        zip_filename,
        zip_bytes,
        max_zip_bytes=max_zip_bytes,
        max_zip_files=max_zip_files,
        max_zip_total_uncompressed=max_zip_total_uncompressed,
        max_zip_single_file=max_zip_single_file,
        max_zip_nesting=max_zip_nesting,
    )
    if not analysis.get("ok"):
        return {"ok": False, "error": analysis.get("error"), "files": []}

    output: List[UnifiedFile] = []

    def _extract_level(current_bytes: bytes, node: Dict[str, object], prefix: str = "") -> None:
        with zipfile.ZipFile(io.BytesIO(current_bytes)) as zf:
            for file_info in ensure_list(node.get("files")):
                name = (file_info.get("name") or "").strip()
                if not name or not is_safe_zip_member(name):
                    continue
                if is_ignored_zip_member(name):
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
                elif lower.endswith((".jpg", ".jpeg")):
                    mime = "image/jpeg"
                elif lower.endswith(".png"):
                    mime = "image/png"

                output.append(UnifiedFile(name=full_name, mime_type=mime, data=raw, source=f"zip:{zip_filename}"))

    try:
        _extract_level(zip_bytes, analysis)
        return {"ok": True, "error": None, "files": output, "analysis": analysis}
    except Exception as e:
        return {"ok": False, "error": str(e), "files": [], "analysis": analysis}
