"""
Modelos de datos compartidos de ReadMail.

UnifiedFile, ClientRecord y ClientMatchResult espejan EXACTAMENTE las
clases homonimas de reademail.py (incluyendo properties y defaults).
Son los modelos que fluyen entre zip_handler, document_classifier,
client_matching y modulos futuros.
"""

from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class ClientRecord:
    name: str
    normalized_name: str
    nit: Optional[str] = None
    normalized_nit: Optional[str] = None
    active: bool = True
    raw_row: Dict[str, str] = field(default_factory=dict)


@dataclass
class ClientMatchResult:
    record: Optional[ClientRecord] = None
    raw: Optional[str] = None
    source: str = ""


@dataclass
class UnifiedFile:
    name: str
    mime_type: str
    data: bytes
    source: str
    extracted_text: str = ""

    @property
    def lower_name(self) -> str:
        return self.name.lower()

    @property
    def is_pdf(self) -> bool:
        return self.lower_name.endswith(".pdf") or self.mime_type == "application/pdf"

    @property
    def is_xml(self) -> bool:
        return self.lower_name.endswith(".xml") or self.mime_type in ("application/xml", "text/xml")

    @property
    def is_image(self) -> bool:
        return self.lower_name.endswith((".jpg", ".jpeg", ".png")) or self.mime_type in ("image/jpeg", "image/png")
