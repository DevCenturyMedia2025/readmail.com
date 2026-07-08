"""
Modelos de datos compartidos de ReadMail.

UnifiedFile espeja EXACTAMENTE la clase homonima de reademail.py
(incluyendo sus properties is_pdf, is_xml, is_image). Es el modelo que
fluye entre zip_handler, document_classifier y modulos futuros.
"""

from dataclasses import dataclass


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
