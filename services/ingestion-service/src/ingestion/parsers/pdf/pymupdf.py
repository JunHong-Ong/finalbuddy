import base64
import hashlib
from typing import Any
from uuid import uuid4

import fitz
from bookbuddy_models import Document, Element, Segment

from ingestion.parser import BaseParser


class PDFParser(BaseParser):
    def extract(self, data: bytes) -> tuple[str, fitz.Document]:
        content_hash = hashlib.sha256(data).hexdigest()
        fitz_doc = fitz.open(stream=data, filetype="pdf")
        return content_hash, fitz_doc

    def map(self, raw: Any) -> Document:
        content_hash, fitz_doc = raw
        doc_id = uuid4()
        segments: list[Segment] = []

        for page in fitz_doc:
            elements: list[Element] = []

            for order, block in enumerate(page.get_text("dict")["blocks"]):
                if block["type"] == 0:  # text block
                    text = "\n".join(
                        "".join(span["text"] for span in line["spans"])
                        for line in block["lines"]
                    )
                    if text.strip():
                        elements.append(Element(type="text", content=text, order=order))
                elif block["type"] == 1:  # image block
                    b64 = base64.b64encode(block["image"]).decode("utf-8")
                    elements.append(Element(type="image", content=b64, order=order))

            segments.append(
                Segment(document_id=doc_id, index=page.number, elements=elements)
            )

        fitz_doc.close()

        return Document(
            id=doc_id,
            content_hash=content_hash,
            file_type="pdf",
            segment_count=len(segments),
            segments=segments,
        )
