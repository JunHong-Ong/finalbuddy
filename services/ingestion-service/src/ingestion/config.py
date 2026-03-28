from ingestion.parser import BaseParser
from ingestion.parsers.pdf.pymupdf import PDFParser

PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "pdf": PDFParser,
}
