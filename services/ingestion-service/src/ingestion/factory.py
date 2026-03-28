from bookbuddy_models import Document

from ingestion.config import PARSER_REGISTRY
from ingestion.parser import BaseParser


class ParserFactory:
    def get_parser(self, file_type: str) -> BaseParser:
        cls = PARSER_REGISTRY.get(file_type)
        if cls is None:
            raise ValueError(f"No parser registered for file type: {file_type!r}")
        return cls()

    def parse(self, data: bytes, file_type: str) -> Document:
        return self.get_parser(file_type).parse(data)
