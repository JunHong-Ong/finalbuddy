from pydantic_settings import BaseSettings, SettingsConfigDict

from ingestion.parser import BaseParser
from ingestion.parsers.pdf.pymupdf import PDFParser


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    ingestion_host: str
    ingestion_port: int


settings = Settings()

PARSER_REGISTRY: dict[str, type[BaseParser]] = {
    "pdf": PDFParser,
}
