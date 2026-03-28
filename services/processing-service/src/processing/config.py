from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    processing_host: str
    processing_port: int


settings = Settings()

CHUNK_SIZE = 512
CHUNK_OVERLAP = 64
TOKENIZER_MODEL = "google/gemma-3-1b-it"
