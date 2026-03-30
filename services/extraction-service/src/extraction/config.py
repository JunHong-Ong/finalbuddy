from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    extraction_host: str
    extraction_port: int

    graph_host: str
    graph_port: int
    poll_interval: int = 10
    batch_size: int = 32

    @property
    def graph_url(self) -> str:
        return f"http://{self.graph_host}:{self.graph_port}"


settings = Settings()
