from uuid import UUID

from pydantic import BaseModel


class Keyword(BaseModel):
    name: str
    aliases: list[str] = []


class Entity(BaseModel):
    keyword: str
    span: str
    start: int
    end: int


class ExtractionResult(BaseModel):
    chunk_id: UUID
    entities: list[Entity]
