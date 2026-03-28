from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Chunk(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    document_id: UUID
    section_title: str
    chunk_index: int
    text: str