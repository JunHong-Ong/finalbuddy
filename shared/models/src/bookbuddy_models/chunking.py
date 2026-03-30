from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class Chunk(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    document_id: UUID
    segment_id: UUID
    chunk_index: int
    chunk_position: float
    text: str