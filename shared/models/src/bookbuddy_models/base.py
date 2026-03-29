from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseDocument(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    content_hash: str
    file_type: str


class BaseSegment(BaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    document_id: UUID
    index: int
    level: int = 0