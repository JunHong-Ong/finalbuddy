from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class BaseDocument(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    content_hash: str
    file_type: str