from pydantic import BaseModel

from bookbuddy_models.base import BaseDocument, BaseSegment


class Element(BaseModel):
    type: str
    content: str
    order: int


class Segment(BaseSegment):
    elements: list[Element]


class Document(BaseDocument):
    segment_count: int
    segments: list[Segment]
