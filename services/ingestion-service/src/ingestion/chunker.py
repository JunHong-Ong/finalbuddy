from abc import ABC, abstractmethod
from uuid import UUID

from bookbuddy_models import Chunk, Document


class BaseChunker(ABC):
    def chunk(self, document: Document) -> list[Chunk]:
        raw = self.split(document)
        return self.map(document, raw)

    @abstractmethod
    def split(self, document: Document) -> list[tuple[UUID, list[str]]]:
        """
        Walk the document's segments, split each segment's text into chunks.

        Returns a list of (segment_id, chunk_texts) pairs.
        """
        pass

    @abstractmethod
    def map(self, document: Document, raw: list[tuple[UUID, list[str]]]) -> list[Chunk]:
        """
        Convert (segment_id, chunk_texts) pairs into Chunk model instances.
        chunk_index resets to 0 at the start of each segment.
        """
        pass
