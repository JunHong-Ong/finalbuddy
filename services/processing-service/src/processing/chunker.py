from abc import ABC, abstractmethod

from bookbuddy_models import Chunk, Document


class BaseChunker(ABC):
    def chunk(self, document: Document) -> list[Chunk]:
        raw = self.split(document)
        return self.map(document, raw)

    @abstractmethod
    def split(self, document: Document) -> list[tuple[str, list[str]]]:
        """
        Walk the document's elements, group text by section, and split each
        group into chunks.

        Returns a list of (section_title, chunk_texts) pairs.
        """
        pass

    @abstractmethod
    def map(self, document: Document, raw: list[tuple[str, list[str]]]) -> list[Chunk]:
        """
        Convert (section_title, chunk_texts) pairs into Chunk model instances.
        chunk_index resets to 0 at the start of each section.
        """
        pass
