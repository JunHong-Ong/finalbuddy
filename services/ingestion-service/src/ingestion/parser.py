from abc import ABC, abstractmethod
from typing import Any

from bookbuddy_models import Document


class BaseParser(ABC):
    def parse(self, data: bytes) -> Document:
        raw = self.extract(data)
        doc = self.map(raw)
        return self.postprocess(doc)

    @abstractmethod
    def extract(self, data: bytes) -> Any:
        """Extract raw content from the source bytes."""
        pass

    @abstractmethod
    def map(self, raw: Any) -> Document:
        """Map extracted raw content into the Document model."""
        pass

    def postprocess(self, doc: Document) -> Document:
        """Optional post-processing. Default: no-op."""
        return doc
