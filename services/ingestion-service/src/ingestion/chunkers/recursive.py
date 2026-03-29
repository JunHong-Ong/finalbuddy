from uuid import UUID

from bookbuddy_models import Chunk, Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from transformers import AutoTokenizer

from ingestion.chunker import BaseChunker
from ingestion.config import CHUNK_OVERLAP, CHUNK_SIZE, TOKENIZER_MODEL

_tokenizer = AutoTokenizer.from_pretrained(TOKENIZER_MODEL)

_splitter = RecursiveCharacterTextSplitter.from_huggingface_tokenizer(
    _tokenizer,
    separators=["\n\n", ". ", ""],
    chunk_size=CHUNK_SIZE,
    chunk_overlap=CHUNK_OVERLAP,
)


class RecursiveChunker(BaseChunker):
    def split(self, document: Document) -> list[tuple[UUID, list[str]]]:
        results: list[tuple[UUID, list[str]]] = []

        for segment in document.segments:
            text_parts = [
                element.content
                for element in sorted(segment.elements, key=lambda e: e.order)
                if element.type == "text"
            ]
            if text_parts:
                chunks = _splitter.split_text("\n".join(text_parts))
                if chunks:
                    results.append((segment.uuid, chunks))

        return results

    def map(self, document: Document, raw: list[tuple[UUID, list[str]]]) -> list[Chunk]:
        chunks: list[Chunk] = []
        for segment_id, texts in raw:
            for index, text in enumerate(texts):
                chunks.append(
                    Chunk(
                        document_id=document.uuid,
                        segment_id=segment_id,
                        chunk_index=index,
                        text=text,
                    )
                )
        return chunks
